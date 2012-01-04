/*

   Derby - Class org.apache.derby.impl.jdbc.authentication.AuthenticationServiceBase

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.jdbc.authentication;

import org.apache.derby.authentication.UserAuthenticator;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.jdbc.AuthenticationService;

import org.apache.derby.iapi.reference.Limits;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.daemon.Serviceable;

import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.services.property.PropertyFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.services.property.PropertySetCallback;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.Attribute;

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.util.StringUtil;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.PasswordHasher;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import java.util.Dictionary;
import java.util.Properties;
import org.apache.derby.iapi.reference.SQLState;

/**
 * <p>
 * This is the authentication service base class.
 * </p>

 * <p>
 * There can be 1 Authentication Service for the whole Derby
 * system and/or 1 authentication per database.
 * In a near future, we intend to allow multiple authentication services
 * per system and/or per database.
 * </p>
 *
 * <p>
 * It should be extended by the specialized authentication services.
 * </p>
 *
 * <p><strong>IMPORTANT NOTE:</strong></p>
 *
 * <p>
 * User passwords are encrypted using a message digest algorithm
 * if they're stored in the database; otherwise they are not encrypted
 * if they were defined at the system level.
 * </p>
 *
 * <p>
 * The passwords can be encrypted using two different schemes:
 * </p>
 *
 * <ul>
 * <li>The SHA-1 authentication scheme, which was the only available scheme
 * in Derby 10.5 and earlier. This scheme uses the SHA-1 message digest
 * algorithm.</li>
 * <li>The configurable hash authentication scheme, which allows the users to
 * specify which message digest algorithm to use.</li>
 * </ul>
 *
 * <p>
 * In order to use the configurable hash authentication scheme, the users have
 * to set the {@code derby.authentication.builtin.algorithm} property (on
 * system level or database level) to the name of an algorithm that's available
 * in one of the security providers registered on the system. If this property
 * is not set, or if it's set to NULL or an empty string, the SHA-1
 * authentication scheme is used.
 * </p>
 *
 * <p>
 * Which scheme to use is decided when a password is about to be stored in the
 * database. One database may therefore contain passwords stored using
 * different schemes. In order to determine which scheme to use when comparing
 * a user's credentials with those stored in the database, the stored password
 * is prefixed with an identifier that tells which scheme is being used.
 * Passwords stored using the SHA-1 authentication scheme are prefixed with
 * {@link PasswordHasher#ID_PATTERN_SHA1_SCHEME}. Passwords that are stored using the
 * configurable hash authentication scheme are prefixed with
 * {@link PasswordHasher#ID_PATTERN_CONFIGURABLE_HASH_SCHEME} and suffixed with the name of
 * the message digest algorithm.
 * </p>
 */
public abstract class AuthenticationServiceBase
	implements AuthenticationService, ModuleControl, ModuleSupportable, PropertySetCallback {

	protected UserAuthenticator authenticationScheme; 

	// required to retrieve service properties
	private AccessFactory store;

	/**
		Trace flag to trace authentication operations
	*/
	public static final String AuthenticationTrace =
						SanityManager.DEBUG ? "AuthenticationTrace" : null;

    /**
        Userid with Strong password substitute DRDA security mechanism
    */
    protected static final int SECMEC_USRSSBPWD = 8;

	//
	// constructor
	//
	public AuthenticationServiceBase() {
	}

	protected void setAuthenticationService(UserAuthenticator aScheme) {
		// specialized class is the principal caller.
		this.authenticationScheme = aScheme;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(this.authenticationScheme != null, 
				"There is no authentication scheme for that service!");
		
			if (SanityManager.DEBUG_ON(AuthenticationTrace)) {

				java.io.PrintWriter iDbgStream =
					SanityManager.GET_DEBUG_STREAM();

				iDbgStream.println("Authentication Service: [" +
								this.toString() + "]");
				iDbgStream.println("Authentication Scheme : [" +
								this.authenticationScheme.toString() + "]");
			}
		}
	}

	/**
	/*
	** Methods of module control - To be overriden
	*/

	/**
		Start this module.  In this case, nothing needs to be done.
		@see org.apache.derby.iapi.services.monitor.ModuleControl#boot

		@exception StandardException upon failure to load/boot
		the expected authentication service.
	 */
	 public void boot(boolean create, Properties properties)
	  throws StandardException
	 {
			//
			// we expect the Access factory to be available since we're
			// at boot stage.
			//
			store = (AccessFactory)
				Monitor.getServiceModule(this, AccessFactory.MODULE);
			// register to be notified upon db properties changes
			// _only_ if we're on a database context of course :)

			PropertyFactory pf = (PropertyFactory)
				Monitor.getServiceModule(this, org.apache.derby.iapi.reference.Module.PropertyFactory);
			if (pf != null)
				pf.addPropertySetNotification(this);

	 }

	/**
	 * @see org.apache.derby.iapi.services.monitor.ModuleControl#stop
	 */
	public void stop() {

		// nothing special to be done yet.
	}
	/*
	** Methods of AuthenticationService
	*/

	/**
	 * Authenticate a User inside JBMS.T his is an overload method.
	 *
	 * We're passed-in a Properties object containing user credentials information
	 * (as well as database name if user needs to be validated for a certain
	 * database access).
	 *
	 * @see
	 * org.apache.derby.iapi.jdbc.AuthenticationService#authenticate
	 *
	 *
	 */
	public boolean authenticate(String databaseName, Properties userInfo) throws java.sql.SQLException
	{
		if (userInfo == (Properties) null)
			return false;

		String userName = userInfo.getProperty(Attribute.USERNAME_ATTR);
		if ((userName != null) && userName.length() > Limits.DB2_MAX_USERID_LENGTH) {
		// DB2 has limits on length of the user id, so we enforce the same.
		// This used to be error 28000 "Invalid authorization ID", but with v82,
		// DB2 changed the behavior to return a normal "authorization failure
		// occurred" error; so that means just return "false" and the correct
		// exception will be thrown as usual.
			return false;
		}

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(AuthenticationTrace)) {

				java.io.PrintWriter iDbgStream =
					SanityManager.GET_DEBUG_STREAM();

				iDbgStream.println(
								" - Authentication request: user [" +
							    userName + "]"+ ", database [" +
							    databaseName + "]");
				// The following will print the stack trace of the
				// authentication request to the log.  
				//Throwable t = new Throwable();
				//istream.println("Authentication Request Stack trace:");
				//t.printStackTrace(istream.getPrintWriter());
			}
		}
		return this.authenticationScheme.authenticateUser(userName,
						  userInfo.getProperty(Attribute.PASSWORD_ATTR),
						  databaseName,
						  userInfo
						 );
	}

	/**
	 * Returns a property if it was set at the database or
	 * system level. Treated as SERVICE property by default.
	 *
	 * @return a property string value.
	 **/
	public String getProperty(String key) {

		String propertyValue = null;
		TransactionController tc = null;

		try {

		  if (store != null)
          {
            tc = store.getTransaction(
                ContextService.getFactory().getCurrentContextManager());
          }

		  propertyValue =
			PropertyUtil.getServiceProperty(tc,
											key,
											(String) null);
		  if (tc != null) {
			tc.commit();
			tc = null;
		  }

		} catch (StandardException se) {
			// Do nothing and just return
		}

		return propertyValue;
	}

	public String getDatabaseProperty(String key) {

		String propertyValue = null;
		TransactionController tc = null;

		try {

		  if (store != null)
			tc = store.getTransaction(
                ContextService.getFactory().getCurrentContextManager());

		  propertyValue =
			PropertyUtil.getDatabaseProperty(tc, key);

		  if (tc != null) {
			tc.commit();
			tc = null;
		  }

		} catch (StandardException se) {
			// Do nothing and just return
		}

		return propertyValue;
	}

	public String getSystemProperty(String key) {

		boolean dbOnly = false;
		dbOnly = Boolean.valueOf(
					this.getDatabaseProperty(
							Property.DATABASE_PROPERTIES_ONLY)).booleanValue();

		if (dbOnly)
			return null;

		return PropertyUtil.getSystemProperty(key);
	}

	/*
	** Methods of PropertySetCallback
	*/
	public void init(boolean dbOnly, Dictionary p) {
		// not called yet ...
	}

	/**
	  @see PropertySetCallback#validate
	*/
	public boolean validate(String key, Serializable value, Dictionary p)	{
		return key.startsWith(org.apache.derby.iapi.reference.Property.USER_PROPERTY_PREFIX);
	}
	/**
	  @see PropertySetCallback#validate
	*/
	public Serviceable apply(String key,Serializable value,Dictionary p)
	{
		return null;
	}
	/**
	  @see PropertySetCallback#map
	  @exception StandardException Thrown on error.
	*/
	public Serializable map(String key, Serializable value, Dictionary p)
		throws StandardException
	{
		// We only care for "derby.user." property changes
		// at the moment.
		if (!key.startsWith(org.apache.derby.iapi.reference.Property.USER_PROPERTY_PREFIX)) return null;
		// We do not encrypt 'derby.user.<userName>' password if
		// the configured authentication service is LDAP as the
		// same property could be used to store LDAP user full DN (X500).
		// In performing this check we only consider database properties
		// not system, service or application properties.

		String authService =
			(String)p.get(org.apache.derby.iapi.reference.Property.AUTHENTICATION_PROVIDER_PARAMETER);

		if ((authService != null) &&
			 (StringUtil.SQLEqualsIgnoreCase(authService, org.apache.derby.iapi.reference.Property.AUTHENTICATION_PROVIDER_LDAP)))
			return null;

		// Ok, we can encrypt this password in the db
		String userPassword = (String) value;

		if (userPassword != null) {
			// encrypt (digest) the password
			// the caller will retrieve the new value
            String userName =
                    key.substring(Property.USER_PROPERTY_PREFIX.length());
            userPassword =
                    encryptUsingDefaultAlgorithm(userName, userPassword, p);
		}

		return userPassword;
	}


	// Class implementation

	protected final boolean requireAuthentication(Properties properties) {

		//
		// we check if derby.connection.requireAuthentication system
		// property is set to true, otherwise we are the authentication
		// service that should be run.
		//
		String requireAuthentication = PropertyUtil.getPropertyFromSet(
					properties,
					org.apache.derby.iapi.reference.Property.REQUIRE_AUTHENTICATION_PARAMETER
														);
		return Boolean.valueOf(requireAuthentication).booleanValue();
	}

	/**
     * <p>
	 * This method encrypts a clear user password using a
	 * Single Hash algorithm such as SHA-1 (SHA equivalent)
	 * (it is a 160 bits digest)
     * </p>
	 *
     * <p>
	 * The digest is returned as an object string.
     * </p>
     *
     * <p>
     * This method is only used by the SHA-1 authentication scheme.
     * </p>
	 *
	 * @param plainTxtUserPassword Plain text user password
	 *
	 * @return encrypted user password (digest) as a String object
     *         or {@code null} if the plaintext password is {@code null}
	 */
	protected String encryptPasswordSHA1Scheme(String plainTxtUserPassword)
	{
		if (plainTxtUserPassword == null)
			return null;

		MessageDigest algorithm = null;
		try
		{
			algorithm = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException nsae)
		{
					// Ignore as we checked already during service boot-up
		}

		algorithm.reset();
		byte[] bytePasswd = null;
        bytePasswd = toHexByte(plainTxtUserPassword);
		algorithm.update(bytePasswd);
		byte[] encryptVal = algorithm.digest();
        String hexString = PasswordHasher.ID_PATTERN_SHA1_SCHEME +
                StringUtil.toHexString(encryptVal,0,encryptVal.length);
		return (hexString);

	}

    /**
     * <p>
     * Convert a string into a byte array in hex format.
     * </p>
     *
     * <p>
     * For each character (b) two bytes are generated, the first byte
     * represents the high nibble (4 bits) in hexadecimal ({@code b & 0xf0}),
     * the second byte represents the low nibble ({@code b & 0x0f}).
     * </p>
     *
     * <p>
     * The character at {@code str.charAt(0)} is represented by the first two
     * bytes in the returned String.
     * </p>
     *
     * <p>
     * New code is encouraged to use {@code String.getBytes(String)} or similar
     * methods instead, since this method does not preserve all bits for
     * characters whose codepoint exceeds 8 bits. This method is preserved for
     * compatibility with the SHA-1 authentication scheme.
     * </p>
     *
     * @param str string
     * @return the byte[] (with hexadecimal format) form of the string (str)
     */
    private static byte[] toHexByte(String str)
    {
        byte[] data = new byte[str.length() * 2];

        for (int i = 0; i < str.length(); i++)
        {
            char ch = str.charAt(i);
            int high_nibble = (ch & 0xf0) >>> 4;
            int low_nibble = (ch & 0x0f);
            data[i] = (byte)high_nibble;
            data[i+1] = (byte)low_nibble;
        }
        return data;
    }

    /**
     * <p>
     * Encrypt a password using the default hash algorithm for this system
     * before it's stored in the database.
     * </p>
     *
     * <p>
     * If the data dictionary supports the configurable hash authentication
     * scheme, and the property {@code derby.authentication.builtin.algorithm}
     * is a non-empty string, the password will be encrypted using the
     * algorithm specified by that property. Otherwise, we fall back to the new
     * authentication scheme based on SHA-1. The algorithm used is encoded in
     * the returned token so that the code that validates a user's credentials
     * knows which algorithm to use.
     * </p>
     *
     * @param user the user whose password to encrypt
     * @param password the plain text password
     * @param props database properties
     * @return a digest of the user name and password formatted as a string,
     *         or {@code null} if {@code password} is {@code null}
     * @throws StandardException if the specified algorithm is not supported
     */
    private String encryptUsingDefaultAlgorithm(String user,
                                                String password,
                                                Dictionary props)
            throws StandardException
    {
        if ( password ==  null ) { return null; }

        PasswordHasher  hasher = getDataDictionary().makePasswordHasher( props );

        if ( hasher != null ) { return hasher.hashAndEncode( user, password ); }
        else { return encryptPasswordSHA1Scheme(password); }
    }

    /**
     * Find the data dictionary for the current connection.
     *
     * @return the {@code DataDictionary} for the current connection
     */
    private static DataDictionary getDataDictionary() {
        LanguageConnectionContext lcc = (LanguageConnectionContext)
            ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
        return lcc.getDataDictionary();
    }

    /**
     * Strong Password Substitution (USRSSBPWD).
     *
     * This method generates a password substitute to authenticate a client
     * which is using a DRDA security mechanism such as SECMEC_USRSSBPWD.
     *
     * Depending how the user is defined in Derby and if BUILTIN
     * is used, the stored password can be in clear-text (system level)
     * or encrypted (hashed - *not decryptable*)) (database level) - If the
     * user has authenticated at the network level via SECMEC_USRSSBPWD, it
     * means we're presented with a password substitute and we need to
     * generate a substitute password coming from the store to compare with
     * the one passed-in.
     *
     * The substitution algorithm used is the same as the one used in the
     * SHA-1 authentication scheme ({@link PasswordHasher#ID_PATTERN_SHA1_SCHEME}), so in
     * the case of database passwords stored using that scheme, we can simply
     * compare the received hash with the stored hash. If the configurable
     * hash authentication scheme {@link PasswordHasher#ID_PATTERN_CONFIGURABLE_HASH_SCHEME}
     * is used, we have no way to find out if the received hash matches the
     * stored password, since we cannot decrypt the hashed passwords and
     * re-apply another hash algorithm. Therefore, strong password substitution
     * only works if the database-level passwords are stored with the SHA-1
     * scheme.
     *
     * NOTE: A lot of this logic could be shared with the DRDA decryption
     *       and client encryption managers - This will be done _once_
     *       code sharing along with its rules are defined between the
     *       Derby engine, client and network code (PENDING).
     * 
     * Substitution algorithm works as follow:
     *
     * PW_TOKEN = SHA-1(PW, ID)
     * The password (PW) and user name (ID) can be of any length greater
     * than or equal to 1 byte.
     * The client generates a 20-byte password substitute (PW_SUB) as follows:
     * PW_SUB = SHA-1(PW_TOKEN, RDr, RDs, ID, PWSEQs)
     * 
     * w/ (RDs) as the random client seed and (RDr) as the server one.
     * 
     * See PWDSSB - Strong Password Substitution Security Mechanism
     * (DRDA Vol.3 - P.650)
     *
	 * @return a substituted password.
     */
    protected String substitutePassword(
                String userName,
                String password,
                Properties info,
                boolean databaseUser) {

        MessageDigest messageDigest = null;

        // PWSEQs's 8-byte value constant - See DRDA Vol 3
        byte SECMEC_USRSSBPWD_PWDSEQS[] = {
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01
                };
        
        // Generated password substitute
        byte[] passwordSubstitute;

        try
        {
            messageDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException nsae)
        {
            // Ignore as we checked already during service boot-up
        }
        // IMPORTANT NOTE: As the password is stored single-hashed in the
        // database, it is impossible for us to decrypt the password and
        // recompute a substitute to compare with one generated on the source
        // side - Hence, we have to generate a password substitute.
        // In other words, we cannot figure what the original password was -
        // Strong Password Substitution (USRSSBPWD) cannot be supported for
        // targets which can't access or decrypt passwords on their side.
        //
        messageDigest.reset();

        byte[] bytePasswd = null;
        byte[] userBytes = toHexByte(userName);

        if (SanityManager.DEBUG)
        {
            // We must have a source and target seed 
            SanityManager.ASSERT(
              (((String) info.getProperty(Attribute.DRDA_SECTKN_IN) != null) &&
              ((String) info.getProperty(Attribute.DRDA_SECTKN_OUT) != null)), 
                "Unexpected: Requester or server seed not available");
        }

        // Retrieve source (client)  and target 8-byte seeds
        String sourceSeedstr = info.getProperty(Attribute.DRDA_SECTKN_IN);
        String targetSeedstr = info.getProperty(Attribute.DRDA_SECTKN_OUT);

        byte[] sourceSeed_ =
            StringUtil.fromHexString(sourceSeedstr, 0, sourceSeedstr.length());
        byte[] targetSeed_ =
            StringUtil.fromHexString(targetSeedstr, 0, targetSeedstr.length());

        String hexString = null;
        // If user is at the database level, we don't encrypt the password
        // as it is already encrypted (BUILTIN scheme) - we only do the
        // BUILTIN encryption if the user is defined at the system level
        // only - this is required beforehands so that we can do the password
        // substitute generation right afterwards.
        if (!databaseUser)
        {
            bytePasswd = toHexByte(password);
            messageDigest.update(bytePasswd);
            byte[] encryptVal = messageDigest.digest();
            hexString = PasswordHasher.ID_PATTERN_SHA1_SCHEME +
                StringUtil.toHexString(encryptVal, 0, encryptVal.length);
        }
        else
        {
            // Already encrypted from the database store
            // NOTE: If the password was stored with the configurable hash
            // authentication scheme, the stored password will have been hashed
            // with a different algorithm than the hashed password sent from
            // the client. Since there's no way to decrypt the stored password
            // and rehash it with the algorithm that the client uses, we are
            // not able to compare the passwords, and the connection attempt
            // will fail.
            hexString = password;
        }

        // Generate the password substitute now

        // Generate some 20-byte password token
        messageDigest.update(userBytes);
        messageDigest.update(toHexByte(hexString));
        byte[] passwordToken = messageDigest.digest();
        
        // Now we generate the 20-byte password substitute
        messageDigest.update(passwordToken);
        messageDigest.update(targetSeed_);
        messageDigest.update(sourceSeed_);
        messageDigest.update(userBytes);
        messageDigest.update(SECMEC_USRSSBPWD_PWDSEQS);

        passwordSubstitute = messageDigest.digest();

        return StringUtil.toHexString(passwordSubstitute, 0,
                                      passwordSubstitute.length);
    }
}
