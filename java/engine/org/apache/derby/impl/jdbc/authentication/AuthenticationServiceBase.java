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
import org.apache.derby.iapi.services.i18n.MessageService;

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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.io.Serializable;
import java.util.Dictionary;
import java.util.Properties;
import java.util.Date;

/**
 * This is the authentication service base class.
 * <p>
 * There can be 1 Authentication Service for the whole Derby
 * system and/or 1 authentication per database.
 * In a near future, we intend to allow multiple authentication services
 * per system and/or per database.
 * <p>
 * It should be extended by the specialized authentication services.
 *
 * IMPORTANT NOTE:
 * --------------
 * User passwords are encrypted using SHA-1 message digest algorithm
 * if they're stored in the database; otherwise they are not encrypted
 * if they were defined at the system level.
 * SHA-1 digest is single hash (one way) digest and is considered very
 * secure (160 bits).
 *
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
		Pattern that is prefixed to the stored password in the new authentication scheme
	*/
	public static final String ID_PATTERN_NEW_SCHEME = "3b60";

    /**
        Userid with Strong password substitute DRDA security mechanism
    */
    protected static final int SECMEC_USRSSBPWD = 8;

	/**
		Length of the encrypted password in the new authentication scheme
		See Beetle4601
	*/
	public static final int MAGICLEN_NEWENCRYPT_SCHEME=44;

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
			userPassword = encryptPassword(userPassword);
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
	 * This method encrypts a clear user password using a
	 * Single Hash algorithm such as SHA-1 (SHA equivalent)
	 * (it is a 160 bits digest)
	 *
	 * The digest is returned as an object string.
	 *
	 * @param plainTxtUserPassword Plain text user password
	 *
	 * @return encrypted user password (digest) as a String object
	 */
	protected String encryptPassword(String plainTxtUserPassword)
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
        bytePasswd = StringUtil.toHexByte(
                plainTxtUserPassword,0,plainTxtUserPassword.length());
		algorithm.update(bytePasswd);
		byte[] encryptVal = algorithm.digest();
        String hexString = ID_PATTERN_NEW_SCHEME +
                StringUtil.toHexString(encryptVal,0,encryptVal.length);
		return (hexString);

	}

    /**
     * Strong Password Substitution (USRSSBPWD).
     *
     * This method generate a password subtitute to authenticate a client
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

        // Pattern that is prefixed to the BUILTIN encrypted password
        String ID_PATTERN_NEW_SCHEME = "3b60";

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
        byte[] userBytes = StringUtil.toHexByte(userName, 0, userName.length());

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
            bytePasswd = StringUtil.toHexByte(password, 0, password.length());
            messageDigest.update(bytePasswd);
            byte[] encryptVal = messageDigest.digest();
            hexString = ID_PATTERN_NEW_SCHEME +
                StringUtil.toHexString(encryptVal, 0, encryptVal.length);
        }
        else
            // Already encrypted from the database store
            hexString = password;

        // Generate the password substitute now

        // Generate some 20-byte password token
        messageDigest.update(userBytes);
        messageDigest.update(
                StringUtil.toHexByte(hexString, 0, hexString.length()));
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
