/*

   Derby - Class org.apache.derby.impl.jdbc.authentication.AuthenticationServiceBase

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

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

import org.apache.derby.iapi.reference.DB2Limit;

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
 * There can be 1 Authentication Service for the whole Cloudscape
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
 * @author Francois
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
		if ((userName != null) && userName.length() > DB2Limit.MAX_USERID_LENGTH) {
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
		bytePasswd = AuthenticationServiceBase.toHexByte(plainTxtUserPassword,0,plainTxtUserPassword.length());
		algorithm.update(bytePasswd);
		byte[] encryptVal = algorithm.digest();
		String hexString = ID_PATTERN_NEW_SCHEME + org.apache.derby.iapi.util.StringUtil.toHexString(encryptVal,0,encryptVal.length);
		return (hexString);

	}
	/**
  
	   Convert a string into a byte array in hex format.
	   <BR>
	   For each character (b) two bytes are generated, the first byte 
	   represents the high nibble (4 bits) in hexidecimal (<code>b & 0xf0</code>),
           the second byte 
	   represents the low nibble (<code>b & 0x0f</code>).
	   <BR>
	   The character at <code>str.charAt(0)</code> is represented by the first two bytes 
	   in the returned String.

	   @param	str string 
	   @param	offset	starting character (zero based) to convert.
	   @param	length	number of characters to convert.

	   @return the byte[]  (with hexidecimal format) form of the string (str) 
	*/
	public static byte[] toHexByte(String str, int offset, int length)
	{
  	    byte[] data = new byte[(length - offset) * 2];
	    int end = offset+length;

            for (int i = offset; i < end; i++)
 	    {
	        char ch = str.charAt(i);
		int high_nibble = (ch & 0xf0) >>> 4;
		int low_nibble = (ch & 0x0f);
		data[i] = (byte)high_nibble;
		data[i+1] = (byte)low_nibble;
	    }
	    return data;
	}
}
