/*

   Derby - Class org.apache.derby.impl.jdbc.authentication.BasicAuthenticationServiceImpl

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

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.authentication.UserAuthenticator;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.util.StringUtil;

import java.util.Properties;
// security imports - for SHA-1 digest
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.Serializable;
import java.util.Dictionary;

/**
 * This authentication service is the basic Cloudscape User authentication
 * level support.
 *
 * It is activated upon setting derby.authentication.provider database
 * or system property to 'BUILTIN'.
 * <p>
 * It instantiates & calls the basic User authentication scheme at runtime.
 * <p>
 * In 2.0, users can now be defined as database properties.
 * If derby.database.propertiesOnly is set to true, then in this
 * case, only users defined as database properties for the current database
 * will be considered.
 *
 * @author Francois
 */
public final class BasicAuthenticationServiceImpl
	extends AuthenticationServiceBase implements UserAuthenticator {

	//
	// ModuleControl implementation (overriden)
	//

	/**
	 *  Check if we should activate this authentication service.
	 */
	public boolean canSupport(Properties properties) {

		if (!requireAuthentication(properties))
			return false;

		//
		// We check 2 System/Database properties:
		//
		//
		// - if derby.authentication.provider is set to 'BUILTIN'.
		//
		// and in that case we are the authentication service that should
		// be run.
		//

		String authenticationProvider = PropertyUtil.getPropertyFromSet(
					properties,
					org.apache.derby.iapi.reference.Property.AUTHENTICATION_PROVIDER_PARAMETER);

		if ( (authenticationProvider != null) &&
			 (authenticationProvider.length() != 0) &&
			 (!(StringUtil.SQLEqualsIgnoreCase(authenticationProvider,
				  org.apache.derby.iapi.reference.Property.AUTHENTICATION_PROVIDER_BUILTIN))))
			return false;
		else
			return true;	// Yep, we're on!
	}

	/**
	 * @see org.apache.derby.iapi.services.monitor.ModuleControl#boot
	 * @exception StandardException upon failure to load/boot the expected
	 * authentication service.
	 */
	public void boot(boolean create, Properties properties)
	  throws StandardException {

		// We need authentication
		// setAuthentication(true);

		// we call the super in case there is anything to get initialized.
		super.boot(create, properties);

		// Initialize the MessageDigest class engine here
		// (we don't need to do that ideally, but there is some
		// overhead the first time it is instantiated.
		// SHA-1 is expected in jdk 1.1x and jdk1.2
		// This is a standard name: check,
		// http://java.sun.com/products/jdk/1.{1,2}
		//					/docs/guide/security/CryptoSpec.html#AppA 
		try {
			MessageDigest digestAlgorithm = MessageDigest.getInstance("SHA-1");
			digestAlgorithm.reset();

		} catch (NoSuchAlgorithmException nsae) {
			throw Monitor.exceptionStartingModule(nsae);
		}

		// Set ourselves as being ready and loading the proper
		// authentication scheme for this service
		//
		this.setAuthenticationService(this);
	}

	/*
	** UserAuthenticator methods.
	*/

	/**
	 * Authenticate the passed-in user's credentials.
	 *
	 * @param userName		The user's name used to connect to JBMS system
	 * @param userPassword	The user's password used to connect to JBMS system
	 * @param databaseName	The database which the user wants to connect to.
	 * @param info			Additional jdbc connection info.
	 */
	public boolean	authenticateUser(String userName,
								 String userPassword,
								 String databaseName,
								 Properties info
									)
	{
		// let's check if the user has been defined as a valid user of the
		// JBMS system.
		// We expect to find and match a System property corresponding to the
		// credentials passed-in.
		//
		if (userName == null)
			// We don't tolerate 'guest' user for now.
			return false;

		//
		// Check if user has been defined at the database or/and
		// system level. The user (administrator) can configure it the
		// way he/she wants (as well as forcing users properties to
		// be retrieved at the datbase level only).
		//
		String definedUserPassword = null, passedUserPassword = null;

		String userNameProperty = org.apache.derby.iapi.reference.Property.USER_PROPERTY_PREFIX.concat(userName);

		// check if user defined at the database level
		definedUserPassword = getDatabaseProperty(userNameProperty);

		if (definedUserPassword != null) {
			// encrypt passed-in password
			passedUserPassword = encryptPassword(userPassword);

		} else {

			// check if user defined at the system level
			definedUserPassword = getSystemProperty(userNameProperty);
			passedUserPassword = userPassword;
		}

		if (definedUserPassword == null)
			// no such user found
			return false;

		// check if the passwords match
		if (!definedUserPassword.equals(passedUserPassword))
			return false;

		// NOTE: We do not look at the passed-in database name value as
		// we rely on the authorization service that was put in
		// in 2.0 . (if a database name was passed-in)

		// We do have a valid user
		return true;
	}

	/*
	** Encryption related methods.
	*/
}
