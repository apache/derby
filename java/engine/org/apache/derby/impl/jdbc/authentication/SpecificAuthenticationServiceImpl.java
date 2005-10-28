/*

   Derby - Class org.apache.derby.impl.jdbc.authentication.SpecificAuthenticationServiceImpl

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

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.authentication.UserAuthenticator;

import org.apache.derby.iapi.services.property.PropertyUtil;

import java.util.Properties;

/**
 * This authentication service is a specific/user defined User authentication
 * level support.
 * <p>
 * It calls the specific User authentication scheme defined by the user/
 * administrator.
 *
 * @author Francois
 */
public class SpecificAuthenticationServiceImpl
	extends AuthenticationServiceBase {

	private String specificAuthenticationScheme;

	//
	// ModuleControl implementation (overriden)
	//

	/**
	 *  Check if we should activate this authentication service.
	 */
	public boolean canSupport(Properties properties) {

		//
		// we check 2 things:
		// - if derby.connection.requireAuthentication system
		//   property is set to true.
		// - if derby.authentication.provider is set and is not equal
		//	 to LDAP or BUILTIN.
		//
		// and in that case we are the authentication service that should
		// be run.
		//
		if (!requireAuthentication(properties))
			return false;

		specificAuthenticationScheme = PropertyUtil.getPropertyFromSet(
					properties,
					org.apache.derby.iapi.reference.Property.AUTHENTICATION_PROVIDER_PARAMETER);
		if (
			 ((specificAuthenticationScheme != null) &&
			  (specificAuthenticationScheme.length() != 0) &&

			  (!((StringUtil.SQLEqualsIgnoreCase(specificAuthenticationScheme,
					  org.apache.derby.iapi.reference.Property.AUTHENTICATION_PROVIDER_BUILTIN)) ||
			  (specificAuthenticationScheme.equalsIgnoreCase(
					  org.apache.derby.iapi.reference.Property.AUTHENTICATION_PROVIDER_LDAP))  ))))
			return true;
		else
			return false;
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

		// We must retrieve and load the authentication scheme that we were
		// told to. The class loader will report an exception if it could not
		// find the class in the classpath.
		//
		// We must then make sure that the ImplementationScheme loaded,
		// implements the published UserAuthenticator interface we
		// provide.
		//

		Throwable t;
		try {

			Class sasClass = Class.forName(specificAuthenticationScheme);
			if (!UserAuthenticator.class.isAssignableFrom(sasClass)) {
				throw StandardException.newException(SQLState.AUTHENTICATION_NOT_IMPLEMENTED,
					specificAuthenticationScheme, "org.apache.derby.authentication.UserAuthenticator");
			}

			UserAuthenticator aScheme = (UserAuthenticator) sasClass.newInstance();

			// Set ourselves as being ready and loading the proper
			// authentication scheme for this service
			//
			this.setAuthenticationService(aScheme);

			return;

		} catch (ClassNotFoundException cnfe) {
			t = cnfe;
		} catch (InstantiationException ie) {
			t = ie;
		} catch (IllegalAccessException iae) {
			t = iae;
		}
		throw StandardException.newException(SQLState.AUTHENTICATION_SCHEME_ERROR, t,
					specificAuthenticationScheme);
	}
}
