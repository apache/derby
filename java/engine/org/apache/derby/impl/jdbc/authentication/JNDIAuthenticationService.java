/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.jdbc.authentication
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.jdbc.authentication;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.authentication.UserAuthenticator;

import org.apache.derby.iapi.util.StringUtil;

import java.util.Properties;

/**
 * This is the JNDI Authentication Service base class.
 * <p>
 * It instantiates the JNDI authentication scheme defined by the user/
 * administrator. Cloudscape supports LDAP JNDI providers.
 * <p>
 * The user can configure its own JNDI provider by setting the
 * system or database property derby.authentication.provider .
 *
 * @author Francois
 */

public class JNDIAuthenticationService
	extends AuthenticationServiceBase {

	private String authenticationProvider;

	//
	// constructor
	//

	// call the super
	public JNDIAuthenticationService() {
		super();
	}

	//
	// ModuleControl implementation (overriden)
	//

	/**
	 *  Check if we should activate the JNDI authentication service.
	 */
	public boolean canSupport(Properties properties) {

		if (!requireAuthentication(properties))
			return false;

		//
		// we check 2 things:
		//
		// - if derby.connection.requireAuthentication system
		//   property is set to true.
		// - if derby.authentication.provider is set to one
		// of the JNDI scheme we support (i.e. LDAP).
		//

		authenticationProvider = PropertyUtil.getPropertyFromSet(
					properties,
						org.apache.derby.iapi.reference.Property.AUTHENTICATION_PROVIDER_PARAMETER);

		 if ( (authenticationProvider != null) &&
			   (StringUtil.SQLEqualsIgnoreCase(authenticationProvider,
				  	org.apache.derby.iapi.reference.Property.AUTHENTICATION_PROVIDER_LDAP)))
			return true;

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
		// told to.

		// Set ourselves as being ready and loading the proper
		// authentication scheme for this service
		UserAuthenticator aJNDIAuthscheme;

		// we're dealing with LDAP
		aJNDIAuthscheme = new LDAPAuthenticationSchemeImpl(this, properties);	
		this.setAuthenticationService(aJNDIAuthscheme);
	}
}
