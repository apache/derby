/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.jdbc.authentication
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.jdbc.authentication;

import org.apache.derby.iapi.reference.MessageId;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.authentication.UserAuthenticator;

import org.apache.derby.iapi.services.property.PropertyUtil;

import java.util.Properties;

/**
 * This authentication service does not care much about authentication.
 * <p>
 * It is a quiescient authentication service that will basically satisfy
 * any authentication request, as JBMS system was not instructed to have any
 * particular authentication scheme to be loaded at boot-up time.
 *
 * @author Francois
 */
public final class NoneAuthenticationServiceImpl
	extends AuthenticationServiceBase implements UserAuthenticator {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	//
	// ModuleControl implementation (overriden)
	//

	/**
	 *  Check if we should activate this authentication service.
	 */
	public boolean canSupport(Properties properties) {

		return !requireAuthentication(properties);
	}

	/**
	 * @see org.apache.derby.iapi.services.monitor.ModuleControl#boot
	 * @exception StandardException upon failure to load/boot the expected
	 * authentication service.
	 */
	public void boot(boolean create, Properties properties) 
	  throws StandardException {

		// we call the super in case there is anything to get initialized.
 		super.boot(create, properties);

		// nothing special to be done, other than setting other than
		// setting ourselves as being ready and loading the proper
		// authentication scheme for this service
		//.
		this.setAuthenticationService(this);
	}

	/*
	** UserAuthenticator
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
		// Since this authentication service does not really provide
		// any particular authentication, therefore we satisfy the request.
		// and always authenticate successfully the user.
		//
		return true;
	}

}
