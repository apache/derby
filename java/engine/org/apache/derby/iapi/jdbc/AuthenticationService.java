/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.jdbc
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.jdbc;

import java.util.Properties;
import java.sql.SQLException;

/**
 *
 * The AuthenticationService provides a mechanism for authenticating
 * users willing to access JBMS.
 * <p>
 * There can be different and user defined authentication schemes, as long
 * the expected interface here below is implementing and registered
 * as a module when JBMS starts-up.
 * <p>
 */
public interface AuthenticationService 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	public static final String MODULE =
								"org.apache.derby.iapi.jdbc.AuthenticationService";
	/**
	 * Authenticate a User inside JBMS.
	 *
	 * @param info			Connection properties info.
	 * failure.
	 */
	public boolean authenticate(String databaseName, Properties info)
	  throws SQLException;
}
