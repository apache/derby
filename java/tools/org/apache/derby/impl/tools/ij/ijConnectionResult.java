/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * @author ames
 */
class ijConnectionResult extends ijResultImpl {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	Connection conn;

	ijConnectionResult(Connection c) {
		conn = c;
	}

	public boolean isConnection() { return true; }

	public Connection getConnection() { return conn; }

	public SQLWarning getSQLWarnings() throws SQLException { return conn.getWarnings(); }
	public void clearSQLWarnings() throws SQLException { conn.clearWarnings(); }
}
