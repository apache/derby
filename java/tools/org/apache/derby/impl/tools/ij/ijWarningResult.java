/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * This is an impl for just returning warnings from
 * JDBC objects we don't want the caller to touch.
 * They are already cleared from the underlying
 * objects, doing clearSQLWarnings here is redundant.
 *
 * @author ames
 */
class ijWarningResult extends ijResultImpl {

	SQLWarning warn;

	ijWarningResult(SQLWarning w) {
		warn = w;
	}

	public SQLWarning getSQLWarnings() { return warn; }
	public void clearSQLWarnings() { warn = null; }
}
