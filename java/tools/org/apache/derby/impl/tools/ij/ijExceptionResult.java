/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;

import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * This is an impl for just returning errors from
 * JDBC statements. Used by Async to capture its result
 * for WaitFor.
 *
 * @author ames
 */
class ijExceptionResult extends ijResultImpl {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	SQLException except;

	ijExceptionResult(SQLException e) {
		except = e;
	}

	public boolean isException() { return true; }
	public SQLException getException() { return except; }

	public SQLWarning getSQLWarnings() { return null; }
	public void clearSQLWarnings() { }
}
