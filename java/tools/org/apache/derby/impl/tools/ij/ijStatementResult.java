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
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * This is an impl for a statement execution; the result
 * is either an update count or result set depending
 * on what was executed.
 *
 * @author ames
 */
class ijStatementResult extends ijResultImpl {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	Statement statement;
	boolean closeWhenDone;

	ijStatementResult(Statement s, boolean c) {
		statement = s;
		closeWhenDone = c;
	}

	public boolean isStatement() { return true; }
	public boolean isResultSet() throws SQLException { return statement.getUpdateCount() == -1; }
	public boolean isUpdateCount() throws SQLException { return statement.getUpdateCount() >= 0; }

	public Statement getStatement() { return statement; }
	public int getUpdateCount() throws SQLException { return statement.getUpdateCount(); }
	public ResultSet getResultSet() throws SQLException { return statement.getResultSet(); }

	public void closeStatement() throws SQLException { if (closeWhenDone) statement.close(); }

	public SQLWarning getSQLWarnings() throws SQLException { return statement.getWarnings(); }
	public void clearSQLWarnings() throws SQLException { statement.clearWarnings(); }
}
