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
import java.util.Vector;

/**
 * This is an impl for a statement execution; the result
 * is either an update count or result set depending
 * on what was executed.
 *
 * @author ames
 */
class ijMultiResult extends ijResultImpl {

	Vector results = new Vector();
	Statement statement;
	ResultSet rs;
	boolean closeWhenDone;

	ijMultiResult(Statement s, ResultSet rs, boolean c) {
		statement = s;
		this.rs = rs;
		closeWhenDone = c;
	}

	public void addStatementResult(Statement s) throws SQLException {
System.out.println("adding statement "+results.size()+1);
		if (s.getUpdateCount() >=0)
			results.addElement(new Integer(s.getUpdateCount()));
		else
			results.addElement(s.getResultSet());
	}

	public boolean isMulti() { return true; }

	public Statement getStatement() { return statement; }
	public ResultSet getResultSet() { return rs; }
	public void closeStatement() throws SQLException { if (closeWhenDone) statement.close(); }

	public SQLWarning getSQLWarnings() { return null; }
	public void clearSQLWarnings() { }
}
