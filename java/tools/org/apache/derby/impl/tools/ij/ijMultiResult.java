/*

   Derby - Class org.apache.derby.impl.tools.ij.ijMultiResult

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
