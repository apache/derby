/*

   Derby - Class org.apache.derby.impl.tools.ij.ijStatementResult

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

/**
 * This is an impl for a statement execution; the result
 * is either an update count or result set depending
 * on what was executed.
 *
 * @author ames
 */
class ijStatementResult extends ijResultImpl {

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
