/*

   Derby - Class org.apache.derby.iapi.jdbc.BrokeredStatementControl

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.jdbc;

import java.sql.*;

/**
	Provides control over a BrokeredStatement, BrokeredPreparedStatement or BrokeredCallableStatement
*/
public interface BrokeredStatementControl
{
	/**
		Can cursors be held across commits.
	*/
	public void checkHoldCursors(int holdability) throws SQLException;

	/**
		Return the real JDBC statement for the brokered statement
		when this is controlling a Statement.
	*/
	public Statement	getRealStatement() throws SQLException;

	/**
		Return the real JDBC PreparedStatement for the brokered statement
		when this is controlling a PreparedStatement.
	*/
	public PreparedStatement	getRealPreparedStatement() throws SQLException;


	/**
		Return the real JDBC CallableStatement for the brokered statement
		when this is controlling a CallableStatement.
	*/
	public CallableStatement	getRealCallableStatement() throws SQLException;

	/**
		Optionally wrap a returned ResultSet in another ResultSet.
	*/
	public ResultSet	wrapResultSet(ResultSet rs);
}
