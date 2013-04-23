/*

   Derby - Class org.apache.derby.impl.tools.ij.ijResultSetResult

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

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

import org.apache.derby.iapi.tools.ToolUtils;

/**
 * This impl is intended to be used with a resultset,
 * where the execution of the statement is already complete.
 */
public class ijResultSetResult extends ijResultImpl {

	private ResultSet resultSet;
	private Statement statement;

	private int[]     displayColumns = null;
	private int[]     columnWidths = null;

	/**
	 * Create a ijResultImpl that represents a result set.
	 */
	public ijResultSetResult(ResultSet r) throws SQLException {
		resultSet = r;
		statement = resultSet.getStatement();
	}

	/**
	 * Create a ijResultImpl that represents a result set, only
	 * displaying a subset of the columns, using specified column widths.
	 * 
	 * @param r The result set to display
	 * @param display Which column numbers to display, or null to display
	 *                all columns.
	 * @param widths  The widths of the columns specified in 'display', or
	 *                null to display using default column sizes.
	 */
	public ijResultSetResult(ResultSet r, int[] display,
							 int[] widths) throws SQLException {
		resultSet = r;
		statement = resultSet.getStatement();

		displayColumns = ToolUtils.copy( display );
		columnWidths   = ToolUtils.copy( widths );
	}

	public boolean isResultSet() throws SQLException { return statement==null || statement.getUpdateCount() == -1; }

	public ResultSet getResultSet() throws SQLException { return resultSet; }

	public void closeStatement() throws SQLException { if(statement!=null) statement.close(); else resultSet.close(); }

	public int[] getColumnDisplayList() { return ToolUtils.copy( displayColumns ); }
	public int[] getColumnWidthList() { return ToolUtils.copy( columnWidths ); }

	public SQLWarning getSQLWarnings() throws SQLException { return resultSet.getWarnings(); }
	public void clearSQLWarnings() throws SQLException { resultSet.clearWarnings(); }
}
