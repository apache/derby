/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_Table

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.tools.dblook;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import org.apache.derby.tools.dblook;

public class DB_Table {

	// Prepared statements use throughout the DDL
	// generation process.
	private static PreparedStatement getColumnInfoStmt;
	private static PreparedStatement getColumnTypeStmt;
	private static PreparedStatement getAutoIncStmt;

	/* ************************************************
	 * Generate the DDL for all user tables in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @param tableIdToNameMap Mapping of table ids to table
	 *  names, for quicker reference.
	 * @return The DDL for the tables has been written
	 *  to output via Logs.java.
	 ****/

	public static void doTables(Connection conn, HashMap tableIdToNameMap)
		throws SQLException
	{

		// Prepare some statements for general use by this class.

		getColumnInfoStmt =
			conn.prepareStatement("SELECT C.COLUMNNAME, C.REFERENCEID, " +
			"C.COLUMNNUMBER FROM SYS.SYSCOLUMNS C, SYS.SYSTABLES T WHERE T.TABLEID = ? " +
			"AND T.TABLEID = C.REFERENCEID ORDER BY C.COLUMNNUMBER");

		getColumnTypeStmt = 
			conn.prepareStatement("SELECT COLUMNDATATYPE, COLUMNDEFAULT FROM SYS.SYSCOLUMNS " +
			"WHERE REFERENCEID = ? AND COLUMNNAME = ?");

		getAutoIncStmt = 
			conn.prepareStatement("SELECT AUTOINCREMENTSTART, " +
//IC see: https://issues.apache.org/jira/browse/DERBY-308
			"AUTOINCREMENTINC, COLUMNNAME, REFERENCEID, COLUMNDEFAULT FROM SYS.SYSCOLUMNS " +
			"WHERE COLUMNNAME = ? AND REFERENCEID = ?");

		// Walk through list of tables and generate the DDL for
		// each one.

		boolean firstTime = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-5021
		Set entries = tableIdToNameMap.entrySet();
		for (Iterator itr = entries.iterator(); itr.hasNext(); ) {

            Map.Entry entry = (Map.Entry)itr.next();
			String tableId = (String)entry.getKey();
			String tableName = (String)entry.getValue();
			if (dblook.isExcludedTable(tableName))
			// table isn't included in user-given list; skip it.
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_TablesHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			Logs.writeToNewDDL("CREATE TABLE " + tableName + " (");

			// Get column list, and write DDL for each column.
			boolean firstCol = true;
			getColumnInfoStmt.setString(1, tableId);
			ResultSet columnRS = getColumnInfoStmt.executeQuery();
			while (columnRS.next()) {
				String colName = dblook.addQuotes(columnRS.getString(1));
				String createColString = createColumn(colName, columnRS.getString(2),
					columnRS.getInt(3));
				if (!firstCol)
					createColString = ", " + createColString;

				Logs.writeToNewDDL(createColString);
				firstCol = false;
			}

			columnRS.close();
			Logs.writeToNewDDL(")");
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		} // outer while.

		getColumnInfoStmt.close();
		getColumnTypeStmt.close();
		getAutoIncStmt.close();

	}

	/* ************************************************
	 * Generate the DDL for a specific column of the
	 * the table corresponding to the received tableId.
	 * @param colName the name of the column to generate.
	 * @param tableId Which table the column belongs to.
	 * @param colNum the number of the column to generate (1 =>
	 *  1st column, 2 => 2nd column, etc)
	 * @return The generated DDL, as a string.
	 ****/

	private static String createColumn(String colName, String tableId,
		int colNum) throws SQLException
	{

		getColumnTypeStmt.setString(1, tableId);
		getColumnTypeStmt.setString(2, dblook.stripQuotes(colName));

		ResultSet rs = getColumnTypeStmt.executeQuery();
		StringBuffer colDef = new StringBuffer();
		if (rs.next()) {

			colDef.append(dblook.addQuotes(dblook.expandDoubleQuotes(
				dblook.stripQuotes(colName))));
			colDef.append(" ");
			colDef.append(rs.getString(1));
			if (!reinstateAutoIncrement(colName, tableId, colDef) &&
						 rs.getString(2) != null) {

                String defaultText = rs.getString(2);

                if ( defaultText.startsWith( "GENERATED ALWAYS AS" ) )
                { colDef.append( " " ); }
				else { colDef.append(" DEFAULT "); }
                
				colDef.append( defaultText );
			}
		}

		rs.close();
		return colDef.toString();

	}

	/* ************************************************
	 * Generate autoincrement DDL for a given column and write it to
	 * received StringBuffer
	 * @param colName: Name of column that is autoincrement.
	 * @param tableId: Id of table in which column exists.
	 * @param colDef: StringBuffer to which DDL will be added.
	 * @return True if autoincrement DDL has been generated.
	 ****/

	public static boolean reinstateAutoIncrement(String colName,
		String tableId, StringBuffer colDef) throws SQLException
	{

		getAutoIncStmt.setString(1, dblook.stripQuotes(colName));
		getAutoIncStmt.setString(2, tableId);
		ResultSet autoIncCols = getAutoIncStmt.executeQuery();
		if (autoIncCols.next()) {

			long start = autoIncCols.getLong(1);
			if (!autoIncCols.wasNull()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-308
				colDef.append(" GENERATED ");
				colDef.append(autoIncCols.getObject(5) == null ? 
					      "ALWAYS ":"BY DEFAULT ");
				colDef.append("AS IDENTITY (START WITH ");
				colDef.append(autoIncCols.getLong(1));
				colDef.append(", INCREMENT BY ");
				colDef.append(autoIncCols.getLong(2));
				colDef.append(")");
				return true;
			}
		}

		return false;

	}

}

