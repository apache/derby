/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_Index

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.tools.dblook;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.derby.tools.dblook;

public class DB_Index {

	/* ************************************************
	 * Generate the DDL for all indexes in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @return The DDL for the indexes has been written
	 *  to output via Logs.java.
	 ****/

	public static void doIndexes(Connection conn)
		throws SQLException
	{

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT TABLEID, CONGLOMERATENAME, " +
			"DESCRIPTOR, SCHEMAID, ISINDEX, ISCONSTRAINT FROM SYS.SYSCONGLOMERATES " +
			"ORDER BY TABLEID");

		boolean firstTime = true;
		while (rs.next()) {

			if (!rs.getBoolean(5) ||	// (isindex == false)
				rs.getBoolean(6))		// (isconstraint == true)
			// then skip it.
				continue;

			String tableId = rs.getString(1);
			String tableName = dblook.lookupTableId(tableId);
			if (tableName == null)
			// then tableId isn't a user table, so we can skip it.
				continue;
			else if (dblook.isExcludedTable(tableName))
			// table isn't specified in user-given list.
				continue;

			String iSchema = dblook.lookupSchemaId(rs.getString(4));
			if (dblook.isIgnorableSchema(iSchema))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("CSLOOK_IndexesHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			String iName = dblook.addQuotes(
				dblook.expandDoubleQuotes(rs.getString(2)));
			iName = iSchema + "." + iName;

			StringBuffer createIndexString = createIndex(iName, tableName,
				tableId, rs.getString(3));

			Logs.writeToNewDDL(createIndexString.toString());
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		rs.close();
		stmt.close();

	}

	/* ************************************************
	 * Generate DDL for a specific index.
	 * @param ixName Name of the index.
	 * @param tableName Name of table on which the index exists.
	 * @param tableId Id of table on which the index exists.
	 * @param ixDescribe Column list for the index.
	 * @return The DDL for the specified index, as a string
	 *  buffer.
	 ****/

	private static StringBuffer createIndex(String ixName, String tableName,
		String tableId, String ixDescribe) throws SQLException
	{

		StringBuffer sb = new StringBuffer("CREATE ");
		if (ixDescribe.indexOf("UNIQUE") != -1)
			sb.append("UNIQUE ");

		// Note: We leave the keyword "BTREE" out since it's not
		// required, and since it is not recognized by DB2.

		sb.append("INDEX ");
		sb.append(ixName);
		sb.append(" ON ");
		sb.append(tableName);
		sb.append(" (");
		sb.append(dblook.getColumnListFromDescription(tableId, ixDescribe));
		sb.append(")");
		return sb;

	}

}
