/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_Schema

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
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.HashMap;

import org.apache.derby.tools.dblook;

public class DB_Schema {

	/* ************************************************
	 * Generate the DDL for all schemas in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @param tablesOnly true if we're only generating objects
	 *  specific to a particular table (in which case
	 *  we don't generate schemas).
	 * @return The DDL for the schemas has been written
	 *  to output via Logs.java.
	 ****/

	public static void doSchemas(Connection conn,
		boolean tablesOnly) throws SQLException
	{

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT SCHEMANAME, SCHEMAID " +
			"FROM SYS.SYSSCHEMAS");

		boolean firstTime = true;
		while (rs.next()) {

			String sName = dblook.addQuotes(
				dblook.expandDoubleQuotes(rs.getString(1)));
			if (tablesOnly || dblook.isIgnorableSchema(sName))
				continue;

			if (sName.equals("\"APP\""))
			// don't have to create this one.
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("CSLOOK_SchemasHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			Logs.writeToNewDDL("CREATE SCHEMA " + sName);
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		rs.close();
		stmt.close();

	}

}
