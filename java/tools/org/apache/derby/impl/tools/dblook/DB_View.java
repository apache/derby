/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_View

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

public class DB_View {

	/* ************************************************
	 * Generate the DDL for all views in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @return The DDL for the views has been written
	 *  to output via Logs.java.
	 ****/

	public static void doViews(Connection conn)
		throws SQLException {

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT V.VIEWDEFINITION, " +
			"T.TABLENAME, T.SCHEMAID, V.COMPILATIONSCHEMAID FROM SYS.SYSVIEWS V, " +
			"SYS.SYSTABLES T WHERE T.TABLEID = V.TABLEID");

		boolean firstTime = true;
		while (rs.next()) {

			String viewSchema = dblook.lookupSchemaId(rs.getString(3));
			if (dblook.isIgnorableSchema(viewSchema))
				continue;

			if (!dblook.stringContainsTargetTable(rs.getString(1)))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("CSLOOK_ViewsHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			// We are using the exact text that was entered by the user,
			// which means the view name that is given might not include
			// the schema in which the view was created.  So, we change
			// our schema to be the one in which the view was created
			// before we execute the create statement.
			Logs.writeToNewDDL("SET SCHEMA ");
			Logs.writeToNewDDL(dblook.lookupSchemaId(rs.getString(4)));
			Logs.writeStmtEndToNewDDL();

			// Now, go ahead and create the view.
			Logs.writeToNewDDL(dblook.removeNewlines(rs.getString(1)));
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		// Set schema back to default ("APP").
		if (!firstTime) {
			Logs.reportMessage("CSLOOK_DefaultSchema");
			Logs.writeToNewDDL("SET SCHEMA \"APP\"");
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
		}

		rs.close();
		stmt.close();
		return;

	}

}
