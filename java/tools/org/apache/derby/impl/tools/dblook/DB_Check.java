/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_Check

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
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.derby.tools.dblook;

public class DB_Check {

	/* ************************************************
	 * Generate the DDL for all checks in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @return The DDL for the indexes has been written
	 *  to output via Logs.java.
	 ****/

	public static void doChecks(Connection conn)
		throws SQLException
	{

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT CS.CONSTRAINTNAME, " +
			"CS.TABLEID, CS.SCHEMAID, CK.CHECKDEFINITION FROM SYS.SYSCONSTRAINTS CS, " +
			"SYS.SYSCHECKS CK WHERE CS.CONSTRAINTID = " +
			"CK.CONSTRAINTID AND CS.STATE != 'D' ORDER BY CS.TABLEID");

		boolean firstTime = true;
		while (rs.next()) {

			String tableId = rs.getString(2);
			String tableName = dblook.lookupTableId(tableId);
			if (dblook.isExcludedTable(tableName))
			// table isn't specified in user-given list; skip it.
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("CSLOOK_ChecksHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			StringBuffer chkString = createCheckString(tableName, rs);
			Logs.writeToNewDDL(chkString.toString());
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		stmt.close();
		rs.close();
		return;

	}

	/* ************************************************
	 * Generate DDL for a specific check.
	 * @param tableName Name of the table on which the check
	 *   exists.
	 * @param aCheck Information about the check in question.
	 * @return The DDL for the specified check has been
	 *  generated returned as a StringBuffer.
	 ****/

	private static StringBuffer createCheckString (String tableName,
		ResultSet aCheck) throws SQLException
	{

		StringBuffer sb = new StringBuffer ("ALTER TABLE ");
		sb.append(tableName);
		sb.append(" ADD");

		String constraintName = dblook.addQuotes(
			dblook.expandDoubleQuotes(aCheck.getString(1)));
		sb.append(" CONSTRAINT ");
		sb.append(constraintName);
		sb.append(" CHECK ");
		sb.append(dblook.removeNewlines(aCheck.getString(4)));

		return sb;

	}

}
