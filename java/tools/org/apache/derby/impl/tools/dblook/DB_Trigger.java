/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_Trigger

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

public class DB_Trigger {

	/* ************************************************
	 * Generate the DDL for all triggers in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @return The DDL for the triggers has been written
	 *  to output via Logs.java.
	 ****/

	public static void doTriggers (Connection conn)
		throws SQLException
	{

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT TRIGGERNAME, SCHEMAID, " +
			"EVENT, FIRINGTIME, TYPE, TABLEID, REFERENCEDCOLUMNS, " + 
			"TRIGGERDEFINITION, REFERENCINGOLD, REFERENCINGNEW, OLDREFERENCINGNAME, " +
			"NEWREFERENCINGNAME FROM SYS.SYSTRIGGERS WHERE STATE != 'D'");

		boolean firstTime = true;
		while (rs.next()) {

			String trigName = dblook.addQuotes(
				dblook.expandDoubleQuotes(rs.getString(1)));
			String trigSchema = dblook.lookupSchemaId(rs.getString(2));

			if (dblook.isIgnorableSchema(trigSchema))
				continue;

			trigName = trigSchema + "." + trigName;
			String tableName = dblook.lookupTableId(rs.getString(6));

			// We'll write the DDL for this trigger if either 1) it is on
			// a table in the user-specified list, OR 2) the trigger text
			// contains a reference to a table in the user-specified list.

			if (!dblook.stringContainsTargetTable(rs.getString(8)) &&
				(dblook.isExcludedTable(tableName)))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("CSLOOK_TriggersHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			String createTrigString = createTrigger(trigName,
				tableName, rs);

			Logs.writeToNewDDL(createTrigString);
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		rs.close();
		stmt.close();

	}

	/* ************************************************
	 * Generate DDL for a specific trigger.
	 * @param trigName Name of the trigger.
	 * @param tableName Name of the table on which the trigger
	 *  fires.
	 * @param aTrig Information about the trigger.
	 * @return The DDL for the current trigger is returned
	 *  as a String.
	 ****/

	private static String createTrigger(String trigName, String tableName,
		ResultSet aTrig) throws SQLException
	{

		StringBuffer sb = new StringBuffer ("CREATE TRIGGER ");
		sb.append(trigName);

		// Firing time.
		if (aTrig.getString(4).charAt(0) == 'A')
			sb.append(" AFTER ");
		else
			sb.append(" NO CASCADE BEFORE ");

		// Event.
		switch (aTrig.getString(3).charAt(0)) {
			case 'I':	sb.append("INSERT");
						break;
			case 'D':	sb.append("DELETE");
						break;
			case 'U':	sb.append("UPDATE");
						String updateCols = aTrig.getString(7);
						if (!aTrig.wasNull()) {
							sb.append(" OF ");
							sb.append(dblook.getColumnListFromDescription(
								aTrig.getString(6), updateCols));
						}
						break;
			default:	// shouldn't happen.
						Logs.debug("INTERNAL ERROR: unexpected trigger event: " + 
							aTrig.getString(3), (String)null);
						break;
		}

		// On table...
		sb.append(" ON ");
		sb.append(tableName);

		// Referencing...
		char trigType = aTrig.getString(5).charAt(0);
		String oldReferencing = aTrig.getString(11);
		String newReferencing = aTrig.getString(12);
		if ((oldReferencing != null) || (newReferencing != null)) {
			sb.append(" REFERENCING");
			if (aTrig.getBoolean(9)) {
				sb.append(" OLD");
				if (trigType == 'S')
				// Statement triggers work on tables.
					sb.append("_TABLE AS ");
				else
				// don't include "ROW" keyword (DB2 doesn't).
					sb.append(" AS ");
				sb.append(oldReferencing);
			}
			if (aTrig.getBoolean(10)) {
				sb.append(" NEW");
				if (trigType == 'S')
				// Statement triggers work on tables.
					sb.append("_TABLE AS ");
				else
				// don't include "ROW" keyword (DB2 doesn't).
					sb.append(" AS ");
				sb.append(newReferencing);
			}
		}

		// Trigger type (row/statement).
		sb.append(" FOR EACH ");
		if (trigType == 'S')
			sb.append("STATEMENT ");
		else
			sb.append("ROW ");

		// DB2 requires the following keywords in order to work.
		sb.append("MODE DB2SQL ");

		// Finally, the trigger action.
		sb.append(dblook.removeNewlines(aTrig.getString(8)));
		return sb.toString();

	}

}
