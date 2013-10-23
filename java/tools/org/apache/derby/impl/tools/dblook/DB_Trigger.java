/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_Trigger

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
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.tools.dblook;

public class DB_Trigger {

    // Column name constants for SYS.SYSTRIGGERS
    private static final String TRIGGERNAME = "TRIGGERNAME";
    private static final String SCHEMAID = "SCHEMAID";
    private static final String EVENT = "EVENT";
    private static final String FIRINGTIME = "FIRINGTIME";
    private static final String TYPE = "TYPE";
    private static final String TABLEID = "TABLEID";
    private static final String REFERENCEDCOLUMNS = "REFERENCEDCOLUMNS";
    private static final String TRIGGERDEFINITION = "TRIGGERDEFINITION";
    private static final String REFERENCINGOLD = "REFERENCINGOLD";
    private static final String REFERENCINGNEW = "REFERENCINGNEW";
    private static final String OLDREFERENCINGNAME = "OLDREFERENCINGNAME";
    private static final String NEWREFERENCINGNAME = "NEWREFERENCINGNAME";
    private static final String WHENCLAUSETEXT = "WHENCLAUSETEXT";

    /** ************************************************
	 * Generate the DDL for all triggers in a given
	 * database.
	 * @param conn Connection to the source database.
     * @param supportsWhenClause Tells whether the database supports the
     *   trigger WHEN clause.
	 ****/

    public static void doTriggers(Connection conn, boolean supportsWhenClause)
		throws SQLException
	{

		Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
                "SELECT * FROM SYS.SYSTRIGGERS WHERE STATE != 'D'");

		boolean firstTime = true;
		while (rs.next()) {

			String trigName = dblook.addQuotes(
                dblook.expandDoubleQuotes(rs.getString(TRIGGERNAME)));
            String trigSchema = dblook.lookupSchemaId(rs.getString(SCHEMAID));

			if (dblook.isIgnorableSchema(trigSchema))
				continue;

			trigName = trigSchema + "." + trigName;
            String tableName = dblook.lookupTableId(rs.getString(TABLEID));

            // Get the WHEN clause text, if there is a WHEN clause. The
            // WHENCLAUSETEXT column is only present if the data dictionary
            // version is 10.11 or higher (DERBY-534).
            String whenClause =
                    supportsWhenClause ? rs.getString(WHENCLAUSETEXT) : null;

			// We'll write the DDL for this trigger if either 1) it is on
			// a table in the user-specified list, OR 2) the trigger text
			// contains a reference to a table in the user-specified list.

            if (!dblook.stringContainsTargetTable(
                    rs.getString(TRIGGERDEFINITION)) &&
                !dblook.stringContainsTargetTable(whenClause) &&
				(dblook.isExcludedTable(tableName)))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_TriggersHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			String createTrigString = createTrigger(trigName,
                tableName, whenClause, rs);

			Logs.writeToNewDDL(createTrigString);
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		rs.close();
		stmt.close();

	}

    /** ************************************************
	 * Generate DDL for a specific trigger.
	 * @param trigName Name of the trigger.
	 * @param tableName Name of the table on which the trigger
	 *  fires.
     * @param whenClause The WHEN clause text (possibly {@code null}).
	 * @param aTrig Information about the trigger.
	 * @return The DDL for the current trigger is returned
	 *  as a String.
	 ****/

	private static String createTrigger(String trigName, String tableName,
        String whenClause, ResultSet aTrig) throws SQLException
	{

        StringBuilder sb = new StringBuilder("CREATE TRIGGER ");
		sb.append(trigName);

		// Firing time.
        if (aTrig.getString(FIRINGTIME).charAt(0) == 'A') {
			sb.append(" AFTER ");
        } else {
			sb.append(" NO CASCADE BEFORE ");
        }

		// Event.
        String event = aTrig.getString(EVENT);
        switch (event.charAt(0)) {
			case 'I':	sb.append("INSERT");
						break;
			case 'D':	sb.append("DELETE");
						break;
			case 'U':	sb.append("UPDATE");
                        String updateCols = aTrig.getString(REFERENCEDCOLUMNS);
						//DERBY-5839 dblook run on toursdb fails on triggers
						//	with java.lang.StringIndexOutOfBoundsException in
						//	dblook.log
						//We document that SYSTRIGGERS.REFERENCEDCOLUMNS is not
						// part of the public API and hence that allows Derby 
						// to change underneath the behavior of the column.
						// Prior to 10.9, this column only had information
						// about columns referenced by UPDATE trigger. But,
						// with 10.9, we use this column to also hold 
						// information about the trigger columns being used 
						// inside trigger action plan. This enables Derby to 
						// read only necessary columns from trigger table. But
						// because of this change, it is not enough in dblook
						// to check if SYSTRIGGERS.REFERENCEDCOLUMNS.wasNull. 
						// We need to also check if the string representation 
						// of that column is "NULL". Making this change fixes
						// DERBY-5839
						if (!aTrig.wasNull() && !updateCols.equals("NULL")) {
							sb.append(" OF ");
							sb.append(dblook.getColumnListFromDescription(
                                aTrig.getString(TABLEID), updateCols));
						}
						break;
			default:	// shouldn't happen.
						Logs.debug("INTERNAL ERROR: unexpected trigger event: " + 
                                   event, (String)null);
						break;
		}

		// On table...
		sb.append(" ON ");
		sb.append(tableName);

		// Referencing...
        char trigType = aTrig.getString(TYPE).charAt(0);
        String oldReferencing = aTrig.getString(OLDREFERENCINGNAME);
        String newReferencing = aTrig.getString(NEWREFERENCINGNAME);
		if ((oldReferencing != null) || (newReferencing != null)) {
			sb.append(" REFERENCING");
            if (aTrig.getBoolean(REFERENCINGOLD)) {
				sb.append(" OLD");
				if (trigType == 'S')
				// Statement triggers work on tables.
					sb.append("_TABLE AS ");
				else
				// don't include "ROW" keyword (DB2 doesn't).
					sb.append(" AS ");
				sb.append(oldReferencing);
			}
            if (aTrig.getBoolean(REFERENCINGNEW)) {
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

        // Finally, the trigger action, which consists of an optional WHEN
        // clause and the triggered SQL statement.
        if (whenClause != null) {
            sb.append("WHEN (");
            sb.append(dblook.removeNewlines(whenClause));
            sb.append(") ");
        }
        sb.append(dblook.removeNewlines(aTrig.getString(TRIGGERDEFINITION)));

		return sb.toString();

	}

}
