/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_Key

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

public class DB_Key {

	// Prepared statements used throughout the DDL
	// generation process.
	private static PreparedStatement getReferenceCols;
	private static boolean printedHeader;

	/* ************************************************
	 * Generate the DDL for all keys in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @return The DDL for the keys has been written
	 *  to output via Logs.java.
	 ****/

	public static void doKeys(Connection conn)
		throws SQLException
	{

		printedHeader = false;
		getReferenceCols = conn.prepareStatement("SELECT CG.TABLEID, " +
			"CG.DESCRIPTOR FROM SYS.SYSCONGLOMERATES CG, SYS.SYSKEYS K WHERE " +
			"K.CONSTRAINTID = ? AND K.CONGLOMERATEID = CG.CONGLOMERATEID");

		// Non-foreign keys, first.
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT CS.CONSTRAINTNAME, CS.TYPE, " +
			"CS.TABLEID, CS.CONSTRAINTID, CS.SCHEMAID, CG.DESCRIPTOR, CG.ISCONSTRAINT " +
			"FROM SYS.SYSCONSTRAINTS CS, SYS.SYSCONGLOMERATES CG, SYS.SYSKEYS K " +
			"WHERE CS.STATE != 'D' AND CS.CONSTRAINTID = K.CONSTRAINTID AND " +
			"CG.CONGLOMERATEID = K.CONGLOMERATEID ORDER BY CS.TABLEID");
		createKeysFrom(rs);

		// Now, foreign keys.
		rs = stmt.executeQuery("SELECT CS.CONSTRAINTNAME, CS.TYPE, CS.TABLEID, " +
			"CS.CONSTRAINTID, CS.SCHEMAID, CG.DESCRIPTOR, CG.ISCONSTRAINT, " +
			"K.DELETERULE, K.UPDATERULE, K.KEYCONSTRAINTID FROM SYS.SYSCONSTRAINTS CS, " +
			"SYS.SYSCONGLOMERATES CG, SYS.SYSFOREIGNKEYS K WHERE CS.STATE != 'D' " +
			"AND CS.CONSTRAINTID = K.CONSTRAINTID AND CG.CONGLOMERATEID = " +
			"K.CONGLOMERATEID ORDER BY CS.TABLEID");
		createKeysFrom(rs);

		getReferenceCols.close();
		stmt.close();
		rs.close();
		return;

	}

	/* ************************************************
	 * Generate the DDL for the a set of keys in the
	 * source database.
	 * @param rs Info on keys to dump; either a set of non-
	 *  foreign keys (primary and unique), or a set of
	 *  foreign keys.
	 * @return DDL for the receive set of keys has
	 *  been written to output via Logs.java.
	 ****/

	private static void createKeysFrom (ResultSet rs)
		throws SQLException
	{

		boolean firstTime = true;
		while (rs.next()) {

			if (!rs.getBoolean(7))
			// this row is NOT for a constraint, so skip it.
				continue;

			String tableId = rs.getString(3);
			String tableName = dblook.lookupTableId(tableId);
			if (dblook.isExcludedTable(tableName))
			// table isn't included in user-given list; skip it.
				continue;

			if (firstTime) {
				printHeader();
				if (rs.getString(2).equals("F"))
					Logs.reportMessage("CSLOOK_ForeignHeader");
				else
					Logs.reportMessage("CSLOOK_PrimUniqueHeader");
			}

			StringBuffer kString = createKeyString(tableId, tableName, rs);

			if (rs.getString(2).equals("F")) {
			// foreign key; we have to figure out the references info.
				kString.append(makeFKReferenceClause(rs.getString(10),
					rs.getString(8).charAt(0), rs.getString(9).charAt(0)));
			}

			Logs.writeToNewDDL(kString.toString());
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		return;

	}

	/* ************************************************
	 * Generate DDL for a specific key.
	 * @param tableId Id of table on which the key exists.
	 * @param tableName Name of table on which the key exists.
	 * @param aKey Info on the key to generate.
	 * @return DDL for the specified key is returned as
	 *  a string.
	 ****/

	private static StringBuffer createKeyString (String tableId,
		String tableName, ResultSet aKey)
		throws SQLException
	{

		StringBuffer sb = new StringBuffer ("ALTER TABLE ");
		sb.append(tableName);
		sb.append(" ADD");

		String constraintName = dblook.addQuotes(
			dblook.expandDoubleQuotes(aKey.getString(1)));
		sb.append(" CONSTRAINT ");
		sb.append(constraintName);
		sb.append(expandKeyType(aKey.getString(2).charAt(0)));

		// For keys, we need to get the column list.
		sb.append("(");
		sb.append(dblook.getColumnListFromDescription(
			tableId, aKey.getString(6)));
		sb.append(")");

		return sb;

	}

	/* ************************************************
	 * Takes a character representing a key type and
	 * returns the full type name (as it will appear in
	 * in the DDL).
	 * @param keyType Key type as a char.
	 * @return Key type as a full string.
	 ****/

	private static String expandKeyType(char keyType) {

		switch (keyType) {
			case 'P':
				return " PRIMARY KEY ";
			case 'U':
				return " UNIQUE ";
			case 'F':
				return " FOREIGN KEY ";
			default:
				// shouldn't happen.
				Logs.debug("INTERNAL ERROR: unexpected key type" +
					keyType, (String)null);
				return "";
		}

	}

	/* ************************************************
	 * Generate the DDL for a foreign key's "REFERENCES"
	 * clause.
	 * @param constraintId Id of the foreign key constraint.
	 * @param deleteChar What action to take on delete.
	 * @param updateChar What action to take on update.
	 * @return The DDL for the references clause of the
	 *  foreign key, returned as a string.
	 ****/
	
	private static String makeFKReferenceClause(String constraintId,
		char deleteChar, char updateChar)
		throws SQLException
	{

		StringBuffer refClause = new StringBuffer();

		getReferenceCols.setString(1, constraintId);
		ResultSet colsRS = getReferenceCols.executeQuery();
		colsRS.next();
		refClause.append(" REFERENCES ");
		refClause.append(dblook.lookupTableId(colsRS.getString(1)));
		refClause.append(" (");
		refClause.append(dblook.getColumnListFromDescription(
			colsRS.getString(1), colsRS.getString(2)));
		refClause.append(")");

		// On delete.
		refClause.append(" ON DELETE ");
		switch (deleteChar) {
 			case 'R':	refClause.append("NO ACTION"); break;
 			case 'S':	refClause.append("RESTRICT"); break;
 			case 'C':	refClause.append("CASCADE"); break;
 			case 'U':	refClause.append("SET NULL"); break;
			default:	// shouldn't happen.
						Logs.debug("INTERNAL ERROR: unexpected 'on-delete' action: " +
							deleteChar, (String)null);
						break;
		}

		// On update
		refClause.append(" ON UPDATE ");
		switch (updateChar) {
 			case 'R':	refClause.append("NO ACTION"); break;
 			case 'S':	refClause.append("RESTRICT"); break;
			default:	// shouldn't happen.
						Logs.debug("INTERNAL ERROR: unexpected 'on-update' action: " +
							updateChar, (String)null);
						break;
		}

		colsRS.close();
		return refClause.toString();

	}

	/* ************************************************
	 * Print a simple header to output.
	 ****/

	private static void printHeader() {

		if (printedHeader)
			return;

		Logs.reportString("----------------------------------------------");
		Logs.reportMessage("CSLOOK_KeysHeader");
		Logs.reportString("----------------------------------------------\n");
		printedHeader = true;

	}

}
