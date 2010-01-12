/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_GrantRevoke

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

import java.util.StringTokenizer;

import org.apache.derby.tools.dblook;

public class DB_GrantRevoke {

	/** ************************************************
	 * Generate Grant & Revoke statements if sqlAuthorization is on
	 * 
	 * @param conn Connection to use
	 * @param at10_6 True if the databse level is 10.6 or higher
	 */
	public static void doAuthorizations(Connection conn, boolean at10_6)
		throws SQLException {

		// First generate table privilege statements
		Statement stmt = conn.createStatement();
        ResultSet rs;

        if ( at10_6 )
        {
            // Generate udt privilege statements
            rs = stmt.executeQuery("SELECT P.GRANTEE, S.SCHEMANAME, A.ALIAS, P.PERMISSION, P.OBJECTTYPE FROM " +
                                   "SYS.SYSPERMS P, SYS.SYSALIASES A, SYS.SYSSCHEMAS S WHERE A.SCHEMAID = " +
                                   "S.SCHEMAID AND P.OBJECTID = A.ALIASID AND A.ALIASTYPE='A'");
            generateUDTPrivs(rs);
            
            // Generate sequence privilege statements
            rs = stmt.executeQuery("SELECT P.GRANTEE, S.SCHEMANAME, SEQ.SEQUENCENAME, P.PERMISSION, P.OBJECTTYPE FROM " +
                                   "SYS.SYSPERMS P, SYS.SYSSEQUENCES SEQ, SYS.SYSSCHEMAS S WHERE SEQ.SCHEMAID = " +
                                   "S.SCHEMAID AND P.OBJECTID = SEQ.SEQUENCEID");
            generateSequencePrivs(rs);
        }

        rs = stmt.executeQuery("SELECT GRANTEE, SCHEMANAME, TABLENAME, SELECTPRIV, " +
			"DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV FROM " +
			"SYS.SYSTABLEPERMS P, SYS.SYSTABLES T, SYS.SYSSCHEMAS S WHERE T.SCHEMAID = " +
			"S.SCHEMAID AND T.TABLEID = P.TABLEID");
		generateTablePrivs(rs);

		// Generate column privilege statements
		rs = stmt.executeQuery("SELECT GRANTEE, SCHEMANAME, TABLENAME, TYPE, COLUMNS FROM " +
			"SYS.SYSCOLPERMS P, SYS.SYSTABLES T, SYS.SYSSCHEMAS S WHERE T.SCHEMAID = " +
			"S.SCHEMAID AND T.TABLEID = P.TABLEID");
		generateColumnPrivs(rs, conn);

		// Generate routine privilege statements
		rs = stmt.executeQuery("SELECT GRANTEE, SCHEMANAME, ALIAS, ALIASTYPE FROM " +
			"SYS.SYSROUTINEPERMS P, SYS.SYSALIASES A, SYS.SYSSCHEMAS S WHERE A.SCHEMAID = " +
			"S.SCHEMAID AND P.ALIASID = A.ALIASID");
		generateRoutinePrivs(rs);

		rs.close();
		stmt.close();
		return;

	}

	/** ************************************************
	 * Generate table privilege statements
	 * 
	 * @param rs Result set holding required information
	 ****/
	private static void generateTablePrivs(ResultSet rs)
		throws SQLException
	{
		boolean firstTime = true;
		while (rs.next()) {

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage( "DBLOOK_TablePrivHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			String authName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(1)));
			String schemaName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(2)));
			String tableName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(3)));
			String fullName = schemaName + "." + tableName;

			if (dblook.isIgnorableSchema(schemaName))
				continue;

			Logs.writeToNewDDL(tablePrivStatement(rs, fullName, authName));
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;
		}
	}

	private static String separatorStr(boolean addSeparator)
	{
		return (addSeparator) ? ", " : "";
	}
	
	/** **************************************************
	 * Generate table privilege statement for the current row
	 *
	 * @param rs 		ResultSet holding tableperm information
	 * @param fullName	Table's qualified name
	 * @param authName	Authorization id for grant statement
	 */
	private static String tablePrivStatement(ResultSet rs, String fullName, String authName)
		throws SQLException
	{
		boolean addSeparator = false;
		StringBuffer grantStmt = new StringBuffer("GRANT ");

		if (rs.getString(4).toUpperCase().equals("Y")) 
		{
			grantStmt.append("SELECT");
			addSeparator = true;
		}

		if (rs.getString(5).toUpperCase().equals("Y"))
		{
			grantStmt.append(separatorStr(addSeparator)+ "DELETE");
			addSeparator = true;
		}

		if (rs.getString(6).toUpperCase().equals("Y"))
		{
			grantStmt.append(separatorStr(addSeparator)+ "INSERT");
			addSeparator = true;
		}

		if (rs.getString(7).toUpperCase().equals("Y"))
		{
			grantStmt.append(separatorStr(addSeparator)+ "UPDATE");
			addSeparator = true;
		}

		if (rs.getString(8).toUpperCase().equals("Y"))
		{
			grantStmt.append(separatorStr(addSeparator)+ "REFERENCES");
			addSeparator = true;
		}

		if (rs.getString(9).toUpperCase().equals("Y"))
		{
			grantStmt.append(separatorStr(addSeparator)+ "TRIGGER");
			addSeparator = true;
		}

		grantStmt.append(" ON " + fullName + " TO " + authName);

		return grantStmt.toString();
	}

	/** ************************************************
	 * Generate column privilege statements
	 * 
	 * @param rs	ResultSet holding column privilege information
	 * @param conn	Connection to use. Used to get another ResultSet
	 ****/

	private static void generateColumnPrivs(ResultSet rs, Connection conn)
		throws SQLException
	{
		boolean firstTime = true;
		while (rs.next()) {
			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage( "DBLOOK_ColumnPrivHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			String authName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(1)));
			String schemaName = dblook.expandDoubleQuotes(rs.getString(2));
			String tableName = dblook.expandDoubleQuotes(rs.getString(3));

			if (dblook.isIgnorableSchema(schemaName))
				continue;

			// Create another resultSet to get column names
			Statement stmtCols = conn.createStatement();
			String queryCols = "SELECT COLUMNNUMBER, COLUMNNAME " +
				"FROM SYS.SYSCOLUMNS C, SYS.SYSTABLES T, SYS.SYSSCHEMAS S " +
				"WHERE T.TABLEID = C.REFERENCEID and S.SCHEMAID = T.SCHEMAID "+
				"and T.TABLENAME = '"+tableName+"' AND SCHEMANAME = '"+schemaName +
				"' ORDER BY COLUMNNUMBER";

			ResultSet rsCols= stmtCols.executeQuery(queryCols);
			String fullName = dblook.addQuotes(schemaName) + "." + dblook.addQuotes(tableName);

			Logs.writeToNewDDL(columnPrivStatement(rs, fullName, authName, rsCols));
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;
		}
	}

	private static String privTypeToString(String privType)
	{
		if (privType.equals("S"))
			return "SELECT";
		else if (privType.equals("R"))
			return "REFERENCES";
		else if (privType.equals("U"))
			return "UPDATE";

		// Should throw an exception?
		return "";
	}

	/** ************************************************
	 * Generate one column grant statement
	 * 
	 * @param columns	List of columns to grant required privs
	 * @param rsCols	ResultSet for mapping column numbers to names
	 ****/

	private static String mapColumnsToNames(String columns, ResultSet rsCols)
		throws SQLException
	{
		StringBuffer colNames = new StringBuffer();
		rsCols.next();
		int curColumn = 1;
		boolean addSeparator = false;

		// Strip out outer {} in addition to spaces and comma
		StringTokenizer st = new StringTokenizer(columns, " ,{}");
		while (st.hasMoreTokens())
		{
			int colNum = Integer.parseInt(st.nextToken());
			while (colNum+1 > curColumn)
			{
				rsCols.next();
				curColumn = rsCols.getInt(1);
			}
			colNames.append(separatorStr(addSeparator));
			colNames.append(rsCols.getString(2));
			addSeparator = true;
		}

		return colNames.toString();
	}

	/** ************************************************
	 * 
	 * @param rs		ResultSet with info for this GRANT statement
	 * @param fullName	Full qualified name of the table
	 * @param authName	Authorization name for this GRANT
	 * @param rsCols	ResultSet for mapping column numbers to names
	 ****/

	private static String columnPrivStatement(ResultSet rs, String fullName,
			String authName, ResultSet rsCols) throws SQLException
	{
		StringBuffer grantStmt = new StringBuffer("GRANT ");

		String privType = rs.getString(4).toUpperCase();
		String columns = rs.getString(5);
		grantStmt.append(privTypeToString(privType));
		grantStmt.append("(");
		grantStmt.append(mapColumnsToNames(columns, rsCols));
		grantStmt.append(")");
		grantStmt.append(" TO ");
		grantStmt.append(authName);

		return grantStmt.toString();
	}

	/** ************************************************
	 * Generate udt privilege statements
	 *
	 * @param rs ResultSet holding required information
	 ****/
	public static void generateUDTPrivs(ResultSet rs) throws SQLException
	{
		boolean firstTime = true;
		while (rs.next()) {
			String authName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(1)));
			String schemaName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(2)));
			String aliasName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(3)));
			String fullName = schemaName + "." + aliasName;
			String permission = rs.getString(4);
			String objectType = rs.getString(5);

			if (dblook.isIgnorableSchema(schemaName))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_UDTPrivHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			Logs.writeToNewDDL(genericPrivStatement(fullName, authName, permission, objectType ));
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;
		}
	}
	/** ************************************************
	 * Generate sequence privilege statements
	 *
	 * @param rs ResultSet holding required information
	 ****/
	public static void generateSequencePrivs(ResultSet rs) throws SQLException
	{
		boolean firstTime = true;
		while (rs.next()) {
			String authName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(1)));
			String schemaName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(2)));
			String sequenceName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(3)));
			String fullName = schemaName + "." + sequenceName;
			String permission = rs.getString(4);
			String objectType = rs.getString(5);

			if (dblook.isIgnorableSchema(schemaName))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_SequencePrivHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			Logs.writeToNewDDL(genericPrivStatement(fullName, authName, permission, objectType ));
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;
		}
	}
	private static String genericPrivStatement(String fullName, String authName, String permission, String objectType )
		throws SQLException
	{
		boolean addSeparator = false;
		StringBuffer grantStmt = new StringBuffer("GRANT " + permission + " ON " + objectType + " " );

		grantStmt.append(fullName);
		grantStmt.append(" TO ");
		grantStmt.append(authName);

		return grantStmt.toString();
	}

	/** ************************************************
	 * Generate routine privilege statements
	 *
	 * @param rs ResultSet holding required information
	 ****/
	public static void generateRoutinePrivs(ResultSet rs) throws SQLException
	{
		boolean firstTime = true;
		while (rs.next()) {
			String authName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(1)));
			String schemaName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(2)));
			String aliasName = dblook.addQuotes
				(dblook.expandDoubleQuotes(rs.getString(3)));
			String fullName = schemaName + "." + aliasName;
			String aliasType = rs.getString(4);

			if (dblook.isIgnorableSchema(schemaName))
				continue;

			// Ignore SYSCS_UTIL privileges as all new databases automatically get them
			if (schemaName.equals("\"SYSCS_UTIL\""))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_RoutinePrivHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			Logs.writeToNewDDL(routinePrivStatement(fullName, authName, aliasType));
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;
		}
	}

	private static String routinePrivStatement(String fullName, String authName, String aliasType)
		throws SQLException
	{
		boolean addSeparator = false;
		StringBuffer grantStmt = new StringBuffer("GRANT EXECUTE ON ");

		grantStmt.append((aliasType.equals("P")) ? "PROCEDURE " : "FUNCTION ");
		grantStmt.append(fullName);
		grantStmt.append(" TO ");
		grantStmt.append(authName);

		return grantStmt.toString();
	}
}
