/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.cslook
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.cslook;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;

import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;

import org.apache.derby.tools.dblook;

public class DB_Table {

	/* 
		IBM Copyright &copy notice.
	*/
	/**
		IBM Copyright &copy notice.
	*/

	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

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
			"AUTOINCREMENTINC, COLUMNNAME, REFERENCEID FROM SYS.SYSCOLUMNS " +
			"WHERE COLUMNNAME = ? AND REFERENCEID = ?");

		// Walk through list of tables and generate the DDL for
		// each one.

		boolean firstTime = true;
		Set tableIds = tableIdToNameMap.keySet();
		for (Iterator itr = tableIds.iterator(); itr.hasNext(); ) {

			String tableId = (String)itr.next();
			String tableName = (String)(tableIdToNameMap.get(tableId));
			if (dblook.isExcludedTable(tableName))
			// table isn't included in user-given list; skip it.
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("CSLOOK_Header", "tables");
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
			if (rs.getString(2) != null) {
				colDef.append(" DEFAULT ");
				colDef.append(rs.getString(2));
			}
			reinstateAutoIncrement(colName, tableId, colDef);
		}

		rs.close();
		return colDef.toString();

	}

	/* ************************************************
	 * Generate autoincrement DDL for a given column.
	 * @param colName: Name of column that is autoincrement.
	 * @param tableId: Id of table in which column exists.
	 * @param colDef: StringBuffer to which DDL will be added.
	 * @return The DDL for all autoincrement columns
	 *  has been written to the received string buffer.
	 ****/

	public static void reinstateAutoIncrement(String colName,
		String tableId, StringBuffer colDef) throws SQLException
	{

		getAutoIncStmt.setString(1, dblook.stripQuotes(colName));
		getAutoIncStmt.setString(2, tableId);
		ResultSet autoIncCols = getAutoIncStmt.executeQuery();
		if (autoIncCols.next()) {

			long start = autoIncCols.getLong(1);
			if (!autoIncCols.wasNull()) {
				colDef.append(" GENERATED ALWAYS AS IDENTITY (START WITH ");
				colDef.append(autoIncCols.getLong(1));
				colDef.append(", INCREMENT BY ");
				colDef.append(autoIncCols.getLong(2));
				colDef.append(")");
			}
		}

		return;

	}

}

