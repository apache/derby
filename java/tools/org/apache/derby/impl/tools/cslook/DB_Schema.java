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
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.HashMap;

import org.apache.derby.tools.dblook;

public class DB_Schema {

	/* 
		IBM Copyright &copy notice.
	*/
	/**
		IBM Copyright &copy notice.
	*/

	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

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
				Logs.reportMessage("CSLOOK_Header", "schemas");
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
