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

public class DB_View {

	/* 
		IBM Copyright &copy notice.
	*/
	/**
		IBM Copyright &copy notice.
	*/

	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

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
				Logs.reportMessage("CSLOOK_Header", "views");
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
