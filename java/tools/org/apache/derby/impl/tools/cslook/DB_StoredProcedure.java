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
import java.sql.DatabaseMetaData;

import java.util.HashMap;
import org.apache.derby.tools.dblook;

public class DB_StoredProcedure {

	/* 
		IBM Copyright &copy notice.
	*/
	/**
		IBM Copyright &copy notice.
	*/

	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

	// Prepared statements use throughout the DDL
	// generation process.
	private static PreparedStatement getSpecificInfoQuery;

	/* ************************************************
	 * Generate the DDL for all stored procedures in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @return The DDL for the stored procedures has been
	 *  written to output via Logs.java.
	 ****/

	public static void doStoredProcedures(Connection conn)
		throws SQLException {

		// Note: it is safe to cast the long varchar column "javaclassname"
		// to varchar(128) because it is defined to correspond to the
		// 'aliasid' column for a given stored procedure; since the aliasid
		// column is varchar(128), javaclassname can't be any larger.  We
		// have to do this cast because DB2 mode doesn't allow equality
		// checks between long varchar columns.  Note also: the check for
		// aliastype='S' must come first for this cast to be "safe".
		getSpecificInfoQuery = conn.prepareStatement("SELECT ALIAS, " +
			"SYSTEMALIAS FROM SYS.SYSALIASES WHERE ALIASTYPE='S' AND " +
			"(CAST (JAVACLASSNAME AS VARCHAR(128))) = ?");

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT ALIAS, ALIASINFO, " +
			"ALIASID, SCHEMAID, JAVACLASSNAME, SYSTEMALIAS FROM SYS.SYSALIASES " +
			"WHERE ALIASTYPE='P'");

		boolean firstTime = true;
		while (rs.next()) {

			if (rs.getBoolean(6))
			// it's a system alias, so we ignore it.
				continue;

			String procSchema = dblook.lookupSchemaId(rs.getString(4));
			if (dblook.isIgnorableSchema(procSchema))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("CSLOOK_Header", "stored procedures");
				Logs.reportString("----------------------------------------------\n");
			}

			String procName = rs.getString(1);
			String procFullName = dblook.addQuotes(
				dblook.expandDoubleQuotes(procName));
			procFullName = procSchema + "." + procFullName;

			String creationString = createProcString(procFullName, rs);
			Logs.writeToNewDDL(creationString);
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		rs.close();
		stmt.close();
		getSpecificInfoQuery.close();
		return;

	}

	/* ************************************************
	 * Generate DDL for a specific stored procedure.
	 * @param procName Name of the current stored procedure.
	 * @param aProc Info about the current stored procedure.
	 * @return DDL for the current stored procedure is
	 *   returned, as a String.
	 ****/

	private static String createProcString(String procName,
		ResultSet aProc) throws SQLException
	{

		StringBuffer proc = new StringBuffer("CREATE PROCEDURE ");
		proc.append(procName);
		proc.append(" ");

		String params = aProc.getString(2);

		// Just grab the parameter part; we'll get the method name later.
		proc.append(params.substring(params.indexOf("("), params.length()));
		proc.append(" ");

		// Now add the external name.
		proc.append("EXTERNAL NAME '");
		proc.append(aProc.getString(5));
		proc.append(".");
		// Get method name from parameter string fetched above.
		proc.append(params.substring(0, params.indexOf("(")));
		proc.append("' ");

		// Specific name (when implemented...)
		getSpecificInfoQuery.setString(1, aProc.getString(3));
		ResultSet specificRS = getSpecificInfoQuery.executeQuery();
		if (specificRS.next()) {
			if (!specificRS.getBoolean(2)) {
			// only process it if it's not a system alias.
				proc.append("SPECIFIC ");
				proc.append(specificRS.getString(1));
			}
		}

		specificRS.close();

		return proc.toString();

	}

}
