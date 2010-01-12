/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_Alias

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;

import java.util.HashMap;
import org.apache.derby.tools.dblook;

public class DB_Alias
{
    private static final char UDT_TYPE = 'A';
    private static final char PROCEDURE_TYPE = 'P';
    private static final char FUNCTION_TYPE = 'F';

	// Prepared statements use throughout the DDL
	// generation process.

	/* ************************************************
	 * Generate the DDL for all stored procedures,
	 * functions, and UDTs in a given database and write it to
	 * output via Logs.java.
	 * @param conn Connection to the source database.
	 * @param at10_6 True if the database is at 10.6 or higher
	 ****/

	public static void doProceduresFunctionsAndUDTs(Connection conn, boolean at10_6 )
		throws SQLException {

		// First do stored procedures.
		PreparedStatement ps = conn.prepareStatement
            (
             "SELECT ALIAS, ALIASINFO, " +
             "ALIASID, SCHEMAID, JAVACLASSNAME, SYSTEMALIAS FROM SYS.SYSALIASES " +
             "WHERE ALIASTYPE=?");

        //
        // UDTs come before procedures and functions right now because
        // procedures and functions can have args and return values which
        // have UDT types. If we add functions to the signatures of UDTs,
        // then we will have to do some trickier dependency analysis in order
        // to interleave routine and UDT ddl.
        //

        if ( at10_6 )
        {
            generateDDL( ps, UDT_TYPE ); // UDT_TYPE => for UDTs
        }
        generateDDL( ps, PROCEDURE_TYPE );	// PROCEDURE_TYPE => for PROCEDURES
		generateDDL( ps, FUNCTION_TYPE );	// FUNCTION_TYPE => for FUNCTIONS

        ps.close();

		return;
	}

    /* ************************************************
	 * Generate the DDL for the stored procedures,
	 * functions, or UDTs in a given database, depending on the
	 * the received aliasType.
	 * @param rs Result set holding either stored procedures
	 *  or functions.
	 * @param aliasType Indication of whether we're generating
	 *  stored procedures or functions.
	 ****/
	private static void generateDDL(PreparedStatement ps, char aliasType)
		throws SQLException
	{
        ps.setString( 1, new String( new char[] { aliasType } ) );

        ResultSet rs = ps.executeQuery();

        generateDDL( rs, aliasType );

        rs.close();
    }


	/* ************************************************
	 * Generate the DDL for the stored procedures,
	 * functions, or UDTs in a given database, depending on the
	 * the received aliasType.
	 * @param rs Result set holding either stored procedures
	 *  or functions.
	 * @param aliasType Indication of whether we're generating
	 *  stored procedures or functions.
	 ****/
	private static void generateDDL(ResultSet rs, char aliasType)
		throws SQLException
	{

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
                switch( aliasType )
                {
                case UDT_TYPE: Logs.reportMessage( "DBLOOK_UDTHeader" ); break;
                case PROCEDURE_TYPE: Logs.reportMessage( "DBLOOK_StoredProcHeader" ); break;
                case FUNCTION_TYPE: Logs.reportMessage( "DBLOOK_FunctionHeader" ); break;
                }
				Logs.reportString("----------------------------------------------\n");
			}

			String aliasName = rs.getString(1);
			String fullName = dblook.addQuotes(
				dblook.expandDoubleQuotes(aliasName));
			fullName = procSchema + "." + fullName;

			String creationString = createProcFuncOrUDTString(
				fullName, rs, aliasType);
			Logs.writeToNewDDL(creationString);
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

	}

	/* ************************************************
	 * Generate DDL for a specific stored procedure or
	 * function.
	 * @param aliasName Name of the current procedure/function
	 * @param aliasInfo Info about the current procedure/function
	 * @param aliasType Indicator of whether we're generating
	 *  a stored procedure or a function.
	 * @return DDL for the current stored procedure is
	 *   returned, as a String.
	 ****/

	private static String createProcFuncOrUDTString(String aliasName,
		ResultSet aliasInfo, char aliasType) throws SQLException
	{

		StringBuffer alias = new StringBuffer("CREATE ");

        switch( aliasType )
        {
        case UDT_TYPE: alias.append( "TYPE " ); break;
        case PROCEDURE_TYPE: alias.append("PROCEDURE "); break;
        case FUNCTION_TYPE: alias.append("FUNCTION "); break;
        }
		alias.append(aliasName);
		alias.append(" ");

		String params = aliasInfo.getString(2);

        if ( aliasType != UDT_TYPE )
        {
            // Just grab the parameter part; we'll get the method name later.
            alias.append(params.substring(params.indexOf("("), params.length()));
            alias.append(" ");
        }

		// Now add the external name.
		alias.append("EXTERNAL NAME '");
		alias.append(aliasInfo.getString(5));

        if ( aliasType == UDT_TYPE )
        {
            alias.append("' ");
            alias.append( params );
        }
        else
        {
            alias.append(".");
            // Get method name from parameter string fetched above.
            alias.append(params.substring(0, params.indexOf("(")));
            alias.append("' ");
        }

		return alias.toString();

	}

	/* ************************************************
	 * Generate the DDL for all synonyms in a given
	 * database. On successul return, the DDL for the
	 * synonyms has been written to output via Logs.java.
	 * @param conn Connection to the source database.
	 * @return 
	 ****/
	public static void doSynonyms(Connection conn) throws SQLException
	{
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT ALIAS, SCHEMAID, " +
			"ALIASINFO, SYSTEMALIAS FROM SYS.SYSALIASES A WHERE ALIASTYPE='S'");

		boolean firstTime = true;
		while (rs.next()) {
			if (rs.getBoolean(4))
			// it's a system alias, so we ignore it.
				continue;

			String aliasSchema = dblook.lookupSchemaId(rs.getString(2));
			if (dblook.isIgnorableSchema(aliasSchema))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_SynonymHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			String aliasName = rs.getString(1);
			String aliasFullName = dblook.addQuotes(
				dblook.expandDoubleQuotes(aliasName));
			aliasFullName = aliasSchema + "." + aliasFullName;

			Logs.writeToNewDDL("CREATE SYNONYM "+aliasFullName+" FOR "+rs.getString(3));
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;
		}

		rs.close();
		stmt.close();
		return;

	}
}
