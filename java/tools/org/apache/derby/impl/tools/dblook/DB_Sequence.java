/*

   Derby - Class org.apache.derby.impl.tools.dblook.DB_Sequence

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

/**
 * Dblook implementation for SEQUENCEs.
 */
public class DB_Sequence
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


	/**
     * <p>
	 * Generate the DDL for all sequences and output it via Logs.java.
     * </p>
     *
	 * @param conn Connection to the source database.
     */

	public static void doSequences( Connection conn )
		throws SQLException
    {
        // exclude system-generated sequences. see DERBY-6542.
		PreparedStatement ps = conn.prepareStatement
            (
             "SELECT SCHEMAID, SEQUENCENAME, SEQUENCEDATATYPE, STARTVALUE, MINIMUMVALUE, MAXIMUMVALUE, INCREMENT, CYCLEOPTION\n" +
             "FROM SYS.SYSSEQUENCES\n" +
             "WHERE CAST( SCHEMAID AS CHAR( 36) ) != '8000000d-00d0-fd77-3ed8-000a0a0b1900'"
             );
        ResultSet rs = ps.executeQuery();

		boolean firstTime = true;
		while (rs.next())
        {
            int  col = 1;
            String schemaName = dblook.lookupSchemaId( rs.getString( col++ ) );
            String sequenceName = rs.getString( col++ );
            String typeName = stripNotNull( rs.getString( col++ ) );
            long startValue = rs.getLong( col++ );
            long minimumValue = rs.getLong( col++ );
            long maximumValue = rs.getLong( col++ );
            long increment = rs.getLong( col++ );
            String cycleOption = "Y".equals( rs.getString( col++ ) ) ? "CYCLE" : "NO CYCLE";

			if (firstTime)
            {
				Logs.reportString("----------------------------------------------");
                Logs.reportMessage( "DBLOOK_SequenceHeader" );
				Logs.reportString("----------------------------------------------\n");
			}

			String fullName = dblook.addQuotes( dblook.expandDoubleQuotes( sequenceName ) );
			fullName = schemaName + "." + fullName;

			String creationString = createSequenceString
                ( fullName, typeName, startValue, minimumValue, maximumValue, increment, cycleOption );
			Logs.writeToNewDDL(creationString);
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;
		}

        rs.close();
        ps.close();
	}
    /** Strip the trailing NOT NULL off of the string representation of a datatype */
    private static String stripNotNull( String datatypeName )
    {
        int idx = datatypeName.indexOf( "NOT" );
        if ( idx > 0 ) { return datatypeName.substring( 0, idx ); }
        else { return datatypeName; }
    }

	/**
     * <p>
	 * Generate DDL for a specific sequence.
     * </p>
     *
     * @param fullName Fully qualified name of the sequence
     * @param dataTypeName Name of the datatype of the sequence
     * @param startValue First value to use in the range of the sequence
     * @param minimumValue Smallest value in the range
     * @param maximumValue Largest value in the range
     * @param increment Step size of the sequence
     * @param cycleOption CYCLE or NO CYCLE
     *
	 * @return DDL for the current stored sequence
     */
	private static String createSequenceString
        (
         String fullName,
         String dataTypeName,
         long startValue,
         long minimumValue,
         long maximumValue,
         long increment,
         String cycleOption
         )
		throws SQLException
	{
		StringBuffer buffer = new StringBuffer();

        buffer.append( "CREATE SEQUENCE " + fullName + '\n' );

        buffer.append( "    AS " + dataTypeName + '\n' );

        buffer.append( "    START WITH " + Long.toString( startValue ) + '\n' );

        buffer.append( "    INCREMENT BY " + Long.toString( increment ) + '\n' );

        buffer.append( "    MAXVALUE " + Long.toString( maximumValue ) + '\n' );

        buffer.append( "    MINVALUE " + Long.toString( minimumValue ) + '\n' );

        buffer.append( "    " + cycleOption + '\n' );

		return buffer.toString();
	}

}
