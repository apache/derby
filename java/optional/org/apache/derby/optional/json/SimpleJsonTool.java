/*

   Derby - Class org.apache.derby.optional.json.SimpleJsonTool

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.optional.json;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.json.simple.JSONArray;

import org.apache.derby.iapi.sql.dictionary.OptionalTool;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.optional.api.SimpleJsonUtils;
import org.apache.derby.optional.utils.ToolUtilities;

/**
 * <p>
 * OptionalTool which adds support types and functions for
 * using the JSON.simple toolkit at https://code.google.com/p/json-simple/.
 * Creates the following schema objects in the current schema:
 * </p>
 *
 * <ul>
 * <li><b>JSONArray</b> - A UDT bound to the JSON.simple JSONArray class.</li>
 * <li><b>toJSON</b> - A function for packaging up query results as a
 * JSONArray. Each cell in the array is a row. Each row
 * has key/value pairs for all columns returned by the query.</li>
 * <li><b>readArrayFromString</b> - A function which turns a JSON document
 * string into a JSONArray.</li>
 * <li><b>readArrayFromFile</b> - A function which reads a file containing a
 * JSON document and turns it into a JSONArray.</li>
 * <li><b>readArrayFromURL</b> - A function which reads a JSON document from an
 * URL address and turns it into a JSONArray.</li>
 * </ul>
 */
public	class   SimpleJsonTool  implements OptionalTool
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    private FunctionDescriptor[]    _functionDescriptors =
    {
        new FunctionDescriptor
        (
            "toJSON",
            "create function toJSON" +
            "\n(" +
            "\n\tqueryString varchar( 32672 )," +
            "\n\tqueryArgs varchar( 32672 ) ..." +
            "\n)\nreturns JSONArray\n" +
            "\nlanguage java parameter style derby reads sql data" +
            "\nexternal name 'org.apache.derby.optional.json.SimpleJsonTool.toJSON'\n"
        ),

        new FunctionDescriptor
        (
            "readArrayFromString",
            "create function readArrayFromString( document varchar( 32672 ) )\n" +
            "returns JSONArray\n" +
            "language java parameter style java contains sql\n" +
            "external name 'org.apache.derby.optional.api.SimpleJsonUtils.readArrayFromString'\n"
        ),

        new FunctionDescriptor
        (
            "readArrayFromFile",
            "create function readArrayFromFile\n" +
            "( fileName varchar( 32672 ), characterSetName varchar( 100 ) )\n" +
            "returns JSONArray\n" +
            "language java parameter style java contains sql\n" +
            "external name 'org.apache.derby.optional.api.SimpleJsonUtils.readArrayFromFile'\n"
        ),

        new FunctionDescriptor
        (
            "readArrayFromURL",
            "create function readArrayFromURL\n" +
            "( urlString varchar( 32672 ), characterSetName varchar( 100 ) )\n" +
            "returns JSONArray\n" +
            "language java parameter style java contains sql\n" +
            "external name 'org.apache.derby.optional.api.SimpleJsonUtils.readArrayFromURL'\n"
        ),

        new FunctionDescriptor
        (
            "arrayToClob",
            "create function arrayToClob( jsonDocument JSONArray ) returns clob\n" +
            "language java parameter style java no sql\n" +
            "external name 'org.apache.derby.optional.api.SimpleJsonUtils.arrayToClob'"
        ),
    };

    ////////////////////////////////////////////////////////////////////////
    //
    //	STATE
    //
    ////////////////////////////////////////////////////////////////////////

    public static final class FunctionDescriptor
    {
        public final String functionName;
        public final String creationDDL;

        public FunctionDescriptor( String functionName, String creationDDL )
        {
            this.functionName = functionName;
            this.creationDDL = creationDDL;
        }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor required by the OptionalTool contract */
    public  SimpleJsonTool() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OptionalTool BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Binds a UDT to JSONArray and creates a function to turn a query result
     * into a JSONArray. There are no parameters to this method.
     * </p>
     */
    public  void    loadTool( String... configurationParameters )
        throws SQLException
    {
        if ( (configurationParameters != null) && (configurationParameters.length != 0) )
        {
            throw wrap( LocalizedResource.getMessage( "OT_BadLoadUnloadArgs" ) );
        }

        Connection          derbyConn = getDerbyConnection();

        createUDT( derbyConn );
        createFunctions( derbyConn );
        
        boolean sqlAuthorizationEnabled = ToolUtilities.sqlAuthorizationEnabled( derbyConn );
        if ( sqlAuthorizationEnabled ) { grantPermissions( derbyConn ); }
    }

    /**
     * Grant permissions to use the newly loaded UDT and FUNCTIONs.
     */
    private void    grantPermissions( Connection conn )  throws SQLException
    {
        executeDDL( conn, "grant usage on type JSONArray to public" );

        for ( FunctionDescriptor desc : _functionDescriptors )
        {
            executeDDL( conn, "grant execute on function " + desc.functionName + " to public" );
        }
    }

    /**
     * <p>
     * Removes the function and UDT created by loadTool().
     * </p>
     * </ul>
     */
    public  void    unloadTool( String... configurationParameters )
        throws SQLException
    {
        if ( (configurationParameters != null) && (configurationParameters.length != 0) )
        {
            throw wrap( LocalizedResource.getMessage( "OT_BadLoadUnloadArgs" ) );
        }

        Connection          derbyConn = getDerbyConnection();

        dropFunctions( derbyConn );
        dropUDT( derbyConn );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SQL FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Compile a query (with optional ? parameters) and pack the results
     * into a JSONArray. This is the entry point which the simpleJson
     * tool binds to the function name "toJSON".
     * </p>
     */
    public  static  JSONArray   toJSON
        (
         String queryString,
         String... queryArgs
         )
        throws SQLException
    {
        PreparedStatement   ps = null;

        try {
            Connection  conn = getDerbyConnection();
            
            ps = conn.prepareStatement( queryString );

            if ( queryArgs != null )
            {
                for ( int i = 0; i < queryArgs.length; i++  )
                {
                    ps.setString( i + 1, queryArgs[ i ] );
                }
            }

            ResultSet   rs = ps.executeQuery();

            return SimpleJsonUtils.toJSON( rs );
        }
        finally
        {
            if ( ps != null )
            {
                ps.close();
            }
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // (UN)REGISTRATION MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private void    createUDT
        (
         Connection     derbyConn
        )
        throws SQLException
    {
        String  createString =
            "create type JSONArray external name 'org.json.simple.JSONArray' language java";
        
        executeDDL( derbyConn, createString );
    }

    private void    dropUDT
        (
         Connection     derbyConn
        )
        throws SQLException
    {
        String  createString =
            "drop type JSONArray restrict";
        
        executeDDL( derbyConn, createString );
    }

    private void    createFunctions
        (
         Connection     derbyConn
        )
        throws SQLException
    {
        for ( FunctionDescriptor desc : _functionDescriptors )
        {
            executeDDL( derbyConn, desc.creationDDL );
        }
    }

    private void    dropFunctions
        (
         Connection     derbyConn
        )
        throws SQLException
    {
        for ( FunctionDescriptor desc : _functionDescriptors )
        {
            String              dropString = "drop function " + desc.functionName;

            executeDDL( derbyConn, dropString );
        }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	SQL MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    private void    executeDDL
        ( Connection conn, String text )
        throws SQLException
    {
        PreparedStatement   ddl = prepareStatement( conn, text );
        ddl.execute();
        ddl.close();
    }
    
    private PreparedStatement   prepareStatement
        ( Connection conn, String text )
        throws SQLException
    {
        return conn.prepareStatement( text );
    }

    private SQLException    wrap( String errorMessage )
    {
        String  sqlState = org.apache.derby.shared.common.reference.SQLState.JAVA_EXCEPTION.substring( 0, 5 );

        return new SQLException( errorMessage, sqlState );
    }

    private static  Connection  getDerbyConnection() throws SQLException
    {
        return DriverManager.getConnection( "jdbc:default:connection" );
    }

}
