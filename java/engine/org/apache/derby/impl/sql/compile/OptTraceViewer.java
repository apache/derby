/*

   Derby - Class org.apache.derby.impl.sql.compile.OptTraceViewer

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

package org.apache.derby.impl.sql.compile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.sql.dictionary.OptionalTool;

/**
 * <p>
 * OptionalTool for viewing the output created when you xml-trace the optimizer.
 * </p>
 */
public	class   OptTraceViewer  implements OptionalTool
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    //
    //	STATE
    //
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor required by the OptionalTool contract */
    public  OptTraceViewer() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OptionalTool BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Takes the following arguments:
     * </p>
     *
     * <ul>
     * <li>fileURL - The url of the file holding the xml trace. E.g.: "file:///Users/me/mainline/z.txt"</li>
     * </ul>
     *
     * <p>
     * ...and creates the following schema objects for viewing the xml trace of the optimizer:
     * </p>
     *
     * <ul>
     * <li>ArrayList - A user-defined type wrapping java.util.ArrayList.</li>
     * <li>asList - A factory function for creating ArrayLists.</li>
     * <li>planCost - An XmlVTI for viewing xml traces.</li>
     * <li>planCost - A view which passes the file name to the XmlVTI.</li>
     * </ul>
     */
    public  void    loadTool( String... configurationParameters )
        throws SQLException
    {
        if ( (configurationParameters == null) || (configurationParameters.length != 1) )
        { throw wrap( MessageService.getTextMessage( SQLState.LANG_BAD_OPTIONAL_TOOL_ARGS ) ); }

        String  fileURL = configurationParameters[ 0 ];
        String  createView = XMLOptTrace.PLAN_COST_VIEW.replace( "FILE_URL", fileURL );
        
        Connection          conn = getDerbyConnection();

        executeDDL
            (
             conn,
             "create type ArrayList external name 'java.util.ArrayList' language java"
             );
        executeDDL
            (
             conn,
             "create function asList( cell varchar( 32672 ) ... ) returns ArrayList\n" +
             "language java parameter style derby no sql\n" +
             "external name 'org.apache.derby.vti.XmlVTI.asList'\n"
             );
        executeDDL( conn, XMLOptTrace.PLAN_COST_VTI );
        executeDDL( conn, createView );
    }

    /**
     * <p>
     * Drop the schema objects which were created for viewing the xml file
     * containing the optimizer trace.
     * </p>
     */
    public  void    unloadTool( String... configurationParameters )
        throws SQLException
    {
        Connection          conn = getDerbyConnection();

        executeDDL( conn, "drop view planCost" );
        executeDDL( conn, "drop function planCost" );
        executeDDL( conn, "drop function asList" );
        executeDDL( conn, "drop type ArrayList restrict" );
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    private Connection  getDerbyConnection() throws SQLException
    {
        return DriverManager.getConnection( "jdbc:default:connection" );
    }

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

    /** Wrap an exception in a SQLException */
    private SQLException    wrap( Throwable t )
    {
        return new SQLException( t.getMessage(), t );
    }
    
    private SQLException    wrap( String errorMessage )
    {
        String  sqlState = org.apache.derby.shared.common.reference.SQLState.JAVA_EXCEPTION.substring( 0, 5 );

        return new SQLException( errorMessage, sqlState );
    }
}

