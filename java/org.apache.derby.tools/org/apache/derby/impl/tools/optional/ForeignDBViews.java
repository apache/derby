/*

   Derby - Class org.apache.derby.impl.tools.optional.ForeignDBViews

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

package org.apache.derby.impl.tools.optional;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;

import org.apache.derby.iapi.sql.dictionary.OptionalTool;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.vti.ForeignTableVTI;

/**
 * <p>
 * OptionalTool to create wrapper functions and views for all of the user tables
 * in a foreign database.
 * </p>
 */
public	class   ForeignDBViews  implements OptionalTool
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    private static  final   int XML_TYPE = 2009;    // needed if you aren't compiling against Java 6

    //
    // It's ok to get these SQLStates when trying to drop an object on behalf
    // of unloadTool(). The idea is to make unloadTool() idempotent.
    //
    private static  final   String[]    SAFE_DROP_SQLSTATES =
    {
        org.apache.derby.shared.common.reference.SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
        org.apache.derby.shared.common.reference.SQLState.LANG_OBJECT_DOES_NOT_EXIST,
        org.apache.derby.shared.common.reference.SQLState.LANG_SCHEMA_DOES_NOT_EXIST,
    };

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
    public  ForeignDBViews() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OptionalTool BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Creates a local Derby schema for every foreign schema which contains
     * a user table. Then creates a table function and convenience view for every
     * user table found in the foreign database. The parameters to this method are:
     * </p>
     *
     * <ul>
     * <li>foreignConnectionURL (required) - URL to connect to the foreign database</li>
     * <li>schemaPrefix (optional) - If not specified, then the local Derby schema which is
     * created has the same name as the foreign schema. Otherwise, this prefix is prepended
     * to the names of the local Derby schemas which are created.</li>
     * </ul>
     */
    public  void    loadTool( String... configurationParameters )
        throws SQLException
    {
        if ( (configurationParameters == null) || (configurationParameters.length < 1) )
        {
            throw wrap( LocalizedResource.getMessage( "OT_BadLoadUnloadArgs" ) );
        }

        String              foreignConnectionURL = configurationParameters[ 0 ];
        String              schemaPrefix = (configurationParameters.length == 1) ? null : configurationParameters[ 1 ];
        Connection          foreignConn = getForeignConnection( foreignConnectionURL );
        Connection          derbyConn = getDerbyConnection();
        DatabaseMetaData    foreignDBMD = foreignConn.getMetaData();
        ResultSet           tableCursor = getForeignTables( foreignDBMD );

        while( tableCursor.next() )
        {
            registerForeignTable
                (
                 foreignDBMD,
                 tableCursor.getString( 2 ),
                 tableCursor.getString( 3 ),
                 foreignConnectionURL,
                 schemaPrefix,
                 derbyConn
                 );
        }

        tableCursor.close();
        foreignConn.close();
    }

    /**
     * <p>
     * Removes the schemas, table functions, and views created by loadTool().
     * </p>
     *
     * <ul>
     * <li>connectionURL (required) - URL to connect to the foreign database</li>
     * <li>schemaPrefix (optional) - See loadTool() for more information on this argument.</li>
     * </ul>
     */
    public  void    unloadTool( String... configurationParameters )
        throws SQLException
    {
        if ( (configurationParameters == null) || (configurationParameters.length < 1) )
        {
            throw wrap( LocalizedResource.getMessage( "OT_BadLoadUnloadArgs" ) );
        }

        String              foreignConnectionURL = configurationParameters[ 0 ];
        String              schemaPrefix = (configurationParameters.length == 1) ? null : configurationParameters[ 1 ];
        Connection          foreignConn = getForeignConnection( foreignConnectionURL );
        Connection          derbyConn = getDerbyConnection();
        DatabaseMetaData    foreignDBMD = foreignConn.getMetaData();
        ResultSet           tableCursor = getForeignTables( foreignDBMD );
        HashSet<String> schemas = new HashSet<String>();

        while( tableCursor.next() )
        {
            String          derbySchemaName = getDerbySchemaName( schemaPrefix, tableCursor.getString( 2 ) );
            String          objectName = tableCursor.getString( 3 );

            if ( derbySchemaName != null ) { schemas.add( derbySchemaName ); }
            
            dropObject( derbyConn, derbySchemaName, objectName, "view", false );
            dropObject( derbyConn, derbySchemaName, objectName, "function", false );
        }

        tableCursor.close();
        foreignConn.close();

        // now drop the schemas created by loadTool()
        for ( String schemaName : schemas ) { dropDerbySchema( derbyConn, schemaName ); }

        // now drop the connection to the foreign database
        ForeignTableVTI.dropConnection( foreignConnectionURL );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // REGISTRATION MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private void    registerForeignTable
        (
         DatabaseMetaData   foreignDBMD,
         String             foreignSchemaName,
         String             foreignTableName,
         String             foreignConnectionURL,
         String             schemaPrefix,
         Connection     derbyConn
        )
        throws SQLException
    {
        //
        // Create DDL string for registering the table function
        //
        StringBuilder       tfBuffer = new StringBuilder();
        String              derbySchemaName = getDerbySchemaName( schemaPrefix, foreignSchemaName );
        String              dotSchemaName = dotSeparatedSchemaName( derbySchemaName );

        createDerbySchema( derbyConn, derbySchemaName );

        tfBuffer.append( "create function " + dotSchemaName + delimitedID( foreignTableName ) );
        tfBuffer.append( "\n(" );
        tfBuffer.append( "\n\tforeignSchemaName varchar( 32672 )," );
        tfBuffer.append( "\n\tforeignTableName varchar( 32672 )," );
        tfBuffer.append( "\n\tconnectionURL varchar( 32672 )" );
        tfBuffer.append( "\n)\nreturns table\n(" );
        
        ResultSet           columnCursor = foreignDBMD.getColumns( null, foreignSchemaName, foreignTableName, "%" );
        int                 columnCount = 0;
        while( columnCursor.next() )
        {
            tfBuffer.append( "\n\t" );
            if ( columnCount > 0 ) { tfBuffer.append( ", " ); }
            columnCount++;

            tfBuffer.append( delimitedID( columnCursor.getString( 4 ) ) );
            tfBuffer.append( " " );
            tfBuffer.append
                (
                 mapType
                 (
                  columnCursor.getInt( 5 ),
                  columnCursor.getInt( 7 ),
                  columnCursor.getInt( 9 ),
                  columnCursor.getString( 6 )
                  )
                 );
        }
        columnCursor.close();

        tfBuffer.append( "\n)" );
        tfBuffer.append( "\nlanguage java parameter style derby_jdbc_result_set no sql" );
        tfBuffer.append( "\nexternal name 'org.apache.derby.vti.ForeignTableVTI.readForeignTable'" );

        String          tfDDL = tfBuffer.toString();
        
        //
        // Create DDL string for registering the view
        //
        StringBuilder   viewBuffer = new StringBuilder();

        viewBuffer.append( "create view " + dotSchemaName + delimitedID( foreignTableName ) );
        viewBuffer.append( "\nas select *" );
        viewBuffer.append( "\nfrom table" );
        viewBuffer.append( "\n(\n" );
        viewBuffer.append( "\t" + dotSchemaName + delimitedID( foreignTableName ) );
        viewBuffer.append( "\n\t(" );
        viewBuffer.append( "\n\t\t" + stringLiteral( foreignSchemaName ) + "," );
        viewBuffer.append( "\n\t\t" + stringLiteral( foreignTableName ) + "," );
        viewBuffer.append( "\n\t\t" + stringLiteral( foreignConnectionURL ) );
        viewBuffer.append( "\n\t)" );
        viewBuffer.append( "\n) s" );

        String          viewDDL = viewBuffer.toString();

        //
        // Now create the table function and view.
        //
        executeDDL( derbyConn, tfDDL );
        executeDDL( derbyConn, viewDDL );
    }

    /**
     * <p>
     * Get a cursor through the user tables in the foreign database.
     * </p>
     */
    private ResultSet   getForeignTables( DatabaseMetaData foreignDBMD )
        throws SQLException
    {
        return foreignDBMD.getTables( null, null, "%", new String[] { "TABLE" } );
    }

    /**
     * <p>
     * Create a Derby schema if it does not already exist.
     * </p>
     */
    private void    createDerbySchema
        (
         Connection derbyConn,
         String     derbySchemaName
         )
        throws SQLException
    {
        if ( derbySchemaName == null ) { return; }
        
        PreparedStatement   existsPS = prepareStatement
            ( derbyConn, "select count(*) from sys.sysschemas where schemaname = ?" );
        existsPS.setString( 1, derbySchemaName );
        ResultSet   existsRS = existsPS.executeQuery();
        existsRS.next();
        boolean exists = existsRS.getInt( 1 ) > 0;
        existsRS.close();
        existsPS.close();

        if ( !exists )
        {
            executeDDL
                ( derbyConn, "create schema " + delimitedID( derbySchemaName ) );
        }
    }

    /**
     * <p>
     * Drop a Derby schema.
     * </p>
     */
    private void    dropDerbySchema
        (
         Connection derbyConn,
         String     derbySchemaName
         )
        throws SQLException
    {
        if ( derbySchemaName == null ) { return; }

        dropObject( derbyConn, null, derbySchemaName, "schema", true );
    }
        
    /**
     * <p>
     * Get the name of the local Derby schema corresponding to a foreign schema name.
     * Returns null if the default (current) schema is to be used.
     * </p>
     */
    private String  getDerbySchemaName
        (
         String schemaPrefix,
         String foreignSchemaName
         )
    {
        if ( foreignSchemaName == null ) { return null; }
        else if ( schemaPrefix == null ) { return foreignSchemaName; }
        else { return schemaPrefix + foreignSchemaName; }
    }

    /**
     * <p>
     * Turn a Derby schema name into a schema name suitable for use
     * in a dot-separated object name.
     * </p>
     */
    private String  dotSeparatedSchemaName( String rawName )
    {
        if ( rawName == null ) { return ""; }
        else { return delimitedID( rawName ) + "."; }
    }
    
    /**
     * <p>
     * Get the type of an external database's column as a Derby type name.
     * </p>
     *
     */
    private String    mapType( int jdbcType, int precision, int scale, String foreignTypeName )
        throws SQLException
    {
        switch( jdbcType )
        {
        case    Types.BIGINT:           return "bigint";
        case    Types.BINARY:           return "char " + precisionToLength( precision ) + "  for bit data";
        case    Types.BIT:              return "boolean";
        case    Types.BLOB:             return "blob";
        case    Types.BOOLEAN:          return "boolean";
        case    Types.CHAR:             return "char" + precisionToLength( precision );
        case    Types.CLOB:             return "clob";
        case    Types.DATE:             return "date";
        case    Types.DECIMAL:          return "decimal" + precisionAndScale( precision, scale );
        case    Types.DOUBLE:           return "double";
        case    Types.FLOAT:            return "float";
        case    Types.INTEGER:          return "integer";
        case    Types.LONGVARBINARY:    return "long varchar for bit data";
        case    Types.LONGVARCHAR:      return "long varchar";
        case    Types.NUMERIC:          return "numeric" + precisionAndScale( precision, scale );
        case    Types.REAL:             return "real";
        case    Types.SMALLINT:         return "smallint";
        case    Types.TIME:             return "time";
        case    Types.TIMESTAMP:        return "timestamp";
        case    Types.TINYINT:          return "smallint";
        case    Types.VARBINARY:        return "varchar " + precisionToLength( precision ) + "  for bit data";
        case    Types.VARCHAR:          return "varchar" + precisionToLength( precision );
        case    XML_TYPE:               return "xml";
 
        default:
            throw wrap
                (
                 LocalizedResource.getMessage
                 (
                  "OT_UnknownForeignDataType",
                  Integer.toString( jdbcType ),
                  foreignTypeName
                 )
                );
        }
    }

    /**
     * <p>
     * Turns precision into a length designator.
     * </p>
     *
     */
    private String  precisionToLength( int precision )
    {
        return "( " + precision + " )";
    }

    /**
     * <p>
     * Build a precision and scale designator.
     * </p>
     *
     */
    private String  precisionAndScale( int precision, int scale )
    {
        return "( " + precision + ", " + scale + " )";
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	UNREGISTRATION MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Drop a schema object. If the object does not exist, silently
     * swallow the error.
     * </p>
     */
    private void    dropObject
        (
         Connection         conn,
         String             schemaName,
         String             objectName,
         String             objectType,
         boolean        restrict
        )
        throws SQLException
    {
        String              dotSchemaName = dotSeparatedSchemaName( schemaName );
        String              restrictString = restrict ? " restrict" : "";

        try {
            executeDDL
                (
                 conn,
                 "drop " + objectType + " " + dotSchemaName + delimitedID( objectName ) + restrictString
                 );
        }
        catch (SQLException se)
        {
            String  actualSQLState = se.getSQLState();

            for ( String safeSQLState : SAFE_DROP_SQLSTATES )
            {
                if ( actualSQLState.startsWith( safeSQLState ) ) { return; }
            }

            throw se;
        }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	Connection MANAGEMENT
    //
    ////////////////////////////////////////////////////////////////////////

    private Connection  getForeignConnection( String connectionURL )
        throws SQLException
    {
        return DriverManager.getConnection( connectionURL );
    }

    private Connection  getDerbyConnection() throws SQLException
    {
        return DriverManager.getConnection( "jdbc:default:connection" );
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	SQL MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    private String  delimitedID( String text )  { return IdUtil.normalToDelimited( text ); }
    private String  stringLiteral( String text )  { return StringUtil.quoteStringLiteral( text ); }

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

}

