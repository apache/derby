/*

   org.apache.derby.optional.dump.RawDBReader

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

package org.apache.derby.optional.dump;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.sql.dictionary.OptionalTool;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.tools.dblook;

/**
 * <p>
 * OptionalTool to create wrapper functions and views for all of the user heap conglomerates
 * in the seg0 subdirectory of a database.
 * </p>
 */
public	class   RawDBReader  implements OptionalTool
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
    public  RawDBReader() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OptionalTool BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Creates the following:
     * </p>
     *
     * <ul>
     * <li>A control schema containing views on SYSSCHEMAS, SYSCONGLOMERATES, SYSTABLES
     * and SYSCOLUMNS in the raw database.</li>
     * <li>A schema for every user schema in the raw database.</li>
     * <li>Table functions and views on every user table in the raw database..</li>
     * </ul>
     *
     * <p>
     * In addition, the tool creates a script for siphoning data out of the raw
     * database.
     * </p>
     *
     * <p>
     * Takes the following arguments:
     * </p>
     *
     * <ul>
     * <li>recoveryScript (required) - Name of the recovery script file which
     * the tool will write.</li>
     * <li>controlSchema (required) - Name of a schema in which control objects will be
     * created. These include the views on the raw database's SYSSCHEMAS, SYSCONGLOMERATES,
     * SYSTABLES, and SYSCOLUMNS catalogs. May not be null or an empty string.</li>
     * <li>schemaPrefix (required) - This prefix is prepended
     * to the names of the local Derby schemas which are created. May not be null or empty.</li>
     * <li>rawDBLocation (required) - Absolute path to the raw database directory.
     * That is the directory which contains service.properties. May not be null or empty.</li>
     * <li>encryptionAttributes (required) - Encryption attributes. May be null
     * if encryption is not being used.</li>
     * <li>dbo (required) - User name of the owner of the raw database. May be null
     * if authentication is not being used.</li>
     * <li>dboPassword (required) - Password for the owner of the raw database. May be null
     * if authentication is not being used.</li>
     * </ul>
     */
    public  void    loadTool( String... configurationParameters )
        throws SQLException
    {
        if ( (configurationParameters == null) || (configurationParameters.length < 7) )
        { throw badArgs( "Wrong number of arguments." ); }

        int     idx = 0;
        String  recoveryScript = configurationParameters[ idx++ ];
        String  controlSchema = configurationParameters[ idx++ ];
        String  schemaPrefix = configurationParameters[ idx++ ];
        String  rawDBLocation = configurationParameters[ idx++ ];
        String  encryptionAttributes = configurationParameters[ idx++ ];
        String  dbo = configurationParameters[ idx++ ];
        String  dboPassword = configurationParameters[ idx++ ];
        
        if ( nullOrEmpty( recoveryScript) )  { throw badArgs( "Null or empty recovery script argument." ); }
        if ( nullOrEmpty( controlSchema) )  { throw badArgs( "Null or empty control schema argument." ); }
        if ( nullOrEmpty( schemaPrefix ) )  { throw badArgs( "Null or empty schema prefix argument." ); }
        if ( nullOrEmpty( rawDBLocation ) )  { throw badArgs( "Null or empty database location argument." ); }
        if ( nullOrEmpty( dbo)  )   { throw badArgs( "Null or empty database owner argument." ); }

        Connection  conn = getDerbyConnection();

        createControlSchema( conn, controlSchema, rawDBLocation, encryptionAttributes, dbo, dboPassword );
        createUserSchemas( conn, controlSchema, schemaPrefix, rawDBLocation, encryptionAttributes, dbo, dboPassword );
        createViews( conn, recoveryScript, controlSchema, schemaPrefix, rawDBLocation, encryptionAttributes, dbo, dboPassword );
    }

    /** Returns true if the text is null or empty */
    private boolean nullOrEmpty( String text ) { return ( (text == null) || (text.length() == 0) ); }

    /**
     * <p>
     * Removes the schemas, table functions, and views created by loadTool().
     * </p>
     *
     * <p>
     * Takes the following arguments:
     * </p>
     *
     * <ul>
     * <li>controlSchema (required) - Name of the schema in which control objects were created
     * by loadTool(). May not be null or an empty string.</li>
     * <li>schemaPrefix (required) - This is the prefix which was prepended
     * to the names of the local Derby schemas which loadTool() created. May not be null or empty.</li>
     * </ul>
     */
    public  void    unloadTool( String... configurationParameters )
        throws SQLException
    {
        if ( (configurationParameters == null) || (configurationParameters.length < 2) )
        { throw badArgs( "Wrong number of arguments." ); }
        
        int     idx = 0;
        String  controlSchema = configurationParameters[ idx++ ];
        String  schemaPrefix = configurationParameters[ idx++ ];

        if ( nullOrEmpty( controlSchema) )  { throw badArgs( "Null or empty control schema argument." ); }
        if ( nullOrEmpty( schemaPrefix ) )  { throw badArgs( "Null or empty schema prefix argument." ); }

        Connection  conn = getDerbyConnection();

        dropViews( conn, schemaPrefix );
        dropUserSchemas( conn, schemaPrefix );
        dropControlSchema( conn, controlSchema );
    }
    /** Return a "bad args" exception */
    private SQLException    badArgs( String message )
    {
        return new SQLException( message );
    }
    

    ////////////////////////////////////////////////////////////////////////
    //
    //	LOADING MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Create and populate the schema which holds control objects.
     */
    private void    createControlSchema
        (
         Connection conn,
         String controlSchema,
         String rawDBLocation,
         String encryptionAttributes,
         String dbo,
         String dboPassword
         )
        throws SQLException
    {
        // create the schema and move into it
        executeDDL( conn, "create schema " + controlSchema );
        executeDDL( conn, "set schema " + controlSchema );

        // create a generic UDT for use with all object types
        executeDDL( conn, "create type serializable external name 'java.io.Serializable' language java" );

        // create table functions and views on core catalogs
        createTable
            (
             conn,
             controlSchema,
             "SYSCONGLOMERATES", 
             "( schemaid char(36), tableid char(36), conglomeratenumber bigint, conglomeratename varchar( 128), isindex boolean, descriptor serializable, isconstant boolean, conglomerateid char( 36 ) )",
             "c20.dat",
             rawDBLocation,
             encryptionAttributes,
             dbo,
             dboPassword
             );
        createTable
            (
             conn,
             controlSchema,
             "SYSCOLUMNS", 
             "( referenceid char(36), columnname varchar(128), columnnumber int, columndatatype serializable, columndefault serializable, columndefaultid char(36), autoincrementvalue bigint, autoincrementstart bigint, autoincrementinc bigint )",
             "c90.dat",
             rawDBLocation,
             encryptionAttributes,
             dbo,
             dboPassword
             );
        createTable
            (
             conn,
             controlSchema,
             "SYSSCHEMAS", 
             DataFileVTI.SYSSCHEMAS_SIGNATURE,
             DataFileVTI.SYSSCHEMAS_CONGLOMERATE_NAME,
             rawDBLocation,
             encryptionAttributes,
             dbo,
             dboPassword
             );
        createTable
            (
             conn,
             controlSchema,
             "SYSTABLES", 
             DataFileVTI.SYSTABLES_SIGNATURE,
             DataFileVTI.SYSTABLES_CONGLOMERATE_NAME,
             rawDBLocation,
             encryptionAttributes,
             dbo,
             dboPassword
             );
    }

    /**
     * Create a table function and view for a raw database table.
     */
    private void    createTable
        (
         Connection conn,
         String schema,
         String tableName,
         String tableSignature,
         String heapFileName,
         String rawDBLocation,
         String encryptionAttributes,
         String dbo,
         String dboPassword
         )
        throws SQLException
    {
        String  qualifiedName = schema + "." + tableName;
        String  dataFileVTIClassName = DataFileVTI.class.getName();
        
        executeDDL
            (
             conn,
            "create function " + qualifiedName + "\n" +
            "(\n" +
            "    databaseDirectoryName varchar( 32672 ),\n" +
            "    dataFileName varchar( 32672 ),\n" +
            "    tableSignature varchar( 32672 ),\n" +
            "    encryptionAttributes varchar( 32672 ),\n" +
            "    userName varchar( 32672 ),\n" +
            "    password varchar( 32672 )\n" +
            ")\n" +
            "returns table\n" + tableSignature +
            "language java\n" +
            "parameter style derby_jdbc_result_set\n" +
            "no sql\n" +
            "external name '" + dataFileVTIClassName + ".dataFileVTI'\n"
            );
        executeDDL
            (
             conn,
             "create view " + qualifiedName + "\n" +
             "as select * from table\n" +
             "(\n" +
             "    " + qualifiedName + "\n" +
             "    (\n" +
             "        '" + rawDBLocation + "',\n" +
             "        '" + heapFileName + "',\n" +
             "        '" + tableSignature + "',\n" +
             "        " + singleQuote( encryptionAttributes ) + ",\n" +
             "        " + singleQuote( dbo ) + ",\n" +
             "        " + singleQuote( dboPassword ) + "\n" +
             "    )\n" +
             ") t\n"
             );
    }

    /**
     * Create user schemas.
     */
    private void    createUserSchemas
        (
         Connection conn,
         String controlSchema,
         String schemaPrefix,
         String rawDBLocation,
         String encryptionAttributes,
         String dbo,
         String dboPassword
         )
        throws SQLException
    {
        PreparedStatement   ps = prepareStatement
            (
             conn,
             "select schemaName\n" +
             "from "+ controlSchema + ".sysschemas\n" +
             "where schemaName not like 'SYS%' and schemaName != 'NULLID' and schemaName != 'SQLJ'\n"
             );
        ResultSet   rs = ps.executeQuery();

        while ( rs.next() )
        {
            String  schemaName = makeSchemaName( schemaPrefix , rs.getString( 1 ) );

            executeDDL( conn, "create schema " + schemaName );
        }

        rs.close();
        ps.close();
    }
    /** Make the name of a local schema from a prefix and a raw name */
    private String  makeSchemaName( String schemaPrefix, String rawName )
    {
        return IdUtil.normalToDelimited( schemaPrefix + rawName );
    }
    
    /**
     * Create table functions and views on user tables. Write the recovery
     * script.
     */
    private void    createViews
        (
         Connection conn,
         String recoveryScriptName,
         String controlSchema,
         String schemaPrefix,
         String rawDBLocation,
         String encryptionAttributes,
         String dbo,
         String dboPassword
         )
        throws SQLException
    {
        File                recoveryScript = new File( recoveryScriptName );
        PrintWriter         scriptWriter = null;
        try {
            scriptWriter = new PrintWriter( recoveryScript );
        }
        catch (Exception e) { throw wrap( e ); }

        String              localDBName = ((EmbedConnection) conn).getDBName();
        scriptWriter.println( "connect 'jdbc:derby:" + localDBName + "';\n" );
        
        PreparedStatement   ps = prepareStatement
            (
             conn,
             "select s.schemaName, t.tableName, g.conglomerateNumber, c.columnName, c.columnNumber, c.columnDatatype\n" +
             "from " + controlSchema + ".sysschemas s,\n" +
             controlSchema + ".systables t,\n" +
             controlSchema + ".sysconglomerates g,\n" +
             controlSchema + ".syscolumns c\n" +
             "where s.schemaName not like 'SYS%' and schemaName != 'NULLID' and schemaName != 'SQLJ'\n" +
             "and s.schemaID = t.schemaID\n" +
             "and t.tableID = g.tableID and not g.isindex\n" +
             "and t.tableID = c.referenceID\n" +
             "order by s.schemaName, t.tableName, c.columnNumber"
             );
        ResultSet   rs = ps.executeQuery();

        ArrayList<String>   columnNames = new ArrayList<String>();
        ArrayList<TypeDescriptor>   columnTypes = new ArrayList<TypeDescriptor>();
        String  rawSchemaName = null;
        String  rawTableName = null;
        String  schemaName = null;
        String  tableName = null;
        long    conglomerateNumber = -1L;
        while ( rs.next() )
        {
            int     col = 1;
            String  currentRawSchemaName = rs.getString( col++ );
            String  currentRawTableName = rs.getString( col++ );

            if ( !currentRawSchemaName.equals( rawSchemaName ) )
            {
                scriptWriter.println
                    ( "create schema " + IdUtil.normalToDelimited( currentRawSchemaName ) + ";\n" );
            }            
                
            String  newSchemaName = makeSchemaName( schemaPrefix, currentRawSchemaName );
            String  newTableName = IdUtil.normalToDelimited( currentRawTableName );

            if ( schemaName != null )
            {
                if ( !schemaName.equals( newSchemaName ) || !tableName.equals( newTableName ) )
                {
                    createView
                        (
                         conn,
                         scriptWriter,
                         controlSchema,
                         rawSchemaName,
                         rawTableName,
                         schemaName,
                         tableName,
                         conglomerateNumber,
                         columnNames,
                         columnTypes,
                         rawDBLocation,
                         encryptionAttributes,
                         dbo,
                         dboPassword
                         );
                    columnNames.clear();
                    columnTypes.clear();
                }
            }

            rawSchemaName = currentRawSchemaName;
            rawTableName = currentRawTableName;
            schemaName = newSchemaName;
            tableName = newTableName;
            conglomerateNumber = rs.getLong( col++ );
            columnNames.add( normalizeColumnName( rs.getString( col++ ) ) );
            col++;  // only need the column number to order the results
            columnTypes.add( (TypeDescriptor) rs.getObject( col++ ) );
        }

        // create last view
        if ( schemaName != null )
        {
            createView
                (
                 conn,
                 scriptWriter,
                 controlSchema,
                 rawSchemaName,
                 rawTableName,
                 schemaName,
                 tableName,
                 conglomerateNumber,
                 columnNames,
                 columnTypes,
                 rawDBLocation,
                 encryptionAttributes,
                 dbo,
                 dboPassword
                 );
        }

        rs.close();
        ps.close();
        scriptWriter.flush();
        scriptWriter.close();
    }
    /** Use dblook methods to normalize the name of a column */
    private String  normalizeColumnName( String raw )
    {
        return dblook.addQuotes
            (
             dblook.expandDoubleQuotes
             (
              dblook.stripQuotes
              (
               dblook.addQuotes( raw )
               )
              )
             );
    }

    /**
     * Create the table function and view for a single raw table.
     * Add statements to the recovery script for siphoning data out
     * of the raw database into the current database.
     */
    private void    createView
        (
         Connection conn,
         PrintWriter scriptWriter,
         String controlSchema,
         String rawSchemaName,
         String rawTableName,
         String schemaName,
         String tableName,
         long   conglomerateNumber,
         ArrayList<String>  columnNames,
         ArrayList<TypeDescriptor>  columnTypes,
         String rawDBLocation,
         String encryptionAttributes,
         String dbo,
         String dboPassword
         )
        throws SQLException
    {
        String  conglomerateName = "c" + Long.toHexString( conglomerateNumber ) + ".dat";
        String  tableSignature = makeTableSignature( controlSchema, columnNames, columnTypes );
        String  localTableName =
            IdUtil.normalToDelimited( rawSchemaName ) + "." +
            IdUtil.normalToDelimited( rawTableName );
        String  viewName = schemaName + "." + tableName;
        
        scriptWriter.println( "-- siphon data out of " + conglomerateName );
        scriptWriter.println
            (
             "create table " + localTableName + " as select * from " +
             viewName + " with no data;"
             );
        scriptWriter.println( "insert into " + localTableName + " select * from " + viewName + ";\n" );
        
        createTable
            (
             conn,
             schemaName,
             tableName, 
             tableSignature,
             conglomerateName,
             rawDBLocation,
             encryptionAttributes,
             dbo,
             dboPassword
             );
    }
    /** Make the signature of a table from its column names and types */
    private String  makeTableSignature
        (
         String controlSchema,  // for serializable types
         ArrayList<String>  columnNames,
         ArrayList<TypeDescriptor>  columnTypes
         )
    {
        StringBuilder   buffer = new StringBuilder();

        buffer.append( "( " );
        for ( int i = 0; i < columnNames.size(); i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnNames.get( i ) + " " );

            TypeDescriptor  type = columnTypes.get( i );
            if ( type.isUserDefinedType() ) { buffer.append( controlSchema + ".serializable" ); }
            else { buffer.append( type.getSQLstring() ); }
        }
        buffer.append( " )" );

        return buffer.toString();
    }

    
    /** Return "null" if string is null, otherwise single quote it */
    private String  singleQuote( String text )   { return text == null ? "null" : "'" + text + "'"; }
    
    ////////////////////////////////////////////////////////////////////////
    //
    //	UNLOADING MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Drop the table functions and views on user tables.
     */
    private void    dropViews
        (
         Connection conn,
         String schemaPrefix
         )
        throws SQLException
    {
        PreparedStatement   ps = prepareStatement
            (
             conn,
             "select s.schemaName, t.tableName\n" +
             "from sys.sysschemas s, sys.systables t\n" +
             "where s.schemaName like '" + schemaPrefix + "%'\n" +
             "and s.schemaID = t.schemaID"
             );
        ResultSet   rs = ps.executeQuery();

        while ( rs.next() )
        {
            int col = 1;
            dropTable
                (
                 conn,
                 IdUtil.normalToDelimited( rs.getString( col++ ) ),
                 IdUtil.normalToDelimited( rs.getString( col++ ) )
                 );
        }

        rs.close();
        ps.close();
    }

    /**
     * Drop the table function and view for a raw database catalog.
     */
    private void    dropTable
        (
         Connection conn,
         String schema,
         String tableName
         )
        throws SQLException
    {
        String  qualifiedName = schema + "." + tableName;
        
        executeDDL( conn, "drop view " + qualifiedName );
        executeDDL( conn, "drop function " + qualifiedName );
    }

    /**
     * Drop the now empty schemas which held the table functions and views
     * on raw conglomerates.
     */
    private void    dropUserSchemas
        (
         Connection conn,
         String schemaPrefix
         )
        throws SQLException
    {
        PreparedStatement   ps = prepareStatement
            (
             conn,
             "select s.schemaName\n" +
             "from sys.sysschemas s\n" +
             "where s.schemaName like '" + schemaPrefix + "%'\n"
             );
        ResultSet   rs = ps.executeQuery();

        while ( rs.next() )
        {
            String  schemaName = IdUtil.normalToDelimited( rs.getString( 1 ) );
            
            executeDDL( conn, "drop schema " + schemaName + " restrict" );
        }
        
        rs.close();
        ps.close();
    }

    /**
     * Drop the schema which holds the table functions and views on the
     * raw external core conglomerates.
     */
    private void    dropControlSchema
        (
         Connection conn,
         String controlSchema
         )
        throws SQLException
    {
        executeDDL( conn, "set schema sys" );

        dropTable( conn, controlSchema, "SYSTABLES" );
        dropTable( conn, controlSchema, "SYSSCHEMAS" );
        dropTable( conn, controlSchema, "SYSCOLUMNS" );
        dropTable( conn, controlSchema, "SYSCONGLOMERATES" );

        executeDDL( conn, "drop type " + controlSchema + ".serializable restrict" );
        executeDDL( conn, "drop schema " + controlSchema + " restrict" );
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	Connection MANAGEMENT
    //
    ////////////////////////////////////////////////////////////////////////

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

    private SQLException    wrap( Throwable t )
    {
        String  errorMessage = t.getMessage();
        String  sqlState = org.apache.derby.shared.common.reference.SQLState.JAVA_EXCEPTION.substring( 0, 5 );

        return new SQLException( errorMessage, sqlState, t );
    }
    
}
