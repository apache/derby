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
 * in the seg0 subdirectory of a corrupt database.
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
     * Creates the following schema objects in a healthy database in order
     * to siphon data out of a corrupt database:
     * </p>
     *
     * <ul>
     * <li>A control schema containing views on SYSSCHEMAS, SYSCONGLOMERATES, SYSTABLES
     * and SYSCOLUMNS in the corrupt database.</li>
     * <li>A schema for every user schema in the corrupt database.</li>
     * <li>Table functions and views on every user table in the corrupt database.</li>
     * </ul>
     *
     * <p>
     * In addition, the tool creates a script for siphoning data out of the corrupt
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
     * created. These include the views on the corrupt database's SYSSCHEMAS, SYSCONGLOMERATES,
     * SYSTABLES, and SYSCOLUMNS catalogs. May not be null or an empty string.</li>
     * <li>schemaPrefix (required) - This prefix is prepended
     * to the names of the schemas which are created in the healthy database.
     * May not be null or empty.</li>
     * <li>corruptDBLocation (required) - Absolute path to the corrupt database directory.
     * That is the directory which contains service.properties. May not be null or empty.</li>
     * <li>encryptionAttributes (required) - Encryption attributes which were
     * used to connect to the corrupt database when it was bootable. May be null
     * if encryption is not being used.</li>
     * <li>dbo (required) - User name of the owner of the corrupt database. May be null
     * if authentication is not being used.</li>
     * <li>dboPassword (required) - Password for the owner of the corrupt database. May be null
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
        String  corruptDBLocation = configurationParameters[ idx++ ];
        String  encryptionAttributes = configurationParameters[ idx++ ];
        String  dbo = configurationParameters[ idx++ ];
        String  dboPassword = configurationParameters[ idx++ ];
        
        if ( nullOrEmpty( recoveryScript) )
        { throw badArgs( "Null or empty recovery script argument." ); }
        if ( nullOrEmpty( controlSchema) )
        { throw badArgs( "Null or empty control schema argument." ); }
        if ( nullOrEmpty( schemaPrefix ) )
        { throw badArgs( "Null or empty schema prefix argument." ); }
        if ( nullOrEmpty( corruptDBLocation ) )
        { throw badArgs( "Null or empty database location argument." ); }
        if ( nullOrEmpty( dbo)  )
        { throw badArgs( "Null or empty database owner argument." ); }

        Connection  conn = getDerbyConnection();

        createControlSchema( conn, controlSchema, corruptDBLocation,
                             encryptionAttributes, dbo, dboPassword );
        createUserSchemas( conn, controlSchema, schemaPrefix, corruptDBLocation,
                           encryptionAttributes, dbo, dboPassword );
        createViews( conn, recoveryScript, controlSchema, schemaPrefix, corruptDBLocation,
                     encryptionAttributes, dbo, dboPassword );
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
     * <li>controlSchema (required) - Name of the schema in which loadTool() created control objects
     * in the healthy database. May not be null or an empty string.</li>
     * <li>schemaPrefix (required) - This is the prefix which was prepended
     * to the names of the schemas which loadTool() created in the healthy
     * database. May not be null or empty.</li>
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

//IC see: https://issues.apache.org/jira/browse/DERBY-6136
        if ( nullOrEmpty( controlSchema) )
        { throw badArgs( "Null or empty control schema argument." ); }
        if ( nullOrEmpty( schemaPrefix ) )
        { throw badArgs( "Null or empty schema prefix argument." ); }

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
         String corruptDBLocation,
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
             corruptDBLocation,
             encryptionAttributes,
             dbo,
             dboPassword
             );
        createTable
            (
             conn,
             controlSchema,
             "SYSCOLUMNS", 
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
             "( referenceid char(36), columnname varchar(128), columnnumber int, columndatatype serializable, columndefault serializable, columndefaultid char(36), autoincrementvalue bigint, autoincrementstart bigint, autoincrementinc bigint, autoincrementcycle boolean )",
             "c90.dat",
             corruptDBLocation,
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
             corruptDBLocation,
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
             corruptDBLocation,
             encryptionAttributes,
             dbo,
             dboPassword
             );
    }

    /**
     * Create a table function and view for a corrupt database table.
     */
    private void    createTable
        (
         Connection conn,
         String schema,
         String tableName,
         String tableSignature,
         String heapFileName,
         String corruptDBLocation,
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
             "        '" + corruptDBLocation + "',\n" +
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
         String corruptDBLocation,
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
    /** Make the name of a local schema from a prefix and the name of a corrupt schema */
    private String  makeSchemaName( String schemaPrefix, String corruptName )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
        return IdUtil.normalToDelimited( schemaPrefix + corruptName );
    }
    
    /**
     * Create table functions and views on corrupt user tables. These objects
     * are created in the healthy database. Write the recovery
     * script.
     */
    private void    createViews
        (
         Connection conn,
         String recoveryScriptName,
         String controlSchema,
         String schemaPrefix,
         String corruptDBLocation,
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
        String  corruptSchemaName = null;
        String  corruptTableName = null;
        String  schemaName = null;
        String  tableName = null;
        long    conglomerateNumber = -1L;
        while ( rs.next() )
        {
            int     col = 1;
            String  currentCorruptSchemaName = rs.getString( col++ );
            String  currentCorruptTableName = rs.getString( col++ );

            if ( !currentCorruptSchemaName.equals( corruptSchemaName ) )
            {
                scriptWriter.println
                    ( "create schema " + IdUtil.normalToDelimited( currentCorruptSchemaName ) + ";\n" );
            }            
                
            String  newSchemaName = makeSchemaName( schemaPrefix, currentCorruptSchemaName );
            String  newTableName = IdUtil.normalToDelimited( currentCorruptTableName );

            if ( schemaName != null )
            {
                if ( !schemaName.equals( newSchemaName ) || !tableName.equals( newTableName ) )
                {
                    createView
                        (
                         conn,
                         scriptWriter,
                         controlSchema,
                         corruptSchemaName,
                         corruptTableName,
                         schemaName,
                         tableName,
                         conglomerateNumber,
                         columnNames,
                         columnTypes,
                         corruptDBLocation,
                         encryptionAttributes,
                         dbo,
                         dboPassword
                         );
                    columnNames.clear();
                    columnTypes.clear();
                }
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-6136
            corruptSchemaName = currentCorruptSchemaName;
            corruptTableName = currentCorruptTableName;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
                 corruptSchemaName,
                 corruptTableName,
                 schemaName,
                 tableName,
                 conglomerateNumber,
                 columnNames,
                 columnTypes,
                 corruptDBLocation,
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
    private String  normalizeColumnName( String unnormalizedName )
    {
        return dblook.addQuotes
            (
             dblook.expandDoubleQuotes
             (
              dblook.stripQuotes
              (
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
               dblook.addQuotes( unnormalizedName )
               )
              )
             );
    }

    /**
     * Create the table function and view for a single corrupt table.
     * Add statements to the recovery script for siphoning data out
     * of the corrupt database into the healthy database.
     */
    private void    createView
        (
         Connection conn,
         PrintWriter scriptWriter,
         String controlSchema,
         String corruptSchemaName,
         String corruptTableName,
         String schemaName,
         String tableName,
         long   conglomerateNumber,
         ArrayList<String>  columnNames,
         ArrayList<TypeDescriptor>  columnTypes,
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
         String corruptDBLocation,
         String encryptionAttributes,
         String dbo,
         String dboPassword
         )
        throws SQLException
    {
        String  conglomerateName = "c" + Long.toHexString( conglomerateNumber ) + ".dat";
        String  tableSignature = makeTableSignature( controlSchema, columnNames, columnTypes );
        String  localTableName =
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
            IdUtil.normalToDelimited( corruptSchemaName ) + "." +
            IdUtil.normalToDelimited( corruptTableName );
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
             corruptDBLocation,
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
     * Drop the table functions and views on the corrupt user tables.
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
     * Drop the table function and view for a catalog in the
     * corrupt database.
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
     * on conglomerates in the corrupt database.
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
     * corrupt core conglomerates.
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
        String  sqlState = org.apache.derby.shared.common.reference.SQLState.JAVA_EXCEPTION
            .substring( 0, 5 );

        return new SQLException( errorMessage, sqlState, t );
    }
    
}
