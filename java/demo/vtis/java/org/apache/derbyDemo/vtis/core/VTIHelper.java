/*

Derby - Class org.apache.derbyDemo.vtis.core.VTIHelper

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

package org.apache.derbyDemo.vtis.core;

import java.io.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.sql.*;

/**
 * <p>
 * VTI helper methods.
 * </p>
 *
  */
public  abstract  class   VTIHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   boolean DEBUG = true;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Unregister a VTI.
     * </p>
     */
    public  static  void unregisterVTI
        (
         Method         method
         )
        throws SQLException
    {
        String              methodName = method.getName();
        String              sqlName = doubleQuote( methodName );

        dropObject( "function", sqlName, false );
    }

    /**
     * <p>
     * Register a VTI.
     * </p>
     */
    public  static  void registerVTI
        (
         Method         method,
         String[]       columnNames,
         String[]       columnTypes,
         boolean        readsSqlData
         )
        throws Exception
    {
        String              methodName = method.getName();
        String              sqlName = doubleQuote( methodName );
        Class               methodClass = method.getDeclaringClass();
        Class[]             parameterTypes = method.getParameterTypes();
        int                 parameterCount = parameterTypes.length;
        int                 columnCount = columnNames.length;
        StringBuilder       buffer = new StringBuilder();

        buffer.append( "create function " );
        buffer.append( sqlName );
        buffer.append( "\n( " );
        for ( int i = 0; i < parameterCount; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            
            String      parameterType = mapType( parameterTypes[ i ] );

            buffer.append( "arg" ); buffer.append( i );
            buffer.append( " " ); buffer.append( parameterType );
        }
        buffer.append( " )\n" );

        buffer.append( "returns table\n" );
        buffer.append( "( " );
        for ( int i = 0; i < columnCount; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }

            buffer.append( "\"" + columnNames[ i ] + "\"" );
            buffer.append( " " ); buffer.append( columnTypes[ i ] );
        }
        buffer.append( " )\n" );
        
        buffer.append( "language java\n" );
        buffer.append( "parameter style DERBY_JDBC_RESULT_SET\n" );
        if ( readsSqlData ) { buffer.append( "reads sql data\n" ); }
        else { buffer.append( "no sql\n" ); }
        
        buffer.append( "external name " );
        buffer.append( "'" );
        buffer.append( methodClass.getName() ); buffer.append( "." ); buffer.append( methodName );
        buffer.append( "'\n" );
        
        executeDDL( buffer.toString() );
    }

    /**
     * <p>
     * Execute a DDL statement to drop an object if it exists. Swallow exceptions.
     * </p>
     */
    public  static  void    dropObject( String objectType, String objectName, boolean objectIfMissing )
        throws SQLException
    {
        String              dropDDL = "drop " + objectType + " " + objectName;
        Connection          conn = getLocalConnection();

        // Drop the object if it does exist. Swallow exception if it doesn't.
        print( dropDDL );
        try {
            PreparedStatement   dropStatement = conn.prepareStatement( dropDDL );
            
            dropStatement.execute();
            dropStatement.close();
        } catch (SQLException s)
        {
            if ( objectIfMissing ) { throw s; }
        }
    }
    
    /**
     * <p>
     * Execute a DDL statement
     * </p>
     */
    public  static  void    executeDDL( String ddl )
        throws SQLException
    {
        Connection                  conn = getLocalConnection();

        // now register the function
        print( ddl );
        try {
            PreparedStatement   createStatement = conn.prepareStatement( ddl );
            
            createStatement.execute();
            createStatement.close();
        }
        catch (SQLException t)
        {
            SQLException    s = new SQLException( "Could not execute DDL:\n" + ddl );

            s.setNextException( t );

            throw s;
        }
    }
    
    /**
     * <p>
     * Double-quote a name.
     * </p>
     *
     */
    public  static  String doubleQuote( String name )
    { return '\"' + name + '\"'; }

    /**
     * <p>
     * Debug helper method to print a diagnostic.
     * </p>
     */
    public  static  void    print( String text )
    {
        if ( DEBUG ) { System.out.println( text ); }
    }

    /**
     * <p>
     * Get the connection to the local database.
     * </p>
     */
    public  static  Connection    getLocalConnection()
        throws SQLException
    { return DriverManager.getConnection("jdbc:default:connection"); }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Map a Java type to a SQL type.
     * </p>
     */
    private  static  String  mapType( Class javaType )
        throws Exception
    {
        if ( javaType == Long.class ) { return "bigint"; }
        else if ( javaType == Blob.class ) { return "blob"; }
        else if ( javaType == String.class ) { return "varchar( 32672 )"; }
        else if ( javaType == Clob.class ) { return "clob"; }
        else if ( javaType == BigDecimal.class ) { return "decimal"; }
        else if ( javaType == Double.class ) { return "double"; }
        else if ( javaType == Float.class ) { return "float"; }
        else if ( javaType == Integer.class ) { return "integer"; }
        else if ( javaType == byte[].class ) { return "long varchar for bit data"; }
        else if ( javaType == Short.class ) { return "smallint"; }
        else if ( javaType == Timestamp.class ) { return "timestamp"; }
        else { throw new Exception( "Unsupported type of argument to Table Function: " + javaType.getName() ); }
    }
    

}

