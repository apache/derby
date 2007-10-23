/*

Derby - Class org.apache.derbyDemo.vtis.core.QueryVTIHelper

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
import java.sql.*;
import java.util.*;

/**
 * <p>
 * This is a set of helper methods executing a query against an external database. This
 * class maintains a cache of connections to external databases. Each connection
 * is identified by its connection URL. In addition to materializing external
 * data sets, this class provides a database procedure for closing an external connection.
 * </p>
 *
  */
public  abstract    class   QueryVTIHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  Hashtable<String,Connection>    _openConnections = new Hashtable<String,Connection>();

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // DATABASE PROCEDURES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Register this method as a database procedure, then use it to register
     * all of the QueryRow table functions in a class.
     * </p>
     */
    public  static  void  registerQueryRowVTIs( String className, String connectionURL )
        throws Exception
    {
        // find public static methods which return ResultSet
        Class           theClass = Class.forName( className );
        Method[]        methods = theClass.getMethods();
        int             count = methods.length;
        Method          candidate = null;
        QueryRow        queryRowAnnotation = null;

        for ( int i = 0; i < count; i++ )
        {
            candidate = methods[ i ];

            int         modifiers = candidate.getModifiers();

            if (
                Modifier.isPublic( modifiers ) &&
                Modifier.isStatic( modifiers ) &&
                candidate.getReturnType() == ResultSet.class
                )
            {
                queryRowAnnotation = candidate.getAnnotation( QueryRow.class );

                if ( queryRowAnnotation != null )
                {
                    VTIHelper.unregisterVTI( candidate );
                    
                    registerVTI
                        (
                         candidate,
                         queryRowAnnotation.jdbcDriverName(),
                         connectionURL,
                         queryRowAnnotation.query(),
                         new String[] {}
                         );
                }
            }            
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Close the connection associated with an URL.
     * </p>
     */
    public  static  void closeConnection( String connectionURL )
        throws SQLException
    {
        Connection      conn = _openConnections.get( connectionURL );

        if ( conn != null ) { conn.close(); }

        _openConnections.remove( connectionURL );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Register a method as a Derby Table Function. We assume the following:
     * </p>
     *
     * <ul>
     * <li>The method is public and static.</li>
     * <li>The method returns a ResultSet.</li>
     * </ul>
     *
     */
    public  static  void  registerVTI
        ( Method method, String jdbcDriverName, String connectionURL, String query, String[] queryArgs )
        throws Exception
    {
        QueryRow            annotation = method.getAnnotation( QueryRow.class );
        ResultSet           rs = instantiateVTI( jdbcDriverName, connectionURL, query, queryArgs );
        ResultSetMetaData   rsmd = rs.getMetaData();
        String[]            columnNames = getColumnNames( rsmd );
        String[]            columnTypes = getColumnTypes( rsmd );

        VTIHelper.registerVTI( method, columnNames, columnTypes, true );

        rs.close();
    }
    
    /**
     * <p>
     * Create a VTI ResultSet. It is assumed that our caller is a
     * QueryRow-annotated method with no arguments.
     * </p>
     *
     */
    public  static  ResultSet  instantiateQueryRowVTI( String connectionURL )
        throws SQLException
    {
        QueryRow          annotation = null;
        
        try {
            // look up the method on top of us
            StackTraceElement[]     stack = (new Throwable()).getStackTrace();
            StackTraceElement       caller = stack[ 1 ];
            Class                   callerClass = Class.forName( caller.getClassName() );
            String                  methodName = caller.getMethodName();
            Method                  method = callerClass.getMethod
                ( methodName, new Class[] { String.class } );
            
            annotation = method.getAnnotation( QueryRow.class );
        } catch (Throwable t) { throw new SQLException( t.getMessage() ); }

        String              jdbcDriverName = annotation.jdbcDriverName();
        String              query = annotation.query();

        return instantiateVTI( jdbcDriverName, connectionURL, query, new String[] {} );
    }
    
    /**
     * <p>
     * Create an external ResultSet given a driver, a connection url, and a query.
     * </p>
     *
     */
    public static  ResultSet  instantiateVTI( String jdbcDriverName, String connectionURL, String query, String[] queryArgs )
        throws SQLException
    {
        Connection      conn = getConnection( jdbcDriverName, connectionURL );
        int             count = 0;

        if ( queryArgs != null ) { count = queryArgs.length; }

        VTIHelper.print( query );
        
        PreparedStatement           ps = conn.prepareStatement( query );
        //ParameterMetaData           pmd = ps.getParameterMetaData();

        for ( int i = 0; i < count; i++ )
        {
            String      arg = queryArgs[ i ];
            int         param = i + 1;

            if ( arg == null )
            {
                VTIHelper.print( "Setting parameter " + param + " to null" );
                //ps.setNull( param, pmd.getParameterType( param ) );
                ps.setNull( param, Types.VARCHAR );
            }
            else {
                VTIHelper.print( "Setting parameter " + param + " to " + arg );
                ps.setString( param, arg );
            }
        }

        return ps.executeQuery();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the column names from an external ResultSet.
     * </p>
     *
     */
    private static  String[]    getColumnNames( ResultSetMetaData rsmd )
        throws SQLException
    {
        int                     columnCount = rsmd.getColumnCount();
        String[]            columnNames = new String[ columnCount ];

        for ( int i = 0; i < columnCount; i++ ) { columnNames[ i ] = rsmd.getColumnName( i + 1 ); }

        return columnNames;
    }

    /**
     * <p>
     * Get the column types from an external ResultSet.
     * </p>
     *
     */
    private static  String[]    getColumnTypes( ResultSetMetaData rsmd )
        throws SQLException
    {
        int                     columnCount = rsmd.getColumnCount();
        String[]            columnTypes = new String[ columnCount ];

        for ( int i = 0; i < columnCount; i++ )
        { columnTypes[ i ] = getColumnType( rsmd, i + 1 ); }

        return columnTypes;
    }

    /**
     * <p>
     * Get the type of an external database's column as a Derby type name.
     * </p>
     *
     */
    private static  String    getColumnType( ResultSetMetaData rsmd, int idx )
        throws SQLException
    {
        int         jdbcType = rsmd.getColumnType( idx );
        int         precision = rsmd.getPrecision( idx );
        int         scale = rsmd.getScale( idx );

        switch( jdbcType )
        {
        case    Types.BIGINT:                           return "bigint";
        case    Types.BINARY:                           return "char " + precisionToLength( precision ) + "  for bit data";
        case    Types.BIT:                                  return "smallint";
        case    Types.BLOB:                             return "blob";
        case    Types.BOOLEAN:                      return "smallint";
        case    Types.CHAR:                             return "char" + precisionToLength( precision );
        case    Types.CLOB:                             return "clob";
        case    Types.DATE:                             return "date";
        case    Types.DECIMAL:                      return "decimal" + precisionAndScale( precision, scale );
        case    Types.DOUBLE:                       return "double";
        case    Types.FLOAT:                            return "float";
        case    Types.INTEGER:                      return "integer";
        case    Types.LONGVARBINARY:        return "long varchar for bit data";
        case    Types.LONGVARCHAR:          return "long varchar";
        case    Types.NUMERIC:                      return "numeric" + precisionAndScale( precision, scale );
        case    Types.REAL:                             return "real";
        case    Types.SMALLINT:                     return "smallint";
        case    Types.TIME:                             return "time";
        case    Types.TIMESTAMP:                return "timestamp";
        case    Types.TINYINT:                      return "smallint";
        case    Types.VARBINARY:                return "varchar " + precisionToLength( precision ) + "  for bit data";
        case    Types.VARCHAR:                      return "varchar" + precisionToLength( precision );
 
        default:
            throw new SQLException
                ( "Unknown external data type. JDBC type = " + jdbcType + ", external type name = " + rsmd.getColumnTypeName( idx ) );
        }
    }

    /**
     * <p>
     * Turns precision into a length designator.
     * </p>
     *
     */
    private static  String  precisionToLength( int precision )
    {
        return "( " + precision + " )";
    }
    
    /**
     * <p>
     * Build a precision and scale designator.
     * </p>
     *
     */
    private static  String  precisionAndScale( int precision, int scale )
    {
        return "( " + precision + ", " + scale + " )";
    }
    
    ///////////////////////////////////////////////////////////////
    //
    // CONNECTION MANAGEMENT MINIONS
    //
    ///////////////////////////////////////////////////////////////

    /**
     * <p>
     * Look up an open connection. If it doesn't exist, create a Connection.
     * </p>
     *
     */
    protected static  Connection  getConnection( String jdbcDriverName, String connectionURL )
        throws SQLException
    {
        Connection      conn = _openConnections.get( connectionURL );
        
        if ( conn == null )
        {
            // as necessary, fault in the jdbc driver which accesses the external dbms

            try {
                Class.forName( jdbcDriverName );
            }
            catch (ClassNotFoundException e)
            {
                throw new SQLException( "Could not find " + jdbcDriverName + " on the classpath." );
            }
            
            try {
                conn = DriverManager.getConnection( connectionURL );
                _openConnections.put( connectionURL, conn );
            }
            catch (SQLException s)
            {
                SQLException    t = new SQLException( "Could not open a connection to " + connectionURL + ". Perhaps the foreign database is not up and running? Details: " + s.getMessage() );

                t.setNextException( s );

                throw t;
            }
        }

        return conn;
    }
    
 
}
