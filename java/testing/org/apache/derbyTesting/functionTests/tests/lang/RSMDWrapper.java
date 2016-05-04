/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.RSMDWrapper

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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.vti.StringColumnVTI;

/**
 * <p>
 * Table function wrapping the result set meta data for a query.
 * </p>
 */
public class RSMDWrapper extends StringColumnVTI
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final String[] COLUMN_NAMES = new String[]
    {
        "getCatalogName",
        "getColumnClassName",
        "getColumnDisplaySize",
        "getColumnLabel",
        "getColumnName",
        "getColumnType",
        "getColumnTypeName",
        "getPrecision",
        "getScale",
        "getSchemaName",
        "getTableName",
        "isAutoIncrement",
        "isCaseSensitive",
        "isCurrency",
        "isDefinitelyWritable",
        "isNullable",
        "isReadOnly",
        "isSearchable",
        "isSigned",
        "isWritable",
    };

    private static final int[] COLUMN_TYPES = new int[]
    {
        Types.VARCHAR,
        Types.VARCHAR,
        Types.INTEGER,
        Types.VARCHAR,
        Types.VARCHAR,
        Types.INTEGER,
        Types.VARCHAR,
        Types.INTEGER,
        Types.INTEGER,
        Types.VARCHAR,
        Types.VARCHAR,
        Types.BOOLEAN,
        Types.BOOLEAN,
        Types.BOOLEAN,
        Types.BOOLEAN,
        Types.INTEGER,
        Types.BOOLEAN,
        Types.BOOLEAN,
        Types.BOOLEAN,
        Types.BOOLEAN,
    };

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private Method[] _methods;
    private ResultSetMetaData _rsmd;
    private int _rowCount;
    private int _currentRow;
    private Integer _currentRowNumber;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TABLE FUNCTION
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This is the method which is registered as a table function.
     * </p>
     */
    public static ResultSet getResultSetMetaData( String query )
        throws Exception
    {
        return new RSMDWrapper( DriverManager.getConnection( "jdbc:default:connection" ), query );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public RSMDWrapper( Connection conn, String query ) throws Exception
    {
        super( COLUMN_NAMES );

        loadMethods();

        PreparedStatement ps = conn.prepareStatement( query );
        ResultSet rs = ps.executeQuery();

        _rsmd = rs.getMetaData();
        _rowCount = _rsmd.getColumnCount();
        _currentRow = 0;

        rs.close();
        ps.close();
    }
    private void loadMethods() throws Exception
    {
        int count = COLUMN_NAMES.length;

        _methods = new Method[ count ];

        for ( int i = 0; i < count; i++ )
        {
            _methods[ i ] = ResultSetMetaData.class.getMethod( COLUMN_NAMES[ i ], Integer.TYPE );
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // IMPLEMENTATIONS OF ABSTRACT METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public boolean next() throws SQLException
    {
        if ( _currentRow >= _rowCount ) { return false; }

        _currentRow++;
        _currentRowNumber = _currentRow;
        
        return true;
    }
    public void close()
    {
        _rsmd = null;
    }
    public ResultSetMetaData getMetaData() { return null; }

    protected  String  getRawColumn( int columnNumber ) throws SQLException
    {
        int zeroIdx = columnNumber - 1;
        Method method = _methods[ zeroIdx ];

        Object result = null;

        try {
            result = method.invoke( _rsmd, _currentRowNumber );
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new SQLException( "Could not invoke method '" + method + "'" );
        }

        int columnType = COLUMN_TYPES[ zeroIdx ];
        switch( columnType )
        {
            case Types.VARCHAR: return (String) result;
            case Types.INTEGER: return ((Integer) result).toString();
            case Types.BOOLEAN: return ((Boolean) result).toString();
            default: throw new SQLException( "Unknown data type: " + columnType );
        }
    }
}
