/*

Derby - Class org.apache.derby.optional.api.SimpleJsonVTI

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

package org.apache.derby.optional.api;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.derby.vti.VTITemplate;

import org.apache.derby.optional.utils.ToolUtilities;

/**
 * <p>
 * This is a table function which turns a JSON array into a relational
 * ResultSet. This table function relies on the JSON.simple JSONArray class
 * found at https://code.google.com/p/json-simple/.
 * Each object in the array is turned into a row.
 * The shape of the row is declared by the CREATE FUNCTION ddl
 * and the shape corresponds to the key names found in the row objects.
 * Provided that the values in those objects have the expected type,
 * the following ResultSet accessors can be called:
 * </p>
 *
 * <ul>
 *  <li>getString()</li>
 *  <li>getBoolean()</li>
 *  <li>getByte()</li>
 *  <li>getShort()</li>
 *  <li>getInt()</li>
 *  <li>getLong()</li>
 *  <li>getFloat()</li>
 *  <li>getDouble()</li>
 *  <li>getObject()</li>
 *  <li>getBigDecimal()</li>
 * </ul>
 *
 * <p>
 * This table function relies on the JSONArray type loaded by the simpleJson optional
 * tool. This table function can be combined with other JSONArray-creating
 * functions provided by that tool.
 * </p>
 *
 * <p>
 * Here's an example of how to use this VTI on a JSON document read across
 * the network using the readArrayFromURL function provided by the simpleJson tool:
 * </p>
 *
 * <pre>
 * call syscs_util.syscs_register_tool( 'simpleJson', true );
 *
 * create function thermostatReadings( jsonDocument JSONArray )
 * returns table
 * (
 *   "id" int,
 *   "temperature" float,
 *   "fanOn" boolean
 * )
 * language java parameter style derby_jdbc_result_set contains sql
 * external name 'org.apache.derby.optional.api.SimpleJsonVTI.readArray';
 * 
 * select * from table
 * (
 *    thermostatReadings
 *    (
 *       readArrayFromURL( 'https://thermostat.feed.org', 'UTF-8' )
 *    )
 * ) t;
 * </pre>
 *
 * <p>
 * That returns a table like this:
 * </p>
 *
 * <pre>
 * id         |temperature             |fanOn
 * ------------------------------------------
 * 1          |70.3                    |true 
 * 2          |65.5                    |false
 * </pre>
 *
 * <p>
 * Here's an example of how to use this VTI on a JSON document string
 * with the assistance of the readArrayFromString function provided by the simpleJson tool:
 * </p>
 *
 * <pre>
 * select * from table
 * (
 *    thermostatReadings
 *    (
 *       readArrayFromString
 *       (
 *        '[ { "id": 1, "temperature": 70.3, "fanOn": true }, { "id": 2, "temperature": 65.5, "fanOn": false } ]'
 *       )
 *    )
 * ) t;
 * </pre>
 */
public class SimpleJsonVTI extends VTITemplate
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

    private JSONArray   _topArray = null;
    private int         _nextIdx = 0;
    private JSONObject  _currentRow = null;
    private boolean     _wasNull = true;

    private Connection  _connection;
    private VTITemplate.ColumnDescriptor[]  _returnColumns;
    private SQLWarning  _warning = null;
    
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Construct from a JSONArray object.
     */
    private SimpleJsonVTI( JSONArray array )
        throws SQLException
    {
        _topArray = (JSONArray) array;
        _connection = DriverManager.getConnection( "jdbc:default:connection" );
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	TABLE FUNCTIONS (to be bound by CREATE FUNCTION ddl)
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Create a SimpleJsonVTI from a JSONArray object.
     * </p>
     *
     * @param array a json array
     *
     * @return a VTI for reading the json array
     * @throws SQLException on error
     */
    public  static  SimpleJsonVTI   readArray( JSONArray array )
        throws SQLException
    {
        return new SimpleJsonVTI( array );
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	ResultSet BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////////////

    public  void    close() throws SQLException
    {
        _connection = null;
        _returnColumns = null;
        _topArray = null;
        _currentRow = null;
    }
    
    public  boolean next()  throws SQLException
    {
        if ( _topArray == null ) { return false; }
        if ( _nextIdx >= _topArray.size() ) { return false; }

        if ( _returnColumns == null ) { _returnColumns = getReturnTableSignature( _connection ); }

        Object obj = _topArray.get( _nextIdx );

        if ( (obj == null) || !(obj instanceof JSONObject) )
        {
            _currentRow = null;
            String  cellType = (obj == null) ? "NULL" : obj.getClass().getName();
            addWarning( "Row " + _nextIdx + " is not a JSON object. It is a " + cellType );
        }
        else
        {
            _currentRow = (JSONObject) obj;
        }

        _nextIdx++;
        return true;
    }

    public boolean wasNull() { return _wasNull; }

    public SQLWarning getWarnings() throws SQLException { return null; }
    public void clearWarnings() throws SQLException { throw notImplemented( "clearWarnings" ); }

    ////////////////////////////////////////////////////////////////////////
    //
    //	TYPE-SPECIFIC ACCESSORS
    //
    ////////////////////////////////////////////////////////////////////////

    public String getString(int columnIndex) throws SQLException
    {
        Object  obj = getColumn( columnIndex );
        if ( _wasNull ) { return null; }

        return obj.toString();
    }
    
    public boolean getBoolean(int columnIndex) throws SQLException
    {
        Object  obj = getColumn( columnIndex );
        if ( nullOrWrongType( Boolean.class, columnIndex, obj ) ) { return false; }

        return ((Boolean) obj).booleanValue();
    }

    public byte getByte(int columnIndex) throws SQLException
    {
        Number  number = getNumber( columnIndex );
        if ( _wasNull ) { return (byte) 0; }

        return number.byteValue();
    }

    public short getShort(int columnIndex) throws SQLException
    {
        Number  number = getNumber( columnIndex );
        if ( _wasNull ) { return (byte) 0; }

        return number.shortValue();
    }

    public int getInt(int columnIndex) throws SQLException
    {
        Number  number = getNumber( columnIndex );
        if ( _wasNull ) { return (byte) 0; }

        return number.intValue();
    }

    public long getLong(int columnIndex) throws SQLException
    {
        Number  number = getNumber( columnIndex );
        if ( _wasNull ) { return (byte) 0; }

        return number.longValue();
    }

    public float getFloat(int columnIndex) throws SQLException
    {
        Number  number = getNumber( columnIndex );
        if ( _wasNull ) { return (byte) 0; }

        return number.floatValue();
    }

    public double getDouble(int columnIndex) throws SQLException
    {
        Number  number = getNumber( columnIndex );
        if ( _wasNull ) { return (byte) 0; }

        return number.doubleValue();
    }

    public Object getObject(int columnIndex) throws SQLException
    {
        return getColumn( columnIndex );
    }
    
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        String  stringValue = getString( columnIndex );
        if ( _wasNull ) { return null; }

        try {
            return new BigDecimal( stringValue );
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        catch (Throwable t) { throw ToolUtilities.wrap( t ); }
    }
    
    ////////////////////////////////////////////////////////////////////////
    //
    //	MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the i-th (1-based) column as a Number.
     * </p>
     */
    private Number  getNumber( int idx )
        throws SQLException
    {
        Object  obj = getColumn( idx );

        if ( nullOrWrongType( Number.class, idx, obj ) ) { return null; }

        return (Number) obj;
    }
    
    /**
     * <p>
     * Get the i-th (1-based) column as an object.
     * </p>
     */
    private Object  getColumn( int idx )
        throws SQLException
    {
        if ( (idx < 1) || (idx > _returnColumns.length) )
        {
            throw new SQLException( "Column index " + idx + " is out of bounds." );
        }

        if ( _currentRow == null )
        {
            _wasNull = true;
            return null;
        }

        Object  value = _currentRow.get( _returnColumns[ idx - 1 ].columnName );

        _wasNull = (value == null);

        return value;
    }

    /**
     * <p>
     * Add a "wrong type" warning and return true if the object
     * has the wrong type. Return true if the object is null.
     * Otherwise, return false.
     * </p>
     */
    private boolean nullOrWrongType( Class correctType, int columnIdx, Object obj )
    {
        if ( _wasNull ) { return true; }
        
        if ( !correctType.isInstance( obj ) )
        {
            String  desiredType = correctType.getName();
            String  valueType = obj.getClass().getName();
            addWarning
                (
                 "Column " + columnIdx +
                 " in row " + _nextIdx +
                 " is not a " + desiredType +
                 ". It is a " + valueType + "."
                 );

            _wasNull = true;
            return true;
        }

        return false;
    }

    /**
     * <p>
     * Add a warning to the connection.
     * </p>
     */
    private void addWarning( String warningText )
    {
        SQLWarning  warning = new SQLWarning( warningText );

        if ( _warning == null ) { _warning = warning; }
        else { _warning.setNextWarning( warning ); }
    }
    
}
