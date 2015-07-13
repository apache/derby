/*

   Derby - Class org.apache.derby.optional.api.SimpleJsonUtils

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

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.derby.iapi.util.StringUtil;

/**
 * <p>
 * Utility methods for simple JSON support.
 * </p>
 */
public abstract class SimpleJsonUtils
{
    /////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////
    //
    //  PUBLIC BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Pack a ResultSet into a JSONArray. This method could be called
     * client-side on any query result from any DBMS. Each row is
     * converted into a JSONObject whose keys are the corresponding
     * column names from the ResultSet.
     * Closes the ResultSet once it has been drained. Datatypes map
     * to JSON values as follows:
     * </p>
     *
     * <ul>
     * <li><i>NULL</i> - The JSON null literal.</li>
     * <li><i>SMALLINT, INT, BIGINT</i> - JSON integer values.</li>
     * <li><i>DOUBLE, FLOAT, REAL, DECIMAL, NUMERIC</i> - JSON floating point values.</li>
     * <li><i>CHAR, VARCHAR, LONG VARCHAR, CLOB</i> - JSON string values.</li>
     * <li><i>BLOB, VARCHAR FOR BIT DATA, LONG VARCHAR FOR BIT DATA</i> - The
     * byte array is turned into a hex string (2 hex digits per byte) and the
     * result is returned as a JSON string.</li>
     * <li><i>All other types</i> - Converted to JSON
     * string values via their toString() methods.</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    public  static  JSONArray   toJSON( ResultSet rs )
        throws SQLException
    {
        ResultSetMetaData   rsmd = rs.getMetaData();
        int                 columnCount = rsmd.getColumnCount();
        JSONArray           result = new JSONArray();

        try {
            while( rs.next() )
            {
                JSONObject  row = new JSONObject();

                for ( int i = 1; i <= columnCount; i++ )
                {
                    String  keyName = rsmd.getColumnName( i );
                    Object  value = getLegalJsonValue( rs.getObject( i ) );

                    row.put( keyName, value );
                }

                result.add( row );
            }
        }
        finally
        {
            if ( rs != null )
            {
                rs.close();
            }
        }

        return result;
    }

    /////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Turns an object into something which is a legal JSON value.
     * </p>
     */
    private static  Object  getLegalJsonValue( Object obj )
        throws SQLException
    {
        if (
            (obj == null) ||
            (obj instanceof Long) ||
            (obj instanceof Double) ||
            (obj instanceof Boolean) ||
            (obj instanceof String) ||
            (obj instanceof JSONObject) ||
            (obj instanceof JSONArray)
            )
        {
            return obj;
        }
        // other exact integers
        else if (
                 (obj instanceof Byte) ||
                 (obj instanceof Short) ||
                 (obj instanceof Integer)
                 )
        {
            return ((Number) obj).longValue();
        }
        // all other numbers, including BigDecimal
        else if (obj instanceof Number) { return ((Number) obj).doubleValue(); }
        else if (obj instanceof Clob)
        {
            Clob    clob = (Clob) obj;
            return clob.getSubString( 1, (int) clob.length() );
        }
        else if (obj instanceof Blob)
        {
            Blob    blob = (Blob) obj;
            return formatBytes( blob.getBytes( 1, (int) blob.length() ) );
        }
        if (obj instanceof byte[])
        {
            return formatBytes( (byte[]) obj );
        }
        // catch-all
        else { return obj.toString(); }
    }

    private static  String  formatBytes( byte[] bytes )
    {
        return StringUtil.toHexString( bytes, 0, bytes.length );
    }

    private static  Connection  getDerbyConnection() throws SQLException
    {
        return DriverManager.getConnection( "jdbc:default:connection" );
    }
}
