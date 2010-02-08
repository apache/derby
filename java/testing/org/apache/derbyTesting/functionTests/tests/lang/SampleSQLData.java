/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SampleSQLData

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

/**
 * A simple SQLData class.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;

public class SampleSQLData implements SQLData
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

    private byte[] _data;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public SampleSQLData() {}

    public SampleSQLData( byte[] data )
    {
        _data = data;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static SampleSQLData makeSampleSQLData( int length )
    {
        return new SampleSQLData( new byte[ length ] );
    }

    public static String toString( SampleSQLData data ) { return data._data.toString(); }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SQLData BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public String getSQLTypeName() { return "SampleSQLData"; }
    
    public void writeSQL( SQLOutput out ) throws SQLException
    {
        out.writeBytes( _data );
    }

    public void readSQL( SQLInput in, String typeName ) throws SQLException
    {
        _data = in.readBytes();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER Object OVERRIDES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public String toString()
    {
        return "SampleSQLData( " + _data.length + " )";
    }
}
