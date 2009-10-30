/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.IntegerArrayVTI

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.util.Arrays;

import org.apache.derby.vti.RestrictedVTI;
import org.apache.derby.vti.Restriction;

/**
 * A VTI which returns a row of ints.
 */
public class IntegerArrayVTI extends StringArrayVTI implements RestrictedVTI
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static String[] _lastProjection;
    private static Restriction _lastRestriction;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public IntegerArrayVTI( String[] columnNames, int[][] rows )
    {
        super( columnNames, stringify( rows ) );
    }
    public IntegerArrayVTI( String[] columnNames, Integer[][] rows )
    {
        super( columnNames, stringify( rows ) );
    }
    private static String[][] stringify( int[][] rows )
    {
        int outerCount = rows.length;

        String[][] retval = new String[ outerCount ][];

        for ( int i = 0; i < outerCount; i++ )
        {
            int[] rawRow = rows[ i ];
            int innerCount = rawRow.length;
            String[] row = new String[ innerCount ];
            
            retval[ i ] = row;

            for ( int j = 0; j < innerCount; j++ )
            {
                row[ j ] = Integer.toString( rawRow[ j ] );
            }
        }

        return retval;
    }
    private static String[][] stringify( Integer[][] rows )
    {
        int outerCount = rows.length;

        String[][] retval = new String[ outerCount ][];

        for ( int i = 0; i < outerCount; i++ )
        {
            Integer[] rawRow = rows[ i ];
            int innerCount = rawRow.length;
            String[] row = new String[ innerCount ];
            
            retval[ i ] = row;

            for ( int j = 0; j < innerCount; j++ )
            {
                Integer raw = rawRow[ j ];
                String value = raw == null ? null : raw.toString();
                row[ j ] = value;
            }
        }

        return retval;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // RestrictedVTI BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public void initScan( String[] columnNames, Restriction restriction ) throws SQLException
    {
        _lastProjection = columnNames;
        _lastRestriction = restriction;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static String getLastProjection() { return ( (_lastProjection == null) ? null : Arrays.asList( _lastProjection ).toString() ); }
    public static String getLastRestriction() { return ( ( _lastRestriction == null ) ? null : _lastRestriction.toSQL() ); }
    
}
