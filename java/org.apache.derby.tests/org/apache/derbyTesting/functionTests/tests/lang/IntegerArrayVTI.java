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
    private static int _lastQualifedRowCount;

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
    // ResultSet OVERRIDES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public boolean next() throws SQLException
    {
        while ( true )
        {
            boolean anotherRow = super.next();
            if ( !anotherRow ) { return false; }

            if ( qualifyCurrentRow() )
            {
                _lastQualifedRowCount++;
                return true;
            }
        }
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
        _lastQualifedRowCount = 0;
    }

    // Return true if the qualification succeeds on the current row
    private boolean qualifyCurrentRow() throws SQLException
    {
        if ( _lastRestriction == null ) { return true; }

        return qualifyCurrentRow( _lastRestriction );
    }
    private boolean qualifyCurrentRow( Restriction restriction ) throws SQLException
    {
        if ( restriction instanceof Restriction.AND )
        {
            Restriction.AND and = (Restriction.AND) restriction;

            return qualifyCurrentRow( and.getLeftChild() ) && qualifyCurrentRow( and.getRightChild() );
        }
        else if ( restriction instanceof Restriction.OR )
        {
            Restriction.OR or = (Restriction.OR) restriction;

            return qualifyCurrentRow( or.getLeftChild() ) || qualifyCurrentRow( or.getRightChild() );
        }
        else if ( restriction instanceof Restriction.ColumnQualifier )
        {
            return applyColumnQualifier( (Restriction.ColumnQualifier) restriction );
        }
        else { throw new SQLException( "Unknown type of Restriction: " + restriction.getClass().getName() ); }
    }
    private boolean applyColumnQualifier( Restriction.ColumnQualifier qc ) throws SQLException
    {
        int operator = qc.getComparisonOperator();
        int column = getInt( qc.getColumnName() );
        boolean columnWasNull = wasNull();

        if ( columnWasNull )
        {
            if ( operator == Restriction.ColumnQualifier.ORDER_OP_ISNULL ) { return true; }
            else if ( operator == Restriction.ColumnQualifier.ORDER_OP_ISNOTNULL ) { return false; }
            else { return false; }
        }
        else if ( operator == Restriction.ColumnQualifier.ORDER_OP_ISNULL ) { return false; }
        else if ( operator == Restriction.ColumnQualifier.ORDER_OP_ISNOTNULL ) { return true; }

        int constant = ((Integer) qc.getConstantOperand()).intValue();

        switch ( operator )
        {
        case Restriction.ColumnQualifier.ORDER_OP_EQUALS: return ( column == constant );
        case Restriction.ColumnQualifier.ORDER_OP_GREATEROREQUALS: return ( column >= constant );
        case Restriction.ColumnQualifier.ORDER_OP_GREATERTHAN: return ( column > constant );
        case Restriction.ColumnQualifier.ORDER_OP_LESSOREQUALS: return ( column <= constant );
        case Restriction.ColumnQualifier.ORDER_OP_LESSTHAN: return ( column < constant );
        default: throw new SQLException( "Unknown comparison operator: " + operator );
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static String getLastProjection() { return ( (_lastProjection == null) ? null : Arrays.asList( _lastProjection ).toString() ); }
    public static String getLastRestriction() { return ( ( _lastRestriction == null ) ? null : _lastRestriction.toSQL() ); }
    public static int getLastQualifiedRowCount() { return _lastQualifedRowCount; }
    
}
