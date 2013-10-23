/*

   Derby - Class org.apache.derby.iapi.types.LocatedRow

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

package org.apache.derby.iapi.types;

/**
 * <p>
 * Mutable holder for the column values and RowLocation of a conglomerate row.
 * Use with caution because values and arrays are not copied when they
 * are passed in and out.
 * </p>
 */
public class LocatedRow
{
    ////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ////////////////////////////////////////////////////////////////

    private DataValueDescriptor[]   _columnValues;
    private RowLocation                 _rowLocation;

    ////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Construct from column values and row location.
     * </p>
     */
    public  LocatedRow
        (
         DataValueDescriptor[]  columnValues,
         RowLocation                rowLocation
         )
    {
        _columnValues = columnValues;
        _rowLocation = rowLocation;
    }

    /**
     * <p>
     * Construct from an array of column values, where the last value
     * is the RowLocation.
     * </p>
     */
    public  LocatedRow( DataValueDescriptor[] columnsAndRowLocation )
    {
        int     columnCount = columnsAndRowLocation.length - 1;
        int     idx = 0;

        _columnValues = new DataValueDescriptor[ columnCount ];
        for ( ; idx < columnCount; idx++ )
        { _columnValues[ idx ] = columnsAndRowLocation[ idx ]; }
        _rowLocation = (RowLocation) columnsAndRowLocation[ idx ];
    }

    ////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ////////////////////////////////////////////////////////////////

    /** Get the array of column values */
    public  DataValueDescriptor[]   columnValues() { return _columnValues; }

    /**
     * Flatten this LocatedRow into a DataValueDescriptor[] where the last cell
     * contains the RowLocation.
     */
    public  DataValueDescriptor[]   flatten()
    {
        return flatten( _columnValues, _rowLocation );
    }

    /** Get the RowLocation */
    public  RowLocation rowLocation() { return _rowLocation; }

    ////////////////////////////////////////////////////////////////
    //
    // STATIC BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////

    /** Append a RowLocation to the end of a column array */
    public  static  DataValueDescriptor[]   flatten
        ( DataValueDescriptor[] columnValues, RowLocation rowLocation )
    {
        DataValueDescriptor[]   result =
            new DataValueDescriptor[ columnValues.length + 1 ];
        int                             idx = 0;

        for ( ; idx < columnValues.length; idx++ )
        { result[ idx ] = columnValues[ idx ]; }
        result[ idx ] = rowLocation;

        return result;
    }

}
