/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.IntArray

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
 * A UDT which contains an array of ints.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectInput;

public class IntArray implements Externalizable, Comparable
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

    private int[] _data;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public IntArray() {}

    public IntArray( int[] data )
    {
        _data = data;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static IntArray makeIntArray( int... values )
    {
        return new IntArray( values );
    }

    public static IntArray makeIntArray( int length )
    {
        return new IntArray( new int[ length ] );
    }

    public static IntArray setCell( IntArray array, int cellNumber, int cellValue )
    {
        array._data[ cellNumber ] = cellValue;

        return array;
    }

    public static int getCell( IntArray array, int cellNumber ) { return array._data[ cellNumber ]; }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Externalizable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public void writeExternal( ObjectOutput out ) throws IOException
    {
        int length = _data.length;

        out.writeInt( length );

        for ( int i = 0; i < length; i++ ) { out.writeInt( _data[ i ] ); }
    }

    public void readExternal( ObjectInput in ) throws IOException
    {
        int length = in.readInt();

        _data = new int[ length ];

        for ( int i = 0; i < length; i++ ) { _data[ i ] = in.readInt(); }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Comparable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public int compareTo( Object other )
    {
        if ( other == null ) { return -1; }
        if ( !( other instanceof IntArray) ) { return -1; }

        IntArray that = (IntArray) other;

        int minLength = (this._data.length <= that._data.length) ? this._data.length : that._data.length;

        int result;
        for ( int i = 0; i < minLength; i++ )
        {
            result = this._data[ i ] - that._data[ i ];

            if ( result != 0 ) { return result; }
        }

        result = this._data.length - that._data.length;

        return result;
    }

    public boolean equals( Object other ) { return ( compareTo( other ) == 0 ); }

    public int hashCode()
    {
        int firstValue;
        int secondValue;

        if ( _data.length== 0 )
        {
            firstValue = 1;
            secondValue = 1;
        }
        else
        {
            firstValue = _data[ 0 ];
            secondValue = _data[ _data.length -1 ];
        }

        return firstValue^secondValue;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER Object OVERRIDES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public String toString()
    {
        StringBuffer buffer = new StringBuffer();
        int length = _data.length;

        buffer.append( "[ " );
        for ( int i = 0; i < length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( _data[ i ] );
        }
        buffer.append( " ]" );

        return buffer.toString();
    }
}
