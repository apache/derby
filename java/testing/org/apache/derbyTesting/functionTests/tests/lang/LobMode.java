/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LobMode

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

import java.sql.Clob;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.derby.agg.Aggregator;

/**
 * <p>
 * This is a version of GenericMode for use with types which are not Comparable.
 * The class started out for use with Blob and Clob but was pressed into service
 * for other types also.
 * </p>
 *
 * <p>
 * In particular, this is a mode aggregator for use with the
 * JDBC date/time classes too. You can't use GenericMode with those types
 * because they do not satisfy its type bounds. That is because they inherit the
 * Comparable implementation of java.util.Date rather than implementing
 * their own more specific version of Comparable. That is,
 * java.sql.Date implements Comparable<java.util.Date> rather than
 * Comparable<java.sql.Date>.
 * </p>
 */
public  class   LobMode<V>    implements  Aggregator<V,V,LobMode<V>>
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   ClobMode extends LobMode<Clob> {}
    public  static  final   class   BlobMode extends LobMode<Blob> {}
    public  static  final   class   BinaryMode extends LobMode<byte[]> {}
    public  static  final   class   DateMode extends LobMode<Date> {}
    public  static  final   class   TimeMode extends LobMode<Time> {}
    public  static  final   class   TimestampMode extends LobMode<Timestamp> {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private HashMap<String,Accumulator<V>>  _accumulators;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  LobMode() {}
    
    public  void    init()
    {
        _accumulators = new HashMap<String,Accumulator<V>>();
    }
    public  void    accumulate( V value )
    {
        getAccumulator( value ).add( 1 );
    }
    public  void    merge( LobMode<V> otherAggregator )
    {
        HashMap<String,Accumulator<V>>  otherAccumulators = otherAggregator._accumulators;
        
        for ( Accumulator<V> accumulator : otherAccumulators.values() )
        {
            V   value = accumulator.getValue();
            getAccumulator( value ).add( accumulator.getCount() );
        }
    }

    public  V terminate()
    {
        return _accumulators.isEmpty() ? null : Collections.max( _accumulators.values() ).getValue();
    }

    private Accumulator<V>   getAccumulator( V value )
    {
        String      key = toString( value );
        
        Accumulator<V>   retval = _accumulators.get( key );
        if ( retval == null )
        {
            retval = new Accumulator<V>( value );
            _accumulators.put( key, retval );
        }

        return retval;
    }

    static  String  toString( Object raw )
    {
        try {
            if ( raw instanceof Clob )
            {
                Clob    clob = (Clob) raw;

                return clob.getSubString( 1L, (int) clob.length() );
            }
            else if ( raw instanceof Blob )
            {
                Blob    blob = (Blob) raw;
                byte[]  bytes = blob.getBytes( 1L, (int) blob.length() );

                return new String( bytes, "UTF-8" );
            }
            else if ( raw instanceof Date ) { return raw.toString(); }
            else if ( raw instanceof Time ) { return raw.toString(); }
            else if ( raw instanceof Timestamp ) { return raw.toString(); }
            else if ( raw instanceof byte[] )
            {
                byte[]  bytes = (byte[]) raw;

                return new String( bytes, "UTF-8" );
            }
        } catch (Exception e) { throw new IllegalArgumentException( e ); }

        throw new IllegalArgumentException( "Unsupported object type: " + raw.getClass().getName() );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   Accumulator<V> implements  Comparable<Accumulator<V>>
    {
        private V       _value;
        private int         _count;

        public  Accumulator( V value )
        {
            _value = value;
            _count = 0;
        }

        public  void    add( int increment ) { _count += increment; }

        public  V getValue() { return _value; }
        public  int     getCount() { return _count; }

        // Comparable behavior
        public  int compareTo( Accumulator<V> that )
        {
            int retval = this._count - that._count;

            if ( retval != 0 ) { return retval; }
            else { return LobMode.toString( this._value ).compareTo( LobMode.toString( that._value ) ); }
        }
    }
}
