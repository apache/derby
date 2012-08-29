/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GenericMode

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

import java.util.Arrays;
import java.util.HashMap;

import org.apache.derby.agg.Aggregator;

public  class   GenericMode<V extends Comparable<V>>    implements  Aggregator<V,V,GenericMode<V>>
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   IntMode extends GenericMode<Integer> {}
    public  static  final   class   StringMode extends GenericMode<String> {}
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private HashMap<V,Accumulator<V>>  _accumulators;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  GenericMode() {}
    
    public  void    init()
    {
        _accumulators = new HashMap<V,Accumulator<V>>();
    }
    public  void    accumulate( V value )
    {
        getAccumulator( value ).add( 1 );
    }
    public  void    merge( GenericMode<V> otherAggregator )
    {
        HashMap<V,Accumulator<V>>  otherAccumulators = otherAggregator._accumulators;
        
        for ( V value : otherAccumulators.keySet() )
        {
            getAccumulator( value ).add( otherAccumulators.get( value ).getCount() );
        }
    }

    // Generic arrays can't be created, so we have to cast the value on the way out.
    // Suppress the resulting compiler warning.
    @SuppressWarnings("unchecked")
    public  V terminate()
    {
        int     numAccumulators = _accumulators.size();
        if ( numAccumulators == 0 ) { return null; }
        
        Accumulator[]   accumulators = new Accumulator[ numAccumulators ];

        accumulators = _accumulators.values().toArray( accumulators );
        Arrays.sort( accumulators );
        
        return (V) accumulators[ numAccumulators - 1 ].getValue();
    }

    private Accumulator<V>   getAccumulator( V value )
    {
        Accumulator<V>   retval = _accumulators.get( value );
        if ( retval == null )
        {
            retval = new Accumulator<V>( value );
            _accumulators.put( value, retval );
        }

        return retval;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   Accumulator<V extends Comparable<V>> implements  Comparable<Accumulator<V>>
    {
        private V _value;
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
            if ( that == null ) { return 1; }

            int retval = this._count - that._count;

            if ( retval != 0 ) { return retval; }
            else { return this._value.compareTo( that._value ); }
        }
    }
}
