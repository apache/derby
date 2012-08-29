/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate

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

public  class   ModeAggregate    implements  Aggregator<Integer,Integer,ModeAggregate>
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private HashMap<Integer,Accumulator>  _accumulators;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  ModeAggregate() {}
    
    public  void    init()
    {
        _accumulators = new HashMap<Integer,Accumulator>();
    }
    public  void    accumulate( Integer value )
    {
        getAccumulator( value ).add( 1 );
    }
    public  void    merge( ModeAggregate otherAggregator )
    {
        HashMap<Integer,Accumulator>  otherAccumulators = otherAggregator._accumulators;
        
        for ( Integer value : otherAccumulators.keySet() )
        {
            getAccumulator( value ).add( otherAccumulators.get( value ).getCount() );
        }
    }

    public  Integer terminate()
    {
        int     numAccumulators = _accumulators.size();
        if ( numAccumulators == 0 ) { return null; }
        
        Accumulator[]   accumulators = new Accumulator[ numAccumulators ];

        accumulators = _accumulators.values().toArray( accumulators );
        Arrays.sort( accumulators );
        
        return accumulators[ numAccumulators - 1 ].getValue();
    }

    private Accumulator   getAccumulator( Integer value )
    {
        Accumulator   retval = _accumulators.get( value );
        if ( retval == null )
        {
            retval = new Accumulator( value );
            _accumulators.put( value, retval );
        }

        return retval;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   Accumulator implements  Comparable<Accumulator>
    {
        private Integer _value;
        private int         _count;

        public  Accumulator( Integer value )
        {
            _value = value;
            _count = 0;
        }

        public  void    add( int increment ) { _count += increment; }

        public  Integer getValue() { return _value; }
        public  int     getCount() { return _count; }

        // Comparable behavior
        public  int compareTo( Accumulator that )
        {
            if ( that == null ) { return 1; }

            int retval = this._count - that._count;

            if ( retval != 0 ) { return retval; }
            else { return this._value.intValue() - that._value.intValue(); }
        }
    }
}
