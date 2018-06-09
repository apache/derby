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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.derby.agg.Aggregator;

/**
 * <p>
 * This is a generic mode aggregator for testing with many types.
 * </p>
 */
public  class   GenericMode<B extends Comparable<B>>    implements  Aggregator<B,B,GenericMode<B>>
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   BigDecimalMode extends GenericMode<BigDecimal> {}
    public  static  final   class   BooleanMode extends GenericMode<Boolean> {}
    public  static  final   class   ShortMode extends GenericMode<Short> {}
    public  static  final   class   IntMode extends GenericMode<Integer> {}
    public  static  final   class   BigintMode extends GenericMode<Long> {}
    public  static  final   class   DoubleMode extends GenericMode<Double> {}
    public  static  final   class   RealMode extends GenericMode<Float> {}
    public  static  final   class   StringMode extends GenericMode<String> {}
    public  static  final   class   FullNameMode extends GenericMode<FullName> {}
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private HashMap<B,Accumulator<B>>  _accumulators;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  GenericMode() {}
    
    public  void    init()
    {
        _accumulators = new HashMap<B,Accumulator<B>>();
    }
    public  void    accumulate( B value )
    {
        getAccumulator( value ).add( 1 );
    }
    public  void    merge( GenericMode<B> otherAggregator )
    {
        HashMap<B,Accumulator<B>>  otherAccumulators = otherAggregator._accumulators;
        
        for ( B value : otherAccumulators.keySet() )
        {
            getAccumulator( value ).add( otherAccumulators.get( value ).getCount() );
        }
    }

    public  B terminate()
    {
        return _accumulators.isEmpty() ? null : Collections.max( _accumulators.values() ).getValue();
    }

    private Accumulator<B>   getAccumulator( B value )
    {
        Accumulator<B>   retval = _accumulators.get( value );
        if ( retval == null )
        {
            retval = new Accumulator<B>( value );
            _accumulators.put( value, retval );
        }

        return retval;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   Accumulator<B extends Comparable<B>> implements  Comparable<Accumulator<B>>
    {
        private B _value;
        private int         _count;

        public  Accumulator( B value )
        {
            _value = value;
            _count = 0;
        }

        public  void    add( int increment ) { _count += increment; }

        public  B getValue() { return _value; }
        public  int     getCount() { return _count; }

        // Comparable behavior
        public  int compareTo( Accumulator<B> that )
        {
            int retval = this._count - that._count;

            if ( retval != 0 ) { return retval; }
            else { return this._value.compareTo( that._value ); }
        }
    }
}
