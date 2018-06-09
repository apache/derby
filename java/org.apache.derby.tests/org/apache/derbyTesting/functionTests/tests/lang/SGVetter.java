/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SequenceGeneratorTest

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

import java.math.BigInteger;

/**
 * <p>
 * Machine to validate the operation of the sequence generator. This is a
 * re-implementation of the sequence generator in a less efficient style whose
 * correctness is easier to reason about.
 * </p>
 */
public class SGVetter
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final long MINIMUM_CACHED_VALUE_COUNT = 1L;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private boolean _CAN_CYCLE;
    private BigInteger _STEP;
    private BigInteger _MAX;
    private BigInteger _MIN;
    private BigInteger _RESTART;
    private long _ALLOCATION_COUNT;
    private boolean _INCREASING;

    private BigInteger _currentValue;
    private long        _valuesRemaining;
    private BigInteger _upperBound;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public SGVetter
        (
         Long currentValue,
         boolean canCycle,
         long step,
         long max,
         long min,
         long restart,
         long allocationCount
         )
        throws Exception
    {
        if ( step >= max ) { throw new IllegalArgumentException(); }
        if ( step <= min ) { throw new IllegalArgumentException(); }
        if ( restart > max ) { throw new IllegalArgumentException(); }
        if ( restart < min ) { throw new IllegalArgumentException(); }

        if ( currentValue != null )
        {
            if ( currentValue.longValue() > max ) { throw new IllegalArgumentException(); }
            if ( currentValue.longValue() < min ) { throw new IllegalArgumentException(); }
        }

        if ( currentValue == null ) { _currentValue = null; }
        else { _currentValue = BigInteger.valueOf( currentValue.longValue() ); }
        
        _CAN_CYCLE = canCycle;
        _STEP = BigInteger.valueOf( step );
        _MAX = BigInteger.valueOf( max );
        _MIN = BigInteger.valueOf( min );
        _RESTART = BigInteger.valueOf( restart );
        _ALLOCATION_COUNT = allocationCount;
        _INCREASING = (_STEP.compareTo( BigInteger.valueOf( 0L ) ) > 0);

        _upperBound = _currentValue;
        _valuesRemaining = MINIMUM_CACHED_VALUE_COUNT;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the next value in the sequence. Returns null if the sequence is exhausted.
     * </p>
     */
    public Long getNextValue()
    {
        if ( _currentValue == null ) { return null; }

        BigInteger retval = cloneBigInteger( _currentValue );

        advance();

        return retval.longValue();
    }

    /** Get the upper bound */
    public Long getUpperBound()
    {
        if ( _upperBound == null ) { return null; }
        else { return _upperBound.longValue(); }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // LOGIC TO ADVANCE THE SEQUENCE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private void advance()
    {
        BigInteger newValue = bump( _currentValue );

        boolean overflowed = ( newValue.compareTo( _currentValue ) == 0 );

        if ( overflowed && !_CAN_CYCLE )
        {
            _currentValue = null;
            _upperBound = null;
            return;
        }

        _valuesRemaining--;
        if ( _valuesRemaining < MINIMUM_CACHED_VALUE_COUNT )
        {
            for ( long i = 0; i < _ALLOCATION_COUNT; i++ )
            {
                _upperBound = bump( _upperBound );
            }

            _valuesRemaining = _ALLOCATION_COUNT;
        }

        _currentValue = newValue;
    }

    // increment the original value. if this overflows and cycling is not allowed
    // return the original value.
    private BigInteger bump( BigInteger original )
    {
        BigInteger newValue = original.add( _STEP );

        if ( overflowed( newValue ) )
        {
            if ( !_CAN_CYCLE ) { newValue = cloneBigInteger( original ); }
            else { newValue = _RESTART; }
        }

        return newValue;
    }

    private boolean overflowed( BigInteger newValue )
    {
        boolean overflowed = _INCREASING ? ( newValue.compareTo( _MAX ) > 0 ) : ( newValue.compareTo( _MIN ) < 0 );

        return overflowed;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private BigInteger cloneBigInteger( BigInteger original )
    {
        return new BigInteger( original.toByteArray() );
    }
}

