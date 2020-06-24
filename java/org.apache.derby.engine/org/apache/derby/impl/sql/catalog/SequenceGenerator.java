/*

   Derby - Class org.apache.derby.impl.sql.catalog.SequenceGenerator

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
package org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.SequencePreallocator;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * <p>
 * This is a generic machine for pre-allocating ranges of sequence numbers in order
 * to improve concurrency. The public methods are synchronized and should be brief.
 * The caller of the methods in this class is responsible for updating values on disk
 * when the generator is exhausted or when it needs to allocate a new range of values.
 * </p>
 *
 * <p>
 * The most used method in this class is getCurrentValueAndAdvance(). This method
 * returns the next number in the range managed by the sequence generator. This
 * method will raise an exception if the sequence generator is exhausted. Otherwise
 * getCurrentValueAndAdvance() hands back a tuple of return values:
 * </p>
 *
 * <blockquote>
 * ( <i>status, currentValue, lastAllocatedValue, numberOfValuesAllocated</i> )
 * </blockquote>
 *
 * <p>
 * The <i>status</i> field takes the following values:
 * </p>
 *
 * <ul>
 * <li><b>RET_I_AM_CONFUSED</b> - This value should never be returned. If this value comes back,
 * then the sequence generator is confused.</li>
 * <li><b>RET_OK</b> - This means that the generator has successfully obtained a
 * next value. That value is <i>currentValue</i>.</li>
 * <li><b>RET_MARK_EXHAUSTED</b> - This means that the generator has reached the end of its
 * legal range and is handing back its very last value. The caller must mark the catalogs
 * to indicate that the range is exhausted. The very last value being handed back
 * is <i>currentValue</i>.</li>
 * <li><b>RET_ALLOCATE_NEW_VALUES</b> - This means that the generator has come to the end
 * of its pre-allocated values. The caller needs to update the catalog to grab a new range of
 * legal values and then call allocateNewRange() to tell the generator that the range was
 * successfully allocated. The remaining values in the return tuple have these meanings:
 *  <ul>
 *  <li><i>currentValue</i> - This is what is expected to be the current value in the catalog before
 *   allocating a new range. If, in fact, this is not the value in the catalog, then we are racing with
 *   another session to drain values from the generator and update the disk. Do not update
 *   the catalog if the value there is not <i>currentValue</i>. Instead, assume that another session
 *   got in ahead of us and grabbed a new range of values. Simply call getCurrentValueAndAdvance()
 *   again.</li>
 *  <li><i>lastAllocatedValue</i> - This is the next value to write to the catalog.</li>
 *  <li><i>numberOfValuesAllocated</i> - This is the number of values which were allocated
 *   if we successfully updated the catalog. If we successfully updated the catalog, then we
 *   should call allocateNewRange(), handing it this value so that it can reset its range. As a
 *   sanity check, we also hand allocateNewRange() the <i>currentValue</i> that we were
 *   given. The allocateNewRange() method will assume we're racing another session and will
 *   ignore us if its sense of <i>currentValue</i> does not agree with ours.</li>
 *  </ul>
 * </li>
 * </ul>
 *
 *
 * <p>
 * It may happen that getCurrentValueAndAdvance() tells its caller to allocate a new
 * range of sequence numbers in the system catalog. If the caller successfully allocates
 * a new range, the caller should call allocateNewRange() to tell the generator to update
 * its internal memory of that range.
 * </p>
 *
 *
 * <p>
 * The peekAtCurrentValue() method is provided so that unused, pre-allocated values can
 * be flushed when the sequence generator is being discarded. The caller updates the
 * catalog with the value returned by peekAtCurrentValue(). The peekAtCurrentValue() method
 * is also called by the syscs_peek_at_sequence() function which users should call rather
 * than try to scan the underlying catalog themselves.
 * </p>
 *
 */
public class SequenceGenerator
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** If pre-allocation drops below this level, then we need to grab another chunk of numbers */
    private static final int PREALLOCATION_THRESHHOLD = 1;

    //
    // Return values from dispatchCommand().
    //
    public static final int RET_I_AM_CONFUSED = 0; // should never see this
    public static final int RET_OK = RET_I_AM_CONFUSED + 1;
    public static final int RET_MARK_EXHAUSTED = RET_OK + 1;
    public static final int RET_ALLOCATE_NEW_VALUES = RET_MARK_EXHAUSTED + 1;

    //
    // Offsets into array of longs returned by getCurrentValueAndAdvance()
    //
    public static final int CVAA_STATUS = 0;
    public static final int CVAA_CURRENT_VALUE = CVAA_STATUS + 1;
    public static final int CVAA_LAST_ALLOCATED_VALUE = CVAA_CURRENT_VALUE + 1;
    public static final int CVAA_NUMBER_OF_VALUES_ALLOCATED = CVAA_LAST_ALLOCATED_VALUE + 1;
    public static final int CVAA_LENGTH = CVAA_NUMBER_OF_VALUES_ALLOCATED + 1;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANT STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    //
    // The following state is initialized when this SequenceGenerator is created.
    //

    // True if the generator can wrap-around, that is, if the generator was declared to CYCLE.
    private final boolean _CAN_CYCLE;

    // True if the increment value is positive.
    private final boolean _STEP_INCREASES;

    // This is the step-size for the generator. It is non-zero. It is positive for
    // generators which increment and it is negative for generators which decrement.
    private final long _INCREMENT;

    // This is the highest (most positive) value which the generator is willing to hand out.
    private final long _MAX_VALUE;

    // This is lowest (most negative) value which the generator is willing to hand out.
    private final long _MIN_VALUE;

    // This is where we restart the sequence if we wrap around.
    private final long _RESTART_VALUE;

    // Name of the schema that the sequence lives in.
    private final String _SCHEMA_NAME;

    // Name of the sequence.
    private final String _SEQUENCE_NAME;

    // Logic to determine how many values to pre-allocate
    private final   SequencePreallocator    _PREALLOCATOR;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // VARIABLES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // True if the generator can not produce any more values.
    private boolean _isExhausted;

    // This is the next value which the generator will hand out.
    private long _currentValue;

    // This is the remaining number of values which were pre-allocated on disk
    // by bumping the contents of SYSSEQUENCES.CURRENTVALUE.
    private long _remainingPreallocatedValues;


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Normal constructor */
    public SequenceGenerator
        (
         Long currentValue,
         boolean canCycle,
         long increment,
         long maxValue,
         long minValue,
         long restartValue,
         String schemaName,
         String sequenceName,
         SequencePreallocator   sequencePreallocator
         )
    {
        if ( currentValue == null )
        {
            _isExhausted = true;
            _currentValue = 0;
        }
        else
        {
            _isExhausted = false;
            _currentValue = currentValue.longValue();
        }

        _CAN_CYCLE = canCycle;
        _INCREMENT = increment;
        _MAX_VALUE = maxValue;
        _MIN_VALUE = minValue;
        _RESTART_VALUE = restartValue;
        _STEP_INCREASES = ( _INCREMENT > 0 );
        _SCHEMA_NAME = schemaName;
        _SEQUENCE_NAME = sequenceName;
        _PREALLOCATOR = sequencePreallocator;

        //
        // Next call to getCurrentValueAndAdvance() will cause  us to ask our caller to allocate a new range of values.
        //
        _remainingPreallocatedValues = 1L;
    }

    /**
     * <p>
     * Clone this sequence generator. This method supports the special bulk-insert optimization in
     * InsertResultSet.
     * </p>
     *
     * @param restart   True if the clone should be reset to start at the beginning instead of at the current value.
     */
    public synchronized SequenceGenerator clone( boolean restart )
    {
        Long    startValue;

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        if ( restart ) { startValue = _RESTART_VALUE; }
        else if ( _isExhausted ) { startValue = null; }
        else { startValue = _currentValue; }

        return new SequenceGenerator
            (
             startValue,
             _CAN_CYCLE,
             _INCREMENT,
             _MAX_VALUE,
             _MIN_VALUE,
             _RESTART_VALUE,
             _SCHEMA_NAME,
             _SEQUENCE_NAME,
             _PREALLOCATOR
             );
    }
    
    /**
     * <p>
     * Clone this sequence generator. This method supports the special bulk-insert optimization in
     * InsertResultSet.
     * </p>
     *
     * @param newStartValue New value to start with.
     */
    public synchronized SequenceGenerator clone( Long newStartValue )
    {
        return new SequenceGenerator
            (
             newStartValue,
             _CAN_CYCLE,
             _INCREMENT,
             _MAX_VALUE,
             _MIN_VALUE,
             _RESTART_VALUE,
             _SCHEMA_NAME,
             _SEQUENCE_NAME,
             _PREALLOCATOR
             );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the name of the schema of this sequence generator. Technically, this doesn't need to be
     * synchronized. But it is simpler to just maintain a rule that all public methods
     * should be synchronized.
     * </p>
     */
    public synchronized String getSchemaName() { return _SCHEMA_NAME; }
    
    /**
     * <p>
     * Get the name of this sequence generator. Technically, this doesn't need to be
     * synchronized. But it is simpler to just maintain a rule that all public methods
     * should be synchronized.
     * </p>
     */
    public synchronized String getName() { return _SEQUENCE_NAME; }
    
    /**
     * <p>
     * Allocate a new range. Is a NOP if the current value is not what we expected. See the
     * class header comment for more information on how this method is used.
     * </p>
     */
    public synchronized void allocateNewRange( long expectedCurrentValue, long numberOfAllocatedValues )
    {
        if ( _currentValue == expectedCurrentValue )
        {
            _remainingPreallocatedValues = numberOfAllocatedValues;
        }
    }
     
    /**
     * <p>
     * Peek at the current value of the sequence generator without advancing the
     * generator. Returns null if the generator is exhausted.
     * </p>
     */
    public synchronized Long peekAtCurrentValue()
    {
        Long currentValue = null;

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        if ( !_isExhausted ) { currentValue = _currentValue; }
        
        return currentValue;
    }
    
    /**
     * <p>
     * Get the next sequence number managed by this generator and advance the number. Could raise an
     * exception if the legal range is exhausted and wrap-around is not allowed--that is,
     * if NO CYCLE was specified when the sequence was defined. See the class header comment
     * for a description of how this method operates.
     * </p>
     *
     * @return Returns an array of longs indexed by the CVAA_* constants.
     */
    public synchronized long[] getCurrentValueAndAdvance()
        throws StandardException
    {
        if ( _isExhausted )
        {
            throw StandardException.newException
                ( SQLState.LANG_SEQUENCE_GENERATOR_EXHAUSTED, _SCHEMA_NAME, _SEQUENCE_NAME );
        }

        long retval[] = new long[ CVAA_LENGTH ];
        retval[ CVAA_STATUS ] = RET_I_AM_CONFUSED;
        retval[ CVAA_CURRENT_VALUE ] = _currentValue;

        advanceValue( retval );
        
        return retval;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PRIVATE BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * <p>
     * Advance the sequence generator. Pre-allocate a range of new values if
     * necessary.
     * </p>
     *
     * @param retval Array of return values to fill in: see CVAA_* constants
     */
    private void advanceValue( long[] retval ) throws StandardException
    {
        long nextValue = _currentValue + _INCREMENT;

        if ( overflowed( _currentValue, nextValue ) )
        {
            // Take this generator offline if we've wrapped around but cycling really isn't allowed.
            if ( !_CAN_CYCLE )
            {
                markExhausted( retval );
                return;
            }

            // Otherwise, cycling is allowed.
	    if(_INCREMENT>0)
	        nextValue = _MIN_VALUE;
	    
	    else 
	        nextValue = _MAX_VALUE;
        }

        _remainingPreallocatedValues--;
        if ( _remainingPreallocatedValues < PREALLOCATION_THRESHHOLD )
        {
            computeNewAllocation( _currentValue, retval );
            return;
        }
        else
        {
            _currentValue = nextValue;
            retval[ CVAA_STATUS ] = RET_OK;
            return;
        }
    }

    /**
     * <p>
     * Mark the generator as exhausted.
     * </p>
     */
    private void markExhausted( long[] retval )
    {
        _isExhausted = true;
        retval[ CVAA_STATUS ] = RET_MARK_EXHAUSTED;

        return;
    }

    /**
     * <p>
     * Return true if an overflow/underflow occurred. This happens if
     * the originalValue and incrementedValue have opposite sign. Overflow
     * also occurs if the incrementedValue falls outside the range of the
     * sequence.
     * </p>
     */
    private boolean overflowed( long originalValue, long incrementedValue )
    {
        boolean overflowed = ( _STEP_INCREASES == ( incrementedValue < originalValue ) );
        
        if ( !overflowed )
        {
            if ( _STEP_INCREASES ) { overflowed = ( incrementedValue > _MAX_VALUE ); }
            else { overflowed = ( incrementedValue < _MIN_VALUE ); }
        }

        return overflowed;
    }

    /**
     * <p>
     * Compute the number of values to allocate. The range may wrap around.
     * </p>
     *
     * @param oldCurrentValue INPUT Used to compute how many values need to be allocated
     * @param retval OUTPUT Array of values to fill in (see CVAA_* constants)
     *
     * @throws StandardException if any error occurs.
     */
    private void computeNewAllocation( long oldCurrentValue, long[] retval ) throws StandardException
    {
        int preferredValuesPerAllocation = computePreAllocationCount();
        
        //
        // The values are growing toward one of the endpoints of the legal range,
        // either the largest legal value or the smallest legal value. First find out
        // how many values are left between the current value and the endpoint
        // we are growing toward.
        //
        long remainingLegalValues = computeRemainingValues( oldCurrentValue );

        long newValueOnDisk;
        long valuesToAllocate;

        if ( remainingLegalValues >= preferredValuesPerAllocation )
        {
            newValueOnDisk = oldCurrentValue + ( preferredValuesPerAllocation * _INCREMENT );
            valuesToAllocate = preferredValuesPerAllocation;
        }
        else
        {
            // We wrapped around.

            if ( _CAN_CYCLE )
            {
                long spillOverValues = preferredValuesPerAllocation - remainingLegalValues;

                // account for the fact that the restart value itself is a legal value
                spillOverValues--;

                newValueOnDisk = _RESTART_VALUE + ( spillOverValues * _INCREMENT );
                valuesToAllocate = preferredValuesPerAllocation;
            }
            else
            {
                // wrap around not allowed
                
                if ( remainingLegalValues <= 0 )
                {
                    markExhausted( retval );
                    return;
                }
                else
                {
                    valuesToAllocate = remainingLegalValues;
                    newValueOnDisk = oldCurrentValue + ( valuesToAllocate * _INCREMENT );
                }
            }
        }

        //account for the fact that the current value is already allocated
        retval[ CVAA_NUMBER_OF_VALUES_ALLOCATED ] = valuesToAllocate + 1;
        retval[ CVAA_LAST_ALLOCATED_VALUE ] = newValueOnDisk ;
        retval[ CVAA_STATUS ] = RET_ALLOCATE_NEW_VALUES;
    }
    
    /**
     * <p>
     * Get the number of values remaining until we bump against an endpoint of the legal range of values.
     * This is a positive number and so may understate the number of remaining values if the datatype is BIGINT.
     * </p>
     *
     */
    private long computeRemainingValues( long oldCurrentValue )
    {
        long spaceLeft = _STEP_INCREASES ? _MAX_VALUE - oldCurrentValue : -( _MIN_VALUE - oldCurrentValue );

        // if overflow occurred, the range is very large
        if ( spaceLeft < 0L ) { spaceLeft = Long.MAX_VALUE; }

        long divisor = _STEP_INCREASES ? _INCREMENT : -_INCREMENT;

        return spaceLeft / divisor;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SETUP/TEARDOWN MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This method returns the number of values to pre-allocate when we
     * grab a new chunk of values. This is a bit of defensive coding to cover
     * the case when the sequence's parameters are absurdly large.
     * </p>
     */
    private int computePreAllocationCount()
    {
        int happyResult = _PREALLOCATOR.nextRangeSize(  _SCHEMA_NAME, _SEQUENCE_NAME );
        int unhappyResult = PREALLOCATION_THRESHHOLD;

        if ( happyResult < unhappyResult ) { return unhappyResult; }

        double min = _MIN_VALUE;
        double max = _MAX_VALUE;
        double range = max - min;
        double step = _INCREMENT;
        if ( step < 0.0 ) { step = -step; }

        double chunkSize = step * happyResult;

        if ( chunkSize > Long.MAX_VALUE ) { return unhappyResult; }
        if ( chunkSize > range ) { return unhappyResult; }

        return happyResult;
    }
}
