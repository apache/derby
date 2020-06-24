/*

 Derby - Class org.apache.derbyTesting.system.nstest.NsTestError

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
package org.apache.derbyTesting.system.nstest;

/**
 * <p>
 * An descriptor for an error seen by NsTest. These are placed in a
 * HashMap keyed by the error's stack trace.
 * </p>
 */
public  class   NsTestError implements Comparable<NsTestError>
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

    private Throwable   _throwable;
    private int             _count;
    private long            _firstOccurrenceTime;
    private long            _lastOccurrenceTime;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Construct from a Throwable */
    public  NsTestError( Throwable throwable )
    {
        _throwable = throwable;
        _count = 1;
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
        _firstOccurrenceTime = System.currentTimeMillis();
        _lastOccurrenceTime = _firstOccurrenceTime;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the Throwable wrapped by this descriptor */
    public  Throwable   throwable() { return _throwable; }

    /** Get the number of times this error was seen */
    public  int count() { return _count; }

    /** Get the timestamp of the first occurrence */
    public  long    getFirstOccurrenceTime() { return _firstOccurrenceTime; }

    /** Get the timestamp of the last occurrence */
    public  long    getLastOccurrenceTime() { return _lastOccurrenceTime; }
  
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Comparable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  int compareTo( NsTestError that )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
        if ( that == null ) { return -1; }
        else
        {
            long    thisVal = this._firstOccurrenceTime;
            long    thatVal = that._firstOccurrenceTime;

            if ( thisVal < thatVal ) { return -1; }
            else if ( thisVal > thatVal ) { return 1; }
            else { return 0; }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Increment the number of times this error was seen */
    public  void    increment()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
        _count++;
        _lastOccurrenceTime = System.currentTimeMillis();
    }
    
}

