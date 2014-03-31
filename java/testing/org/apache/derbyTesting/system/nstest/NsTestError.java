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
public  class   NsTestError
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
  
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Increment the number of times this error was seen */
    public  void    increment() { _count++; }
    
}

