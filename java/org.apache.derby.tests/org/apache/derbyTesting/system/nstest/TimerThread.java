/*

 Derby - Class org.apache.derbyTesting.system.nstest.TimerThread

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
 * A thread which sleeps for a specified time period, then wakes up
 * and terminates the VM.
 * </p>
 */
public class TimerThread extends Thread
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

    private long    _sleepTime;
    private boolean _continueRunning = true;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public TimerThread( long sleepTime )
    {
        _sleepTime = sleepTime;
	}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Thread BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Implementation of run() method to sleep, wake up, and kill the program.
	 * 
	 */
	public void run() {

        long    remainingTime = _sleepTime;
        
        while ( _continueRunning )
        {
            long    cycleStartTime = System.currentTimeMillis();
            
            try {
                if ( remainingTime > 0L )
                {
                    NsTest.logger.println( "TimerThread sleeping for " + remainingTime + " milliseconds." );
                    sleep( remainingTime );
                }

                //
                // End the program. This will fire the shutdown hook which
                // prints the final statistics.
                //
                NsTest.logger.println( "TimerThread attempting to shut down the program." );
                NsTest.printStatistics();
                Runtime.getRuntime().halt( 0 );
                
            } catch (java.lang.InterruptedException ie)
            {
                NsTest.printException( TimerThread.class.getName(), ie );
            }
            
            long    elapsedTime = System.currentTimeMillis() - cycleStartTime;

            remainingTime = remainingTime - elapsedTime;
        }
	}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** This method marks the thread for rundown */
    public  void    stopNow() { _continueRunning = false; }

}

