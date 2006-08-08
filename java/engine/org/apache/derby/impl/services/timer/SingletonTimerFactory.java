/*

   Derby - Class org.apache.derby.impl.services.timer.SingletonTimerFactory

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

package org.apache.derby.impl.services.timer;

import org.apache.derby.iapi.services.timer.TimerFactory;
import org.apache.derby.iapi.services.monitor.ModuleControl;

import org.apache.derby.iapi.error.StandardException;

import java.util.Timer;
import java.util.Properties;


/**
 * This class implements the TimerFactory interface.
 * It creates a singleton Timer instance.
 *
 * The class implements the ModuleControl interface,
 * because it needs to cancel the Timer at system shutdown.
 *
 * @see TimerFactory
 * @see ModuleControl
 */
public class SingletonTimerFactory
    implements
        TimerFactory,
        ModuleControl
{
    /**
     * Singleton Timer instance.
     */
    private Timer singletonTimer;

    /**
     * Initializes this TimerFactory with a singleton Timer instance.
     */
    public SingletonTimerFactory()
    {
        /**
         * Even though we implement the ModuleControl interface,
         * we initialize the object here rather than in boot, since
         * a) We avoid synchronizing access to singletonTimer later
         * b) We don't need any properties
         */
        singletonTimer = new Timer(true); // Run as daemon
    }

    /**
     * Returns a Timer object that can be used for adding TimerTasks
     * that cancel executing statements.
     *
     * Implements the TimerFactory interface.
     *
     * @return a Timer object for cancelling statements.
     *
     * @see TimerFactory
     */
    public Timer getCancellationTimer()
    {
        return singletonTimer;
    }

    /**
     * Currently does nothing, singleton Timer instance is initialized
     * in the constructor.
     *
     * Implements the ModuleControl interface.
     *
     * @see ModuleControl
     */
    public void boot(boolean create, Properties properties)
        throws
            StandardException
    {
        // Do nothing, instance already initialized in constructor
    }

    /**
     * Cancels the singleton Timer instance.
     * 
     * Implements the ModuleControl interface.
     *
     * @see ModuleControl
     */
    public void stop()
    {
        singletonTimer.cancel();
    }
}
