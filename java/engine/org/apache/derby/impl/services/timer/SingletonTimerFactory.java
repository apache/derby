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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Timer;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;


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
     * The number of times {@link #cancel(TimerTask)} has been called.
     * Used for determining whether it's time to purge cancelled tasks from
     * the timer.
     */
    private final AtomicInteger cancelCount = new AtomicInteger();

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
         // DERBY-3745 We want to avoid leaking class loaders, so 
         // we make sure the context class loader is null before
         // creating the thread
        ClassLoader savecl = getContextClassLoader();
        if (savecl != null) {
            setContextClassLoader(null);
        }

        singletonTimer = new Timer(true); // Run as daemon

        if (savecl != null) {
            // Restore the original context class loader.
            setContextClassLoader(savecl);
        }
    }

    // TimerFactory interface methods

    /** {@inheritDoc} */
    public void schedule(TimerTask task, long delay) {
        singletonTimer.schedule(task, delay);
    }

    /** {@inheritDoc} */
    public void cancel(TimerTask task) {
        task.cancel();

        // DERBY-6114: Cancelled tasks stay in the timer's queue until they
        // are scheduled to run, unless we call the purge() method. This
        // prevents garbage collection of the tasks. Even though the tasks
        // are small objects, there could be many of them, especially when
        // both the transaction throughput and tasks' delays are high, it
        // could lead to OutOfMemoryErrors. Since purge() could be a heavy
        // operation if the queue is big, we don't call it every time a task
        // is cancelled.
        //
        // When Java 7 has been made the lowest supported level, we should
        // consider replacing the java.util.Timer instance with a
        // java.util.concurrent.ScheduledThreadPoolExecutor, and call
        // setRemoveOnCancelPolicy(true) on the executor.
        if (cancelCount.incrementAndGet() % 1000 == 0) {
            singletonTimer.purge();
        }
    }

    // ModuleControl interface methods

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

    // Helper methods

    private static ClassLoader getContextClassLoader() {
        try {
            return AccessController.doPrivileged(
                    new PrivilegedAction<ClassLoader>() {
                public ClassLoader run() {
                    return Thread.currentThread().getContextClassLoader();
                }
            });
        } catch (SecurityException se) {
            // Ignore security exception. Versions of Derby before
            // the DERBY-3745 fix did not require getContextClassLoader
            // privileges. We may leak class loaders if we are not
            // able to do this, but we can't just fail.
            return null;
        }
    }

    private static void setContextClassLoader(final ClassLoader cl) {
        try {
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                public Void run() {
                    Thread.currentThread().setContextClassLoader(cl);
                    return null;
                }
            });
        } catch (SecurityException se) {
            // Ignore security exception. Earlier versions of Derby, before
            // the DERBY-3745 fix, did not require setContextClassLoader
            // permissions. We may leak class loaders if we are not able to
            // set this, but cannot just fail.
        }
    }

}
