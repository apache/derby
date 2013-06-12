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
         // DERBY-3745 We want to avoid leaking class loaders, so 
         // we make sure the context class loader is null before
         // creating the thread
        ClassLoader savecl = null;
        boolean hasGetClassLoaderPerms = false;
        try {
            savecl = AccessController.doPrivileged(
            new PrivilegedAction<ClassLoader>() {
                public ClassLoader run()  {
                    return Thread.currentThread().getContextClassLoader();
                }
            });
            hasGetClassLoaderPerms = true;
        } catch (SecurityException se) {
            // Ignore security exception. Versions of Derby before
            // the DERBY-3745 fix did not require getClassLoader 
            // privs.  We may leak class loaders if we are not
            // able to do this but we can't just fail.
        }
        if (hasGetClassLoaderPerms)
            try {
                AccessController.doPrivileged(
                new PrivilegedAction<Object>() {
                    public Object run()  {
                        Thread.currentThread().setContextClassLoader(null);
                        return null;
                    }
                });
            } catch (SecurityException se) {
                // ignore security exception.  Earlier versions of Derby, before the 
                // DERBY-3745 fix did not require setContextClassloader permissions.
                // We may leak class loaders if we are not able to set this, but 
                // cannot just fail.
            }
        singletonTimer = new Timer(true); // Run as daemon
        if (hasGetClassLoaderPerms)
            try {
                final ClassLoader tmpsavecl = savecl;
                AccessController.doPrivileged(
                new PrivilegedAction<Object>() {
                    public Object run()  {
                        Thread.currentThread().setContextClassLoader(tmpsavecl);
                        return null;
                    }
                });
            } catch (SecurityException se) {
                // ignore security exception.  Earlier versions of Derby, before the 
                // DERBY-3745 fix did not require setContextClassloader permissions.
                // We may leak class loaders if we are not able to set this, but 
                // cannot just fail.
            }
    }

    /**
     * Returns a Timer object that can be used for adding TimerTasks
     * that cancel executing statements.
     *
     * @return a Timer object for cancelling statements.
     */
    Timer getCancellationTimer()
    {
        return singletonTimer;
    }

    // TimerFactory interface methods

    /** {@inheritDoc} */
    public void schedule(TimerTask task, long delay) {
        singletonTimer.schedule(task, delay);
    }

    /** {@inheritDoc} */
    public void cancel(TimerTask task) {
        task.cancel();
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
}
