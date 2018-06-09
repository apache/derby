/*
 * Derby - Class org.apache.derbyTesting.functionTests.util.Barrier
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.util;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

/**
 * A deadlock watch utlity.  An instance of this class can be created and started
 * and it will look for a Java level deadlock at the time given  if not stopped.
 * A deadlock is detected using JMX and the ThreadMXBean
 */
public class DeadlockWatchdog implements Runnable {
    
    private boolean stopped = false;
    private long timeout;
    
    public DeadlockWatchdog(long timeout) {
        this.timeout = timeout;
    }
    
    public synchronized void start() {
        stopped = false;
        Thread t = new Thread(this, "WATCHDOG");
        t.setDaemon(true);
        t.start();        
    }
    
    public synchronized void stop() {
       stopped = true;
        notifyAll();
    }
    
    public synchronized void run() {
        final long until = System.currentTimeMillis() + timeout;
        long now;
        while (!stopped && until > (now = System.currentTimeMillis())) {
            try {
                wait(until - now);
            } catch (InterruptedException e) {
            }
        }
        if (!stopped) {
            try {
                boolean res = AccessController.doPrivileged(
                        new PrivilegedExceptionAction<Boolean>() {
                    public Boolean run() throws IOException, MalformedObjectNameException, InstanceNotFoundException, MBeanException, ReflectionException {
                        return checkForDeadlock();
                    }
                });

                if (res) {
                    System.err.println("Deadlock detected");
                    System.exit(1);
                }
            } catch (Exception x) {
                System.err.println("Watchdog failed: " + x.toString());
                System.exit(1);
            }
        }
    }    
    
    boolean checkForDeadlock() throws MalformedObjectNameException, InstanceNotFoundException, MBeanException, ReflectionException, IOException {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long[] findDeadlockedThreads = bean.findDeadlockedThreads();
        if (null != findDeadlockedThreads && 0 != findDeadlockedThreads.length) {
            return true;
        }

        return false;
    }  
}
