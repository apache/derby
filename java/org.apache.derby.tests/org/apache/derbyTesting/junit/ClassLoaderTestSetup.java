/*
 * Derby - Class org.apache.derbyTesting.junit.ClassLoaderTestSetup
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

package org.apache.derbyTesting.junit;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import junit.framework.Test;

/**
 * A decorator that changes the context class loader for the current
 * configuration and resets it afterwards.
 */
public class ClassLoaderTestSetup extends BaseJDBCTestSetup {

    private ClassLoader oldLoader;

    /**
     * Create a decorator that makes {@code test} run with non-default
     * class loader. It also shuts down the engine so Derby classes will
     * be loaded with the new class loader.
     *
     * @param test the test to decorate
     */
    public ClassLoaderTestSetup(Test test) {
        super(test);
    }

    private static ClassLoader makeClassLoader() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6881
        PrivilegedAction<ClassLoader> pa = () -> new URLClassLoader(new URL[0]);
        return AccessController.doPrivileged(pa);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        TestConfiguration.getCurrent().shutdownEngine();
        oldLoader = getThreadLoader();
//IC see: https://issues.apache.org/jira/browse/DERBY-6881
        setThreadLoader(makeClassLoader());
    }

    @Override
    protected void tearDown() throws Exception {
        setThreadLoader(oldLoader);
        super.tearDown();
    }

    /**
     * Force this thread to use a specific class loader.
     * @param which class loader to set
     *
     * @throws  SecurityException
     *          if the current thread cannot set the context ClassLoader
     */
    public static void setThreadLoader(final ClassLoader which) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                java.lang.Thread.currentThread().setContextClassLoader(which);
              return null;
            }
        });
    }

    /**
     * <p>
     * Retrieve the class loader currently being used by this thread.
     * </p>
     * @return the current context class loader
     */
    public static ClassLoader getThreadLoader() {
        return AccessController.doPrivileged(
                new PrivilegedAction<ClassLoader>() {
            public ClassLoader run() {
                return Thread.currentThread().getContextClassLoader();
            }
        });
    }

}
