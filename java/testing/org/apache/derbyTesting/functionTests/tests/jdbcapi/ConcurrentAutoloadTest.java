/*

Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.ConcurrentAutoloadTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that autoloading works correctly also in the case where it's invoked
 * from two threads at the same time. This test case must run in a separate
 * JVM to make sure the driver hasn't already been loaded.
 */
public class ConcurrentAutoloadTest extends BaseJDBCTestCase {
    public ConcurrentAutoloadTest(String name) {
        super(name);
    }

    public void testConcurrentAutoloading() throws Exception {

        if (!TestConfiguration.loadingFromJars()) {
            // Autoloading only happens when running from jars.
            return;
        }

        if (!JDBC.vmSupportsJDBC41()) {
            // Only run this test case on JDBC 4.1 (Java 7) and newer. Although
            // autoloading is supposed to work on JDBC 4.0 (Java 6) too, there
            // is a bug on Java 6 that causes problems when autoloading happens
            // in multiple threads at once. See DERBY-4480.
            return;
        }

        TestConfiguration tc = getTestConfiguration();
        final String url = tc.getJDBCUrl() + ";create=true";
        final String user = tc.getUserName();
        final String pw = tc.getUserPassword();

        final List<Throwable> errors =
                Collections.synchronizedList(new ArrayList<Throwable>());

        Runnable r = new Runnable() {
            public void run() {
                try {
                    DriverManager.getConnection(url, user, pw);
                } catch (Throwable t) {
                    errors.add(t);
                }
            }
        };

        Thread t1 = new Thread(r);
        Thread t2 = new Thread(r);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        if (!errors.isEmpty()) {
            if (errors.size() > 1) {
                // Since we can only link one exception to the assert failure,
                // print all stack traces if we have multiple errors.
                for (int i = 0; i < errors.size(); i++) {
                    printStackTrace((Throwable) errors.get(i));
                }
            }
            fail("Thread failed", (Throwable) errors.get(0));
        }
    }

}
