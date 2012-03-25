/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CompressTableTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test cases for compressing tables.
 */
public class CompressTableTest extends BaseJDBCTestCase {

    public CompressTableTest(String name) {
        super(name);
    }

    public static Test suite() {
        // compress table is an embedded feature, no need to run network tests
        return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(CompressTableTest.class));
    }

    /**
     * Test that statement invalidation works when SYSCS_COMPRESS_TABLE calls
     * and other statements accessing the same table execute concurrently.
     * DERBY-4275.
     */
    public void testConcurrentInvalidation() throws Exception {
        Statement s = createStatement();
        s.execute("create table d4275(x int)");
        s.execute("insert into d4275 values 1");

        // Object used by the main thread to tell the helper thread to stop.
        // The helper thread stops once the list is non-empty.
        final List stop = Collections.synchronizedList(new ArrayList());

        // Holder for anything thrown by the run() method in the helper thread.
        final Throwable[] error = new Throwable[1];

        // Set up a helper thread that executes a query against the table
        // until the main thread tells it to stop.
        Connection c2 = openDefaultConnection();
        final PreparedStatement ps = c2.prepareStatement("select * from d4275");

        Thread t = new Thread() {
            public void run() {
                try {
                    while (stop.isEmpty()) {
                        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
                    }
                } catch (Throwable t) {
                    error[0] = t;
                }
            }
        };

        t.start();

        // Compress the table while a query is being executed against the
        // same table to force invalidation of the running statement. Since
        // the problem we try to reproduce is timing-dependent, do it 100
        // times to increase the chance of hitting the bug.
        try {
            for (int i = 0; i < 100; i++) {
                s.execute(
                    "call syscs_util.syscs_compress_table('APP', 'D4275', 1)");
            }
        } finally {
            // We're done, so tell the helper thread to stop.
            stop.add(Boolean.TRUE);
        }

        t.join();

        // Before DERBY-4275, the helper thread used to fail with an error
        // saying the container was not found.
        if (error[0] != null) {
            fail("Helper thread failed", error[0]);
        }

        // Cleanup.
        ps.close();
        c2.close();
    }
}
