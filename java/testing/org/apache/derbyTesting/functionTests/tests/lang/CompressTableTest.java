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
import java.util.concurrent.atomic.AtomicBoolean;
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

    //DERBY-5750(Sending an empty string as table name to compress table 
    // procedure or empty string as index name to update statistics procedure 
    // makes the parser throw an exception.)
    //
    //No table name will result in the same exception that a user would
    // get when issuing the compress table sql directly without the table name
    // eg alter table compress sequential
    // Notice that the table name is missing in the compress sql above
    public void testCompressTableWithEmptyParamsDerby5750() throws SQLException {
        Statement s = createStatement();
        s.execute("create table DERBY5750_t1 (c11 int)");
        
        //Following 2 statements will give exception since there is no schema
        // named empty string
        assertStatementError(
        		"42Y07", s,
        		"call syscs_util.syscs_compress_table('','DERBY5750_T1',1)");
        assertStatementError(
        		"42Y07", s,
        		"call syscs_util.syscs_compress_table('','',1)");

        //null schema name will translate to current schema
        s.execute("call syscs_util.syscs_compress_table(null,'DERBY5750_T1',1)");

        //Following 2 statements will give exception since there is no table  
        // named empty string
        assertStatementError(
        		"42X05", s,
        		"call syscs_util.syscs_compress_table(null,'',1)");
        assertStatementError(
        		"42X05", s,
                "call syscs_util.syscs_compress_table('APP','',1)");

        //Following 2 statements will give exception since table name can't 
        // be null
        assertStatementError(
        		"42X05", s,
        		"call syscs_util.syscs_compress_table(null,null,1)");
        assertStatementError(
        		"42X05", s,
        		"call syscs_util.syscs_compress_table('APP',null,1)");

        s.execute("call syscs_util.syscs_compress_table('APP','DERBY5750_T1',1)");
        
        s.execute("drop table DERBY5750_t1");    	
    }
    
    /**
     * Test that SYSCS_COMPRESS_TABLE and SYSCS_INPLACE_COMPRESS_TABLE work
     * when the table name contains a double quote. It used to raise a syntax
     * error. Fixed as part of DERBY-1062.
     */
    public void testCompressTableWithDoubleQuoteInName() throws SQLException {
        Statement s = createStatement();
        s.execute("create table app.\"abc\"\"def\" (x int)");
        s.execute("call syscs_util.syscs_compress_table('APP','abc\"def',1)");
        s.execute("call syscs_util.syscs_inplace_compress_table('APP'," +
                  "'abc\"def', 1, 1, 1)");
        s.execute("drop table app.\"abc\"\"def\"");
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
        // The helper thread stops once the value is set to true.
        final AtomicBoolean stop = new AtomicBoolean();

        // Holder for anything thrown by the run() method in the helper thread.
        final Throwable[] error = new Throwable[1];

        // Set up a helper thread that executes a query against the table
        // until the main thread tells it to stop.
        Connection c2 = openDefaultConnection();
        final PreparedStatement ps = c2.prepareStatement("select * from d4275");

        Thread t = new Thread() {
            public void run() {
                try {
                    while (!stop.get()) {
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
            stop.set(true);
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
