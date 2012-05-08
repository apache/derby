/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.memory.Derby5730Test
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.memory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SpawnedProcess;

/**
 * Regression test for DERBY-5730. Repeatedly boot and shut down a database.
 * In between call a function in the SYSFUN schema, a different one each time.
 * Without the fix, a reference to the currently running database instance will
 * be leaked for each iteration, and eventually an OutOfMemoryError is raised.
 */
public class Derby5730Test extends BaseTestCase {

    public Derby5730Test(String name) {
        super(name);
    }

    public static Test suite() {
        // The test case uses DriverManager, so require JDBC 3.0 or higher.
        if (JDBC.vmSupportsJDBC3()) {
            return new TestSuite(Derby5730Test.class);
        }
        return new TestSuite("Derby5730Test - skipped");
    }

    /**
     * Test case for DERBY-5730. The memory leak is only reproduced if the
     * test case runs with capped heap size and the SYSFUN functions have not
     * been called previously in the same JVM process. Spawn a new process to
     * satisfy those requirements.
     */
    public void testLeak() throws IOException {
        String[] cmd = {"-Xmx16M", getClass().getName()};
        SpawnedProcess sp = new SpawnedProcess(execJavaCmd(cmd), "DERBY-5730");
        if (sp.complete() != 0) {
            fail(sp.getFailMessage("Process failed"));
        }
    }

    private final static Integer ZERO = Integer.valueOf("0");
    private final static Integer ONE = Integer.valueOf("1");
    private final static Integer TWO = Integer.valueOf("2");

    /**
     * These are the functions in the SYSFUN schema. The second value in each
     * row tells how many arguments the function takes.
     */
    private final static Object[][] FUNCTIONS = {
        {"ACOS", ONE},
        {"ASIN", ONE},
        {"ATAN", ONE},
        {"ATAN2", TWO},
        {"COS", ONE},
        {"SIN", ONE},
        {"TAN", ONE},
        {"PI", ZERO},
        {"DEGREES", ONE},
        {"RADIANS", ONE},
        {"LN", ONE},
        {"LOG", ONE},
        {"LOG10", ONE},
        {"EXP", ONE},
        {"CEIL", ONE},
        {"CEILING", ONE},
        {"FLOOR", ONE},
        {"SIGN", ONE},
        {"RANDOM", ZERO},
        {"RAND", ONE},
        {"COT", ONE},
        {"COSH", ONE},
        {"SINH", ONE},
        {"TANH", ONE},
    };

    /**
     * Boot and repeatedly reboot a database, calling SYSFUN functions in
     * between. Eventually runs out of memory if DERBY-5730 is not fixed.
     * Must run with capped memory size (-Xmx16M) to expose the memory leak.
     */
    public static void main(String[] args) throws SQLException {
        for (int i = 0; i < FUNCTIONS.length; i++) {
            Connection c = DriverManager.getConnection(
                    "jdbc:derby:memory:derby5730;create=true");
            prepareFunction(c,
                            (String) FUNCTIONS[i][0],
                            ((Integer) FUNCTIONS[i][1]).intValue());
            growDatabaseFootprint(c);
            c.close();
            try {
                DriverManager.getConnection(
                    "jdbc:derby:memory:derby5730;shutdown=true");
                fail("Shutdown should throw exception");
            } catch (SQLException sqle) {
                BaseJDBCTestCase.assertSQLState("08006", sqle);
            }
        }
    }

    /**
     * Prepare a call to a function. Close the statement once it is prepared.
     * Before the bug was fixed, preparing a call to a function in SYSFUN that
     * hadn't been prepared in the same JVM process, would leak a reference to
     * the current database instance.
     */
    private static void prepareFunction(Connection c, String name, int args)
            throws SQLException {
        StringBuffer sql = new StringBuffer("VALUES ");
        sql.append(name);
        sql.append('(');
        for (int i = 0; i < args; i++) {
            if (i > 0) sql.append(',');
            sql.append('?');
        }
        sql.append(')');

        String sqlText = sql.toString();

        System.out.println(sqlText);
        PreparedStatement ps = c.prepareStatement(sqlText);
        ps.close();
    }

    /**
     * Perform some database operations so that the internal structures of
     * the database instance (caches, for example) are filled, and the memory
     * footprint of the database instance grows. This is done to make the
     * test case run out of memory faster.
     */
    private static void growDatabaseFootprint(Connection c)
            throws SQLException {
        DatabaseMetaData dmd = c.getMetaData();
        JDBC.assertDrainResults(dmd.getColumns(null, "%", "%", "%"));
        JDBC.assertDrainResults(dmd.getTables(null, "%", "%", null));
    }
}
