/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.Bug5052rtsTest
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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * testing gathering of runtime statistics for for the resultsets/statements not
 * closed by the usee, but get closed when garbage collector collects such
 * objects and closes them by calling the finalize.
 * 
 */
public class Bug5052rtsTest extends BaseJDBCTestCase {

    /**
     * Basic constructor.
     */
    public Bug5052rtsTest(String name) {
        super(name);
    }

    /**
     * Sets the auto commit to false.
     */
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    /**
     * Returns the implemented tests.
     * 
     * @return An instance of <code>Test</code> with the implemented tests to
     *         run.
     */
    public static Test suite() {
        return new CleanDatabaseTestSetup(TestConfiguration
                .embeddedSuite(Bug5052rtsTest.class)) {
            protected void decorateSQL(Statement stmt) throws SQLException {
                stmt
                        .execute("create table tab1 (COL1 int, COL2 smallint, COL3 real)");
                stmt.executeUpdate("insert into tab1 values(1, 2, 3.1)");
                stmt.executeUpdate("insert into tab1 values(2, 2, 3.1)");
            }
        };
    }

    /**
     * Make sure NullPointerException does not occur if 
     * RuntimeStatistics is used and ResultSet is not closed by the user
     * 
     * @throws SQLException
     */
    public void testBug5052() throws SQLException {
        Statement stmt0 = createStatement();
        Statement stmt1 = createStatement();
        Statement stmt2 = createStatement();
        CallableStatement cs;
        ResultSet rs;
        ResultSet rs1;

        /* case1: Setting runtime statistics on just before result set close. */
        rs = stmt0.executeQuery("select * from tab1"); // opens the result set

        while (rs.next()) {
            // System.out.println(rs.getString(1));
        }

        // set the runtime statistics on now.
        cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(?)");
        cs.setInt(1, 1);
        cs.execute();
        cs.close();

        rs.close();

        cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(?)");
        cs.setInt(1, 0);
        cs.execute();
        cs.close();

        /* case2: Statement/Resultset getting closed by the Garbage collector. */
        rs = stmt1.executeQuery("select * from tab1"); // opens the result set

        while (rs.next()) {
            // System.out.println(rs.getString(1));
        }
        // set the runtime statistics on now.
        cs = prepareCall(
                "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(?)");
        cs.setInt(1, 1);
        cs.execute();
        cs.close();

        rs1 = stmt2.executeQuery("select count(*) from tab1"); // opens the
                                                                // result set

        while (rs1.next()) {
            // System.out.println(rs1.getString(1));
        }

        for (int i = 0; i < 3; i++) {
            System.gc();
            System.runFinalization();
            // sleep for sometime to make sure garbage collector kicked in
            // and collected the result set object.
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ie) {
                fail("Unexpected interruption!");
            }
        }

        commit(); // This should have failed before we fix 5052
    }
}
