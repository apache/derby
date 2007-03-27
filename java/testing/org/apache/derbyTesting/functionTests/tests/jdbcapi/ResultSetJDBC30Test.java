/*
 *
 * Derby - Class ResultSetJDBC30Test
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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;

import java.sql.*;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test of additional methods in JDBC3.0 result set
 */
public class ResultSetJDBC30Test extends BaseJDBCTestCase {

    /** Creates a new instance of ResultSetJDBC30Test */
    public ResultSetJDBC30Test(String name) {
        super(name);
    }

    /**
     * Set up the connection to the database.
     */
    public void setUp() throws  Exception {
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement();
        stmt.execute("create table t (i int, s smallint, r real, "+
            "d double precision, dt date, t time, ts timestamp, "+
            "c char(10), v varchar(40) not null, dc dec(10,2))");
        stmt.execute("insert into t values(1,2,3.3,4.4,date('1990-05-05'),"+
                     "time('12:06:06'),timestamp('1990-07-07 07:07:07.07'),"+
                     "'eight','nine', 11.1)");
        stmt.close();
        commit();
    }

    protected void tearDown() throws Exception {
        Statement stmt = createStatement();
        stmt.executeUpdate("DROP TABLE t");
        commit();
        super.tearDown();
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(ResultSetJDBC30Test.class);
    }


    public void testNotImplementedMethods() throws Exception {
        Statement stmt = createStatement();

        ResultSet rs = stmt.executeQuery("select * from t");
        assertTrue("FAIL - row not found", rs.next());

        try {
            rs.getURL(8);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        }
        try {
            rs.getURL("c");
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        }
        try {
            rs.updateRef(8, null);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        } catch (NoSuchMethodError nsme) {
            assertTrue("FAIL - ResultSet.updateRef not present - correct for" +
                    " JSR169", JDBC.vmSupportsJSR169());
        }
        try {
            rs.updateRef("c", null);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        } catch (NoSuchMethodError nsme) {
            assertTrue("FAIL - ResultSet.updateRef not present - correct for" +
                    " JSR169", JDBC.vmSupportsJSR169());
        }
        try {
            rs.updateBlob(8, null);
            if (usingEmbedded()) {
                fail("FAIL - Shouldn't reach here. Method is being invoked" +
                        " on a read only resultset.");
            } else {
                fail("FAIL - Shouldn't reach here. Method not implemented" +
                        " yet.");
            }
        } catch (SQLException se) {
            assertSQLState(UPDATABLE_RESULTSET_API_DISALLOWED, se);
        }
        try {
            rs.updateBlob("c", null);
            if (usingEmbedded()) {
                fail("FAIL - Shouldn't reach here. Method is being invoked" +
                        " on a read only resultset.");
            } else {
                fail("FAIL - Shouldn't reach here. Method not implemented" +
                        " yet.");
            }
        } catch (SQLException se) {
            assertSQLState(UPDATABLE_RESULTSET_API_DISALLOWED, se);
        }
        try {
            rs.updateClob(8, null);
            if (usingEmbedded()) {
                fail("FAIL - Shouldn't reach here. Method is being invoked" +
                        " on a read only resultset.");
            } else {
                fail("FAIL - Shouldn't reach here. Method not implemented" +
                        " yet.");
            }
        } catch (SQLException se) {
            assertSQLState(UPDATABLE_RESULTSET_API_DISALLOWED, se);
        }
        try {
            rs.updateClob("c", null);
            if (usingEmbedded()) {
                fail("FAIL - Shouldn't reach here. Method is being invoked" +
                        " on a read only resultset.");
            } else {
                fail("FAIL - Shouldn't reach here. Method not implemented" +
                        " yet.");
            }
        } catch (SQLException se) {
            assertSQLState(UPDATABLE_RESULTSET_API_DISALLOWED, se);
        }
        try {
            rs.updateArray(8, null);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        } catch (NoSuchMethodError nsme) {
            assertTrue("FAIL - ResultSet.updateArray not present - correct" +
                    " for JSR169", JDBC.vmSupportsJSR169());
        }
        try {
            rs.updateArray("c", null);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        } catch (NoSuchMethodError nsme) {
            assertTrue("FAIL - ResultSet.updateArray not present - correct" +
                    " for JSR169", JDBC.vmSupportsJSR169());
        }

        rs.close();
        stmt.close();
        commit();
    }

    public void testCloseResultSetAutoCommit() throws Exception {
        //
        // Check our behavior around closing result sets when auto-commit
        // is true.  Test with both holdable and non-holdable result sets
        //
        getConnection().setAutoCommit(true);

        // Create a non-updatable holdable result set, and then try to
        // update it
        getConnection().setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        Statement stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = stmt.executeQuery("select * from t");
        rs.next();

        checkForCloseOnException(rs, true);

        rs.close();
        stmt.close();

        // Create a non-updatable non-holdable result set, and then try to
        // update it
        getConnection().setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);

        rs = stmt.executeQuery("select * from t");
        rs.next();

        checkForCloseOnException(rs, false);

        rs.close();
        stmt.close();
        commit();
    }

    private void checkForCloseOnException(ResultSet rs, boolean holdable)
            throws Exception
    {
        try {
            rs.updateBlob("c",null);
            fail("FAIL - rs.updateBlob() on a read-only result set" +
                "should not have succeeded");
        } catch (SQLException ex) {}
        // The result set should not be closed on exception, this call should
        // not cause an exception
        rs.beforeFirst();
    }

    private static final String NOT_IMPLEMENTED = "0A000";
    private static final String UPDATABLE_RESULTSET_API_DISALLOWED = "XJ083";
}
