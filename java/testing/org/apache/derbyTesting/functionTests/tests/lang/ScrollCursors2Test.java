/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ScrollCursors2Test
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public class ScrollCursors2Test extends BaseJDBCTestCase {

    public ScrollCursors2Test(String name) {
        super(name);

    }

    private static boolean isDerbyNetClient = usingDerbyNetClient();

    /**
     * Set up the test.
     * 
     * This method creates the table used by the rest of the test.
     * 
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */

    public void setUp() throws SQLException {

        Statement s_i_r;

        s_i_r = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        /* Create a table */
        s_i_r.execute("create table t (i int, c50 char(50))");

        /* Populate the table */
        s_i_r.execute("insert into t (i) values (2), (3), (4), (5), (6)");
        s_i_r.execute("update t set c50 = RTRIM(CAST (i AS CHAR(50)))");
        s_i_r.close();
        isDerbyNetClient = usingDerbyNetClient();
    }

    public void tearDown() throws SQLException, Exception {
        dropTable("T");
        commit();
        super.tearDown();
    }

    public void testForwardOnlyNegative() throws SQLException {
        Connection conn = getConnection();
        PreparedStatement ps_f_r = null;
        ResultSet rs;
        Statement s_f_r = null;

        s_f_r = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        // We should have gotten no warnings and a read only forward only cursor
        JDBC.assertNoWarnings(conn.getWarnings());

        conn.clearWarnings();

        // Verify that setMaxRows(-1) fails
        try {
            s_f_r.setMaxRows(-1);
            // Should never get here
            fail("setMaxRows(-1) expected to fail");
        } catch (SQLException sqle) {
            /* Check to be sure the exception is the one we expect */

            assertEquals("XJ063", sqle.getSQLState());
        }
        // Verify maxRows still 0
        assertEquals("getMaxRows() expected to return 0", 0, s_f_r.getMaxRows());

        // Verify that result set from statement is
        // scroll insensitive and read only
        rs = s_f_r.executeQuery("select * from t");
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

        // Verify that first(), etc. don't work
        try {
            rs.first();
            // Should never get here
            fail("first() expected to fail");
        } catch (SQLException sqle) {
            assertOnlyOnScrollableException(sqle);
        }

        try {
            rs.beforeFirst();
            // Should never get here
            fail("beforeFirst() expected to fail");
        } catch (SQLException sqle) {
            assertOnlyOnScrollableException(sqle);

        }
        try {
            rs.isBeforeFirst();
            // Should never get here
            fail("isBeforeFirst() expected to fail");

        } catch (SQLException sqle) {
            // Check to be sure the exception is the one we expect
            assertOnlyOnScrollableException(sqle);
        }
        try {
            rs.isAfterLast();
            // Should never get here
            fail("isAfterLast() expected to fail");
        } catch (SQLException sqle) {
            // Check to be sure the exception is the one we expect
            assertOnlyOnScrollableException(sqle);

        }
        try {
            rs.isFirst();
            // Should never get here
            fail("isFirst() expected to fail");
        } catch (SQLException sqle) {
            // Check to be sure the exception is the one we expect
            assertOnlyOnScrollableException(sqle);

        }
        try {
            rs.isLast();
            // Should never get here
            fail("isLast() expected to fail");
        } catch (SQLException sqle) {
            // Check to be sure the exception is the one we expect

            assertOnlyOnScrollableException(sqle);

        }
        try {
            rs.absolute(1);
            // Should never get here
            fail("absolute() expected to fail");
        } catch (SQLException sqle) {
            assertOnlyOnScrollableException(sqle);

        }
        try {
            rs.relative(1);
            // Should never get here
            fail("relative() expected to fail");
        } catch (SQLException sqle) {
            assertOnlyOnScrollableException(sqle);
        }

        // setFetchDirection should fail
        try {
            rs.setFetchDirection(ResultSet.FETCH_FORWARD);
            // Should never get here
            fail("setFetchDirection() expected to fail");
        } catch (SQLException sqle) {
            assertOnlyOnScrollableException(sqle);
        }

        // Book says that getFetchDirection(), getFetchSize() and
        // setFetchSize() are all okay.
        if (isDerbyNetClient)
            assertEquals(0, rs.getFetchSize());
        else
            assertEquals(1, rs.getFetchSize());

        rs.setFetchSize(5);
        assertEquals(5,rs.getFetchSize());
      
        assertEquals(ResultSet.FETCH_FORWARD,rs.getFetchDirection());
            

        rs.close();
        s_f_r.close();

        ps_f_r = prepareStatement("select * from t",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        // We should have gotten no warnings and a read only forward only cursor
        JDBC.assertNoWarnings(conn.getWarnings());
        
        conn.clearWarnings();

        // Verify that result set from statement is
        // scroll insensitive and read only
        rs = ps_f_r.executeQuery();
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        
        assertEquals(ResultSet.CONCUR_READ_ONLY,rs.getConcurrency());
        
        // Verify that first() doesn't work
        try {
            rs.first();
            // Should never get here
            fail("first() expected to fail");
        } catch (SQLException sqle) {
            assertOnlyOnScrollableException(sqle);

        }
        rs.close();
        ps_f_r.close();

    }

    /**
     * Positive tests for forward only cursors.
     * 
     * This method tests forward only cursors.
     * 
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */

    public void testForwardOnlyPositive() throws SQLException {
        Connection conn = getConnection();
        ResultSet rs;
        Statement s_f_r = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        // We should have gotten no warnings and a read only forward only cursor
        JDBC.assertNoWarnings(conn.getWarnings());
        conn.clearWarnings();

        // Verify that setMaxRows(4) succeeds
        s_f_r.setMaxRows(5);
        assertEquals(5, s_f_r.getMaxRows());

        rs = s_f_r.executeQuery("values 1, 2, 3, 4, 5, 6");
        // Iterate straight thru RS, expect only 5 rows.
        JDBC.assertDrainResults(rs, 5);
        
        s_f_r.close();

    }

    /**
     * Scroll sensitive cursor tests
     * 
     * This method tests scroll sensitive cursors. (Not implemented, so we
     * should get back scroll insensitive curors with read only concurrency.)
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */

    public void testScrollSensitive() throws SQLException {
        Connection conn = getConnection();
        ResultSet rs;
        SQLWarning warning;
        Statement s_s_r = null; // sensitive, read only
        Statement s_s_u = null; // sensitive, updatable

        s_s_r = createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        // We should have gotten a warning and a scroll insensitive cursor
        warning = conn.getWarnings();
        assertNotNull(warning);
        conn.clearWarnings();

        // Verify that result set from statement is
        // scroll insensitive and read only
        rs = s_s_r.executeQuery("select * from t");
        assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs.close();

        // Close the statement
        s_s_r.close();

        s_s_u = createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        // We should have gotten 1 warning and a updatable scroll
        // insensitive cursor.
        warning = conn.getWarnings();
        assertNotNull(warning);
        conn.clearWarnings();

        // Verify that result set from statement is
        // scroll insensitive and read only
        rs = s_s_u.executeQuery("select * from t");
        assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());

        rs.close();
    }

    /**
     * Positive tests for scroll insensitive cursor.
     * 
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testScrollInsensitivePositive() throws SQLException {
        Connection conn = getConnection();

        ResultSet rs;

        // insensitive, read only
        Statement s_i_r = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        // We should not have gotten any warnings
        // and should have gotten a scroll insensitive cursor
        JDBC.assertNoWarnings(conn.getWarnings());

        conn.clearWarnings();

        // run a query
        rs = s_i_r.executeQuery("select * from t");
        // verify scroll insensitive and read only
        assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        // We should be positioned before the 1st row
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.absolute(0));
        // still expected to be before first
        assertTrue(rs.isBeforeFirst());
        // go to first row
        assertTrue(rs.first());
        assertEquals(rs.getInt(1), 2);
        assertTrue(rs.isFirst());

        // move to before first
        rs.beforeFirst();
        assertTrue(rs.isBeforeFirst());
        // move to last row
        assertTrue(rs.last());
        assertTrue(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertEquals(6, rs.getInt(1));
        assertFalse("not expecting to find another row", rs.next());
        assertTrue(rs.isAfterLast());
        // We're after the last row, verify that only isAfterLast()
        // returns true
        assertFalse(rs.isLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isBeforeFirst());
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
        if (isDerbyNetClient)
            assertEquals(64, rs.getFetchSize());
        else
            assertEquals(1, rs.getFetchSize());
        rs.setFetchSize(5);
        assertEquals(5, rs.getFetchSize());

        // setFetchSize() to 0 should have no effect.
        // for client server, fetchSize should have to 64
        rs.setFetchSize(0);
        if (isDerbyNetClient)
            assertEquals(64, rs.getFetchSize());
        else
            assertEquals(5, rs.getFetchSize());
        // done
        rs.close();

        // Empty result set tests (DERBY-992)
        rs = s_i_r.executeQuery("select * from t where 1=0");
        rs.afterLast();
        assertFalse("afterLast() on empty RS should be no-op", rs.isAfterLast());
        rs.beforeFirst();
        assertFalse("beforeFirst() on empty RS should be no-op", rs
                .isBeforeFirst());

        rs.close();

        PreparedStatement ps_i_r = prepareStatement("select * from t",
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        // We should not have gotten any warnings
        // and should have gotten a prepared scroll insensitive cursor
        JDBC.assertNoWarnings(conn.getWarnings());

        rs = ps_i_r.executeQuery();
        // make sure it's scrollable
        rs.last();
        rs.close();
        ps_i_r.close();

        // Check setMaxRows()/getMaxRows()
        assertEquals(0, s_i_r.getMaxRows());
        s_i_r.setMaxRows(5);
        assertEquals(5, s_i_r.getMaxRows());

        rs = s_i_r.executeQuery("values 1, 2, 3, 4, 5, 6");
        assertNotNull(rs);
        // Iterate straight thru RS, expect only 5 rows.
        for (int index = 1; index < 6; index++) {
            assertTrue(rs.next());

        }
        // We should not see another row (only 5, not 6)
        assertFalse(rs.next());
        rs.close();
        // Jump around and verify setMaxRows() works.
        rs = s_i_r.executeQuery("values 1, 2, 3, 4, 5, 6");
        assertNotNull(rs);
        assertTrue(rs.last());

        // Iterate backwards thru RS, expect only 4 more (5 total) rows.
        for (int index = 1; index < 5; index++) {
            assertTrue(rs.previous());
        }
        // We should not see another row (only 5, not 6)
        assertFalse(rs.previous());
        rs.close();
        rs = s_i_r.executeQuery("values 1, 2, 3, 4, 5, 6");
        assertNotNull(rs);

        rs.afterLast();
        // Iterate backwards thru RS, expect only 5 rows.
        for (int index = 1; index < 6; index++) {
            assertTrue(rs.previous());

        }
        // We should not see another row (only 5, not 6)
        assertFalse(rs.previous());
        rs.close();
        // Verify setting maxRows back to 0 works.
        s_i_r.setMaxRows(0);
        rs = s_i_r.executeQuery("values 1, 2, 3, 4, 5, 6");
        assertNotNull(rs);

        // Iterate straight thru RS, expect 6 rows.
        for (int index = 1; index < 7; index++) {
            assertTrue(rs.next());

        }
        // We should not see another row
        assertFalse(rs.next());

        rs.close();

    }

    /**
     * Negative tests for scroll insensitive cursor.
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testScrollInsensitiveNegative() throws SQLException {
        Connection conn = getConnection();

        ResultSet rs;
        // insensitive, read only
        Statement s_i_r = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        // We should not have gotten any warnings
        // and should have gotten a scroll insensitive cursor
        JDBC.assertNoWarnings(conn.getWarnings());
        conn.clearWarnings();

        // Verify that setMaxRows(-1) fails
        try {
            s_i_r.setMaxRows(-1);
            // Should never get here
            fail("setMaxRows(-1) expected to fail");
        } catch (SQLException sqle) {
            /* Check to be sure the exception is the one we expect */
            assertEquals("XJ063", sqle.getSQLState());

        }
        // Verify maxRows still 0
        assertEquals(0, s_i_r.getMaxRows());

        // Empty result set
        rs = s_i_r.executeQuery("select * from t where 1=0");
        // isBeforeFirst() and isAfterLast() should always return false
        // when result set is empty
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.next());
        assertFalse(rs.previous());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.relative(0));
        assertFalse(rs.relative(1));
        assertFalse(rs.relative(-1));
        assertFalse(rs.absolute(0));
        assertFalse(rs.absolute(1));
        assertFalse(rs.absolute(-1));
        rs.close();
        // End of empty result set tests

        // Non-empty result set
        rs = s_i_r.executeQuery("select * from t");
        // Negative fetch size
        try {
            rs.setFetchSize(-5);
            fail("setFetchSize(-5) expected to fail");

        } catch (SQLException sqle) {
            /* Check to be sure the exception is the one we expect */
            assertEquals("XJ062", sqle.getSQLState());

        }
        rs.close();
        s_i_r.close();
        
    }

    /**
     * CallableStatement tests.
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */

    public void testCallableStatements() throws SQLException {
        Connection conn = getConnection();

        SQLWarning warning;
        CallableStatement cs_s_r = null; // sensitive, read only
        CallableStatement cs_s_u = null; // sensitive, updatable
        CallableStatement cs_i_r = null; // insensitive, read only
        CallableStatement cs_f_r = null; // forward only, read only

        cs_s_r = prepareCall("values cast (? as Integer)",
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

        // We should have gotten 1 warnings
        warning = conn.getWarnings();
        assertNotNull(warning);
        if (!isDerbyNetClient)
            assertEquals("01J02", warning.getSQLState());
        else
            assertEquals("01J10", warning.getSQLState());

        JDBC.assertNoWarnings(warning.getNextWarning());

        conn.clearWarnings();
        cs_s_r.close();

        cs_s_u = prepareCall("values cast (? as Integer)",
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        // We should have gotten 1 warning
        warning = conn.getWarnings();
        assertNotNull(warning);
        if (!isDerbyNetClient)
            assertEquals("01J02", warning.getSQLState());
        else
            assertEquals("01J10", warning.getSQLState());

        JDBC.assertNoWarnings(warning.getNextWarning());
        conn.clearWarnings();
        cs_s_u.close();

        cs_i_r = prepareCall("values cast (? as Integer)",
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        // We should have gotten 0 warnings
        JDBC.assertNoWarnings(conn.getWarnings());

        conn.clearWarnings();
        cs_i_r.close();

        cs_f_r = prepareCall("values cast (? as Integer)",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        // We should have gotten 0 warnings
        JDBC.assertNoWarnings(conn.getWarnings());

        conn.clearWarnings();
        cs_f_r.close();

    }

    /**
     * Tests for PreparedStatement.getMetaData().
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testGetMetaData() throws SQLException {

        PreparedStatement ps_f_r = null; // forward only, read only
        ResultSet rs;
        ResultSetMetaData rsmd_ps;
        ResultSetMetaData rsmd_rs;

        ps_f_r = prepareStatement("select c50, i, 43 from t",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        rsmd_ps = ps_f_r.getMetaData();
        assertNotNull(rsmd_ps);

        // Now get meta data from result set
        rs = ps_f_r.executeQuery();
        rsmd_rs = rs.getMetaData();
        assertNotNull(rsmd_rs);

        // check column count
        assertEquals(rsmd_ps.getColumnCount(), rsmd_rs.getColumnCount());

        // get column name for 2nd column
        assertEquals(rsmd_ps.getColumnName(2), rsmd_rs.getColumnName(2));
        assertEquals(rsmd_ps.isReadOnly(2), rsmd_rs.isReadOnly(2));

        rs.close();
        ps_f_r.close();

    }

    public void testScrollVerifyMaxRowWithFetchSize() throws SQLException {
        scrollVerifyMaxRowWithFetchSize(10, 10);
        scrollVerifyMaxRowWithFetchSize(10, 5);
        scrollVerifyMaxRowWithFetchSize(10, 0);
        scrollVerifyMaxRowWithFetchSize(0, 0);
        scrollVerifyMaxRowWithFetchSize(0, 5);
        scrollVerifyMaxRowWithFetchSize(0, 10);
        scrollVerifyMaxRowWithFetchSize(0, 15);
    }

    /**
     * Tests for maxRow and fetchSize with scrollable cursors
     * 
     * 
     * @param maxRows
     *            The maxRows value to use
     * @param fetchSize
     *            The fetchSize value to use
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    private void scrollVerifyMaxRowWithFetchSize(int maxRows, int fetchSize)
            throws SQLException {

        ResultSet rs;
        Statement s_i_r = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        s_i_r.setMaxRows(maxRows);

        // Execute query
        rs = s_i_r
                .executeQuery("values 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15");
        rs.setFetchSize(fetchSize);

        // this should not affect the ResultSet because
        s_i_r.setMaxRows(2);
        if (maxRows == 0)
            maxRows = 15;
        assertNotNull(rs);

        // Start from before first
        // Iterate straight thru RS, expect only maxRows rows.
        for (int index = 1; index < maxRows + 1; index++) {
            assertTrue("rs.next() failed, index = " + index, rs.next());
            assertEquals(index, rs.getInt(1));

        }
        // We should not see another row (only maxRows, not total)
        assertFalse(rs.next());

        // Start from first and verify maxRows
        assertTrue(rs.first());

        // Iterate forward thru RS, expect only (maxRows - 1) more rows.
        for (int index = 1; index < maxRows; index++) {
            assertTrue(rs.next());
            assertEquals(index + 1, rs.getInt(1));

        }
        // We should not see another row (only maxRows, not total)
        assertFalse(rs.next());

        // Start from afterLast and verify maxRows
        rs.afterLast();
        // Iterate backwards thru RS, expect only (maxRows - 1) rows.
        for (int index = 1; index < maxRows + 1; index++) {
            assertTrue(rs.previous());
            assertEquals(maxRows - index + 1, rs.getInt(1));
        }
        // We should not see another row (only maxRows, not total)
        assertFalse(rs.previous());

        // Start from last and verify maxRows
        assertTrue(rs.last());

        // Iterate backwards thru RS, expect only (maxRows - 1) more rows.
        for (int index = 1; index < maxRows; index++) {
            assertTrue(rs.previous());
            assertEquals((maxRows - index), rs.getInt(1));

        }
        // We should not see another row (only 5, not 6)
        assertFalse(rs.previous());
        rs.last();
        int rows = rs.getRow();

        rs.absolute(rows / 2);
        assertFalse(rs.relative(-1 * (rows)));
        assertTrue(rs.isBeforeFirst());

        rs.absolute(rows / 2);
        assertFalse(rs.relative(rows));
        assertTrue(rs.isAfterLast());
        rs.absolute(rows / 2);
        assertFalse("absolute(" + (rows + 1)
                + ") should return false, position outside of the resultSet",
                rs.absolute(rows + 1));

        rs.absolute(rows / 2);
        assertFalse(rs.absolute((-1) * (rows + 1)));

        assertTrue(rs.isBeforeFirst());

        rs.close();

    }

    private void assertOnlyOnScrollableException(SQLException sqle) {
        if (!isDerbyNetClient) {
            assertEquals("XJ061", sqle.getSQLState());
        } else {
            assertEquals("XJ125", sqle.getSQLState());
        }
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(ScrollCursors2Test.class);
    }

}
