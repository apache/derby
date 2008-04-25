/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.LOBLocatorReleaseTest

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;

import junit.framework.Test;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests of accessing large objects (LOBs) with locators.
 */
public class LOBLocatorReleaseTest
        extends BaseJDBCTestCase {

    public LOBLocatorReleaseTest(String name) {
        super(name);
    }

    /**
     * Tests that the code path for LOB locator release works fine for result
     * sets without LOBs.
     *
     * @throws SQLException if the test fails for some reason
     */
    public void testNoLOBs()
            throws SQLException {
        // Test a forward only result set, with autocommit.
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("select * from sys.systables");
        while (rs.next()) {
            // Do nothing, just iterate through.
        }
        rs.close();

        // Basic test checking that the scrollable result code path works.
        stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                               ResultSet.CONCUR_READ_ONLY);
        getConnection().setAutoCommit(false);
        rs = stmt.executeQuery("select * from sys.systables");
        rs.absolute(3);
        while (rs.next()) {
            // Do nothing, just iterate through.
        }
        // Just navigate randomly.
        rs.previous();
        rs.absolute(2);
        rs.relative(2);
        rs.afterLast();
        rs.first();
        rs.next();
        rs.last();
        rs.beforeFirst();
        // Close the statement instead of the result set first.
        stmt.close();
        rs.close();
        rollback();
    }

    /**
     * Test basic operations on forward only result sets.
     *
     * @throws SQLException if something causes the test to fail
     */
    public void testForwardOnlyWithNoNulls()
            throws SQLException {
        forwardOnlyTest("LOBLOC_NO_NULLS");
    }

    /**
     * Test basic operations on forward only result sets containing NULL LOBs.
     * <p>
     * This requires some special care because NUL LOBs don't have a locator.
     *
     * @throws SQLException if something causes the test to fail
     */
    public void testForwardOnlyWithNulls()
            throws SQLException {
        forwardOnlyTest("LOBLOC_WITH_NULLS");
    }

    private void forwardOnlyTest(String table)
            throws SQLException {
        final String sql = "select dBlob, dClob from " + table;
        getConnection().setAutoCommit(false);
        // Just loop through.
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            // Just iterate through.
        }
        rs.close();

        // Loop through and get references to some of the LOBs.
        // When you get a LOB reference, the locator shuold only be freed on
        // explicit calls to free (requires Java SE 6) or commit/rollback.
        rs = stmt.executeQuery(sql);
        int index = 0;
        while (rs.next()) {
            if (index % 2 == 0) {
                Blob b = rs.getBlob(1);
                if (!rs.wasNull()) {
                    b.length();
                }
            }
            if (index % 3 == 0) {
                Clob c = rs.getClob(2);
                if (!rs.wasNull()) {
                    c.length();
                }
            }
            // Clear all LOB mappings after 10 rows.
            if (index == 9) {
                commit();
            }
            index++;
        }
        rs.close();
        stmt.close();

        // Close the statement after a few rows.
        stmt = createStatement();
        rs = stmt.executeQuery(sql);
        rs.next();
        rs.next();
        stmt.close();
        // The LOB mapping is cleared on a commit.
        commit();

        // Close the result set after a few rows and a rollback.
        stmt = createStatement();
        rs = stmt.executeQuery(sql);
        rs.next();
        rs.next();
        rollback();
        rs.close();
    }

    /**
     * Tests that the LOB objects are not closed when closing the result set.
     *
     * @throws SQLException if something causes the test to fail
     */
    public void testBlobClobStateForwardOnlyWithNoNulls()
            throws SQLException {
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, dClob from LOBLOC_NO_NULLS");
        rs.next();
        Blob b = rs.getBlob(1);
        final long blobLength = b.length();
        rs.next();
        Clob c = rs.getClob(2);
        final long clobLength = c.length();
        rs.next();
        rs.close();
        // The LOB objects should still be usable.
        assertEquals(blobLength, b.length());
        assertEquals(clobLength, c.length());
        commit();
        try {
            // This should fail because the locator has been released.
            c.getSubString(1, 9);
            fail("Locator should have been released, causing the call to fail");
        } catch (SQLException sqle) {
            assertSQLState("XJ215", sqle);
        }
    }

    /**
     * Tests that the LOB objects are not closed when closing the result set.
     *
     * @throws SQLException if something causes the test to fail
     */
    public void testBlobClobStateAfterCloseOnScrollable()
            throws SQLException {
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                         ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery(
                "select dBlob, dClob from LOBLOC_NO_NULLS");
        rs.next();
        rs.relative(5);
        Blob b = rs.getBlob(1);
        final long blobLength = b.length();
        rs.next();
        Clob c = rs.getClob(2);
        final long clobLength = c.length();
        rs.first();
        rs.close();
        // The LOB objects should still be usable.
        assertEquals(blobLength, b.length());
        assertEquals(clobLength, c.length());
        commit();
        try {
            // This should fail because the locator has been released.
            c.getSubString(1, 9);
            fail("Locator should have been released, causing the call to fail");
        } catch (SQLException sqle) {
            assertSQLState("XJ215", sqle);
        }
    }
    /**
     * Test navigation on a scrollable result set with LOB columns.
     */
    public void testScrollableWithNoNulls()
            throws SQLException {
        scrollableTest("LOBLOC_NO_NULLS", ResultSet.CONCUR_READ_ONLY);
        scrollableTest("LOBLOC_NO_NULLS", ResultSet.CONCUR_UPDATABLE);
    }

    /**
     * Test navigation on a scrollable result set with LOB columns containing
     * some NULL values.
     */
    public void testScrollableWithNulls()
            throws SQLException {
        scrollableTest("LOBLOC_WITH_NULLS", ResultSet.CONCUR_READ_ONLY);
        scrollableTest("LOBLOC_WITH_NULLS", ResultSet.CONCUR_UPDATABLE);
    }

    /**
     * Tests a sequence of operations on a scrollable result set.
     *
     * @param table the table to query
     * @param rsConcurrency the result set concurrency
     */
    private void scrollableTest(String table, int rsConcurrency)
            throws SQLException {
        final String sql = "select dBlob, dClob from " + table;
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                         rsConcurrency);
        ResultSet rs = stmt.executeQuery(sql);
        // Just iterate through and close.
        while (rs.next()) {}
        rs.close();

        // Do some random navigation.
        rs = stmt.executeQuery(sql);
        rs.next();
        rs.beforeFirst();
        rs.first();
        rs.relative(3);
        rs.previous();
        rs.last();
        rs.absolute(5);
        rs.afterLast();
        rs.next();
    }

    /**
     * Tests that the cursor can be positioned on the current row multiple
     * times on a scrollable resultset.
     * <p>
     * The motivation for the test is that the locators assoicated with the
     * current row must not be released multiple times.
     */
    public void testScrollableMoveToCurrentRow()
            throws SQLException {
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                         ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "select dBlob, dClob from LOBLOC_NO_NULLS");
        rs.next();
        rs.moveToCurrentRow();
        rs.moveToCurrentRow();
    }

    /**
     * Tests that absolute positioning can be called for the same row multiple
     * times on a scrollable resultset.
     */
    public void testScrollableAbsoluteRow()
            throws SQLException {
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                         ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "select dBlob, dClob from LOBLOC_NO_NULLS");
        rs.next();
        rs.absolute(4);
        rs.absolute(4);
        rs.absolute(4);
    }

    /**
     * Tests a sequence of operations on a scrollable, updatable resultset.
     *
     * @throws SQLException if the test fails
     */
    public void testScrollableUpdateWithLocators()
            throws SQLException {
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                         ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery(
                "select dBlob, dClob from LOBLOC_NO_NULLS");
        rs.absolute(3);
        Clob c1 = rs.getClob(2);
        final int origLength = (int)c1.length();
        final String origContent = c1.getSubString(1, origLength);
        // Do a change
        c1.setString(origLength, "FIRSTPASS");
        rs.absolute(7);
        rs.next();
        // Move back to row 3
        rs.absolute(3);
        Clob c2 = rs.getClob(2);
        assertEquals(origContent, c2.getSubString(1, (int)c2.length()));
        rs.updateRow(); // Should be a no-op
        rs.absolute(3);
        // Expect this to fail if the restriction that LOB columns cannot be
        // accessed more than once is enforced.
        Clob c3 = rs.getClob(2);
        assertEquals(origContent, c3.getSubString(1, (int)c3.length()));
        rs.previous();
        rs.next();
        Clob c4 = rs.getClob(2);
        final String newContent = "THIS IS THE NEW VALUE!";
        c4.setString(1, newContent);
        rs.updateClob(2, c4);
        rs.updateRow();
        c4.setString(1, "THIS IS NOT NOT NOT THE NEW VALUE!");
        rs.updateRow();
        rs.next();
        rs.absolute(3);
        Clob c5 = rs.getClob(2);
        assertEquals(newContent, c5.getSubString(1, (int)c5.length()));
        rollback();
        assertInvalid(c1);
        assertInvalid(c2);
        assertInvalid(c3);
        assertInvalid(c4);
        assertInvalid(c5);
    }

    /**
     * Asserts that the Clob is invalid by invoking a method on it (that is
     * supposed to fail) and catching the exception. Fails if no exception is
     * thrown, or the wrong exception is thrown.
     *
     * @param clob the Clob to check
     */
    private void assertInvalid(Clob clob) {
        try {
            clob.getSubString(1, (int)clob.length());
            fail("Clob should have been invalidated");
        } catch (SQLException sqle) {
            assertSQLState("XJ215", sqle);
        }
    }

    /**
     * Returns a default suite running in a client-server environment.
     * <p>
     * The tests in this class is only meant to be run with client-server.
     *
     * @return A test suite.
     */
    public static Test suite() {
        return new CleanDatabaseTestSetup(
                TestConfiguration.clientServerSuite(
                                                LOBLocatorReleaseTest.class)) {
            /**
             * Populates two tables with LOB data.
             */
            protected void decorateSQL(Statement s) throws SQLException {
                s.executeUpdate("create table LOBLOC_NO_NULLS " +
                        "(dBlob BLOB not null, dClob CLOB not null)");
                Connection con = s.getConnection();
                PreparedStatement ps = con.prepareStatement(
                        "insert into LOBLOC_NO_NULLS values (?,?)");
                String cContent = "A little test Clob";
                byte[] bContent;
                try {
                    bContent = cContent.getBytes("US-ASCII");
                } catch (UnsupportedEncodingException uee) {
                    SQLException sqle = new SQLException();
                    sqle.initCause(uee);
                    throw sqle;
                }
                for (int i=0; i < 25; i++) {
                    ps.setBytes(1, bContent);
                    ps.setString(2, cContent);
                    ps.executeUpdate();
                }
                ps.close();
                s.executeUpdate("create table LOBLOC_WITH_NULLS " +
                        "(dBlob BLOB, dClob CLOB)");
                ps = con.prepareStatement(
                        "insert into LOBLOC_WITH_NULLS values (?,?)");
                for (int i=0; i < 25; i++) {
                    if (i % 3 == 0) {
                        ps.setNull(1, Types.BLOB);
                    } else {
                        ps.setBytes(1, bContent);
                    }
                    if (i % 4 == 0) {
                        ps.setNull(2, Types.CLOB);
                    } else {
                        ps.setString(2, cContent);
                    }
                    ps.executeUpdate();
                }
                ps.close();
                con.commit();
            }
        };
    }
}
