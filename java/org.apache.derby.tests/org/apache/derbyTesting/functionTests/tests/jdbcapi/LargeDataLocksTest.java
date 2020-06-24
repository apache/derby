/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.LargeDataLocksTest
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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.Utilities;

public class LargeDataLocksTest extends BaseJDBCTestCase {

    public LargeDataLocksTest(String name) {
        super(name);
    }

    /**
     * Test that ResultSet.getCharacterStream does not hold locks after the
     * ResultSet is closed
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testGetCharacterStream() throws SQLException, IOException {
        // getCharacterStream() no locks expected after retrieval
        int numChars = 0;
        Statement stmt = createStatement();
        String sql = "SELECT bc from t1";
        // First with getCharacterStream
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        java.io.Reader characterStream = rs.getCharacterStream(1);
        // Extract all the characters
        int read = characterStream.read();
        while (read != -1) {
            read = characterStream.read();
            numChars++;
        }
        assertEquals(38000, numChars);
        rs.close();
        assertLockCount(0);
        commit();
    }

    /**
     * Verify that getBytes does not hold locks after ResultSet is closed.
     * 
     * @throws SQLException
     */
    public void testGetBytes() throws SQLException {
        // getBytes() no locks expected after retrieval
        Statement stmt = createStatement();
        String sql = "SELECT bincol from t1";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        byte[] value = rs.getBytes(1);
        assertEquals(38000, value.length);
        rs.close();
        assertLockCount(0);
        commit();

    }

    /**
     * Verify that getBinaryStream() does not hold locks after retrieval
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testGetBinaryStream() throws SQLException, IOException {
        int numBytes = 0;
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement();
        String sql = "SELECT bincol from t1";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        InputStream stream = rs.getBinaryStream(1);
        int read = stream.read();
        while (read != -1) {
            read = stream.read();
            numBytes++;
        }
        assertEquals(38000, numBytes);
        rs.close();
        assertLockCount(0);
        commit();
    }

    /**
     * Test that ResultSet.getString() does not hold locks after the ResultSet
     * is closed
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testGetString() throws SQLException, IOException {
        // getString() no locks expected after retrieval
        Statement stmt = createStatement();
        String sql = "SELECT bc from t1";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        String value = rs.getString(1);
        assertEquals(38000, value.length());
        rs.close();
//IC see: https://issues.apache.org/jira/browse/DERBY-4825
//IC see: https://issues.apache.org/jira/browse/DERBY-4825
//IC see: https://issues.apache.org/jira/browse/DERBY-4825
//IC see: https://issues.apache.org/jira/browse/DERBY-4825
        assertLockCount(0);
        commit();
    }

    /**
     * Assert that the lock table contains a certain number of locks. Fail and
     * dump the contents of the lock table if the lock table does not contain
     * the expected number of locks.
     *
     * @param expected the expected number of locks
     */
    private void assertLockCount(int expected) throws SQLException {
        // Count the locks in a new connection so that we don't accidentally
        // make the default connection auto-commit and release locks.
        Connection conn = openDefaultConnection();
        Statement stmt = conn.createStatement();

        // First wait for post-commit work to complete so that we don't count
        // locks held by the background worker thread.
        stmt.execute("call wait_for_post_commit()");
//IC see: https://issues.apache.org/jira/browse/DERBY-4825

        ResultSet rs = stmt.executeQuery("select * from syscs_diag.lock_table");
        ResultSetMetaData meta = rs.getMetaData();

        // Build an error message with the contents of the lock table as
        // we walk through it.
        StringBuffer msg = new StringBuffer(
                "Unexpected lock count. Contents of lock table:\n");
        int count;
        for (count = 0; rs.next(); count++) {
            msg.append(count + 1).append(": ");
            for (int col = 1; col <= meta.getColumnCount(); col++) {
                String name = meta.getColumnName(col);
                Object val = rs.getObject(col);
                msg.append(name).append('=').append(val).append(' ');
            }
            msg.append('\n');
        }

        rs.close();
        stmt.close();
        conn.close();

        assertEquals(msg.toString(), expected, count);
    }

    public static Test baseSuite(String name) {

//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(LargeDataLocksTest.class);

        return new CleanDatabaseTestSetup(suite) {

            /**
             * Create and populate table
             * @see org.apache.derbyTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
             */
            protected void decorateSQL(Statement s) throws SQLException {
                Connection conn = getConnection();
                conn.setAutoCommit(false);
                PreparedStatement ps = null;
                String sql;

                sql = "CREATE TABLE t1 (bc CLOB(1M), bincol BLOB(1M), datalen int)";
                s.executeUpdate(sql);

                // Insert big and little values
                sql = "INSERT into t1 values(?,?,?)";
                ps = conn.prepareStatement(sql);

                ps.setCharacterStream(1, new java.io.StringReader(Utilities
                        .repeatChar("a", 38000)), 38000);
//IC see: https://issues.apache.org/jira/browse/DERBY-3780
                try {
                    ps.setBytes(2, Utilities.repeatChar("a", 38000).getBytes("US-ASCII"));
                } catch (UnsupportedEncodingException ue) {
                    // Shouldn't happen US-ASCII should always be supported
                    BaseTestCase.fail(ue.getMessage(), ue);
                }
                ps.setInt(3, 38000);
                ps.executeUpdate();
                ps.close();

                // Create a procedure for use by assertLockCount() to ensure
                // that the background worker thread has completed all the
                // post-commit work.
//IC see: https://issues.apache.org/jira/browse/DERBY-4825
                s.execute("CREATE PROCEDURE WAIT_FOR_POST_COMMIT() "
                        + "LANGUAGE JAVA EXTERNAL NAME "
                        + "'org.apache.derbyTesting.functionTests.util."
                        + "T_Access.waitForPostCommitToFinish' "
                        + "PARAMETER STYLE JAVA");

                conn.commit();
            }
        };
    }

    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("LargeDataLocksTest");
        suite.addTest(baseSuite("LargeDataLocksTest:embedded"));
        // Disable for client until DERBY-2892 is fixed
        suite.addTest(TestConfiguration.clientServerDecorator(baseSuite("LargeDataLocksTest:client")));
        return suite;

    }

}
