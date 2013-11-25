/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LockTableTest

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
// Note: This test could be refined by modifying the BaseJDBCTestCase
//       method assertStatementError(new String[],Statement,String)
//       and all methods down that chain to search for the variable
//       values in the SQL error messages as well, in this case, in this
//       case, to check for 'exclusive' or 'share' in error X0202.

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests the LOCK TABLE in various modes.
 */
public class LockTableTest extends BaseJDBCTestCase {

    public LockTableTest(String name) {
        super(name);
    }

    /**
     * Construct top level suite in this JUnit test
     * The suite is wrapped in a DatabasePropertyTestSetup to
     * lower the locking times.
     *
     * @return A suite containing embedded fixtures
     */
    public static Test suite() {
        Properties properties = new Properties();
        properties.setProperty("derby.storage.rowLocking", "false");
        properties.setProperty("derby.locks.waitTimeout", "7");
        properties.setProperty("derby.locks.deadlockTimeout", "5");

        Test suite = TestConfiguration.embeddedSuite (LockTableTest.class);
        suite = new DatabasePropertyTestSetup(suite, properties, true);
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the schemas and table used in the test cases.
             *
             * @throws SQLException
             */
            protected void decorateSQL(Statement s) throws SQLException {
                Connection conn = getConnection();
                conn.setAutoCommit(false);
                s.executeUpdate("create schema u1");
                s.executeUpdate("create schema u2");
                conn.commit();
            }
        };
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Tear-down the fixture by removing the tables and schemas
     * @throws Exception
     */
    protected void tearDown() throws Exception {
        // first drop t2 only - it is only used in one fixture.
        // if doing this in the other block, the schemas might not
        // get dropped.
        Statement s = createStatement();
/*        try {
            s.executeUpdate("drop table u1.t2");
        } catch(SQLException sqe) {
            if (!(sqe.getSQLState().equalsIgnoreCase("42X05") 
                    || sqe.getSQLState().equalsIgnoreCase("42Y55")))
            {
                fail("oops in teardown, encountered some other error than " +
                		"'object does not exist' or " +
                		"'cannot drop object because it doesn't exist'");
                sqe.printStackTrace();
            }
        }
        finally {
            s.close();
        }*/
        try {
            s = createStatement();
            s.executeUpdate("drop table u1.t1");
            s.executeUpdate("drop schema u1 restrict");
            s.executeUpdate("drop schema u2 restrict");
        } catch(SQLException sqe) {
            if (!(sqe.getSQLState().equalsIgnoreCase("42X05") 
                    || sqe.getSQLState().equalsIgnoreCase("42Y55")))
            {
                fail("oops in teardown, encountered some other error than " +
                		"'object does not exist' or " +
                		"'cannot drop object because it doesn't exist'");
                sqe.printStackTrace();
            }
        }
        finally {
            s.close();
        }
        super.tearDown();
    }

    /** sets up the connection for a user
     * 
     * @return A connection with autocommit set to false
     * @exception SQLException
     */
    private Connection setConnection(String userString) throws SQLException {
        Connection c1 = openUserConnection(userString);
        c1.setAutoCommit(false);
        return c1;
    }

    /* create table t1, used in most of the fixtures
     * 
     * @exception SQLException
     */
    private void setupTable(Statement s) throws SQLException {
        s.executeUpdate("create table t1(c1 int)");
        s.executeUpdate("insert into t1 values 1");
    }

    /* get the query to get the locks
     * 
     * @return String with the query
     */
    public static String getSelectLocksString() {
        String sql = 
                "select " + 
                        "cast(username as char(8)) as username, " +
                        "cast(t.type as char(15)) as trantype, " +
                        "cast(l.type as char(8)) as type, " +
                        "cast(lockcount as char(3)) as cnt, " +
                        "mode, " +
                        "cast(tablename as char(12)) as tabname, " +
                        "cast(lockname as char(10)) as lockname, " +
                        "state, " +
                        "status " +
                        "from syscs_diag.lock_table l " +
                        "right outer join syscs_diag.transaction_table t " +
                        "on l.xid = t.xid where l.tableType <> 'S' " +
                        "order by " +
                        "tabname, type desc, mode, cnt, lockname";
        return sql;
    }
    
    /**
     * Tests that LOCK TABLE is not allowed on system tables.
     * 
     * @exception SQLException
     */
    public void testSystemTable() throws SQLException {
        Statement s = createStatement();
        assertStatementError("42X62", s,
                "lock table sys.systables in share mode");
        s.close();
    }

    /**
     * Tests LOCK TABLE command - exclusive vs exclusive mode
     * 
     * @exception SQLException
     */
    public void testTXvsTXLocks() throws SQLException {
        //set up the connections;
        Connection c1 = setConnection("U1");
        Statement s1 = c1.createStatement();
        Connection c2 = setConnection("U2");
        Statement s2 = c2.createStatement();

        setupTable(s1);
        c1.commit();

        s1.executeUpdate("lock table u1.t1 in exclusive mode");
        // We expect X0X02 (Table cannot be locked 'EXCLUSIVE' mode) 
        // and 40XL1 (A lock could not be obtained within the time requested).
        assertStatementError(new String[] {"X0X02","40XL1"},s2,
                "lock table u1.t1 in exclusive mode");
        // verify we still have the lock
        ResultSet rs = s1.executeQuery(getSelectLocksString());
        JDBC.assertFullResultSet(rs, new String[][]{
                {"U1", "UserTransaction", "TABLE", "1",
                    "X", "T1", "Tablelock", "GRANT", "ACTIVE"}
        });
        // verify user 1 can insert into the table
        s1.executeUpdate("insert into t1 values 2");
        rs = s1.executeQuery("select count(*) from t1");
        JDBC.assertSingleValueResultSet(rs, "2");
        // But user 2 should not be able to insert
        assertStatementError("40XL1", s2, "insert into u1.t1 values 9");
        rs = s1.executeQuery("select count(*) from t1");
        JDBC.assertSingleValueResultSet(rs, "2");
        // but select should be ok
        rs = s1.executeQuery("select count(*) from u1.t1");
        JDBC.assertSingleValueResultSet(rs, "2");
        rs.close();
        c1.commit();
        s1.executeUpdate("drop table U1.t1");
        c1.commit();
        s1.close();
        s2.close();
        c1.close();
        c2.rollback();
        c2.close();
    }

    /**
     * Tests LOCK TABLE command - exclusive vs shared mode
     * 
     * @exception SQLException
     */
    public void testTXvsTSLocks() throws SQLException {
        Connection c1 = setConnection("U1");
        Statement s1 = c1.createStatement();
        Connection c2 = setConnection("U2");
        Statement s2 = c2.createStatement();

        setupTable(s1);
        c1.commit();

        // - test TX vs TS locks
        s1.executeUpdate("lock table t1 in exclusive mode");
        // We expect X0X02 (Table cannot be locked in 'SHARE' mode) 
        // and 40XL1 (A lock could not be obtained within the time requested).
        assertStatementError(new String[] {"X0X02","40XL1"},s2,
                "lock table u1.t1 in share mode");
        // verify we still have the lock
        ResultSet rs = s1.executeQuery(getSelectLocksString());
        JDBC.assertFullResultSet(rs, new String[][]{
                {"U1", "UserTransaction", "TABLE", "1",
                    "X", "T1", "Tablelock", "GRANT", "ACTIVE"}
        });
        // verify we can still insert into the table
        s1.executeUpdate("insert into t1 values 3");
        rs = s1.executeQuery("select count(*) from t1");
        JDBC.assertSingleValueResultSet(rs, "2");
        s1.executeUpdate("drop table U1.t1");
        c1.commit();
        s1.close();
        s2.close();
        c1.close();
        c2.rollback();
        c2.close();
    }

    /**
     * Tests LOCK TABLE command - shared vs exclusive mode
     * 
     * @exception SQLException
     */
    public void testTSvsTXLocks() throws SQLException {
        Connection c1 = setConnection("U1");
        Statement s1 = c1.createStatement();
        Connection c2 = setConnection("U2");
        Statement s2 = c2.createStatement();

        setupTable(s1);
        c1.commit();

        // -- test TS vs TX locks
        s1.executeUpdate("lock table t1 in share mode");
        // We expect X0X02 (Table cannot be locked in 'EXLUSIVE' mode) 
        // and 40XL1 (A lock could not be obtained within the time requested).
        assertStatementError(new String[] {"X0X02","40XL1"},s2,
                "lock table u1.t1 in exclusive mode");
        // verify we still have the lock
        ResultSet rs = s1.executeQuery(getSelectLocksString());
        JDBC.assertFullResultSet(rs, new String[][]{
                {"U1", "UserTransaction", "TABLE", "1",
                    "S", "T1", "Tablelock", "GRANT", "ACTIVE"}
        });
        // verify insert
        s1.executeUpdate("insert into t1 values 4");
        rs = s1.executeQuery("select count(*) from t1");
        JDBC.assertSingleValueResultSet(rs, "2");
        s1.executeUpdate("drop table U1.t1");
        c1.commit();
        s1.close();
        s2.close();
        c1.close();
        c2.rollback();
        c2.close();
    }

    /**
     * Tests LOCK TABLE command - shared vs shared mode
     * 
     * @exception SQLException
     */
    public void testTSvsTSLocks() throws SQLException {
        Connection c1 = setConnection("U1");
        Statement s1 = c1.createStatement();
        Connection c2 = setConnection("U2");
        Statement s2 = c2.createStatement();

        setupTable(s1);
        c1.commit();

        // -- test TS vs TS locks
        s1.executeUpdate("lock table t1 in share mode");
        // expect success on lock, but now user 1 may not update.
        assertUpdateCount(s2, 0, "lock table u1.t1 in share mode");
        // verify we have two locks
        ResultSet rs = s1.executeQuery(getSelectLocksString());
        JDBC.assertFullResultSet(rs, new String[][]{
                {"U2", "UserTransaction", "TABLE", "1",
                    "S", "T1", "Tablelock", "GRANT", "ACTIVE"},
                {"U1", "UserTransaction", "TABLE", "1",
                    "S", "T1", "Tablelock", "GRANT", "ACTIVE"}
        });
        // verify that with a share lock for user 2 place, user 1 cannot insert
        assertStatementError("40XL1", s1, "insert into t1 values 5");
        rs = s1.executeQuery("select count(*) from t1");
        JDBC.assertSingleValueResultSet(rs, "1");
        c2.rollback();
        c1.rollback();
        s1.executeUpdate("drop table U1.t1");
        c1.commit();
        s1.close();
        s2.close();
        c1.close();
        c2.close();
    }

    /**
     * test with rollback.
     * 
     * @exception SQLException
     */
    public void testWithRolledBack() throws SQLException {
        Connection c1 = setConnection("U1");
        Statement s1 = c1.createStatement();
        Connection c2 = setConnection("U2");
        Statement s2 = c2.createStatement();

        setupTable(s1);
        c1.commit();

        // -- create another table
        s1.executeUpdate("create table t2(c1 int)");
        c1.commit();

        // verify that the user getting error on lock table
        // doesn't get rolled back, so other locks remain in  place.
        s1.executeUpdate("lock table t1 in share mode");
        s2.executeUpdate("lock table u1.t2 in share mode");
        // Attempt to lock t1 in exclusive mode, while it has been share-locked.
        // We expect X0X02 (Table cannot be locked in 'EXCLUSIVE' mode) 
        // and 40XL1 (A lock could not be obtained within the time requested).
        assertStatementError(new String[] {"X0X02","40XL1"},s2,
                "lock table u1.t1 in exclusive mode");
        // verify the other user still has the lock
        ResultSet rs = s1.executeQuery(getSelectLocksString());
        JDBC.assertFullResultSet(rs, new String[][]{
                {"U1", "UserTransaction", "TABLE", "1",
                    "S", "T1", "Tablelock", "GRANT", "ACTIVE"},
                {"U2", "UserTransaction", "TABLE", "1",
                    "S", "T2", "Tablelock", "GRANT", "ACTIVE"}
        });
        c2.rollback();
        c1.rollback();
        s1.executeUpdate("drop table U1.t2");
        s1.executeUpdate("drop table U1.t1");
        c1.commit();
        s1.close();
        s2.close();
        c1.close();
        c2.close();
    }
}
