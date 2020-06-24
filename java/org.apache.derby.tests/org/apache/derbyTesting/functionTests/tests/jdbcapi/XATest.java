/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.XATest
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


import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import static junit.framework.Assert.fail;
import junit.framework.Test;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derbyTesting.functionTests.util.DeadlockWatchdog;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import static org.apache.derbyTesting.junit.BaseJDBCTestCase.assertSQLState;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.Utilities;
import org.apache.derbyTesting.junit.XATestUtil;

public class XATest extends BaseJDBCTestCase {
    //create own policy file
    private static final String POLICY_FILE_NAME =
            "org/apache/derbyTesting/functionTests/tests/jdbcapi/XATest.policy";

    public static final String LOCKTIMEOUT="40XL1";
    
    public XATest(String name) {
        super(name);
//IC see: https://issues.apache.org/jira/browse/DERBY-2474

    }

    /**
     * A single connection and 1 phase commit.
     * 
     * 
     * Original "SQL" from xaSimplePositive.sql <code>
     xa_connect ;
     xa_start xa_noflags 0;
     xa_getconnection;
     drop table foo;
     create table foo (a int);
     insert into foo values (0);
     select * from foo;
     run resource '/org/apache/derbyTesting/functionTests/tests/store/global_xactTable.view';
     select * from global_xactTable where gxid is not null order by gxid;
     xa_end xa_success 0;
     xa_commit xa_1phase 0;
     
     xa_datasource 'wombat' shutdown;
     </code>
     * 
     * @throws SQLException
     * @throws XAException
     * @throws XAException
     */
    public void testSingleConnectionOnePhaseCommit() throws SQLException,
            XAException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2474

        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "wombat");

        XAConnection xac = xads.getXAConnection();

        XAResource xar = xac.getXAResource();

        Xid xid = XATestUtil.getXid(0, 32, 46);

        xar.start(xid, XAResource.TMNOFLAGS);

        Connection conn = xac.getConnection();
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

        Statement s = conn.createStatement();
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, s
                .getResultSetHoldability());

        s.execute("create table foo (a int)");
        s.executeUpdate("insert into foo values (0)");

        ResultSet rs = s.executeQuery("select * from foo");
        JDBC.assertDrainResults(rs, 1);

        String[][] expectedRows = { { "(0", "ACTIVE", "false", "APP",
                "UserTransaction" } };

        XATestUtil.checkXATransactionView(conn, expectedRows);

        s.close();
        xar.end(xid, XAResource.TMSUCCESS);

        // 1 phase commit
        xar.commit(xid, true);

        conn.close();
        xac.close();

    }

    /*
     * Two interleaving transaction and prepare/commit prepare/rollback.
     * 
     * (original test said two connections but only one connection was opened)
     * 
     * <code> xa_datasource 'wombat'; xa_connect user 'sku' password 'testxa' ;
     * 
     * xa_start xa_noflags 1; xa_getconnection; insert into APP.foo values (1);
     * xa_end xa_suspend 1;
     * 
     * xa_start xa_noflags 2; insert into APP.foo values (2); xa_end xa_suspend
     * 2;
     * 
     * xa_start xa_resume 1; insert into APP.foo values (3); xa_end xa_suspend
     * 1;
     * 
     * xa_start xa_resume 2; insert into APP.foo values (4); select * from
     * APP.global_xactTable where gxid is not null order by gxid; -- this
     * prepare won't work since transaction 1 has been suspended - XA_PROTO
     * xa_prepare 1;
     * 
     * select * from APP.global_xactTable where gxid is not null order by gxid;
     * xa_end xa_success 2; -- this assumes a resume xa_end xa_success 1;
     * xa_prepare 1; xa_prepare 2; -- both transactions should be prepared
     * select * from APP.global_xactTable where gxid is not null order by gxid; --
     * NOTE: The following call to "xa_recover xa_startrscan" is apt to --
     * return the result set rows in reverse order when changes to -- the Derby
     * engine affect the number of transactions that it takes -- to create a
     * database. The transactions are stored in a hash table -- based on a
     * global and local id, and when the number of transactions -- changes, the
     * (internal) local id can change, which may lead to a -- change in the
     * result set order. This order is determined by the -- JVM's hashing
     * algorithm. Examples of changes to the engine that -- can affect this
     * include ones that cause more commits or that -- change the amount of data
     * being stored, such as changes to the -- metadata statements (which is
     * what prompted this explanation in -- the first place). Ultimately, the
     * problem is that there is no -- way to order the return values from
     * "xa_recover" since it is an -- ij internal statement, not SQL...
     * xa_recover xa_startrscan; xa_recover xa_noflags;
     * 
     * xa_commit xa_2Phase 1; xa_rollback 2; -- check results xa_start
     * xa_noflags 3; select * from APP.global_xactTable where gxid is not null
     * order by gxid; select * from APP.foo; xa_end xa_success 3;
     * 
     * xa_prepare 3; -- should fail with XA_NOTA because we prepared a read only
     * transaction xa_commit xa_1Phase 3; disconnect; </code>
     */
    public void testInterleavingTransactions() throws SQLException, XAException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        Statement preStatement = getConnection().createStatement();
        preStatement.execute("create table fooInterleaving (a int)");
        preStatement.execute("insert into fooInterleaving values (0)");
        preStatement.close();
        
        XADataSource xads = J2EEDataSource.getXADataSource();

        XAConnection xac = xads.getXAConnection("sku", "testxa");
        XAResource xar = xac.getXAResource();

        Xid xid1 = XATestUtil.getXid(1, 93, 18);
        Xid xid2 = XATestUtil.getXid(2, 45, 77);

        xar.start(xid1, XAResource.TMNOFLAGS);

        Connection conn = xac.getConnection();

        Statement s = conn.createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        s.executeUpdate("insert into APP.fooInterleaving values (1)");
        xar.end(xid1, XAResource.TMSUSPEND);

        xar.start(xid2, XAResource.TMNOFLAGS);
        s.executeUpdate("insert into APP.fooInterleaving values (2)");
        xar.end(xid2, XAResource.TMSUSPEND);

        xar.start(xid1, XAResource.TMRESUME);
        s.executeUpdate("insert into APP.fooInterleaving values (3)");
        xar.end(xid1, XAResource.TMSUSPEND);

        xar.start(xid2, XAResource.TMRESUME);
        s.executeUpdate("insert into APP.fooInterleaving values (4)");

        String[][] expectedRows = {
                { "(1", "ACTIVE", "false", "SKU", "UserTransaction" },
                { "(2", "ACTIVE", "false", "SKU", "UserTransaction" } };

        XATestUtil.checkXATransactionView(conn, expectedRows);

        // this prepare won't work since
        // transaction 1 has been suspended - XA_PROTO
        try {
            xar.prepare(xid1);
            fail("FAIL - prepare on suspended transaction");
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_PROTO)
                XATestUtil.dumpXAException(
                        "FAIL - prepare on suspended transaction", e);

        }

        // check it was not prepared

        XATestUtil.checkXATransactionView(conn, expectedRows);

        xar.end(xid2, XAResource.TMSUCCESS);

        xar.end(xid1, XAResource.TMSUCCESS);

        xar.prepare(xid1);
        xar.prepare(xid2);

        // both should be prepared.
        expectedRows = new String[][] {
                { "(1", "PREPARED", "false", "SKU", "UserTransaction" },
                { "(2", "PREPARED", "false", "SKU", "UserTransaction" } };

        XATestUtil.checkXATransactionView(conn, expectedRows);

        Xid[] recoveredStart = xar.recover(XAResource.TMSTARTRSCAN);
        assertEquals(2, recoveredStart.length);
        Xid[] recovered = xar.recover(XAResource.TMNOFLAGS);
        assertEquals(0, recovered.length);
        Xid[] recoveredEnd = xar.recover(XAResource.TMENDRSCAN);
        assertEquals(0, recoveredEnd.length);

        for (int i = 0; i < recoveredStart.length; i++) {
            Xid xid = recoveredStart[i];
            if (xid.getFormatId() == 1) {
                // commit 1 with 2pc
                xar.commit(xid, false);
            } else if (xid.getFormatId() == 2) {
                xar.rollback(xid);
            } else {
                fail("FAIL: unknown xact");
            }
        }

        // check the results
        Xid xid3 = XATestUtil.getXid(3, 2, 101);
        xar.start(xid3, XAResource.TMNOFLAGS);
        expectedRows = new String[][] { { "(3", "IDLE", "NULL", "SKU",
                "UserTransaction" } };
        XATestUtil.checkXATransactionView(conn, expectedRows);
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        ResultSet rs = s.executeQuery("select * from APP.fooInterleaving");
        expectedRows = new String[][] { { "0" }, { "1" }, { "3" } };
        JDBC.assertFullResultSet(rs, expectedRows);

        rs.close();
        xar.end(xid3, XAResource.TMSUCCESS);

        int pr = xar.prepare(xid3);
        if (pr != XAResource.XA_RDONLY)
            fail("FAIL - prepare on read only xact returned " + pr);

        try {
            xar.commit(xid3, true);
            fail("FAIL - 2pc commit on read-only xact");
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }

        s.close();
        conn.close();
        xac.close();

    }

    /**
     * Tests on INIT STATE (no tr Original SQL from xaStateTran.sql. <code>

     -- the following should error XAER_NOTA
     xa_start xa_join 11;
     -- the following should error XAER_NOTA
     xa_start xa_resume 11;
     -- the following should error XAER_NOTA
     xa_end xa_success 11;
     -- the following should error XAER_NOTA
     xa_end xa_fail 11;
     -- the following should error XAER_NOTA
     xa_end xa_suspend 11;
     -- the following should error XAER_NOTA
     xa_prepare 11;
     -- the following should error XAER_NOTA
     xa_commit xa_1phase 11;
     -- the following should error XAER_NOTA
     xa_commit xa_2phase 11;
     -- the following should error XAER_NOTA
     xa_rollback 11;
     -- the following should error XAER_NOTA
     xa_forget 11;
     </code>
     */
    public void testNoTransaction() throws SQLException, XAException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2474
        XADataSource xads = J2EEDataSource.getXADataSource();
        XAConnection xac = xads.getXAConnection();
        XAResource xar = xac.getXAResource();

        Xid xid11 = XATestUtil.getXid(11, 3, 128);

        try {
            xar.start(xid11, XAResource.TMJOIN);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }

        try {
            xar.start(xid11, XAResource.TMRESUME);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }

        try {
            xar.end(xid11, XAResource.TMSUCCESS);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }
        try {
            xar.end(xid11, XAResource.TMFAIL);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }

        try {
            xar.end(xid11, XAResource.TMSUSPEND);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }

        try {
            xar.prepare(xid11);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }
        try {
            xar.commit(xid11, false);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }
        try {
            xar.commit(xid11, true);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }
        try {
            xar.rollback(xid11);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }
        try {
            xar.forget(xid11);
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_NOTA)
                throw e;
        }
    }

    /**
     * Morph a connection between local anf global transactions.
     */
    public void testMorph() throws SQLException, XAException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        Statement preStatement = getConnection().createStatement();
        preStatement.execute("create table fooMorph (a int)");
        preStatement.executeUpdate("insert into APP.fooMorph values (0)");
        preStatement.executeUpdate("insert into APP.fooMorph values (1)");
        preStatement.executeUpdate("insert into APP.fooMorph values (2)");
        preStatement.executeUpdate("insert into APP.fooMorph values (3)");
        preStatement.executeUpdate("insert into APP.fooMorph values (4)");
        preStatement.close();
        
        XADataSource xads = J2EEDataSource.getXADataSource();
        XAConnection xac = xads.getXAConnection();

        XAResource xar = xac.getXAResource();

        Connection conn = xac.getConnection();

        /*
         * autocommit off; insert into foo values (1); select * from
         * global_xactTable where gxid is not null order by gxid,username;
         * commit;
         */
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        s.executeUpdate("insert into APP.fooMorph values (2001)");
        // no rows expected
        XATestUtil.checkXATransactionView(conn, null);
        conn.commit();

        /*
         * autocommit on; insert into foo values (2); select * from
         * global_xactTable where gxid is not null order by gxid,username;
         * 
         */

        conn.setAutoCommit(true);
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        s.executeUpdate("insert into APP.fooMorph values (2002)");
        XATestUtil.checkXATransactionView(conn, null);

        /*
         * -- morph the connection to a global transaction xa_start xa_noflags
         * 1; select * from global_xactTable where gxid is not null order by
         * gxid,username; insert into foo values (3);
         */

        Xid xid = XATestUtil.getXid(1001, 66, 13);
        xar.start(xid, XAResource.TMNOFLAGS);
        String[][] expectedRows = { { "(1", "IDLE", "NULL", "APP",
                "UserTransaction" } };
        XATestUtil.checkXATransactionView(conn, expectedRows);
        s.executeUpdate("insert into APP.fooMorph values (2003)");
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155

        /*
         * -- disallowed commit; -- disallowed rollback; -- disallowed
         * autocommit on; -- OK autocommit off;
         */
        try {
            conn.commit();
            fail("FAIL: commit allowed in global xact");
        } catch (SQLException e) {
        }

        try {
            conn.rollback();
            fail("FAIL: roll back allowed in global xact");
        } catch (SQLException e) {
        }
        try {
            conn.setAutoCommit(true);
            fail("FAIL: setAutoCommit(true) allowed " + "in global xact");
        } catch (SQLException e) {
        }
        try {
            conn.setSavepoint();
            fail("FAIL: setSavepoint() allowed in global xact");
        } catch (SQLException e) {
        }
        try {
            conn.setSavepoint("badsavepoint");
            fail("FAIL: setSavepoint(String) allowed in " + "global xact");
        } catch (SQLException e) {
        }

        conn.setAutoCommit(false);

        // s was created in local mode so it has holdibilty
        // set, will execute but ResultSet will have close on commit

        // DERBY-1158 query with holdable statement
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        s.executeQuery("select * from APP.fooMorph where A >= 2000").close();
        s.close();

        // statement created in global xact is CLOSE_CURSORS_AT_COMMIT
        s = conn.createStatement();
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, s
                .getResultSetHoldability());

        /*
         * select * from foo; xa_end xa_success 1; xa_prepare 1;
         */
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        ResultSet rs = s.executeQuery("select * from APP.fooMorph where A >= 2000");
        expectedRows = new String[][] { { "2001" }, { "2002" }, { "2003" } };

        rs.close();

        xar.end(xid, XAResource.TMSUCCESS);
        xar.prepare(xid);

        /*
         * -- dup id xa_start xa_noflags 1;
         */
        try {
            xar.start(xid, XAResource.TMNOFLAGS);
            fail("FAIL - start with duplicate XID");
        } catch (XAException e) {
            if (e.errorCode != XAException.XAER_DUPID)
                throw e;
        }

        /*
         * xa_start xa_noflags 2; -- still should disallow autommit; autocommit
         * on; -- still should disallow commit and rollback commit; rollback;
         * select * from global_xactTable where gxid is not null order by
         * gxid,username; xa_end xa_suspend 2;
         */

        Xid xid2 = XATestUtil.getXid(1002, 23, 3);
        xar.start(xid2, XAResource.TMNOFLAGS);
        try {
            conn.commit();
            fail("FAIL: commit allowed in global xact");
        } catch (SQLException e) {
        }
        try {
            conn.rollback();
            fail("FAIL: roll back allowed in global xact");
        } catch (SQLException e) {
        }
        try {
            conn.setAutoCommit(true);
            fail("FAIL: setAutoCommit(true) allowed in global xact");
        } catch (SQLException e) {
        }
        conn.setAutoCommit(false);

        xar.end(xid2, XAResource.TMSUSPEND);

        /*
         * -- get local connection again xa_getconnection;
         * 
         * insert into foo values (5); -- autocommit should be on by default;
         * commit;
         * 
         * autocommit off; insert into foo values (6); -- commit and rollback is
         * allowed on local connection rollback;
         * 
         * insert into foo values (6); commit;
         */
        conn = xac.getConnection();
        s = conn.createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        s.executeUpdate("insert into APP.fooMorph values (2005)");
        conn.commit();
        conn.setAutoCommit(false);
        s.executeUpdate("insert into APP.fooMorph values (2006)");
        conn.rollback();
        s.executeUpdate("insert into APP.fooMorph values (2007)");
        conn.commit();

        expectedRows = new String[][] {
                { "(1", "PREPARED", "false", "APP", "UserTransaction" },
                { "(1", "IDLE", "NULL", "APP", "UserTransaction" } };
        XATestUtil.checkXATransactionView(conn, expectedRows);
        /*
         * -- I am still able to commit other global transactions while I am
         * attached to a -- local transaction. xa_commit xa_2phase 1; xa_end
         * xa_success 2; xa_rollback 2;
         */
        xar.commit(xid, false);
        xar.end(xid2, XAResource.TMSUCCESS);
        xar.rollback(xid2);

        XATestUtil.checkXATransactionView(conn, null);
        rs = s.executeQuery("select * from APP.fooMorph where A >= 2000");
        expectedRows = new String[][] { { "2001" }, { "2002" }, { "2003" },
                { "2005" }, { "2007" } };
        JDBC.assertFullResultSet(rs, expectedRows);
        rs.close();
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
//IC see: https://issues.apache.org/jira/browse/DERBY-3319
        conn.rollback();
        conn.close();

        /*
         * xa_getconnection; select * from global_xactTable where gxid is not
         * null order by gxid,username; select * from foo; autocommit off;
         * delete from foo;
         */
        conn = xac.getConnection();
        conn.setAutoCommit(false);
        s = conn.createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        s.executeUpdate("delete from app.fooMorph");
        rs = s.executeQuery("select * from APP.fooMorph");
        JDBC.assertEmpty(rs);
        rs.close();

        /*
         * -- yanking a local connection away should rollback the changes
         */
        conn = xac.getConnection();
        conn.setAutoCommit(false);
        s = conn.createStatement();
        rs = s.executeQuery("select * from APP.fooMorph where A >= 2000");
        expectedRows = new String[][] { { "2001" }, { "2002" }, { "2003" },
                { "2005" }, { "2007" } };
        JDBC.assertFullResultSet(rs, expectedRows);

        /*
         * -- cannot morph it if the local transaction is not idle xa_start
         * xa_noflags 3; commit; -- now morph it to a global transaction
         * xa_start xa_noflags 3;
         */
        Xid xid3 = XATestUtil.getXid(1003, 27, 9);
        try {
            xar.start(xid3, XAResource.TMNOFLAGS);
            fail("FAIL XAResource.start on a global transaction with an active local transaction (autocommit false)");
        } catch (XAException xae) {
            if (xae.errorCode != XAException.XAER_OUTSIDE)
                throw xae;
        }
        conn.commit();
        xar.start(xid3, XAResource.TMNOFLAGS);

        /*
         * -- now I shouldn't be able to yank it xa_getconnection;
         */
        // DERBY-341 - client skip XAConnection with active local xact
        if (usingEmbedded()) {
            try {
                xac.getConnection();
                fail("FAIL: getConnection with active global xact");
            } catch (SQLException sqle) {
                assertSQLState("XJ059", sqle);
            }
        }
        /*
         * select * from foo; delete from foo;
         * 
         * xa_end xa_fail 3; xa_rollback 3; -- local connection again
         * xa_getconnection; select * from global_xactTable where gxid is not
         * null order by gxid,username; select * from foo;
         */
        s = conn.createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        s.executeUpdate("delete from APP.fooMorph");
        rs = s.executeQuery("select * from APP.fooMorph where A >= 2000");
        JDBC.assertEmpty(rs);

        rs.close();
        try {
            xar.end(xid3, XAResource.TMFAIL);
        } catch (XAException e) {
            if (e.errorCode != XAException.XA_RBROLLBACK)
                throw e;
        }
        xar.rollback(xid3);

        conn = xac.getConnection();
        s = conn.createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        rs = s.executeQuery("select * from APP.fooMorph where A >= 2000");
        expectedRows = new String[][] { { "2001" }, { "2002" }, { "2003" },
                { "2005" }, { "2007" } };
        JDBC.assertFullResultSet(rs, expectedRows);
        rs.close();

        s.close();
        conn.close();

    }

    /**
     * This test checks the fix on DERBY-4310, for not repreparing PreparedStatements
     * upon calling close() on them.
     */
    public void testDerby4310PreparedStatement() throws SQLException, XAException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "wombat");

        XAConnection xaconn = xads.getXAConnection();
       
        XAResource xar = xaconn.getXAResource();
        Xid xid = XATestUtil.getXid(1,93,18);
        
        /* Create the table and insert some records into it. */
        Connection conn = xaconn.getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("CREATE TABLE foo4310_PS (I INT)");

        conn.createStatement().executeUpdate("insert into APP.foo4310_PS values (0)");
        conn.createStatement().executeUpdate("insert into APP.foo4310_PS values (1)");
        conn.createStatement().executeUpdate("insert into APP.foo4310_PS values (2)");
        conn.commit();
        
        /* Prepare and execute the statement to be tested */
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM APP.foo4310_PS");
        ps.executeQuery().close();

        /* Start and end a transaction on the XAResource object */
        xar.start(xid, XAResource.TMNOFLAGS);
        xar.end(xid, XAResource.TMSUCCESS);
        
        /* Drop the table on a parallel, regular connection */
        Connection conn2 = getConnection();
        Statement s2 = conn2.createStatement();
        s2.execute("DROP TABLE foo4310_PS");
        conn2.commit();
        conn2.close();
        
        try {
            /* Try to close the prepared statement. This would throw an exception
             * before the fix, claiming that the table was not found. */
            ps.close();
        } finally {
            /* Rollback the transaction and close the connections */
            xar.rollback(xid);
            conn.close();
            xaconn.close();
        }
        
    }
    
    /**
     * This test checks the fix on DERBY-4310, for not repreparing CallableStatements
     * upon calling close() on them.
     */
    public void testDerby4310CallableStatement() throws SQLException, XAException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "wombat");

        XAConnection xaconn = xads.getXAConnection();
       
        XAResource xar = xaconn.getXAResource();
        Xid xid = XATestUtil.getXid(1,93,18);
        
        /* Create the procedure bazed on XATest.zeroArg() */
        Connection conn = xaconn.getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("CREATE PROCEDURE ZA() LANGUAGE JAVA "+
                        "EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.jdbcapi.XATest.zeroArg' "+
                        "PARAMETER STYLE JAVA");
        conn.commit();
        
        /* Prepare and execute CallableStatement based on the procedure above */
        CallableStatement cs = conn.prepareCall("CALL ZA()");
        cs.execute();

        /* Start and end a transaction on the XAResource object */
        xar.start(xid, XAResource.TMNOFLAGS);
        xar.end(xid, XAResource.TMSUCCESS);
        
        /* Drop the procedure on a parallel, regular connection */
        Connection conn2 = getConnection();
        Statement s2 = conn2.createStatement();
        s2.execute("DROP PROCEDURE ZA");
        conn2.commit();
        conn2.close();
        
        try {
            /* Try to close the prepared statement. This would throw an exception
             * before the fix, claiming that the table was not found. */
            cs.close();
        } finally {
            /* Rollback the transaction and close the connections */
            xar.rollback(xid);
            conn.close();
            xaconn.close();
        }
        
    }
    
    /**
     * Derby-966 holdability and global/location transactions. (work in
     * progress)
     */
    public void testDerby966() throws SQLException, XAException {
        Statement preStatement = getConnection().createStatement();
        preStatement.execute("create table foo966 (a int)");
        preStatement.executeUpdate("insert into APP.foo966 values (0)");
        preStatement.executeUpdate("insert into APP.foo966 values (1)");
        preStatement.executeUpdate("insert into APP.foo966 values (2)");
        preStatement.executeUpdate("insert into APP.foo966 values (3)");
        preStatement.executeUpdate("insert into APP.foo966 values (4)");
        preStatement.executeUpdate("insert into APP.foo966 values (2001)");
        preStatement.executeUpdate("insert into APP.foo966 values (2002)");
        preStatement.executeUpdate("insert into APP.foo966 values (2003)");
        preStatement.executeUpdate("insert into APP.foo966 values (2005)");
        preStatement.executeUpdate("insert into APP.foo966 values (2007)");
        preStatement.close();
        
        XADataSource xads = J2EEDataSource.getXADataSource();
        XAConnection xac = xads.getXAConnection();
        XAResource xar = xac.getXAResource();

        Xid xid = XATestUtil.getXid(996, 9, 48);

        Connection conn = xac.getConnection();

        // Obtain Statements and PreparedStatements
        // with all the holdability options.
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());

        Statement sdh = conn.createStatement();
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, sdh
                .getResultSetHoldability());

//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        checkHeldRS(conn, sdh, sdh.executeQuery("select * from app.foo966"));
        PreparedStatement psdh = conn.prepareStatement("SELECT * FROM APP.foo966");
        PreparedStatement psdh_d = conn
                .prepareStatement("DELETE FROM APP.foo966 WHERE A < -99");
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, psdh
                .getResultSetHoldability());
        checkHeldRS(conn, psdh, psdh.executeQuery());

        Statement shh = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, shh
                .getResultSetHoldability());
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        checkHeldRS(conn, shh, shh.executeQuery("select * from app.foo966"));
        PreparedStatement pshh = conn.prepareStatement("SELECT * FROM APP.foo966",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        PreparedStatement pshh_d = conn.prepareStatement(
                "DELETE FROM APP.foo966 WHERE A < -99",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, shh
                .getResultSetHoldability());
        checkHeldRS(conn, pshh, pshh.executeQuery());

        Statement sch = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, sch
                .getResultSetHoldability());

//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        checkHeldRS(conn, sch, sch.executeQuery("select * from app.foo966"));
        PreparedStatement psch = conn.prepareStatement("SELECT * FROM APP.foo966",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
        PreparedStatement psch_d = conn.prepareStatement(
                "DELETE FROM APP.foo966 WHERE A < -99",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psch_d
                .getResultSetHoldability());
        checkHeldRS(conn, psch, psch.executeQuery());

        // set the connection's holdabilty to false
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

        Statement sdc = conn.createStatement();
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, sdc
                .getResultSetHoldability());
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        checkHeldRS(conn, sdc, sdc.executeQuery("select * from app.foo966"));
        PreparedStatement psdc = conn.prepareStatement("SELECT * FROM APP.foo966");
        PreparedStatement psdc_d = conn
                .prepareStatement("DELETE FROM APP.foo966 WHERE A < -99");
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psdc
                .getResultSetHoldability());
        checkHeldRS(conn, psdc, psdc.executeQuery());

        Statement shc = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psdc
                .getResultSetHoldability());
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        checkHeldRS(conn, shc, shc.executeQuery("select * from app.foo966"));
        PreparedStatement pshc = conn.prepareStatement("SELECT * FROM APP.foo966",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        PreparedStatement pshc_d = conn.prepareStatement(
                "DELETE FROM APP.foo966 WHERE A < -99",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, pshc
                .getResultSetHoldability());

        checkHeldRS(conn, pshc, pshc.executeQuery());

        Statement scc = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, scc
                .getResultSetHoldability());
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        checkHeldRS(conn, scc, scc.executeQuery("select * from app.foo966"));
        PreparedStatement pscc = conn.prepareStatement("SELECT * FROM APP.foo966",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
        PreparedStatement pscc_d = conn.prepareStatement(
                "DELETE FROM APP.foo966 WHERE A < -99",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, pscc
                .getResultSetHoldability());

        checkHeldRS(conn, pscc, pscc.executeQuery());

        // Revert back to holdable
        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);

//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        ResultSet rs = sdh.executeQuery("SELECT * FROM APP.foo966");
        rs.next();
        // before commit
        assertEquals(0, +rs.getInt(1));
        conn.commit();
        // aftercommit
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();

        // ensure a transaction is active to test DERBY-1025
        rs = sdh.executeQuery("SELECT * FROM APP.foo966");
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155

        // This switch to global is ok because conn
        // is in auto-commit mode, thus the start performs
        // an implicit commit to complete the local transaction.

        // start a global xact and test those statements.
        xar.start(xid, XAResource.TMNOFLAGS);

        // Statements not returning ResultSet's should be ok
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        sdh.executeUpdate("DELETE FROM APP.foo966 where A < -99");
        shh.executeUpdate("DELETE FROM APP.foo966 where A < -99");
        sch.executeUpdate("DELETE FROM APP.foo966 where A < -99");

        ArrayList<ResultSet> openRS = new ArrayList<ResultSet>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

        // Statements obtained while default was hold.
        // All should work, holability will be downgraded
        // to close on commit for those Statements with hold set.
        openRS.add(sdh.executeQuery("SELECT * FROM APP.foo966"));
        openRS.add(shh.executeQuery("SELECT * FROM APP.foo966"));
        openRS.add(sch.executeQuery("SELECT * FROM APP.foo966"));

        // PreparedStatements obtained while default was hold.
        // Holdability should be downgraded.
        openRS.add(psdh.executeQuery());
        openRS.add(pshh.executeQuery());
        openRS.add(psch.executeQuery());

        // Statements not returning ResultSet's should be ok
//IC see: https://issues.apache.org/jira/browse/DERBY-1159
        psdh_d.executeUpdate();
        pshh_d.executeUpdate();
        psch_d.executeUpdate();

        // Statements not returning ResultSet's should be ok
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
        sdc.executeUpdate("DELETE FROM APP.foo966 where A < -99");
        shc.executeUpdate("DELETE FROM APP.foo966 where A < -99");
        scc.executeUpdate("DELETE FROM APP.foo966 where A < -99");

        // Statements obtained while default was close.
        // all should return close on commit ResultSets
        openRS.add(sdc.executeQuery("SELECT * FROM APP.foo966"));
        openRS.add(shc.executeQuery("SELECT * FROM APP.foo966"));
        openRS.add(scc.executeQuery("SELECT * FROM APP.foo966"));

        // PreparedStatements obtained while default was close.
        openRS.add(psdc.executeQuery());
        openRS.add(pshc.executeQuery());
        openRS.add(pscc.executeQuery());

        // Statements not returning ResultSet's should be ok
//IC see: https://issues.apache.org/jira/browse/DERBY-1159
        psdc_d.executeUpdate();
        pshc_d.executeUpdate();
        pscc_d.executeUpdate();

        // All the ResultSets should be open. Run a simple
        // test, clearWarnings throws an error if the ResultSet
        // is closed. Also would be nice here to use the new
        // JDBC 4.0 method getHoldabilty to ensure the
        // holdability is reported correctly.
        int orsCount = 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (ResultSet ors : openRS) {
            ors.clearWarnings();
            orsCount++;
        }
        assertEquals("Incorrect number of open result sets", 12, orsCount);

        // Test we cannot switch the connection to holdable
        try {
            conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
            fail("FAIL - set holdability in global xact.");
        } catch (SQLException sqle) {
            assertSQLState("XJ05C", sqle);
        }

        // JDBC 4.0 (proposed final draft) section allows
        // drivers to change the holdability when creating
        // a Statement object and attach a warning to the Connection.
        Statement sglobalhold = conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, sglobalhold
                .getResultSetHoldability());

        sglobalhold.close();

        // DERBY2481 Client does not downgrade PreparedStatement holdability
        if (!usingDerbyNetClient()) {
            PreparedStatement psglobalhold = conn.prepareStatement(
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
                    "SELECT * FROM APP.foo966", ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psglobalhold
                    .getResultSetHoldability());

            psglobalhold.close();

            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, sdh
                    .getResultSetHoldability());
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, sch
                    .getResultSetHoldability());

            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psdh
                    .getResultSetHoldability());
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, pshh
                    .getResultSetHoldability());
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psch
                    .getResultSetHoldability());

            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, sdc
                    .getResultSetHoldability());
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, shc
                    .getResultSetHoldability());
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, scc
                    .getResultSetHoldability());

            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psdh_d
                    .getResultSetHoldability());
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, pshh_d
                    .getResultSetHoldability());
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psch_d
                    .getResultSetHoldability());
        }

        xar.end(xid, XAResource.TMSUCCESS);
        if (xar.prepare(xid) != XAResource.XA_RDONLY)
            fail("FAIL prepare didn't indicate r/o");

        // All the ResultSets should be closed. Run a simple
        // test, clearWarnings throws an error if the ResultSet
        // is closed.
        int crsCount = 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (ResultSet crs : openRS) {
            try {
                crs.clearWarnings();
            } catch (SQLException sqle) {
            }
            crsCount++;
        }
        assertEquals("After global transaction closed ResultSets ", 12,
                crsCount);

        // Check the statements revert to holdable as required.
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, sdh
                .getResultSetHoldability());
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, shh
                .getResultSetHoldability());
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, sch
                .getResultSetHoldability());

        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, psdh
                .getResultSetHoldability());
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, pshh
                .getResultSetHoldability());
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psch
                .getResultSetHoldability());

        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, sdc
                .getResultSetHoldability());
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, shc
                .getResultSetHoldability());
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, scc
                .getResultSetHoldability());

        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, psdh_d
                .getResultSetHoldability());
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, pshh_d
                .getResultSetHoldability());
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, psch_d
                .getResultSetHoldability());

        // intermittently we get a failure in this test if we do not
        // rollback, see DERBY-6248.
        conn.rollback();
        conn.close();

    }

    /**
     * DERBY-4731
     * Test using a GLOBAL TEMPORARY TABLE  table in an
     * XA transaction and leaving it active during two phase commit.
     * Before the fix this test would throw the following at commit
     * time:
     * ERROR 40XT0: An internal error was identified by RawStore module. 
     *
     *
     * @throws XAException 
     * @throws SQLException 
     * 
     */
    public void testXATempTableD4731_RawStore() 
        throws SQLException, XAException {
        doXATempTableD4731Work(true, false, XATestUtil.getXid(997, 9, 49));
    }
    

    /**
     * DERBY-4735 Temp tables with XA transactions
     * an Assert will occur on prepare if only
     * temp table work is done in the xact.
     *
     * @throws XAException 
     * @throws SQLException 
     * 
     */
    public void xtestXATempTableD4735_Assert() 
        throws SQLException, XAException {

          doXATempTableD4731Work(false, false, XATestUtil.getXid(999,  11, 51));
          doXATempTableD4731Work(false, true,  XATestUtil.getXid(1000, 12, 52));
    }

    /**
     * DERBY-4743 Temp tables with XA transactions
     *
     * Verifies that global temporary table can not be accessed after the
     * commit of the XA transaction.
     *
     * @throws XAException 
     * @throws SQLException 
     * 
     */
    public void testXATempTableD4743() 
        throws SQLException, XAException {

          doXATempTableD4731Work(true, true, XATestUtil.getXid(998, 10, 50));
    }
 
    /**
     * DERBY-5552 Check that lock timeout does not destroy connection
     * during an XA Transaction.
     * 
     * @throws SQLException
     * @throws XAException
     */
    public void testXALockTimeout() throws SQLException, XAException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "wombat");
       
        // Get the first connection and lock table in 
        // xa transaction
        XAConnection xaconn = xads.getXAConnection();
        XAResource xar = xaconn.getXAResource();
        Xid xid = XATestUtil.getXid(998,10,50);
   
        Connection conn = xaconn.getConnection();
        Statement s = conn.createStatement();
        xar.start(xid, XAResource.TMNOFLAGS);
        s.executeUpdate("INSERT INTO TABLT VALUES(2)");
        
        // Get a second connection and global xact
        // and try to select causing lock timeout
        XAConnection xaconn2 = xads.getXAConnection();
        XAResource xar2 = xaconn2.getXAResource();
        Xid xid2 = XATestUtil.getXid(999,11,51);
        Connection conn2 = xaconn2.getConnection();
        // Set to serializable so we get lock timeout
        conn2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        xar2.start(xid2, XAResource.TMNOFLAGS);
        Statement s2 = conn2.createStatement();
        s2.executeUpdate("INSERT INTO TABLT VALUES(3)");
        assertGlobalXactCount(2);
        try {
            ResultSet rs = s2.executeQuery("SELECT * FROM TABLT");
            fail("Should have gotten lock timeout error: " + LOCKTIMEOUT);
        } catch (SQLException se) {
            assertSQLState(LOCKTIMEOUT,se);
            }
        // After the lock timeout we just have one global transaction.
        // lock timeout implicitly rolled back xid2
        assertGlobalXactCount(1);
        assertConnOK(conn);
        // DERBY-5552 Make sure connection is ok after lock timeout
        assertConnOK(conn2);
        //Should be able to end and commit xid1
        xar.end(xid, XAResource.TMSUCCESS);
        xar.prepare(xid);
        xar.commit(xid, false);
        
        // xid2 should have already been rolled back so end should fail
        try {    
            xar2.end(xid2, XAResource.TMSUCCESS);
            fail("Should have gotten exception ending xid2");
        } catch (XAException xae) {
            //xae.printStackTrace();
            assertEquals(XAException.XA_RBTIMEOUT, xae.errorCode);
            
        } 
     
        // Should have no locks on TABLT
        Statement drops = createStatement();
        drops.executeUpdate("DROP TABLE TABLT");
        // verify there are no more global transactions
        assertGlobalXactCount(0);
        
        // Need to explicitly rollback xid2 as it ended with
        // an implicit rollback XA_RBTIMEOUT
        xar2.rollback(xid2);
        
        // Make sure both connections can be used to make a new global xact
        xar.start(xid, XAResource.TMNOFLAGS);
        s.executeUpdate("CREATE TABLE TABLT (I INT)");
        s.executeUpdate("INSERT INTO TABLT VALUES(1)");
        xar.end(xid, XAResource.TMSUCCESS);
        xar.prepare(xid);
        xar.commit(xid, false);
        
        // now the other connection ..
        xar2.start(xid2, XAResource.TMNOFLAGS);
        s2.executeUpdate("INSERT INTO TABLT VALUES(2)");
        xar2.end(xid2, XAResource.TMSUCCESS);
        xar.prepare(xid2);
        xar.commit(xid2, false);
        assertGlobalXactCount(0);
        conn.close();
        xaconn.close();
        conn2.close();
        xaconn2.close();
        
    }
    
    /**
     * DERBY-6879 Check that a XA transaction timeout while a cleanupOnError is
     * being performed does not cause a Java level deadlock.
     *
     * The strategy is to cause a XA statement to wait for a period that is
     * longer than the XA transaction timeout which will allow the timeout to
     * cancel the XA transaction. That is accomplished by locking a table using
     * a standard connection and then issuing a select on the same table using a
     * XA connection. The select will wait for the required locks to be
     * available up to the lock timeout. For the test to work correctly the XA
     * transaction timeout must be less than the lock timeout.
     *
     * A dealock watchdog is used to detect the deadlock until the DERBY-6879
     * issue is fixed.
     *
     * @throws SQLException
     * @throws XAException
     */
    public void testDerby6879() throws SQLException, XAException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "wombat");

        Connection conn = J2EEDataSource.getConnectionPoolDataSource().getPooledConnection().getConnection();
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        s.execute("LOCK TABLE TABLT IN EXCLUSIVE MODE");

        // Get a second connection and global xact
        // and try to select causing lock timeout
        XAConnection xaconn2 = xads.getXAConnection();
        XAResource xar2 = xaconn2.getXAResource();
        xar2.setTransactionTimeout(2);
        Xid xid2 = XATestUtil.getXid(6879, 11, 51);
        Connection conn2 = xaconn2.getConnection();
        // Set to serializable so we get lock timeout
        conn2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        xar2.start(xid2, XAResource.TMNOFLAGS);
        Statement s2 = conn2.createStatement();
        assertGlobalXactCount(1);
        DeadlockWatchdog wd = new DeadlockWatchdog(30 * 1000);
        wd.start();
        try {
            ResultSet rs = s2.executeQuery("SELECT * FROM TABLT");
            fail("Should have gotten lock timeout error: " + LOCKTIMEOUT);
        } catch (SQLException se) {
            assertSQLState(LOCKTIMEOUT, se);
        }
        wd.stop();

        // xid2 should have already been rolled back so end should fail
        try {
            xar2.end(xid2, XAResource.TMSUCCESS);
            fail("Should have gotten exception ending xid2");
        } catch (XAException xae) {
            assertTrue(xae.errorCode >= XAException.XAER_OUTSIDE || xae.errorCode <= XAException.XAER_ASYNC);
        }

        conn.commit();

        conn.close();
        conn2.close();
        xaconn2.close();
    }
    
    
    /**
     * The two cases for DERBY-4371 do essentially the same thing. Except doing
     * logged work causes the RawStore error and doing only temp table 
     * operations causes the assert.
     *  
     * @param doLoggedWorkInXact
     * @throws SQLException
     * @throws XAException
     */
    private void doXATempTableD4731Work(
    boolean doLoggedWorkInXact,
    boolean access_temp_table_after_xaendandcommit,
    Xid     xid)
        throws SQLException, XAException{

        XADataSource xads = J2EEDataSource.getXADataSource();
        XAConnection xaconn = xads.getXAConnection();
        XAResource xar = xaconn.getXAResource();

        xar.start(xid, XAResource.TMNOFLAGS);
        Connection conn = xaconn.getConnection();
        Statement s = conn.createStatement(); 
        if (doLoggedWorkInXact){
            // need to do some real work in our transaction
            // so make a table
            makeARealTable(s);
        }
        
        // make the temp table
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.T1 ( XWSID INT, XCTID INT, XIID CHAR(26), XVID SMALLINT, XLID CHAR(8) FOR BIT DATA) ON COMMIT DELETE ROWS NOT LOGGED ON ROLLBACK DELETE ROWS");

        // insert a row
        PreparedStatement ps = 
            conn.prepareStatement("INSERT INTO SESSION.T1 VALUES (?,?,?,?,?)");
        ps.setInt(1,1);
        ps.setInt(2,1);
        ps.setString(3,"hello");
        ps.setShort(4, (short) 1);
        ps.setBytes(5, new byte[] {0x0,0x1});
        ps.executeUpdate();
        ResultSet rs = s.executeQuery("SELECT count(*) FROM SESSION.t1");
        JDBC.assertFullResultSet(rs, new String[][] {{"1"}});
        rs.close();
        // You could work around the issue by dropping the TEMP table
        //s.executeUpdate("DROP TABLE SESSION.T1");
        xar.end(xid, XAResource.TMSUCCESS);

        assertEquals(
            (doLoggedWorkInXact ? XAResource.XA_OK : XAResource.XA_RDONLY),
            xar.prepare(xid));

        if (doLoggedWorkInXact)
        {
            // if you don't do logged work in the transaction, then the 
            // prepare with be done with XAResource.XA_RDONLY.  If you
            // try to commit a prepared read only transaction you will get 
            // an error, so only commit prepared transactions here that did
            // some "real" logged work.
            xar.commit(xid,false); 
        }

        if (access_temp_table_after_xaendandcommit)
        {
            // temp table should not be available after commit of an XA
            // transaction.
            assertStatementError(
                "42X05", s, "SELECT count(*) FROM SESSION.t1");
            conn.commit();
        }


        s.close();
        conn.close();
        xaconn.close();
    }


    private void makeARealTable(Statement s) throws SQLException {
        try {
            s.executeUpdate("DROP TABLE REALTABLE1");
        } catch (SQLException se) {
            {
            s.executeUpdate("CREATE TABLE REALTABLE1 (i int)");
            }
        }
    }
    
    /**
     * Check the held state of a ResultSet by fetching one row, executing a
     * commit and then fetching the next. Checks the held state matches the
     * behaviour.
     */
    private static void checkHeldRS(Connection conn, Statement s, ResultSet rs)
            throws SQLException {
        // DERBY-1008 - can't run with client
        if (!usingDerbyNetClient()) {
            if (s.getConnection() != conn)
                fail("FAIL - mismatched statement & Connection");
        }
        if (rs.getStatement() != s) {
            // DERBY-1009
            fail("FAIL - mismatched statement & ResultSet "
                    + " Statement class " + s.getClass()
                    + " ResultSet' Statements class "
                    + rs.getStatement().getClass());
        }

        boolean held = (ResultSet.HOLD_CURSORS_OVER_COMMIT == s
                .getResultSetHoldability());
        rs.next();
        assertEquals(0, rs.getInt(1));
        conn.commit();

        try {
            rs.next();
        } catch (SQLException sqle) {
            boolean ok = !held;

            if (ok) {
                assertSQLState("XCL16", sqle);
            } else {
                fail("Held cursor closed on commit");
            }
        }

        rs.close();
        conn.commit();
    }
    
    /** 
     * Dummy method for testDerby4310* fixtures
     */
    public static void zeroArg() {  }

    public static Test baseSuite(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(XATest.class);

        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the table used in the test cases.
             * 
             */
            protected void decorateSQL(Statement s) throws SQLException {
                XATestUtil.createXATransactionView(s);
                // Table for lock timeout test
                s.executeUpdate("CREATE TABLE TABLT (I INT)");
                s.executeUpdate("INSERT INTO TABLT VALUES(1)");
            }

        };
    }
    
    /**
     * Excecute a simple SQL statement to assert that the connection is valid
     * @param conn Connection to check
     * @throws SQLException on error
     */
    private static void assertConnOK(Connection conn) throws SQLException{
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("VALUES(1)");
        JDBC.assertSingleValueResultSet(rs, "1");
      }
    
    
    /**
     * Verify the expected number of global transactions.
     * Run test with -Dderby.tests.debug to see the full transaction table on the
     * console
     * 
     * @param expectedCount expected number of global transaction
     * @throws SQLException on error
     */
    private void assertGlobalXactCount(int expectedCount ) throws SQLException {
        Connection conn = openDefaultConnection();
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(
              "SELECT COUNT(*) FROM syscs_diag.transaction_table WHERE GLOBAL_XID IS NOT NULL");
        rs.next();
        int count = rs.getInt(1);
        if (TestConfiguration.getCurrent().isVerbose()) {
            System.out.println("assertGlobalXactCount(" + expectedCount +
                    "): Full Transaction Table ...");
            Utilities.showResultSet(s.executeQuery(
                    "SELECT * FROM syscs_diag.transaction_table"));
        }
        assertTrue("Expected " + expectedCount + "global transactions but saw " + count, expectedCount == count);
    }
    
    /**
     * Runs the test fixtures in embedded and client.
     * 
     * @return test suite
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("XATest");
        // no XA for JSR169
        if (JDBC.vmSupportsJSR169())
            return suite;

        suite.addTest(baseSuite("XATest:embedded"));

        suite.addTest(TestConfiguration
                .clientServerDecorator(baseSuite("XATest:client")));
        
        Test test = DatabasePropertyTestSetup.setLockTimeouts(suite, 3, 5);
        test = decorateWithPolicy(test);
        return test;
    }   
    
    // grant ALL FILES execute, and getPolicy permissions,
    // as well as write for the trace files.
    private static Test decorateWithPolicy(Test test) {
        //
        // Install a security manager using the initial policy file. This is 
        // needed foro the DeadlockWatchdog to allow it to access the ThreadMXBean
        // to check for a deadlock
        //
        return new SecurityManagerSetup(test, POLICY_FILE_NAME);
    }
    
    
}

