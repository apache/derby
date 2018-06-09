/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.XATransactionTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import junit.framework.Test;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.XATestUtil;

/** The test of the properties of the XA transaction interface implementation.
  */
public class XATransactionTest extends BaseJDBCTestCase {

	/**
	  * This test does following 
	  * 1)Start the network server
	  * 2)Start a local xa transaction
	  * 3)Do not commit the local XA transaction
	  * 4)Shutdown the network server
	  * 5)Start the server again
	  * 
	  * Before the fix for DERBY-4053 went in, step 4) would not shutdown the
	  * server properly because of the pending local XA transaction. During the
	  * server shutdown, we try to close all the open connections but the close 
	  * on the XA connection results into an exception because there is still a
	  * pending transaction. That exception is not handled by the server and
	  * because of that, all the code necessary to shutdown the server is not
	  * executed. The next time around, step 5), when we try to bring up the
	  * server again, it ends up hanging
	  * 2009-07-09 21:21:28.828 GMT : Invalid reply from network server: Insufficient data.
	  * 2009-07-09 21:21:28.843 GMT : Could not listen on port 1527 on host 127.0.0.1: java.net.BindException: Address already in use: JVM_Bind
	  * 
	  * The fix for DERBY-4053 makes sure that before calling close on local XA
	  * transaction, we first rollback any transaction active on the 
	  * connection. 
	 */
	public void testPendingLocalTranAndServerShutdown() throws Exception {
        if (usingEmbedded())
            return;
        //1)Server must be up already through the Derby junit framework
        //2)Start a local xa transaction
        XADataSource xaDataSource = J2EEDataSource.getXADataSource();
        XAConnection xaconn = xaDataSource.getXAConnection();
        XAResource xar = xaconn.getXAResource();
        Connection conn = xaconn.getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("create table tab(i int)");
        s.executeUpdate("insert into tab values (1),(2),(3),(4)");
        conn.commit();
        conn.setAutoCommit(false);
        ResultSet rs = s.executeQuery("select * from tab");
        rs.next();
        //3)Do not commit this pending local XA transaction
    	
        //4)Shutdown the network server
        //bring the server down while the local xa transaction is still active
        TestConfiguration.getCurrent().stopNetworkServer();
        
        //5)Start the server again
        TestConfiguration.getCurrent().startNetworkServer();
	}
	
    /** Tests whether it is possible to reconstruct the original Xid value
      * correctly from SYSCS_DIAG.TRANSACTION_TABLE. */
    public void testGlobalXIDinTransactionTable() throws Exception {
        Statement stm = getConnection().createStatement();
        stm.execute("create table XATT2 (i int, text char(10))");

        XADataSource xaDataSource = J2EEDataSource.getXADataSource();
        XAConnection xaConn = xaDataSource.getXAConnection();
        XAResource xaRes = xaConn.getXAResource();
        Connection conn = xaConn.getConnection();

        // create large enough xid
        byte[] gid = new byte[64];
        byte[] bid = new byte[64];
        for (int i=0; i < 64; i++) {
            gid[i] = (byte) i;
            bid[i] = (byte) (64 - i);
        }
        Xid xid = XATestUtil.getXid(0x1234, gid, bid);

        // get the stuff required to execute the global transaction
        xaConn = xaDataSource.getXAConnection();
        xaRes = xaConn.getXAResource();
        conn = xaConn.getConnection();

        // start the transaction with that xid
        xaRes.start(xid, XAResource.TMNOFLAGS);

        // do some work
        stm = conn.createStatement();
        stm.execute("insert into XATT2 values (1234, 'Test_Entry')");
        stm.close();

        // end the wotk on the transaction branch
        xaRes.end(xid, XAResource.TMSUCCESS);

        ResultSet rs = null;
        stm = null;

        try {
            // check the output of the global xid in 
            // syscs_diag.transaction_table
            stm = getConnection().createStatement();

            String query = "select global_xid from syscs_diag.transaction_table"
                         + " where global_xid is not null";

            // execute the query to obtain the xid of the global transaction
            rs = stm.executeQuery(query);

            // there should be at least one globaltransaction in progress
            assertTrue(rs.next());

            // check whether the xid obtained matches the original xid
            Xid rXid = parseXid(rs.getString(1));
            assertEquals(xid, rXid);

            // there should be at most one global transaction in progress
            assertFalse(rs.next());

        } catch (Exception ex) {
            try {
                // close all the stuff
                if (rs != null)
                    rs.close();
                if (stm != null)
                    stm.close();

                // rollback the global transaction
                xaRes.rollback(xid);
                // close the connection
                xaConn.close();
            } catch (Exception e) {
                // ignore the exception because it
                // would hide the original exception
            }
            // throw the stuff further
            throw ex;
        }

        // close all the stuff
        rs.close();
        stm.close();

        // rollback the global transaction
        xaRes.rollback(xid);

        // close the connection
        xaConn.close();
    }


    /** Tests the functionality of the XA transaction timeout.
      * <p>
      * It executes 66 global transactions during the test. Everyone
      * of them just inserts a row into XATT table. The rows inserted
      * by the transactions are different. Some of these transactions
      * are committed and some of them are left in different stages.
      * The stage of the transaction in which it is left is chosed
      * depending on division remainders.
      * </p>
      * <p>
      * After finishing these 1000 transactions a select statement is executed
      * on that table. However, if there are still some unfinished transactions
      * that were not aborted they will hold a lock on a XATT table until they
      * will get rolled back by the transaction timeout. The number of rows
      * in the XATT table is calculated. It is then compared with the excepted
      * number of rows (the transaction we know we have committed).
      * </p>
      * <p>
      * The call to xaRes.setTransactionTimeout(5) before the call
      * to xaRes.start() makes the transactions to be rolled back
      * due to timeout.
      * </p>
      */
    public void testXATransactionTimeout() throws Exception {

        /* The number of statements to execute in timeout related test. */
        int timeoutStatementsToExecute = 66;

        /* Specifies the number of total executed statements per one
           commited statement in timeout related test. */
        int timeoutCommitEveryStatement = 3;

        /* Specifies the number of statements that should be commited
           during a timeout related test. */
        int timeoutStatementsCommitted
            = (timeoutStatementsToExecute + timeoutCommitEveryStatement - 1)
                / timeoutCommitEveryStatement;

        Statement stm = getConnection().createStatement();
        stm.execute("create table XATT (i int, text char(10))");

        XADataSource xaDataSource = J2EEDataSource.getXADataSource();
        XAConnection[] xaConn = new XAConnection[timeoutStatementsToExecute];
        XAResource xaRes = null;
        Connection conn = null;

        for (int i=0; i < timeoutStatementsToExecute; i++) {
            xaConn[i] = xaDataSource.getXAConnection();
            xaRes = xaConn[i].getXAResource();
            conn = xaConn[i].getConnection();

            Xid xid = createXid(123, i);
            xaRes.setTransactionTimeout(8);
            xaRes.start(xid, XAResource.TMNOFLAGS);

            stm = conn.createStatement();
            stm.execute("insert into XATT values (" + i + ", 'Test_Entry')");

            if (i % timeoutCommitEveryStatement == 0) {
                stm.close();
                xaRes.end(xid, XAResource.TMSUCCESS);
                xaRes.prepare(xid);
                xaRes.commit(xid, false);
            } else if (i % 11 != 0) {
                // check the tiemout for transactions disassociated
                // with failure.
                try {
                    xaRes.end(xid, XAResource.TMFAIL);
                    fail();
                } catch (XAException ex) {
                    if (ex.errorCode < XAException.XA_RBBASE
                        || ex.errorCode > XAException.XA_RBEND)
                    {
                        throw ex;
                    }
                }
                stm.close();
            } else if (i % 2 == 0) {
                // check the timeout for transactions disassociated
                // with success.
                xaRes.end(xid, XAResource.TMSUCCESS);
                stm.close();
            } 
        }

        ResultSet rs = null;

        stm = getConnection().createStatement();
        rs = stm.executeQuery("select count(*) from XATT");
        rs.next();

        // Check whether the correct number of transactions
        // was rolled back
        assertTrue(rs.getInt(1) == timeoutStatementsCommitted);

        // test the timeout during the statement run
        XAConnection xaConn2 = xaDataSource.getXAConnection();
        xaRes = xaConn2.getXAResource();
        conn = xaConn2.getConnection();

        Xid xid = createXid(124, 100);
        xaRes.setTransactionTimeout(10);
        xaRes.start(xid, XAResource.TMNOFLAGS);

        stm = conn.createStatement();

        // Check whether the statement was correctly timed out
        // and the appropriate exception was thrown
        try {
            // Run this kind of statement just to be sure
            // it will not finish before it will time out
            rs = stm.executeQuery(
                 "select count(*) from sys.syscolumns a, sys.syscolumns b, "
               + "sys.syscolumns c, sys.syscolumns d, sys.syscolumns e "
               + "group by a.referenceid, b.referenceid, c.referenceid, "
               + "d.referenceid");
            fail("An exception is expected here");
        } catch (SQLException ex) {
            // Check the sql state of the thrown exception
            assertSQLState(
                SQLState.LANG_STATEMENT_CANCELLED_OR_TIMED_OUT.substring(0,5),
                ex);
        }

        // perform a select on the table just to be sure that all
        // the transactions were rolled back.
        stm = getConnection().createStatement();
        rs = stm.executeQuery("select count(*) from XATT");
        rs.next();

        // Go throught the XA Connections just to be sure that no code
        // optimization would garbage collect them before (and thus
        // the transactions might get rolled back by a different
        // code).
        for (int i=0; i < timeoutStatementsToExecute; i++) {
            assertNotNull(xaConn[i]);
            xaConn[i].close();
        }

        // Again, check whether the correct number of transactions
        // was rolled back
        assertTrue(rs.getInt(1) == timeoutStatementsCommitted);
    }

    /**
     * DERBY-4232: Test that an XA transaction can be suspended and resumed
     * when a timeout is in effect.
     */
    public void testTransactionTimeoutAndSuspendResume() throws Exception {
        XADataSource xads = J2EEDataSource.getXADataSource();
        XAConnection xac = xads.getXAConnection();
        XAResource xar = xac.getXAResource();
        Xid xid = XATestUtil.getXid(1, 2, 3);

        // Start work in a new transaction with a timeout
        xar.setTransactionTimeout(500);
        xar.start(xid, XAResource.TMNOFLAGS);

        // Suspend the transaction
        xar.end(xid, XAResource.TMSUSPEND);

        // Resume the transaction (used to fail with a XAER_PROTO on the
        // network client)
        xar.start(xid, XAResource.TMRESUME);

        // End the transaction and free up the resources
        xar.end(xid, XAResource.TMSUCCESS);
        xar.rollback(xid);
        xac.close();
    }

    /**
     * DERBY-4232: Test that two branches can be joined after the timeout has
     * been set.
     */
    public void testTransactionTimeoutAndJoin() throws Exception {
        XADataSource xads = J2EEDataSource.getXADataSource();
        XAConnection xac1 = xads.getXAConnection();
        XAResource xar1 = xac1.getXAResource();
        Xid xid1 = XATestUtil.getXid(4, 5, 6);

        // Start/end work in a new transaction
        xar1.setTransactionTimeout(500);
        xar1.start(xid1, XAResource.TMNOFLAGS);
        xar1.end(xid1, XAResource.TMSUCCESS);

        // Create a new branch that can be joined with the existing one
        XAConnection xac2 = xads.getXAConnection();
        XAResource xar2 = xac2.getXAResource();
        xar2.setTransactionTimeout(500);

        // Do some work on the new branch before joining (the bug won't be
        // reproduced if we join with a fresh branch)
        Xid xid2 = XATestUtil.getXid(4, 5, 7);
        xar2.start(xid2, XAResource.TMNOFLAGS);
        xar2.end(xid2, XAResource.TMSUCCESS);
        xar2.rollback(xid2);

        assertTrue(
                "Branches can only be joined if RM is same",
                xar1.isSameRM(xar2));

        // Join the branches. This used to fail with XAER_PROTO on the
        // network client.
        xar2.start(xid1, XAResource.TMJOIN);

        // End the transaction and free up the resources
        xar2.end(xid1, XAResource.TMSUCCESS);
        xar2.rollback(xid1);
        xac1.close();
        xac2.close();
    }

    /**
     * DERBY-4141 XAExceptions caused by SQLExceptions should have a
     * non-zero errorCode. SESSION_SEVERITY or greater map to
     * XAException.XAER_RMFAIL. Lesser exceptions map to XAException.XAER_RMERR 
     * @throws Exception
     */
    public void testXAExceptionErrorCodeOnSQLExceptionDerby4141() throws Exception {
        XADataSource xaDataSource = J2EEDataSource.getXADataSource();
        XAConnection xaConn = xaDataSource.getXAConnection();
        XAResource xaRes = xaConn.getXAResource();        
        Xid xid = createXid(123, 1);
        // close the XAConnection so we get an SQLException on
        // start();
        xaConn.close();
        try {
            xaRes.start(xid, XAResource.TMNOFLAGS);
            fail("Should have gotten an XAException. xaConn is closed.");
        } catch (XAException xae) {
            assertEquals(XAException.XAER_RMFAIL, xae.errorCode);
        }
    }

    /**
     * This fixture triggers DERBY-1016. It creates an XA transaction, executes
     * an update over it and then prepares the transaction. Trying to forget
     * after preparing should throw XAER_PROTO and not XAER_NOTA.
     */
    public void testForgetExceptionDerby1016PROTO() throws XAException, SQLException {      
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "wombat");
        
        XAConnection xaconn = xads.getXAConnection();
        XAResource xar = xaconn.getXAResource();
        Xid xid = createXid(93,18);
        xar.start(xid, XAResource.TMNOFLAGS);
        Connection conn = xaconn.getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("CREATE TABLE Derby1016 (I INT)");
        xar.end(xid, XAResource.TMSUCCESS);
        xar.prepare(xid);
        try {
            xar.forget(xid);
            fail("FAIL: prepared XA-Transaction forgotten");
        } catch (XAException XAeForget) {
            assertEquals("FAIL: Got unexpected exception "
                          + XAeForget.getMessage()   + " errorCode: " 
                          + XAeForget.errorCode  + "  calling forget on a prepared transaction",
                        XAException.XAER_PROTO, XAeForget.errorCode);
        } finally {
            s.close();
            xar.rollback(xid);
            conn.close(); 
            xaconn.close();
        }
    }
 
    /**
     * Further test case prompted by DERBY-1016. Tests that XAER_NOTA is thrown
     * if no transaction exists.
     */
    public void testForgetExceptionDerby1016NOTA() throws XAException, SQLException {      
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "wombat");
        
        XAConnection xaconn = xads.getXAConnection();
        XAResource xar = xaconn.getXAResource();
        Xid xid = createXid(93,18);
        xar.start(xid, XAResource.TMNOFLAGS);
        Connection conn = xaconn.getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("CREATE TABLE Derby1016 (I INT)");
        xar.end(xid, XAResource.TMSUCCESS);
        xar.prepare(xid);
        xar.commit(xid, false);
        try {
            // since the transaction was committed, it should no longer exist
            // thus, forget should now throw an XAER_NOTA
            xar.forget(xid);
            fail("FAIL: able to forget committed XA-Transaction");
        } catch (XAException XAeForget) {
            assertEquals("FAIL: Got unexpected exception "
                          + XAeForget.getMessage()   + " errorCode: " 
                          + XAeForget.errorCode  + "  calling forget on a committed transaction",
                        XAException.XAER_NOTA, XAeForget.errorCode);
        } finally {
            s.executeUpdate("DROP TABLE Derby1016");
            conn.commit();
            s.close();
            conn.close(); 
            xaconn.close();
        }
    }

    /**
     * <p>
     * Regression test case for DERBY-5562.
     * </p>
     *
     * <p>
     * The timer that aborts long-running transactions if a transaction timeout
     * has been specified, was not cancelled when preparing a read-only
     * transaction. Since read-only transactions are implicitly committed when
     * they are prepared, this meant that the timer would try to abort an
     * already completed transaction. In addition to printing a confusing
     * message in derby.log about the transaction being rolled back, when it
     * actually had been committed, this could also make the timer roll back
     * the wrong transaction, if a new transaction with the same Xid was
     * started later.
     * </p>
     *
     * <p>
     * This test case exposes the bug by running a read-only transaction with
     * a timeout and preparing it, and then starting a new transaction with the
     * same Xid and no timeout. The bug would cause the second transaction to
     * time out.
     * </p>
     */
    public void testDerby5562ReadOnlyTimeout()
            throws InterruptedException, SQLException, XAException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        XAConnection xac = xads.getXAConnection();
        XAResource xar = xac.getXAResource();

        Xid xid = createXid(55, 62);

        // Set a transaction timeout. This should be relatively short so that
        // the test case doesn't need to wait very long to trigger the timeout.
        // However, it needs to be long enough to let the first transaction go
        // through without hitting the timeout. Hopefully, four seconds is
        // enough. If the test case starts failing intermittently during the
        // first transaction, we might have to raise the timeout (and raise the
        // sleep time in the second transaction correspondingly).
        assertTrue(xar.setTransactionTimeout(4));

        // Start first transaction.
        xar.start(xid, XAResource.TMNOFLAGS);
        Connection c = xac.getConnection();
        Statement s = c.createStatement();
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from sysibm.sysdummy1"),
                "Y");
        s.close();
        c.close();
        xar.end(xid, XAResource.TMSUCCESS);

        // Prepare the first transaction. Since it's a read-only transaction,
        // it'll be automatically committed, so there's no need to call commit.
        assertEquals("XA_RDONLY", XAResource.XA_RDONLY, xar.prepare(xid));

        // Reset the timeout for the second transaction.
        assertTrue(xar.setTransactionTimeout(0));

        // Start second transaction.
        xar.start(xid, XAResource.TMNOFLAGS);
        c = xac.getConnection();
        s = c.createStatement();
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from sysibm.sysdummy1"),
                "Y");
        s.close();
        c.close();

        // Keep the transaction running so long that it must have exceeded the
        // timeout for the previous transaction.
        Thread.sleep(5000);

        // End the transaction. Since there's no timeout on this transaction,
        // it should work. Before DERBY-5562 was fixed, it would fail because
        // it had been rolled back by the timer from the previous transaction.
        xar.end(xid, XAResource.TMSUCCESS);
        assertEquals("XA_RDONLY", XAResource.XA_RDONLY, xar.prepare(xid));

        xac.close();
    }

    /* ------------------- end helper methods  -------------------------- */

    /** Create the Xid object for global transaction identification
      * with the specified identification values.
      * @param gtrid Global Transaction ID
      * @param bqual Branch Qualifier
      */
    static Xid createXid(int gtrid, int bqual) throws XAException {
        byte[] gid = new byte[2]; gid[0]= (byte) (gtrid % 256); gid[1]= (byte) (gtrid / 256);
        byte[] bid = new byte[2]; bid[0]= (byte) (bqual % 256); bid[1]= (byte) (bqual / 256);
        return XATestUtil.getXid(0x1234, gid, bid);
    }

    /** Parses the xid value from the string. The format of the input string is
      * the same as the global_xid column in syscs_diag.transaction_table table -
      * '(formatid_in_dec,global_transaction_id_in_hex,branch_qualifier_in_hex)'
      * @param str Global Transaction ID converted to a string.
      * @return The xid object corresponding to the xid specified in a string.
      */
    private static Xid parseXid(String str) {
        assertNotNull(str);
        assertTrue(str.matches("\\(\\p{Digit}+,\\p{XDigit}+,\\p{XDigit}+\\)"));

        String formatIdS = str.substring(1, str.indexOf(','));
        String gtidS = str.substring(str.indexOf(',')+1, str.lastIndexOf(','));
        String bqualS = str.substring(str.lastIndexOf(',')+1, str.length()-1);

        assertTrue(gtidS.length() % 2 == 0);
        assertTrue(bqualS.length() % 2 == 0);

        int fmtid = Integer.parseInt(formatIdS);
        byte[] gtid = new byte[gtidS.length()/2];
        byte[] bqual = new byte[bqualS.length()/2];

        for (int i=0; i < gtid.length; i++) {
            gtid[i] = (byte) Integer.parseInt(gtidS.substring(2*i, 2*i + 2), 16);
        }

        for (int i=0; i < bqual.length; i++) {
            bqual[i] = (byte) Integer.parseInt(bqualS.substring(2*i, 2*i + 2), 16);
        }

        return XATestUtil.getXid(fmtid, gtid, bqual);
    }

    public XATransactionTest(String name) {
        super(name);
    }

    public static Test suite() {
        // the test requires XADataSource to run
        if (JDBC.vmSupportsJDBC3()) {
            Test test = TestConfiguration.defaultSuite(XATransactionTest.class);
            // Set the lock timeout back to the default, because when
            // running in a bigger suite the value may have been
            // altered by an earlier test
            test = DatabasePropertyTestSetup.setLockTimeouts(test, 20, 60);
            return test;
        }

        return new BaseTestSuite(
            "XATransactionTest cannot run without XA support");
    }
}
