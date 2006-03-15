/*

 Derby - Class org.apache.derby.impl.services.bytecode.CodeChunk

 Copyright 2006 The Apache Software Foundation or its licensors, as applicable.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.functionTests.util.XATestUtil;

/**
 * XATests harvested from SQL XA tests.
 * Modified so that they can be run with NetworkServer.
 */
public class XATest {

    /**
     * Run all the tests.
     */
    public static void main(String[] args) throws Exception {
        ij.getPropertyArg(args);
        Connection dmc = ij.startJBMS();
        
        showHoldStatus("initial ", dmc);
        
        XATestUtil.createXATransactionView(dmc);
        dmc.close();

        XADataSource dsx = TestUtil.getXADataSource(cleanProperties());

        // tests originally from xaSimplePositive.sql
        singleConnectionOnePhaseCommit(dsx);
        xaShutdown();
        interleavingTransactions(dsx);

        xaShutdown();

        // tests originally from xaStateTran.sql
        noTransaction(dsx);

        // test originally from xaMorph.sql
        morph(dsx);
        
        // DERBY-966 holdability testing
        derby966(dsx);

        // for cleaning up, make a clean new connection
        Connection dmc2 = ij.startJBMS();
        cleanUp(dmc2);

        System.out.println("XATest complete");
    }

    /**
     * Get the basic set of properties for an XADataSource.
     * Only sets databaseName to wombat.
     * @return
     */
    private static Properties cleanProperties() {
        Properties dsAttrs = new Properties();
        dsAttrs.setProperty("databaseName", "wombat");
        return dsAttrs;
    }

    /**
     * Shutdown the database through an XADataSource.
     */
    private static void xaShutdown() {

        Properties dsAttrs = cleanProperties();

        if (TestUtil.isEmbeddedFramework())
            dsAttrs.put("shutdownDatabase", "shutdown");
        else
            dsAttrs.put("connectionAttributes", "shutdown=true");

        XADataSource dsx = TestUtil.getXADataSource(dsAttrs);

        try {
            dsx.getXAConnection().getConnection();
        } catch (SQLException sqle) {
            if ("08006".equals(sqle.getSQLState()))
                return;
            TestUtil.dumpSQLExceptions(sqle);
        }
        System.out.println("FAIL: no exception on shutdown");
    }

    /*
     ** Test cases
     */

    /**
     * A single connection and 1 phase commit.
     * 
     
     Original "SQL" from xaSimplePositive.sql
     <code>
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
     * @throws SQLException 
     * @throws XAException 
     */
    private static void singleConnectionOnePhaseCommit(XADataSource xads) {
        System.out.println("singleConnectionOnePhaseCommit");
        try {
            XAConnection xac = xads.getXAConnection();

            XAResource xar = xac.getXAResource();

            Xid xid = XATestUtil.getXid(0, 32, 46);

            xar.start(xid, XAResource.TMNOFLAGS);

            Connection conn = xac.getConnection();
            
            showHoldStatus("XA ", conn);

            Statement s = conn.createStatement();
            showHoldStatus("XA ", s);

            s.execute("create table foo (a int)");
            s.executeUpdate("insert into foo values (0)");

            ResultSet rs = s.executeQuery("select * from foo");
            JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
            rs.close();

            XATestUtil.showXATransactionView(conn);

            s.close();
            xar.end(xid, XAResource.TMSUCCESS);

            // 1 phase commit
            xar.commit(xid, true);

            conn.close();
            xac.close();

        } catch (SQLException sqle) {
            TestUtil.dumpSQLExceptions(sqle);
            sqle.printStackTrace(System.out);
        } catch (XAException e) {
            XATestUtil.dumpXAException("singleConnectionOnePhaseCommit", e);
        }
    }

    /*
     * Two interleaving transaction and prepare/commit prepare/rollback.
     * 
     * (original test said two connections but only one connection was opened)

     <code>
     xa_datasource 'wombat';
     xa_connect user 'sku' password 'testxa' ;

     xa_start xa_noflags 1;
     xa_getconnection;
     insert into APP.foo values (1);
     xa_end xa_suspend 1;

     xa_start xa_noflags 2;
     insert into APP.foo values (2);
     xa_end xa_suspend 2;

     xa_start xa_resume 1;
     insert into APP.foo values (3);
     xa_end xa_suspend 1;

     xa_start xa_resume 2;
     insert into APP.foo values (4);
     select * from APP.global_xactTable where gxid is not null order by gxid;
     -- this prepare won't work since transaction 1 has been suspended - XA_PROTO
     xa_prepare 1;

     select * from APP.global_xactTable where gxid is not null order by gxid;
     xa_end xa_success 2;

     -- this assumes a resume
     xa_end xa_success 1;
     xa_prepare 1;
     xa_prepare 2;

     -- both transactions should be prepared
     select * from APP.global_xactTable where gxid is not null order by gxid;

     -- NOTE: The following call to "xa_recover xa_startrscan" is apt to
     -- return the result set rows in reverse order when changes to
     -- the Derby engine affect the number of transactions that it takes
     -- to create a database.  The transactions are stored in a hash table
     -- based on a global and local id, and when the number of transactions
     -- changes, the (internal) local id can change, which may lead to a
     -- change in the result set order.  This order is determined by the
     -- JVM's hashing algorithm. Examples of changes to the engine that
     -- can affect this include ones that cause more commits or that
     -- change the amount of data being stored, such as changes to the
     -- metadata statements (which is what prompted this explanation in
     -- the first place).  Ultimately, the problem is that there is no
     -- way to order the return values from "xa_recover" since it is an
     -- ij internal statement, not SQL...
     xa_recover xa_startrscan;
     xa_recover xa_noflags;

     xa_commit xa_2Phase 1;
     xa_rollback 2;

     -- check results
     xa_start xa_noflags 3;
     select * from APP.global_xactTable where gxid is not null order by gxid;
     select * from APP.foo;
     xa_end xa_success 3;

     xa_prepare 3;

     -- should fail with XA_NOTA because we prepared a read only transaction 
     xa_commit xa_1Phase 3;
     disconnect;
     </code>
     */
    private static void interleavingTransactions(XADataSource xads) {
        System.out.println("interleavingTransactions");
        try {
            XAConnection xac = xads.getXAConnection("sku", "testxa");
            XAResource xar = xac.getXAResource();

            Xid xid1 = XATestUtil.getXid(1, 93, 18);
            Xid xid2 = XATestUtil.getXid(2, 45, 77);

            xar.start(xid1, XAResource.TMNOFLAGS);

            Connection conn = xac.getConnection();

            Statement s = conn.createStatement();
            s.executeUpdate("insert into APP.foo values (1)");
            xar.end(xid1, XAResource.TMSUSPEND);

            xar.start(xid2, XAResource.TMNOFLAGS);
            s.executeUpdate("insert into APP.foo values (2)");
            xar.end(xid2, XAResource.TMSUSPEND);

            xar.start(xid1, XAResource.TMRESUME);
            s.executeUpdate("insert into APP.foo values (3)");
            xar.end(xid1, XAResource.TMSUSPEND);

            xar.start(xid2, XAResource.TMRESUME);
            s.executeUpdate("insert into APP.foo values (4)");

            XATestUtil.showXATransactionView(conn);

            // this prepare won't work since
            // transaction 1 has been suspended - XA_PROTO
            try {
                xar.prepare(xid1);
                System.out.println("FAIL - prepare on suspended transaction");
            } catch (XAException e) {
                if (e.errorCode != XAException.XAER_PROTO)
                    XATestUtil.dumpXAException(
                            "FAIL - prepare on suspended transaction", e);

            }

            // check it was not prepared
            XATestUtil.showXATransactionView(conn);

            xar.end(xid2, XAResource.TMSUCCESS);

            xar.end(xid1, XAResource.TMSUCCESS);

            xar.prepare(xid1);
            xar.prepare(xid2);

            // both should be prepared.
            XATestUtil.showXATransactionView(conn);

            Xid[] recoveredStart = xar.recover(XAResource.TMSTARTRSCAN);
            System.out.println("recovered start " + recoveredStart.length);
            Xid[] recovered = xar.recover(XAResource.TMNOFLAGS);
            System.out.println("recovered " + recovered.length);
            Xid[] recoveredEnd = xar.recover(XAResource.TMENDRSCAN);
            System.out.println("recovered end " + recoveredEnd.length);

            for (int i = 0; i < recoveredStart.length; i++) {
                Xid xid = recoveredStart[i];
                if (xid.getFormatId() == 1) {
                    // commit 1 with 2pc
                    xar.commit(xid, false);
                } else if (xid.getFormatId() == 2) {
                    xar.rollback(xid);
                } else {
                    System.out.println("FAIL: unknown xact");
                }
            }

            // check the results
            Xid xid3 = XATestUtil.getXid(3, 2, 101);
            xar.start(xid3, XAResource.TMNOFLAGS);
            XATestUtil.showXATransactionView(conn);
            ResultSet rs = s.executeQuery("select * from APP.foo");
            JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
            rs.close();
            xar.end(xid3, XAResource.TMSUCCESS);

            int pr = xar.prepare(xid3);
            if (pr != XAResource.XA_RDONLY)
                System.out.println("FAIL - prepare on read only xact returned "
                        + pr);

            try {
                xar.commit(xid3, true);
                System.out.println("FAIL - 2pc commit on read-only xact");
            } catch (XAException e) {
                if (e.errorCode != XAException.XAER_NOTA)
                    throw e;
            }

            s.close();
            conn.close();
            xac.close();
        } catch (SQLException sqle) {
            TestUtil.dumpSQLExceptions(sqle);
        } catch (XAException e) {
            XATestUtil.dumpXAException("interleavingTransactions", e);
        }
    }

    /**  
     Tests on INIT STATE (no tr
     Original SQL from xaStateTran.sql. 
     <code>

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
    private static void noTransaction(XADataSource xads) {
        System.out.println("noTransaction");
        try {
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
        } catch (SQLException e) {
            TestUtil.dumpSQLExceptions(e);
        } catch (XAException e) {
            XATestUtil.dumpXAException("noTransaction", e);
        }
    }

    /**
     * Morph a connection between local anf global transactions.
     */
    private static void morph(XADataSource xads) {
        System.out.println("morph");

        try {
            XAConnection xac = xads.getXAConnection();

            XAResource xar = xac.getXAResource();

            Connection conn = xac.getConnection();

            /*
             autocommit off;
             insert into foo values (1);
             select * from global_xactTable where gxid is not null order by gxid,username;
             commit;
             */
            conn.setAutoCommit(false);
            Statement s = conn.createStatement();
            s.executeUpdate("insert into APP.foo values (2001)");
            XATestUtil.showXATransactionView(conn);
            conn.commit();

            /*
             autocommit on;
             insert into foo values (2);
             select * from global_xactTable where gxid is not null order by gxid,username;
             
             */

            conn.setAutoCommit(true);
            s.executeUpdate("insert into APP.foo values (2002)");
            XATestUtil.showXATransactionView(conn);

            /*
             -- morph the connection to a global transaction
             xa_start xa_noflags 1;
             select * from global_xactTable where gxid is not null order by gxid,username;
             insert into foo values (3);
             */

            Xid xid = XATestUtil.getXid(1001, 66, 13);
            xar.start(xid, XAResource.TMNOFLAGS);
            XATestUtil.showXATransactionView(conn);
            s.executeUpdate("insert into APP.foo values (2003)");

            /*
             -- disallowed
             commit;
             -- disallowed
             rollback;
             -- disallowed
             autocommit on;
             -- OK
             autocommit off;
             */
            try {
                conn.commit();
                System.out.println("FAIL: commit allowed in global xact");
            } catch (SQLException e) {
            }

            try {
                conn.rollback();
                System.out.println("FAIL: roll back allowed in global xact");
            } catch (SQLException e) {
            }
            try {
                conn.setAutoCommit(true);
                System.out
                        .println("FAIL: setAutoCommit(true) allowed in global xact");
            } catch (SQLException e) {
            }
            conn.setAutoCommit(false);

            // s was created in local mode so it has holdibilty
            // set, 
            try {
                s.executeQuery("select * from APP.foo where A >= 2000");
                System.out.println("FAIL: query with holdable statement");
            } catch (SQLException sqle) {
                TestUtil.dumpSQLExceptions(sqle, true);
            }
            s.close();

            s = conn.createStatement();
            boolean holdable = s.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT;
            System.out.println("Statement created in global has holdabilty: "
                    + holdable);

            /*
             select * from foo;
             xa_end xa_success 1;
             xa_prepare 1;
             */
            ResultSet rs = s
                    .executeQuery("select * from APP.foo where A >= 2000");
            JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
            rs.close();

            xar.end(xid, XAResource.TMSUCCESS);
            xar.prepare(xid);

            /*
             -- dup id
             xa_start xa_noflags 1;
             */
            try {
                xar.start(xid, XAResource.TMNOFLAGS);
                System.out.println("FAIL - start with duplicate XID");
            } catch (XAException e) {
                if (e.errorCode != XAException.XAER_DUPID)
                    throw e;
            }

            /*
             xa_start xa_noflags 2;
             -- still should disallow autommit;
             autocommit on;
             -- still should disallow commit and rollback 
             commit;
             rollback;
             select * from global_xactTable where gxid is not null order by gxid,username;
             xa_end xa_suspend 2;
             */

            Xid xid2 = XATestUtil.getXid(1002, 23, 3);
            xar.start(xid2, XAResource.TMNOFLAGS);
            try {
                conn.commit();
                System.out.println("FAIL: commit allowed in global xact");
            } catch (SQLException e) {
            }
            try {
                conn.rollback();
                System.out.println("FAIL: roll back allowed in global xact");
            } catch (SQLException e) {
            }
            try {
                conn.setAutoCommit(true);
                System.out
                        .println("FAIL: setAutoCommit(true) allowed in global xact");
            } catch (SQLException e) {
            }
            conn.setAutoCommit(false);

            xar.end(xid2, XAResource.TMSUSPEND);

            // DERBY-1004
            if (TestUtil.isDerbyNetClientFramework()) {
                System.out.println("DERBY-1004 Call conn.close to avoid exception with client");
                conn.close();
            }

            /*
             -- get local connection again
             xa_getconnection;

             insert into foo values (5);

             -- autocommit should be on by default;
             commit;

             autocommit off;
             insert into foo values (6);

             -- commit and rollback is allowed on local connection
             rollback;

             insert into foo values (6);
             commit;
             */
            conn = xac.getConnection();
            s = conn.createStatement();
            s.executeUpdate("insert into APP.foo values (2005)");
            conn.commit();
            conn.setAutoCommit(false);
            s.executeUpdate("insert into APP.foo values (2006)");
            conn.rollback();
            s.executeUpdate("insert into APP.foo values (2007)");
            conn.commit();

            XATestUtil.showXATransactionView(conn);
            /*
             -- I am still able to commit other global transactions while I am attached to a
             -- local transaction.
             xa_commit xa_2phase 1;
             xa_end xa_success 2;
             xa_rollback 2;
             */
            xar.commit(xid, false);
            xar.end(xid2, XAResource.TMSUCCESS);
            xar.rollback(xid2);

            XATestUtil.showXATransactionView(conn);
            rs = s.executeQuery("select * from APP.foo where A >= 2000");
            JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
            rs.close();

            conn.close();

            /*
             xa_getconnection;
             select * from global_xactTable where gxid is not null order by gxid,username;
             select * from foo;
             autocommit off;
             delete from foo;
             */
            conn = xac.getConnection();
            conn.setAutoCommit(false);
            s = conn.createStatement();
            s.executeUpdate("delete from app.foo");
            rs = s.executeQuery("select * from APP.foo");
            JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
            rs.close();

            // DERBY-1004
            if (TestUtil.isDerbyNetClientFramework()) {
                System.out.println("DERBY-1004 Call conn.rollback to avoid exception with client");
                conn.rollback();
            }
            /*
             -- yanking a local connection away should rollback the changes
             */
            conn = xac.getConnection();
            conn.setAutoCommit(false);
            s = conn.createStatement();
            rs = s.executeQuery("select * from APP.foo where A >= 2000");
            JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
            rs.close();

            /*
             -- cannot morph it if the local transaction is not idle
             xa_start xa_noflags 3;
             commit;
             -- now morph it to a global transaction
             xa_start xa_noflags 3;
             */
            Xid xid3 = XATestUtil.getXid(1003, 27, 9);
            try {
                xar.start(xid3, XAResource.TMNOFLAGS);
                System.out.println("FAIL XAResource.start on a global transaction with an active local transaction (autocommit false)");
            } catch (XAException xae) {
                if (xae.errorCode != XAException.XAER_OUTSIDE)
                    throw xae;
                System.out.println("Correct XAException on starting a global transaction with an active local transaction (autocommit false)");
            }
            conn.commit();
            xar.start(xid3, XAResource.TMNOFLAGS);

            /*
             -- now I shouldn't be able to yank it
             xa_getconnection;
             */
            if (TestUtil.isDerbyNetClientFramework()) {
                System.out.println("DERBY-341 - Client skipping XAConnection with active local transaction");              
            } else {
            try {
                xac.getConnection();
                System.out
                        .println("FAIL: getConnection with active global xact");
            } catch (SQLException sqle) {
                TestUtil.dumpSQLExceptions(sqle, true);
            }
            }
            /*
             select * from foo;
             delete from foo;

             xa_end xa_fail 3;
             xa_rollback 3;

             -- local connection again
             xa_getconnection;
             select * from global_xactTable where gxid is not null order by gxid,username;
             select * from foo;
             */
            s = conn.createStatement();
            s.executeUpdate("delete from APP.foo");
            rs = s.executeQuery("select * from APP.foo where A >= 2000");
            JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
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
            rs = s.executeQuery("select * from APP.foo where A >= 2000");
            JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
            rs.close();

            s.close();
            conn.close();

        } catch (SQLException e) {
            TestUtil.dumpSQLExceptions(e);
            e.printStackTrace(System.out);
        } catch (XAException e) {
            XATestUtil.dumpXAException("morph", e);
        }

    }
    
    /**
     * Derby-966 holdability and global/location transactions.
     * (work in progress)
     */
    private static void derby966(XADataSource xads)
    {
        System.out.println("derby966");
        
        try {
            XAConnection xac = xads.getXAConnection();
            XAResource xar = xac.getXAResource();

            Xid xid = XATestUtil.getXid(996, 9, 48);
            
            Connection conn = xac.getConnection();
            
            // Obtain Statements and PreparedStatements
            // with all the holdability options.
            
            showHoldStatus("Local ", conn);
           
            Statement sdh = conn.createStatement();
            showHoldStatus("Local(held) default ", sdh);
            checkHeldRS(conn, sdh, sdh.executeQuery("select * from app.foo"));
            PreparedStatement psdh = conn.prepareStatement("SELECT * FROM APP.FOO");
            showHoldStatus("Local(held) default Prepared", psdh);
            checkHeldRS(conn, psdh, psdh.executeQuery());
            
            Statement shh = conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            showHoldStatus("Local(held) held ", shh);
            checkHeldRS(conn, shh, shh.executeQuery("select * from app.foo"));
            PreparedStatement pshh =
                conn.prepareStatement("SELECT * FROM APP.FOO",
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
            showHoldStatus("Local(held) held Prepared", pshh);
            checkHeldRS(conn, pshh, pshh.executeQuery());
                        
            Statement sch = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.CLOSE_CURSORS_AT_COMMIT);
            showHoldStatus("Local(held) close ", sch);
            checkHeldRS(conn, sch, sch.executeQuery("select * from app.foo"));
            PreparedStatement psch =
                conn.prepareStatement("SELECT * FROM APP.FOO",
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.CLOSE_CURSORS_AT_COMMIT);
            showHoldStatus("Local(held) close Prepared", psch);
            checkHeldRS(conn, psch, psch.executeQuery());
         
            // set the connection's holdabilty to false
            conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
            
            Statement sdc = conn.createStatement();
            showHoldStatus("Local(close) default ", sdc);
            checkHeldRS(conn, sdc, sdc.executeQuery("select * from app.foo"));
            PreparedStatement psdc = conn.prepareStatement("SELECT * FROM APP.FOO");
            showHoldStatus("Local(close) default Prepared", psdc);
            checkHeldRS(conn, psdc, psdc.executeQuery());
 
            Statement shc = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
            showHoldStatus("Local(close) held ", shc);
            checkHeldRS(conn, shc, shc.executeQuery("select * from app.foo"));
            PreparedStatement pshc =
                conn.prepareStatement("SELECT * FROM APP.FOO",
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT);
            showHoldStatus("Local(close) held Prepared", pshc);
            checkHeldRS(conn, pshc, pshc.executeQuery());
            
            Statement scc = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.CLOSE_CURSORS_AT_COMMIT);
            showHoldStatus("Local(close) close ", scc);
            checkHeldRS(conn, scc, scc.executeQuery("select * from app.foo"));
            PreparedStatement pscc =
                conn.prepareStatement("SELECT * FROM APP.FOO",
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.CLOSE_CURSORS_AT_COMMIT);
            showHoldStatus("Local(close) close Prepared", pscc);
            checkHeldRS(conn, pscc, pscc.executeQuery());
            
            // Revert back to holdable
            conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
            
            ResultSet rs = sdh.executeQuery("SELECT * FROM APP.FOO");
            rs.next(); System.out.println("BGBC " + rs.getInt(1));
            conn.commit();
            rs.next(); System.out.println("BGAC " + rs.getInt(1));
            
            // This switch to global is ok because conn
            // is in auto-commit mode, thus the start performs
            // an implicit commit to complete the local transaction.
            
            // DERBY-1025 Client only bug
            if (TestUtil.isDerbyNetClientFramework()) {
                System.out.println("DERBY-1025 Call conn.commit to avoid exception with client");
                conn.commit();
            }
            System.out.println("START GLOBAL TRANSACTION");
            // start a global xact and test those statements.
            xar.start(xid, XAResource.TMNOFLAGS);
            
            // Statements obtained while default was hold.
            // Only sch should work as held cursors not supported in XA
            try {
                sdh.executeQuery("SELECT * FROM APP.FOO").close();
                System.out.println("FAIL - held Statement in global");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e, true);
            }
            try {
                shh.executeQuery("SELECT * FROM APP.FOO").close();
                System.out.println("FAIL - held Statement in global");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e, true);
            }
            sch.executeQuery("SELECT * FROM APP.FOO").close();
            
            // PreparedStatements obtained while default was hold.
            // Only sch should work as held cursors not supported in XA
            try {
                psdh.executeQuery().close();
                System.out.println("FAIL - held Statement in global");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e, true);
            }
            try {
                pshh.executeQuery().close();
                System.out.println("FAIL - held Statement in global");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e, true);
            }
            psch.executeQuery().close();
             
            // Statements obtained while default was close.
            // Only sch should work as held cursors not supported in XA
            sdc.executeQuery("SELECT * FROM APP.FOO").close();
            try {
                shc.executeQuery("SELECT * FROM APP.FOO").close();
                System.out.println("FAIL - held Statement in global");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e, true);
            }
            scc.executeQuery("SELECT * FROM APP.FOO").close();
            
            // PreparedStatements obtained while default was close.
           psdc.executeQuery().close();
           try {
                pshc.executeQuery().close();
                System.out.println("FAIL - held Statement in global");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e, true);
            }
            pscc.executeQuery().close();
                   
            // Test we cannot switch the connection to holdable
            // or create a statement with holdable.
            try {
                conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
                System.out.println("FAIL - set holdability in global xact.");
            } catch (SQLException sqle)
            {
                TestUtil.dumpSQLExceptions(sqle, true);
            }
            
            try {
                    conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    System.out.println("FAIL - Statement holdability in global xact.");
            } catch (SQLException sqle) {
                TestUtil.dumpSQLExceptions(sqle, true);
            }
            try {
                conn.prepareStatement(
                "SELECT * FROM APP.FOO",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
                System.out.println("FAIL - PreparedStatement holdability in global xact.");
        } catch (SQLException sqle) {
            TestUtil.dumpSQLExceptions(sqle, true);
        }
            xar.end(xid, XAResource.TMSUCCESS);
            if (xar.prepare(xid) != XAResource.XA_RDONLY)
                System.out.println("FAIL prepare didn't indicate r/o");
            
            conn.close();
            
            System.out.println("derby966 complete");
                
        } catch (SQLException e) {
            TestUtil.dumpSQLExceptions(e);
            e.printStackTrace(System.out);
        } catch (XAException e) {
            XATestUtil.dumpXAException("derby966", e);
        }
    }
    
    /**
     * Check the held state of a ResultSet by fetching
     * one row, executing a commit and then fetching the
     * next. Checks the held state matches the behaviour.
    */
    private static void checkHeldRS(Connection conn,
            Statement s, ResultSet rs) throws SQLException
    {
        if (s.getConnection() != conn)
            System.out.println("FAIL - mismatched statement & Connection");
        if (rs.getStatement() != s)
        {
            // DERBY-1009
            System.out.println("FAIL - mismatched statement & ResultSet");
            System.out.println("Statement class " + s.getClass());
            System.out.println("ResultSet' Statements class " + rs.getStatement().getClass());
         }

        boolean held = s.getResultSetHoldability() ==
            ResultSet.HOLD_CURSORS_OVER_COMMIT;
        
        System.out.println("ResultSet " + holdStatus(s.getResultSetHoldability()));
        
        rs.next();
        System.out.println("  BC A=" + rs.getInt(1));
        conn.commit();
       
        try {
            while (rs.next())
            {
                rs.getInt(1);
                System.out.println("  AC A=" + rs.getInt(1));
            }
           if (!held)
               System.out.println("FAIL: non-held cursor not closed by commit");
        } catch (SQLException sqle)
        {
            boolean ok = !held;
            boolean showError = true;
            if (ok) {
                if (TestUtil.isEmbeddedFramework()) {
                    if ("XCL16".equals(sqle.getSQLState()))
                        showError = false;
                } else if (TestUtil.isDerbyNetClientFramework()) {
                    // No SQL state yet from client error.
                    showError = false;
                }
            }
            if (showError)
                TestUtil.dumpSQLExceptions(sqle, ok);
            else if (ok)
                System.out.println("Non-held ResultSet correctly closed after commit");
        }
        
        rs.close();
        conn.commit();
    }
    
    /**
     * Show the held status of the Statement.
    */
    private static void showHoldStatus(String tag, Statement s) throws SQLException
    {
        System.out.println(tag + "Statement holdable " +
                holdStatus(s.getResultSetHoldability()));
    }
    /**
     * Show the held status of the Connection.
    */
    private static void showHoldStatus(String tag, Connection conn) throws SQLException
    {
        System.out.println(tag + "Connection holdable " +
                holdStatus(conn.getHoldability()));
    }
    
    private static String holdStatus(int holdability)
    {
        String s;
        switch (holdability)
        {
        case ResultSet.CLOSE_CURSORS_AT_COMMIT:
            s = "CLOSE_CURSORS_AT_COMMIT ";
            break;
        case ResultSet.HOLD_CURSORS_OVER_COMMIT:
            s = "HOLD_CURSORS_OVER_COMMIT ";
            break;
        default:
            s = "UNKNOWN HOLDABILITY ";
            break;
        }
        
        return s + Integer.toString(holdability);
    }
    
    /*
     * 5 interleaving transactions.
     * Taken from the SQL test xaANotherTest.
     * <code>
xa_connect user 'mamta' password 'mamta' ;

-- global connection 1
xa_start xa_noflags 1;
xa_getconnection;
insert into APP.foo values (1);
xa_end xa_suspend 1;

-- global connection 2
xa_start xa_noflags 2;
insert into APP.foo values (2);
xa_end xa_suspend 2;

-- global connection 3
xa_start xa_noflags 3;
insert into APP.foo values (3);
xa_end xa_suspend 3;

-- global connection 4
xa_start xa_noflags 4;
insert into APP.foo values (4);
xa_end xa_suspend 4;

-- global connection 5
xa_start xa_noflags 5;
insert into APP.foo values (5);
xa_end xa_suspend 5;

xa_start xa_resume 1;
insert into APP.foo values (11);
xa_end xa_suspend 1;

xa_start xa_resume 5;
insert into APP.foo values (55);
xa_end xa_suspend 5;

xa_start xa_resume 2;
insert into APP.foo values (22);
xa_end xa_suspend 2;

xa_start xa_resume 4;
insert into APP.foo values (44);
xa_end xa_suspend 4;

xa_start xa_resume 3;
insert into APP.foo values (33);
xa_end xa_suspend 3;

-- prepare all the global connections except the first one. This way, we will see all
-- the global transactions prepared so far after the database shutdown and restart.
xa_end xa_success 2;
xa_prepare 2;
xa_end xa_success 3;
xa_prepare 3;
xa_end xa_success 4;
xa_prepare 4;
xa_end xa_success 5;
xa_prepare 5;

     * </code>
     */
    private static void interleavingTransactions5(XADataSource xads) throws SQLException
    {
        System.out.println("interleavingTransactions5");
        
        XAConnection xac = xads.getXAConnection("mamta", "mamtapwd");
        
    }

    private static void cleanUp(Connection conn) throws SQLException
    {
        String testObjects[] = { "view XATESTUTIL.global_xactTable", 
                                 "schema XATESTUTIL restrict", "table app.foo", "table foo" };
        Statement stmt = conn.createStatement();
        TestUtil.cleanUpTest(stmt, testObjects);
        conn.commit();
        stmt.close();
    }
 
}
