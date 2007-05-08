/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DataSourceTest

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

import java.io.File;
import java.io.Serializable;
import java.security.AccessController;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Iterator;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.jdbc.ClientConnectionPoolDataSource;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.jdbc.ClientXADataSource;
import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.derby.jdbc.EmbeddedSimpleDataSource;
import org.apache.derby.jdbc.EmbeddedXADataSource;
import org.apache.derbyTesting.functionTests.util.SecurityCheck;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the various embedded DataSource implementations of Derby.
 * 
 * Performs SecurityCheck analysis on the JDBC objects returned.
 * This is because this test returns to the client a number of
 * different implementations of Connection, Statement etc.
 * 
 * @see org.apache.derbyTesting.functionTests.util.SecurityCheck
 *
 */
public class DataSourceTest extends BaseJDBCTestCase {

    protected static String dbName = 
        TestConfiguration.getCurrent().getDefaultDatabaseName();
    
    /**
     * A hashtable of opened connections.  This is used when checking to
     * make sure connection strings are unique; we need to make sure all
     * the connections are closed when we are done, so they are stored
     * in this hashtable
     */
    protected static Hashtable conns = new Hashtable();
    
    /** The expected format of a connection string. In English:
     * "<classname>@<hashcode> (XID=<xid>), (SESSION = <sessionid>),
     *  (DATABASE=<dbname>), (DRDAID = <drdaid>)"
     */
    private static final String CONNSTRING_FORMAT = 
        "\\S+@[0-9]+.* \\(XID = .*\\), \\(SESSIONID = [0-9]+\\), " +
        "\\(DATABASE = [A-Za-z]+\\), \\(DRDAID = .*\\) "; 
    
    
    /**
     * Hang onto the SecurityCheck class while running the
     * tests so that it is not garbage collected during the
     * test and lose the information it has collected,
     * in case it should get printed out.
     */
    private final Object nogc = SecurityCheck.class;
    
    public DataSourceTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        if (JDBC.vmSupportsJSR169())
        {
            // test uses unsupported classes like DriverManager, XADataSource,
            // ConnectionPoolDataSource, ConnectionEvenListenere, as well as
            // unsupported methods, like Connection.setTypeMap()...
            TestSuite suite = 
                new TestSuite("DatasourceTest cannot run with JSR169");
            return suite;
        }
        else
        {
            return TestConfiguration.defaultSuite(DataSourceTest.class);
        }
    }
    
    /**
     * Set up the conection to the database.
     */
    public void setUp() throws  Exception {
        Statement s = createStatement();
        s.executeUpdate("create table autocommitxastart(i int)");
        s.executeUpdate("insert into autocommitxastart values 1,2,3,4,5");
        s.executeUpdate("create schema SCHEMA_Patricio");
        s.executeUpdate("create table " +
            "SCHEMA_Patricio.Patricio (id VARCHAR(255), value INTEGER)");
        s.executeUpdate("create table intTable(i int)");
        s.executeUpdate("create table hold_30 " +
            "(id int not null primary key, b char(30))");
        s.executeUpdate(
            "create procedure checkConn2(in dsname varchar(20)) " +
            "parameter style java language java modifies SQL DATA " +
            "external name " +
            "'org.apache.derbyTesting.functionTests.tests.jdbcapi." +
            this.getNestedMethodName() +
            "'");

        // theoretically, commit should be unnecessary, because 
        // autocommit should be true by default.
        commit();
        s.close();
    }
    
    public void tearDown() throws Exception {
        getConnection().setAutoCommit(false);
        Statement s = createStatement();
        s.executeUpdate("drop table autocommitxastart");
        s.executeUpdate("drop table intTable");
        s.executeUpdate("drop table hold_30");
        s.executeUpdate("drop table SCHEMA_Patricio.Patricio");
        s.executeUpdate("drop schema SCHEMA_Patricio restrict");
        s.executeUpdate("drop procedure checkConn2");
        // should be automatic?        
        commit();
        s.close();

        // attempt to get rid of any left-over trace files
        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                for (int i=0 ; i < 6 ; i++)
                {   
                    String traceFileName = "trace" + (i+1) + ".out";
                    File traceFile = new File(traceFileName);
                    if (traceFile.exists())
                    {
                        // if it exists, attempt to get rid of it
                        traceFile.delete();
                    }
                } 
                return null;
            }
        });
        super.tearDown();
    }

    /* comment out. leaving in, just in case it's ever relevant.
     * when uncommented, this will run when network server tests are
     * started, and then reflect the results of the embedded checks.
    // perform security analysis of the public api for the embedded engine
    public void testDataSourceAPI() throws SQLException, ClassNotFoundException
    {
        SecurityCheck.report();
    }
     */
    
    public void testAllDataSources() throws SQLException, Exception
    {
        Connection dmc = getConnection();
        CallableStatement cs = dmc.prepareCall("call checkConn2(?)");
        cs.setString(1,"Nested");
        try {
            cs.execute();
        } catch (SQLException sqle) {
            assertSQLState("40XC0", sqle);
        }
        cs.setString(1,"Nested2");
        cs.execute();
        
        String EmptyMapValue=null;
        // Note: currently, not supported
        String NullMapValue=null;
        String MapMapValue=null;
        if (usingEmbedded())
        {
            EmptyMapValue="OK"; NullMapValue="XJ081"; MapMapValue="0A000";
        }
        else if (usingDerbyNetClient())
        {
            EmptyMapValue="0A000"; NullMapValue="0A000"; MapMapValue="0A000";
        }
        Object[] expectedValues = {
            new Integer(ResultSet.HOLD_CURSORS_OVER_COMMIT), "XJ010",
            new Integer(2), new Boolean(true), new Boolean(false), 
            EmptyMapValue, NullMapValue, MapMapValue};

        assertConnectionOK(expectedValues, "DriverManager ", dmc);
    
        if (usingEmbedded())
            assertTenConnectionsUnique();

        DataSource dscs = JDBCDataSource.getDataSource(dbName);
        if (usingEmbedded()) 
                assertToString(dscs);

        DataSource ds = dscs;
        assertConnectionOK(expectedValues, "DataSource", ds.getConnection());
        
        DataSource dssimple = null;
        // simple datasource is only supported with embedded
        if (usingEmbedded())
        {
            EmbeddedSimpleDataSource realdssimple = 
                new EmbeddedSimpleDataSource();
            realdssimple.setDatabaseName(dbName);
            ds = realdssimple;
            dssimple = (DataSource)realdssimple;
            assertConnectionOK(
                expectedValues, "SimpleDataSource", ds.getConnection());
        }
            
        ConnectionPoolDataSource dsp = 
            J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(dsp, "DatabaseName", dbName);        
        
        if (usingEmbedded()) 
            assertToString(dsp);

        PooledConnection pc = dsp.getPooledConnection();
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(
                pc, "javax.sql.PooledConnection");
        }
        pc.addConnectionEventListener(new AssertEventCatcher(1));

        // DERBY-2531
        // with Network Server / DerbyNetClient, the assertConnectionOK check
        // returns a different connection object...
        assertConnectionOK(
            expectedValues, "ConnectionPoolDataSource", pc.getConnection());
        assertConnectionOK(
            expectedValues, "ConnectionPoolDataSource", pc.getConnection());

        XADataSource dsx = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(dsx, "DatabaseName", dbName);
        if (usingEmbedded())
            assertToString(dsx);

        // shutdown db and check all's still ok thereafter
        TestConfiguration.getCurrent().shutdownDatabase();

        dmc = getConnection();
        cs = dmc.prepareCall("call checkConn2(?)");
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(
                cs, "java.sql.CallableStatement");
        }
        cs.setString(1,"Nested");
        try {
            cs.execute();
        } catch (SQLException sqle) {
            assertSQLState("40XC0", sqle);
        }
        cs.setString(1, "Nested2");
        cs.execute();

        XAConnection xac = dsx.getXAConnection();
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(xac, "javax.sql.XAConnection");
        }
        xac.addConnectionEventListener(new AssertEventCatcher(3));
        assertConnectionOK(
            expectedValues, "XADataSource", xac.getConnection());

        pc = dsp.getPooledConnection();
        pc.addConnectionEventListener(new AssertEventCatcher(2));
        assertConnectionOK(
            expectedValues, "ConnectionPoolDataSource", pc.getConnection());

        // test "local" XAConnections
        xac = dsx.getXAConnection();
        xac.addConnectionEventListener(new AssertEventCatcher(4));
        assertConnectionOK(
            expectedValues, "XADataSource", xac.getConnection());
        assertConnectionOK(
            expectedValues, "XADataSource", xac.getConnection());
        xac.close();

        // test "global" XAConnections
        xac = dsx.getXAConnection();
        xac.addConnectionEventListener(new AssertEventCatcher(5));
        XAResource xar = xac.getXAResource();
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
           SecurityCheck.assertSourceSecurity(
                xar, "javax.transaction.xa.XAResource");
        }
        Xid xid = new cdsXid(1, (byte) 35, (byte) 47);
        xar.start(xid, XAResource.TMNOFLAGS);
        Connection xacc = xac.getConnection();
        xacc.close();
        expectedValues[0] = new Integer(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        if (usingEmbedded())
            expectedValues[1] = "XJ058";
        expectedValues[3] = new Boolean(false);
        assertConnectionOK(
            expectedValues, "Global XADataSource", xac.getConnection());
        assertConnectionOK(
            expectedValues, "Global XADataSource", xac.getConnection());

        xar.end(xid, XAResource.TMSUCCESS);

        expectedValues[0] = new Integer(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        expectedValues[3] = new Boolean(true);
        assertConnectionOK(expectedValues, 
            "Switch to local XADataSource", xac.getConnection());
        assertConnectionOK(expectedValues, 
            "Switch to local XADataSource", xac.getConnection());

        Connection backtoGlobal = xac.getConnection();

        xar.start(xid, XAResource.TMJOIN);
        expectedValues[0] = new Integer(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        expectedValues[3] = new Boolean(false);
        assertConnectionOK(expectedValues, 
            "Switch to global XADataSource", backtoGlobal);
        assertConnectionOK(expectedValues, 
            "Switch to global XADataSource", xac.getConnection());
        xar.end(xid, XAResource.TMSUCCESS);
        xar.commit(xid, true);

        xac.close();
    }
    
    public void testClosedCPDSConnection() throws SQLException, Exception {
        // verify that outstanding updates from a closed connection, obtained
        // from a ConnectionPoolDataSource, are not committed, but rolled back.
        ConnectionPoolDataSource dsp = 
            J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(dsp, "DatabaseName", dbName);        
        PooledConnection pc = dsp.getPooledConnection();
        Connection c1 = pc.getConnection();
        Statement s = c1.createStatement();
        c1.setAutoCommit(false);

        // this update should get rolled back later
        s.executeUpdate("insert into intTable values(1)");
        // this should automatically close the original connection
        c1 = pc.getConnection();

        ResultSet rs = 
            c1.createStatement().executeQuery("select count(*) from intTable");
        rs.next();
        assertEquals(0, rs.getInt(1));
        c1.close();
        
        // check connection objects are closed once connection is closed
        try {
            rs.next();
            fail("ResultSet is open for a closed connection obtained from PooledConnection");
        } catch (SQLException sqle) {
            // 08003 - No current connection; XCL16 - ResultSet not open
            if (usingEmbedded())
                assertSQLState("08003", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("XCL16", sqle);
        }

        try {
            s.executeUpdate("update intTable set i = 1");
            fail("Statement is open for a closed connection " +
                "obtained from PooledConnection");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }

        pc.close();
        pc = null;
        PoolReset("ConnectionPoolDataSource", dsp.getPooledConnection());
        s.close();
        rs.close();
        c1.close();
    }

    public void testClosedXADSConnection() throws SQLException, Exception {
        // verify that outstanding updates from a closed connection, obtained
        // from an XADataSource, are not committed, but rolled back.
        XADataSource dsx = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(dsx, "DatabaseName", dbName);
        XAConnection xac = dsx.getXAConnection();
        Connection c1 = xac.getConnection();
        Statement s = c1.createStatement();

        c1.setAutoCommit(false);

        // this update should be rolled back
        s.executeUpdate("insert into intTable values(2)");
        
        c1 = xac.getConnection();

        ResultSet rs = c1.createStatement().executeQuery(
           "select count(*) from intTable");
        rs.next();

        assertEquals(0, rs.getInt(1));

        rs.close();
        c1.close();
        xac.close();
        xac = null;

        PoolReset("XADataSource", dsx.getXAConnection());
    }

    public void testGlobalLocalInterleaf() throws SQLException, XAException {    
        // now some explicit tests for how connection state behaves
        // when switching between global transactions and local
        // and setting connection state.
        // some of this may be tested elsewhere too.

        XADataSource dsx = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(dsx, "DatabaseName", dbName);
        XAConnection xac = dsx.getXAConnection();
        xac.addConnectionEventListener(new AssertEventCatcher(6));
        XAResource xar = xac.getXAResource();
        Xid xid = new cdsXid(1, (byte) 93, (byte) 103);

        // series 1 - Single connection object
        Connection cs1 = xac.getConnection();
        // initial local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        xar.start(xid, XAResource.TMNOFLAGS);
        // initial X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            false, false, cs1);
        cs1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        cs1.setReadOnly(true);
        setHoldability(cs1, false); // close cursors
        // modified X1
        boolean ReadOnly = false;
        // see DERBY-911, ReadOnly state different for Embedded/DerbyNetClient
        if (usingEmbedded())
            ReadOnly = true;
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        xar.end(xid, XAResource.TMSUCCESS);
        // the underlying local transaction/connection must pick up the
        // state of the Connection handle cs1
        // modified local:
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, ReadOnly, cs1);
        
        cs1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        cs1.setReadOnly(false);
        setHoldability(cs1, false); // close cursors

        // reset local
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);

        // now re-join the transaction, should pick up the read-only
        // and isolation level from the transaction,
        // holdability remains that of this handle.
        xar.start(xid, XAResource.TMJOIN);
        // re-join X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        xar.end(xid, XAResource.TMSUCCESS);

        // back to local - should be the same as the reset local
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        
        // test suspend/resume
        // now re-join the transaction (X1) for the second time, should pick
        // up the read-only and isolation level from the transaction,
        // holdability remains that of this handle.
        xar.start(xid, XAResource.TMJOIN);
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        
        xar.end(xid, XAResource.TMSUSPEND);
        // local after suspend
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        
        xar.start(xid, XAResource.TMRESUME);
        // resume X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        
        xar.end(xid, XAResource.TMSUCCESS);
        // back to local (second time)
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        
        cs1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        cs1.setReadOnly(true);
        setHoldability(cs1, true); // hold
        cs1.close();
        
        cs1 = xac.getConnection();
        // new handle - local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, cs1);
        cs1.close();
        
        xar.start(xid, XAResource.TMJOIN);
        cs1 = xac.getConnection();
        // re-join with new handle X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        cs1.close();
        xar.end(xid, XAResource.TMSUCCESS);

        // now get a connection (attached to a local)
        // attach to the global and commit it.
        // state should be that of the local after the commit.
        cs1 = xac.getConnection();
        cs1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        // pre-X1 commit - local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, cs1);
        xar.start(xid, XAResource.TMJOIN);
        // pre-X1 commit - X1
        assertConnectionState(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, ReadOnly, cs1);
        xar.end(xid, XAResource.TMSUCCESS);
        // post-X1 end - local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, cs1);
        xar.commit(xid, true);
        // post-X1 commit - local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, cs1);
        cs1.close();
    }
    
    // really part of testGlobalLocalInterLeaf:
    /**
     * @throws SQLException
     * @throws XAException
     */
    public void testSetIsolationWithStatement() 
    throws SQLException, XAException {
        // DERBY-421 Setting isolation level with SQL was not getting 
        // handled correctly 
        // Some more isolation testing using SQL and JDBC api
        XADataSource dsx = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(dsx, "DatabaseName", dbName);
        XAConnection xac = dsx.getXAConnection();
        xac.addConnectionEventListener(new AssertEventCatcher(6));
        XAResource xar = xac.getXAResource();
        Connection conn = xac.getConnection();
        Statement s = conn.createStatement();
        // initial local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_COMMITTED,
            true, false, conn);

        // Issue setTransactionIsolation in local transaction
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        // setTransactionIsolation in local
        assertConnectionState(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, false, conn);

        Xid xid;
        //Issue SQL to change isolation in local transaction
        s.executeUpdate("set current isolation = RR");
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_SERIALIZABLE,
            true, false, conn);

        xid = new cdsXid(1, (byte) 35, (byte) 47);
        xar.start(xid, XAResource.TMNOFLAGS);
        // 1st global (new)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            Connection.TRANSACTION_SERIALIZABLE,
            false, false, conn);
        xar.end(xid, XAResource.TMSUCCESS);

        // local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT,
            Connection.TRANSACTION_SERIALIZABLE,
            true, false, conn);
        //Issue SQL to change isolation in local transaction
        s.executeUpdate("set current isolation = RS");
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, conn);

        // DERBY-1325 - Isolation level of local connection does not get reset after ending 
        // a global transaction that was joined/resumed if the isolation level was changed 
        // using SQL 
        xar.start(xid, XAResource.TMJOIN);
        // 1st global(existing)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_SERIALIZABLE,
            false, false, conn);
        xar.end(xid, XAResource.TMSUCCESS);
        // local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, conn);
        // DERBY-1325 end test 

        Xid xid2 = new cdsXid(1, (byte) 93, (byte) 103);
        xar.start(xid2, XAResource.TMNOFLAGS);
        // 2nd global (new)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            false, false, conn);
        xar.end(xid2, XAResource.TMSUCCESS);

        xar.start(xid, XAResource.TMJOIN);
        // 1st global (existing)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_SERIALIZABLE,
            false, false, conn);
        xar.end(xid, XAResource.TMSUCCESS);

        //local 
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            true, false, conn);

        xar.start(xid, XAResource.TMJOIN);
        
        // 1st global (existing)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_SERIALIZABLE,
            false, false, conn);
        // Issue SQL to change isolation in 1st global transaction
        s.executeUpdate("set current isolation = UR");
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            false, false, conn);
        xar.end(xid, XAResource.TMSUCCESS);

        // local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, false, conn);

        xar.start(xid2, XAResource.TMJOIN);
        // 2nd global (existing)
        assertConnectionState(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            Connection.TRANSACTION_REPEATABLE_READ,
            false, false, conn);
        xar.end(xid2, XAResource.TMSUCCESS);
        xar.rollback(xid2);
        // (After 2nd global rollback ) local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, false, conn);

        xar.rollback(xid);
        // (After 1st global rollback) local
        assertConnectionState(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            Connection.TRANSACTION_READ_UNCOMMITTED,
            true, false, conn);
    }

    // This test includes some short-hand descriptions of the test cases
    // left in for reference to the original non-junit test
    public void testReuseAcrossGlobalLocal() throws SQLException, XAException {

        // DERBY-2533 -
        // network server cannot run this test - it hits a protocol error
        // on tearDown. Embedded requires a database shutdown
        if (usingDerbyNetClient())
            return;
        
        int[] onetwothree = {1,2,3};
        int[] three = {3};
        int[] pspc = {1, 4}; // expected parameter count for prepared statements
        int[] cspc = {2, 12, 12}; // for callable statements
        
        // statics for testReuseAcrossGlobalLocal
        int[] StatementExpectedValues = {
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY,
            ResultSet.FETCH_REVERSE, 444, 713, 19, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT};
            //ResultSet.CLOSE_CURSORS_AT_COMMIT};
        int[] PreparedStatementExpectedValues = { 
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY,
            ResultSet.FETCH_REVERSE, 888, 317, 91,
            ResultSet.HOLD_CURSORS_OVER_COMMIT};
        int[] CallableStatementExpectedValues = { 
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY,
            ResultSet.FETCH_REVERSE, 999, 137, 85,
            ResultSet.HOLD_CURSORS_OVER_COMMIT};

        XADataSource dsx = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(dsx, "DatabaseName", dbName);
        XAConnection xac = dsx.getXAConnection();
        xac.addConnectionEventListener(new AssertEventCatcher(6));
        XAResource xar = xac.getXAResource();
        Xid xid = new cdsXid(1, (byte) 103, (byte) 119);

        // now check re-use of *Statement objects across local/global 
        // connections.
        Connection cs1 = xac.getConnection();

        // ensure read locks stay around until end-of transaction
        cs1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        cs1.setAutoCommit(false);
        assertLocks(null, cs1);

        Statement sru1 = cs1.createStatement();
        sru1.setCursorName("SN1");
        sru1.executeUpdate("insert into intTable values 1,2,3");
        Statement sruBatch = cs1.createStatement();
        sruBatch.setCursorName("sruBatch");
        Statement sruState = createFloatStatementForStateChecking(
            StatementExpectedValues, cs1);
        PreparedStatement psruState = createFloatStatementForStateChecking(
            new int[] {1, 4}, PreparedStatementExpectedValues, cs1, 
            "select i from intTable where i = ?");
        CallableStatement csruState = createFloatCallForStateChecking(
            new int[] {2, 12, 12}, CallableStatementExpectedValues, cs1, 
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)");
        PreparedStatement psParams = 
            cs1.prepareStatement("select * from intTable where i > ?");
        psParams.setCursorName("params");
        psParams.setInt(1, 2);
        // Params-local-1
        resultSetQuery("params", three, psParams.executeQuery());

        sruBatch.addBatch("insert into intTable values 4");
        // sru1-local-1
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        cs1.commit(); // need to commit to switch to an global connection;

        // simple case - underlying connection is re-used for global.
        xar.start(xid, XAResource.TMNOFLAGS); 
        // Expecting downgrade because global transaction sru1-global-2 is 
        // using a statement with holdability true
        // sru1-global-2
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        sruBatch.addBatch("insert into intTable values 5");
        Statement sru2 = cs1.createStatement();
        sru2.setCursorName("OAK2");
        //sru2-global-3
        queryOnStatement("OAK2", onetwothree, cs1, sru2);
        // Expecting downgrade because global transaction sru1-global-4 is
        // using a statement with holdability true
        // sru1-global-4
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        // Global statement
        StatementExpectedValues[6] = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        PreparedStatementExpectedValues[6] = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        CallableStatementExpectedValues[6] = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        assertStatementState(null, StatementExpectedValues ,sruState);
        // Global PreparedStatement
        assertStatementState(pspc, PreparedStatementExpectedValues, psruState);
        // Global CallableStatement
        assertStatementState(cspc, CallableStatementExpectedValues, csruState);
        // Params-global-1
        resultSetQuery("params", three, psParams.executeQuery());

        xar.end(xid, XAResource.TMSUCCESS);
        // now a new underlying connection is created
        // sru1-local-5
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        // sru2-local-6
        queryOnStatement("OAK2", onetwothree, cs1, sru2);
        sruBatch.addBatch("insert into intTable values 6,7");
        Statement sru3 = cs1.createStatement();
        sru3.setCursorName("SF3");
        // sru3-local-7
        queryOnStatement("SF3", onetwothree, cs1, sru3);
        // Two transactions should hold locks (global and the current XA);
        // LOCAL
        StatementExpectedValues[6] = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        PreparedStatementExpectedValues[6] = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        CallableStatementExpectedValues[6] = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        assertStatementState(null, StatementExpectedValues, sruState); 
        assertStatementState(pspc, PreparedStatementExpectedValues, psruState);
        assertStatementState(cspc, CallableStatementExpectedValues, csruState);
        // Params-local-2
        resultSetQuery("params", three, psParams.executeQuery());
        assertLocks(new int[] {14,14}, cs1);
        cs1.commit();

        // attach the XA transaction to another connection and see what happens
        XAConnection xac2 = dsx.getXAConnection();
        xac2.addConnectionEventListener(new AssertEventCatcher(5));
        XAResource xar2 = xac2.getXAResource();

        xar2.start(xid, XAResource.TMJOIN);
        Connection cs2 = xac2.getConnection();

        // these statements were generated by cs1 and thus are still
        // in a local connection.
        // sru1-local-8
        queryOnStatement("SN1", onetwothree, cs1, sru1);
        // sru2-local-9
        queryOnStatement("OAK2", onetwothree, cs1, sru2);
        // sru3-local-10
        queryOnStatement("SF3", onetwothree, cs1, sru3);
        sruBatch.addBatch("insert into intTable values 8");
        // LOCAL 2
        assertStatementState(null, StatementExpectedValues, sruState);
        assertStatementState(pspc, PreparedStatementExpectedValues, psruState);
        assertStatementState(cspc, CallableStatementExpectedValues, csruState);

        assertLocks(new int[] {14, 12}, cs1);

        int[] updateCounts = sruBatch.executeBatch();
        int[] expectedUpdateCounts = {1, 1, 2, 1};
        // sruBatch update counts: 
        for (int i = 0; i < updateCounts.length; i++) {
            assertEquals(expectedUpdateCounts[i], updateCounts[i]);
        }
        // sruBatch
        queryOnStatement(
            "sruBatch", new int[] {1,2,3,4,5,6,7,8}, cs1, sruBatch);

        xar2.end(xid, XAResource.TMSUCCESS);
        xac2.close();

        // allow close on already closed XAConnection
        xac2.close();
        xac2.addConnectionEventListener(null);
        xac2.removeConnectionEventListener(null);

        // test methods against a closed XAConnection and its resource
        try {
            xac2.getXAResource();
            // DERBY-2532
            // Network Server does not think this is worth an exception.
            if (usingEmbedded())
                fail("expected SQLException on " +
                    "closed XAConnection.getXAResource");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
        try {
            xac2.getConnection();
            fail ("expected SQLException on XAConnection.getConnection");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
        try {
            xar2.start(xid, XAResource.TMJOIN);
            fail ("expected XAException on XAResource.TMJOIN");
        } catch (XAException xae) {
            assertXAException("XAResource.start", xae);
        }
        try {
            xar2.end(xid, XAResource.TMJOIN);
            fail ("expected XAException on XAResource.TMJOIN");
        } catch (XAException xae) {
            assertXAException("XAResource.end", xae);
        }
        try {
            xar2.commit(xid, true);
            fail ("expected XAException on XAResource.commit");
        } catch (XAException xae) {
            assertXAException("XAResource.commit", xae);
        }
        try {
            xar2.prepare(xid);
            fail ("expected XAException on XAResource.prepare");
        } catch (XAException xae) {
            assertXAException("XAResource.prepare", xae);
        }
        try {
            xar2.recover(0);
            fail ("expected XAException on XAResource.recover");
        } catch (XAException xae) {
            assertXAException("XAResource.recover", xae);
        }
        try {
            xar2.prepare(xid);
            fail ("expected XAException on XAResource.prepare");
        } catch (XAException xae) {
            assertXAException("XAResource.prepare", xae);
        }
        try {
            xar2.isSameRM(xar2);
            fail ("expected XAException on XAResource.isSameRM");
        } catch (XAException xae) {
            assertXAException("XAResource.isSameRM", xae);
        }
        
        // close everything
        cs1.rollback();
        sruState.close();
        psruState.close();
        csruState.close();
        psParams.close();
        sruBatch.close();
        sru1.close();
        sru2.close();
        sru3.close();
        cs1.close();
        cs2.close();
        xac.removeConnectionEventListener(null);
        xac.close();
        xac2.close();
        
        // but, still not enough.
        // what with all the switching between global and local transactions
        // we still have a lock open on intTable, which will interfere with
        // our tearDown efforts. Bounce the database.
        TestConfiguration.getCurrent().shutdownDatabase();
    }
    
    public void testSetSchemaInXAConnection() throws SQLException {
        // tests that set schema works correctly in an XA connection.

        XADataSource dsx = J2EEDataSource.getXADataSource();
        XAConnection xac3 = dsx.getXAConnection();
        Connection conn3 = xac3.getConnection();
        Statement st3 = conn3.createStatement();
        st3.execute("SET SCHEMA SCHEMA_Patricio");
        st3.close();

        PreparedStatement ps3 = 
            conn3.prepareStatement("INSERT INTO Patricio VALUES (?, ?)");
        ps3.setString(1, "Patricio");
        ps3.setInt(2, 3);
        ps3.executeUpdate();

        assertEquals(1, ps3.getUpdateCount());
        ps3.close();
        conn3.close();
        xac3.close();
    }
    
    
    // test that an xastart in auto commit mode commits the existing work.
    // test fix of a bug ('beetle 5178') wherein XAresource.start() when 
    // auto-commit is true did not implictly commit any transaction
    // Also tests DERBY-1025, same description, but for client.
    public void testAutoCommitOnXAResourceStart() throws SQLException, XAException {

        XADataSource dsx = J2EEDataSource.getXADataSource();
        XAConnection xac4 = dsx.getXAConnection();
        Xid xid4a= null;

        // We get an XAID_DUP error from networkserver when attempting
        // the XAResource.start below if we use the same xid.
        // Possibly because we're in the same jvm.
        // When the test is run with clientserverSuite, rather than default,
        // this wasn't needed, so just create a different id for client
        if (usingEmbedded())
            xid4a = new cdsXid(4, (byte) 23, (byte) 76);
        else if (usingDerbyNetClient())
            xid4a = new cdsXid(5, (byte) 23, (byte) 76);
            
        Connection conn4 = xac4.getConnection();
        assertTrue(conn4.getAutoCommit());

        Statement s4 = conn4.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet rs4 = s4.executeQuery("select i from autocommitxastart");
        rs4.next();
        assertEquals(1, rs4.getInt(1));
        rs4.next();
        assertEquals(2, rs4.getInt(1));

        // XAResource().start should commit the transaction
        xac4.getXAResource().start(xid4a, XAResource.TMNOFLAGS);
        xac4.getXAResource().end(xid4a, XAResource.TMSUCCESS);

        try {
            rs4.next();
            fail ("expected an exception indicating resultset is closed.");
        } catch (SQLException sqle) {
            // Embedded gets 08003. No current connection (DERBY-2620)        	
        	if (usingDerbyNetClient())
        		assertSQLState("XCL16",sqle);
        }

        conn4.setAutoCommit(false);
        assertFalse(conn4.getAutoCommit());

        rs4 = s4.executeQuery("select i from autocommitxastart");
        rs4.next();
        assertEquals(1, rs4.getInt(1));
        rs4.next();
        assertEquals(2, rs4.getInt(1));
        
         // Get a new xid to begin another transaction. 
        if (usingEmbedded())
            xid4a = new cdsXid(4, (byte) 93, (byte) 103);
        else if (usingDerbyNetClient())
            xid4a = new cdsXid(5, (byte) 93, (byte) 103);

        try {
            xac4.getXAResource().start(xid4a, XAResource.TMNOFLAGS);
        } catch (XAException xae) {
            if (usingEmbedded())
                assertNull(xae.getMessage());
            else if (usingDerbyNetClient())
            {
                // This should give XAER_OUTSIDE exception because
                // the resource manager is busy in the local transaction
                assertTrue(xae.getMessage().indexOf("XAER_OUTSIDE") >=0 );
            }
            assertEquals(-9, xae.errorCode);
        }
        
        rs4.next();
        assertEquals(3, rs4.getInt(1));
        rs4.close();

        conn4.rollback();
        conn4.close();
        xac4.close();
    }

    public void testReadOnlyToWritableTran() throws SQLException, Exception
    {
        // TESTING READ_ONLY TRANSACTION FOLLOWED BY WRITABLE TRANSACTION
        // Test following sequence of steps
        // 1)start a read-only global transaction 
        // 2)finish that read-only transaction
        // 3)start another global transaction 

        XADataSource dsx = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(dsx, "DatabaseName", dbName);
        XAConnection xac5 = dsx.getXAConnection();
        Xid xid5a = new cdsXid(5, (byte) 119, (byte) 129);
        Connection conn5 = xac5.getConnection();
        Statement sru5a = conn5.createStatement();
        XAResource xar = xac5.getXAResource();
        xar.start(xid5a, XAResource.TMNOFLAGS);
        conn5.setReadOnly(true);

        // Read-Only XA transaction;
        // holdability: (hold, or close cursors over commit) , 
        // transaction isolation: read-committed, 
        // auto-commit false, read-only true (with embedded)
        if (usingEmbedded()) 
        {
            assertConnectionState(
                ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                Connection.TRANSACTION_READ_COMMITTED,
                false, true, conn5);
        }
        // Note: the original test had no comments about this difference
        //       between Embedded and DerbyNetClient, this has apparently
        //       been accepted behavior.
        else if (usingDerbyNetClient())
        {
            assertConnectionState(
                ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                Connection.TRANSACTION_READ_COMMITTED,
                false, false, conn5);
        }
        
        ResultSet rs5 = sru5a.executeQuery(
            "select count(*) from autocommitxastart");
        rs5.next();
        assertEquals(5, rs5.getInt(1));
        rs5.close();
        xar.end(xid5a, XAResource.TMSUCCESS);
        xar.commit(xid5a, true);
        conn5.close();
        
        //now start a new transaction
        conn5 = xac5.getConnection();
        sru5a = conn5.createStatement();
        xar.start(xid5a, XAResource.TMNOFLAGS);
        
        // Writeable XA transaction
        // holdability: (hold, or close cursors over commit) , 
        // transaction isolation: read-committed, 
        // auto-commit false, read-only false
        assertConnectionState(
                ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                Connection.TRANSACTION_READ_COMMITTED,
                false, false, conn5);
        sru5a.executeUpdate("insert into autocommitxastart values 6,7");
        rs5 = sru5a.executeQuery("select count(*) from autocommitxastart");
        rs5.next();
        assertEquals(7, rs5.getInt(1));
        xar.end(xid5a, XAResource.TMSUCCESS);
        xar.commit(xid5a, true);
        conn5.close();
        xac5.close();
        sru5a.close();
    }
    
    // test jira-derby 95 - a NullPointerException was returned when passing
    // an incorrect database name, should now give error:
    // XCY00 - invalid valid for property ...  
    // with DataSource
    public void testJira95ds() throws SQLException {
        if (usingEmbedded())
        {
            try {
                DataSource ds = JDBCDataSource.getDataSource();
                // non-existent database
                JDBCDataSource.setBeanProperty(ds, "databaseName", "jdbc:derby:wombat");
                ds.getConnection();
                fail ("expected an SQLException!");
            } catch (SQLException sqle) {
                // DERBY-2498: with client, getting a NullPointerException.
                // Note also: the NPE does not occur with XADataSource - see
                // testJira95xads().
                if (usingEmbedded())
                    assertSQLState("XCY00", sqle);
            } catch (Exception e) {
                e.printStackTrace();
                // DERBY-2498, when fixed, remove 'if'
                if (usingEmbedded())
                    fail ("unexpected exception: " + e.toString());
            }
        } 
    }

    // test jira-derby 95 - a NullPointerException was returned when passing
    // an incorrect database name, should now give error XCY00   
    // with ConnectionPoolDataSource
    public void testJira95pds() throws SQLException {
        try {
            ConnectionPoolDataSource pds = J2EEDataSource.getConnectionPoolDataSource();
            JDBCDataSource.setBeanProperty(pds, "databaseName", "jdbc:derby:boo");
            pds.getPooledConnection();
            fail ("expected an SQLException!");
        } catch (SQLException sqle) {
            // DERBY-2498 - when fixed, remove if
            if (usingEmbedded())
                assertSQLState("XCY00", sqle);
        } catch (Exception e) {
            // DERBY-2498 - when fixed, remove if
            if (usingEmbedded())
                fail ("unexpected exception: " + e.toString());
        }
    }
    
    // test jira-derby 95 - a NullPointerException was returned when passing
    // an incorrect database name, should now give error XCY00   
    // with XADataSource
    public void testJira95xads() throws SQLException {
        try {
            XADataSource dxs = J2EEDataSource.getXADataSource();
            JDBCDataSource.setBeanProperty(dxs, "databaseName", "jdbc:derby:boo");
            dxs.getXAConnection().getConnection();
            fail ("expected an SQLException!");
        } catch (SQLException sqle) {
            assertSQLState("XCY00", sqle);
        } catch (Exception e) {
            fail ("unexpected exception: " + e.toString());
        }
    }
    
    public void testBadConnectionAttributeSyntax() throws SQLException {
        
        // DataSource - bad connattr syntax
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(ds, "ConnectionAttributes", "bad");
        try {
            ds.getConnection();
            fail ("should have seen an error");
        } catch (SQLException e) {
            if (usingEmbedded())
                assertSQLState("XJ028", e);
            else if (usingDerbyNetClient())
                assertSQLState("XJ212", e);
        } 
        JDBCDataSource.clearStringBeanProperty(ds, "ConnectionAttributes");

        // ConnectionPoolDataSource - bad connatr syntax
        ConnectionPoolDataSource cpds = J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(cpds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(cpds, "ConnectionAttributes", "bad");
        try {
            cpds.getPooledConnection();
            fail ("should have seen an error");
        } catch (SQLException e) {
            assertSQLState("XJ028", e);
        } 
        JDBCDataSource.clearStringBeanProperty(cpds, "ConnectionAttributes");

        // XADataSource - bad connattr syntax");
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "ConnectionAttributes", "bad");
        try {
            xads.getXAConnection();
            fail ("should have seen an error");
        } catch (SQLException e) {
            assertSQLState("XJ028", e);
        } 
        JDBCDataSource.clearStringBeanProperty(xads, "ConnectionAttributes");
    } // End testBadConnectionAttributeSyntax
        
    /**
     * Check that database name set using setConnectionAttributes is not used
     * by ClientDataSource. This method tests DERBY-1130.
     * 
     * @throws SQLException
     */
    public void testClientDSConnectionAttributes() throws SQLException {
        if (usingEmbedded())
            return;
        
        ClientDataSource ds = new ClientDataSource();

        // DataSource - EMPTY; expect error 08001 in all cases
        // 08001: Required Derby DataSource property databaseName not set.
        dsConnectionRequests(new String[]  
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"}, ds);

        // DataSource - connectionAttributes=databaseName=<valid name>
        ds.setConnectionAttributes("databaseName=" + dbName);
        dsConnectionRequests(new String[]  
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"}, ds);
        ds.setConnectionAttributes(null);

        // Test that (invalid) database name specified in connection
        // attributes is not used
        // DataSource - databaseName=<valid db> and 
        // connectionAttributes=databaseName=kangaroo
        ds.setConnectionAttributes("databaseName=kangaroo");
        ds.setDatabaseName(dbName);
        dsConnectionRequests(new String[]  
            {"OK","08001","OK","OK",
             "08001","08001","OK","OK","OK"}, ds);
        ds.setConnectionAttributes(null);
        ds.setDatabaseName(null);

        // now with ConnectionPoolDataSource
        ClientConnectionPoolDataSource cpds = 
            new ClientConnectionPoolDataSource();
        // ConnectionPoolDataSource - EMPTY
        dsConnectionRequests(new String[]  
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"}, 
            (ConnectionPoolDataSource)cpds);

        // ConnectionPoolDataSource 
        // - connectionAttributes=databaseName=<valid dbname>
        cpds.setConnectionAttributes("databaseName=" + dbName);
        dsConnectionRequests(new String[]  
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"},
            (ConnectionPoolDataSource)cpds);
        cpds.setConnectionAttributes(null);

        // Test that database name specified in connection attributes is 
        // not used
        // ConnectionPoolDataSource - databaseName=wombat and 
        // connectionAttributes=databaseName=kangaroo
        cpds.setConnectionAttributes("databaseName=kangaroo");
        cpds.setDatabaseName(dbName);
        dsConnectionRequests(new String[]  
            {"OK","08001","OK","OK","08001","08001","OK","OK","OK"},
            (ConnectionPoolDataSource)cpds);
        cpds.setConnectionAttributes(null);
        cpds.setDatabaseName(null);

        // now with XADataSource
        ClientXADataSource xads = new ClientXADataSource();
        // XADataSource - EMPTY
        dsConnectionRequests(new String[]  
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"}, 
            (XADataSource) xads);

        // XADataSource - connectionAttributes=databaseName=<valid dbname>
        xads.setConnectionAttributes("databaseName=wombat");
        dsConnectionRequests(new String[]  
            {"08001","08001","08001","08001",
             "08001","08001","08001","08001","08001"},
            (XADataSource) xads);
        xads.setConnectionAttributes(null);

        // Test that database name specified in connection attributes is not used
        // XADataSource - databaseName=wombat and 
        // connectionAttributes=databaseName=kangaroo
        xads.setConnectionAttributes("databaseName=kangaroo");
        xads.setDatabaseName("wombat");
        dsConnectionRequests(new String[]  
            {"OK","08001","OK","OK","08001","08001","OK","OK","OK"},
            (XADataSource) xads);
        xads.setConnectionAttributes(null);
        xads.setDatabaseName(null);
    } // End testClientDSConnectionAttributes
            
    // Following test is similar to testClientDSRequestAuthentication, but
    // for embedded datasources.
    // This subtest does not run for network server, the database shutdown
    // is done using setDatabaseShutdown.
    public static void testDSRequestAuthentication() throws SQLException {

        if (usingDerbyNetClient())
            return;
        
        EmbeddedDataSource ds = new EmbeddedDataSource();

        // DataSource - EMPTY
        dsConnectionRequests(new String[] {  
             "XJ004","XJ004","XJ004","XJ004",
             "XJ004","XJ004","XJ004","XJ004","XJ004"}, ds);
 
        // DataSource - connectionAttributes=databaseName=wombat");
        ds.setConnectionAttributes("databaseName=" + dbName);
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ004",
            "XJ004","XJ004","XJ004","XJ004","XJ004"}, ds);
        ds.setConnectionAttributes(null);

        // DataSource - attributesAsPassword=true");
        ds.setAttributesAsPassword(true);
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"}, ds);
        ds.setAttributesAsPassword(false);

        // DataSource - attributesAsPassword=true, 
        // connectionAttributes=databaseName=kangaroo");
        ds.setAttributesAsPassword(true);
        ds.setConnectionAttributes("databaseName=kangaroo");
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"}, ds);
        ds.setAttributesAsPassword(false);
        ds.setConnectionAttributes(null);

        // Enable Authentication;

        setDatabaseProperty("derby.user.fred", "wilma");
        setDatabaseProperty("derby.user.APP", "APP");
        setDatabaseProperty("derby.authentication.provider", "BUILTIN");
        setDatabaseProperty("derby.connection.requireAuthentication", "true");
        
        ds.setShutdownDatabase("shutdown");
        try {
            ds.getConnection();
        } catch (SQLException sqle) {
            assertSQLState("XJ015", sqle);
        }

        ds.setDatabaseName(null);
        ds.setShutdownDatabase(null);

        // "AUTHENTICATION NOW ENABLED");

        // DataSource - attributesAsPassword=true
        ds.setAttributesAsPassword(true);
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"}, ds);
        ds.setAttributesAsPassword(false);

        // ensure the DS property password is not treated as a set of 
        // attributes.
        // DataSource - attributesAsPassword=true, user=fred, 
        //     password=databaseName=wombat;password=wilma
        ds.setAttributesAsPassword(true);
        ds.setUser("fred");
        ds.setPassword("databaseName=" + dbName + ";password=wilma");
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"}, ds);
        ds.setAttributesAsPassword(false);
        ds.setUser(null);
        ds.setPassword(null);
        ds = null;

        // now with ConnectionPoolDataSource
        EmbeddedConnectionPoolDataSource cpds = new EmbeddedConnectionPoolDataSource();
        // ConnectionPoolDataSource - EMPTY
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ004",
            "XJ004","XJ004","XJ004","XJ004","XJ004"},
            (ConnectionPoolDataSource)cpds);

        // ConnectionPoolDataSource - 
        // connectionAttributes=databaseName=wombat
        cpds.setConnectionAttributes("databaseName=" + dbName);
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ004",
            "XJ004","XJ004","XJ004","XJ004","XJ004"},
            (ConnectionPoolDataSource)cpds);
        cpds.setConnectionAttributes(null);

        // ConnectionPoolDataSource - attributesAsPassword=true
        cpds.setAttributesAsPassword(true);
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"},
            (ConnectionPoolDataSource)cpds);
        cpds.setAttributesAsPassword(false);
        
        // ensure the DS property password is not treated as a set of
        // attributes.
        // ConnectionPoolDataSource - attributesAsPassword=true, 
        //     user=fred, password=databaseName=wombat;password=wilma");
        cpds.setAttributesAsPassword(true);
        cpds.setUser("fred");
        cpds.setPassword("databaseName=" + dbName + ";password=wilma");
        dsConnectionRequests(new String[] {  
            "XJ004","XJ004","XJ004","XJ028",
            "XJ028","XJ004","XJ004","XJ004","XJ004"},
            (ConnectionPoolDataSource)cpds);
        cpds.setAttributesAsPassword(false);
        cpds.setUser(null);
        cpds.setPassword(null);
        cpds = null;

        // now with XADataSource
        EmbeddedXADataSource xads = new EmbeddedXADataSource();
        // XADataSource - EMPTY
        dsConnectionRequests(new String[] {  
            "08006","08006","08006","08006",
            "08006","08006","08006","08006","08006"},
            (XADataSource) xads);

        // XADataSource - databaseName=wombat
        xads.setDatabaseName(dbName);
        dsConnectionRequests(new String[] {  
            "08004","08004","08004","OK",
            "08004","08004","08004","08004","08004"},
            (XADataSource) xads);
        xads.setDatabaseName(null);

        // XADataSource - connectionAttributes=databaseName=wombat");
        xads.setConnectionAttributes("databaseName=" + dbName);
        dsConnectionRequests(new String[] {  
            "08006","08006","08006","08006",
            "08006","08006","08006","08006","08006"},
            (XADataSource) xads);
        xads.setConnectionAttributes(null);

        // XADataSource - attributesAsPassword=true
        xads.setAttributesAsPassword(true);
        dsConnectionRequests(new String[] {  
            "08006","08006","08006","08006",
            "08006","08006","08006","08006","08006"},
            (XADataSource) xads);
        xads.setAttributesAsPassword(false);

        // XADataSource - databaseName=wombat, attributesAsPassword=true
        xads.setDatabaseName(dbName);
        xads.setAttributesAsPassword(true);
        dsConnectionRequests(new String[] {  
            "08004","08004","08004","XJ028",
            "XJ028","08004","08004","OK","08004"},
            (XADataSource) xads);
        xads.setAttributesAsPassword(false);
        xads.setDatabaseName(null);
        
        setDatabaseProperty("derby.connection.requireAuthentication", "false");
        TestConfiguration.getCurrent().shutdownDatabase();
    }
    
    /**
     * Check that traceFile connection attribute functions correctly.
     * tracefile was tested in checkDriver, but not for DataSources.
     * tracefile= was used in datasourcepermissions_net, but that's 
     * incorrect syntax. Note that we're not checking the contents of
     * the tracefile.
     *
     * Note also that this test cannot run against a remote server.
     *  
     * @throws SQLException
     */
    public void testClientTraceFileDSConnectionAttribute() throws SQLException
    {
        if (usingEmbedded())
            return;

        String traceFile;

        // DataSource
        ClientDataSource ds = new ClientDataSource();
        ds.setDatabaseName(dbName);

        // DataSource - setTransationAttributes
        traceFile = "trace1.out";
        ds.setConnectionAttributes("traceFile="+traceFile);
        // In this scenario, we *only* get a tracefile, if we first get a 
        // successful connection, followed by an unsuccessful connection. 
        // So, we cannot just use ds.getConnection()
        dsGetBadConnection(ds);
        ds.setConnectionAttributes(null);
        // DataSource - setTraceFile
        traceFile = "trace2.out";
        ds.setTraceFile(traceFile);
        ds.getConnection();
        ds.setTraceFile(null);
        ds.setDatabaseName(null);

        // now with ConnectionPoolDataSource
        ClientConnectionPoolDataSource cpds = new ClientConnectionPoolDataSource();
        cpds.setDatabaseName(dbName);

        traceFile = "trace3.out";
        cpds.setConnectionAttributes("traceFile="+traceFile);
        // DERBY-2468 - trace3.out does not get created
        cpds.getConnection();
        cpds.setConnectionAttributes(null);

        traceFile = "trace4.out";
        cpds.setTraceFile(traceFile);
        cpds.getConnection();
        cpds.setTraceFile(null);
        cpds.setDatabaseName(null);

        // now with XADataSource
        ClientXADataSource xads = new ClientXADataSource();
        xads.setDatabaseName(dbName);

        traceFile = "trace5.out";
        xads.setConnectionAttributes("traceFile="+traceFile);
        xads.getConnection();
        // DERBY-2468 - trace5.out does not get created
        xads.setConnectionAttributes(null);

        traceFile = "trace6.out";
        xads.setTraceFile(traceFile);
        xads.getConnection();
        xads.setTraceFile(null);
        xads.setDatabaseName(null);

        assertTraceFilesExist();
    }
        
    /* -- Helper Methods for testClientTraceFileDSConnectionAttribute -- */
    
    private static void dsGetBadConnection(DataSource ds) {
        // first get a good connection, or we don't get a
        // traceFile when using connectionattributes.
        // also, we do not get a tracefile that way unless
        // we see an error.
        // with setTraceFile, we *always* get a file, even
        // with just a successful connection.
        try {
            ds.getConnection();
            ds.getConnection(null, null);
            fail("expected an sqlException");
        } catch (SQLException sqle) {
            assertSQLState("08001", sqle);
        }
    }
    
    /**
     * Check that trace file exists in <framework> directory
     */
    private static void assertTraceFilesExist() 
    {
        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                for (int i=0 ; i < 6 ; i++)
                {   
                    String traceFileName = "trace" + (i+1) + ".out";
                    File traceFile = new File(traceFileName);
                    if (i == 2 || i == 4)
                        continue;
                    else
                    {
                        assertTrue(traceFile.exists());
                    }
                } 
                return null;
            }
        });
    }

    /**
     * Check that messageText connection attribute functions correctly.
     * retrievemessagetext was tested in checkdriver, and derbynet/testij,
     * but not tested for datasources, and in datasourcepermissions_net,
     * but as it has nothing to do with permissions/authentication,
     * this test seems a better place for it. 
     *  
     * @throws SQLException
     */
    public void testClientMessageTextConnectionAttribute() throws SQLException
    {
        if (usingEmbedded())
            return;
        
        String retrieveMessageTextProperty = "retrieveMessageText";
        Connection conn;

        // DataSource
        // DataSource - retrieveMessageTextProperty
        ClientDataSource ds = new ClientDataSource();
        ds.setDatabaseName(dbName);
        ds.setConnectionAttributes(retrieveMessageTextProperty + "=false");
        conn = ds.getConnection();
        assertMessageText(conn,"false");
        conn.close();
        // now try with retrieveMessageText = true
        ds.setConnectionAttributes(retrieveMessageTextProperty + "=true");
        conn = ds.getConnection();
        assertMessageText(conn,"true");
        ds.setConnectionAttributes(null);
        conn.close();

        // now with ConnectionPoolDataSource
        // ConnectionPoolDataSource - retrieveMessageTextProperty
        ClientConnectionPoolDataSource cpds = new ClientConnectionPoolDataSource();
        cpds.setDatabaseName(dbName);
        cpds.setConnectionAttributes(
                retrieveMessageTextProperty + "=false");
        conn = cpds.getConnection();
        assertMessageText(conn,"false");
        conn.close();
        cpds.setConnectionAttributes(
                retrieveMessageTextProperty + "=true");
        conn = cpds.getConnection();
        assertMessageText(conn,"true");
        cpds.setConnectionAttributes(null);
        conn.close();

        // now with XADataSource
        ClientXADataSource xads = new ClientXADataSource();
        //XADataSource - retrieveMessageTextProperty
        xads.setDatabaseName(dbName);
        xads.setConnectionAttributes(
                retrieveMessageTextProperty + "=false");
        conn = xads.getConnection();
        assertMessageText(conn,"false");
        conn.close();
        xads.setConnectionAttributes(
                retrieveMessageTextProperty + "=true");
        conn = xads.getConnection();
        assertMessageText(conn,"true");
        conn.close();
        xads.setConnectionAttributes(null);
    }

    /* -- Helper Method for testClientMessageTextDSConnectionAttribute -- */

    private static void assertMessageText(
            Connection conn, String retrieveMessageTextValue) 
    throws SQLException
    {
        try {
            conn.createStatement().executeQuery("SELECT * FROM APP.NOTTHERE");
        }
        catch (SQLException e)
        {
            assertSQLState("42X05", e);
            if (retrieveMessageTextValue.equals("true") )
            {
                assertTrue(e.getMessage().indexOf("does not exist") >= 0);
            }
            else
            {
                // retrieveMessageTextValue is false
                assertTrue(e.getMessage().indexOf("does not exist") == -1);
            }
        }
    }

    /**
     * Check that messageText connection attribute functions correctly.
     * retrievemessagetext was tested in checkdriver, and derbynet/testij
     * (but not tested for datasources), and in datasourcepermissions_net,
     * but as it has nothing to do with permissions/authentication,
     * this test seems a better place for it. 
     *  
     * @throws SQLException
     */
    public void testClientDescriptionConnectionAttribute() 
    throws SQLException, Exception {

        if (usingEmbedded())
            return;
        
        // DataSource
        String setDescription = 
            "Everything you ever wanted to know about this datasource";
        String getDescription;

        // DataSource - setDescription
        ClientDataSource ds = new ClientDataSource();
        ds.setDatabaseName(dbName);
        ds.setDescription(setDescription);
        ds.getConnection();
        getDescription = ds.getDescription();
        assertEquals(setDescription, getDescription);
        ds.setDescription(null);

        // ConnectionPoolDataSource - setDescription
        ClientConnectionPoolDataSource cpds = 
            new ClientConnectionPoolDataSource();
        cpds.setDatabaseName(dbName);
        cpds.setDescription(setDescription);
        cpds.getConnection();
        getDescription = cpds.getDescription();
        assertEquals(setDescription, getDescription);
        cpds.setDescription(null);

        // XADataSource - setDescription
        ClientXADataSource xads = new ClientXADataSource();
        xads.setDatabaseName(dbName);
        xads.setDescription(setDescription);
        xads.getConnection();
        getDescription = xads.getDescription();
        assertEquals(setDescription, getDescription);
        xads.setDescription(null);
    }

    /* ------------------ JDBC30 (and up) Fixtures ------------------ */
    
    public void testXAHoldability() throws SQLException, XAException {
        // DERBY-2533 - 
        // This test, when run with Network server / DerbyNetClient
        // leaves the database is a bad state which results in a
        // network protocol error
        if (usingDerbyNetClient())
            return;
        // START XA HOLDABILITY TEST
        XADataSource dscsx = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(dscsx, "databaseName", dbName);

        XAConnection xac = dscsx.getXAConnection();
        XAResource xr = xac.getXAResource();
        Xid xid = new cdsXid(25, (byte) 21, (byte) 01);
        Connection conn1 = xac.getConnection();
        // check that autocommit is true; default for a connection
        assertTrue(conn1.getAutoCommit());
        // check that holdability is HOLD_CURSORS_OVER_COMMIT in a default
        // CONNECTION(not in xa transaction yet)
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, conn1.getHoldability());
        // start a global transaction and default holdability and 
        // autocommit will be switched to match Derby XA restrictions
        xr.start(xid, XAResource.TMNOFLAGS);
        // So, now autocommit should be false for connection because it is
        // part of the global transaction
        assertFalse(conn1.getAutoCommit());
        // Connection's holdability is now CLOSE_CURSORS_AT_COMMIT because
        // it is part of the global transaction
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, conn1.getHoldability());
        
        xr.end(xid, XAResource.TMSUCCESS);
        conn1.commit();
        conn1.close();

        xid = new cdsXid(27, (byte) 21, (byte) 01);
        xr.start(xid, XAResource.TMNOFLAGS);
        conn1 = xac.getConnection();
        // CONNECTION(in xa transaction) HOLDABILITY:
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, conn1.getHoldability());
        // Autocommit on Connection inside global transaction should be false
        assertFalse(conn1.getAutoCommit());
        xr.end(xid, XAResource.TMSUCCESS);
        conn1.rollback();

        Connection conn = xac.getConnection();
        conn.setAutoCommit(false);
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        // CONNECTION(non-xa transaction) HOLDABILITY: 
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

        Statement s = conn.createStatement();
        // STATEMENT HOLDABILITY: 
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, s.getResultSetHoldability());

        s.executeUpdate("insert into hold_30 values " +
            "(1,'init2'), (2, 'init3'), (3,'init3')");
        s.executeUpdate("insert into hold_30 values " +
            "(4,'init4'), (5, 'init5'), (6,'init6')");
        s.executeUpdate("insert into hold_30 values " +
            "(7,'init7'), (8, 'init8'), (9,'init9')");

        // STATEMENT HOLDABILITY :
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, s.getResultSetHoldability());

        Statement sh = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        PreparedStatement psh = conn.prepareStatement(
            "select id from hold_30 for update", ResultSet.TYPE_FORWARD_ONLY, 
            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        CallableStatement csh = conn.prepareCall(
            "select id from hold_30 for update", ResultSet.TYPE_FORWARD_ONLY, 
            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);

        // STATEMENT HOLDABILITY :
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, sh.getResultSetHoldability());
        // PREPARED STATEMENT HOLDABILITY :
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, psh.getResultSetHoldability());
        // CALLABLE STATEMENT HOLDABILITY :
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, csh.getResultSetHoldability());

        ResultSet rsh = sh.executeQuery("select id from hold_30 for update");
        rsh.next();  
        assertEquals(1, rsh.getInt(1)); // H@1 id
        rsh.next(); 
        assertEquals(2, rsh.getInt(1)); // H@2 id 
        conn.commit();
        rsh.next(); 
        assertEquals(3, rsh.getInt(1)); // H@3 id 
        conn.commit();

        xid = new cdsXid(23, (byte) 21, (byte) 01);
        xr.start(xid, XAResource.TMNOFLAGS);
        Statement stmtInsideGlobalTransaction = conn.createStatement();
        PreparedStatement prepstmtInsideGlobalTransaction = 
            conn.prepareStatement("select id from hold_30");
        CallableStatement callablestmtInsideGlobalTransaction = 
            conn.prepareCall("select id from hold_30");

        // CONNECTION(xa) HOLDABILITY:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
        // STATEMENT(this one was created with holdability false, outside the 
        // global transaction. Check its holdability inside global transaction
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, s.getResultSetHoldability());
        // STATEMENT(this one was created with holdability true, 
        // outside the global transaction. Check its holdability inside 
        // global transaction:
        // DERBY-2531: network server / DerbyNetClient has a different value 
        // than embedded.
        if (usingEmbedded())
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                sh.getResultSetHoldability());
        else if (usingDerbyNetClient())
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
                sh.getResultSetHoldability());
        // STATEMENT(this one was created with default holdability inside this
        // global transaction. Check its holdability:
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            stmtInsideGlobalTransaction.getResultSetHoldability());
        // PREPAREDSTATEMENT(this one was created with default holdability
        // inside this global transaction. Check its holdability:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
        prepstmtInsideGlobalTransaction.getResultSetHoldability());
        // CALLABLESTATEMENT(this one was created with default holdability 
        // inside this global transaction. Check its holdability:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
        callablestmtInsideGlobalTransaction.getResultSetHoldability()); 

        ResultSet rsx = s.executeQuery("select id from hold_30 for update");

        rsx.next(); 
        assertEquals(1, rsx.getInt(1)); // X@1 id
        rsx.next(); 
        assertEquals(2, rsx.getInt(1)); // X@2 id
        xr.end(xid, XAResource.TMSUCCESS);

        // result set should not be useable, since it is part of a detached
        // XAConnection
        try {
            rsx.next();
            fail("rsx's connection not active id ");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }

        // result set should not be useable, it should have been closed by
        // the xa start.
        try {
            rsh.next();
            fail("rsh's connection not active id ");
        } catch (SQLException sqle) {
            if (usingEmbedded())
                assertSQLState("08003", sqle);
            else if (usingDerbyNetClient())
                assertSQLState("XCL16", sqle);
        }

        // resume XA transaction and keep using rs");
        xr.start(xid, XAResource.TMJOIN);
        Statement stmtAfterGlobalTransactionResume = conn.createStatement();
        PreparedStatement prepstmtAfterGlobalTransactionResume = 
            conn.prepareStatement("select id from hold_30");
        CallableStatement callablestmtAfterGlobalTransactionResume = 
            conn.prepareCall("select id from hold_30");

        // Check holdability of various jdbc objects after resuming XA 
        // transaction
        // CONNECTION(xa) HOLDABILITY:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,conn.getHoldability());
        // STATEMENT(this one was created with holdability false, outside the
        // global transaction. Check its holdability inside global transaction
        assertEquals(
            ResultSet.CLOSE_CURSORS_AT_COMMIT, s.getResultSetHoldability());
        // STATEMENT(this one was created with holdability true, outside the 
        // global transaction. Check its holdability inside global transaction
        if (usingEmbedded())
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                sh.getResultSetHoldability());
        else if (usingDerbyNetClient())
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
                sh.getResultSetHoldability());
        // STATEMENT(this one was created with default holdability inside the
        // global transaction when it was first started. Check its holdability
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            stmtInsideGlobalTransaction.getResultSetHoldability());
        // PREPAREDSTATEMENT(this one was created with default holdability 
        // inside the global transaction when it was first started. Check its
        // holdability) 
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            prepstmtInsideGlobalTransaction.getResultSetHoldability());
        // CALLABLESTATEMENT(this one was created with default holdability 
        // inside the global transaction when it was first started. Check its
        // holdability) HOLDABILITY
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            callablestmtInsideGlobalTransaction.getResultSetHoldability());
        // STATEMENT(this one was created with default holdability after the
        // global transaction was resumed. Check its holdability
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            stmtAfterGlobalTransactionResume.getResultSetHoldability());
        // PREPAREDSTATEMENT(this one was created with default holdability 
        // after the global transaction was resumed. Check its holdability
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            prepstmtAfterGlobalTransactionResume.getResultSetHoldability());
        // CALLABLESTATEMENT(this one was created with default holdability
        // after the global transaction was resumed. Check its holdability
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            callablestmtAfterGlobalTransactionResume.getResultSetHoldability());
        // DERBY-1370           
        if (usingEmbedded())
        {
            // Network XA BUG gives result set closed
            rsx.next();  
            assertEquals(3, rsx.getInt(1)); // X@3 id
        }
        xr.end(xid, XAResource.TMSUCCESS);

        if (xr.prepare(xid) != XAResource.XA_RDONLY)
            xr.commit(xid, false);

        // try again once the xa transaction has been committed.            
        try {
            rsx.next(); 
            fail("rsx's connection not active id (B)");
        } catch (SQLException sqle) {
            assertSQLState("XCL16", sqle);
        }
        try {
            rsh.next(); 
            fail ("rsh's should be closed (B)");
        } catch (SQLException sqle) {
            assertSQLState("XCL16", sqle);
        }

        // Set connection to hold
        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // CONNECTION(held) HOLDABILITY:
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, 
            conn.getHoldability());

        xid = new cdsXid(24, (byte) 21, (byte) 01);
        xr.start(xid, XAResource.TMNOFLAGS);
        // CONNECTION(xa) HOLDABILITY:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
        try {
            conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
            fail("allowed to set hold mode in xa transaction");
        } catch (SQLException sqle) {
            assertSQLState("XJ05C", sqle);
        }

        // JDBC 4.0 (proposed final draft) section 16.1.3.1 allows Statements 
        // to be created with a different holdability if the driver cannot 
        // support it. In this case the driver does not support holdability in
        // a global transaction, so a valid statement is returned with close 
        // cursors on commit.
        Statement shxa = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // HOLDABLE Statement in global xact " 
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
            s.getResultSetHoldability());
        assertEquals(10000, conn.getWarnings().getErrorCode());
        shxa.close();

        shxa = conn.prepareStatement("select id from hold_30",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // HOLDABLE PreparedStatement in global xact 
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            s.getResultSetHoldability());
        assertEquals(10000, conn.getWarnings().getErrorCode());
        shxa.close();

        shxa = conn.prepareCall("CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // HOLDABLE CallableStatement in global xact:
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            s.getResultSetHoldability());
        assertEquals(10000, conn.getWarnings().getErrorCode());
        shxa.close();

        // check we can use a holdable statement set up in local mode.
        // holdability is downgraded, tested in XATest.java
        // DERBY-1370           
        if(usingEmbedded()) {
            // STATEMENT HOLDABILITY:
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
                sh.getResultSetHoldability());
            sh.executeQuery("select id from hold_30").close();
            sh.execute("select id from hold_30");
            sh.getResultSet().close();

            // PREPARED STATEMENT HOLDABILITY:
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, 
                psh.getResultSetHoldability());
            psh.executeQuery().close();
            psh.execute();
            psh.getResultSet().close();

            // CALLABLE STATEMENT HOLDABILITY:
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
                csh.getResultSetHoldability());
            csh.executeQuery().close();
            csh.execute();
            csh.getResultSet().close();
        }        

        // but an update works
        sh.executeUpdate("insert into hold_30 values(10, 'init10')");

        xr.end(xid, XAResource.TMSUCCESS);

        // CONNECTION(held) HOLDABILITY:
        assertEquals(
            ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());

        s.close();
        sh.close();
        csh.close();
        psh.close();
        rsx.close();
        stmtInsideGlobalTransaction.close();
        prepstmtInsideGlobalTransaction.close();
        callablestmtInsideGlobalTransaction.close();
        stmtAfterGlobalTransactionResume.close();
        prepstmtAfterGlobalTransactionResume.close();
        callablestmtAfterGlobalTransactionResume.close();
        conn.close();
        xac.close();
        TestConfiguration.getCurrent().shutdownDatabase();
        // END XA HOLDABILITY TEST");
    }
    
    /**
     * Tests for DERBY-1144
     * 
     * This test tests that holdability, autocomit, and transactionIsolation 
     * are reset on getConnection for PooledConnections obtaind from 
     * connectionPoolDataSources 
     * 
     * DERBY-1134 has been filed for more comprehensive testing of client 
     * connection state. 
     * 
     * @throws SQLException
     */
    public void testDerby1144PooledDS() throws SQLException {
    
        PooledConnection pc1 = null;

        // Test holdability   
        ConnectionPoolDataSource ds = 
            J2EEDataSource.getConnectionPoolDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        pc1 = ds.getPooledConnection();
        assertPooledConnHoldability("PooledConnection", pc1);
        pc1.close();
        
        // Test autocommit
        pc1 = ds.getPooledConnection();
        assertPooledConnAutoCommit("PooledConnection", pc1);
        pc1.close();
        
        // Test pooled connection isolation
        pc1 = ds.getPooledConnection();
        assertPooledConnIso("PooledConnection" , pc1);   
        pc1.close();
    }
    
    public void testDerby1144XADS() throws SQLException {
       
        XADataSource xds = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xds, "databaseName", dbName);
        // Test xa connection isolation
        XAConnection xpc1 = xds.getXAConnection();        
        assertPooledConnIso("XAConnection", xpc1);                 
        xpc1.close();
    }

    /* -------------- Helper Methods for testDerby1144 -------------- */
    
    /**
     * Make sure autocommit gets reset on PooledConnection.getConnection()
     * @param desc      description of connection
     * @param pc1       pooled connection to test
     * @throws SQLException
     */
    private static void assertPooledConnAutoCommit(
        String desc, PooledConnection pc1) throws SQLException 
    {
        // ** Verify autoCommit state
        Connection conn  = pc1.getConnection();
        conn.setAutoCommit(true);
        // reset the connection and see if the autocommit has changed 
        conn = pc1.getConnection();
        boolean autocommit  = conn.getAutoCommit();
        // autocommit should get reset on getConnection
        assertTrue(autocommit);
        conn.close();
    }


    /**
     * Checks that Holdability gets reset on PooledConnection.getConnection()
     * @param desc
     * @param pc1
     * @throws SQLException
     */
    private static void assertPooledConnHoldability(
        String desc, PooledConnection pc1) throws SQLException 
    { 
        // **Test holdability state
        Connection conn  = pc1.getConnection();
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        // reset the connection and see if the holdability gets reset
        // to HOLD_CURSORS_OVER_COMMIT
        conn = pc1.getConnection();
        assertConnHoldability(conn, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        conn.close();
    }


    /**
     * Verify connection holdablity is expected holdability
     * @param conn
     * @param expectedHoldability 
     *   * @throws SQLException
     */
    private static void assertConnHoldability(
        Connection conn, int expectedHoldability) throws SQLException 
    {
        int holdability = conn.getHoldability();
        assertEquals (expectedHoldability, holdability);
    }

    /**
     * Test that isolation is reset on PooledConnection.getConnection()
     * @param pooledConnType   Descripiton of the type of pooled connection
     * @param pc               PooledConnection or XAConnection  
     * @throws SQLException
     */
    private void assertPooledConnIso(
        String pooledConnType, PooledConnection pc) throws SQLException {
        Connection conn = pc.getConnection();

        setupDerby1144Table(conn);

        // *** Test isolation level reset on conntype.getConnection()          
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        assertIsoLocks(conn, Connection.TRANSACTION_READ_UNCOMMITTED);

        conn.close();
        //Get a new connection with pooledConnType.getConnection()
        // Isolation level should be reset to READ_COMMITTED
        Connection newconn = pc.getConnection();
        assertIsoLocks(newconn, Connection.TRANSACTION_READ_COMMITTED);
    }

    /*
     * insert two rows into the simple table for DERBY-1144 tests
     * @param conn
     * @throws SQLException
     */
    private static void  setupDerby1144Table(Connection conn) 
    throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("INSERT INTO intTable VALUES(1)");
        stmt.executeUpdate("INSERT INTO intTable VALUES(2)");

        conn.commit ();
    }
    
    /* 
     * Checks locks for designated isolation level on the connection.
     * Currently only supports TRANSACTION_READ_COMMITTED and 
     * TRANSACTION_READ_UNCOMMITTED
     * @param conn   Connection to test
     * @param isoLevel expected isolation level
     *
     */
    private void assertIsoLocks(Connection conn, int expectedIsoLevel)
    throws SQLException {
        int conniso = conn.getTransactionIsolation();
        assertEquals(expectedIsoLevel, conniso);

        boolean selectTimedOut = selectTimesoutDuringUpdate(conn);
        // expect a lock timeout for READ_COMMITTED
        switch (conniso) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                assertFalse(selectTimedOut); break;
            case Connection.TRANSACTION_READ_COMMITTED:
                assertTrue(selectTimedOut); break;
            default:
                System.out.println("No test support for isolation level");
        }
    }
    
    /*
     * Determine if a select on this connection during update will timeout.
     * Used to establish isolation level.  If the connection isolation level
     * is <code> Connection.TRANSACTION_READ_UNCOMMITTED </code> it will not
     * timeout.  Otherwise it should.  
     * 
     * @param conn   Connection to test.
     * @return  true if the select got a lock timeout, false otherwise.
     */
    private boolean selectTimesoutDuringUpdate(Connection conn) 
    throws SQLException {

        Connection updateConn=null;
        conn.setAutoCommit(false);

        try {
            // create another connection and do an update but don't commit
            updateConn = openDefaultConnection();
            updateConn.setAutoCommit(false);

            // First update the rows on the update connection
            Statement upStmt = updateConn.createStatement();
            upStmt.executeUpdate("update intTable set i = 3");

            // now see if we can select them
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("Select * from intTable");
            while (rs.next()){};
            rs.close();
        }
        catch (SQLException e)
        {
            if (e.getSQLState().equals("40XL1"))
            {
                // If we got a lock timeout this is not read uncommitted
                return true;
            }   
        }
        finally {
            try {
                conn.rollback();
                updateConn.rollback();
            }catch (SQLException  se) {
                se.printStackTrace();
            }
        }
        return false;
    }
    
    /* -------------------- Other Helper Methods -------------------- */
    
    private void assertConnectionState(
        int expectedHoldability, int expectedIsolation,
        boolean expectedCommitSetting, boolean expectedReadOnly, 
        Connection conn) throws SQLException 
    {
        assertEquals(expectedHoldability, conn.getHoldability());
        assertEquals(expectedIsolation, conn.getTransactionIsolation());
        assertEquals(expectedCommitSetting, conn.getAutoCommit());
        assertEquals(expectedReadOnly, conn.isReadOnly());
    }

    private static void setDatabaseProperty(String property, String value) 
    throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        Connection cadmin = ds.getConnection();
        CallableStatement cs = cadmin.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        cs.setString(1, property);
        cs.setString(2, value);
        cs.execute();

        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        
        cs.close();
        cadmin.close();
    }
    
    private void setHoldability(Connection conn, boolean hold) throws SQLException {

        conn.setHoldability(hold ? ResultSet.HOLD_CURSORS_OVER_COMMIT : ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }
    
    private static void dsConnectionRequests(
        String[] expectedValues, DataSource ds) {

        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(ds, "javax.sql.DataSource");
        }
        
        try {
            ds.getConnection();
            if (!expectedValues[0].equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValues[0], sqle);
        }
        dsConnectionRequest(expectedValues[1], ds, null, null);
        dsConnectionRequest(expectedValues[2], ds, "fred", null);
        dsConnectionRequest(expectedValues[3], ds, "fred", "wilma");
        dsConnectionRequest(expectedValues[4], ds, null, "wilma");
        dsConnectionRequest(
            expectedValues[5], ds, null, "databaseName=wombat");
        dsConnectionRequest(
            expectedValues[6], ds, "fred", "databaseName=wombat");
        dsConnectionRequest(expectedValues[7], 
            ds, "fred", "databaseName=wombat;password=wilma");
        dsConnectionRequest(expectedValues[8], 
            ds, "fred", "databaseName=wombat;password=betty");
    }

    private static void dsConnectionRequest(
        String expectedValue, DataSource ds, String user, String ConnAttr)
    {
        try {
            ds.getConnection(user, ConnAttr);
            if (!expectedValue.equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValue, sqle);
        }
    }
    
    private static void dsConnectionRequests(
        String[] expectedValues, ConnectionPoolDataSource ds) {
        try {
            ds.getPooledConnection();
            if (!expectedValues[0].equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValues[0], sqle);
        }

        dsConnectionRequest(expectedValues[1], ds, null, null);
        dsConnectionRequest(expectedValues[2], ds, "fred", null);
        dsConnectionRequest(expectedValues[3], ds, "fred", "wilma");
        dsConnectionRequest(expectedValues[4], ds, null, "wilma");
        dsConnectionRequest(
            expectedValues[5], ds, null, "databaseName=wombat");
        dsConnectionRequest(
            expectedValues[6], ds, "fred", "databaseName=wombat");
        dsConnectionRequest(expectedValues[7], 
            ds, "fred", "databaseName=wombat;password=wilma");
        dsConnectionRequest(expectedValues[8], 
            ds, "fred", "databaseName=wombat;password=betty");
    }
    
    private static void dsConnectionRequest(String expectedValue, 
        ConnectionPoolDataSource ds, String user, String ConnAttr)
    {
        try {
            ds.getPooledConnection(user, ConnAttr);
            if (!expectedValue.equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValue, sqle);
        }
    }
        
    private static void dsConnectionRequests(
        String[] expectedValues, XADataSource ds) {
        try {
            ds.getXAConnection();
            if (!expectedValues[0].equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValues[0], sqle);
        }

        dsConnectionRequest(expectedValues[1], ds, null, null);
        dsConnectionRequest(expectedValues[2], ds, "fred", null);
        dsConnectionRequest(expectedValues[3], ds, "fred", "wilma");
        dsConnectionRequest(expectedValues[4], ds, null, "wilma");
        dsConnectionRequest(
            expectedValues[5], ds, null, "databaseName=" + dbName);
        dsConnectionRequest(
            expectedValues[6], ds, "fred", "databaseName=" + dbName);
        dsConnectionRequest(expectedValues[7], 
            ds, "fred", "databaseName=" + dbName + ";password=wilma");
        dsConnectionRequest(expectedValues[8], 
            ds, "fred", "databaseName=" + dbName + ";password=betty");
    }
    
    private static void dsConnectionRequest(String expectedValue, 
            XADataSource ds, String user, String ConnAttr)
    {
        try {
            ds.getXAConnection(user, ConnAttr);
            if (!expectedValue.equals("OK"))
                fail (" expected connection to fail, but was OK");
        } catch (SQLException sqle) {
            assertSQLState(expectedValue, sqle);
        }
    }

    protected void assertXAException(String tag, XAException xae) {

        // for all our cases, we expect some kind of closed con error
        // but the message is different for embedded vs. network server
        if (usingEmbedded())
            assertEquals("No current connection.", xae.getMessage());
        else if (usingDerbyNetClient())
            assertEquals(
                "XAER_RMFAIL : No current connection.", xae.getMessage());
        Throwable t = xae.getCause();
        if (t instanceof SQLException)
            assertSQLState("08003", (SQLException)t);
    }

    private static void queryOnStatement(String expectedCursorName,
        int[] expectedValues, Connection conn, Statement s) 
    throws SQLException {

        try {
            // DERBY-2531
            // network server gives mismatched connections. See also
            // comment in testAllDataSources()
            if (usingEmbedded())
                assertEquals(conn, s.getConnection());
            resultSetQuery(expectedCursorName, expectedValues,
                s.executeQuery("select * from intTable"));
        } catch (SQLException sqle) {
            fail (" did not expect sql exception");
        }
    }

    private static void resultSetQuery(String expectedCursorName, 
        int[] expectedValues, ResultSet rs) throws SQLException 
    {
        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(rs, "java.sql.ResultSet");
        }
        assertEquals(expectedCursorName, rs.getCursorName());
        int index=0;
        while (rs.next()) {
            assertEquals(expectedValues[index], rs.getInt(1));
            index++;
        }
        assertEquals(expectedValues.length, index++);
        rs.close();
    }

    private static void assertLocks(int[] expectedValues, Connection conn) 
    throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(
            "SELECT XID, sum(cast (LOCKCOUNT AS INT)) " +
            "FROM SYSCS_DIAG.LOCK_TABLE AS L GROUP BY XID");

        // Don't output actual XID's as they tend for every catalog change
        // to the system.
        int xact_index = 0;
        while (rs.next()) {
            if (expectedValues != null)
                assertEquals(expectedValues[xact_index], rs.getInt(2));
            else 
                fail("expected no locks");
            xact_index++;
        }
        if (expectedValues != null)
            assertEquals(expectedValues.length, xact_index);
        
        rs.close();
        s.close();
    }

    private void assertStatementState(int[] parameterExpectedValues, 
        int[] expectedValues, Statement s) 
    throws SQLException {
        assertEquals(expectedValues[0], s.getResultSetType());
        assertEquals(
            expectedValues[1], s.getResultSetConcurrency());
        assertEquals(
            expectedValues[2], s.getFetchDirection());
        assertEquals(expectedValues[3], s.getFetchSize());
        assertEquals(expectedValues[4], s.getMaxFieldSize());
        assertEquals(expectedValues[5], s.getMaxRows());
        assertEquals(expectedValues[6], s.getResultSetHoldability());

        if (s instanceof PreparedStatement) {
            PreparedStatement ps = (PreparedStatement) s;
            ParameterMetaData psmd = ps.getParameterMetaData();
            // Parameter count:
            assertEquals(parameterExpectedValues[0], psmd.getParameterCount());
            for (int i = 1; i <= psmd.getParameterCount(); i++) {
                assertEquals(parameterExpectedValues[i], psmd.getParameterType(i));
            }
        }
    }

    /**
    Create a statement with modified State.
     */
    private Statement createFloatStatementForStateChecking(
        int[] StatementExpectedValues, Connection conn)
    throws SQLException {
        Statement s = internalCreateFloatStatementForStateChecking(conn);
        s.setCursorName("StokeNewington");
        s.setFetchDirection(ResultSet.FETCH_REVERSE);
        s.setFetchSize(444);
        s.setMaxFieldSize(713);
        s.setMaxRows(19);

        // Create
        assertStatementState(null, StatementExpectedValues, s);
        return s;
    }

    private Statement internalCreateFloatStatementForStateChecking(
        Connection conn) throws SQLException {
        return conn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, 
            ResultSet.CONCUR_READ_ONLY, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private PreparedStatement createFloatStatementForStateChecking(
        int[] parameterExpectedValues, int[] PreparedStatementExpectedValues,
        Connection conn, String sql) 
    throws SQLException {
        PreparedStatement s = 
            internalCreateFloatStatementForStateChecking(conn, sql);
        s.setCursorName("StokeNewington");
        s.setFetchDirection(ResultSet.FETCH_REVERSE);
        s.setFetchSize(888);
        s.setMaxFieldSize(317);
        s.setMaxRows(91);

        // PreparedStatement Create        
        assertStatementState(
            parameterExpectedValues, PreparedStatementExpectedValues, s);
        return s;
    }
    
    private PreparedStatement internalCreateFloatStatementForStateChecking(
        Connection conn, String sql) throws SQLException {
        return conn.prepareStatement(sql, 
            ResultSet.TYPE_SCROLL_INSENSITIVE, 
            ResultSet.CONCUR_READ_ONLY, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private CallableStatement createFloatCallForStateChecking(
        int[] parameterExpectedValues, int[] CallableStatementExpectedValues,
        Connection conn, String sql) 
    throws SQLException 
    {
        CallableStatement s = 
            internalCreateFloatCallForStateChecking(conn, sql);
        s.setCursorName("StokeNewington");
        s.setFetchDirection(ResultSet.FETCH_REVERSE);
        s.setFetchSize(999);
        s.setMaxFieldSize(137);
        s.setMaxRows(85);

        // Callable Statement Create
        assertStatementState(
            parameterExpectedValues, CallableStatementExpectedValues, s);
        return s;
    }
    
    private CallableStatement internalCreateFloatCallForStateChecking(
        Connection conn, String sql) throws SQLException {
        return conn.prepareCall(sql, 
            ResultSet.TYPE_SCROLL_INSENSITIVE, 
            ResultSet.CONCUR_READ_ONLY, 
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    private void assertConnectionOK(
        Object[] expectedValues, String dsName, Connection conn) 
    throws SQLException { 
        
        assertEquals(
            ((Integer)expectedValues[0]).intValue(), conn.getHoldability());

        // check it's a 3.0 connection object by checking if 
        // set & release Savepoint is ok.
        try {
            conn.releaseSavepoint(conn.setSavepoint());
            if (conn.getAutoCommit())
                fail("expected a SQLExpection (savepoint with autocommit on");
            if (!((String)expectedValues[1]).equals("OK"))
                fail("expected a SQLExpection (savepoint with autocommit on");
        } catch (SQLException sqle) {
            // we expect savepoints exceptions because either
            // it's a global transaction, or it's in auto commit mode.
            if (conn.getAutoCommit())
                assertSQLState("XJ010", sqle);
            else if (((String)expectedValues[1]).equals("OK"))
                fail ("unexpected JDBC 3.0 savepoint SQL Exception");
            else 
                assertSQLState((String)expectedValues[1], sqle);
        }

        // Running connection checks
        // connection checks currently only implemented for Embedded
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(conn, "java.sql.Connection");
            SecurityCheck.assertSourceSecurity(
                conn.getMetaData(), "java.sql.DatabaseMetaData");
        }

        assertEquals(((Integer)expectedValues[2]).intValue(), 
            conn.getTransactionIsolation());
        assertEquals(((Boolean)expectedValues[3]).booleanValue(), 
            conn.getAutoCommit());
        assertEquals(((Boolean)expectedValues[4]).booleanValue(), 
            conn.isReadOnly());

        if (dsName.endsWith("DataSource"))
            assertNull(conn.getWarnings());

        Statement s1 = conn.createStatement();
        assertStatementOK(dsName, conn, s1);
        assertStatementOK(dsName, conn, conn.createStatement
            (ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));

        Connection c1 = conn.getMetaData().getConnection();
        // c1 and conn should be the same connection object.
        if (!usingDerbyNetClient() && dsName.indexOf("DataSource")>=0)
            assertEquals(c1, conn);

        // Derby-33 - setTypeMap on connection
        try {
            conn.setTypeMap(java.util.Collections.EMPTY_MAP);
            if (!((String)expectedValues[5]).equals("OK"))
                fail (" expected an sqlexception on setTypeMap(EMPTY_MAP)");
        } catch (SQLException sqle) {
            if (((String)expectedValues[5]).equals("OK"))
                fail ("setTypeMap(EMPTY_MAP) failed ");
            else
                assertSQLState((String)expectedValues[5], sqle);
        }
        try {
            // expect 0A000 - not implemented for client,
            //        XJ081 - invalid null value passed as map for embedded
            conn.setTypeMap(null);
            fail ("setTypeMap(null) should throw exception");
        } catch (SQLException sqle) {
            assertSQLState((String)expectedValues[6], sqle);
        }
        try {
            // a populated map, not implemented
            java.util.Map map = new java.util.HashMap();
            map.put("name", "class");
            conn.setTypeMap(map);
            if (!((String)expectedValues[7]).equals("OK"))
                fail (" expected an sqlexception on setTypeMap(map)");
        } catch (SQLException sqle) {
            if (((String)expectedValues[7]).equals("OK"))
                fail ("setTypeMap(valid value) failed ");
            else
                assertSQLState((String)expectedValues[7], sqle);
        }

        assertConnectionPreClose(dsName, conn);
        conn.close();

        // method calls on a closed connection
        try {
            conn.close(); // expect no error
        } catch (SQLException sqle) {
            fail(" unexpected exception on <closedconn>.close() ");
        }
        try {
            conn.createStatement();
            fail (dsName + " <closedconn>.createStatement(), " +
                "expected 08003 - No current connection");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
        try {
            s1.execute("values 1");
            fail(dsName + " <closedstmt>.execute(), " +
                "expected 08003 - No current connection");
        } catch (SQLException sqle) {
            assertSQLState("08003", sqle);
        }
    }

    private void assertConnectionPreClose(String dsName, Connection conn) 
    throws SQLException {

        // before closing the connection, attempt to change holdability
        // and readOnly
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

        if (!dsName.equals("Nested2"))
        {
            try {
                conn.setReadOnly(true);
            } catch (SQLException sqle) {
                // cannot set read-only in an active transaction, & sometimes
                // connections are active at this point.
                assertSQLState("25501", sqle);
            }
        }
    }
    
    private void assertStatementOK(String dsName, Connection conn, Statement s)
    throws SQLException {

        // checks currently only implemented for embedded 
        if (usingEmbedded())
        {
            SecurityCheck.assertSourceSecurity(s, "java.sql.Statement");
        }

        Connection c1 = s.getConnection();
        if (c1 != conn)
        {
            // with DerbyNetClient and any kind of DataSource, this goes wrong
            if (!usingDerbyNetClient() && (dsName.indexOf("DataSource") >= 0))
                fail ("incorrect connection object returned for Statement.getConnection()");
        }

        s.addBatch("insert into intTable values 1");
        s.addBatch("insert into intTable values 2,3");
        int[] states = s.executeBatch();
        if (states[0] != 1)
            fail ("invalid update count for first batch statement");
        if (states[1] != 2)
            fail ("invalid update count for second batch statement");

        ResultSet rs = s.executeQuery("VALUES 1");
        if (rs.getStatement() != s)
            fail ("incorrect Statement object returned for ResultSet.getStatement for " + dsName);
        rs.close();
        s.close();
    }

    /**
    When a connection is being pooled, the underlying JDBC embedded
    connection object is re-used. As each application gets a new
    Connection object, that is really a wrapper around the old connection
    it should reset any connection spoecific state on the embedded connection
    object.
     */
    private static void PoolReset(String type, PooledConnection pc) throws SQLException
    {
        PoolResetWork("1", "C", pc.getConnection());
        PoolResetWork("2", "", pc.getConnection());
        PoolResetWork("3", "D", pc.getConnection());

        pc.close();
    }

    private static void PoolResetWork(
        String expectedID, String tableAction, Connection conn) 
    throws SQLException
    {
        Statement s = conn.createStatement();
        if (tableAction.equals("C"))
        {
            s.execute("CREATE TABLE PoolResetWork (id int generated always as identity, name varchar(25))");
        }

        ResultSet rs = s.executeQuery("VALUES IDENTITY_VAL_LOCAL()");
        rs.next();
        String val = rs.getString(1);
        if (!rs.wasNull() || (val != null))
            fail ("initial call to IDENTITY_VAL_LOCAL is not NULL!" + val);
        rs.close();

        s.executeUpdate("INSERT INTO PoolResetWork(name) values ('derby-222')");

        rs = s.executeQuery("VALUES IDENTITY_VAL_LOCAL()");
        rs.next();
        val = rs.getString(1);
        assertEquals(expectedID, val);
        rs.close();

        if (tableAction.equals("D"))
        {
            s.execute("DROP TABLE PoolResetWork");
        }

        s.close();
        conn.close();
    }

    /**
     * Make sure this connection's string is unique (DERBY-243)
     */
    private static void assertToString(Connection conn) throws Exception
    {
        assertStringFormat(conn);
        String str = conn.toString();

        if ( conns.containsKey(str))
        {
            throw new Exception("ERROR: Connection toString() is not unique: " 
                    + str);
        }
        conns.put(str, conn);
    }

    /** 
     * Check the format of a pooled connection
     **/
    private static void assertStringFormat(PooledConnection pc) throws Exception
    {
        String prefix = assertStringPrefix(pc);
        String connstr = pc.toString();
        String format = prefix + " \\(ID = [0-9]+\\), Physical Connection = " +
            "<none>|" + CONNSTRING_FORMAT;
        assertTrue(connstr.matches(format));
    }

    /**
     * Check the format of the connection string.  This is the default test
     * to run if this is not a BrokeredConnection class
     */
    private static void assertStringFormat(Connection conn) //throws Exception
    {
        assertStringPrefix(conn);
        String str = conn.toString(); 
        assertTrue(str.matches(CONNSTRING_FORMAT));
    }

    /**
     * Make sure the connection string starts with the right prefix, which
     * is the classname@hashcode.
     *
     * @return the expected prefix string, this is used in further string
     *   format checking
     */
    private static String assertStringPrefix(Object conn) //throws Exception
    {
        String connstr = conn.toString();
        String prefix = conn.getClass().getName() + "@" + conn.hashCode();
        // Connection class and has code for connection string should
        // match prefix
        assertTrue(connstr.startsWith(prefix));

        return prefix;
    }

    /**
     * Check uniqueness of connection strings coming from a
     * DataSouce
     */
    private static void assertToString(DataSource ds) throws Exception
    {
        clearConnections();

        int numConnections = 10;
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            Connection conn = ds.getConnection();
            assertToString(conn);
        }

        clearConnections();
    }

    /**
     * Clear out and close connections in the connections
     * hashtable. 
     */
    private static void clearConnections() throws SQLException
    {
        java.util.Iterator it = conns.values().iterator();
        while ( it.hasNext() )
        {
            Connection conn = (Connection)it.next();
            conn.close();
        }
        conns.clear();
    }

    /**
     * Get connections  using getConnection() and make sure
     * they're unique
     */
    private void assertTenConnectionsUnique() throws Exception
    {
        clearConnections();
        // Open ten connections rather than just two to
        // try and catch any odd uniqueness bugs.  Still
        // no guarantee but is better than just two.
        int numConnections = 10;
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            Connection conn = openDefaultConnection();
            assertToString(conn);
        }

        // Now close the connections
        clearConnections();
    }

    /**
     * Check uniqueness of strings for an XA data source
     */
    private static void assertToString(XADataSource xds) throws Exception
    {
        int numConnections = 10;

        //  First get a bunch of pooled connections
        //  and make sure they're all unique
        Hashtable xaConns = new Hashtable();
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            XAConnection xc = xds.getXAConnection();
            assertStringFormat(xc);
            String str = xc.toString();
            // XA connection toString should be unique
            assertNull(xaConns.get(str));
            xaConns.put(str, xc);
        }

        // Now check that connections from each of these
        // pooled connections have different string values
        Iterator it = xaConns.values().iterator();
        clearConnections();
        while ( it.hasNext() )
        {
            XAConnection xc = (XAConnection)it.next();
            Connection conn = xc.getConnection();
            assertToString(conn);
        }
        clearConnections();

        // Now clear out the pooled connections
        it = xaConns.values().iterator();
        while ( it.hasNext() )
        {
            XAConnection xc = (XAConnection)it.next();
            xc.close();
        }
        xaConns.clear();
    }

    /**
     * Check uniqueness of strings with a pooled data source.
     * We want to check the PooledConnection as well as the
     * underlying physical connection. 
     */
    private static void assertToString(ConnectionPoolDataSource pds)
    throws Exception
    {
        int numConnections = 10;

        //  First get a bunch of pooled connections
        //  and make sure they're all unique
        Hashtable pooledConns = new Hashtable();
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            PooledConnection pc = pds.getPooledConnection();
            assertStringFormat(pc);
            String str = pc.toString();
            // Pooled connection toString should be unique
            assertNull( pooledConns.get(str));
            pooledConns.put(str, pc);
        }

        // Now check that connections from each of these
        // pooled connections have different string values
        Iterator it = pooledConns.values().iterator();
        clearConnections();
        while ( it.hasNext() )
        {
            PooledConnection pc = (PooledConnection)it.next();
            Connection conn = pc.getConnection();
            assertToString(conn);
        }
        clearConnections();

        // Now clear out the pooled connections
        it = pooledConns.values().iterator();
        while ( it.hasNext() )
        {
            PooledConnection pc = (PooledConnection)it.next();
            pc.close();
        }
        pooledConns.clear();
    }

    /**
     * Return the Java class and method for the procedure
     * for the nested connection test.
     * checkDataSource 30 will override.
     */
    private String getNestedMethodName()
    {
        return "DataSourceTest.checkNesConn";
    }

    // calling checkConnection 
    // - for use in a procedure to get a nested connection.
    public static void checkNesConn (String dsName) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        String EmptyMapValue=null;
        // Note: currently, not supported
        String NullMapValue=null;
        String MapMapValue=null;
        if (usingEmbedded())
        {
            EmptyMapValue="OK"; NullMapValue="XJ081"; MapMapValue="0A000";
        }
        else if (usingDerbyNetClient())
        {
            EmptyMapValue="0A000"; NullMapValue="0A000"; MapMapValue="0A000";
        }
        Object[] expectedValues = { 
            new Integer(ResultSet.HOLD_CURSORS_OVER_COMMIT), "OK",
            new Integer(2), new Boolean(false), new Boolean(false), 
            EmptyMapValue, NullMapValue, MapMapValue};

        new DataSourceTest("DataSourceTest").assertConnectionOK(
            expectedValues, dsName, conn);
    }
}

class cdsXid implements Xid, Serializable
{
  private static final long serialVersionUID = 64467338100036L;

    private final int format_id;
    private byte[] global_id;
    private byte[] branch_id;


    cdsXid(int xid, byte b1, byte b2)
    {
        format_id = xid;
        global_id = new byte[Xid.MAXGTRIDSIZE];
        branch_id = new byte[Xid.MAXBQUALSIZE];

        for (int i = 0; i < global_id.length; i++) {
            global_id[i] = b1;
        }

        for (int i = 0; i < branch_id.length; i++) {
            branch_id[i] = b2;
        }
    }

    /**
     * Obtain the format id part of the Xid.
     * <p>
     *
     * @return Format identifier. O means the OSI CCR format.
     **/
    public int getFormatId()
    {
        return(format_id);
    }

    /**
     * Obtain the global transaction identifier part of XID as an array of 
     * bytes.
     * <p>
     *
     * @return A byte array containing the global transaction identifier.
     **/
    public byte[] getGlobalTransactionId()
    {
        return(global_id);
    }

    /**
     * Obtain the transaction branch qualifier part of the Xid in a byte array.
     * <p>
     *
     * @return A byte array containing the branch qualifier of the transaction.
     **/
    public byte[] getBranchQualifier()
    {
        return(branch_id);
    }
}

class AssertEventCatcher implements ConnectionEventListener
{
    private final int catcher;

    AssertEventCatcher(int which) {
        catcher=which;
    }

    // ConnectionEventListener methods
    public void connectionClosed(ConnectionEvent event)
    {
        // System.out.print("EVENT("+catcher+"):connectionClosed");
        SQLException sqle = event.getSQLException();
        if (sqle != null)
            System.out.print("DataSourceTest-" + catcher + "; SQLSTATE="
                + sqle.getSQLState());
    }

    public void connectionErrorOccurred(ConnectionEvent event)
    {
        // System.out.print("EVENT("+catcher+"):connectionErrorOccurred");
        SQLException sqle = event.getSQLException();
        if (sqle != null)
            System.out.print("DataSourceTest-" + catcher + "; SQLSTATE=" +
                sqle.getSQLState());
    }
}
