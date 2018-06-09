/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.Derby5165Test
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

public class Derby5165Test extends BaseJDBCTestCase {
    
    public Derby5165Test(String name) {
        super(name);
    }

    public static Test suite() {
        // the test requires XADataSource to run
        if (JDBC.vmSupportsJDBC3()) {
            //Test test = TestConfiguration.defaultSuite(Derby5165Test.class);
            Test test = TestConfiguration.embeddedSuite(Derby5165Test.class);
            
            // Set the lock timeout back to the default, because when
            // running in a bigger suite the value may have been
            // altered by an earlier test
            test = DatabasePropertyTestSetup.setLockTimeouts(test, 2, 4);
            
            test = TestConfiguration.singleUseDatabaseDecorator( test, "d5165db" );
            test = TestConfiguration.singleUseDatabaseDecorator( test, "d5165db2");
            test = TestConfiguration.singleUseDatabaseDecorator( test, "d5165db3" );
            test = TestConfiguration.singleUseDatabaseDecorator( test, "d5165db4");
            
            return test;
        }

        return new BaseTestSuite(
            "Derby5165Test cannot run without XA support");
    }
    
    public void testXAUpdateLockKeptPastDBRestart()
            throws InterruptedException, SQLException, XAException {
        if (usingDerbyNetClient())
            return;
        // step 0 - initialize db and xa constructs
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "d5165db");
        XAConnection xac = xads.getXAConnection();
        XAResource xar = xac.getXAResource();
        Connection conn = xac.getConnection();
        // step 1 - perform update with XA, using Xid xid1 
        Statement s = conn.createStatement();
        String tableName = "d5165t";
        createAndLoadTable(conn, tableName, true);
        conn.commit();
        
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from " + tableName),
                "1");
        
        conn.close();
        s.close();
        
        Xid xid1 = new MyXid(1, 2, 3);
        xar.start(xid1, XAResource.TMNOFLAGS);
        Connection c1 = xac.getConnection();
        Statement s1 = c1.createStatement();
        s1.execute("update " + tableName + " set x = 2 where x = 1");
        xar.end(xid1, XAResource.TMSUCCESS);

        // step 2-prepare xid1 with XA but do NOT commit
        xar.prepare(xid1);

        // step 3 - 'restart' the database
        try {
            // so first shutdown the database
            DataSource ds = JDBCDataSource.getDataSource("d5165db");
            JDBCDataSource.shutdownDatabase(ds);
        } catch (Exception e) {
            // Ignore shutdown successful exception
        }
        // the next Connection call automatically starts up the database again 
        Connection c2 = openConnection("d5165db");

        // step 4 - if the bug occurs, the updates of step 1 will be visible
        // however, the XA transaction was not committed, so this should
        // time out.
        Statement s2 = c2.createStatement();
        try { 
            ResultSet rs = s2.executeQuery("select * from " + tableName);
            //System.out.println("Contents of T:");
            while (rs.next()) {
                //    System.out.println(rs.getInt(1));
                rs.getInt(1);
            }
            rs.close();
            fail("expected a timeout");
        } catch (SQLException sqle ) {
            // for debugging uncomment following lines:
            //System.out.println("expected exception: ");
            //e.printStackTrace();
            assertSQLState("40XL1", sqle);
        }

        s.close();
        c2.close();
        s2.close();
        
        xac.close();
    }
    
    public void testXAInsertLockKeptPastDBRestart()
            throws InterruptedException, SQLException, XAException {
        if (usingDerbyNetClient())
            return;
        // open a connection to force creation of the second database
        Connection ctmp = openConnection("d5165db2");
        ctmp.close();
        
        // step 0 - initialize db and xa constructs
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "d5165db2");
        XAConnection xac = xads.getXAConnection();
        XAResource xar = xac.getXAResource();
        Connection conn = xac.getConnection();
        // step 1 - perform insert with XA, using Xid xid1 
        Statement s = conn.createStatement();
        String tableName = "d5165t";
        createAndLoadTable(conn, tableName, true);
        conn.commit();
        
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from " + tableName),
                "1");
        
        conn.close();
        s.close();
        
        Xid xid1 = new MyXid(1, 2, 3);
        xar.start(xid1, XAResource.TMNOFLAGS);
        Connection c1 = xac.getConnection();
        Statement s1 = c1.createStatement();
        s1.execute("insert into " + tableName + " values 2");
        xar.end(xid1, XAResource.TMSUCCESS);

        // step 2-prepare xid1 with XA but do NOT commit
        xar.prepare(xid1);

        // step 3 - 'restart' the database
        try {
            // so first shutdown the database
            DataSource ds = JDBCDataSource.getDataSource("d5165db2");
            JDBCDataSource.shutdownDatabase(ds);
        } catch (Exception e) {
            // Ignore shutdown successful exception
        }
        // the next Connection call automatically starts up the database again 
        Connection c2 = openConnection("d5165db2");

        // step 4 - if the bug occurs, the updates of step 1 will be visible
        // however, the XA transaction was not committed, so this should have
        // timed out.
        Statement s2 = c2.createStatement();
        try { 
            ResultSet rs = s2.executeQuery("select * from " + tableName);
            while (rs.next()) {
                //    System.out.println(rs.getInt(1));
                rs.getInt(1);
            }
            rs.close();
            fail("expected a timeout");
        } catch (SQLException sqle ) {
            assertSQLState("40XL1", sqle);
        }

        s.close();
        c2.close();
        s2.close();
        
        xac.close();
    }
    
    public void testXAUpdateLockKeptPastCrashedDBRestart() throws Exception
    {
        // call a forked process - this one will do something,
        // then *not* shutdown, but not doing anything else either,
        // implying a crash when the jvm is done
        // This will force the connect to recover the database.
        // Pass in the name of the database to be used.
        assertLaunchedJUnitTestMethod("org.apache.derbyTesting." +
                "functionTests.tests.jdbcapi.Derby5165Test.launchUpdate",
                "d5165db3");
        // Call the second forked process. This will connect and check,
        // forcing recovery.
        assertLaunchedJUnitTestMethod("org.apache.derbyTesting." +
                "functionTests.tests.jdbcapi.Derby5165Test.checkUpdate",
                "d5165db3");
    }

    public void testXAInsertLockKeptPastCrashedDBRestart() throws Exception
    {
        // call a forked process - this one will do something,
        // then *not* shutdown, but not doing anything else either,
        // implying a crash when the jvm is done
        // This will force the connect to recover the database.
        // Pass in the name of the database to be used.
        assertLaunchedJUnitTestMethod("org.apache.derbyTesting." +
                "functionTests.tests.jdbcapi.Derby5165Test.launchInsert",
                "d5165db4");
        // Call the second forked process. This will connect and check,
        // forcing recovery.
        assertLaunchedJUnitTestMethod("org.apache.derbyTesting." +
                "functionTests.tests.jdbcapi.Derby5165Test.checkInsert",
                "d5165db4");
    }

    public void launchUpdate() throws Exception
    {
        // setup to setup for update
        // open a connection to the database
        Connection simpleconn = getConnection();
        setAutoCommit(false);
        String tableName = "d5165t2";
        createAndLoadTable(simpleconn, tableName, true);
        
        // step 0 - initialize db and xa constructs
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "d5165db3");
        XAConnection xac = xads.getXAConnection();
        XAResource xar = xac.getXAResource();
        Connection conn = xac.getConnection();
        // step 1 - perform update with XA, using Xid xid1 
        Statement s = conn.createStatement();
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from " + tableName),
                "1");
        
        conn.close();
        s.close();
        
        Xid xid1 = new MyXid(1, 2, 3);
        xar.start(xid1, XAResource.TMNOFLAGS);
        Connection c1 = xac.getConnection();
        Statement s1 = c1.createStatement();
        s1.execute("update " + tableName + " set x = 2 where x = 1");
        xar.end(xid1, XAResource.TMSUCCESS);

        // step 2-prepare xid1 with XA but do NOT commit
        xar.prepare(xid1);
        
        // doing nothing further should stop this jvm process.
    }

    public void checkUpdate() throws Exception
    {
        String tableName = "d5165t2";
        // getting the connection will start and thus recover the db
        Connection c2 = getConnection();
        setAutoCommit(false);

        // step 4 - if the bug occurs, the updates of step 1 will be visible
        // however, the XA transaction was not committed, so this should
        // time out.
        Statement s2 = c2.createStatement();
        try { 
            ResultSet rs = s2.executeQuery(
                    "select * from " + tableName);
            while (rs.next()) {
                rs.getInt(1);
            }
            rs.close();
            fail("expected a timeout");
        } catch (SQLException sqle ) {
            assertSQLState("40XL1", sqle);
        }
    }

    public void launchInsert() throws Exception
    {
        String tableName = "d5165t3";
        // setup to setup for update
        // open a connection to the database
        Connection simpleconn = getConnection();
        setAutoCommit(false);        
        createAndLoadTable(simpleconn, tableName, true);
        
        // step 0 - initialize db and xa constructs
        XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "d5165db4");
        XAConnection xac = xads.getXAConnection();
        XAResource xar = xac.getXAResource();
        Connection conn = xac.getConnection();
        // step 1 - perform insert with XA, using Xid xid1 
        Statement s = conn.createStatement();
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from " + tableName),
                "1");
        
        conn.close();
        s.close();
        
        Xid xid1 = new MyXid(1, 2, 3);
        xar.start(xid1, XAResource.TMNOFLAGS);
        Connection c1 = xac.getConnection();
        Statement s1 = c1.createStatement();
        // insert
        s1.execute("insert into " + tableName + " values 2");
        xar.end(xid1, XAResource.TMSUCCESS);

        // step 2-prepare xid1 with XA but do NOT commit
        xar.prepare(xid1);
        
        // doing nothing further should stop this jvm process.
    }

    public void checkInsert() throws Exception
    {
        String tableName = "d5165t3";
        // getting the connection will start and thus recover the db
        Connection c2 = getConnection();
        setAutoCommit(false);

        // step 4 - if the bug occurs, the updates of step 1 will be visible
        // however, the XA transaction was not committed, so this should
        // time out.
        Statement s2 = c2.createStatement();
        try { 
            ResultSet rs = s2.executeQuery(
                    "select * from " + tableName);
            while (rs.next()) {
                rs.getInt(1);
            }
            rs.close();
            fail("expected a timeout");
        } catch (SQLException sqle ) {
            assertSQLState("40XL1", sqle);
        }
    }
    
    /**
     * Create and load a table.
     *
     * @param create_table  If true, create new table - otherwise load into
     *                      existing table.
     * @param tblname       table to use.
     *
     * @exception  SQLException  Standard exception policy.
     **/
    private void createAndLoadTable(
            Connection conn, 
            String     tblname,
            boolean    create_table)
                    throws SQLException
                    {
        if (create_table)
        {
            Statement s = conn.createStatement();

            s.executeUpdate("create table " + tblname + "(x int)");
            s.executeUpdate("insert into " + tblname + " values 1");
            conn.commit();
            
            JDBC.assertSingleValueResultSet(
                    s.executeQuery("select * from " + tblname),
                    "1");
            s.close();
            println("table created: " + tblname);
        }
    }

    private static class MyXid implements Xid {
        int formatId;
        byte txid, bq;

        MyXid(int formatId, int txid, int bq) {
            this.formatId = formatId;
            this.txid = (byte) txid;
            this.bq = (byte) bq;
        }

        @Override
        public int getFormatId() {
            return formatId;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return new byte[] { txid };
        }

        @Override
        public byte[] getBranchQualifier() {
            return new byte[] { bq };
        }
    }

}
