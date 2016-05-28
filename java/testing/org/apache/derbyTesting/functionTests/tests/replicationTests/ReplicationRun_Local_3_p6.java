/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_3_p6
 
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
package org.apache.derbyTesting.functionTests.tests.replicationTests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derby.jdbc.ClientDataSourceInterface;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * Test for DERBY-3896.
 * 
 */

public class ReplicationRun_Local_3_p6 extends ReplicationRun_Local_3
{
    
    String getDerbyServerPID = null;

    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_3_p6(String testcaseName)
    {
        super(testcaseName);

    }
    
    public static Test suite()
    {
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Local_3_p6 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_3_p6.class);
        
        return SecurityManagerSetup.noSecurityManager(suite);

    }
    
    /* ENABLE WHEN DERBY-3896 IS FIXED! 
    public void testReplication_Local_3_p6_DERBY_3896()
    throws Exception
    {
        derby_3896(false); // Autocommit off during create table before starting replication
    }
    */
    
    /**
     * Test the DERBY-3896 scenario but with autocommit on which
     * does not fail.
     * DERBY-3896: Autocommit off during create table before starting replication 
     * causes uncommitted data not being replicated.
     * @throws java.lang.Exception
     */
    public void testReplication_Local_3_p6_autocommit_OK()
    throws Exception
    {
        derby_3896(true);
    }
    
    private void derby_3896(boolean autocommit)
    throws Exception
    {
        // makeReadyForReplication(); expanded for more control:
        // BEGIN
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
        startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES,
                masterServerPort,
                masterDbSubPath);
        
        startServer(slaveJvmVersion, derbySlaveVersion,
                slaveServerHost,
                ALL_INTERFACES,
                slaveServerPort,
                slaveDbSubPath);
        
        startServerMonitor(slaveServerHost);
        
        // Must expand bootMasterDatabase() since it also freezes db!
        // BEGIN P1
        String URL = masterURL(replicatedDb)
                +";create=true"
                +useEncryption(true);

        if ( masterServerHost.equalsIgnoreCase("localhost") || localEnv )
        {
            util.DEBUG("bootMasterDatabase getConnection("+URL+")");
            Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
            Connection conn = DriverManager.getConnection(URL);
            conn.close();
        }
        else
        {
            assertTrue("NOT IMPLEMENTED for non-local host!", false);
        }
        // END P1
        
        // Create and fill a table to have a NON-empty master db.
        
        // BEGIN DERBY-3896
        Connection mConn = _getConnection(masterDatabasePath, masterDbSubPath, 
                replicatedDb, masterServerHost, masterServerPort);
        
        if ( ! autocommit ) mConn.setAutoCommit(false); // Autocommit off.
        // Autocommit off causes uncommitted data not being replicated - DERBY-3896.
        
        PreparedStatement ps = mConn.prepareStatement("create table t(i integer)");
        ps.execute();
        ps = mConn.prepareStatement("insert into t values 0,1,2,3");
        int _noTuplesInserted = 4;
        ps.execute();
        
        _verifyTable(mConn, _noTuplesInserted);
        // END DERBY-3896
        
        // BEGIN P2
        util.DEBUG("************************** DERBY-???? Preliminary needs to freeze db before copying to slave and setting replication mode.");
        if ( masterServerHost.equalsIgnoreCase("localhost") || localEnv )
        {
           URL = masterURL(replicatedDb);
            Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
            util.DEBUG("bootMasterDatabase getConnection("+URL+")");
            Connection conn = DriverManager.getConnection(URL);
            Statement s = conn.createStatement();
            s.execute("call syscs_util.syscs_freeze_database()");
            conn.close();
        }
        else
        {
            assertTrue("NOT IMPLEMENTED for non-local host!", false);
        }
        util.DEBUG("bootMasterDatabase done.");
        // END P2
        
        // Copy master to slave:
        initSlave(slaveServerHost,
                jvmVersion,
                replicatedDb);
        
        startSlave(jvmVersion, replicatedDb,
                slaveServerHost,
                slaveServerPort,
                slaveServerHost,
                slaveReplPort,
                testClientHost);
        
        startMaster(jvmVersion, replicatedDb,
                masterServerHost,
                masterServerPort,
                masterServerHost,
                slaveServerPort,
                slaveServerHost,
                slaveReplPort);
        // END        
        
        mConn.commit(); 
        
        failOver(jvmVersion,
                masterDatabasePath, masterDbSubPath, replicatedDb,
                masterServerHost,  // Where the master db is run.
                masterServerPort,
                testClientHost);
        
        connectPing(slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb,
                slaveServerHost,slaveServerPort,
                testClientHost);
        
        // verifySlave(); // Can't be used here because we don't have standard load (tables).
        
        Connection sConn = _getConnection(slaveDatabasePath, slaveDbSubPath, 
                replicatedDb,slaveServerHost,slaveServerPort);
        _verifyTable(sConn, _noTuplesInserted); // Verify the test specific table!
        
        // We should verify the master as well, at least to see that we still can connect.
        // verifyMaster(); // Can't be used here because we don't have standard load (tables).
        
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
    }
        
    private Connection _getConnection(String databasePath, String dbSubPath, 
            String dbName, String serverHost, int serverPort)
        throws Exception
    {
        ClientDataSourceInterface ds;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientDataSource");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientDataSource40");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        }

        ds.setDatabaseName(databasePath +FS+ dbSubPath +FS+ dbName);
        ds.setServerName(serverHost);
        ds.setPortNumber(serverPort);
        ds.setConnectionAttributes(useEncryption(false));
        return ds.getConnection();
    }
    
    private void _verifyTable(Connection conn, int noTuplesInserted)
        throws SQLException
    {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        rs = s.executeQuery("select max(i) from t");
        rs.next();
        int max = rs.getInt(1);
        util.DEBUG("_verify: " + count + "/" + noTuplesInserted + " " + max +
                   "/" + (noTuplesInserted - 1));
        assertEquals("Expected "+ noTuplesInserted +" tuples, got "+ count +".",
                     noTuplesInserted, count);
        assertEquals("Expected " +(noTuplesInserted-1) +" max, got " + max +".",
                     noTuplesInserted - 1, max);        
    }
    
}
