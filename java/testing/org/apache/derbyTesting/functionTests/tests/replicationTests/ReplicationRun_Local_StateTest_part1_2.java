/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_StateTest_part1_2
 
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
import java.sql.SQLException;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * 
 */

public class ReplicationRun_Local_StateTest_part1_2 extends ReplicationRun
{
    
    /**
     * Creates a new instance of ReplicationRun_Local_StateTest_part1
     * 
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_StateTest_part1_2(String testcaseName)
    {
        super(testcaseName);
    }
        
    public static Test suite()
    {
        TestSuite suite = new TestSuite("ReplicationRun_Local_StateTest_part1_2 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_StateTest_part1_2.class );
        
        return SecurityManagerSetup.noSecurityManager(suite);
    }
    
    //////////////////////////////////////////////////////////////
    ////
    //// The replication test framework (testReplication()):
    //// a) "clean" replication run starting master and slave servers,
    ////     preparing master and slave databases,
    ////     starting and stopping replication and doing
    ////     failover for a "normal"/"failure free" replication
    ////     test run.
    ////
    //////////////////////////////////////////////////////////////
    
    public void testReplication_Local_StateTest_part1_2()
    throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
        masterServer = startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES, // masterServerHost, // "0.0.0.0", // All. or use masterServerHost for interfacesToListenOn,
                masterServerPort,
                masterDbSubPath); // Distinguishing master/slave
        
        
        slaveServer = startServer(slaveJvmVersion, derbySlaveVersion,
                slaveServerHost,
                ALL_INTERFACES, // slaveServerHost, // "0.0.0.0", // All. or use slaveServerHost for interfacesToListenOn,
                slaveServerPort,
                slaveDbSubPath); // Distinguishing master/slave
        
        startServerMonitor(slaveServerHost);
        
        bootMasterDatabase(jvmVersion,
                masterDatabasePath +FS+ masterDbSubPath,
                replicatedDb,
                masterServerHost, // Where the startreplication command must be given
                masterServerPort, // master server interface accepting client requests
                null // bootLoad, // The "test" to start when booting db.
                );
        
        initSlave(slaveServerHost,
                jvmVersion,
                replicatedDb); // Trunk and Prototype V2: copy master db to db_slave.
        
        startSlave(jvmVersion, replicatedDb,
                slaveServerHost, // slaveClientInterface // where the slave db runs
                slaveServerPort,
                slaveServerHost, // for slaveReplInterface
                slaveReplPort,
                testClientHost);
        
        // With master started above, next will fail! 
        // Also seems failover will fail w/XRE21!
        // Further testing: skipping next startMaster seems to 
        // NOT remove failover failure!
        /* TEMP: should be operational already - try skipping this. */
        startMaster(jvmVersion, replicatedDb,
                masterServerHost, // Where the startMaster command must be given
                masterServerPort, // master server interface accepting client requests
                masterServerHost, // An interface on the master: masterClientInterface (==masterServerHost),
                slaveServerPort, // Not used since slave don't allow clients.
                slaveServerHost, // for slaveReplInterface
                slaveReplPort);
         /* */
        
        _testPostStartedMasterAndSlave_StopMaster(); // Not in a state to continue.
                
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
    }


    private void _testPostStartedMasterAndSlave_StopMaster()
    throws InterruptedException
    {
        Connection conn = null;
        String db = null;
        String connectionURL = null;
        
        // 1. Attempt to perform stopMaster on slave. Should fail.
        db = slaveDatabasePath +FS+ReplicationRun.slaveDbSubPath 
                +FS+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";stopMaster=true";
        util.DEBUG("1. testPostStartedMasterAndSlave_StopMaster: " 
                + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("Unexpectedly connected as: " + connectionURL);
            assertTrue("Unexpectedly connected as: " + connectionURL,false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            //  SQLCODE: -1, SQLSTATE: 08004
            assertTrue("connectionURL +  failed: " + msg, 
                    SQLState.LOGIN_FAILED.equals(ss));
            util.DEBUG("stopMaster on slave failed as expected: " 
                    + connectionURL + " " + msg);
        }
        // Default replication test sequence still OK.
        
        // 2. stopMaster on master: OK
        db = masterDatabasePath +FS+ReplicationRun.masterDbSubPath 
                +FS+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";stopMaster=true";
        util.DEBUG("2. testPostStartedMasterAndSlave_StopMaster: " 
                + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("Connected as expected: " + connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG("stopMaster on master failed: " + connectionURL + " " + msg);
            assertTrue("stopMaster on master failed: " + connectionURL + " " + msg,false);
        }
        // Not meaningful to continue default replication test sequence after this point!
        
        // 3. Connect to slave which now is not in non-replication mode is OK.
        db = slaveDatabasePath +FS+ReplicationRun.slaveDbSubPath +FS+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db;
        util.DEBUG("3. testPostStartedMasterAndSlave_StopMaster: " + connectionURL);
        // Try a sleep:
        Thread.sleep(15000L);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("Successfully connected: " + connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG("3. Connect to slave unexpectedly failed : " 
                    + connectionURL + " " + msg);
            assertTrue("3. Connect to slave unexpectedly failed : " 
                    + connectionURL + " " + msg, false);
        }
        
        // 4. stopMaster on slave which now is not in replication mode should fail.
        db = slaveDatabasePath +FS+ReplicationRun.slaveDbSubPath +FS+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";stopMaster=true";
        util.DEBUG("4. testPostStartedMasterAndSlave_StopMaster: " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("4. Unexpectedly connected: " + connectionURL);
            assertTrue("4. Unexpectedly connected: " + connectionURL,false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            assertTrue("4. stopMaster on slave failed with: " 
                    + connectionURL + " " + msg, 
                    SQLState.REPLICATION_NOT_IN_MASTER_MODE.equals(ss));
            util.DEBUG("4. stopMaster on slave failed as expected: " 
                    + connectionURL + " " + msg);
        }
        
        // 5. Connect master which now is now in non-replication mode should succeed.
        db = masterDatabasePath +FS+ReplicationRun.masterDbSubPath +FS+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db;
        util.DEBUG("5. testPostStartedMasterAndSlave_StopMaster: " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("5. Successfully connected: " + connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG("5. Connect to db not in master mode unexpectedly failed : " 
                    + connectionURL + " " + msg);
            assertTrue("5. Connect to db not in master mode unexpectedly failed : " 
                    + connectionURL + " " + msg, false);
        }

        // 6. Attempt to do stopmaster on master which now is now in non-replication mode should fail.
        db = masterDatabasePath +FS+ReplicationRun.masterDbSubPath 
                +FS+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";stopMaster=true";
        util.DEBUG("6. testPostStartedMasterAndSlave_StopMaster: " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("6. Unexpectedly connected: " + connectionURL);
            assertTrue("6. Unexpectedly connected: " + connectionURL,false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            assertTrue("6. stopMaster on server not in master mode failed with: " 
                    + connectionURL + " " + msg, 
                    SQLState.REPLICATION_NOT_IN_MASTER_MODE.equals(ss));
            util.DEBUG("6. stopMaster on server not in master mode failed as expected: " 
                    + connectionURL + " " + msg);
        }
    }
    
}
