/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_StateTest_part1
 
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
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * 
 */

public class ReplicationRun_Local_StateTest_part1 extends ReplicationRun
{
    final static String CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE = "08004";
    final static String LOGIN_FAILED = "08004";
    final static String REPLICATION_DB_NOT_BOOTED = "XRE11";
    final static String REPLICATION_MASTER_ALREADY_BOOTED = "XRE22";
    final static String REPLICATION_NOT_IN_MASTER_MODE = "XRE07";
    final static String REPLICATION_SLAVE_STARTED_OK = "XRE08";



/**
     * Creates a new instance of ReplicationRun_Local_StateTest_part1
     * 
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_StateTest_part1(String testcaseName)
    {
        super(testcaseName);
    }
        
    public static Test suite()
    {
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Local_StateTest_part1 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_StateTest_part1.class );
        
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
    
    public void testReplication_Local_StateTest_part1()
    throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        // State test. Continuation OK.
        _testPreStartedMasterServer(); 
        
        initMaster(masterServerHost,
                replicatedDb);
        
        startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES, // masterServerHost, // "0.0.0.0", // All. or use masterServerHost for interfacesToListenOn,
                masterServerPort,
                masterDbSubPath); // Distinguishing master/slave
        
        // State test. 
        _testPreStartedSlaveServer(); 
        
        startServer(slaveJvmVersion, derbySlaveVersion,
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
        
        // State test. 
        _testPreInitSlave();
        
        initSlave(slaveServerHost,
                jvmVersion,
                replicatedDb); // Trunk and Prototype V2: copy master db to db_slave.
        
        // State test. 
        _testPreStartedSlave(); // Currently NOOP
        
        startSlave(jvmVersion, replicatedDb,
                slaveServerHost, // slaveClientInterface // where the slave db runs
                slaveServerPort,
                slaveServerHost, // for slaveReplInterface
                slaveReplPort,
                testClientHost);
        
        // State test. 
        _testPreStartedMaster();
        
        // With master started above, next will fail! 
        // Also seems failover will fail w/XRE21! : DERBY-3358!
        // Further testing: skipping next startMaster seems to 
        // NOT remove failover failure!
        /* TEMP: should be operational already - try skipping this. * /
        startMaster(jvmVersion, replicatedDb,
                masterServerHost, // Where the startMaster command must be given
                masterServerPort, // master server interface accepting client requests
                masterServerHost, // An interface on the master: masterClientInterface (==masterServerHost),
                slaveServerPort, // Not used since slave don't allow clients.
                slaveServerHost, // for slaveReplInterface
                slaveReplPort);
         / * */
        
        // Used to run positive tests.
        // Handle negative testing in State.testPostStartedMasterAndSlave().
        // Observe that it will not be meaningful to do runTest if State.XXXX()
        // has led to incorrect replication state wrt. replicationTest.
        
        replicationTest = "org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationTestRun";
        util.DEBUG("replicationTest: " + replicationTest);
        replicationVerify = "org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationTestRun_Verify";
        util.DEBUG("replicationVerify: " + replicationVerify);

        runTest(replicationTest, // Returns immediatly if replicationTest is null.
                jvmVersion,
                testClientHost,
                masterServerHost, masterServerPort,
                replicatedDb);
        
        failOver(jvmVersion,
                masterDatabasePath, masterDbSubPath, replicatedDb,
                masterServerHost,  // Where the master db is run.
                masterServerPort,
                testClientHost);
        
        connectPing(slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb,
                slaveServerHost,slaveServerPort,
                testClientHost);
        
        verifySlave();
        
        // We should verify the master as well, at least to see that we still can connect.
        verifyMaster();
        
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
    }

    private void _testPreStartedMasterServer()
    {
        Connection conn = null;
        String db = masterDatabasePath +FS+ReplicationRun.masterDbSubPath +FS+ replicatedDb;
        String connectionURL = "jdbc:derby:"
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";startMaster=true"
                + ";slavehost=" + slaveServerHost
                + ";slaveport=" + slaveServerPort;
        util.DEBUG("testPreStartedMasterServer: " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG("testStartMasterConnect_Illegal: " + msg);
            assertTrue("Unexpected SQLException: " + msg, "08001".equals(ss));
            util.DEBUG("As expected.");
            return;
        }
        assertTrue("Expected SQLException: '08001 " + db + "'",false);
    }

    private void _testPreStartedSlaveServer()
    {
        Connection conn = null;
        String db = slaveDatabasePath +FS+ReplicationRun.slaveDbSubPath +FS+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";startSlave=true"
                + ";slavehost=" + slaveServerHost 
                + ";slaveport=" + slaveServerPort;
        util.DEBUG("testPreStartedSlaveServer: " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG("testStartSlaveConnect_Illegal: " + msg);
            assertTrue("Unexpected SQLException: " + msg, "08001".equals(ss));
            util.DEBUG("As expected.");
            return;
        }
        assertTrue("Expected SQLException: '08001 " + db + "'",false);
    }

    private void _testPreInitSlave()
    {
        String db = null;
        String connectionURL = null;
        Connection conn = null;
        
        // 1.  stopMaster on master: fail
        db = masterDatabasePath +FS+ReplicationRun.masterDbSubPath +FS+ replicatedDb;
        connectionURL = "jdbc:derby:"
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";stopMaster=true";
        util.DEBUG("1. testPreInitSlave:" + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("Unexpectedly connected: " + connectionURL);
            assertTrue("Unexpectedly connected: " + connectionURL,false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            // SQLState.REPLICATION_NOT_IN_MASTER_MODE

            BaseJDBCTestCase.assertSQLState(
                "stopMaster on master failed: " + msg,
                REPLICATION_NOT_IN_MASTER_MODE,
                se);
            util.DEBUG("stopMaster on master failed as expected: " + connectionURL + " " + msg);
        }
        
        // 2. stopSlave on slave: fail
        db = slaveDatabasePath +FS+ReplicationRun.slaveDbSubPath +FS+ replicatedDb;
        connectionURL = "jdbc:derby:"
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";stopSlave=true";
        util.DEBUG("2. testPreInitSlave: " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("Unexpectedly connected: " + connectionURL);
            assertTrue("Unexpectedly connected: " + connectionURL,false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            BaseJDBCTestCase.assertSQLState(
                "stopSlave on slave failed: " + msg,
                REPLICATION_DB_NOT_BOOTED,
                se);
            util.DEBUG("stopSlave on slave failed as expected: " + connectionURL + " " + msg);
        }
    }

    private void _testPreStartedSlave()
    {
        Connection conn = null;
        String db = slaveDatabasePath +FS+ReplicationRun.slaveDbSubPath +FS+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";startSlave=true"
                + ";slaveHost=" + slaveServerHost 
                + ";slavePort=" + slaveReplPort;
        util.DEBUG("testPreStartedSlave: Test moved to TestPostStartedMasterAndSlave! " + connectionURL);
        if (true)return;
        
        // First StartSlave connect ok:
        try
        {
            conn = DriverManager.getConnection(connectionURL); 
            // Will hang until startMaster!
            util.DEBUG("1. Successfully connected as: " + connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG(msg);
            BaseJDBCTestCase.assertSQLState(
                "2. Unexpected SQLException: " + msg,
                REPLICATION_SLAVE_STARTED_OK,
                se);
        }
        
        // Next StartSlave connect should fail:
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("2. Unexpectedly connected as: " + connectionURL);
            assertTrue("2. Unexpectedly connected as: " + connectionURL, false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG(msg);
            BaseJDBCTestCase.assertSQLState(
                "2. Unexpected SQLException: " + msg,
                LOGIN_FAILED,
                se);
        }
        
    }

    private void _testPreStartedMaster()
    throws Exception
    {
        Connection conn = null;
        String db = masterDatabasePath +FS+ReplicationRun.masterDbSubPath +FS+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";startMaster=true"
                + ";slaveHost=" + slaveServerHost 
                + ";slavePort=" + slaveReplPort;
        util.DEBUG("_testPreStartedMaster: " + connectionURL);
        // First StartMaster connect ok:
        // Must use "full" startMaster - including wait-loop.
        
        // Better move this to testPostStartedMasterAndSlave!
        startMaster(jvmVersion, replicatedDb,
                masterServerHost, // Where the startMaster command must be given
                masterServerPort, // master server interface accepting client requests
                masterServerHost, // An interface on the master: masterClientInterface (==masterServerHost),
                slaveServerPort, // Not used since slave don't allow clients.
                slaveServerHost, // for slaveReplInterface
                slaveReplPort);
        /* REMOVE!
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            assertTrue("1. Unexpected!: startMaster Successfully connected as: " 
                    + connectionURL, false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG(msg);
            util.DEBUG("1. startMaster: No connection as expected: " + msg);
            BaseJDBCTestCase.assertSQLState(
                "1. Unexpected SQLException: " + msg,
                REPLICATION_CONNECTION_EXCEPTION,
                se);
        }
        */
        
        util.DEBUG("2. startMaster attempt should fail on: " + connectionURL);
        // util.DEBUG("********************'' 2. CURRENTLY HANGS!!!! Skipping.");
        // if (false) // FIXME! ENABLE WHEN DERBY-3358 COMMITTED!
        {
        // A 2. StartMaster connect should fail:
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("2. Unexpectedly connected as: " + connectionURL);
            assertTrue("2. Unexpectedly connected as: " + connectionURL, false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG("2. startMaster No connection as expected: " + msg);
            BaseJDBCTestCase.assertSQLState(
                "2. Unexpected SQLException: " + msg,
                REPLICATION_MASTER_ALREADY_BOOTED,
                se);
        }
        }
        
        // A 2. StartSlave connect should fail:
        util.DEBUG("startSlave attempt should fail on: " + connectionURL);
        db = slaveDatabasePath +FS+ReplicationRun.slaveDbSubPath +FS+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";startSlave=true"
                + ";slaveHost=" + slaveServerHost 
                + ";slavePort=" + slaveReplPort;
        util.DEBUG(connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("3. startSlave Unexpectedly connected as: " + connectionURL);
            assertTrue("3. startSlave Unexpectedly connected as: " + connectionURL, false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            util.DEBUG("3. startSlave No connection as expected: " + msg);
            BaseJDBCTestCase.assertSQLState(
                "3. Unexpected SQLException: " + msg,
                CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE,
                se);
        }
    }
    
}
