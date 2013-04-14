/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local
 
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

import java.sql.SQLException;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * 
 */

public class ReplicationRun_Local extends ReplicationRun
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local(String testcaseName)
    {
        super(testcaseName);
    }
    
    /**
     * Creates a new instance of ReplicationRun_Local running with authentication.
     */
    public ReplicationRun_Local(String testcaseName, String user, String password )
    {
        super( testcaseName, user, password );
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("ReplicationRun_Local Suite");
        
        suite.addTestSuite( ReplicationRun_Local.class );
        
        return SecurityManagerSetup.noSecurityManager(suite);

    }
    
    public static Test localAuthenticationSuite()
    {
        String      user = "KIWI";
        String      password = "KIWI_password";
        TestSuite suite = new TestSuite("ReplicationRun_Local Suite Local Authentication Suite");

        suite.addTest( new ReplicationRun_Local( "testReplication_Local_TestStraightReplication", user, password ) );
        suite.addTest( new ReplicationRun_Local( "testReplication_Local_LogFilesSynched", user, password ) );

        return SecurityManagerSetup.noSecurityManager( suite );
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
    
    /**
     * Test the "normal" replication scenario:
     * Load on the master db while replicating to slave db,
     * then verify that slave db is correct after failover.
     * @throws java.lang.Exception
     */
    public void testReplication_Local_TestStraightReplication()
    throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
        NetworkServerTestSetup.waitForAvailablePort(masterServerPort);
        NetworkServerTestSetup.waitForAvailablePort(slaveServerPort);
        NetworkServerTestSetup.waitForAvailablePort(slaveReplPort);

        startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES, // masterServerHost, // "0.0.0.0", // All. or use masterServerHost for interfacesToListenOn,
                masterServerPort,
                masterDbSubPath); // Distinguishing master/slave
        
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
        
        initSlave(slaveServerHost,
                jvmVersion,
                replicatedDb); // Trunk and Prototype V2: copy master db to db_slave.
        
        startSlave(jvmVersion, replicatedDb,
                slaveServerHost, // slaveClientInterface // where the slave db runs
                slaveServerPort,
                slaveServerHost, // for slaveReplInterface
                slaveReplPort,
                testClientHost);
        
        startMaster(jvmVersion, replicatedDb,
                masterServerHost, // Where the startMaster command must be given
                masterServerPort, // master server interface accepting client requests
                masterServerHost, // An interface on the master: masterClientInterface (==masterServerHost),
                slaveServerPort, // Not used since slave don't allow clients.
                slaveServerHost, // for slaveReplInterface
                slaveReplPort);
        
        
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
        // As of 2008-02-06 master does not accept shutdown after replication, so:
        // do a 'kill pid' after ending the test run
        
    }

    /**
     * DERBY-3382: Test that start replication fails if master db is updated
     * after copying the db to the slave location
     * @throws java.lang.Exception on test errors.
     */
    public void testReplication_Local_LogFilesSynched() throws Exception {

        cleanAllTestHosts();
        initEnvironment();
        initMaster(masterServerHost, replicatedDb);

        startServer(masterJvmVersion,
                                   derbyMasterVersion,
                                   masterServerHost,
                                   ALL_INTERFACES,
                                   masterServerPort,
                                   masterDbSubPath);
        startServer(slaveJvmVersion,
                                  derbySlaveVersion,
                                  slaveServerHost,
                                  ALL_INTERFACES,
                                  slaveServerPort,
                                  slaveDbSubPath);

        startServerMonitor(slaveServerHost);

        bootMasterDatabase(jvmVersion,
                           masterDatabasePath + FS + masterDbSubPath,
                           replicatedDb,
                           masterServerHost,
                           masterServerPort,
                           null);

        // copy db to slave
        initSlave(slaveServerHost,
                  jvmVersion,
                  replicatedDb);

        // database has now been copied to slave. Updating the master
        // database at this point will cause unsynced log files
        executeOnMaster("call syscs_util.syscs_unfreeze_database()");
        executeOnMaster("create table breakLogSynch (v varchar(20))");
        executeOnMaster("drop table breakLogSynch");

        // startSlave is supposed do fail. We check the sql state in
        // assertSqlStateSlaveConn below
        startSlave(jvmVersion, replicatedDb,
                   slaveServerHost,
                   slaveServerPort,
                   slaveServerHost,
                   slaveReplPort,
                   testClientHost);

        SQLException sqlexception = null;
        try {
            startMaster(jvmVersion, replicatedDb,
                        masterServerHost,
                        masterServerPort,
                        masterServerHost,
                        slaveServerPort,
                        slaveServerHost,
                        slaveReplPort);
        } catch (SQLException sqle) {
            sqlexception = sqle;
        }
        // the startMaster connection attempt should fail with exception XRE05
        if (sqlexception == null) {
            fail("Start master did not get the expected SQL Exception XRE05");
        } else {
            BaseJDBCTestCase.assertSQLState("Unexpected SQL state.",
                                            "XRE05",
                                            sqlexception);
        }

        // The startSlave connection attempt should fail with exception XJ040
        assertSqlStateSlaveConn("XJ040");

        stopServer(jvmVersion, derbyVersion,
                   masterServerHost, masterServerPort);
        stopServer(jvmVersion, derbyVersion,
                   slaveServerHost, slaveServerPort);

    }

    
}
