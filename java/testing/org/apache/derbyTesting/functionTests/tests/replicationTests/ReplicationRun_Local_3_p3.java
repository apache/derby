/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_3_p3
 
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

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * 
 */

public class ReplicationRun_Local_3_p3 extends ReplicationRun_Local_3
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_3_p3(String testcaseName)
    {
        super(testcaseName);
    }
    
    protected void setUp() throws Exception
    {
        super.setUp();
    }
    
    protected void tearDown() throws Exception
    {
        super.tearDown();
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("ReplicationRun_Local_3_p3 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_3_p3.class    );
        
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
    
    public void testReplication_Local_3_p3_StateNegativeTests()
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
        
        /* In ReplicationRun_Local_3_p1
        // 4 separate test
        // master db created...
        // slave: connect 'startSlave=true;create=true'
         */
        
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
        
        runTest(null, // Returns immediatly if replicationTest is null.
                jvmVersion,
                testClientHost,
                masterServerHost, masterServerPort,
                replicatedDb);
        
        /* In ReplicationRun_Local_3_p1
        // 1 separate test
        // slave: stopSlave
       
        // 2 separate test
        // master: stopSlave
        // master: stopMaster
        // slave: stopSlave
         */
        
        // 3 separate test
        // stopMaster
        // failover on slave
        assertException(
            _stopMaster(masterServerHost, masterServerPort,
                masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb),
            null); // Implies failover. // OK to continue.
        /* showCurrentState("Post stopMaster +1s", 1000L,
            masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb, 
            masterServerHost, masterServerPort); */
        waitForConnect(100L, 10, 
                masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb, 
                masterServerHost, masterServerPort);
        /* showCurrentState("Post stopMaster +1s", 0L,
            slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
            slaveServerHost, slaveServerPort);
        showCurrentState("Post stopMaster +5s", 5000L,
            slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
            slaveServerHost, slaveServerPort); */
        waitForSQLState("08004", 1000L, 20, // 08004.C.7 - CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE
                slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
                slaveServerHost, slaveServerPort);
        /* Got it above... showCurrentState("Post stopMaster +30s", 30000L,
            slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
            slaveServerHost, slaveServerPort); // 08004 */
        /* Got it above... showCurrentState("Post stopMaster +60s", 30000L,
            masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb, 
            masterServerHost, masterServerPort); // CONNECTED */
        assertException(
            _failOver(masterServerHost, masterServerPort, 
                masterDatabasePath+FS+masterDbSubPath+FS+replicatedDb),
            "XRE07");
        /* _p2: assertException(
            _failOver(slaveServerHost, slaveServerPort, 
                slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb),
                "XRE07"); // Hangs!? even after killMaster server. */
        
        // 5 separate test
        // slave: "normal" connect to slave db
        
        // 6 separate test
        // slave: 'internal-stopslave=true'
        
        /* failOver(jvmVersion,
                masterDatabasePath, masterDbSubPath, replicatedDb,
                masterServerHost,  // Where the master db is run.
                masterServerPort,
                testClientHost); //  XRE07 Could not perform operation because the database is not in replication master mode.
        */
        
        waitForSQLState("08004", 1000L, 20, // 08004.C.7 - CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE
                slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb,
                slaveServerHost, slaveServerPort); // _failOver above fails...
        /*
        connectPing(slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb,
        slaveServerHost,slaveServerPort,
        testClientHost); // 
         */
        
        // Not relevant as we  can not connect. verifySlave();
        
        // We should verify the master as well, at least to see that we still can connect.
        verifyMaster();
        
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
    }
    
}
