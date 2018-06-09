/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_3_p1
 
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
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * Test that
 * - stopSlave is not accepted on replicating slave,
 * - stopSlave is not accepted on replicating master,
 * - stopMaster is accepted on replicating master,
 * - stopSlave is not accepted on non-replicating slave host,
 * - failOver is not accepted on non-replicating master host.
 * 
 */

public class ReplicationRun_Local_3_p1 extends ReplicationRun_Local_3
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_3_p1(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
    {
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Local_3_p1 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_3_p1.class        );
        
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
    
    public void testReplication_Local_3_p1_StateNegativeTests()
    throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
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
        
        // 4. separate test
        // master db created...
        // slave: connect 'startSlave=true;create=true'
        assertException(
        _startSlaveTrueAndCreateTrue(slaveServerHost, slaveServerPort,
            masterDatabasePath +FS+ masterDbSubPath +FS+ replicatedDb),
            "XRE10"); // REPLICATION_CONFLICTING_ATTRIBUTES // OK to continue.
        
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
        
        replicationTest = null; // Used as a flag to verifyMaster and verifySlave!
        runTest(replicationTest, // Returns immediatly if replicationTest is null.
                jvmVersion,
                testClientHost,
                masterServerHost, masterServerPort,
                replicatedDb);
        
        // 1. separate test
        // slave: stopSlave
        assertException(
            stopSlave(slaveServerHost,
                      slaveServerPort,
                      slaveDatabasePath,
                      replicatedDb,
                      true),
            "XRE41"); // SLAVE_OPERATION_DENIED_WHILE_CONNECTED // OK to continue
       
        // 2. separate test
        // master: stopSlave
        // master: stopMaster
        // slave: stopSlave
        assertException(
            stopSlave(masterServerHost,
                      masterServerPort,
                      masterDatabasePath,
                      masterDbSubPath,
                      replicatedDb,
                      true),
            "XRE40"); //  REPLICATION_NOT_IN_SLAVE_MODE // OK to continue
        assertException(
            _stopMaster(masterServerHost, masterServerPort,
                masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb),
            null); // Implies failover. // OK to continue. We have failover.
        /* showCurrentState("Post stopMaster", 0L,
            slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
            slaveServerHost, slaveServerPort);
        showCurrentState("Post stopMaster +1s", 1000L,
            slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
            slaveServerHost, slaveServerPort); */
        waitForConnect(100L, 10, 
                slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
                slaveServerHost, slaveServerPort);
        assertException(
            stopSlave(slaveServerHost,
                      slaveServerPort,
                      slaveDatabasePath,
                      replicatedDb,
                      true),
            "XRE40"); // REPLICATION_NOT_IN_SLAVE_MODE // OK to continue
        /* showCurrentState("Post stopMaster, stopSlave", 0L,
            slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
            slaveServerHost, slaveServerPort);
        showCurrentState("Post stopMaster, stopSlave +1s", 1000L,
            slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
            slaveServerHost, slaveServerPort); */
        /* showCurrentState("Post stopMaster, stopSlave +1s", 0L,
            masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb, 
            masterServerHost, masterServerPort); */
        waitForConnect(100L, 10, 
                masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb, 
                masterServerHost, masterServerPort);
        
        
        /* BEGIN In ReplicationRun_Local_3_p2.java:
        // 3. separate test
        // stopMaster
        // failover on slave
        
        // 5. separate test
        // slave: "normal" connect to slave db
        
        // 6. separate test
        // slave: 'internal-stopslave=true'
        END */
        
        /* failOver(jvmVersion,
                masterDatabasePath, masterDbSubPath, replicatedDb,
                masterServerHost,  // Where the master db is run.
                masterServerPort,
                testClientHost);
        */
        assertException(
            _failOver(masterServerHost, masterServerPort, 
                masterDatabasePath+FS+masterDbSubPath+FS+replicatedDb),
            "XRE07"); // REPLICATION_NOT_IN_MASTER_MODE
        
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
    
}
