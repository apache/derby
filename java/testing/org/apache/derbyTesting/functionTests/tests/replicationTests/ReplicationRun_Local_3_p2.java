/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_3_p2
 
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

public class ReplicationRun_Local_3_p2 extends ReplicationRun_Local_3
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_3_p2(String testcaseName)
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
        TestSuite suite = new TestSuite("ReplicationRun_Local_3_p2 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_3_p2.class  );
        
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
    
    public void replication_Local_3_p2_StateTests(boolean bigInsert,
            boolean immediateStopMaster) // no sleep between startMaster and stopMaster.
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
        
        /* runTest(null, // Returns immediatly if replicationTest is null.
                jvmVersion,
                testClientHost,
                masterServerHost, masterServerPort,
                replicatedDb); */
        String masterDb = masterDatabasePath+FS+masterDbSubPath+FS+replicatedDb;
        // boolean bigInsert = true;
        // boolean immediateStopMaster = false; // no sleep between startMaster and stopMaster.
        int tupsInserted = (bigInsert)?9876:10; 
        _testInsertUpdateDeleteOnMaster(masterServerHost, masterServerPort, 
                masterDb, tupsInserted);
        
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
        if ( ! immediateStopMaster) {
            util.DEBUG("sleep(10000L)");
            Thread.sleep(10000L); // Do we try stopMaster too "close" to startmaster/startSlave?
        }
        assertException(
            _stopMaster(masterServerHost, masterServerPort,
                masterDb),
            null); // Implies failover. // OK to continue.
        // Appears that failover is NOT done when bigInsert==false && immediateStopMaster== true:
        // See below.
        
        waitForConnect(100L, 200, 
                masterDb, 
                masterServerHost, masterServerPort);
        
        String slaveDb = slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb;
        if ( bigInsert==false && immediateStopMaster== true ) // UNEXPECTED BEHAVIOUR
        { // ..._smallInsert_immediateStopMaster()
            waitForSQLState("08004", 100L, 200, // Doing this to reach failover below also in this case! But will fail there!
                    slaveDb, 
                    slaveServerHost, slaveServerPort);
        }else 
        { // Correctly gets connection, i.e. failover has happened.
            waitForConnect(100L, 200,
                    slaveDb,
                    slaveServerHost, slaveServerPort);   
        
            // Only if we can connect:
            _verifyDatabase(slaveServerHost, slaveServerPort, slaveDb,
                    tupsInserted); // Will all tuples be transferred to slave here?
        }

        // 5 separate test
        // slave: "normal" connect to slave db
        
        // 6 separate test
        // slave: 'internal-stopslave=true'
        
        String expected = "XRE07"; // REPLICATION_NOT_IN_MASTER_MODE is correct when failover did happen above w/stopMaster.
        if ( bigInsert==false && immediateStopMaster== true ) expected = null; // UNEXPECTED BEHAVIOUR: null or hang!
        assertException(
            _failOver(slaveServerHost, slaveServerPort, 
                slaveDb),
            expected);

        waitForConnect(100L, 200, 
                slaveDb, 
                slaveServerHost, slaveServerPort);
        
        connectPing(slaveDb,
                slaveServerHost,slaveServerPort,
                testClientHost); // 
        
        // verifySlave();
        _verifyDatabase(slaveServerHost, slaveServerPort, slaveDb,
            tupsInserted);
        
        // We should verify the master as well, at least to see that we still can connect.
        // verifyMaster();
        _verifyDatabase(masterServerHost, masterServerPort, masterDb,
            tupsInserted);
        
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
    }
    public void testReplication_Local_3_p2_StateTests_bigInsert_immediateStopMaster()
    throws Exception
    {
        replication_Local_3_p2_StateTests(true, true); 
    }
    public void testReplication_Local_3_p2_StateTests_smallInsert_immediateStopMaster_DISABLED()
    throws Exception
    {
        // FIXME! ENABLE when DERBY-3617 is RESOLVED - otherwise hangs....
        // ... Now gets connection instead of XRE07!
        // And then we experience hang again...
        // replication_Local_3_p2_StateTests(false, true);
    }
    public void testReplication_Local_3_p2_StateTests_bigInsert_sleepBeforeStopMaster()
    throws Exception
    {
        replication_Local_3_p2_StateTests(true, false);
    }
    public void testReplication_Local_3_p2_StateTests_smallInsert_sleepBeforeStopMaster()
    throws Exception
    {
        replication_Local_3_p2_StateTests(false, false);
    }
    
}
