/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun
 
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

public class ReplicationRun_Full extends ReplicationRun
{
    
    public ReplicationRun_Full(String testcaseName)
    {
        super(testcaseName);
        
        LF = System.getProperties().getProperty("line.separator");
    }
        
    public static Test suite()
    {
        
        TestSuite suite = new TestSuite("Replication_Full Suite");
        
        suite.addTestSuite( ReplicationRun_Full.class );
        
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
    //// b)  Running (positive and negative) tests at the various states 
    ////     of replication to test what is and is not accepted compared to
    ////     the functional specification.
    //// c)  Adding additional load on master and slave servers in 
    ////     different states of replication.
    ////
    //////////////////////////////////////////////////////////////
    
    public void testReplication()
    throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        /* 'testReplication' steps through all states of the
         * replication process.
         * Tests required to be run in these states
         * should be specified in the replicationtest.properties file.
         */
        if ( runUnReplicated )  // test.runUnReplicated
        {
            util.DEBUG("**** BEGIN Running test without replication.");
            initMaster(masterServerHost,
                    replicatedDb);
            startServer(masterJvmVersion, derbyVersion, // No replication
                    masterServerHost,
                    ALL_INTERFACES, // masterServerHost, // "0.0.0.0", // All. or use masterServerHost for interfacesToListenOn,
                    masterServerPort,
                    masterDbSubPath); // Distinguishing master/slave
            runTest(replicationTest,
                    jvmVersion,
                    testClientHost,
                    masterServerHost, masterServerPort,
                    replicatedDb);
            stopServer(masterJvmVersion, derbyMasterVersion,
                    masterServerHost, masterServerPort);
            util.DEBUG("**** END Running test without replication.");
            // util.sleep(5000L, "End of runUnReplicated"); // Just for testing....
        }
        
        ///////////////////////////////////////////////////////
        // State: PreStartedMasterServer, PreStartedSlaveServer
            if (state.testPreStartedMasterServer()) return;
                
        initMaster(masterServerHost,
                replicatedDb); // Prototype V2: copy orig (possibly empty) db to db_master.
        
        masterServer = startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES, // masterServerHost, // "0.0.0.0", // All. or use masterServerHost for interfacesToListenOn,
                masterServerPort,
                masterDbSubPath); // Distinguishing master/slave
        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PreStartedSlaveServer
        
        startOptionalLoad(masterPreRepl,
                masterDbSubPath,
                masterServerHost,
                masterServerPort);
        
            if (state.testPreStartedSlaveServer()) return; // + stop master server!
        
        // Thread.sleep(5000L); // Just for testing....
        slaveServer = startServer(slaveJvmVersion, derbySlaveVersion,
                slaveServerHost,
                ALL_INTERFACES, // slaveServerHost, // "0.0.0.0", // All. or use slaveServerHost for interfacesToListenOn,
                slaveServerPort,
                slaveDbSubPath); // Distinguishing master/slave
        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PostStartedSlaveServer
        //        PreStartedMaster,        PreStartedSlave
        
        // Thread.sleep(15000L); // Just for testing....
        startServerMonitor(slaveServerHost);
        
        xFindServerPID(slaveServerHost, slaveServerPort); // JUST DEBUGGING!
        
        bootMasterDatabase(jvmVersion,
                masterDatabasePath +FS+ masterDbSubPath,
                replicatedDb,
                masterServerHost, // Where the startreplication command must be given
                masterServerPort, // master server interface accepting client requests
                null // bootLoad, // The "test" to start when booting db.
                );
        
        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PostStartedSlaveServer
        //        PostStartedMaster,       PreStartedSlave
        
        startOptionalLoad(masterPostRepl,
                masterDbSubPath,
                masterServerHost,
                masterServerPort);
        
        startOptionalLoad(slavePreSlave,
                slaveDbSubPath,
                slaveServerHost,
                slaveServerPort);
        
        // util.sleep(sleepTime, "Before initSlave"); // A. 'Something wrong with the instants!' if removed!
        // 5secs is too little! 15secs is too little sometimes...!
        // 30secs is too little w/ShutdownSlave!
        
        
            if (state.testPreInitSlave()) return;

        initSlave(/*slaveHost*/ slaveServerHost,
                jvmVersion,
                replicatedDb); // Trunk and Prototype V2: copy master db to db_slave.
        

            if (state.testPreStartedSlave()) return;
                
         startSlave(jvmVersion, replicatedDb,
                slaveServerHost, // slaveClientInterface // where the slave db runs
                slaveServerPort,
                slaveServerHost, // for slaveReplInterface
                slaveReplPort,
                testClientHost);

            if (state.testPreStartedMaster()) return;
        
       startMaster(jvmVersion, replicatedDb,
                masterServerHost, // Where the startMaster command must be given
                masterServerPort, // master server interface accepting client requests
                masterServerHost, // An interface on the master: masterClientInterface (==masterServerHost),
                slaveServerPort, // Not used since slave don't allow clients.
                slaveServerHost, // for slaveReplInterface
                slaveReplPort);
        
        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PostStartedSlaveServer
        //        PostStartedMaster,       PostStartedSlave
        
        startOptionalLoad(masterPostSlave,
                masterDbSubPath,
                masterServerHost,
                masterServerPort);
        
        startOptionalLoad(slavePostSlave,
                slaveDbSubPath,
                slaveServerHost,
                slaveServerPort);
        
        // Thread.sleep(5000L); // Just for testing....
        // util.sleep(10000L, "Before runTest"); // Perf. testing....
        
            if (state.testPostStartedMasterAndSlave()) return;
            // Could be run concurrently with runTest below?
        
        // Used to run positive tests? Handle negative testing in State.testPostStartedMasterAndSlave()?
        // Observe that it will not be meaningful to do runTest if State.XXXX() 
        // has led to incorrect replication state wrt. replicationTest.
        runTest(replicationTest, // Returns immediatly if replicationTest is null.
                jvmVersion,
                testClientHost,
                masterServerHost, masterServerPort,
                replicatedDb);
        
        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PostStartedSlaveServer
        //        PostStartedMaster,       PostStartedSlave
        //        PreStoppedMaster,        PreStoppedSlave
            if (state.testPreStoppedMaster()) return;
        
// PoC        stopMaster(replicatedDb); // v7: RENAMED! FIXME! when 'stopMaster' cmd. available! // master..
/*         stopMaster_ij(jvmVersion, replicatedDb,
                masterServerHost,
                masterServerPort,
                testClientHost);
 */
        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PostStartedSlaveServer
        //        PostStartedMaster,       PostStartedSlave
        //        PostStoppedMaster,       PreStoppedSlave,       PreFailover
        
        // Thread.sleep(5000L); // Just for testing....
        
            if (state.testPreStoppedMasterServer()) return;
        
/* PoC        stopServer(masterJvmVersion, derbyMasterVersion, // v7: NA // PoC V2b: forces failover on slave
                masterServerHost, masterServerPort);
 */
        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PostStartedSlaveServer
        //        PostStartedMaster,       PostStartedSlave
        //        PostStoppedMaster,       PreStoppedSlave
        //        PostStoppedMasterServer, PreStoppedSlaveServer,  PoC:  PostFailover
            if (state.testPreStoppedSlave()) return;
        
        // Thread.sleep(5000L); // Just for testing....
// PoC        stopSlave(replicatedDb); // v7: NEW! FIXME! when 'stopSlave' cmd. available!
/*         stopSlave_ij(jvmVersion, replicatedDb,
                slaveServerHost,
                slaveServerPort,
                testClientHost);
 */
        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PostStartedSlaveServer
        //        PostStartedMaster,       PostStartedSlave
        //        PostStoppedMaster,       PostStoppedSlave
        //        PostStoppedMasterServer, PreStoppedSlaveServer,  PreFailover // PoC:  PostFailover
        
        // Thread.sleep(5000L); // Just for testing....
        failOver(jvmVersion, // On master, which is the normal case.
            masterDatabasePath, masterDbSubPath, replicatedDb,
            masterServerHost,  // Where the master db is run.
            masterServerPort,
            testClientHost);

// PoC        failOver(replicatedDb); // FIXME! when 'failOver' cmd. available!

        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PostStartedSlaveServer
        //        PostStartedMaster,       PostStartedSlave
        //        PostStoppedMaster,       PostStoppedSlave
        //        PostStoppedMasterServer, PreStoppedSlaveServer,  PostFailover
        
        // util.sleep(10000L, "After failover"); // Try to avoid DERBY-3463....
        connectPing(slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb,
                slaveServerHost,slaveServerPort,
                testClientHost);
        
        // If the slave server was shutdown, killed or died, start a "default" server on
        // the same server and port to do the verification:
        int slavePid = xFindServerPID(slaveServerHost, slaveServerPort);
        if ( slavePid == -1 )
        {
            util.DEBUG("WARNING: slave server not available. Starting.");
            slaveServer = startServer(jvmVersion, derbyVersion,
                    slaveServerHost,
                    ALL_INTERFACES, // slaveServerHost, // "0.0.0.0", // All. or use slaveServerHost for interfacesToListenOn,
                    slaveServerPort,
                    slaveDbSubPath); // Distinguishing master/slave
        }
        /* BEGIN Failover do not yet clean replication mode on slave! Must restart the server!*/
        else{
          if (true)
          {
            util.DEBUG("*********************** DERBY-3205/svn 630806. failover does now unset replication mode on slave.");
          }
          else // failover does not unset replication mode on slave.
          {
            util.DEBUG("*********************** DERBY-3205. failover does not unset replication mode on slave.");
            /* That also blocks for connecting!:
            // Do slave db shutdown (and reconnect) to unset... PRELIMINARY!!!
            shutdownDb(slaveServerHost,slaveServerPort,
                    slaveDatabasePath +FS+ slaveDbSubPath,replicatedDb);
             */
            /* */
            restartServer(jvmVersion, derbyVersion, // restart server is too strong!
                    slaveServerHost,
                    ALL_INTERFACES,
                    slaveServerPort,
                    slaveDbSubPath); // Distinguishing master/slave
            /* */
          }
        }/* END */
        
        verifySlave();
        
        // We should verify the master as well, at least to see that we still can connect.
        // If the slave server was shutdown, killed or died, start a "default" server on
        // the same server and port to do the verification:
        int masterPid = xFindServerPID(masterServerHost, masterServerPort);
        if ( masterPid == -1 )
        {
            util.DEBUG("WARNING: master server not available. Starting.");
            masterServer = startServer(jvmVersion, derbyVersion,
                    masterServerHost,
                    ALL_INTERFACES, // masterServerHost, // "0.0.0.0", // All. or use slaveServerHost for interfacesToListenOn,
                    masterServerPort,
                    masterDbSubPath); // Distinguishing master/slave
        }
        verifyMaster(); // NB NB Hangs here with localhost/ReplicationTestRun!
        
        xFindServerPID(slaveServerHost, slaveServerPort); // JUST DEBUGGING!
        
        // Thread.sleep(5000L); // Just for testing....
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        ///////////////////////////////////////////////////////
        // State: PostStartedMasterServer, PostStartedSlaveServer
        //        PostStartedMaster,       PostStartedSlave
        //        PostStoppedMaster,       PostStoppedSlave
        //        PostStoppedMasterServer, PostStoppedSlaveServer,  PostFailover
        
            if (state.testPostStoppedSlaveServer()) return;
        
        // Shutdown master:
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        // As of 2008-02-06 master does not accept shutdown after replication, so:
        masterPid = xFindServerPID(masterServerHost, masterServerPort);
        if ( masterPid != -1 )
        {
        util.DEBUG("*********************** DERBY-3394. master does not accept shutdown after failover.");
        killMaster(masterServerHost,masterServerPort);
        }
        
    }


    
}
