/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_StateTest_part1_1
 
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

public class ReplicationRun_Local_StateTest_part1_1 extends ReplicationRun
{
    final static String REPLICATION_DB_NOT_BOOTED              = "XRE11";
    final static String REPLICATION_NOT_IN_SLAVE_MODE          = "XRE40";
    final static String SLAVE_OPERATION_DENIED_WHILE_CONNECTED = "XRE41";
    final static String REPLICATION_SLAVE_SHUTDOWN_OK          = "XRE42";

    /**
     * Creates a new instance of ReplicationRun_Local_StateTest_part1
     * 
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_StateTest_part1_1(String testcaseName)
    {
        super(testcaseName);
    }
        
    public static Test suite()
    {
        TestSuite suite = new TestSuite("ReplicationRun_Local_StateTest_part1_1 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_StateTest_part1_1.class );
        
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
    
    public void testReplication_Local_StateTest_part1_1()
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
        

        _testPostStartedMasterAndSlave_StopSlave(); // Not in a state to continue.
                
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
    }

    private void _testPostStartedMasterAndSlave_StopSlave()
            throws Exception
    {
        // 1. stopSlave to slave with connection to master should fail.
        util.DEBUG("1. testPostStartedMasterAndSlave_StopSlave");

        assertException(
            stopSlave(slaveServerHost,
                      slaveServerPort,
                      slaveDatabasePath,
                      replicatedDb,
                      true),
            SLAVE_OPERATION_DENIED_WHILE_CONNECTED);

        // Default replication test sequence still OK.

        // 2. stopSlave to a master server should fail:
        util.DEBUG("2. testPostStartedMasterAndSlave_StopSlave");

        assertException(
            stopSlave(masterServerHost,
                      masterServerPort,
                      masterDatabasePath,
                      masterDbSubPath,
                      replicatedDb,
                      true),
            REPLICATION_NOT_IN_SLAVE_MODE);

        // Default replication test sequence still OK.

        // Replication should still be up.

        // Take down master - slave connection:
        killMaster(masterServerHost, masterServerPort);

        // 3.  stopSlave on slave should now result in an exception stating that
        //     the slave database has been shutdown. A master shutdown results
        //     in a behaviour that is similar to what happens when a stopMaster
        //     is called.
        util.DEBUG("3. testPostStartedMasterAndSlave_StopSlave");

        stopSlave(slaveServerHost,
                  slaveServerPort,
                  slaveDatabasePath,
                  replicatedDb,
                  false); // master server is dead

        // Default replication test sequence will NOT be OK after this point.

        // 4. Try a normal connection:
        String db = slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb;

        String connectionURL = "jdbc:derby:"
            + "//" + slaveServerHost + ":" + slaveServerPort + "/"
            + db;

        util.DEBUG("4. testPostStartedMasterAndSlave_StopSlave: " +
                   connectionURL);

        waitForConnect(100L, 10, 
                slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
                slaveServerHost, slaveServerPort);
    }
}
