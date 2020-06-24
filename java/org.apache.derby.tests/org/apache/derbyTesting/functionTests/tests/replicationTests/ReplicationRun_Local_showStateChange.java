/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_showStateChange
 
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
 * This test is intended to be run separatly showing 
 * state change during a "normal" replication session.
 */

public class ReplicationRun_Local_showStateChange extends ReplicationRun
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_showStateChange(String testcaseName)
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Local_showStateChange Suite");
        
        suite.addTestSuite( ReplicationRun_Local_showStateChange.class  );
        
        return SecurityManagerSetup.noSecurityManager(suite);

    }
    
    public void testReplication_Local_showStateChange_showReplState()
    throws Exception
    {
        util.DEBUG(""); // Just to get a nicer print of showCurrentState()...
        
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5729
        startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost, ALL_INTERFACES, masterServerPort,
                masterDbSubPath);
        
        startServer(slaveJvmVersion, derbySlaveVersion,
                slaveServerHost, ALL_INTERFACES, slaveServerPort,
                slaveDbSubPath);
        
        final long L=0L;//1000L;
        final long S=0L;
        boolean outerPrintDebug = util.printDebug;
        util.printDebug = true;
        String masterDb = masterDatabasePath +FS+masterDbSubPath +FS+ replicatedDb;
        String slaveDb = slaveDatabasePath +FS+slaveDbSubPath +FS+ replicatedDb;
        showCurrentState("1 started servers",S, masterDb, masterServerHost, masterServerPort);
        showCurrentState("1 started servers",S, slaveDb, slaveServerHost, slaveServerPort);
        
        startServerMonitor(slaveServerHost);
        
        bootMasterDatabase(jvmVersion,
                masterDatabasePath +FS+ masterDbSubPath, replicatedDb,
                masterServerHost, masterServerPort,
                null // bootLoad, // The "test" to start when booting db.
                );
        showCurrentState("2 master booted",S, masterDb, masterServerHost, masterServerPort);
        showCurrentState("2 master booted",S, slaveDb, slaveServerHost, slaveServerPort);
        
        initSlave(slaveServerHost,
                jvmVersion,
                replicatedDb);
        showCurrentState("3 slave filled",S, masterDb, masterServerHost, masterServerPort);
        // Causes XRE09 'The database has already been booted' in startSlave - CORRECT?:
        // showCurrentState("3 slave filled",S, slaveDb, slaveServerHost, slaveServerPort);
        
        startSlave(jvmVersion, replicatedDb,
                slaveServerHost, slaveServerPort,
                slaveServerHost, 
                slaveReplPort,
                testClientHost);
        showCurrentState("4 slave started",S, masterDb, masterServerHost, masterServerPort);
        // HANGS! on ClientDataSource.getConnection:
        // showCurrentState("4 slave started",S, slaveDb, slaveServerHost, slaveServerPort);
        
        startMaster(jvmVersion, replicatedDb,
                masterServerHost, masterServerPort,
                masterServerHost,
                slaveServerPort, slaveServerHost,
                slaveReplPort);
        showCurrentState("5 master started",S, masterDb, masterServerHost, masterServerPort);
        showCurrentState("5 master started",S, slaveDb, slaveServerHost, slaveServerPort);
        
        // Replication "load"
        util.DEBUG("Running replication load.");
        int tuplesToInsert = 10000;
        _testInsertUpdateDeleteOnMaster(masterServerHost, masterServerPort, 
                masterDb, tuplesToInsert);
        
        failOver(jvmVersion,
                masterDatabasePath, masterDbSubPath, replicatedDb,
                masterServerHost, masterServerPort,
                testClientHost);
        waitForSQLState("08004", 
            10L, 100,
            slaveDb, slaveServerHost, slaveServerPort);
        showCurrentState("6 failover initiated",S, masterDb, masterServerHost, masterServerPort);
        showCurrentState("6 failover initiated",S, slaveDb, slaveServerHost, slaveServerPort);
        waitForConnect(100L, 200,
            slaveDb, slaveServerHost, slaveServerPort);
        showCurrentState("6 failover initiated + wait..",S, masterDb, masterServerHost, masterServerPort);
        showCurrentState("6 failover initiated + wait..",S, slaveDb, slaveServerHost, slaveServerPort);
        
        connectPing(slaveDb,
                slaveServerHost,slaveServerPort,
                testClientHost);
        
        showCurrentState("7 failover completed",S, masterDb, masterServerHost, masterServerPort);
        showCurrentState("7 failover completed",S, slaveDb, slaveServerHost, slaveServerPort);
        util.printDebug = outerPrintDebug;
        // verifySlave();
        _verifyDatabase(slaveServerHost, slaveServerPort,
            slaveDb, tuplesToInsert);
        // We should verify the master as well, at least to see that we still can connect.
        // verifyMaster();
        _verifyDatabase(masterServerHost, masterServerPort,
            masterDb, tuplesToInsert);
    }
    
}
