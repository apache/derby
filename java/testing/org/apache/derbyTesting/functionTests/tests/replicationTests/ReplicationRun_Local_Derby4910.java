/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_Derby4910
 
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
 * Test that the startSlave command doesn't fail if it takes more than a
 * second before the master attempts to connect to the slave. Regression test
 * case for DERBY-4910.
 */

public class ReplicationRun_Local_Derby4910 extends ReplicationRun
{
    
    /**
     * Creates a new instance of this test class.
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_Derby4910(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("Replication test - DERBY-4910");
        
        suite.addTestSuite( ReplicationRun_Local_Derby4910.class );
        
        return SecurityManagerSetup.noSecurityManager(suite);

    }

    /**
     * Test that a slave can wait a while for the master to connect without
     * timing out. The startSlave command used to time out after one second
     * before DERBY-4910.
     */
    public void testSlaveWaitsForMaster() throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
        startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost, ALL_INTERFACES, masterServerPort,
                masterDbSubPath);
        
        startServer(slaveJvmVersion, derbySlaveVersion,
                slaveServerHost, ALL_INTERFACES, slaveServerPort,
                slaveDbSubPath);
        
        startServerMonitor(slaveServerHost);
        
        bootMasterDatabase(jvmVersion,
                masterDatabasePath +FS+ masterDbSubPath, replicatedDb,
                masterServerHost, masterServerPort,
                null // bootLoad, // The "test" to start when booting db.
                );
        
        initSlave(slaveServerHost,
                jvmVersion,
                replicatedDb);
        
        startSlave(jvmVersion, replicatedDb,
                slaveServerHost, slaveServerPort,
                slaveServerHost, 
                slaveReplPort,
                testClientHost);

        // DERBY-4910: The slave used to time out after one second if the
        // master hadn't connected to it yet. Wait for three seconds before
        // starting the master to verify that this isn't a problem anymore.
        Thread.sleep(3000L);
        
        startMaster(jvmVersion, replicatedDb,
                masterServerHost, masterServerPort,
                masterServerHost,
                slaveServerPort, slaveServerHost,
                slaveReplPort);
        
        // Replication "load"
        String dbPath = masterDatabasePath + FS + masterDbSubPath + FS +
                replicatedDb;
        
        int tuplesToInsert = 10000;
        _testInsertUpdateDeleteOnMaster(masterServerHost, masterServerPort, 
                dbPath, tuplesToInsert);
        
        failOver(jvmVersion,
                masterDatabasePath, masterDbSubPath, replicatedDb,
                masterServerHost, masterServerPort,
                testClientHost);
        
        connectPing(slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb,
                slaveServerHost,slaveServerPort,
                testClientHost);
        
        // verifySlave();
        dbPath = slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb;
        _verifyDatabase(slaveServerHost, slaveServerPort,
            dbPath, tuplesToInsert);
        // We should verify the master as well, 
        // at least to see that we still can connect.
        // verifyMaster();
        dbPath = masterDatabasePath +FS+masterDbSubPath +FS+ replicatedDb;
        _verifyDatabase(masterServerHost, masterServerPort,
            dbPath, tuplesToInsert);
    }
}
