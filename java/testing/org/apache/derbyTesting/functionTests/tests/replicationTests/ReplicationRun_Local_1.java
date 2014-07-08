/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_local_1
 
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
 * 
 */

public class ReplicationRun_Local_1 extends ReplicationRun
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_1(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
    {
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Local_1 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_1.class );
        
        return SecurityManagerSetup.noSecurityManager(suite);

    }
    
    public void testReplication_Local_1_InsertUpdateDeleteOnMaster()
    throws Exception
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
