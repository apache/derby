/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_3_p4
 
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
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derby.client.ClientDataSourceInterface;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Verify that "internal_stopslave=true" is
 * NOT accepted as user supplied connection attr.
 * 
 */


public class ReplicationRun_Local_3_p4 extends ReplicationRun
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_3_p4(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Local_3_p4 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_3_p4.class        );
        
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
    
    /**
     * Test to verify that "internal_stopslave=true" is
     * NOT accepted as connection attr. on the slave.
     * "internal_stopslave=true" is for internal use only.
     * @throws java.lang.Exception
     */
    public void testReplication_Local_3_p4_StateNegativeTests()
    throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5729
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        replicationTest = null; // Used as a flag to verifyMaster and verifySlave!
        runTest(replicationTest, // Returns immediatly if replicationTest is null.
                jvmVersion,
                testClientHost,
                masterServerHost, masterServerPort,
                replicatedDb);
                
        
        // 5 separate test
        // slave: "normal" connect to slave db
        assertException(
            _connectToSlave(slaveServerHost, slaveServerPort,
                slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb),
            "08004");
        
        // 6 separate test
        // slave: 'internal_stopslave=true'
        assertException(
            _internal_stopSlave(slaveServerHost, slaveServerPort,
                slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb),
                "XRE43"); // REPLICATION_STOPSLAVE_NOT_INITIATED 
        // -  Unexpected error when trying to stop replication slave mode. To stop repliation slave mode, use operation 'stopSlave' or 'failover'.
        
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
    
    private SQLException _connectToSlave(String slaveServerHost, 
            int slaveServerPort, 
            String dbPath) throws Exception {
        util.DEBUG("_connectToSlave");
        ClientDataSourceInterface ds;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientDataSource");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientDataSource40");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        }

        ds.setDatabaseName(dbPath);
        ds.setServerName(slaveServerHost);
        ds.setPortNumber(slaveServerPort);
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        ds.setConnectionAttributes(useEncryption(false));
        try {
            Connection conn = ds.getConnection(); // 
            conn.close();
            return null; // If successfull - 
        } catch (SQLException se) {
            util.DEBUG(se.getErrorCode()+" "+se.getSQLState()+" "+se.getMessage());
            return se;
        }
    }

    private SQLException _internal_stopSlave(String slaveServerHost, 
            int slaveServerPort, 
            String dbPath) throws Exception {
        util.DEBUG("_internal_stopSlave");
        ClientDataSourceInterface ds;

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientDataSource");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientDataSource40");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        }

        ds.setDatabaseName(dbPath);
        ds.setServerName(slaveServerHost);
        ds.setPortNumber(slaveServerPort);
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        ds.setConnectionAttributes("internal_stopslave=true"
                +useEncryption(false));
        try {
            Connection conn = ds.getConnection(); // 
            conn.close();
            return null; // If successfull - 
        } catch (SQLException se) {
            util.DEBUG(se.getErrorCode()+" "+se.getSQLState()+" "+se.getMessage());
            return se;
        }
    }

    
}
