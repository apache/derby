/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_Encrypted_1
 
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
 * Testing replication of encrypted databases.
 * Required DERBY-3890.
 * 
 */

public class ReplicationRun_Local_Encrypted_1 extends ReplicationRun
{
    
    public ReplicationRun_Local_Encrypted_1(String testcaseName)
    {
        
        super(testcaseName);

    }
    
    protected void setUp() throws Exception
    {
        super.setUp();
        dataEncryption = "bootPassword=dilldall"; // Run the tests with encryption.
    }
    
    protected void tearDown() throws Exception
    {
        dataEncryption = null;
        super.tearDown();
    }
    
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Local_Encrypted_1 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_Encrypted_1.class);
        
        return SecurityManagerSetup.noSecurityManager(suite);

    }
        
    /**
     * Do a simple test to verify replication can be performed
     * on an encrypted database.
     * @throws java.lang.Exception
     */
    public void testReplication_Encrypted_1_stdLoad()
    throws Exception
    {
        makeReadyForReplication();
        
        // Run a "load" on the master to make sure there
        // has been replication to slave.
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
        
        verifySlave(); // Starts slave and does a very simple verification.
        
        // We should verify the master as well, at least to see that we still can connect.
        verifyMaster();
        
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
    }
        
    /**
     * Verify connection to the encrypted, replicated database:
     * A) Database has not been shut down:
     * further connects from the same JVM will succeed
     * - connect without specifying encryption,
     * - blank password,
     * - incorrect password.
     * B) After shutdown:
     * Re-connects without correct password will fail
     * - re-connecting as if un-encrypted,
     * - blank password,
     * - incorrect password.
     * @throws java.lang.Exception
     */
    public void testReplication_Encrypted_1_miniLoad_negative()
    throws Exception
    {
        makeReadyForReplication();
                
        // Replication "load"
        String dbPath = masterDatabasePath + FS + masterDbSubPath + FS +
                replicatedDb;
        
        int tuplesToInsert = 10000;
        _testInsertUpdateDeleteOnMaster(masterServerHost, masterServerPort, 
                dbPath, tuplesToInsert);

        
        failOver(jvmVersion,
                masterDatabasePath, masterDbSubPath, replicatedDb,
                masterServerHost,  // Where the master db is run.
                masterServerPort,
                testClientHost);
                
        connectPing(slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb,
                slaveServerHost,slaveServerPort,
                testClientHost);
        
        // verifySlave();
        String slaveDbPath = slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb;
        _verifyDatabase(slaveServerHost, slaveServerPort,
            slaveDbPath, tuplesToInsert);
        
        // We should verify the master as well, 
        // at least to see that we still can connect.
        // verifyMaster();
        String masterDbPath = masterDatabasePath +FS+masterDbSubPath +FS+ replicatedDb;
        _verifyDatabase(masterServerHost, masterServerPort,
            masterDbPath, tuplesToInsert);
        
        // Since the db has not been shutdown after the correct connect
        // further connects from the same JVM will succeed
        dataEncryption = null;
        assertException(_connectToSlave(slaveServerHost, slaveServerPort,
            slaveDbPath),
            null);
        
        // try connecting with blank password
        dataEncryption = "bootPassword=;";
        assertException(_connectToSlave(slaveServerHost, slaveServerPort,
            slaveDbPath),
            null);
        
        // try connecting with wrong password
        dataEncryption = "bootPassword=dill2dall";
        assertException(_connectToSlave(slaveServerHost, slaveServerPort,
            slaveDbPath),
            null);
        
        // Shutdown the db to test reconnect with incorrect passwords
        shutdownDb(jvmVersion,
            slaveServerHost, slaveServerPort,
            slaveDatabasePath+FS+slaveDbSubPath, replicatedDb,
            testClientHost);
        
        // Negative test - try connecting without password
        dataEncryption = null;
        assertException(_connectToSlave(slaveServerHost, slaveServerPort,
            slaveDbPath),
            "XJ040");
        
        // Negative test - try connecting with blank password
        dataEncryption = "bootPassword=;";
        assertException(_connectToSlave(slaveServerHost, slaveServerPort,
            slaveDbPath),
            "XJ040");
        
        // Negative test - try connecting with wrong password
        dataEncryption = "bootPassword=dill2dall";
        assertException(_connectToSlave(slaveServerHost, slaveServerPort,
            slaveDbPath),
            "XJ040");
        
        // Reset to correct passwd.
        dataEncryption = "bootPassword=dilldall";
        
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
    }
    
    SQLException _connectToSlave(String slaveServerHost, int slaveServerPort,
            String dbPath) 
        throws Exception
    {
        util.DEBUG("_connectToSlave");
        ClientDataSourceInterface ds;

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
        ds.setConnectionAttributes(useEncryption(false));
        try {
            Connection conn = ds.getConnection();
            conn.close();
            return null; // If successfull.
        } catch (SQLException se) {
            return se;
        }       
    }
}
