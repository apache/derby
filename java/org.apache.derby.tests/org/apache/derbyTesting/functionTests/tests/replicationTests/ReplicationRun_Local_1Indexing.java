/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_1Indexing
 
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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derby.client.ClientDataSourceInterface;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * Verify that indexes are replicated.
 */

public class ReplicationRun_Local_1Indexing extends ReplicationRun
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_1Indexing(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Local_1Indexing Suite");
        
        suite.addTestSuite( ReplicationRun_Local_1Indexing.class    );
        
        return SecurityManagerSetup.noSecurityManager(suite);

    }
    
    /**
     * Verify that indexes created on master before failover
     * are available in slave database after failover.
     * @throws java.lang.Exception
     */
    public void testReplication_Local_1_Indexing()
    throws Exception
    {
        makeReadyForReplication();
        
        // Replication "load"
        String masterDbPath = masterDatabasePath + FS + masterDbSubPath + FS +
                replicatedDb;
        
        int tuplesToInsert = 10000;
        executeOnMaster("create table t(i integer primary key, s varchar(64), ii integer)");
        
        executeOnMaster("create index index1ii on t(ii)");
        executeOnMaster("create index index1s on t(s)");
        int tuplesInserted = 0;
        
        _fillTableOnServer(masterServerHost, masterServerPort, 
                masterDbPath, tuplesInserted, tuplesToInsert);
        tuplesInserted = tuplesInserted + tuplesToInsert;
        
        executeOnMaster("drop index index1ii");
        executeOnMaster("drop index index1s");
        
        executeOnMaster("create index index2ii on t(ii)");
        executeOnMaster("create index index2s on t(s)");
        
        _fillTableOnServer(masterServerHost, masterServerPort, 
                masterDbPath, tuplesToInsert, tuplesToInsert);
        tuplesInserted = tuplesInserted + tuplesToInsert;
        
        failOver(jvmVersion,
                masterDatabasePath, masterDbSubPath, replicatedDb,
                masterServerHost, masterServerPort,
                testClientHost);
        
        connectPing(slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb,
                slaveServerHost,slaveServerPort,
                testClientHost);
        
        // verifySlave();
        String slaveDbPath = slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb;
        _verifyDatabase(slaveServerHost, slaveServerPort,
            slaveDbPath, tuplesInserted);
        _verifyIndexOnSlave("index2ii");
        _verifyIndexOnSlave("index2s");
        
        // We should verify the master as well, 
        // at least to see that we still can connect.
        // verifyMaster();
        masterDbPath = masterDatabasePath +FS+ masterDbSubPath +FS+ replicatedDb;
        _verifyDatabase(masterServerHost, masterServerPort,
            masterDbPath, tuplesInserted);
    }
    void _fillTableOnServer(String serverHost, 
            int serverPort,
            String dbPath,
            int startVal,
            int _noTuplesToInsert)
        throws Exception
    {
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
        ds.setServerName(serverHost);
        ds.setPortNumber(serverPort);
        ds.setConnectionAttributes(useEncryption(false));
        Connection conn = ds.getConnection();
        
        PreparedStatement ps = conn.prepareStatement("insert into t values (?,?,?)");
        for (int i = 0; i< _noTuplesToInsert; i++)
        {
            ps.setInt(1,(i+startVal));
            ps.setString(2,"dilldall"+(i+startVal));
            ps.setInt(3,(i+startVal) % (_noTuplesToInsert/10) );
            ps.execute();
            if ( (i % 10000) == 0 ) conn.commit();
        }
        
        _verify(conn, startVal + _noTuplesToInsert);
        
        conn.close();
    }

    private void _verifyIndexOnSlave(String indexName) 
        throws SQLException
    {
        
        // Verify we may drop the index.
        executeOnSlave("drop index " + indexName);  // Will fail if index does no exist
    }
}
