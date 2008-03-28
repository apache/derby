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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.jdbc.ClientDataSource;
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
        TestSuite suite = new TestSuite("ReplicationRun_Local_1 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_1.class );
        
        return SecurityManagerSetup.noSecurityManager(suite);

    }
    
    public void testReplication_Local_1()
    throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
        masterServer = startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost, ALL_INTERFACES, masterServerPort,
                masterDbSubPath);
        
        slaveServer = startServer(slaveJvmVersion, derbySlaveVersion,
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
        _testInsertUpdateDeleteOnMaster(masterServerHost, masterServerPort, 
                dbPath);
        
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
            dbPath);
        // We should verify the master as well, 
        // at least to see that we still can connect.
        // verifyMaster();
        dbPath = masterDatabasePath +FS+masterDbSubPath +FS+ replicatedDb;
        _verifyDatabase(masterServerHost, masterServerPort,
            dbPath);
    }
    
    private final int noTuplesToInsert = 10000;
    private void _testInsertUpdateDeleteOnMaster(String serverHost, 
            int serverPort,
            String dbPath)
        throws SQLException
    {
        util.DEBUG("_testInsertUpdateDeleteOnMaster: " + serverHost + ":" +
                   serverPort + "/" + dbPath);
        ClientDataSource ds = new org.apache.derby.jdbc.ClientDataSource();
        ds.setDatabaseName(dbPath);
        ds.setServerName(serverHost);
        ds.setPortNumber(serverPort);
        Connection conn = ds.getConnection();
        
        PreparedStatement ps = conn.prepareStatement("create table t(i integer primary key, s varchar(64))");
        
        ps.execute();
        
        ps = conn.prepareStatement("insert into t values (?,?)");
        for (int i = 0; i< noTuplesToInsert; i++)
        {
            ps.setInt(1,i);
            ps.setString(2,"dilldall"+i);
            ps.execute();
            if ( (i % 10000) == 0 ) conn.commit();
        }
        
        _verify(conn);
        
        conn.close();
    }
    private void _verifyDatabase(String serverHost, 
            int serverPort,
            String dbPath)
        throws SQLException
    {
        util.DEBUG("_verifyDatabase: "+serverHost+":"+serverPort+"/"+dbPath);
        ClientDataSource ds = new org.apache.derby.jdbc.ClientDataSource();
        ds.setDatabaseName(dbPath);
        ds.setServerName(serverHost);
        ds.setPortNumber(serverPort);
        Connection conn = ds.getConnection();
        
        _verify(conn);
        
        conn.close();
    }
    private void _verify(Connection conn)
        throws SQLException
    {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        rs = s.executeQuery("select max(i) from t");
        rs.next();
        int max = rs.getInt(1);
        util.DEBUG("_verify: " + count + "/" + noTuplesToInsert + " " + max +
                   "/" + (noTuplesToInsert - 1));
        assertEquals("Expected "+ noTuplesToInsert +" tuples, got "+ count +".",
                     noTuplesToInsert, count);
        assertEquals("Expected " +(noTuplesToInsert-1) +" max, got " + max +".",
                     noTuplesToInsert - 1, max);
    }
}
