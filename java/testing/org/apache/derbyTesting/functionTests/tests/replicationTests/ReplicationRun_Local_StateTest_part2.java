/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_StateTest_part2
 
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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * 
 */

public class ReplicationRun_Local_StateTest_part2 extends ReplicationRun
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_StateTest_part2(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("ReplicationRun_Local_StateTest_part2 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_StateTest_part2.class );
        
        return SecurityManagerSetup.noSecurityManager(suite);
    }
        
    /**
     * Verify that correct response to replication "commands":
     * startSlave, startMaster, stopSlave, stopMaster and failOver,
     * are given when the replicating database is in the following states:
     * Failover has been performed and 
     * - slave db has not been shut down,
     * - slave db has been shut down,
     * - slave server has been stopped,
     * - master db has been shut down,
     * - master server has been stopped.
     * @throws java.lang.Exception
     */
    public void testReplication_Local_StateTest_part2()
    throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
        masterServer = startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES,
                masterServerPort,
                masterDbSubPath);
        
        slaveServer = startServer(slaveJvmVersion, derbySlaveVersion,
                slaveServerHost,
                ALL_INTERFACES,
                slaveServerPort,
                slaveDbSubPath);
        
        startServerMonitor(slaveServerHost);
        
        bootMasterDatabase(jvmVersion,
                masterDatabasePath +FS+ masterDbSubPath,
                replicatedDb,
                masterServerHost,
                masterServerPort,
                null // bootLoad, // The "test" to start when booting db.
                );
        
        initSlave(slaveServerHost,
                jvmVersion,
                replicatedDb);
        
        startSlave(jvmVersion, replicatedDb,
                slaveServerHost,
                slaveServerPort,
                slaveServerHost,
                slaveReplPort,
                testClientHost);
        
        startMaster(jvmVersion, replicatedDb,
                masterServerHost,
                masterServerPort,
                masterServerHost,
                slaveServerPort,
                slaveServerHost,
                slaveReplPort);
        
        
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
        
        // Check that this is gone after failover!
        Connection mConn = getConnection(masterServerHost, masterServerPort, 
                    masterDatabasePath, masterDbSubPath, replicatedDb);
        
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
        
        _testPreStoppedSlave(mConn);
        
        shutdownDb(jvmVersion,
            slaveServerHost, slaveServerPort,
            slaveDatabasePath+FS+slaveDbSubPath, replicatedDb,
            testClientHost);
        
        _testPostStoppedSlave();
        // _testPreStoppedSlaveServer();
        
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        _testPostStoppedSlaveServer();
        // _testPreStoppedMaster();
        
        // NOTE:
        // If stopMaster is "connect 'jdbc:...:dbname;shutdown=true';", 
        // this method call will fail because failover has already shutdown 
        // the master database
        
        shutdownDb(jvmVersion,
            masterServerHost, masterServerPort,
            masterDatabasePath+FS+masterDbSubPath, replicatedDb,
            testClientHost);
        
        _testPostStoppedMaster();
        // _testPreStoppedMasterServer();
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
        _testPostStoppedServer();
        
    }

    private void _testPreStoppedSlave(Connection mConn)
        throws Exception
    {
        util.DEBUG("_testPreStoppedSlave");
        /*  
         * Future test case; should probably be done in a separate test 
         * - let Derby instances change roles:
         *   - (M) copy db from slave location to master location
         *   - (M) start old master in slave mode
         *   - (S) start old slave in master mode
         *
         */
        
        // Tests against slave:
        assertException(
                _startSlave(slaveServerHost,slaveServerPort,
                    slaveDatabasePath, replicatedDb,
                    slaveReplPort),
                "XRE09");

        assertException(
                stopSlave(slaveServerHost,slaveServerPort,
                          slaveDatabasePath, replicatedDb,
                          true),
                "XRE40");
        
        assertException(
                _failOver(slaveServerHost,slaveServerPort,
                    slaveDatabasePath, slaveDbSubPath, replicatedDb),
                "XRE07");
        
        // Tests against master:
        assertException(
                _startMaster(masterServerHost,masterServerPort,
                    masterDatabasePath, replicatedDb,
                    slaveServerHost,slaveReplPort),
                "XRE04");
        
        assertException(
                _failOver(masterServerHost,masterServerPort,
                    masterDatabasePath, masterDbSubPath, replicatedDb),
                "XRE07");
        
        // connect / show tables
        assertException(
                _executeQuery(mConn, "select count(*) from SYS.SYSTABLES"), 
                "08006"); // Thats's just how it is...
         assertException(
                _executeQuery(mConn, "select count(*) from SYS.SYSTABLES"), 
                "08003"); // Thats's just how it is...
        
        // Tests against slave:
        Connection sConn = getConnection(slaveServerHost, slaveServerPort, 
                    slaveDatabasePath, slaveDbSubPath, replicatedDb); // OK
         assertException(
                _executeQuery(sConn, "select count(*) from SYS.SYSTABLES"), 
                null); // null: Should be OK
         sConn.close();
    }

    private void _testPostStoppedSlave()
    {
        util.DEBUG("_testPostStoppedSlave");
        /* No value-adding suggestions here exept that calling startSlave 
         * should hang now. 
         */
        util.DEBUG("_testPostStoppedSlave Not yet implemented."
                + " No value-adding suggestions here"
                + " exept that calling startSlave should hang now.");
    }

    private void _testPostStoppedSlaveServer()
    {
        /* No value-adding suggestions here */
        util.DEBUG("_testPostStoppedSlaveServer Not yet implemented."
                + " No value-adding suggestions here.");
    }

    private void _testPostStoppedMaster()
    {
        /* No value-adding suggestions here since the stopMaster method will 
         * not do anything when called after failover
         */
        util.DEBUG("_testPostStoppedMaster Not yet implemented."
                + "No value-adding suggestions here since the stopMaster method"
                + " will not do anything when called after failover");
    }

    private void _testPostStoppedServer()
    {
        /* No value-adding suggestions here */
        util.DEBUG("_testPostStoppedServer Not yet implemented."
                + " No value-adding suggestions here.");
    }
    
    SQLException _startSlave(String slaveServerHost, int slaveServerPort,
            String slaveDatabasePath, String replicatedDb,
            int slaveReplPort)
    {
        String db = slaveDatabasePath +FS+ReplicationRun.slaveDbSubPath +FS+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";startSlave=true"
                + ";slaveHost=" + slaveServerHost 
                + ";slavePort=" + slaveReplPort;
        util.DEBUG(connectionURL);
        try
        {
            Connection conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("startSlave Unexpectedly connected as: " + connectionURL);
            return new SQLException("startSlave Unexpectedly connected");
        }
        catch (SQLException se)
        {
            return se;
        }
    }


    SQLException _failOver(String serverHost, int serverPort, 
            String databasePath, String dbSubPath, String replicatedDb)
    {
        String db = databasePath +FS+dbSubPath +FS+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + serverHost + ":" + serverPort + "/"
                + db
                + ";failover=true";
        util.DEBUG(connectionURL);
        try
        {
            Connection conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("failOver Unexpectedly connected as: " + connectionURL);
            return new SQLException("failOver Unexpectedly connected");
        }
        catch (SQLException se)
        {
            return se;
        }
    }
    
    SQLException _startMaster(String masterServerHost, int masterServerPort,
            String databasePath, String replicatedDb,
            String slaveServerHost,
            int slaveReplPort)
    {
        String db = databasePath +FS+ReplicationRun.masterDbSubPath +FS+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";startMaster=true"
                + ";slaveHost=" + slaveServerHost 
                + ";slavePort=" + slaveReplPort;
        util.DEBUG(connectionURL);
        try
        {
            Connection conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("startMaster Unexpectedly connected as: " + connectionURL);
            return new SQLException("startMaster Unexpectedly connected");
        }
        catch (SQLException se)
        {
            return se;
        }
    }

    SQLException connectTo(String serverHost, int serverPort,
            String databasePath, String dbSubPath, String replicatedDb)
    {
        String db = databasePath +FS+dbSubPath +FS+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + serverHost + ":" + serverPort + "/"
                + db;
        util.DEBUG(connectionURL);
        try
        {
            Connection conn = DriverManager.getConnection(connectionURL);
            util.DEBUG("connectTo Unexpectedly connected as: " + connectionURL);
            return new SQLException("connectTo Unexpectedly connected");
        }
        catch (SQLException se)
        {
            return se;
        }
    }

    SQLException _executeQuery(Connection conn, String query)
    {
        util.DEBUG("executeQuery: " + query);
        try
        {
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery(query);
            return null;        
        }
        catch (SQLException se)
        {
            return se;
        }
    }
}
