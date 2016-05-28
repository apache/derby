/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_3_p3
 
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

import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derby.jdbc.ClientDataSourceInterface;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * Verify that a second failover is not accepted.
 * 
 */

public class ReplicationRun_Local_3_p3 extends ReplicationRun_Local_3
{

    final static String CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE = "08004";
    final static String REPLICATION_NOT_IN_MASTER_MODE     = "XRE07";
    final static int MAX_TRIES = 20;
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_3_p3(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
    {
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Local_3_p3 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_3_p3.class    );
        
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
     * Verify that a second failover is not accepted.
     * @throws java.lang.Exception
     */
    public void testReplication_Local_3_p3_StateNegativeTests()
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
        
        /* In ReplicationRun_Local_3_p1
        // 4 separate test
        // master db created...
        // slave: connect 'startSlave=true;create=true'
         */
        
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
        
        replicationTest = null; // Used as a flag to verifyMaster and verifySlave!
        runTest(replicationTest, // Returns immediatly if replicationTest is null.
                jvmVersion,
                testClientHost,
                masterServerHost, masterServerPort,
                replicatedDb);
        
        /* In ReplicationRun_Local_3_p1
        // 1 separate test
        // slave: stopSlave
       
        // 2 separate test
        // master: stopSlave
        // master: stopMaster
        // slave: stopSlave
         */
        
        // 3 separate test
        // stopMaster
        // shutdown on slave
        assertException(
            _stopMaster(masterServerHost, masterServerPort,
                masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb),
            null); // Implies slave should shut down. // OK to continue.
        /* showCurrentState("Post stopMaster +1s", 1000L,
            masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb, 
            masterServerHost, masterServerPort); */
        waitForConnect(100L, 10, 
                masterDatabasePath + FS + masterDbSubPath + FS + replicatedDb, 
                masterServerHost, masterServerPort);
        /* showCurrentState("Post stopMaster +1s", 0L,
            slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
            slaveServerHost, slaveServerPort);
        showCurrentState("Post stopMaster +5s", 5000L,
            slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb, 
            slaveServerHost, slaveServerPort); */

        // Connect to the ex-slave db, to verify that we can boot it and
        // connect to it.
        //
        // DERBY-4186: We use a loop below, to allow for intermediate state on
        // the slave db after master stopped and before slave reaches the
        // expected final state.
        //
        // If we get here quick enough we see this error state:
        //     CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE
        //
        // The final end state is successful connect (i.e. a reboot) after
        // stopped slave and db shutdown.

        SQLException gotEx = null;
        int tries = MAX_TRIES;

        while (tries-- > 0) {
            gotEx = null;
            try
            {
                String connectionURL =
                    slaveDatabasePath + FS + slaveDbSubPath + FS + replicatedDb;
                ClientDataSourceInterface ds;

                Class<?> clazz;
                if (JDBC.vmSupportsJNDI()) {
                    clazz = Class.forName("org.apache.derby.jdbc.ClientDataSource");
                    ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
                } else {
                    clazz = Class.forName("org.apache.derby.jdbc.BasicClientDataSource40");
                    ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
                }

                ds.setDatabaseName(connectionURL);
                ds.setServerName(slaveServerHost);
                ds.setPortNumber(slaveServerPort);
                ds.getConnection().close();
                util.DEBUG("Successfully connected after shutdown: " +
                           connectionURL);
                break;
            }
            catch (SQLException se)
            {
                if (se.getSQLState().
                        equals(CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE)) {
                    // Try again, shutdown did not complete yet..
                    gotEx = se;
                    util.DEBUG(
                        "got SLAVE_OPERATION_DENIED_WHILE_CONNECTED, sleep");
                    Thread.sleep(1000L);
                    continue;

                } else {
                    // Something else, so report.
                    gotEx = se;
                    break;
                }
            }
        }

        if (gotEx != null) {
            String reason;
            if (gotEx.getSQLState().
                    equals(CANNOT_CONNECT_TO_DB_IN_SLAVE_MODE)) {
                reason = "Tried " + MAX_TRIES + " times...";
            } else {
                reason = "Unexpected SQL state: " + gotEx.getSQLState();
            }

            util.DEBUG(reason);
            throw gotEx;
        }

        // A failover on ex-master db should fail now
        assertException(
            _failOver(masterServerHost, masterServerPort, 
                masterDatabasePath+FS+masterDbSubPath+FS+replicatedDb),
            REPLICATION_NOT_IN_MASTER_MODE);

        // We should verify the master as well, at least to see that we still
        // can connect.
        verifyMaster();
        
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        
    }
    
}
