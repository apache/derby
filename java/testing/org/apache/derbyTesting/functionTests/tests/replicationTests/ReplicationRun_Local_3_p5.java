/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_3_p5
 
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


/**
 * Run a replication test on localhost
 * by using default values for master and slave hosts,
 * and master and slave ports.
 * Test DERBY-3924 verifying the fix for DERBY-3878.
 * 
 */

public class ReplicationRun_Local_3_p5 extends ReplicationRun_Local_3
{
    

    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    // String getDerbyServerPID = null;
    public ReplicationRun_Local_3_p5(String testcaseName)
    {
        super(testcaseName);

    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("ReplicationRun_Local_3_p5 Suite");
        
        suite.addTestSuite( ReplicationRun_Local_3_p5.class);
        
        return SecurityManagerSetup.noSecurityManager(suite);

    }
        
    /**
     * Test that DERBY-3924 fixed DERBY-3878.
     * @throws java.lang.Exception
     */
    public void testReplication_Local_3_p5_DERBY_3878()
    throws Exception
    {
        makeReadyForReplication();
        
        // getDerbyServerPID = userDir +FS+ "getDerbyServerPID";
        // mk_getDerbyServerPID_Cmd(getDerbyServerPID);
        
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
                
        _killMasterServer(); // "Crash" master.

        stopSlave(slaveServerHost,
                  slaveServerPort,
                  slaveDatabasePath,
                  replicatedDb,
                  false); // master server dead

        // Try to re-establish replication mode:
        masterServer = startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES,
                masterServerPort,
                masterDbSubPath);

        if ( masterServerHost.equalsIgnoreCase("localhost") || localEnv )
        {
             String URL = masterURL(replicatedDb);
            Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
            util.DEBUG("bootMasterDatabase getConnection("+URL+")");
            Connection conn = DriverManager.getConnection(URL);
            Statement s = conn.createStatement();
            s.execute("call syscs_util.syscs_freeze_database()");
            conn.close();
        }
        else
        {
            runTest(freezeDB,
                    jvmVersion,
                    testClientHost,
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        initSlave(slaveServerHost, // Copy master contents to slave again.
                jvmVersion,
                replicatedDb);

        /* Slave server still running, so no need to start slave server */
        
        startSlave(jvmVersion, replicatedDb, // should cause an address-already-in-use exception without the fix for DERBY-3878
                slaveServerHost,             // Verified that we get 'Address already in use' and then hangs!
                slaveServerPort,             // without the fix for DERBY-3878
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
        // Should now be back in "normal" replication mode state.
        
        assertSqlStateSlaveConn(REPLICATION_SLAVE_STARTED_OK);
        
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
    
    private void _killMasterServer()
        throws ClassNotFoundException, SQLException
    {
             String URL = masterURL(replicatedDb); // So far only used for master!
            Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
            Connection conn = DriverManager.getConnection(URL);
            Statement s = conn.createStatement();
            conn.setAutoCommit(false); // 
            s.execute("create procedure kill(in p integer)"
                    + " parameter style java"
                    + " no sql"
                    + " language java"
                    + " external name"
                      + "'java.lang.System.exit'");
            try{
                s.execute("call kill(0)");
            } catch (SQLException se)
            {
                int errCode = se.getErrorCode();
                String msg = se.getMessage();
                String sState = se.getSQLState();
                String expectedState = "08006";
                msg = errCode + " " + sState + " " + msg 
                        + ". Expected: "+ expectedState;
                if ( (errCode == 40000)
                && (sState.equalsIgnoreCase(expectedState) ) )
                {
                    util.DEBUG("As expected. " + msg);
                }
                else
                {
                    assertTrue("kill() failed. " + msg, false);
                }
                
            }
            /* The connection is now gone!
            conn.rollback();
            conn.close();
            */
        
    }
    
    /*
    private void _killServer(String masterServerHost, int masterServerPort)
    throws InterruptedException, IOException
    {
        // This will work for "Unix" only!!
        util.DEBUG("_killServer: " + masterServerHost +":" + masterServerPort);

        String PID = runUserCommand(getDerbyServerPID + " " + masterServerPort,testUser);
        runUserCommand("kill " + PID,testUser);

    }
    
    private void mk_getDerbyServerPID_Cmd(String cmdName)
            throws IOException
    {
        String cmd = "#!/bin/bash"
            + LF + "PORTNO=$1"
            + LF + "if [ \"${PORTNO}\" == \"\" ]"
            + LF + "then"
            + LF + "  echo UNDEFINED_PORT_NUMBER"
            + LF + "  PORTNO=UNDEFINED_PORT_NUMBER"
            + LF + "fi"
            + LF + "ps auxwww"
              + "| grep \"org.apache.derby.drda.NetworkServerControl "
              + "start -h 0.0.0.0 -p ${PORTNO}\" | grep -v grep "
              + "| gawk '{ print $2 }'";
        util.writeToFile(cmd, cmdName);
        runUserCommand("chmod +x " + cmdName,testUser);
    }
    */
        
}
