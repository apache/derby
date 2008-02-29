/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.TestPostStartedMasterAndSlave
 
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test behaviour when doing stopSlave after master and slave 
 * has got into replication mode.
 */
public class TestPostStartedMasterAndSlave_StopSlave extends ClientRunner
{
    
    private static ReplicationRun repRun = new ReplicationRun("TestPostStartedMasterAndSlave_StopSlave");

    public TestPostStartedMasterAndSlave_StopSlave(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** TestPostStartedMasterAndSlave_StopSlave.suite()");
        
        initEnvironment();
        
        // String masterHostName = System.getProperty("test.serverHost", "localhost");
        // int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        TestSuite suite = new TestSuite("TestPostStartedMasterAndSlave_StopSlave");
                
        suite.addTest(TestPostStartedMasterAndSlave_StopSlave.suite(slaveServerHost, slaveServerPort)); // master?
        System.out.println("*** Done suite.addTest(TestPostStartedMasterAndSlave_StopSlave.suite())");
        
        return (Test)suite;
    }

    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** TestPostStartedMasterAndSlave_StopSlave.suite(serverHost,serverPort)");
     
        Test t = TestConfiguration.existingServerSuite(TestPostStartedMasterAndSlave_StopSlave.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(TestPostStartedMasterAndSlave_StopSlave.class,false,serverHost,serverPort)");
        return t;
   }
    
    /**
     *
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testStopSlave()
    throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** TestPostStartedMasterAndSlave_StopSlave.testStopSlave "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        String db = null;
        String connectionURL = null;  
        Connection conn = null;
        
        // 1. stopSlave to slave with connection to master should fail.
        db = slaveDatabasePath +"/"+ReplicationRun.slaveDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";stopSlave=true";
        System.out.println("1. " + connectionURL);
        try
        {
            if ( true ) // "if ( !PoC )""
            {
                conn = DriverManager.getConnection(connectionURL); // From anywhere against slaveServerHost?
                System.out.println("Unexpectdly connected as: " + connectionURL);
                // DERBY-???? - assertTrue("Unexpectedly connected: " + connectionURL,false);
            }
            else
            {
                // System.out.println("**** Will hang with PoC code!! so skip... ****");System.out.flush();
            }
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            // SQLCODE: -1, SQLSTATE: XRE41
            assertSQLState(connectionURL +  " failed: ", "XRE41", se);
            System.out.println("1. Failed as expected: " + connectionURL +  " " + msg);
        }
        
        // 2. stopSlave to a master server should fail:
        db = masterDatabasePath +"/"+ReplicationRun.masterDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";stopSlave=true";
        System.out.println("2. " + connectionURL);
        try
        {
            if ( true ) // "if ( !PoC )""
            {
                conn = DriverManager.getConnection(connectionURL); // From anywhere against slaveServerHost?
                System.out.println("Unexpectdly connected as: " + connectionURL);
                // DERBY-???? - assertTrue("Unexpectedly connected: " + connectionURL,false);
            }
            else
            {
                // System.out.println("**** Will hang with PoC code!! so skip... ****");System.out.flush();
            }
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            // SSQLCODE: -1, SQLSTATE: XRE40
            assertSQLState(connectionURL +  " failed: ", "XRE40", se);
            System.out.println("2. Failed as expected: " + connectionURL +  " " + msg);
        }
        
        // Replication should still be up.
        
        // Take down master - slave connection:
        // By OS kill:
        repRun.killMaster(masterServerHost, masterServerPort);
        
        // 3.  stopSlave on slave should now be allowed. Observe that the database shall be shutdown.
        db = slaveDatabasePath +"/"+ReplicationRun.slaveDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";stopSlave=true";
        boolean stopSlaveCorrect = false;
        System.out.println("3. " + connectionURL);
        try
        {
            if ( true ) // "if ( !PoC )""
            {
                conn = DriverManager.getConnection(connectionURL); // From anywhere against slaveServerHost?
                System.out.println("Unexpectedly connected: " + connectionURL);
                assertTrue("Unexpectedly connected: " + connectionURL,false);
            }
            else
            {
                // System.out.println("**** Will hang with PoC code!! so skip... ****");System.out.flush();
            }
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            // SQLCODE: -1, SQLSTATE: XRE41, SQLERRMC: XRE41 // db is shut down.
            assertSQLState(connectionURL +  " failed: ", "XRE41", se);
            System.out.println("3. Failed as expected: " + connectionURL +  " " + msg);
            stopSlaveCorrect = true;
        }
        
        if ( stopSlaveCorrect )
        {
            // 4. Try a normal connection:
            connectionURL = "jdbc:derby:"
                    + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                    + db;
           System.out.println("4. " + connectionURL);
            try
            {
                conn = DriverManager.getConnection(connectionURL); // From anywhere against slaveServerHost?
                System.out.println("4. Connected as expected: " + connectionURL);
            }
            catch (SQLException se)
            {
                int ec = se.getErrorCode();
                String ss = se.getSQLState();
                String msg = ec + " " + ss + " " + se.getMessage();
                System.out.println("DERBY-???: 4. Unexpectedly failed to connect: " + connectionURL +  " " + msg);
                // DERBY-???: assertTrue("Unexpectedly failed to connect: " + connectionURL +  " " + msg, false);
            }
        }
    
    }
    
    public void verify()
    throws SQLException, IOException, InterruptedException
    {

    }
}
