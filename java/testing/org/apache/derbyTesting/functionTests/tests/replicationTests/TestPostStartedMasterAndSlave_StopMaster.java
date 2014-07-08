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
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test behaviour when doing stopMaster after master and slave 
 * has got into replication mode.
 */
public class TestPostStartedMasterAndSlave_StopMaster extends ClientRunner
{
    
    public TestPostStartedMasterAndSlave_StopMaster(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** TestPostStartedMasterAndSlave_StopMaster.suite()");
        
        initEnvironment();
        
        // String masterHostName = System.getProperty("test.serverHost", "localhost");
        // int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        BaseTestSuite suite =
            new BaseTestSuite("TestPostStartedMasterAndSlave_StopMaster");
                
        suite.addTest(TestPostStartedMasterAndSlave_StopMaster.suite(slaveServerHost, slaveServerPort)); // master?
        System.out.println("*** Done suite.addTest(TestPostStartedMasterAndSlave_StopMaster.suite())");
        
        return (Test)suite;
    }

    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** TestPostStartedMasterAndSlave_StopMaster.suite(serverHost,serverPort)");
     
        Test t = TestConfiguration.existingServerSuite(TestPostStartedMasterAndSlave_StopMaster.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(TestPostStartedMasterAndSlave_StopMaster.class,false,serverHost,serverPort)");
        return t;
   }

    /**
     *
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testStopMaster()
    throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** TestPostStartedMasterAndSlave_StopMaster.testStopMaster() "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        Connection conn = null;
        String db = null;
        String connectionURL = null;
        
        // 1. Add attempt to perform stopMaster on slave. Should fail.
        db = slaveDatabasePath +"/"+ReplicationRun.slaveDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";stopMaster=true";
        System.out.println("1. " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL); // From anywhere against masterServerHost?
            System.out.println("Unexpectedly connected as: " + connectionURL);
            assertTrue("Unexpectedly connected as: " + connectionURL,false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            //  SQLCODE: -1, SQLSTATE: 08004
            assertSQLState(msg, "08004", se);
            System.out.println("stopMaster on slave failed as expected: " + connectionURL + " " + msg);
        }
        
        // 2. stopMaster on master: OK
        db = masterDatabasePath +"/"+ReplicationRun.masterDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";stopMaster=true";
        System.out.println("2. " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL); // From anywhere against masterServerHost?
            System.out.println("Connected as expected: " + connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            assertTrue("stopMaster on master failed: " + connectionURL + " " + msg,false);
            System.out.println("stopMaster on master failed: " + connectionURL + " " + msg);
        }
        
        // 3. stopMaster on slave which now is in non-replicating mode should fail.
        db = slaveDatabasePath +"/"+ReplicationRun.slaveDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";stopMaster=true";
        System.out.println("3. " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL); // From anywhere against masterServerHost?
            System.out.println("Unexpectedly connected: " + connectionURL);
            assertTrue("Unexpectedly connected: " + connectionURL,false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            System.out.println("DERBY-????: stopMaster on slave failed: " + connectionURL + " " + msg); // FIXME when DERBY-????
            //  DERBY-????: SQLCODE: 40000, SQLSTATE: 08004
            assertSQLState(msg, "08004", se); // assertTrue("stopMaster on slave failed: " + connectionURL + " " + msg, false);
        }
        
        // 4. Attempt to do stopmaster on master which now is in non-replicating mode should fail.
        db = masterDatabasePath +"/"+ReplicationRun.masterDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";stopMaster=true";
        System.out.println("4. " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL); // From anywhere against masterServerHost?
            System.out.println("DERBY-????: Unexpectedly connected: " + connectionURL); // FIXME when DERBY-????
            // DERBY-???? - assertTrue("Unexpectedly connected: " + connectionURL,false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            assertTrue("stopMaster on server not in master mode failed as expected: " + connectionURL + " " + msg,false);
            System.out.println("stopMaster on server not in master mode failed as expected: " + connectionURL + " " + msg);
        }
        
    }
    
    public void verify()
    throws SQLException, IOException, InterruptedException
    {

    }
}
