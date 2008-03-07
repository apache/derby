/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.TestPreStartedMaster
 
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
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that after startMaster, new attempts for startMaster or startSlave do fail.
 */
public class TestPreStartedMaster extends ClientRunner
{
    
    public TestPreStartedMaster(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** TestPreStartedMaster.suite()");
        
        initEnvironment();
        
        // String masterHostName = System.getProperty("test.serverHost", "localhost");
        // int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        TestSuite suite = new TestSuite("TestPreStartedMaster");
                
        suite.addTest(TestPreStartedMaster.suite(masterServerHost, masterServerPort));
        System.out.println("*** Done suite.addTest(TestPreStartedMaster.suite())");
        
        return (Test)suite;
    }

    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** TestPreStartedMaster.suite(serverHost,serverPort)");
     
        Test t = TestConfiguration.existingServerSuite(TestPreStartedMaster.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(TestPreStartedMaster.class,false,serverHost,serverPort)");
        return t;
   }

    
    /**
     *
     *
     * @throws java.sql.SQLException 
     * @throws java.io.IOException 
     * @throws java.lang.InterruptedException 
     */
    public void testStartMasterConnect_OK()
    throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** TestPreStartedMaster.testStartMasterConnect_OK() "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        Connection conn = null;
        String db = masterDatabasePath +"/"+ReplicationRun.masterDbSubPath +"/"+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";startMaster=true"
                + ";slaveHost=" + slaveServerHost 
                + ";slavePort=" + slaveReplPort;
        System.out.println(connectionURL);
        // First StartMaster connect ok:
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            System.out.println("1. startMaster Successfully connected as: " + connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            System.out.println(msg);
            throw se;
        }
        
        System.out.println("2. startMaster attempt should fail on: " + connectionURL);
        System.out.println("********************'' 2. CURRENTLY HANGS!!!! Skipping.");
        // if (false)
        { // FIXME! PRELIM Hangs!!
        // A 2. StartMaster connect should fail:
        try
        {
            conn = DriverManager.getConnection(connectionURL); // FIXME! PRELIM Hangs!!
            System.out.println("2. Unexpectedly connected as: " + connectionURL);
            assertTrue("2. Unexpectedly connected as: " + connectionURL, false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            System.out.println("2. startMaster No connection as expected: " + msg);
            assertSQLState("2. startMaster Unexpected SQLException: " + msg, "XJ004", se);
        }
        }
        
        // A 2. StartSlave connect should fail:
        db = slaveDatabasePath +"/"+ReplicationRun.slaveDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";startSlave=true"
                + ";slaveHost=" + slaveServerHost 
                + ";slavePort=" + slaveReplPort;
        System.out.println(connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            System.out.println("2. startSlave Unexpectedly connected as: " + connectionURL);
            assertTrue("2. startSlave Unexpectedly connected as: " + connectionURL, false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            System.out.println("2. startSlave No connection as expected: " + msg);
            // SQLCODE: -1, SQLSTATE: XRE09
            assertSQLState("2. startSlave Unexpected SQLException: " + msg, "XRE09", se);
        }
 
    }
    
    public void verifyTestStartMasterConnect_OK()
    throws SQLException, IOException, InterruptedException
    {

    }
}
