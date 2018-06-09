/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.TestPreStartedMasterServer
 
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

public class TestPreStartedMasterServer extends ClientRunner
{
    
    public TestPreStartedMasterServer(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** TestPreStartedMasterServer.suite()");
        
        initEnvironment();
        
        // String masterHostName = System.getProperty("test.serverHost", "localhost");
        // int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        BaseTestSuite suite = new BaseTestSuite("TestPreStartedMasterServer");
                
        suite.addTest(TestPreStartedMasterServer.suite(masterServerHost, masterServerPort));
        System.out.println("*** Done suite.addTest(TestPreStartedMasterServer.suite())");
        
        return (Test)suite;
    }

    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** TestPreStartedMasterServer.suite(serverHost,serverPort)");
     
        Test t = TestConfiguration.existingServerSuite(TestPreStartedMasterServer.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(TestPreStartedMasterServer.class,false,serverHost,serverPort)");
        return t;
   }

    
    /**
     *
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testStartMasterConnect_Illegal()
    throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** TestPreStartedMasterServer.testStartMasterConnect_Illegal() "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        Connection conn = null;
        String db = masterDatabasePath +"/"+ReplicationRun.masterDbSubPath +"/"+ replicatedDb;
        String connectionURL = "jdbc:derby:" 
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";startMaster=true"
                + ";slavehost=" + slaveServerHost 
                + ";slaveport=" + slaveServerPort;
        System.out.println(connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            System.out.println("testStartMasterConnect_Illegal: " + msg);
            // 40000 08001
            assertSQLState("Unexpected SQLException: " + msg, "08001", se);
            return;
        }
        assertTrue("Expected SQLException: '-4499 08001 " + db + "'",false);
    }
    
    public void verifyTestStartMasterConnect_Illegal()
    throws SQLException, IOException, InterruptedException
    {

    }
}
