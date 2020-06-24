/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.TestPreInitSlave
 
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
 * Test behaviour of stopMaster and stopSlave on a system
 * before the slave db is copied in.
 */
public class TestPreInitSlave extends ClientRunner
{
    
    public TestPreInitSlave(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** TestPreInitSlave.suite()");
        
        initEnvironment();
        
        // String masterHostName = System.getProperty("test.serverHost", "localhost");
        // int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("TestPreInitSlave");
                
        suite.addTest(TestPreInitSlave.suite(slaveServerHost, slaveServerPort));
        System.out.println("*** Done suite.addTest(TestPreInitSlave.suite())");
        
        return (Test)suite;
    }

    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** TestPreInitSlave.suite(serverHost,serverPort)");
     
        Test t = TestConfiguration.existingServerSuite(TestPreInitSlave.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(TestPreInitSlave.class,false,serverHost,serverPort)");
        return t;
   }

    
    /**
     *
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void test()
    throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** TestPreInitSlave.testStartSlaveConnect_OK() "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        String db = null;
        String connectionURL = null;
        Connection conn = null;
        
        // 1.  stopMaster on master: fail
        db = masterDatabasePath +"/"+ReplicationRun.masterDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";stopMaster=true";
        System.out.println("1. " + connectionURL);
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
            // SQLCODE: -1, SQLSTATE: XRE07
            assertSQLState("stopMaster on master failed: " + connectionURL + " " + msg, "XRE07", se);
            System.out.println("stopMaster on master failed as expected: " + connectionURL + " " + msg);
        }
        
        // 2. stopSlave on slave: fail
        db = slaveDatabasePath +"/"+ReplicationRun.slaveDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";stopSlave=true";
        System.out.println("2. " + connectionURL);
        try
        {
            conn = DriverManager.getConnection(connectionURL); // From anywhere against slaveServerHost?
            System.out.println("Unexpectedly connected: " + connectionURL);
            assertTrue("Unexpectedly connected: " + connectionURL,false);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            // SQLCODE: 40000 SQLSTATE: 08004
            // FIXME: Is this correct? 'The connection was refused because the database /home/os136789/Replication/testing/db_slave/test;stopSlave=true was not found''
            assertSQLState("stopSlave on slave failed: " + connectionURL + " " + msg, "08004", se);
            System.out.println("stopSlave on slave failed as expected: " + connectionURL + " " + msg);
        }
        
    }
    
    public void verifyTestStartSlaveConnect_OK()
    throws SQLException, IOException, InterruptedException
    {

    }
}
