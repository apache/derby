/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.TestPreStoppedSlaveServer
 
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
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test replication commands against an operational slave server.
 */
public class TestPreStoppedSlaveServer extends ClientRunner
{
    
    public TestPreStoppedSlaveServer(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** TestPreStoppedSlaveServer.suite()");
        
        initEnvironment();
        
        // String masterHostName = System.getProperty("test.serverHost", "localhost");
        // int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("TestPreStoppedSlaveServer");
                
        suite.addTest(TestPreStoppedSlaveServer.suite(slaveServerHost, slaveServerPort));
        System.out.println("*** Done suite.addTest(TestPreStoppedSlaveServer.suite())");
        
        return (Test)suite;
    }

    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** TestPreStoppedSlaveServer.suite(serverHost,serverPort)");
     
        Test t = TestConfiguration.existingServerSuite(TestPreStoppedSlaveServer.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(TestPreStoppedSlaveServer.class,false,serverHost,serverPort)");
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
        System.out.println("**** EMPTY!!! TestPreStoppedSlaveServer.test() "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        /*
        Connection conn = null;
        String db = slaveDatabasePath +"/"+ReplicationRun.slaveDbSubPath +"/"+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";startMaster=true"
                + ";slavehost=" + slaveServerHost 
                + ";slaveport=" + slaveServerPort;
        System.out.println(connectionURL);
        // First StartSlave connect ok:
        try
        {
            conn = DriverManager.getConnection(connectionURL);
            System.out.println("1. Successfully connected as: " + connectionURL);
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            System.out.println(msg);
            throw se; // FIXME!?
        }
         */
        
    }
    
    public void verifyTest()
    throws SQLException, IOException, InterruptedException
    {

    }
}
