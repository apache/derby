/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.TestPostStartedMasterAndSlave_Failover
 
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
 * Test that failover is allowed against master but not against slave.
 */
public class TestPostStartedMasterAndSlave_Failover extends ClientRunner
{
    private static ReplicationRun repRun = new ReplicationRun("TestPostStartedMasterAndSlave_Failover");
    
    public TestPostStartedMasterAndSlave_Failover(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** TestPostStartedMasterAndSlave_Failover.suite()");
        
        initEnvironment();
        
        // String masterHostName = System.getProperty("test.serverHost", "localhost");
        // int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("TestPostStartedMasterAndSlave_Failover");
                
        suite.addTest(TestPostStartedMasterAndSlave_Failover.suite(slaveServerHost, slaveServerPort)); // master?
        System.out.println("*** Done suite.addTest(TestPostStartedMasterAndSlave_Failover.suite())");
        
        return (Test)suite;
    }

    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** TestPostStartedMasterAndSlave_Failover.suite(serverHost,serverPort)");
     
        Test t = TestConfiguration.existingServerSuite(TestPostStartedMasterAndSlave_Failover.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(TestPostStartedMasterAndSlave_Failover.class,false,serverHost,serverPort)");
        return t;
   }
    
    /**
     *
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testFailOver()
    throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** TestPostStartedMasterAndSlave_Failover.testFailOver() "
                +getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        Connection conn = null;
        String db = slaveDatabasePath +"/"+ReplicationRun.slaveDbSubPath +"/"+ replicatedDb;
        String connectionURL = "jdbc:derby:"  
                + "//" + slaveServerHost + ":" + slaveServerPort + "/"
                + db
                + ";failover=true";
        System.out.println(connectionURL);
        try
        {
            // System.out.println("**** Will hang with PoC code!! so skip... ****");System.out.flush();
            if ( true ) // "if ( !PoC )""
            {
                conn = DriverManager.getConnection(connectionURL); // From anywhere against slaveServerHost?
                System.out.println("Successfully connected as: " + connectionURL);
                assertTrue("Successfully connected as: " + connectionURL, false);
            }
            else
            {
                // PoC "simulates" failover doing stop/kill master
                repRun.stopServer(jvmVersion, derbyVersion,
                        masterServerHost, masterServerPort);
            }
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = "As expected: Failover on slave should fail: " + ec + " " + ss + " " + se.getMessage();
            System.out.println(msg);
        }
        
        // Failover on master should succeed:
        db = masterDatabasePath +"/"+ReplicationRun.masterDbSubPath +"/"+ replicatedDb;
        connectionURL = "jdbc:derby:"  
                + "//" + masterServerHost + ":" + masterServerPort + "/"
                + db
                + ";failover=true";
        System.out.println(connectionURL);
        try
        {
            // System.out.println("**** Will hang with PoC code!! so skip... ****");System.out.flush();
            if ( true ) // "if ( !PoC )""
            {
                conn = DriverManager.getConnection(connectionURL); // From anywhere against masterServerHost?
                System.out.println("Unexpectedly connected as: " + connectionURL);
                assertTrue("Unexpectedly connected as: " + connectionURL, false);
            }
            else
            {
                // PoC "simulates" failover doing stop/kill master
                repRun.stopServer(jvmVersion, derbyVersion,
                        masterServerHost, masterServerPort);
            }
        }
        catch (SQLException se)
        {
            int ec = se.getErrorCode();
            String ss = se.getSQLState();
            String msg = ec + " " + ss + " " + se.getMessage();
            // Failover OK: SQLCODE: -1, SQLSTATE: XRE20
            assertSQLState(msg, "XRE20", se);
            System.out.println("Failover on master succeeded: " + connectionURL + " " + msg);
        }
        
                
    }
    
    public void verify()
    throws SQLException, IOException, InterruptedException
    {

    }
}
