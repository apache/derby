/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ShutdownSlave
 
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

public class ShutdownSlave extends BaseJDBCTestCase
{
    private static ReplicationRun repRun = new ReplicationRun("ShutdownSlave");
    
    public ShutdownSlave(String testcaseName)
    {
        super(testcaseName);
    }
    
    /* */
    static String jvmVersion = null;
    static String derbyVersion = null;
    static String slaveServerHost = null;
    static int slaveServerPort = -1;
    static String slaveDatabasePath = null;
    static String masterServerHost = null;
    static int masterServerPort = -1;
    static String masterDatabasePath = null;
    /* */
    
    static void setEnv()
    throws Exception
    {
        repRun.initEnvironment();
        masterServerHost = ReplicationRun.masterServerHost;
        masterServerPort = ReplicationRun.masterServerPort;
        slaveServerHost = ReplicationRun.slaveServerHost;
        slaveServerPort = ReplicationRun.slaveServerPort;
        jvmVersion =      ReplicationRun.masterJvmVersion;
        derbyVersion =    ReplicationRun.derbyMasterVersion;
        masterDatabasePath = ReplicationRun.masterDatabasePath;
        slaveDatabasePath = ReplicationRun.slaveDatabasePath;
    }
    
    /*
    public static Test suite()
    throws Exception
    {
        System.out.println("**** ShutdownSlave.suite()");
        System.out.println("'ShutdownSlave' can not be run outside the 'ReplicationRun' framework.");
        
        setEnv();
        
        TestSuite suite = new TestSuite("ShutdownSlave");
        suite.addTest(ShutdownSlave.suite(masterServerHost, masterServerPort));
        return (Test)suite;
    }
     */
    /**
     * Adds this class to the *existing server* suite.
     */
    /*
    public static Test suite(String serverHost, int serverPort)
    throws IOException
    {
        System.out.println("*** ShutdownSlave.suite("+serverHost+","+serverPort+")");
        
        Test t = TestConfiguration.existingServerSuite(ShutdownSlave.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(ShutdownSlave.class,false,"
                +serverHost+":"+serverPort+")");
        return t;
    }
     */
    
    /**
     * Test shut down slave server during replication.
     *
     * @throws SQLException, IOException, InterruptedException
     */
    /*
    public void testShutdownSlave()
    throws SQLException, IOException, InterruptedException
    {
        String slaveServerURL = "jdbc:derby:"
                +"//"+slaveServerHost+":"+slaveServerPort+"/";
        String slaveDbURL = slaveServerURL
                +ReplicationRun.slaveDatabasePath
                +"/"+ReplicationRun.slaveDbSubPath
                +"/"+ReplicationRun.replicatedDb;
        
        // shutdown(null, false, true); // -,-,true: Use OS kill on server!
        // shutdown(null, false, false); // null,-,-: use networkservercontrol!
        shutdown(slaveServerURL, false, false); // serverURL,false,-: shutdown server!
        // shutdown(slaveDbURL, true, false); // dbURL,true,-: shutdown database!
    }
     */
    
    void shutdown(String url, boolean dbOnly, boolean killServer) // FIXME! Factor out this as common with ShutdownMaster!
    throws SQLException, IOException, InterruptedException
    {
        
        System.out.println("**** ShutdownSlave.shutdown() "
                + getTestConfiguration().getJDBCClient().getJDBCDriverName()
                + " " + url + " dbOnly: " + dbOnly + " killServer: " + killServer);
        
        Connection conn = getConnection(); // To master
        Statement s = conn.createStatement();
        s.executeUpdate("create table t (i integer primary key, vc varchar(20))");
        
        PreparedStatement pSt = prepareStatement("insert into t values (?,?)");
        
        for (int i=0;i<1000;i++)
        {
            pSt.setInt(1, i);
            pSt.setString(2, "i"+i);
            pSt.execute();
            System.out.println("i: "+i);
            if ( i == 500 )
            {
                // conn.commit(); // Force transferring replication data?
                if ( killServer )
                {
                    repRun.killMaster(slaveServerHost, slaveServerPort);
                    // expectNoConnection = true;
                }
                else if (url == null )
                {
                    repRun.stopServer(jvmVersion, derbyVersion, // Shuts down the server
                            slaveServerHost, slaveServerPort);
                }
                else
                { // url specifies server or database shutdown.
                    System.out.println("**** DriverManager.getConnection(\"" + url+";shutdown=true\");");
                    try
                    {
                        DriverManager.getConnection(url+";shutdown=true"); // PoC hangs on dburl.
                        // SQLCODE: -1, SQLSTATE: XJ015
                        // Will not be allowed in final code: when privileges are implemented!
                    }
                    catch( SQLException se)
                    {
                        int errCode = se.getErrorCode();
                        String msg = se.getMessage();
                        String state = se.getSQLState();
                        String expectedState = (dbOnly)? "08004": "XJ015";
                        System.out.println("shutdown Got SQLException: " + errCode + " " + state + " " + msg);
                        if ( (errCode == -1)
                        && (state.equalsIgnoreCase(expectedState) ) )
                        {
                            System.out.println("As expected.");
                            // Continue insert on master. return; 
                            // shutdown db will be refused in this state!
                        }
                        else
                        {
                            throw se;
                        }
                    }
                }
            }
        }
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        System.out.println("count: "+count);
        // s.executeUpdate("drop table t");
    }
}
