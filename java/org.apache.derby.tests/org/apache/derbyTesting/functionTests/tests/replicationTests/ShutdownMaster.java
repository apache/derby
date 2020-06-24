/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ShutdownMaster
 
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
import org.apache.derbyTesting.junit.BaseJDBCTestCase;

public class ShutdownMaster extends BaseJDBCTestCase
{
    private static ReplicationRun repRun = new ReplicationRun("ShutdownMaster");
    
    public ShutdownMaster(String testcaseName)
    {
        super(testcaseName);
    }
    
    /* */
    static String jvmVersion = null;
    static String derbyVersion = null;
    static String masterServerHost = null;
    static int masterServerPort = -1;
    static String masterDatabasePath = null;
    static String slaveServerHost = null;
    static int slaveServerPort = -1;
    static String slaveDatabasePath = null;
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
    
    /**
     * Test shut down master server during replication.
     *
     * @throws SQLException, IOException, InterruptedException
     */
    void shutdown(String url, boolean dbOnly, boolean killServer) // FIXME?! Factor out this as common with ShutdownSlave!
    throws SQLException, IOException, InterruptedException
    {
        
        System.out.println("**** ShutdownMaster.shutdown() "
                + getTestConfiguration().getJDBCClient().getJDBCDriverName()
                + " url: " + url + " dbOnly: " + dbOnly + " killServer: " + killServer);

        Connection conn = getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("create table t (i integer primary key, vc varchar(20))");
        
        PreparedStatement pSt = prepareStatement("insert into t values (?,?)");
        
        boolean expectNoConnection = false;
        for (int i=0;i<1000;i++)
        {
            try
            {
                pSt.setInt(1, i);
                pSt.setString(2, "i"+i);
                pSt.execute();
            }
            catch (SQLException se)
            {
                int errCode = se.getErrorCode();
                String msg = se.getMessage();
                String state = se.getSQLState();
                System.out.println("execute Got SQLException: " + errCode + " " + state + " " + msg);
                if ( expectNoConnection  
                     && (errCode == 40000 && state.equalsIgnoreCase("08006")) /* FIXME! -1/40000 08006*/ )
                {
                    System.out.println("As expected - connection terminated. Quit.");
                    return; // FIXME! Final code will refuse shutdown in this state!
                }
                else
                {
                    throw se;
                }
            }
            System.out.println("i: "+i);
            if ( i == 500 )
            {
                if ( killServer )
                {
                    repRun.killMaster(masterServerHost, masterServerPort);
                    expectNoConnection = true;
                }
                else if ( url == null )
                {
                    System.out.println("**** stopServer: " + masterServerHost + ":" + masterServerPort);
                    repRun.stopServer(jvmVersion, derbyVersion, // Shut down server
                            masterServerHost, masterServerPort);
                    expectNoConnection = true;
                    // FIXME! Final code will refuse shutdown in this state!
                }
                else
                {
                    /* Alternative: NB! url can be with or without database path! */
                    System.out.println("**** DriverManager.getConnection(\"" + url+";shutdown=true\");");
                    try
                    {
                        DriverManager.getConnection(url+";shutdown=true");
                        // Expecting: SQLCODE: -1, SQLSTATE: XJ015 | 08006, SQLERRMC: Derby system shutdown.
                        // FIXME! Final code will refuse shutdown in this state!
                    }
                    catch(SQLException se)
                    {
                        int errCode = se.getErrorCode();
                        String msg = se.getMessage();
                        String state = se.getSQLState();
                        String expectedState = (dbOnly)? "08006": "XJ015";
                        int expectedCode = dbOnly ? 45000 : 50000;
                        System.out.println("shutdown Got SQLException: " + errCode + " " + state + " " + msg);
//IC see: https://issues.apache.org/jira/browse/DERBY-2601
                        if ( (errCode == expectedCode)
                        && (state.equalsIgnoreCase(expectedState) ) )
                        {
                            System.out.println("As expected.");
                            expectNoConnection = true;
                            // return; // FIXME! Final code will refuse shutdown in this state!
                        }
                        else
                        {
                            throw se;
                        }
                    }
                }
            }
        }
        // With PoC V2d:
        this.assertTrue("Should never reach this point in the ShutdownMaster test with PoC V2d!", false);
        // Final code: shutdown of master database or server should not be allowed after 
        // starmaster.
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        System.out.println("count: "+count);
        // s.executeUpdate("drop table t");
    }
}
