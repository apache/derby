/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.KillMaster
 
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

public class KillMaster extends BaseJDBCTestCase
{
    
    public KillMaster(String testcaseName)
    {
        super(testcaseName);
    }
    
    /* */
    private static String masterJvmVersion = null;
    private static String derbyMasterVersion = null;
    private static String masterServerHost = null;
    private static int masterServerPort = -1;
    private static ReplicationRun repRun = new ReplicationRun("KillMaster");
    /* */
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** KillMaster.suite()");
        System.out.println("'KillMaster' can not be run outside the 'ReplicationRun' framework.");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("KillMaster");
         
        String masterHostName = System.getProperty("test.serverHost", "localhost");
        int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));

        masterServerHost = masterHostName;
        masterServerPort = masterPortNo;
        
        repRun.initEnvironment();
        masterJvmVersion = ReplicationRun.masterJvmVersion;
        derbyMasterVersion = ReplicationRun.derbyMasterVersion;
        
        suite.addTest(KillMaster.suite(masterHostName,masterPortNo));
        return (Test)suite;
 }
    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
        throws IOException
    {
        System.out.println("*** KillMaster.replSuite(serverHost,serverPort)");
        
        Test t = TestConfiguration.existingServerSuite(KillMaster.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.defaultExistingServerSuite(KillMaster.class,false,serverHost,serverPort)");
        return t;
    }
    
    /**
     * Test killing master during replication.
     *
     * @throws Exception
     */
    public void testKillMaster() 
        throws Exception
    {
        System.out.println("**** KillMaster.testKillMaster() "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("create table t (i integer primary key, vc varchar(20))");
        
        PreparedStatement pSt = prepareStatement("insert into t values (?,?)");
        int i=0;
        try{
            for (;i<1000;i++)
            {
                pSt.setInt(1, i);
                pSt.setString(2, "i"+i);
                pSt.execute();
                System.out.println("i: "+i);
                if ( i == 500 )
                {
                    /*
                    ReplicationRun.stopServer(masterJvmVersion, derbyMasterVersion,
                        masterServerHost, masterServerPort);
                     */
                    repRun.killMaster(masterServerHost, masterServerPort);
                }
            }
        }catch (SQLException se) {
            System.out.println("SQLException @ i="+i+" ("+se.getMessage()+")");
            if ( i <=500)
            {
                fail("**** Unexpected SQLException @ i="+i+" ("+se.getMessage()+")");
            }
        }
        /* Master not available any more. VerificationClient should check both Master and Slave db!
         */
        conn.close();
        
        Thread.sleep(5000L); // Better sleep a little until master is totally gone?
        repRun.startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ReplicationRun.ALL_INTERFACES, // masterServerHost, // "0.0.0.0", // All. or use masterServerHost for interfacesToListenOn,
                masterServerPort,
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
                ReplicationRun.masterDbSubPath); // Distinguishing master/slave
                                                  // Will only work if default/initial values are used!
                                                  // MUST BE FULL PATH!!!
                
        /* */
        conn = getConnection();
        s = conn.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        System.out.println("count: "+count);
        // s.executeUpdate("drop table t");
        /* */
    }
}
