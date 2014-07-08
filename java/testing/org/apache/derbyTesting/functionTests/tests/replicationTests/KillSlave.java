/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.KillSlave
 
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

public class KillSlave extends BaseJDBCTestCase
{
    private static ReplicationRun repRun = new ReplicationRun("KillSlave");
    
    public KillSlave(String testcaseName)
    {
        super(testcaseName);
    }
        
    private static String slaveServerHost = null;
    private static int slaveServerPort = -1;
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** KillSlave.suite()");
        System.out.println("'KillSlave' can not be run outside the 'ReplicationRun' framework.");
        
        BaseTestSuite suite = new BaseTestSuite("KillSlave");
        
        String masterHostName = System.getProperty("test.serverHost", "localhost");
        int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        repRun.initEnvironment();
        slaveServerHost = ReplicationRun.slaveServerHost;
        slaveServerPort = ReplicationRun.slaveServerPort;
        
        suite.addTest(KillSlave.suite(masterHostName,masterPortNo));
        return (Test)suite;
   }
    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** KillSlave.suite(serverHost,serverPort)");
        Test t = TestConfiguration.existingServerSuite(KillSlave.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.defaultExistingServerSuite(KillSlave.class,false,serverHost,serverPort)");
        return t;
    }
    
    /**
     * Test killing slave during replication.
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testKillSlave() 
        throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** KillSlave.testKillSlave() "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        Connection conn = getConnection();
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
                /* The current PoC does not allow other connections, so this just hangs.
                ReplicationRun.stopServer(slaveJvmVersion, derbySlaveVersion,
                        slaveServerHost, slaveServerPort);
                 */
                repRun.killSlave(slaveServerHost, slaveServerPort);
            }
        }
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        System.out.println("count: "+count);
        // s.executeUpdate("drop table t");
    }
}
