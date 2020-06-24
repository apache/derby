/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.DestroySlaveDB
 
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

public class DestroySlaveDB extends BaseJDBCTestCase
{
    private static String slaveServerHost = null;
    private static ReplicationRun repRun = new ReplicationRun("DestroySlaveDB");
    
    public DestroySlaveDB(String testcaseName)
    {
        super(testcaseName);
    }
        
    public static Test suite()
        throws Exception
    {
        System.out.println("**** DestroySlaveDB.suite()");
        System.out.println("'DestroySlaveDB' can not be run outside the 'ReplicationRun' framework.");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("DestroySlaveDB");
        
        String masterHostName = System.getProperty("test.serverHost", "localhost");
        int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        repRun.initEnvironment();
        slaveServerHost = ReplicationRun.slaveServerHost;
        
        suite.addTest(DestroySlaveDB.suite(masterHostName,masterPortNo));
        return (Test)suite;
    }
    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** DestroySlaveDB.suite("+serverHost+","+serverPort+")");
        
        Test t = TestConfiguration.existingServerSuite(DestroySlaveDB.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(DestroySlaveDB.class,false,"
                +serverHost+","+serverPort+")");
        return t;
   }
    
    /**
     * Test killing slave during replication.
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testDestroySlaveDB() 
        throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** DestroySlaveDB.testDestroySlaveDB() "+
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
                repRun.destroySlaveDB(slaveServerHost);
            }
        }
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        System.out.println("count: "+count);
        // s.executeUpdate("drop table t");
    }
}
