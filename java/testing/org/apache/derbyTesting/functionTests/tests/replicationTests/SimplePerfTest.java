/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.SimplePerfTest
 
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
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

public class SimplePerfTest extends BaseJDBCTestCase
{
    
    public SimplePerfTest(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("**** SimplePerfTest.suite()");
        
        String masterHostName = System.getProperty("test.serverHost", "localhost");
        int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        TestSuite suite = new TestSuite("SimplePerfTest");
        
        tuplesToInsert = Integer.parseInt(System.getProperty("test.inserts", "1000"));
        commitFreq = Integer.parseInt(System.getProperty("test.commitFreq", "0"));
        
        suite.addTest(SimplePerfTest.suite(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(SimplePerfTest.suite())");
        
        return (Test)suite;
    }
    private static int tuplesToInsert = 0;
    private static int commitFreq = 0; // Means autocommit.
    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    {
        System.out.println("*** SimplePerfTest.suite(serverHost,serverPort)");
     
        Test t = TestConfiguration.existingServerSuite(SimplePerfTest.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(SimplePerfTest.class,false,serverHost,serverPort)");
        return t;
   }
    
    /**
     *
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testInserts()
    throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** SimplePerfTest.testInserts() "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        Connection conn = getConnection();
        
        Statement s = conn.createStatement();
        s.executeUpdate("create table t (i integer primary key, vc varchar(20))");
        
        PreparedStatement pSt = prepareStatement("insert into t values (?,?)");
        
        conn.setAutoCommit(commitFreq == 0 ? true : false); // commitFreq == 0 means do autocommit
        
        for (int i=0;i<tuplesToInsert;i++)
        {
            pSt.setInt(1, i);
            pSt.setString(2, "i"+i);
            pSt.execute();
            // commit after each commitFreq insert
            if ((commitFreq != 0) && ((i % commitFreq) == 0)){System.out.println("i: "+i);conn.commit();}
        }
        
        conn.commit();
        
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        System.out.println("count: "+count);
        // s.executeUpdate("drop table t");
    }
    public void verifyTestInserts()
    throws SQLException, IOException, InterruptedException
    {
        System.out.println("**** SimplePerfTest.verifyTestInserts() "+
                getTestConfiguration().getJDBCClient().getJDBCDriverName());
        
        Connection conn = getConnection();
        
        Statement s = conn.createStatement();
        
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        System.out.println("count: "+count);
    }
}
