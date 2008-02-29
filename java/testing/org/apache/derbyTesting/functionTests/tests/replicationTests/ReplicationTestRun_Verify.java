/*

Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationTestRun

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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.functionTests.tests.derbynet.PrepareStatementTest;
import org.apache.derbyTesting.functionTests.tests.lang.AnsiTrimTest;
import org.apache.derbyTesting.functionTests.tests.lang.CreateTableFromQueryTest;
import org.apache.derbyTesting.functionTests.tests.lang.SimpleTest;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBCClient;
import org.apache.derbyTesting.junit.TestConfiguration;

public class ReplicationTestRun_Verify extends BaseJDBCTestCase
{
    
    public ReplicationTestRun_Verify(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("*** ReplicationTestRun_Verify.suite()");
        
        TestSuite suite = new TestSuite("ReplicationTestRun_Verify");
        System.out.println("*** Done new TestSuite()");
        
        String masterHostName = System.getProperty("test.serverHost", "localhost");
        int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        suite.addTest(ReplicationTestRun_Verify.simpleVerify(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.simpleTest())");
        
        return (Test)suite;
    }

    private static Test simpleVerify(String serverHost, int serverPort)
    {
        return TestConfiguration.existingServerSuite(ReplicationTestRun_Verify.class,
                false,serverHost,serverPort);
    }
    
    public void test() // Verification code..
    throws SQLException
    {
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select SCHEMAID, TABLENAME from sys.systables");
        while (rs.next())
        {
            System.out.println(rs.getString(1) + " " + rs.getString(2));
        }
    }
    
}
