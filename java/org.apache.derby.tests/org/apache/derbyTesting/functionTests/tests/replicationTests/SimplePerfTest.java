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
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derby.jdbc.BasicClientDataSource40;
import org.apache.derby.client.ClientDataSourceInterface;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class SimplePerfTest extends BaseJDBCTestCase
{
    
    public SimplePerfTest(String testcaseName)
    {
        super(testcaseName);
    }
    
    private static String dbPath = "UNDEFINED!";
    private static String masterHostName = "UNDEFINED!";
    private static int masterPortNo = -1;
    
    public static Test suite()
        throws Exception
    {        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        masterHostName = System.getProperty("test.serverHost", "localhost");
        masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        dbPath = System.getProperty("test.dbPath", "wombat");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("SimplePerfTest");
        
        tuplesToInsert = Integer.parseInt(System.getProperty("test.inserts", "1000"));
        commitFreq = Integer.parseInt(System.getProperty("test.commitFreq", "0"));
        
        suite.addTest(SimplePerfTest.suite(masterHostName, masterPortNo, dbPath));
        // TestSetup setup = TestConfiguration.additionalDatabaseDecorator(suite,dbPath);
        
        return (Test)suite;
    }
    private static int tuplesToInsert = 0;
    private static int commitFreq = 0; // Means autocommit.
    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort, String dbPath)
    {     
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        Test t = TestConfiguration.existingServerSuite(SimplePerfTest.class,false,serverHost,serverPort,dbPath);
        return SecurityManagerSetup.noSecurityManager(t);
   }
    
    /**
     *
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testInserts()
    throws SQLException, IOException, InterruptedException,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
           ClassNotFoundException, IllegalAccessException, InstantiationException,
           NoSuchMethodException, InvocationTargetException
    {        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        String vc = "";
        for ( int i=0;i<20000;i++ )
        {
            vc = vc+"a";
        }
        
        Connection conn = clientConnection(masterHostName, masterPortNo, dbPath);
        
        Statement s = conn.createStatement();
        try{
        s.executeUpdate("drop table t"); // Should not be required!
        } catch (java.sql.SQLException ignore){
            System.out.println("'drop table t' caused: "+ignore.getMessage());
        }
        s.executeUpdate("create table t (i integer primary key, vc varchar(20100))");
        
        PreparedStatement pSt = conn.prepareStatement("insert into t values (?,?)");
        
        conn.setAutoCommit(commitFreq == 0 ? true : false); // commitFreq == 0 means do autocommit
        
        for (int i=0;i<tuplesToInsert;i++)
        {
            pSt.setInt(1, i);
            pSt.setString(2, vc+i);
            try {pSt.execute();}
            catch (Exception e) {
                System.out.println("Exception when inserting: " + e.getMessage());
                return;
            }
            // commit after each commitFreq insert
            if ((commitFreq != 0) && ((i % commitFreq) == 0)){System.out.println("i: "+i);conn.commit();}
        }
        
        conn.commit();
        
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        System.out.println("count: "+count);
    }
    public void verifyTestInserts()
        throws SQLException, IOException, InterruptedException,
            ClassNotFoundException, IllegalAccessException,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
               InstantiationException, NoSuchMethodException, InvocationTargetException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        Connection conn = clientConnection(masterHostName, masterPortNo, dbPath);
        
        Statement s = conn.createStatement();
        
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        // System.out.println("count: "+count);
    }
    private Connection clientConnection(String hostName, int portNo, String dbPath)
            throws SQLException, ClassNotFoundException, IllegalAccessException,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                   InstantiationException, NoSuchMethodException, InvocationTargetException
    {
        ClientDataSourceInterface ds;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientDataSource");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
//IC see: https://issues.apache.org/jira/browse/DERBY-5955
            ds = new BasicClientDataSource40();
        }

        ds.setDatabaseName(dbPath);
        ds.setServerName(hostName);
        ds.setPortNumber(portNo);
        // ds.setConnectionAttributes(useEncryption(false));
        return ds.getConnection();
    }
}
