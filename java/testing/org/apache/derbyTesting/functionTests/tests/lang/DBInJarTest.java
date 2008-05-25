/*

   Derby - Class 
       org.apache.derbyTesting.functionTests.tests.lang.DBInJarTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;


import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.BatchUpdateTest;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.Utilities;


public class DBInJarTest extends BaseJDBCTestCase {

    public DBInJarTest(String name) {
        super(name);

    }


    /**
     * Create and connect to a database in a jar.
     * @throws SQLException
     */
    public void testConnectDBInJar() throws SQLException
    {
        //      Create database to be jarred up.
        
        Connection beforejarconn = DriverManager.getConnection("jdbc:derby:testjardb;create=true");
        Statement bjstmt = beforejarconn.createStatement();  
        bjstmt.executeUpdate("CREATE TABLE TAB (I INT)");
        bjstmt.executeUpdate("INSERT INTO TAB VALUES(1)");
        shutdownDB("jdbc:derby:testjardb;shutdown=true");
        Statement stmt = createStatement();
        
        stmt.executeUpdate("CALL CREATEARCHIVE('testjardb.jar', 'testjardb','testjardb')");
        Connection jarconn = DriverManager.getConnection("jdbc:derby:jar:(testjardb.jar)testjardb");
        Statement s = jarconn.createStatement();
        
        // try to read from a table.
        ResultSet rs = s.executeQuery("SELECT * from TAB");
        JDBC.assertSingleValueResultSet(rs, "1");
        
        // Try dbmetadata call. DERBY-3546
       rs = jarconn.getMetaData().getSchemas();
       String[][] expectedRows = {{"APP",null},
               {"NULLID",null},
               {"SQLJ",null},
               {"SYS",null},
               {"SYSCAT",null},
               {"SYSCS_DIAG",null},
               {"SYSCS_UTIL",null},
               {"SYSFUN",null},
               {"SYSIBM",null},
               {"SYSPROC",null},
               {"SYSSTAT",null}};
       JDBC.assertFullResultSet(rs, expectedRows);
       shutdownDB("jdbc:derby:jar:(testjardb.jar)testjardb;shutdown=true");
              
       // cleanup databases
      File jarreddb = new File(System.getProperty("derby.system.home") + "/testjardb.jar");
      assertTrue("failed deleting " + jarreddb.getPath(),jarreddb.delete());
      removeDirectory(new File(System.getProperty("derby.system.home") + "/testjardb" ));
    }


    private void shutdownDB(String url) {
        try {
            DriverManager.getConnection(url);
            fail("Expected exception on shutdown");
        } catch (SQLException se) {
            assertSQLState("08006", se);
        }
    }
    
    
    protected static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(DBInJarTest.class);
        // Don't run with security manager, we need access to user.dir to archive
        // the database.
        return new CleanDatabaseTestSetup(SecurityManagerSetup.noSecurityManager(suite)) 
        {
            /**
             * Creates the procedure used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                stmt.execute("create procedure CREATEARCHIVE(jarName VARCHAR(20)" +
                        " , path VARCHAR(20), dbName VARCHAR(20))" +
                        " LANGUAGE JAVA PARAMETER STYLE JAVA" +
                        " NO SQL" +
                        " EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.dbjarUtil.createArchive'");
                
                
            }
        };
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite("DBInJarTest");      
        suite.addTest(baseSuite("DBInJarTest:embedded"));
        return suite;
    
    }
}
