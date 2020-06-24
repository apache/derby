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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;


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

    /**
     * Test for fix of DERBY-4381, by testing the connection to a jar 
     * with a closing parenthesis / round bracket in the name. 
     * DERBY-4381 describes the problem when this round bracket
     * is in the path, but the cause is the same.
     */
    public void testConnectParenDBInJar() throws SQLException
    {
        //      Create database to be jarred up.
        Connection beforejarconn = DriverManager.getConnection(
                "jdbc:derby:testparjardb;create=true");
        Statement bjstmt = beforejarconn.createStatement();  
        bjstmt.executeUpdate("CREATE TABLE PARTAB (I INT)");
        bjstmt.executeUpdate("INSERT INTO PARTAB VALUES(1)");
        shutdownDB("jdbc:derby:testparjardb;shutdown=true");
        Statement stmt = createStatement();
        
        stmt.executeUpdate(
                "CALL CREATEARCHIVE('test)jardb.jar', " +
                "'testparjardb','testparjardb')");
        Connection jarconn = DriverManager.getConnection(
                "jdbc:derby:jar:(test)jardb.jar)testparjardb");
        Statement s = jarconn.createStatement();
        
        // try to read from a table.
        ResultSet rs = s.executeQuery("SELECT * from PARTAB");
        JDBC.assertSingleValueResultSet(rs, "1");
        
        shutdownDB("jdbc:derby:jar:(test)jardb.jar)testparjardb;shutdown=true");
        
        // cleanup databases
        File jarredpardb = new File(System.getProperty("derby.system.home") 
                + "/test)jardb.jar");
        assertTrue("failed deleting " +
                jarredpardb.getPath(),jarredpardb.delete());
        removeDirectory(new File(System.getProperty("derby.system.home") 
                + "/testparjardb" ));
    }
    
    
    
    /**
     * Test various queries that use a hash table that may be spilled to disk
     * if it grows too big. Regression test case for DERBY-2354.
     */
    public void testSpillHashToDisk() throws SQLException {
        createDerby2354Database();

        Connection jarConn =
            DriverManager.getConnection("jdbc:derby:jar:(d2354db.jar)d2354db");

        Statement stmt = jarConn.createStatement();

        // The following statement used to fail with "Feature not implemented"
        // or "Container was opened in read-only mode" before DERBY-2354. It
        // only fails if the hash table used for duplicate elimination spills
        // to disk, which happens if the hash table gets bigger than 1% of the
        // total amount of memory allocated to the JVM. This means it won't
        // expose the bug if the JVM runs with very high memory settings (but
        // it has been tested with 1 GB heap size and then it did spill to
        // disk).
        JDBC.assertDrainResults(
                stmt.executeQuery("select distinct x from d2354"),
                40000);

        // Hash joins have the same problem. Force the big table to be used as
        // the inner table in the hash join.
        JDBC.assertEmpty(stmt.executeQuery(
                "select * from --DERBY-PROPERTIES joinOrder = FIXED\n" +
                "sysibm.sysdummy1 t1(x),\n" +
                "d2354 t2 --DERBY-PROPERTIES joinStrategy = HASH\n" +
                "where t1.x = t2.x"));

        // Scrollable result sets keep the rows they've visited in a hash
        // table, so they may also need to store data on disk temporarily.
        Statement scrollStmt = jarConn.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        JDBC.assertDrainResults(
                scrollStmt.executeQuery("select * from d2354"),
                40000);

        stmt.close();
        scrollStmt.close();
        jarConn.close();

        // Cleanup. Shut down the database and delete it.
        shutdownDB("jdbc:derby:jar:(d2354db.jar)d2354db;shutdown=true");
        removeFiles(new String[] {
            System.getProperty("derby.system.home") + "/d2354db.jar"
        });
    }

    /**
     * Create a database in a jar for use in {@code testSpillHashToDisk}.
     */
    private void createDerby2354Database() throws SQLException {
        // First create an ordinary database with a table.
        Connection conn =
            DriverManager.getConnection("jdbc:derby:d2354db;create=true");
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        s.execute("create table d2354 (x varchar(100))");
        s.close();

        // Insert 40000 unique values into the table. The values should be
        // unique so that they all occupy an entry in the hash table used by
        // the DISTINCT query in the test, and thereby increase the likelihood
        // of spilling to disk.
        PreparedStatement insert =
            conn.prepareStatement(
                "insert into d2354 values ? || " +
                "'some extra data to increase the size of the table'");
        for (int i = 0; i < 40000; i++) {
            insert.setInt(1, i);
            insert.executeUpdate();
        }
        insert.close();

        conn.commit();
        conn.close();

        // Shut down the database and archive it in a jar file.
        shutdownDB("jdbc:derby:d2354db;shutdown=true");

        createStatement().execute(
            "CALL CREATEARCHIVE('d2354db.jar', 'd2354db', 'd2354db')");

        // Clean up the original database directory. We don't need it anymore
        // now that we have archived it in a jar file.
        removeDirectory(
            new File(System.getProperty("derby.system.home") + "/d2354db"));
    }
    
    protected static Test baseSuite(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(name);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("DBInJarTest");
        suite.addTest(baseSuite("DBInJarTest:embedded"));
        return suite;
    
    }
}
