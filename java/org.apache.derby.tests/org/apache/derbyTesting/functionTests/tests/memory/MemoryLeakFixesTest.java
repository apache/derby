/*

Derby - Class org.apache.derbyTesting.functionTests.tests.memory.MemoryLeakFixesTest

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

package org.apache.derbyTesting.functionTests.tests.memory;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;
import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * MemoryLeakFixesTest should be run with -Xmx16M or run 
 * as part of the ant junit-lowmem target. The test is
 * generally successful if it does not run out of memory.
 * Results are not typically checked.
 *
 */

public class MemoryLeakFixesTest extends BaseJDBCTestCase {

    public MemoryLeakFixesTest(String name) {
        super(name);
    }
    private static long HALFMB = 500*1024;
    
    private static int numRows = 100;
    private static int numPreparedStmts = 2000;


    // Tests prepared statements are not leaked if not explicitly closed by
    // user (DERBY-210)
    public void testPrepStmtD210() throws Exception
    {
        Connection conn = getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        conn.setAutoCommit(false);

        Statement stmt = createStatement();

        stmt.execute("create table t1 (lvc  LONG VARCHAR)");
        stmt.close();

        String insertTabSql = "insert into t1 values(?)";
        ps = conn.prepareStatement(insertTabSql);
        for (int i = 0; i < numRows; i++)
        {
            ps.setString(1,"Hello" + i);
            ps.executeUpdate();
        }
        ps.close();



        String selTabSql = "select * from t1";

        for (int i = 0 ; i  < numPreparedStmts; i++)
        {
            ps = conn.prepareStatement(selTabSql);
            rs = ps.executeQuery();

            while (rs.next())
            {
                rs.getString(1);
            }

            rs.close();

            // Do not close the prepared statement
            // because we want to check that it is
            // garbage collected
            //ps.close();
            if ((i % 100) == 0)
                runFinalizerIfNeeded();
        }
        conn.commit();
    }

    // Tests re-execution of a statement without closing the result
    // set (DERBY-557).
    public void testReExecuteD557() throws Exception {
        println("DERBY-557: reExecuteStatementTest() ");
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        for (int i = 0; i < 50000; i++) {
            if ((i % 1000) == 0)
                runFinalizerIfNeeded();
            ResultSet rs = stmt.executeQuery("values(1)");
            // How silly! I forgot to close the result set.
        }
        conn.commit();
        stmt.close();
        conn.close();
    }

    /**
     * Test fix for leak if ResultSets are not closed.
     * @throws Exception
     */
    public void testResultSetgcD3316() throws Exception {
          println("DERBY-3316: Multiple statement executions ");
                
          Connection conn = getConnection();
          Statement s = createStatement();
          s.executeUpdate("CREATE TABLE TAB (col1 varchar(32672))");
          PreparedStatement ps = conn.prepareStatement("INSERT INTO TAB VALUES(?)");
          ps.setString(1,"hello");
          ps.executeUpdate();
          ps.setString(1,"hello");
          ps.executeUpdate();
          ps.close();
          for (int i = 0; i < 2000; i++)
          {
                  s = conn.createStatement();
                  ResultSet rs = s.executeQuery("SELECT * from tab");
                  // drain the resultset
                  while (rs.next());
                  // With DERBY-3316, If I don't explicitly close the resultset or 
                  // statement, we get a leak.
                  //rs.close();
                  //s.close();
                  if ((i % 100) == 0) 
                       runFinalizerIfNeeded();
                  
          }    
          // close the connection to free up all the result sets that our sloppy 
          // user didn't close.
          conn.close();
          conn = getConnection();
          s = conn.createStatement();
          s.executeUpdate("DROP TABLE TAB");
          s.close();
          conn.close();
       }

    /**
     * Tests that the memory usage dosen't increase for each database that is
     * created.
     * <p>
     * The tests is primarily written to ensure that the automatic index
     * statistics daemon doesn't cause memory leakage. One one database is
     * active/booted at a time.
     * <p>
     * See DERBY-5336.
     *
     * @throws SQLException if something goes wrong
     */
    public void testRepeatedDatabaseCreationWithAutoStats()
//IC see: https://issues.apache.org/jira/browse/DERBY-5336
            throws SQLException {
        final String DB_NAME = "derby-memory-test";
        final File DB_DIR = new File("system", DB_NAME);
        DataSource ds = JDBCDataSource.getDataSource(DB_NAME);
    
        // using -Xmx32M typically causes the out of memory error to appear
        // within 20 iterations;  this program was run on Windows 7 64-bit using
        // jdk1.6.0_26
        int iter = 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-5472
        while (iter < 20) {
            
            println("-- " + iter++);
            
            // remove database directory so we can start fresh each time;
            // the memory leak also manifests when a different directory is
            // used each time through, i.e. it is not required that the
            // database be created in the same location over and over
            if (PrivilegedFileOpsForTests.exists(DB_DIR)) {
                assertDirectoryDeleted(DB_DIR);
            }
            
            // create the database
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
            Connection conn = ds.getConnection();
            JDBCDataSource.clearStringBeanProperty(ds, "createDatabase");

            // Disable auto-commit to speed up insert statements.
//IC see: https://issues.apache.org/jira/browse/DERBY-5472
            conn.setAutoCommit(false);
            
            // we'll use this one statement the whole time this db is open
            Statement s = conn.createStatement();
            
            // create a simple schema; the presence of the index is important
            // somehow as the memory leak does not appear without it
            s.executeUpdate("CREATE TABLE TEST (CINT INT)");
            s.executeUpdate("CREATE INDEX NDX ON TEST (CINT)");

            // Insert enough data into the table to make a select from the
            // table trigger the daemon that updates index cardinality
            // statistics. derby.storage.indexStats.debug.createThreshold is
            // currently 100.
//IC see: https://issues.apache.org/jira/browse/DERBY-5472
            PreparedStatement ins =
                    conn.prepareStatement("INSERT INTO TEST VALUES ?");
            for (int i = 0; i < 200; i++) {
                ins.setInt(1, i);
                ins.executeUpdate();
            }
            ins.close();

            conn.commit();

            // Execute a query against the table. This will make the index
            // statistics daemon do its work.
            JDBC.assertDrainResults(
                    s.executeQuery("SELECT * FROM TEST WHERE CINT=42"));

            // done with statement and connection
            s.close();
            conn.rollback();
            conn.close();
            
            // shutdown this database, but not entire derby engine
            JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
            try {
                ds.getConnection();
                fail("Expected shutdown exception");
            } catch (SQLException e) {
                assertSQLState("08006", e);
            } finally {
                JDBCDataSource.clearStringBeanProperty(ds, "shutdownDatabase");
            }

        }

        // extra sanity check making sure that the database was created in the
        // location we assumed
        assertTrue(PrivilegedFileOpsForTests.exists(DB_DIR));
    }

    /**
     * runFinalizerIfNeeded is called periodically for DERBY-4200. With the IBM
     * JVM in some modes, like soft real time or in a single threaded
     * environment on vmware. The finalizer may lag behind the program so much
     * we get an OOM. If we get low on memory, force the finalizer to catch up.
     * 
     */
    private static void runFinalizerIfNeeded() {
        
        Runtime rt = Runtime.getRuntime();
        if (rt.freeMemory() < HALFMB){
            println("Waiting for finalizer ");
            rt.runFinalization();
            

        }

    }
    
    public static Test suite() {
        Test suite = TestConfiguration.defaultSuite(MemoryLeakFixesTest.class);
        return suite;
    }
}
