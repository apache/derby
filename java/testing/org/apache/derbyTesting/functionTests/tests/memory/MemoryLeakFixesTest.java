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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
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
