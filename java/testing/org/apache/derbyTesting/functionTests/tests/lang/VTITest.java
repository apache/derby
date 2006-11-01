/*
    Derby - Class org.apache.derbyTesting.functionTests.tests.lang.VTITest

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

 
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Add tests that use VTI 
 */
 public class VTITest extends BaseJDBCTestCase {
     

     public static Test suite()
     {
         TestSuite suite = new TestSuite("VTITest");
         // requires DriverManager support
         if (JDBC.vmSupportsJDBC2())
            suite.addTest(new VTITest("bulkInsertVtiTest"));
         
         return suite;
     }
     

     public VTITest(String name) {
         super(name);
     }
     

     /**
      * Setup: create a table for this test
      */
     protected void setUp() throws SQLException {
         Statement stmt = createStatement();
         stmt.execute("CREATE TABLE warehouse (id int)");
         stmt.close();
     }
     
     /**
      * Drop the table created during setup.
      * @throws Exception 
      */
     protected void tearDown()
         throws Exception {
         Statement stmt = createStatement();
         stmt.execute("DROP TABLE warehouse");
         stmt.close();
         super.tearDown();
     }
 
  
     /**
      * Execute SYSCS_BULK_INSERT procedure to insert rows.
      * @throws SQLException
      */
     public void bulkInsertVtiTest()
     throws SQLException
     {
        int expectedRows = 10;
        Statement stmt = createStatement();
        stmt.execute("call  SYSCS_UTIL.SYSCS_BULK_INSERT('APP','WAREHOUSE'," +
                "'org.apache.derbyTesting.functionTests.tests.lang.WarehouseVTI',"
                +"\'"+expectedRows+"')");
        stmt.close();
        stmt = createStatement();
        ResultSet  rs  = stmt.executeQuery("SELECT COUNT(*) from warehouse");
        rs.next();
        assertEquals(expectedRows,rs.getInt(1));
        rs.close();
        stmt.close();
     }
 }   
