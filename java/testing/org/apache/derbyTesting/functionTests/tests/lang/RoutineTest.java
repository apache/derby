/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.RoutineTest

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
*/
package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Set of tests for SQL routines.
 * This tests mainly the SQL definition of routines
 * and the server-side behaviour of routines.
 * Calling of procedures is tested in ProcedureTest.
 *
 */
public class RoutineTest extends BaseJDBCTestCase {

    public RoutineTest(String name)
    {
        super(name);
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite(RoutineTest.class, "RoutineTest");
        
        return new CleanDatabaseTestSetup(suite);
    }
    
    /**
     * Test that functions handle being called or not called
     * when it is passed a NULL argument correctly.
     * A function can be declared:
     * RETURNS NULL ON NULL INPUT - any argument being NULL means the
     * function is returns NULL without being called.
     * CALLED ON NULL INPUT (default) - function is always called regardless
     * of any arguement being NULL.
     */
    public void testFunctionNullHandling() throws SQLException, UnsupportedEncodingException
    {
        // Create three simple functions that take an integer and
        // return its value as a VARCHAR().
        int errors = runSQLCommands(
        "CREATE FUNCTION SV_NOCALL(INTEGER) RETURNS VARCHAR(10) " +
           "RETURNS NULL ON NULL INPUT " +
           "EXTERNAL NAME 'java.lang.String.valueOf'  " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA; " +
           
        "CREATE FUNCTION SV_CALL(INTEGER) RETURNS VARCHAR(10) " +
          "CALLED ON NULL INPUT " +
          "EXTERNAL NAME 'java.lang.String.valueOf' " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA; " +
          
        "CREATE FUNCTION SV_DEFAULT(INTEGER) RETURNS VARCHAR(10) " +
          "EXTERNAL NAME 'java.lang.String.valueOf' " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA; ");
        
        assertEquals("errors running DDL", 0, errors);
        
        // Simple cases of calling each function individually
        // Test each function with non-NULL and NULL values.
        PreparedStatement ps = prepareStatement("VALUES SV_NOCALL(?)");
        ps.setInt(1, 42);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "42");  
        ps.setNull(1, Types.INTEGER);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.close();
        
        
        ps = prepareStatement("VALUES SV_CALL(?)");
        ps.setInt(1, 52);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "52"); 
        
        // NULL will attempt to call the function but be blocked
        // because the Java parameter is a primitive. Since
        // the call attempt it made it is enough to show the
        // correct behaviour.
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps);
        
        ps.close();
        
        // Default behaviour maps to CALLED ON NULL INPUT
        ps = prepareStatement("VALUES SV_DEFAULT(?)");
        ps.setInt(1, 62);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "62");
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps);
        ps.close();
        
        // Test that any single argument being null causes NULL to be returned.
        errors = runSQLCommands(
                "CREATE FUNCTION CONCAT_NOCALL(VARCHAR(10), VARCHAR(10)) " +
                   "RETURNS VARCHAR(20) " +
                   "RETURNS NULL ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".concat'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA; " +
                "CREATE FUNCTION CONCAT_CALL(VARCHAR(10), VARCHAR(10)) " +
                   "RETURNS VARCHAR(20) " +
                   "CALLED ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".concat'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA; "
                   
        );  
        assertEquals("errors running DDL", 0, errors);
        
        ps = prepareStatement("VALUES CONCAT_NOCALL(?, ?)");
        ps.setString(1, "good");
        ps.setString(2, "bye");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "goodbye");
        
        ps.setString(1, null);
        ps.setString(2, "bye");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        ps.setString(1, "good");
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        ps.setString(1, null);
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        ps.close();

        ps = prepareStatement("VALUES CONCAT_CALL(?, ?)");
        ps.setString(1, "good");
        ps.setString(2, "bye");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "goodbye");
        
        ps.setString(1, null);
        ps.setString(2, "bye");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "s1NULLbye");

        ps.setString(1, "good");
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "goods2NULL");
        
        ps.setString(1, null);
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "s1NULLs2NULL");
        
        ps.close();
        
        // Now nested calls
        ps = prepareStatement(
          "VALUES CONCAT_NOCALL(CONCAT_NOCALL(?, 'RNNI'), CONCAT_CALL(?, 'CONI'))");
        
        ps.setString(1, "p1");
        ps.setString(2, "p2");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "p1RNNIp2CONI");
        
        ps.setString(1, null);
        ps.setString(2, "p2");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        ps.setString(1, "p1");
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "p1RNNIs1NULLCONI");
        
        ps.setString(1, null);
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.close();

        ps = prepareStatement(
          "VALUES CONCAT_CALL(CONCAT_NOCALL(?, 'RNNI'), CONCAT_CALL(?, 'CONI'))");
      
        ps.setString(1, "p1");
        ps.setString(2, "p2");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "p1RNNIp2CONI");
      
        ps.setString(1, null);
        ps.setString(2, "p2");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "s1NULLp2CONI");
      
        ps.setString(1, "p1");
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "p1RNNIs1NULLCONI");
      
        ps.setString(1, null);
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "s1NULLs1NULLCONI");
        ps.close();

        
        // Nested calls with SQL types that do not need casts
        // and map to primitive types. This had issues see DERBY-479
        errors = runSQLCommands(
                "CREATE FUNCTION SAME_NOCALL(INTEGER) " +
                   "RETURNS INTEGER " +
                   "RETURNS NULL ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".same'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA; " +
                "CREATE FUNCTION SAME_CALL(INTEGER) " +
                   "RETURNS INTEGER " +
                   "CALLED ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".same'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA; "
                   
        );  
        assertEquals("errors running DDL", 0, errors);
        
        ps = prepareStatement("VALUES SAME_NOCALL(SAME_NOCALL(?))");
        ps.setInt(1, 41);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "41");
        ps.setNull(1, Types.INTEGER);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.close();
        
        ps = prepareStatement("VALUES SAME_NOCALL(SAME_CALL(?))");
        ps.setInt(1, 47);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "47");
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps); // Can't pass NULL into primitive type
        ps.close();

        ps = prepareStatement("VALUES SAME_CALL(SAME_NOCALL(?))");
        ps.setInt(1, 41);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "41");
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps); // Can't pass NULL into primitive type
        ps.close();

        ps = prepareStatement("VALUES SAME_CALL(SAME_CALL(?))");
        ps.setInt(1, 53);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "53");
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps); // Can't pass NULL into primitive type
        ps.close();
        
    }
    
    /*
    ** Routine implementations called from the tests but do
    *  not use DriverManager so that this test can be used on
    *  J2ME/CDC/Foundation with JSR169.
    */
    
    public static String concat(String s1, String s2)
    {
        if (s1 == null)
            s1 = "s1NULL";
        if (s2 == null)
            s2 = "s2NULL";
        return s1.concat(s2);
    }
    
    public static int same(int i)
    {
        return i;
    }
}

