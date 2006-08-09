/*
 
   Derby - Class ResultSetCloseTest
 
   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;

import java.sql.*;

/**
 * This class is used to test the fix for DERBY-694.
 *
 * A brief description of DERBY-694 (Got from the description in JIRA)
 *
 * 1) Autocommit off.
 * 2) Have two prepared statements, calling executeQuery() on both
 * 3) Gives two result sets. Can fetch data from both with next().
 * 4) If one statement gets an exception (say, caused by a division by zero)
 * 5) not only this statement's result set is closed, but also the other open
 *    resultset. This happens with the client driver, whereas in embedded mode,
 *    the other result set is unaffected by the exception in the first result set
 *    (as it should be).
 *
 */
public class ResultSetCloseTest extends BaseJDBCTestCase {
    
    Connection con        = null;
    Statement  s          = null;
    PreparedStatement ps1 = null;
    PreparedStatement ps2 = null;
    ResultSet         rs1 = null;
    ResultSet         rs2 = null;
    
    /**
     * Create the tables and the Connection and PreparedStatements that will
     * be used in this test.
     */
    public void setUp()
    throws SQLException {
        con = getConnection();
        con.setAutoCommit(false);
        
        s = con.createStatement();
        
        s.execute("create table t1 (a int)");
        
        s.execute("insert into t1 values(1)");
        s.execute("insert into t1 values(0)");
        s.execute("insert into t1 values(2)");
        s.execute("insert into t1 values(3)");
        
        con.commit();
        
        ps1 = con.prepareStatement("select * from t1");
        
        ps2 = con.prepareStatement("select 10/a from t1");
    }
    
    /**
     * Test that the occurence of the exception in one of the PreparedStatements
     * does not result in the closure of the ResultSet associated with the other
     * Prepared Statements.
     *
     * STEPS :
     * 1) Execute the first PreparedStatement. This should not cause any
     *    SQLException.
     * 2) Now execute the second PreparedStatement. This causes
     *    the expected Divide by zero exception.
     * 3) Now access the first resultset again to ensure this is still open.
     *
     */
    public void testResultSetDoesNotClose() throws SQLException {
        rs1 = ps1.executeQuery();
        
        try {
            rs2 = ps2.executeQuery();
            while(rs2.next());
        } catch(SQLException sqle) {
            //Do Nothing expected exception
        }
        
        while(rs1.next());
        
        con.commit();
    }
    
    /**
     * Destroy the objects used in this test.
     */
    public void tearDown()
    throws SQLException {
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
        }
        
        con = null;
    }
    
    /**
     * Create the test with the given name.
     *
     * @param name name of the test.
     */
    public ResultSetCloseTest(String name) {
        super(name);
    }
    
    /**
     * Create test suite for this test.
     */
    public static Test suite() {
        return new TestSuite(ResultSetCloseTest.class,"ResultSetCloseTest suite");
    }
    
}
