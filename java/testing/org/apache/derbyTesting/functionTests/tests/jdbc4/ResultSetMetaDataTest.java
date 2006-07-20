/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.ResultSetMetaDataTest
 
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
package org.apache.derbyTesting.functionTests.tests.jdbc4;

import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;

import java.sql.*;
import javax.sql.*;


public class ResultSetMetaDataTest extends BaseJDBCTestCase {
    //classes that will be used for the test
    private Connection        conn =null;
    private PreparedStatement ps   =null;
    private ResultSet         rs   =null;
    //The ResultSetMetaData object that will be used throughout the test
    private ResultSetMetaData rsmd =null;
    
    /**
     *
     * Create a test with the given name.
     *
     * @param name name of the test.
     *
     */
    public ResultSetMetaDataTest(String name) {
        super(name);
    }
    
    /**
     * Create a default DataSource
     */
    public void setUp() throws SQLException {
        conn =   getConnection();
        ps   =   conn.prepareStatement("select count(*) from sys.systables");
	rs   =   ps.executeQuery();
        rsmd =   rs.getMetaData();
    }
    
    /**
     * 
     * Initialize the ds to null once the tests that need to be run have been 
     * run
     */
    public void tearDown() throws SQLException {
        if(rs != null && !rs.isClosed())
            rs.close();
        if(ps != null && !ps.isClosed())
            ps.close();
        if(conn != null && !conn.isClosed()) {
            conn.rollback();
            conn.close();
        }
    }

    public void testIsWrapperForResultSetMetaData() throws SQLException {
        assertTrue(rsmd.isWrapperFor(ResultSetMetaData.class));
    }

    public void testUnwrapResultSetMetaData() throws SQLException {
        ResultSetMetaData rsmd2 = rsmd.unwrap(ResultSetMetaData.class);
        assertSame("Unwrap returned wrong object.", rsmd, rsmd2);
    }

    public void testIsWrapperForResultSet() throws SQLException {
        assertFalse(rsmd.isWrapperFor(ResultSet.class));
    }

    public void testUnwrapResultSet() {
        try {
            ResultSet rs = rsmd.unwrap(ResultSet.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        return (new TestSuite(ResultSetMetaDataTest.class,
                              "ResultSetMetaDataTest suite"));
    }
}
