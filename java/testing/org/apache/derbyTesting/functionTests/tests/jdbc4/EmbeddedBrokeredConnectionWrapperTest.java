/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.EmbeddedBrokeredConnectionWrapperTest
 
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

import java.sql.*;
import javax.sql.*;
import junit.framework.*;
import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;

public class EmbeddedBrokeredConnectionWrapperTest extends BaseJDBCTestCase {
    //Classes that will be used in this test
    private PooledConnection  pConn   = null;
    private Connection        conn    = null;
    
    /**
     *
     * Create a test with the given name.
     *
     * @param name name of the test.
     *
     */
    public EmbeddedBrokeredConnectionWrapperTest(String name) {
        super(name);
    }
    
    /**
     * Create the classes that will be used in the
     * test
     *
     */
    public void setUp() throws SQLException {
        //The ConnectionPoolDataSource object
        //used to get a PooledConnection object
        ConnectionPoolDataSource cpDataSource = getConnectionPoolDataSource();
        pConn = cpDataSource.getPooledConnection();
        //doing a getConnection() returns a Connection object 
        //that internally contains a BrokeredConnection40 object
        //this is then used to check the wrapper object
        conn = pConn.getConnection();
    }
    
    /**
     * uninitialize pConn and conn 
     */
    public void tearDown() throws SQLException {
        if(conn != null && !conn.isClosed()) {
            conn.rollback();
            conn.close();
        }
        if(pConn != null) {
            pConn.close();
        }
    }
    
    /**
     *
     * Tests the wrapper methods isWrapperFor and unwrap. Test
     * for the case when isWrapperFor returns true and we call unwrap
     * The test is right now being run in the embedded case only
     *
     */
    public void testisWrapperReturnsTrue() throws SQLException {
        Class<Connection> wrap_class = Connection.class;
        
        //The if should return true enabling us  to call the unwrap method
        //without throwing  an exception
        if(conn.isWrapperFor(wrap_class)) {
            try {
                Connection conn1 =
                        (Connection)conn.unwrap(wrap_class);
            }
            catch(SQLException sqle) {
                fail("Unwrap wrongly throws a SQLException");
            }
        } else {
            fail("isWrapperFor wrongly returns false");
        }
    }
    
    /**
     *
     * Tests the wrapper methods isWrapperFor and unwrap. Test
     * for the case when isWrapperFor returns false and we call unwrap
     * The test is right now being run in the embedded case only
     *
     */
    public void testisWrapperReturnsFalse() throws SQLException {
        Class<ResultSet> wrap_class = ResultSet.class;
        
        //returning false is the correct behaviour in this case
        //Generate a message if it returns true
        if(conn.isWrapperFor(wrap_class)) {
            fail("isWrapperFor wrongly returns true");
        } else {
            try {
                ResultSet rs1 = (ResultSet)
                conn.unwrap(wrap_class);
                fail("unwrap does not throw the expected " +
                        "exception");
            } catch (SQLException sqle) {
                //calling unwrap in this case throws an SQLException
                //check that this SQLException has the correct SQLState
                if(!SQLStateConstants.UNABLE_TO_UNWRAP.equals(sqle.getSQLState())) {
                    throw sqle;
                }
            }
        }
    }
    
    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        return (new TestSuite(EmbeddedBrokeredConnectionWrapperTest.class,
                              "EmbeddedBrokeredConnectionWrapperTest suite"));
    }
}
