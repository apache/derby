/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.ParameterMetaDataWrapperTest
 
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

/**
 * Tests of the <code>java.sql.ParameterMetaData</code> JDBC40 API
 */
public class ParameterMetaDataWrapperTest extends BaseJDBCTestCase {
    
    //Default Connection used by the tests
    Connection conn = null;
    //Default PreparedStatement used by the tests
    PreparedStatement ps = null;
    //Default ParameterMetaData object used by the tests
    ParameterMetaData pmd = null;
    
    /**
     * Create a test with the given name
     *
     * @param name String name of the test
     */
    public ParameterMetaDataWrapperTest(String name) {
        super(name);
    }
    
    /**
     * Create a default Prepared Statement and connection.
     *
     * @throws SQLException if creation of connection or callable statement
     *                      fail.
     */
    public void setUp() 
        throws SQLException {
        conn = getConnection();
        ps   = conn.prepareStatement("values 1");
        pmd  = ps.getParameterMetaData();
    }

    /**
     * Close default Prepared Statement and connection.
     *
     * @throws SQLException if closing of the connection or the callable
     *                      statement fail.
     */
    public void tearDown()
        throws SQLException {
        if(ps != null && !ps.isClosed())
            ps.close();
        if(conn != null && !conn.isClosed())
            conn.close();
    }
    
    /**
     *
     * Tests the wrapper methods isWrapperFor and unwrap. Test
     * for the case when isWrapperFor returns true and we call unwrap
     * The test is right now being run in the embedded case only
     *
     */
    public void testisWrapperReturnsTrue() throws SQLException {
        Class<ParameterMetaData> wrap_class = ParameterMetaData.class;
        
        //The if should return true enabling us  to call the unwrap method
        //without throwing  an exception
        if(pmd.isWrapperFor(wrap_class)) {
            try {
                ParameterMetaData pmd1 =
                        (ParameterMetaData)pmd.unwrap(wrap_class);
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
        //test for the case when isWrapper returns false
        //using some class that will return false when
        //passed to isWrapperFor
        Class<ResultSet> wrap_class = ResultSet.class;
        
        //returning false is the correct behaviour in this case
        //Generate a message if it returns true
        if(pmd.isWrapperFor(wrap_class)) {
            fail("isWrapperFor wrongly returns true");
        } else {
            try {
                ResultSet rs1 = (ResultSet)
                pmd.unwrap(wrap_class);
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
        return (new TestSuite(ParameterMetaDataWrapperTest.class,
                              "ParameterMetaDataWrapperTest suite"));
    }
}
