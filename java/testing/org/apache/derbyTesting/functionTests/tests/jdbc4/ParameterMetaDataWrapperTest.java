/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.ParameterMetaDataWrapperTest
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
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

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests of the <code>java.sql.ParameterMetaData</code> JDBC40 API
 */
public class ParameterMetaDataWrapperTest extends BaseJDBCTestCase {
    
    //Default PreparedStatement used by the tests
    private PreparedStatement ps = null;
    //Default ParameterMetaData object used by the tests
    private ParameterMetaData pmd = null;
    
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
    protected void setUp() 
        throws SQLException {
        ps   = prepareStatement("values 1");
        pmd  = ps.getParameterMetaData();
    }

    /**
     * Close default Prepared Statement and connection.
     *
     * @throws SQLException if closing of the connection or the callable
     *                      statement fail.
     */
    protected void tearDown()
        throws Exception {
        if(ps != null && !ps.isClosed())
            ps.close();
        ps = null;
        pmd = null;
        
        super.tearDown();
    }

    public void testIsWrapperForParameterMetaData() throws SQLException {
        assertTrue(pmd.isWrapperFor(ParameterMetaData.class));
    }

    public void testUnwrapParameterMetaData() throws SQLException {
        ParameterMetaData pmd2 = pmd.unwrap(ParameterMetaData.class);
        assertSame("Unwrap returned wrong object.", pmd, pmd2);
    }

    public void testIsNotWrapperForResultSet() throws SQLException {
        assertFalse(pmd.isWrapperFor(ResultSet.class));
    }

    public void testUnwrapResultSet() {
        try {
            ResultSet rs = pmd.unwrap(ResultSet.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(
            ParameterMetaDataWrapperTest.class);
    }
}
