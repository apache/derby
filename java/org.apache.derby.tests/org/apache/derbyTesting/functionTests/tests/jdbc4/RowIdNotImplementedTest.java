/*
 *
 * Derby - Class RowIdNotImplementedTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.*;

import java.sql.*;

/**
 * Test that all methods and functionality related to RowId reflect that it
 * has not yet been implemented.
 * The tests are written to be run with JDK 1.6.
 * All methods that throws SQLException, should utilize the 
 * SQLFeatureNotSupportedException-subclass. Methods unable to throw
 * SQLException, must throw java.lang.UnsupportedOperationException.
 * As RowId is implemented, tests demonstrating correctness of the API should
 * be moved into the proper test classes (for instance, test updateRowId in
 * the test class for ResultSet).
 * The reason for specifying all tests here was mainly because there were no
 * existing JUnit tests for the various classes implementing RowId methods.
 */
public class RowIdNotImplementedTest 
    extends BaseJDBCTestCase {

    /**
     * Create test with given name.
     *
     * @param name name of test.
     */
    public RowIdNotImplementedTest(String name) {
        super(name);
    }
    

    public void testRowIdInPreparedStatementSetRowId() 
        throws SQLException {
        PreparedStatement pStmt = 
            prepareStatement("select count(*) from sys.systables");
        try {
            pStmt.setRowId(1, null);
            fail("PreparedStatement.setRowId should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }
    
    public void testRowIdInCallableStatementGetRowIdInt()
        throws SQLException {
        CallableStatement cStmt = getCallableStatement();
        try {
            cStmt.getRowId(1);
            fail("CallableStatement.getRowId(int) should not be implemented.");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInCallableStatementGetRowIdString()
        throws SQLException {
        CallableStatement cStmt = getCallableStatement();
        try {
            cStmt.getRowId("some-parameter-name");
            fail("CallableStatement.getRowId(String) should not be " +
                 "implemented.");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInCallableStatementSetRowId()
        throws SQLException {
        CallableStatement cStmt = getCallableStatement();
        try {
            cStmt.setRowId("some-parameter-name", null);
            fail("CallableStatement.setRowId should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInResultSetGetRowIdInt()
        throws SQLException {
        ResultSet rs = getResultSet();
        try {
            rs.getRowId(1);
            fail("ResultSet.getRowId(int) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }
    
    public void testRowIdInResultSetGetRowIdString()
        throws SQLException {
        ResultSet rs = getResultSet();
        try {
            rs.getRowId("some-parameter-name");
            fail("ResultSet.getRowId(String) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInResultSetUpdateRowIdInt()
        throws SQLException {
        ResultSet rs = getResultSet();
        try {
            rs.updateRowId(1, null);
            fail("ResultSet.updateRowId(int) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInResultSetUpdateRowIdString()
        throws SQLException {
        ResultSet rs = getResultSet();
        try {
            rs.updateRowId("some-parameter-name", null);
            fail("ResultSet.updateRowId(String) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdInDatabaseMetaDataRowIdLifeTime() 
        throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        DatabaseMetaData meta = getConnection().getMetaData();
        RowIdLifetime rowIdLifetime = meta.getRowIdLifetime();
        assertEquals("RowIdLifetime should be ROWID_UNSUPPORTED",
            RowIdLifetime.ROWID_UNSUPPORTED,
            rowIdLifetime);
        meta = null;
    }

    /**
     * Create a callable statement.
     *
     * @return a <code>CallableStatement</code>
     * @throws SQLException if creation of CallableStatement fails.
     */
    private CallableStatement getCallableStatement() 
        throws SQLException {
        // No need to actuall call a stored procedure.
        return prepareCall("values 1");
    }

    /**
     * Create a resultset.
     *
     * @return a <code>ResultSet</code>
     * @throws SQLException if creation of ResultSet fails.
     */
    private ResultSet getResultSet()
        throws SQLException {
        // Create a very simple resultset.
        return createStatement().executeQuery("values 1");
    }
    
    /**
     * Return test suite.
     *
     * @return test suite.
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-2023
//IC see: https://issues.apache.org/jira/browse/DERBY-2047
        return TestConfiguration.defaultSuite(RowIdNotImplementedTest.class);
    }
    
} // End class RowIdNotImplementedTest
