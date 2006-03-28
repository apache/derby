/*
 *
 * Derby - Class RowIdNotImplementedTest
 *
 * Copyright 2006 The Apache Software Foundation or its 
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derby.impl.jdbc.EmbedRowId;

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

    /** Default connection used by the tests. */
    private Connection con = null;

    /**
     * Create test with given name.
     *
     * @param name name of test.
     */
    public RowIdNotImplementedTest(String name) {
        super(name);
    }
    
    /**
     * Obtain default connection.
     *
     * @throws SQLException if obtaining connection fails.
     */
    public void setUp()
        throws SQLException {
        con = getConnection();
    }

    /**
     * Do rollback and close on connection.
     *
     * @throws SQLException if rollback or close fails on connection.
     */
    public void tearDown()
        throws SQLException {
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
        }
    }

    public void testRowIdInPreparedStatementSetRowId() 
        throws SQLException {
        PreparedStatement pStmt = 
            con.prepareStatement("select count(*) from sys.systables");
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
        DatabaseMetaData meta = con.getMetaData();
        RowIdLifetime rowIdLifetime = meta.getRowIdLifetime();
        assertEquals("RowIdLifetime should be ROWID_UNSUPPORTED",
            RowIdLifetime.ROWID_UNSUPPORTED,
            rowIdLifetime);
        meta = null;
    }

    public void testRowIdEquals() {
        RowId rowId = getRowId();
        try {
            rowId.equals(rowId);
            fail("RowId.equals should not be implemented");
        } catch (UnsupportedOperationException uoe) {
            // Do nothing, we are fine.
        }
    }
    
    public void testRowIdGetBytes() {
        RowId rowId = getRowId();
        try {
            rowId.getBytes();
            fail("RowId.getBytes should not be implemented");
        } catch (UnsupportedOperationException uoe) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdToString() {
        RowId rowId = getRowId();
        try {
            rowId.toString();
            fail("RowId.toString should not be implemented");
        } catch (UnsupportedOperationException uoe) {
            // Do nothing, we are fine.
        }
    }

    public void testRowIdHashCode() {
        RowId rowId = getRowId();
        try {
            rowId.hashCode();
            fail("RowId.hashCode should not be implemented");
        } catch (UnsupportedOperationException uoe) {
            // Do nothing, we are fine.
        }
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
        return con.prepareCall("values 1");
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
        return con.createStatement().executeQuery("values 1");
    }
    
    /**
     * Create a <code>RowId</code>-object.
     */
    public java.sql.RowId getRowId() {
        EmbedRowId embRowId = new EmbedRowId();
        return (java.sql.RowId)embRowId;
    }
    
    /**
     * Return test suite.
     *
     * @return test suite.
     */
    public static Test suite() {
        return new TestSuite(RowIdNotImplementedTest.class,
                             "RowIdNotImplementedTest suite");
    }
    
} // End class RowIdNotImplementedTest
