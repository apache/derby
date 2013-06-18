/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.XA40Test
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
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.*;

import java.lang.reflect.Method;
import java.sql.*;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;

import org.apache.derby.iapi.jdbc.BrokeredStatement;
import org.apache.derby.iapi.jdbc.BrokeredPreparedStatement;
import org.apache.derby.iapi.jdbc.BrokeredCallableStatement;


/**
 * Test new methods added for XA in JDBC4.
 */
public class XA40Test extends BaseJDBCTestCase {

    /** Default XADataSource used by the tests. */
    private XADataSource xads = null;

    /** Default XAConnection used by the tests. */
    private XAConnection xac = null;

    /** Default XAResource used by the tests. */
    private XAResource xar = null;

    /** Default Connection used by the tests. */
    private Connection con = null;
    
    /**
     * Create a new test with the given name.
     *
     * @param name name of the test.
     */
    public XA40Test(String name) {
        super(name);
    }

    /**
     * Create default XADataSource, XAResource, XAConnection, and
     * Connection for the tests.
     *
     * @throws SQLException if a database access exception occurs.
     */
    public void setUp() 
        throws SQLException {
        xads = J2EEDataSource.getXADataSource();
        xac = xads.getXAConnection();
        xar = xac.getXAResource();
        con = xac.getConnection();
        assertFalse("Connection must be open initially", con.isClosed());
        con.setAutoCommit(false);
    }

    /**
     * Close default connection and XAConnection if necessary.
     *
     * @throws Exception if an exception occurs.
     */
    protected void tearDown() throws Exception {
        // Close default connection
        // Check if connection is open to avoid exception on rollback.
        if (con != null && !con.isClosed()) {
            // Abort changes that may have been done in the test.
            // The test-method may however commit these itself.
            con.rollback();
            con.close();
        }
        if (xac != null) {
            xac.close();
        }
        con = null;
        xads = null;
        xac = null;
        xar = null;
        super.tearDown();
    }

    
    /**
     * Tests isPoolable(), setPoolable(boolean) and default
     * poolability for Statement, (which for XA is actually a
     * BrokeredStatement40 in embedded).
     *
     * @throws SQLException if a database access exception occurs.
     */
    public void testStatementPoolable() throws SQLException {
        Statement s = con.createStatement();
        if (usingEmbedded()) {
            assertTrue("s must be an instance of BrokeredStatement, " +
                       "but is " + s.getClass(), 
                       (s instanceof BrokeredStatement));
        }
        assertFalse("Statement must not be poolable by default", 
                    s.isPoolable()); 
        s.setPoolable(true);
        assertTrue("Statement must be poolable", s.isPoolable());

        s.setPoolable(false);
        assertFalse("Statement cannot be poolable", s.isPoolable());
    }

    /**
     * Tests isPoolable() and setPoolable(boolean) for
     * PreparedStatement, (which for XA is actually a
     * BrokeredPreparedStatement40 in embedded).
     *
     * @throws SQLException if a database access exception occurs.
     */
    public void testPreparedStatementPoolable() throws SQLException {
        PreparedStatement ps = 
            con.prepareStatement("CREATE TABLE foo(i int)");
        if (usingEmbedded()) {
            assertTrue("ps must be an instance of " + 
                       "BrokeredPreparedStatement, " +
                       "but is " + ps.getClass(), 
                       (ps instanceof BrokeredPreparedStatement));
        }
        assertTrue("PreparedStatement must be poolable by default", 
                    ps.isPoolable()); 
        ps.setPoolable(false);
        assertFalse("PreparedStatement cannot be poolable", ps.isPoolable());

        ps.setPoolable(true);
        assertTrue("PreparedStatement must be poolable", ps.isPoolable());
    }

    /**
     * Tests isPoolable() and setPoolable(boolean) and default
     * poolability for CallableStatement (which for XA is actually a
     * BrokeredCallableStatement40 in embedded).
     *
     * @throws SQLException if a database access exception occurs.
     */
    public void testCallableStatementPoolable() throws SQLException {
        CallableStatement cs = 
            con.prepareCall("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        if (usingEmbedded()) {
            assertTrue("cs must be an instance of " + 
                       "BrokeredCallableStatement, " +
                       "but is " + cs.getClass(), 
                       (cs instanceof BrokeredCallableStatement));
        }
        assertTrue("CallableStatement must be poolable by default", 
                    cs.isPoolable()); 
        cs.setPoolable(false);
        assertFalse("CallableStatement cannot be poolable", cs.isPoolable());

        cs.setPoolable(true);
        assertTrue("CallableStatement must be poolable", cs.isPoolable());
    }

    /**
     * <p>
     * Test the JDBC 4.2 statement additions to brokered and logical statements.
     * </p>
     */
    public void testLargeUpdate_jdbc4_2() throws Exception
    {
        StatementTest.largeUpdate_jdbc4_2( con );
    }

    /**
     * <p>
     * Test the JDBC 4.2 additions to brokered CallableStatements.
     * </p>
     */
    public void test_registerOutParameter_jdbc4_2() throws Exception
    {
        if ( isJava8() )
        {
            Class<?>   klass = Class.forName( "org.apache.derbyTesting.functionTests.tests.jdbc4.PreparedStatementTest42" );
            Method  method = klass.getMethod( "registerObjectTest", new Class<?>[] { Connection.class } );

            method.invoke( null, new Object[] { con } );
        }
    }

    /**
     * Create test suite for XA40Test.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(XA40Test.class);
    }
    
} // End class XA40Test
