/*
 
   Derby - Class 
        org.apache.derbyTesting.functionTests.tests.jdbc4.TestJDBC40Exception
 
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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Types;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derby.iapi.reference.Property;

public class TestJDBC40Exception extends BaseJDBCTestCase {

    /** Timeout value to restore to in tearDown(). */
    private int oldTimeout;
    
    public TestJDBC40Exception(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTestSuite(TestJDBC40Exception.class);
        return testSuite;
    }

    protected void setUp() throws SQLException {
        Statement s = createStatement();
        s.execute("create table EXCEPTION_TABLE1 (id integer " +
                  "primary key, data varchar (5))");
        s.execute("insert into EXCEPTION_TABLE1 (id, data)" +
                  "values (1, 'data1')");
        // lower waitTimeout, otherwise testTimeout takes forever
        oldTimeout = getWaitTimeout();
        setWaitTimeout(2);
        s.close();
    }

    protected void tearDown() throws Exception {
        createStatement().execute("drop table EXCEPTION_TABLE1");
        setWaitTimeout(oldTimeout);
        super.tearDown();
    }

    /**
     * Set the value of the waitTimeout property.
     *
     * @param timeout time in seconds to wait for a lock
     * @exception SQLException if a database error occurs
     */
    private void setWaitTimeout(int timeout) throws SQLException {
        PreparedStatement ps = prepareStatement(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.locks.waitTimeout', ?)");
        ps.setInt(1, timeout);
        ps.execute();
        ps.close();
    }

    /**
     * Get the value of the waitTimeout property. If no timeout is set, use
     * org.apache.derby.iapi.reference.Property.WAIT_TIMEOUT_DEFAULT.
     *
     * @return the current timeout in seconds
     * @exception SQLException if a database error occurs
     */
    private int getWaitTimeout() throws SQLException {
        CallableStatement cs = prepareCall(
            "{ ? = CALL SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
            "'derby.locks.waitTimeout') }");
        cs.registerOutParameter(1, Types.VARCHAR);
        cs.execute();
        int timeout = cs.getInt(1);
        if (cs.wasNull()) {
            timeout = Property.WAIT_TIMEOUT_DEFAULT;
        }
        cs.close();
        return timeout;
    }
    
    public void testIntegrityConstraintViolationException()
            throws SQLException {
        try {
            createStatement().execute(
                "insert into EXCEPTION_TABLE1 (id, data) values (1, 'data1')");
            fail("Statement didn't fail.");
        } catch (SQLIntegrityConstraintViolationException e) {
            assertTrue("Unexpected SQL State: " + e.getSQLState(),
                       e.getSQLState().startsWith("23"));
        }
    }
    
    public void testDataException() throws SQLException {
        try {
            createStatement().execute(
                "insert into EXCEPTION_TABLE1 (id, data)" +
                "values (2, 'data1234556')");
            fail("Statement didn't fail.");
        } catch (SQLDataException e) {
            assertTrue("Unexpected SQL State: " + e.getSQLState(),
                       e.getSQLState().startsWith("22"));
        }
    }
    
    public void testConnectionException() throws SQLException {
        Statement stmt = createStatement();
        getConnection().close();
        try {
            stmt.execute("select * from exception1");
            fail("Statement didn't fail.");
        } catch (SQLTransientConnectionException cone) {
            assertTrue("Unexpected SQL State: " + cone.getSQLState(),
                       cone.getSQLState().startsWith("08"));
        }
    }
    
    public void testSyntaxErrorException() throws SQLException {
        try {
            createStatement().execute("insert into EXCEPTION_TABLE1 " +
                                      "(id, data) values ('2', 'data1')");
            fail("Statement didn't fail.");
        } catch (SQLSyntaxErrorException e) {
            assertTrue("Unexpected SQL State: " + e.getSQLState(),
                       e.getSQLState().startsWith("42"));
        }
    }

    public void testTimeout() throws SQLException {
        Connection con1 = openDefaultConnection();
        Connection con2 = openDefaultConnection();
        con1.setAutoCommit(false);
        con2.setAutoCommit(false);
        con1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        con2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        con1.createStatement().execute(
            "select * from EXCEPTION_TABLE1 for update");
        try {
            con2.createStatement().execute(
                "select * from EXCEPTION_TABLE1 for update");
            fail("Statement didn't fail.");
        } catch (SQLTransactionRollbackException e) {
            assertTrue("Unexpected SQL State: " + e.getSQLState(),
                       e.getSQLState().startsWith("40"));
        }
        con1.rollback();
        con1.close();
        con2.rollback();
        con2.close();
    }
}
