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

import java.sql.Connection;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;

import javax.sql.DataSource;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

public class TestJDBC40Exception extends BaseJDBCTestCase {

    public TestJDBC40Exception(String name) {
        super(name);
    }

    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-2023
//IC see: https://issues.apache.org/jira/browse/DERBY-2047
        Test suite = TestConfiguration.defaultSuite(TestJDBC40Exception.class);
        return DatabasePropertyTestSetup.setLockTimeouts(suite, -1, 2);
    }

    protected void setUp() throws SQLException {
        Statement s = createStatement();
        s.execute("create table EXCEPTION_TABLE1 (id integer " +
                  "primary key, data varchar (5))");
        s.execute("insert into EXCEPTION_TABLE1 (id, data)" +
                  "values (1, 'data1')");
        s.close();
    }

    protected void tearDown() throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-2707
        Statement s = createStatement();
        s.execute("drop table EXCEPTION_TABLE1");
        s.close();
        commit();
        super.tearDown();
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
        } catch (SQLNonTransientConnectionException cone) {
            assertTrue("Unexpected SQL State: " + cone.getSQLState(),
                       cone.getSQLState().startsWith("08"));
        }
        
        if (usingEmbedded())
        {
        	// test exception after database shutdown
        	// DERBY-3074
        	stmt = createStatement();
        	TestConfiguration.getCurrent().shutdownDatabase();
        	try {
        		stmt.execute("select * from exception1");
        		fail("Statement didn't fail.");
        	} catch (SQLNonTransientConnectionException cone) {
        		assertTrue("Unexpected SQL State: " + cone.getSQLState(),
        				cone.getSQLState().startsWith("08"));        	  
        	}
        }
        // test connection to server which is not up.
        // DERBY-3075
        if (usingDerbyNetClient()) {
        	DataSource ds = JDBCDataSource.getDataSource();
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        	JDBCDataSource.setBeanProperty(ds, "portNumber", 0);
        	try {
        		ds.getConnection();
        	} catch (SQLNonTransientConnectionException cone) {
        		assertTrue("Unexpected SQL State: " + cone.getSQLState(),
        				cone.getSQLState().startsWith("08"));   
        	}
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
