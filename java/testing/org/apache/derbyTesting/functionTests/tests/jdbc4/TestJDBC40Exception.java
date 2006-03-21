/*
 
   Derby - Class 
        org.apache.derbyTesting.functionTests.tests.jdbc4.TestJDBC40Exception
 
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

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransactionRollbackException;
import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.tests.derbynet.testconnection;

public class TestJDBC40Exception {    
    
    private static final String EXCEPTION_TABLE1 = "EXCEPTION_TABLE1";

	private	static	String[]	_startupArgs;
	
    public TestJDBC40Exception() {
    }
    
    /*
     * Stub methods to be removed after 623 is fixed and test is 
     * moved to use junit
     */
    private Connection getConnection () throws Exception {
		// use the ij utility to read the property file and
		// make the initial connection.
		ij.getPropertyArg( _startupArgs );
		
		Connection	conn_main = ij.startJBMS();
		
        return conn_main;
    }
    
    /*
     * Stub methods to be removed after 623 is fixed and test is 
     * moved to use junit
     */
    private void close (Connection conn) throws SQLException {
        conn.close ();
    }

    /*
     * Stub methods to be removed after 623 is fixed and test is 
     * moved to use junit
     */    
    private void execute (Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement ();
        stmt.execute (sql);
        stmt.close ();
    }
    
    public static Test suite() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTestSuite(TestJDBC40Exception.class);
        return testSuite;
    }
    
    
    public void testException() throws Exception{
        Connection conn = getConnection();
        execute(conn,  "create table " + EXCEPTION_TABLE1 + "(id integer " +
                "primary key, data varchar (5))");
        execute(conn, "insert into " + EXCEPTION_TABLE1 + "(id, data)" +
                "values (1, 'data1')");
        close(conn);
        checkDataException();
        checkIntegrityConstraintViolationException();
        checkSyntaxErrorException();
        checkConnectionException();
        checkTimeout();
    }
    
    private void checkIntegrityConstraintViolationException() throws Exception {
        Connection conn = getConnection();
        try {
            execute(conn, "insert into " + EXCEPTION_TABLE1 + "(id, data)" +
                    "values (1, 'data1')");
        } catch (SQLIntegrityConstraintViolationException e) {
              if (!e.getSQLState().startsWith ("23"))
                System.out.println ("Unexpected SQL State" + e.getSQLState());
        }
    }
    
    private void checkDataException() throws Exception{
        Connection conn = getConnection();
        try {
            execute(conn, "insert into " + EXCEPTION_TABLE1 + "(id, data)" +
                    "values (2, 'data1234556')");
        } catch (SQLDataException e) {
             if (!e.getSQLState().startsWith ("22"))
                System.out.println ("Unexpected SQL State" + e.getSQLState());
        }
    }
    
    private void checkConnectionException() throws Exception {
        Statement stmt = null;
        Connection con = null;
        try {
            con = getConnection();
            stmt = con.createStatement();
            con.close();
            stmt.execute("select * from exception1");
        } catch (SQLTransientConnectionException cone) {
            if (!cone.getSQLState().startsWith ("08"))
                System.out.println ("Unexpected SQL State" + cone.getSQLState());
        }
    }
    
    private void checkSyntaxErrorException() throws Exception{
        Connection conn = getConnection();
        try {
            execute(conn, "insert into " + EXCEPTION_TABLE1 + "(id, data)" +
                    "values ('2', 'data1')");
        } catch (SQLSyntaxErrorException e) {
            if (!e.getSQLState().startsWith ("42"))
                System.out.println ("Unexpected SQL State" + e.getSQLState());
        }
    }
    
    private void checkTimeout() throws Exception {
        Connection con1 = getConnection();
        Connection con2 = getConnection();
        try {
            con1.setAutoCommit(false);
            con2.setAutoCommit(false);
            con1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            con2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            execute(con1, "select * from " + EXCEPTION_TABLE1 + " for update");
            execute(con2, "select * from " + EXCEPTION_TABLE1 + " for update");
        } catch (SQLTransactionRollbackException e) {
              if (!e.getSQLState().startsWith ("40"))
                System.out.println ("Unexpected SQL State" + e.getSQLState());
        }
    }
    
    
    public static void main(String [] args) throws Exception {    
        TestJDBC40Exception test = new TestJDBC40Exception ();

		_startupArgs = args;
        test.testException ();
    }
}
