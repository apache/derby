/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.SetQueryTimeoutTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Collections;

import org.apache.derby.tools.ij;

/**
 * Functional test for the Statement.setQueryTimeout() method.
 *
 * This test consists of four parts:
 *
 * 1. Executes a SELECT query in 4 different threads concurrently.
 *    The query calls a user-defined, server-side function which
 *    delays the execution, so that it takes several seconds even
 *    though the data volume is really low. The fetch operations
 *    take longer time than the timeout value set. Hence, this part
 *    tests getting timeouts from calls to ResultSet.next().
 * 
 *    Two connections are used, two threads execute their statement
 *    in the context of one connection, the other two threads in the
 *    context of the other connection. Of the 4 threads, only one
 *    executes its statement with a timeout value. This way, the test
 *    ensures that the correct statement is affected by setQueryTimeout(),
 *    regardless of what connection/transaction it and other statements
 *    are executed in the context of.
 *    
 * 2. Executes an INSERT query in multiple threads.
 *    This part tests getting timeouts from calls to Statement.execute().
 *    Each thread executes the query in the context of a separate
 *    connection. There is no point in executing multiple statements
 *    on the same connection; since only one statement per connection
 *    executes at a time, there will be no interleaving of execution
 *    between them (contrary to the first part of this test, where
 *    calls to ResultSet.next() may be interleaved between the different
 *    threads).
 *
 *    Half of the threads execute their statement with a timeout value set,
 *    this is to verify that the correct statements are affected by the
 *    timeout, while the other statements execute to completion.
 *
 * 3. Sets an invalid (negative) timeout. Verifies that the correct
 *    exception is thrown.
 *
 * 4. Tests that the query timeout value is not forgotten after the execution
 *    of a statement (DERBY-1692).
 */
public class SetQueryTimeoutTest
{
    private static final int TIMEOUT = 1; // In seconds
    private static final int CONNECTIONS = 100;

    private static void printSQLException(SQLException e)
    {
        while (e != null)
        {
            e.printStackTrace();
            e = e.getNextException();
        }
    }

    /**
     * This Exception class is used for getting fail-fast behaviour in
     * this test. There is no point in wasting cycles running a test to
     * the end when we know that it has failed.
     *
     * In order to enable chaining of exceptions in J2ME, this class defines
     * its own "cause", duplicating existing functionality in J2SE.
     */
    private static class TestFailedException
        extends
            Exception
    {
        private Throwable cause;

        public TestFailedException(Throwable t)
        {
            super();
            cause = t;
        }
        
        public TestFailedException(String message)
        {
            super(message);
            cause = null;
        }
        
        public TestFailedException(String message, Throwable t)
        {
            super(message);
            cause = t;
        }
        
        public String toString()
        {
            if (cause != null) {
                return super.toString() + ": " + cause.toString();
            } else {
                return super.toString();
            }
        }
        
        public void printStackTrace()
        {
            super.printStackTrace();
            if (cause != null) {
                if (cause instanceof SQLException) {
                    SetQueryTimeoutTest.printSQLException((SQLException)cause);
                } else {
                    cause.printStackTrace();
                }
            }
        }
    }

    /**
     * Used for executing the SQL statements for setting up this test
     * (the preparation phase). The queries testing setQueryTimeout()
     * are run by the StatementExecutor class.
     */
    private static void exec(Connection connection,
                             String queryString,
                             Collection ignoreExceptions)
        throws
            TestFailedException
    {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(queryString);
        } catch (SQLException e) {
            String sqlState = e.getSQLState();
            if (!ignoreExceptions.contains(sqlState)) {
                throw new TestFailedException(e); // See finally block below
            }
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ee) {
                    // This will discard an exception possibly thrown above :-(
                    // But we don't worry too much about this, since:
                    // 1. This is just a test
                    // 2. We don't expect close() to throw
                    // 3. If it does, this will be inspected by a developer
                    throw new TestFailedException(ee);
                }
            }
        }
    }

    // Convenience method
    private static void exec(Connection connection,
                             String queryString)
        throws
            TestFailedException
    {
        exec(connection, queryString, Collections.EMPTY_SET);
    }
    
    private static void dropTables(Connection conn, String tablePrefix)
        throws
            TestFailedException
    {
        HashSet<String> ignore = new HashSet<String>();
        ignore.add("42Y55");
        
        exec(conn, "drop table " + tablePrefix + "_orig", ignore);
        exec(conn, "drop table " + tablePrefix + "_copy", ignore);
    }
    
    private static void prepareTables(Connection conn, String tablePrefix)
        throws
            TestFailedException
    {
        System.out.println("Initializing tables with prefix " + tablePrefix);

        dropTables(conn, tablePrefix);
        
        exec(conn,
             "create table " + tablePrefix + "_orig (a int)");

        exec(conn,
             "create table " + tablePrefix + "_copy (a int)");

        exec(conn,
             "insert into "
             + tablePrefix + "_orig"
             + " values(0),(1),(2),(3),(4),(5),(6)");
    }

    /**
     * This is the user-defined function which is called from our queries
     */
    public static int delay(int seconds, int value)
        throws
            SQLException
    {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            // Ignore
        }
        return value;
    }

    private static void prepareForTimedQueries(Connection conn)
        throws
            TestFailedException
    {
        System.out.println("Preparing for testing queries with timeout");

        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            throw new TestFailedException("Should not happen", e);
        }
        
        try {
            exec(conn, "DROP FUNCTION DELAY");
        } catch (Exception e) {
            // Ignore
        }

        exec(conn, "CREATE FUNCTION DELAY(SECONDS INTEGER, VALUE INTEGER) RETURNS INTEGER PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.jdbcapi.SetQueryTimeoutTest.delay'");

        prepareTables(conn, "t");
    }
    
    private static String getFetchQuery(String tablePrefix)
    {
        /**
         * The reason for using the mod function here is to force
         * at least one invocation of ResultSet.next() to read
         * more than one row from the table before returning.
         * This is necessary since timeout is checked only when
         * reading rows from base tables, and when the first row
         * is read, the query still has not exceeded the timeout.
         */
        return "select a from " + tablePrefix + "_orig where mod(DELAY(1,a),3)=0";
    }
    
    private static String getExecQuery(String tablePrefix)
    {
        return "insert into "
            + tablePrefix + "_copy select a from "
            + tablePrefix + "_orig where DELAY(1,1)=1";
    }
    
    public static class StatementExecutor
        extends
            Thread
    {
        private PreparedStatement statement;
        private boolean doFetch;
        private int timeout;
        private SQLException sqlException;
        private String name;
        private long highestRunTime;
        
        public StatementExecutor(PreparedStatement statement,
                                 boolean doFetch,
                                 int timeout)
        {
            this.statement = statement;
            this.doFetch = doFetch;
            this.timeout = timeout;
            highestRunTime = 0;
            sqlException = null;
            if (timeout > 0) {
                try {
                    statement.setQueryTimeout(timeout);
                } catch (SQLException e) {
                    sqlException = e;
                }
            }
        }
        
        private void setHighestRunTime(long runTime)
        {
            synchronized (this) {
                highestRunTime = runTime;
            }
        }
        
        public long getHighestRunTime()
        {
            synchronized (this) {
                return highestRunTime;
            }
        }
        
        private boolean fetchRow(ResultSet resultSet)
            throws
                SQLException
        {
            long startTime = System.currentTimeMillis();
            boolean hasNext = resultSet.next();
            long endTime = System.currentTimeMillis();
            long runTime = endTime - startTime;
            if (runTime > highestRunTime) setHighestRunTime(runTime);
            return hasNext;
        }

        public void run()
        {
            if (sqlException != null)
                return;

            ResultSet resultSet = null;

            try {
                if (doFetch) {
                    long startTime = System.currentTimeMillis();
                    resultSet = statement.executeQuery();
                    long endTime = System.currentTimeMillis();
                    setHighestRunTime(endTime - startTime);
                    while (fetchRow(resultSet)) {
                        yield();
                    }
                } else {
                    long startTime = System.currentTimeMillis();
                    statement.execute();
                    long endTime = System.currentTimeMillis();
                    setHighestRunTime(endTime - startTime);
                }
            } catch (SQLException e) {
                synchronized (this) {
                    sqlException = e;
                }
            } finally {
                if (resultSet != null) {
                    try {
                        resultSet.close();
                    } catch (SQLException ex) {
                        if (sqlException != null) {
                            System.err.println("Discarding previous exception");
                            sqlException.printStackTrace();
                        }
                        sqlException = ex;
                    }
                }
            }
        }

        public SQLException getSQLException()
        {
            synchronized (this) {
                return sqlException;
            }
        }
    }

    /**
     * This method compares a thrown SQLException's SQLState value
     * to an expected SQLState. If they do not match, a
     * TestFailedException is thrown with the given message string.
     */
    private static void expectException(String expectSqlState,
                                        SQLException sqlException,
                                        String failMsg)
        throws
            TestFailedException
    {
        if (sqlException == null) {
            throw new TestFailedException(failMsg);
        } else {
            String sqlState = sqlException.getSQLState();
            if (!expectSqlState.equals(sqlState)) {
                throw new TestFailedException(sqlException);
            }
        }
    }

    // A convenience method which wraps a SQLException
    private static PreparedStatement prepare(Connection conn, String query)
        throws
            TestFailedException
    {
        try {
            return conn.prepareStatement(query);
        } catch (SQLException e) {
            throw new TestFailedException(e);
        }
    }

    /**
     * Part 1 of this test.
     */
    private static void testTimeoutWithFetch(Connection conn1,
                                             Connection conn2)
        throws
            TestFailedException
    {
        System.out.println("Testing timeout with fetch operations");

        try {
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);
        } catch (SQLException e) {
            throw new TestFailedException("Should not happen", e);
        }
        
        // The idea with these 4 statements is as follows:
        // A - should time out
        // B - different stmt on the same connection; should NOT time out
        // C - different stmt on different connection; should NOT time out
        // D - here just to create equal contention on conn1 and conn2

        PreparedStatement statementA = prepare(conn1, getFetchQuery("t"));
        PreparedStatement statementB = prepare(conn1, getFetchQuery("t"));
        PreparedStatement statementC = prepare(conn2, getFetchQuery("t"));
        PreparedStatement statementD = prepare(conn2, getFetchQuery("t"));

        StatementExecutor[] statementExecutor = new StatementExecutor[4];
        statementExecutor[0] = new StatementExecutor(statementA, true, TIMEOUT);
        statementExecutor[1] = new StatementExecutor(statementB, true, 0);
        statementExecutor[2] = new StatementExecutor(statementC, true, 0);
        statementExecutor[3] = new StatementExecutor(statementD, true, 0);
        
        for (int i = 3; i >= 0; --i) {
            statementExecutor[i].start();
        }
        
        for (int i = 0; i < 4; ++i) {
            try {
                statementExecutor[i].join();
            } catch (InterruptedException e) {
                throw new TestFailedException("Should never happen", e);
            }
        }

        /**
         * Actually, there is no guarantee that setting a query timeout
         * for a statement will actually cause a timeout, even if execution
         * of the statement takes longer than the specified timeout.
         *
         * However, these queries execute significantly longer than the
         * specified query timeout. Also, the cancellation mechanism
         * implemented should be quite responsive. In sum, we expect
         * the statement to always time out.
         *
         * If it does not time out, however, we print the highest
         * execution time for the query, as an assistance in determining
         * why it failed. Compare the number to the TIMEOUT constant
         * in this class (note that the TIMEOUT constant is in seconds,
         * while the execution time is in milliseconds). 
         */
        expectException("XCL52",
                        statementExecutor[0].getSQLException(),
                        "fetch did not time out. Highest execution time: "
                        + statementExecutor[0].getHighestRunTime() + " ms");

        System.out.println("Statement 0 timed out");

        for (int i = 1; i < 4; ++i) {
            SQLException sqlException = statementExecutor[i].getSQLException();
            if (sqlException != null) {
                throw new TestFailedException("Unexpected exception in " + i,
                                              sqlException);
            }
            System.out.println("Statement " + i + " completed");
        }

        try {
            statementA.close();
            statementB.close();
            statementC.close();
            statementD.close();
            conn1.commit();
            conn2.commit();
        } catch (SQLException e) {
            throw new TestFailedException(e);
        }
    }

    /**
     * Part two of this test.
     */
    private static void testTimeoutWithExec(Connection[] connections)
        throws
            TestFailedException
    {
        System.out.println("Testing timeout with an execute operation");

        for (int i = 0; i < connections.length; ++i) {
            try {
                connections[i].setAutoCommit(true);
            } catch (SQLException e) {
                throw new TestFailedException("Should not happen", e);
            }
        }

        PreparedStatement statements[] = new PreparedStatement[connections.length];
        for (int i = 0; i < statements.length; ++i) {
            statements[i] = prepare(connections[i], getExecQuery("t"));
        }

        StatementExecutor[] executors = new StatementExecutor[statements.length];
        for (int i = 0; i < executors.length; ++i) {
            int timeout =
                (i % 2 == 0)
                ? TIMEOUT
                : 0;
            executors[i] = new StatementExecutor(statements[i], false, timeout);
        }

        for (int i = 0; i < executors.length; ++i) {
            executors[i].start();
        }

        for (int i = 0; i < executors.length; ++i) {
            try {
                executors[i].join();
            } catch (InterruptedException e) {
                throw new TestFailedException("Should never happen", e);
            }
        }
        
        /**
         * Actually, there is no guarantee that setting a query timeout
         * for a statement will actually cause a timeout, even if execution
         * of the statement takes longer than the specified timeout.
         *
         * However, these queries execute significantly longer than the
         * specified query timeout. Also, the cancellation mechanism
         * implemented should be quite responsive. In sum, we expect
         * the statement to always time out.
         *
         * If it does not time out, however, we print the highest
         * execution time for the query, as an assistance in determining
         * why it failed. Compare the number to the TIMEOUT constant
         * in this class (note that the TIMEOUT constant is in seconds,
         * while the execution time is in milliseconds). 
         */
        for (int i = 0; i < executors.length; ++i) {
            int timeout =
                (i % 2 == 0)
                ? TIMEOUT
                : 0;
            if (timeout > 0) {
                expectException("XCL52",
                                executors[i].getSQLException(),
                                "exec did not time out. Execution time: "
                                + executors[i].getHighestRunTime() + " ms");
            } else {
                SQLException sqlException = executors[i].getSQLException();
                if (sqlException != null) {
                    throw new TestFailedException(sqlException);
                }
            }
        }

        System.out.println("Statements that should time out timed out, and statements that should complete completed");

        for (int i = 0; i < statements.length; ++i) {
            try {
                statements[i].close();
            } catch (SQLException e) {
                throw new TestFailedException(e);
            }
        }
    }
    
    private static void testInvalidTimeoutValue(Connection conn)
        throws
            TestFailedException
    {
        System.out.println("Testing setting a negative timeout value");

        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            throw new TestFailedException("Should not happen", e);
        }

        // Create statement
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("select * from sys.systables");
        } catch (SQLException e) {
            throw new TestFailedException("Should not happen", e);
        }
        
        // Set (invalid) timeout value - expect exception
        try {
            stmt.setQueryTimeout(-1);
        } catch (SQLException e) {
            expectException("XJ074", e,
                        "negative timeout value should give exception");
        }
        
        System.out.println("Negative timeout value caused exception, as expected");
        
        // Execute the statement and fetch result
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();
            System.out.println("Execute returned a ResultSet");
            rs.close();
        } catch (SQLException e) {
            throw new TestFailedException("Should not happen", e);
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                // This will discard an exception possibly thrown above :-(
                // But we don't worry too much about this, since:
                // 1. This is just a test
                // 2. We don't expect close() to throw
                // 3. If it does, this will be inspected by a developer
                throw new TestFailedException("close should not throw", e);
            }
        }
    }

    /** This tests timeout with executeUpdate call. */
    private static void testTimeoutWithExecuteUpdate(Connection conn)
        throws TestFailedException
    {
    	System.out.println("Testing timeout with executeUpdate call.");
        try{
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(TIMEOUT);
            stmt.executeUpdate(getExecQuery("t"));    
        } catch (SQLException sqle) {
        	expectException("XCL52", sqle, "Should have timed out.");
        }
    }
    
    /** Test for DERBY-1692. */
    private static void testRememberTimeoutValue(Connection conn)
        throws TestFailedException
    {
        String sql = getFetchQuery("t");
        try {
            Statement stmt = conn.createStatement();
            testStatementRemembersTimeout(stmt);
            PreparedStatement ps = conn.prepareStatement(sql);
            testStatementRemembersTimeout(ps);
            CallableStatement cs = conn.prepareCall(sql);
            testStatementRemembersTimeout(cs);
        } catch (SQLException sqle) {
            throw new TestFailedException("Should not happen", sqle);
        }
    }

    /** Test that a statement remembers its timeout value when executed
     * multiple times. */
    private static void testStatementRemembersTimeout(Statement stmt)
        throws SQLException, TestFailedException
    {
        System.out.println("Testing that Statement remembers timeout.");
        stmt.setQueryTimeout(1);
        long runTime=0;
        for (int i = 0; i < 3; i++) {
            try {
                ResultSet rs = stmt.executeQuery(getFetchQuery("t"));
                long startTime = System.currentTimeMillis();
                while (rs.next());
                long endTime = System.currentTimeMillis();
                runTime = endTime - startTime;
                throw new TestFailedException("Should have timed out, for " +
                    "statement, iteration: " +i+ ", took (millis): "+runTime);
            } catch (SQLException sqle) {
                expectException("XCL52", sqle, "Should have timed out, got " +
                    "unexpected exception, for statement, iteration: " + i + 
                    ", time taken (millis): " + runTime);
            }
        }
        stmt.close();
    }

    /** Test that a prepared statement remembers its timeout value when
     * executed multiple times. */
    private static void testStatementRemembersTimeout(PreparedStatement ps)
        throws SQLException, TestFailedException
    {
        String name = (ps instanceof CallableStatement) ?
            "CallableStatement" : "PreparedStatement";
        System.out.println("Testing that " + name + " remembers timeout.");
        ps.setQueryTimeout(1);
        for (int i = 0; i < 3; i++) {
            long runTime=0;
            try {
                ResultSet rs = ps.executeQuery();
                long startTime = System.currentTimeMillis();
                while (rs.next()); 
                long endTime = System.currentTimeMillis();
                runTime = endTime - startTime;
                throw new TestFailedException(
                    "Should have timed out, for " + name + ", on iteration " 
                    + i + ", runtime(millis): " + runTime);
           } catch (SQLException sqle) {
                expectException("XCL52", sqle, "Should have timed out, " +
                    "got unexpected exception, for " + name + ", on iteration "
                    + i + ", runtime(millis): " + runTime);
            }
        }
        ps.close();
    }

    /**
     * Main program, makes this class invocable from the command line
     */
    public static void main(String[] args)
    {
        new SetQueryTimeoutTest().go(args);
    }

    /**
     * The actual main bulk of this test.
     * Sets up the environment, prepares tables,
     * runs the tests, and shuts down.
     */
    public void go(String[] args)
    {
        System.out.println("Test SetQueryTimeoutTest starting");

        Connection[] connections = new Connection[CONNECTIONS];

        try {
            // Load the JDBC Driver class
            // use the ij utility to read the property file and
            // create connections
            ij.getPropertyArg(args);
            for (int i = 0; i < connections.length; ++i) {
                connections[i] = ij.startJBMS();
            }

            System.out.println("Got connections");

            for (int i = 0; i < connections.length; ++i) {
                connections[i].setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            }

            prepareForTimedQueries(connections[0]);
            testTimeoutWithFetch(connections[0], connections[1]);
            testTimeoutWithExec(connections);
            testInvalidTimeoutValue(connections[0]);
            testRememberTimeoutValue(connections[0]);
            testTimeoutWithExecuteUpdate(connections[0]);
  
            System.out.println("Test SetQueryTimeoutTest PASSED");
        } catch (Throwable e) {
            System.out.println("Test SetQueryTimeoutTest FAILED");
            e.printStackTrace();
        } finally {
            for (int i = connections.length - 1; i >= 0; --i) {
                if (connections[i] != null) {
                    try {
                        connections[i].close();
                    } catch (SQLException ex) {
                        printSQLException(ex);
                    }
                }
            }
            System.out.println("Closed connections");
        }
    }
}
