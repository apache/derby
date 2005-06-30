/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SetQueryTimeoutTest

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Collections;

import org.apache.derby.tools.ij;
import org.apache.derby.iapi.reference.SQLState;

/**
 * Functional test for the Statement.setQueryTimeout() method.
 *
 * This test consists of three parts:
 *
 * 1. Executes a SELECT query in 4 different threads concurrently.
 *    The query is designed to make the execution time for each
 *    ResultSet.next() operation unpredictable, and in the order
 *    of seconds for many of them. The executeQuery() call finishes
 *    quickly, but the fetch operations may take longer time than
 *    the timeout value set. Hence, this part tests getting timeouts
 *    from calls to ResultSet.next().
 * 
 *    Two connections are used, two threads execute their statement
 *    in the context of one connection, the other two threads in the
 *    context of the other connection. Of the 4 threads, only one
 *    executes its statement with a timeout value. This way, the test
 *    ensures that the correct statement is affected by setQueryTimeout(),
 *    regardless of what connection/transaction it and other statements
 *    are executed in the context of.
 *    
 * 2. Executes a long-running INSERT query in two threads.
 *    This part tests getting timeouts from calls to Statement.execute().
 *    Each thread executes the query in the context of a separate
 *    connection. There is no point in executing multiple statements
 *    on the same connection; since only one statement per connection
 *    executes at a time, there will be no interleaving of execution
 *    between them (contrary to the first part of this test, where
 *    calls to ResultSet.next() may be interleaved between the different
 *    threads).
 *
 *    Only one thread executes its statement with a timeout value set,
 *    this is to verify that the correct statement is affected by the
 *    timeout, while the other statement executes to completion.
 *
 * 3. Sets an invalid (negative) timeout. Verifies that the correct
 *    exception is thrown.
 *
 * @author oyvind.bakksjo@sun.com
 */
public class SetQueryTimeoutTest
{
    private static final int TIMEOUT = 3; // In seconds

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
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(queryString);
            statement.execute();
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
        Collection ignore = new HashSet();
        ignore.add(SQLState.LANG_OBJECT_DOES_NOT_EXIST.substring(0,5));
        
        exec(conn, "drop table " + tablePrefix + "0", ignore);
        exec(conn, "drop table " + tablePrefix + "1", ignore);
        exec(conn, "drop table " + tablePrefix + "2", ignore);
        exec(conn, "drop table " + tablePrefix + "3", ignore);
        exec(conn, "drop table " + tablePrefix + "4", ignore);
        exec(conn, "drop table " + tablePrefix + "5", ignore);
    }
    
    private static void prepareTables(Connection conn, String tablePrefix)
        throws
            TestFailedException
    {
        System.out.println("Initializing tables with prefix " + tablePrefix);

        dropTables(conn, tablePrefix);
        
        exec(conn,
             "create table " + tablePrefix + "1 (a int, b char(1))");

        exec(conn,
             "create table " + tablePrefix + "2 (a int, b char(2))");
        
        exec(conn,
             "create table " + tablePrefix + "3 (a int, b char(4))");

        exec(conn,
             "create table " + tablePrefix + "4 (a int, b char(6))");

        exec(conn,
             "create table " + tablePrefix + "5 (a int, b char(8))");

        exec(conn,
             "insert into "
             + tablePrefix + "1"
             + " values(3,'a')"
             + ",(7,'b')"
             + ",(13,'c')"
             + ",(37,'d')"
             + ",(141,'e')"
             + ",(1,'f')");
        

        exec(conn,
             "insert into "
             + tablePrefix + "2 select "
             + tablePrefix + "1.a+"
             + tablePrefix + "x.a,"
             + tablePrefix + "1.b||"
             + tablePrefix + "x.b from "
             + tablePrefix + "1 join "
             + tablePrefix + "1 as "
             + tablePrefix + "x on 1=1");
        
        exec(conn,
             "insert into "
             + tablePrefix + "3 select "
             + tablePrefix + "2.a+"
             + tablePrefix + "x.a,"
             + tablePrefix + "2.b||"
             + tablePrefix + "x.b from "
             + tablePrefix + "2 join "
             + tablePrefix + "2 as "
             + tablePrefix + "x on 1=1");
        
        exec(conn,
             "insert into "
             + tablePrefix + "4 select "
             + tablePrefix + "3.a+"
             + tablePrefix + "2.a,"
             + tablePrefix + "3.b||"
             + tablePrefix + "2.b from "
             + tablePrefix + "3 join "
             + tablePrefix + "2 on 1=1");
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

        prepareTables(conn, "t");
        prepareTables(conn, "u");
        prepareTables(conn, "v");
        prepareTables(conn, "x");
    }
    
    private static String getFetchQuery(String tablePrefix)
    {
        return "select "
            + tablePrefix + "4.a+"
            + tablePrefix + "3.a,"
            + tablePrefix + "4.b||"
            + tablePrefix + "3.b from "
            + tablePrefix + "4 left join "
            + tablePrefix + "3 on 1=1 where mod("
            + tablePrefix + "4.a+"
            + tablePrefix + "3.a,1000)=0";
    }
    
    private static String getExecQuery(String tablePrefix)
    {
        return "insert into "
            + tablePrefix + "5 select "
            + tablePrefix + "3.a+"
            + tablePrefix + "x.a,"
            + tablePrefix + "3.b from "
            + tablePrefix + "3 left join "
            + tablePrefix + "3 as "
            + tablePrefix + "x on 1=1";
    }
    
    private static class StatementExecutor
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
            try {
                statement.setQueryTimeout(timeout);
            } catch (SQLException e) {
                sqlException = e;
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
            if (!expectSqlState.startsWith(sqlState)) {
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
        PreparedStatement statementB = prepare(conn1, getFetchQuery("u"));
        PreparedStatement statementC = prepare(conn2, getFetchQuery("v"));
        PreparedStatement statementD = prepare(conn2, getFetchQuery("x"));

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
        expectException(SQLState.LANG_STATEMENT_CANCELLED_OR_TIMED_OUT,
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
    private static void testTimeoutWithExec(Connection conn1,
                                            Connection conn2)
        throws
            TestFailedException
    {
        System.out.println("Testing timeout with an execute operation");

        try {
            conn1.setAutoCommit(true);
            conn2.setAutoCommit(true);
        } catch (SQLException e) {
            throw new TestFailedException("Should not happen", e);
        }

        PreparedStatement statementA = prepare(conn1, getExecQuery("t"));
        PreparedStatement statementB = prepare(conn2, getExecQuery("u"));
        
        StatementExecutor exec0 = new StatementExecutor(statementA, false, TIMEOUT);
        StatementExecutor exec1 = new StatementExecutor(statementB, false, 0);
        
        exec1.start();
        exec0.start();

        try {
            exec0.join();
            exec1.join();
        } catch (InterruptedException e) {
            throw new TestFailedException("Should never happen", e);
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
        expectException(SQLState.LANG_STATEMENT_CANCELLED_OR_TIMED_OUT,
                        exec0.getSQLException(),
                        "exec did not time out. Execution time: "
                        + exec0.getHighestRunTime() + " ms");

        System.out.println("Statement 0 timed out");

        SQLException sqlException = exec1.getSQLException();
        if (sqlException != null) {
            throw new TestFailedException(sqlException);
        }

        System.out.println("Statement 1 completed");
        try {
            statementA.close();
            statementB.close();
        } catch (SQLException e) {
            throw new TestFailedException(e);
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
            expectException(SQLState.INVALID_QUERYTIMEOUT_VALUE, e,
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
     * runs part 1 and 2, and shuts down.
     */
    public void go(String[] args)
    {
        System.out.println("Test SetQueryTimeoutTest starting");
        
        Connection conn1 = null;
        Connection conn2 = null;

        try {
            // Load the JDBC Driver class
            // use the ij utility to read the property file and
            // create connections
            ij.getPropertyArg(args);
            conn1 = ij.startJBMS();
            conn2 = ij.startJBMS();

            System.out.println("Got connections");
            
            conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            conn2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

            prepareForTimedQueries(conn1);
            testTimeoutWithFetch(conn1, conn2);
            testTimeoutWithExec(conn1, conn2);
            testInvalidTimeoutValue(conn1);
  
            System.out.println("Test SetQueryTimeoutTest PASSED");
        } catch (Throwable e) {
            System.out.println("Test SetQueryTimeoutTest FAILED");
            e.printStackTrace();
        } finally {
            if (conn2 != null) {
                try {
                    conn2.close();
                } catch (SQLException ex) {
                    printSQLException(ex);
                }
            }
            if (conn1 != null) {
                try {
                    conn1.close();
                } catch (SQLException ex) {
                    printSQLException(ex);
                }
            }
            System.out.println("Closed connections");
        }
    }
}
