/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.StatementTest
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

import org.apache.derby.vti.VTITemplate;
import org.apache.derby.impl.jdbc.EmbedResultSet;
import org.apache.derby.impl.sql.execute.RowUtil;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.BatchUpdateTest;
import org.apache.derbyTesting.functionTests.tests.jdbcapi.SetQueryTimeoutTest;
import org.apache.derbyTesting.functionTests.tests.jdbcapi.Wrapper41Statement;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import org.apache.derby.client.am.ClientStatement;

/**
 * Tests for new methods added for Statement in JDBC4.
 */
public class StatementTest
    extends BaseJDBCTestCase {

   /** Default statement used by the tests. */
    private Statement stmt = null;
    
    /**
     * Create a new test with the given name.
     *
     * @param name name of the test.
     */
    public StatementTest(String name) {
        super(name);
    }

    /**
     * Create default connection and statement.
     *
     * @throws SQLException if setAutoCommit, createStatement or 
     *                      BaseJDBCTestCase.getConnection fails.
     */
    protected void setUp() 
        throws SQLException {
        getConnection().setAutoCommit(false);
        // Create a default statement.
        stmt = createStatement();
        assertFalse("First statement must be open initially", 
                stmt.isClosed());
    }

    /**
     * Close default connection and statement if necessary.
     *
     * @throws SQLException if a database access exception occurs.
     */
    protected void tearDown() 
        throws Exception {
        // Close default statement
        if (stmt != null) {
            stmt.close();
            stmt = null;
        }

        super.tearDown();
    }

    /**
     * Check that <code>isClosed</code> returns <code>true</code> after
     * the statement has been explicitly closed.
     */
    public void testIsClosedBasic()
        throws SQLException {
        ResultSet rs = stmt.executeQuery("select count(*) from stmtTable");
        assertFalse("Statement should still be open", stmt.isClosed());
        rs.close();
        assertFalse("Statement should be open after ResultSet has been " +
                "closed", stmt.isClosed());
        stmt.close();
        assertTrue("Statement should be closed, close() has been called", 
                stmt.isClosed());
    }
    
    /**
     * Test that creating two statements on the same connection does not
     * cause side effects on the statements.
     */
    public void testIsClosedWithTwoStatementsOnSameConnection()
        throws SQLException {
        // Create a second statement on the default connection.
        Statement stmt2 = createStatement();
        assertFalse("Second statement must be open initially", 
                stmt2.isClosed());
        assertFalse("First statement should not be closed when " +
                "creating a second statement", stmt.isClosed());
        ResultSet rs = stmt2.executeQuery("select count(*) from stmtTable");
        assertFalse("Second statement should be open after call to " +
                "execute()", stmt2.isClosed());
        assertFalse("First statement should be open after call to " +
                "second statment's execute()", stmt.isClosed());
        stmt2.close();
        assertTrue("Second statement should be closed, close() has " +
                "been called!", stmt2.isClosed());
        assertFalse("First statement should be open after call to " +
                "second statment's close()", stmt.isClosed());
    }

    /**
     * Test that the two statements created on the connection are closed
     * when the connection itself is closed.
     */
    public void testIsClosedWhenClosingConnection()
        throws SQLException {
        // Create an extra statement for good measure.
        Statement stmt2 = createStatement();
        assertFalse("Second statement must be open initially",
                stmt2.isClosed());
        // Exeute something on it, as opposed to the default statement.
        stmt2.execute("select count(*) from stmtTable");
        assertFalse("Second statement should be open after call to " +
                "execute()", stmt2.isClosed());
        // Close the connection. We must commit/rollback first, or else a
        // "Invalid transaction state" exception is raised.
        rollback();
        Connection con = getConnection();
        con.close();
        assertTrue("Connection should be closed after close()", 
                con.isClosed());
        assertTrue("First statement should be closed, as parent " +
                "connection has been closed", stmt.isClosed());
        assertTrue("Second statement should be closed, as parent " +
                "connection has been closed", stmt2.isClosed());
    }
    
    /**
     * Check the state of the statement when the connection is first attempted
     * closed when in an invalid transaction state, then closed after a
     * commit. According to the JDBC 4 API documentation: </i>"It is strongly 
     * recommended that an application explictly commits or rolls back an 
     * active transaction prior to calling the close method. If the close 
     * method is called and there is an active transaction, 
     * the results are implementation-defined."</i>
     * Derby throws an exception and keeps the connection open.
     */
    public void testIsClosedWhenClosingConnectionInInvalidState()
        throws SQLException {
        stmt.executeQuery("select count(*) from stmtTable");
        // Connection should now be in an invalid transaction state.
        Connection con = stmt.getConnection();
        try {
            con.close();
            fail("Invalid transaction state exception was not thrown");
        } catch (SQLException sqle) {
            String expectedState =
                SQLStateConstants.INVALID_TRANSACTION_STATE_ACTIVE_SQL_TRANSACTION;
            assertSQLState(expectedState, sqle);
        }
        assertFalse("Statement should still be open, because " +
                "Connection.close() failed", stmt.isClosed());
        assertFalse("Connection should still be open", con.isClosed());
        // Do a commit here, since we do a rollback in another test.
        con.commit();
        con.close();
        assertTrue("Connection should be closed after close()", 
                con.isClosed());
        assertTrue("Statement should be closed, because " +
                "the connection has been closed", stmt.isClosed()); 
        stmt.close();
        assertTrue("Statement should still be closed", stmt.isClosed()); 
    }
        
    /**
     * Execute a query on a statement after the parent connection has been
     * closed.
     */
    public void testStatementExecuteAfterConnectionClose() 
        throws SQLException {
        Connection con = stmt.getConnection();
        con.close();
        assertTrue("Connection should be closed after close()", 
                con.isClosed());
        try {
            stmt.executeQuery("select count(*) from stmtTable");
        } catch (SQLException sqle) {
            assertEquals("Unexpected SQL state for performing " +
                    "operations on a closed statement.",
                    SQLStateConstants.CONNECTION_EXCEPTION_CONNECTION_DOES_NOT_EXIST,
                    sqle.getSQLState());
        }
        assertTrue("Statement should be closed, because " +
                "the connection has been closed", stmt.isClosed()); 
    }

    public void testIsWrapperForStatement() throws SQLException {
        assertTrue(stmt.isWrapperFor(Statement.class));
    }

    public void testIsNotWrapperForPreparedStatement() throws SQLException {
        assertFalse(stmt.isWrapperFor(PreparedStatement.class));
    }

    public void testIsNotWrapperForCallableStatement() throws SQLException {
        assertFalse(stmt.isWrapperFor(CallableStatement.class));
    }

    public void testIsNotWrapperForResultSet() throws SQLException {
        assertFalse(stmt.isWrapperFor(ResultSet.class));
    }

    public void testUnwrapStatement() throws SQLException {
        Statement stmt2 = stmt.unwrap(Statement.class);
        assertSame("Unwrap returned wrong object.", stmt, stmt2);
    }

    public void testUnwrapPreparedStatement() {
        try {
            PreparedStatement ps = stmt.unwrap(PreparedStatement.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    public void testUnwrapCallableStatement() {
        try {
            CallableStatement cs = stmt.unwrap(CallableStatement.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    public void testUnwrapResultSet() throws SQLException {
        try {
            ResultSet rs = stmt.unwrap(ResultSet.class);
            fail("Unwrap didn't fail.");
        } catch (SQLException e) {
            assertSQLState("XJ128", e);
        }
    }

    /**
     * Tests isPoolable, setPoolable, and the default poolability.
     */
    public void testPoolable() throws SQLException {
        assertFalse("Statement cannot be poolable by default", 
                    stmt.isPoolable()); 
        stmt.setPoolable(true);
        assertTrue("Statement must be poolable", stmt.isPoolable());

        stmt.setPoolable(false);
        assertFalse("Statement cannot be poolable", stmt.isPoolable());
    }

    /**
     * Test that Statement.setQueryTimeout() causes statements to
     * raise SQLTimeoutException per the JDBC 4.1 spec clarification.
     */
    public  void    test_jdbc4_1_queryTimeoutException() throws Exception
    {
        SQLException    se = null;

        PreparedStatement ps = prepareStatement
            (
             "select columnnumber from sys.syscolumns c, sys.systables t\n" +
             "where t.tablename = 'SYSTABLES'\n" +
             "and t.tableid = c.referenceid\n" +
             "and mod( delay_st( 5, c.columnnumber ), 3 ) = 0"
             );
        println( "Testing timeout exception for a " + ps.getClass().getName() );
        
        SetQueryTimeoutTest.StatementExecutor   executor =
            new SetQueryTimeoutTest.StatementExecutor( ps, true, 1 );
        
        executor.start();
        executor.join();
        
        ps.close();
        
        se = executor.getSQLException();

        assertNotNull( se );
        assertEquals( SQLTimeoutException.class.getName(), se.getClass().getName() );
    }

    /**
     * Test the closeOnCompletion() and isCloseOnCompletion() methods
     * when using ResultSets which close implicitly.
     */
    public void testCompletionClosure_jdbc4_1_implicitRSClosure() throws Exception
    {
        Connection  conn = getConnection();
        conn.setHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
        conn.setAutoCommit( true );

        PreparedStatement   ps;
        ResultSet   rs;
        Wrapper41Statement  wrapper;

        ps = conn.prepareStatement( "values ( 1 )" );
        println( "Testing implicit closure WITH autocommit on a " + ps.getClass().getName() );
        
        wrapper = new Wrapper41Statement( ps );
        wrapper.closeOnCompletion();

        rs = ps.executeQuery();
        rs.next();
        rs.next();

        assertTrue( rs.isClosed() );
        assertTrue( ps.isClosed() );

        conn.setAutoCommit( false );

        // now retry the experiment with an explicit commit

        ps = conn.prepareStatement( "values ( 1 )" );
        println( "Testing implicit closure WITHOUT autocommit on a " + ps.getClass().getName() );
        
        wrapper = new Wrapper41Statement( ps );
        wrapper.closeOnCompletion();

        rs = ps.executeQuery();
        rs.next();
        rs.next();

        assertFalse( rs.isClosed() );
        assertFalse( ps.isClosed() );

        conn.commit();
        
        assertTrue( rs.isClosed() );
        assertTrue( ps.isClosed() );
    }

    /**
     * Test the large update methods added by JDBC 4.2.
     */
    public void testLargeUpdate_jdbc4_2() throws Exception
    {
        Connection  conn = getConnection();

        largeUpdate_jdbc4_2( conn );
    }

    public  static  void    largeUpdate_jdbc4_2( Connection conn )
        throws Exception
    {
        conn.prepareStatement
            (
             "create procedure setRowCountBase( newBase bigint )\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.jdbc4.StatementTest.setRowCountBase'\n"
             ).execute();
        conn.prepareStatement
            (
             "create table bigintTable( col1 int generated always as identity, col2 bigint )"
             ).execute();

        StatementWrapper  sw = new StatementWrapper( conn.createStatement() );

        largeUpdateTest( sw, (long) Integer.MAX_VALUE );
        largeUpdateTest( sw, 0L);

        largeBatchTest( sw, (long) Integer.MAX_VALUE );
        largeBatchTest( sw, 0L);

        largeMaxRowsTest( sw,  ((long) Integer.MAX_VALUE) + 1L );

        largeBatchUpdateExceptionTest( sw, ((long) Integer.MAX_VALUE) + 1L );
    }
    private static  void    largeUpdateTest( StatementWrapper sw, long rowCountBase )
        throws Exception
    {
        // poke the rowCountBase into the engine. all returned row counts will be
        // increased by this amount
        setRowCountBase( sw.getWrappedStatement(), false, rowCountBase );

        largeUpdateTest( sw, rowCountBase, 1L );
        largeUpdateTest( sw, rowCountBase, 3L );
    }
    private static  void    largeUpdateTest( StatementWrapper sw, long rowCountBase, long rowCount )
        throws Exception
    {
        StringBuffer    buffer = new StringBuffer();
        buffer.append( "insert into bigintTable( col2 ) values " );
        for ( long i = 0L; i < rowCount; i++ )
        {
            if ( i > 0L ) { buffer.append( ", " ); }
            buffer.append( "( " + i + " )" );
        }
        String  text = buffer.toString();

        long    expectedResult = rowCountBase + rowCount;

        vetUpdateSize( sw, (int) expectedResult,
                       sw.getWrappedStatement().executeUpdate( text ), expectedResult );
        vetUpdateSize( sw, (int) expectedResult,
                       sw.getWrappedStatement().executeUpdate( text, Statement.RETURN_GENERATED_KEYS ), expectedResult );
        vetUpdateSize( sw, (int) expectedResult,
                       sw.getWrappedStatement().executeUpdate( text, new int[] { 1 } ), expectedResult );
        vetUpdateSize( sw, (int) expectedResult,
                       sw.getWrappedStatement().executeUpdate( text, new String[] { "COL1" } ), expectedResult );

        vetUpdateSize( sw, expectedResult, sw.executeLargeUpdate( text ), expectedResult );
        vetUpdateSize( sw, expectedResult, sw.executeLargeUpdate( text, Statement.RETURN_GENERATED_KEYS ), expectedResult );
        vetUpdateSize( sw, expectedResult, sw.executeLargeUpdate( text, new int[] { 1 } ), expectedResult );
        vetUpdateSize( sw, expectedResult, sw.executeLargeUpdate( text, new String[] { "COL1" } ), expectedResult );
    }
    private static  void    vetUpdateSize( StatementWrapper sw, int expected, int actual, long longAnswer )
        throws Exception
    {
        assertEquals( expected, actual );
        assertEquals( expected, sw.getWrappedStatement().getUpdateCount() );
        assertEquals( longAnswer, sw.getLargeUpdateCount() );
    }
    private static  void    vetUpdateSize( StatementWrapper sw, long expected, long actual, long longAnswer )
        throws Exception
    {
        assertEquals( expected, actual );
        assertEquals( (int) expected, sw.getWrappedStatement().getUpdateCount() );
        assertEquals( longAnswer, sw.getLargeUpdateCount() );
    }
    private static  void    largeBatchTest( StatementWrapper sw, long rowCountBase )
        throws Exception
    {
        println( "Large batch test with rowCountBase = " + rowCountBase );
        
        // poke the rowCountBase into the engine. all returned row counts will be
        // increased by this amount
        sw.getWrappedStatement().clearBatch();
        setRowCountBase( sw.getWrappedStatement(), false, rowCountBase );

        long[]  expectedResult = new long[] { rowCountBase + 1L, rowCountBase + 1L, rowCountBase + 2L };

        createBatch( sw );
        assertEquals( sw.getWrappedStatement().executeBatch(), squashLongs( expectedResult ) );

        createBatch( sw );
        assertEquals( sw.executeLargeBatch(), expectedResult );
    }
    private static  void    createBatch( StatementTest.StatementWrapper sw )
        throws Exception
    {
        sw.getWrappedStatement().clearBatch();
        truncate( sw );
        sw.getWrappedStatement().addBatch( "insert into bigintTable( col2 ) values ( 1 )" );
        sw.getWrappedStatement().addBatch( "update bigintTable set col2 = 2" );
        sw.getWrappedStatement().addBatch( "insert into bigintTable( col2 ) values ( 3 ), ( 4 )" );
    }
    private static  void    largeMaxRowsTest( StatementWrapper sw, long maxRows )
        throws Exception
    {
        println( "Large max rows test with maxRows = " + maxRows );

        long    expectedRowCount = 3L;

        truncate( sw );
        sw.getWrappedStatement().execute( "insert into bigintTable( col2 ) values ( 1 ), ( 2 ), ( 3 ), ( 4 ), ( 5 )" );
        
        setRowCountBase( sw.getWrappedStatement(), usingDerbyNetClient(), maxRows - expectedRowCount );

        sw.setLargeMaxRows( maxRows );
        
        ResultSet   rs = sw.getWrappedStatement().executeQuery( "select * from bigintTable" );
        int     rowCount = 0;
        while( rs.next() ) { rowCount++; }
        rs.close();

        setRowCountBase( sw.getWrappedStatement(), usingDerbyNetClient(), 0L );
        
        assertEquals( expectedRowCount, rowCount );
        assertEquals( maxRows, sw.getLargeMaxRows() );
    }
    private static  void    largeBatchUpdateExceptionTest( StatementWrapper sw, long rowCountBase )
        throws Exception
    {
        println( "Large batch update exception test with rowCountBase = " + rowCountBase );
        
        sw.getWrappedStatement().clearBatch();
        sw.getWrappedStatement().execute( "create table intTable( col1 int generated always as identity, col2 int )" );

        // poke the rowCountBase into the engine. all returned row counts will be
        // increased by this amount
        setRowCountBase( sw.getWrappedStatement(), false, rowCountBase );

        //
        // Create a batch of statements. The last one will die on an overflow condition.
        //
        sw.getWrappedStatement().addBatch( "insert into intTable( col2 ) values ( 1 )" );
        sw.getWrappedStatement().addBatch( "insert into intTable( col2 ) values ( 1 )" );
        sw.getWrappedStatement().addBatch( "update intTable set col2 = 2147483647 + 1" );

        if ( usingEmbedded() )
        {
            BatchUpdateTest.assertBatchExecuteError
                (
                 "22003", sw.getWrappedStatement(),
                 new long[] { rowCountBase + 1L, rowCountBase + 1L }
                 );
        }
        else if ( usingDerbyNetClient() )
        {
            BatchUpdateTest.assertBatchExecuteError
                (
                 "XJ208", sw.getWrappedStatement(),
                 new long[] { rowCountBase + 1L, rowCountBase + 1L, -3 }
                 );
        }
        
        sw.getWrappedStatement().clearBatch();
        setRowCountBase( sw.getWrappedStatement(), false, 0L );
    }
        
    public static  void    setRowCountBase
        ( Statement stmt, boolean onClient, long rowCountBase )
        throws Exception
    {
        if ( onClient )
        {
            ClientStatement.fetchedRowBase = rowCountBase;
        }
        else
        {
            stmt.execute( "call setRowCountBase( " + rowCountBase + " )" );
        }
    }
    private static  void    truncate( StatementTest.StatementWrapper sw )
        throws Exception
    {
        sw.getWrappedStatement().execute( "truncate table bigintTable" );
    }
    private static  int[]   squashLongs( long[] longs )
    {
        int count = (longs == null) ? 0 : longs.length;
        int[]   ints = new int[ count ];
        for ( int i = 0; i < count; i++ ) { ints[ i ] = (int) longs[ i ]; }

        return ints;
    }
    
    /**
     * Create test suite for StatementTest.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("StatementTest suite");
        // Decorate test suite with a TestSetup class.
        suite.addTest(new StatementTestSetup(
                        new TestSuite(StatementTest.class)));
        suite.addTest(TestConfiguration.clientServerDecorator(
            new StatementTestSetup(new TestSuite(StatementTest.class))));
        return suite;
    }

    ////////////////////////////////////////////////////////////////////////
    //
    // NESTED JDBC 4.2 WRAPPER AROUND A Statement
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This wrapper is used to expose JDBC 4.2 methods which can run on
     * VM rev levels lower than Java 8.
     * </p>
     */
    public  static  class   StatementWrapper
    {
        private Statement   _wrappedStatement;

        public  StatementWrapper( Statement wrappedStatement )
        {
            _wrappedStatement = wrappedStatement;
        }

        public  Statement   getWrappedStatement() { return _wrappedStatement; }

        // New methods added by JDBC 4.2
        public  long[] executeLargeBatch() throws SQLException
        {
            return ((long[]) invoke
                (
                 "executeLargeBatch",
                 new Class[] {},
                 new Object[] {}
                 ));
        }
        public  long executeLargeUpdate( String sql ) throws SQLException
        {
            return ((Long) invoke
                (
                 "executeLargeUpdate",
                 new Class[] { String.class },
                 new Object[] { sql }
                 )).longValue();
        }
        public  long executeLargeUpdate( String sql, int autoGeneratedKeys) throws SQLException
        {
            return ((Long) invoke
                (
                 "executeLargeUpdate",
                 new Class[] { String.class, Integer.TYPE },
                 new Object[] { sql, new Integer( autoGeneratedKeys ) }
                 )).longValue();
        }
        public  long executeLargeUpdate( String sql, int[] columnIndexes ) throws SQLException
        {
            return ((Long) invoke
                (
                 "executeLargeUpdate",
                 new Class[] { String.class, columnIndexes.getClass() },
                 new Object[] { sql, columnIndexes }
                 )).longValue();
        }
        public  long executeLargeUpdate( String sql, String[] columnNames ) throws SQLException
        {
            return ((Long) invoke
                (
                 "executeLargeUpdate",
                 new Class[] { String.class, columnNames.getClass() },
                 new Object[] { sql, columnNames }
                 )).longValue();
        }
        public  long getLargeMaxRows() throws SQLException
        {
            return ((Long) invoke
                (
                 "getLargeMaxRows",
                 new Class[] {},
                 new Object[] {}
                 )).longValue();
        }
        public  long getLargeUpdateCount() throws SQLException
        {
            return ((Long) invoke
                (
                 "getLargeUpdateCount",
                 new Class[] {},
                 new Object[] {}
                 )).longValue();
        }
        public  void setLargeMaxRows( long max ) throws SQLException
        {
            invoke
                (
                 "setLargeMaxRows",
                 new Class[] { Long.TYPE },
                 new Object[] { new Long( max ) }
                 );
        }


        // Reflection minion
        protected Object  invoke( String methodName, Class[] argTypes, Object[] argValues )
            throws SQLException
        {
            try {
                Method  method = _wrappedStatement.getClass().getMethod( methodName, argTypes );

                return method.invoke( _wrappedStatement, argValues );
            }
            catch (NoSuchMethodException nsme) { throw wrap( nsme ); }
            catch (SecurityException se) { throw wrap( se ); }
            catch (IllegalAccessException iae) { throw wrap( iae ); }
            catch (IllegalArgumentException iare) { throw wrap( iare ); }
            catch (InvocationTargetException ite) { throw wrap( ite ); }
        }
        private SQLException    wrap( Throwable t ) { return new SQLException( t.getMessage(), t ); }
    }
    
    ////////////////////////////////////////////////////////////////////////
    //
    // PROCEDURE FOR BUMPING THE RETURNED ROW COUNT, FOR TESTING JDBC 4.2.
    //
    ////////////////////////////////////////////////////////////////////////

    /** Set the base which is used for returned row counts and fetched row counters */
    public  static  void    setRowCountBase( long newBase )
    {
        EmbedResultSet.fetchedRowBase = newBase;
        RowUtil.rowCountBase = newBase;
    }

} // End class StatementTest
