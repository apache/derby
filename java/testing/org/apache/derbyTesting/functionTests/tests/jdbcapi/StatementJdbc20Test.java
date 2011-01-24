/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.StatementJdbc20Test
 
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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test of additional methods in JDBC2.0  methods in statement and
 * resultset classes.
 * This test converts the old jdbcapi/statementJdbc20.java test
 * to JUnit.
 */
public class StatementJdbc20Test extends BaseJDBCTestCase {

    private static  final   String  METHOD_NOT_ALLOWED = "XJ016";
    private static  final   String  CLOSED_STATEMENT = "XJ012";
    
    /**
     * Create a test with the given name.
     *
     * @param name name of the test.
     */
    public StatementJdbc20Test(String name) {
        super(name);
    }
    
    /**
     * Create suite containing client and embedded tests and to run
     * all tests in this class
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("StatementJdbc20Test");
        suite.addTest(baseSuite("StatementJdbc20Test:embedded"));
        suite.addTest(
                TestConfiguration.clientServerDecorator(
                baseSuite("StatementJdbc20Test:client")));
        
        return suite;
    }
    
    private static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        
        suite.addTestSuite(StatementJdbc20Test.class);
        
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the tables used in the test
             * cases.
             *
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException {
                
                Connection conn = getConnection();
                
                /**
                 * Creates the table used in the test cases.
                 *
                 */
                stmt.execute("create table tab1 (i int, s smallint, r real)");
                stmt.executeUpdate("insert into tab1 values(1, 2, 3.1)");

                stmt.execute
                    (
                     "create procedure dynamic_results() " +
                     "language java parameter style java external name '" +
                     StatementJdbc20Test.class.getName() + ".dynamicResults' " +
                     "dynamic result sets 2"
                     );

                stmt.execute( "create table t_autogen( a int generated always as identity, b int )" );
            }
        };
    }
    
    /**
     * Testing wrong values for setFetchSize
     * and setFetchDirection.
     *
     * @exception SQLException if error occurs
     */
    public void testWrongVaues() throws SQLException {
        
        Statement stmt = createStatement();
        
        stmt.setFetchSize(25);
        stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
        stmt.setEscapeProcessing(true);
        
        //Error  testing  : set wrong values ..
        try {
            stmt.setFetchSize(-1000);
            fail("setFetchSize(-1000) expected to fail");
        } catch(SQLException e) {
            assertSQLState("XJ065", e);
        }
        
        try {
            stmt.setFetchDirection(-1000);
            fail("setFetchDirection(-1000) expected to fail");
        } catch(SQLException e){
            assertSQLState("XJ064", e);
        }
        
        assertEquals(stmt.getFetchSize(), 25);
        assertEquals(stmt.getFetchDirection(), ResultSet.FETCH_REVERSE);
        
        stmt.close();
    }
    /**
     * Tests reading data from database
     *
     * @exception SQLException 	if error occurs
     */
    public void testReadingData() throws SQLException {
        
        Statement stmt = createStatement();
        ResultSet rs;
        
        // read the data just for the heck of it
        rs = stmt.executeQuery("select * from tab1");
        while (rs.next()) {
            assertEquals(rs.getInt(1), 1);
            assertEquals(rs.getShort(2), 2);
            assertEquals(rs.getDouble(3), 3.1, 0.01);
        }
        
        rs.close();
        stmt.close();
    }
    /**
     * Tests values local to result set and get them back
     *
     * @exception SQLException 	if error occurs
     */
    public void testLocalValuesOfResultSet() throws SQLException {
        
        Statement stmt = createStatement();
        ResultSet rs;
        
        stmt.setFetchSize(25);
        stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
        stmt.setEscapeProcessing(true);
        
        rs = stmt.executeQuery("select * from tab1");
        // Get the constatnts for a result set
        assertEquals(rs.getFetchSize(), 25);
        assertEquals(rs.getFetchDirection(), ResultSet.FETCH_REVERSE);
        
        // change values local to result set and get them back
        rs.setFetchSize(250);
        try{
            rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        }catch(SQLException e){
            
            if (usingEmbedded())
                assertSQLState("XJ061", e);
            else
                assertSQLState("XJ125", e);
        }
        
        assertEquals(rs.getFetchSize(), 250);
        assertEquals(rs.getFetchDirection(), ResultSet.FETCH_REVERSE);
        
        // exception conditions
        stmt.setMaxRows(10);
        try{
            rs.setFetchSize(100);
        } catch(SQLException e){
            assertSQLState("XJ062", e);
        }
        
        //Error  testing  : set wrong values ..
        try{
            rs.setFetchSize(-2000);
            fail("setFetchSize(-2000) expected to fail");
        } catch(SQLException e){
            assertSQLState("XJ062", e);
        }
        
        try{
            rs.setFetchDirection(-2000);
        } catch(SQLException e){
            
            if (usingEmbedded())
                assertSQLState("XJ061", e);
            else
                assertSQLState("XJ125", e);
        }
        
        // set the fetch size values to zero .. to ensure
        // error condtions are correct !
        
        rs.setFetchSize(0);
        stmt.setFetchSize(0);
        
        rs.close();
    }
    /**
     * Tests creating tables with executeQuery which is
     * not allowed on statements that return a row count
     *
     * @exception SQLException
     *                if error occurs
     */
    public void testCreateTableWithExecuteQuery() throws SQLException {
        
        Statement stmt = createStatement();
        ResultSet rs;
        
        //RESOLVE - uncomment tests in 3.5
        // executeQuery() not allowed on statements
        // that return a row count
        try {
            stmt.executeQuery("create table trash(c1 int)");
        } catch (SQLException e) {
            if (usingEmbedded())
                assertSQLState("X0Y78", e);
            else
                assertSQLState("XJ207", e);
        }
        
        // verify that table was not created
        try {
            rs = stmt.executeQuery("select * from trash");
            System.out.println("select from trash expected to fail");
        } catch (SQLException e) {
            assertSQLState("42X05", e);
        }
        
        // executeUpdate() not allowed on statements
        // that return a ResultSet
        try {
            stmt.executeUpdate("values 1");
        } catch (SQLException e) {
            assertSQLState("X0Y79", e);
        }
        
        stmt.close();
        commit();
    }

    /**
     * Test the following clarification made by JDBC 4.1: You should
     * raise an exception when addBatch(String) is called on a PreparedStatement
     * or a CallableStatement.
     */
    public void testAddBatchClarification_jdbc4_1() throws SQLException
    {
        PreparedStatement ps = prepareStatement( "select * from sys.systables" );
        CallableStatement cs = prepareCall( "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)" );

        try {
            ps.addBatch( "select * from sys.systables" );
            fail( "Oops. ps.addBatch() worked." );
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
        
        try {
            cs.addBatch( "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)" );
            fail( "Oops. cs.addBatch() worked." );
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
    }

    /**
     * Test the closeOnCompletion() and isCloseOnCompletion() methods
     * added by JDBC 4.1.
     */
    public void testCompletionClosure_jdbc4_1() throws Exception
    {
        vetClosure( false, false );
        vetClosure( false, true );
        vetClosure( true, false );
        vetClosure( true, true );
    }
    private void vetClosure( boolean closeOnCompletion, boolean delayClosureCall ) throws Exception
    {
        vetClosedSelect( closeOnCompletion, delayClosureCall );
        vetClosedCall( closeOnCompletion, delayClosureCall );
        vetClosedPS( closeOnCompletion, delayClosureCall );
        vetClosedAutoGen( closeOnCompletion, delayClosureCall );
    }
    private void vetClosedSelect( boolean closeOnCompletion, boolean delayClosureCall ) throws Exception
    {
        Statement stmt = createStatement();
        ResultSet rs;

        println( "Verifying SELECT wrapper on " + stmt.getClass().getName() +
                 " with closeOnCompletion = " + closeOnCompletion +
                 " and delayClosureCall = " + delayClosureCall );
        Wrapper41Statement  wrapper = new Wrapper41Statement( stmt );

        if ( !delayClosureCall )
        { setCloseOnCompletion( wrapper, closeOnCompletion ); }

        rs = stmt.executeQuery( "select * from tab1" );

        if ( delayClosureCall )
        { setCloseOnCompletion( wrapper, closeOnCompletion ); }

        rs.close();

        assertEquals( closeOnCompletion, wrapper.isClosed() );

        vetSuccessfulClosure( wrapper, closeOnCompletion );
    }
    private void vetClosedCall( boolean closeOnCompletion, boolean delayClosureCall ) throws Exception
    {
        Statement stmt = createStatement();
        ResultSet rs;

        println( "Verifying CALL wrapper on " + stmt.getClass().getName() +
                 " with closeOnCompletion = " + closeOnCompletion +
                 " and delayClosureCall = " + delayClosureCall );
        Wrapper41Statement  wrapper = new Wrapper41Statement( stmt );

        if ( !delayClosureCall )
        { setCloseOnCompletion( wrapper, closeOnCompletion ); }

        assertTrue( stmt.execute( "call dynamic_results()" ) );

        assertFalse( wrapper.isClosed() );

        ResultSet   rs1 = stmt.getResultSet();

        if ( delayClosureCall )
        { setCloseOnCompletion( wrapper, closeOnCompletion ); }

        assertTrue( stmt.getMoreResults() ); // implicitly closes rs1
        assertFalse( wrapper.isClosed() );

        ResultSet   rs2 = stmt.getResultSet();
        rs2.close();
        assertEquals( closeOnCompletion, wrapper.isClosed() );

        vetSuccessfulClosure( wrapper, closeOnCompletion );
    }
    private void vetClosedPS( boolean closeOnCompletion, boolean delayClosureCall ) throws Exception
    {
        PreparedStatement ps = getConnection().prepareStatement( "select * from tab1" );
        ResultSet rs;

        println( "Verifying PreparedStatement wrapper on " + ps.getClass().getName() +
                 " with closeOnCompletion = " + closeOnCompletion +
                 " and delayClosureCall = " + delayClosureCall );
        Wrapper41Statement  wrapper = new Wrapper41Statement( ps );

        if ( !delayClosureCall )
        { setCloseOnCompletion( wrapper, closeOnCompletion ); }

        rs = ps.executeQuery();

        if ( delayClosureCall )
        { setCloseOnCompletion( wrapper, closeOnCompletion ); }

        rs.close();

        assertEquals( closeOnCompletion, wrapper.isClosed() );

        vetSuccessfulClosure( wrapper, closeOnCompletion );

        if ( !wrapper.isClosed() ) { ps.close(); }
    }
    private void vetClosedAutoGen( boolean closeOnCompletion, boolean delayClosureCall ) throws Exception
    {
        Statement stmt = createStatement();
        ResultSet rs;

        println( "Verifying AUTOGENERATED KEYS wrapper on " + stmt.getClass().getName() +
                 " with closeOnCompletion = " + closeOnCompletion +
                 " and delayClosureCall = " + delayClosureCall );
        Wrapper41Statement  wrapper = new Wrapper41Statement( stmt );

        if ( !delayClosureCall )
        { setCloseOnCompletion( wrapper, closeOnCompletion ); }

        stmt.executeUpdate( "insert into t_autogen( b ) values ( 1 )", Statement.RETURN_GENERATED_KEYS );
        rs = stmt.getGeneratedKeys();

        if ( delayClosureCall )
        { setCloseOnCompletion( wrapper, closeOnCompletion ); }

        assertFalse( wrapper.isClosed() );

        rs.close();

        assertEquals( closeOnCompletion, wrapper.isClosed() );

        vetSuccessfulClosure( wrapper, closeOnCompletion );
    }
    
    private void    setCloseOnCompletion( Wrapper41Statement wrapper, boolean closeOnCompletion )
        throws Exception
    {
        assertFalse( wrapper.isCloseOnCompletion() );
        if ( closeOnCompletion ) { wrapper.closeOnCompletion(); }
        assertEquals( closeOnCompletion, wrapper.isCloseOnCompletion() );
    }
    private void    vetSuccessfulClosure
        ( Wrapper41Statement wrapper, boolean closeOnCompletion )
        throws Exception
    {
        if ( closeOnCompletion )
        {
            try {
                wrapper.closeOnCompletion();
                fail( "Expected closed statement failure." );
            }
            catch (SQLException se)
            {
                assertSQLState( CLOSED_STATEMENT, se );
            }

            try {
                wrapper.isCloseOnCompletion();
                fail( "Expected closed statement failure." );
            }
            catch (SQLException se)
            {
                assertSQLState( CLOSED_STATEMENT, se );
            }
        }
    }
    
    /**
     * <p>
     * Spec clarifications in JDBC 4.1. Verify that an exception is raised if a PreparedStatement
     * or CallableStatement tries to execute one of the methods which compiles SQL text.
     * </p>
     */
    public  void    test_clarification_jdbc4_1()    throws Exception
    {
        Connection  conn = getConnection();

        PreparedStatement   ps = conn.prepareStatement( "select * from sys.syscolumns" );
        CallableStatement   cs = conn.prepareCall( "call dynamic_results()" );

        vetNotAllowed( ps );
        vetNotAllowed( cs );

        ps.close();
        cs.close();
    }
    private void    vetNotAllowed( Statement stmt ) throws Exception
    {
        println( "Vetting a " + stmt.getClass().getName() );
        
        try {
            stmt.execute( "select * from sys.systables" );
            failVNA();
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
        
        try {
            stmt.execute( "select * from sys.systables", Statement.NO_GENERATED_KEYS );
            failVNA();
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
        
        try {
            stmt.execute( "select * from sys.systables", new int[] { 1 } );
            failVNA();
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
        
        try {
            stmt.execute( "select * from sys.systables", new String[] { "COLUMNNAME" } );
            failVNA();
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
        
        try {
            stmt.executeQuery( "select * from sys.systables" );
            failVNA();
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
        
        try {
            stmt.executeUpdate( "insert into tab1 values(1, 2, 3.1)" );
            failVNA();
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
        
        try {
            stmt.executeUpdate( "insert into tab1 values(1, 2, 3.1)", Statement.NO_GENERATED_KEYS );
            failVNA();
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
        
        try {
            stmt.executeUpdate( "insert into tab1 values(1, 2, 3.1)", new int[] { 1 } );
            failVNA();
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
        
        try {
            stmt.executeUpdate( "insert into tab1 values(1, 2, 3.1)", new String[] { "COLUMNNAME" } );
            failVNA();
        }
        catch (SQLException se)
        {
            assertSQLState( METHOD_NOT_ALLOWED, se );
        }
    }
    private void    failVNA() throws Exception
    {
            fail( "Should have failed when run on a PreparedStatement or CallableStatement" );
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // PROCEDURES
    //
    ///////////////////////////////////////////////////////////////////////


    /**
     * Stored procedure which returns 2 ResultSets.
     */
    public static void dynamicResults
        ( ResultSet[] rs1, ResultSet[] rs2 )
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        rs1[0] = c.createStatement().executeQuery("VALUES(1)");
        rs2[0] = c.createStatement().executeQuery("VALUES(2)");
    }

}
