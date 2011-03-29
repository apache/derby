/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.ProcedureTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derby.iapi.types.HarmonySerialClob;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.functionTests.tests.lang.Price;

/**
 * Tests of stored procedures.
 */
public class ProcedureTest extends BaseJDBCTestCase {

    /**
     * Creates a new <code>ProcedureTest</code> instance.
     *
     * @param name name of the test
     */
    public ProcedureTest(String name) {
        super(name);
    }

    // TESTS

    /**
     * Tests that <code>Statement.executeQuery()</code> fails when no
     * result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithNoDynamicResultSets() throws SQLException {
        Statement stmt = createStatement();
        try {
            stmt.executeQuery("CALL RETRIEVE_DYNAMIC_RESULTS(0)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>Statement.executeQuery()</code> succeeds when
     * one result set is returned from a stored procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithOneDynamicResultSet() throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("CALL RETRIEVE_DYNAMIC_RESULTS(1)");
        assertNotNull("executeQuery() returned null.", rs);
        assertSame(stmt, rs.getStatement());
        JDBC.assertDrainResultsHasData(rs);
    }

    /**
     * Tests that <code>Statement.executeQuery()</code> fails when
     * multiple result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithMoreThanOneDynamicResultSet()
        throws SQLException
    {
        Statement stmt = createStatement();
        try {
            stmt.executeQuery("CALL RETRIEVE_DYNAMIC_RESULTS(2)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>Statement.executeUpdate()</code> succeeds when
     * no result sets are returned.
     *
     * <p>Currently, this test fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithNoDynamicResultSets()
        throws SQLException
    {
        Statement stmt = createStatement();
        assertUpdateCount(stmt, 0, "CALL RETRIEVE_DYNAMIC_RESULTS(0)");
        JDBC.assertNoMoreResults(stmt);
    }

    /**
     * Tests that <code>Statement.executeUpdate()</code> fails when a
     * result set is returned from a stored procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithOneDynamicResultSet() throws SQLException {
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("CALL RETRIEVE_DYNAMIC_RESULTS(1)");
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
    }

    /**
     * Tests that <code>PreparedStatement.executeQuery()</code> fails
     * when no result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithNoDynamicResultSets_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 0);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>PreparedStatement.executeQuery()</code>
     * succeeds when one result set is returned from a stored
     * procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithOneDynamicResultSet_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        assertNotNull("executeQuery() returned null.", rs);
        assertSame(ps, rs.getStatement());
        JDBC.assertDrainResultsHasData(rs);

    }

    /**
     * Tests that <code>PreparedStatement.executeQuery()</code> fails
     * when multiple result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithMoreThanOneDynamicResultSet_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 2);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>PreparedStatement.executeUpdate()</code>
     * succeeds when no result sets are returned.
     *
     * <p>Currently, this test fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithNoDynamicResultSets_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 0);
        assertUpdateCount(ps, 0);
        JDBC.assertNoMoreResults(ps);
    }

    /**
     * Tests that <code>PreparedStatement.executeUpdate()</code> fails
     * when a result set is returned from a stored procedure.
     *
     * <p>Currently, this test fails with
     * JCC. However, the corresponding tests for
     * <code>Statement</code> and <code>CallableStatement</code>
     * succeed. Strange...
     *
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithOneDynamicResultSet_prepared()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        ps.setInt(1, 1);
        try {
            ps.executeUpdate();
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
    }

    /**
     * Tests that <code>CallableStatement.executeQuery()</code> fails
     * when no result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithNoDynamicResultSets_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 0);
        try {
            cs.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>CallableStatement.executeQuery()</code>
     * succeeds when one result set is returned from a stored
     * procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithOneDynamicResultSet_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 1);
        ResultSet rs = cs.executeQuery();
        assertNotNull("executeQuery() returned null.", rs);
        assertSame(cs, rs.getStatement());
        JDBC.assertDrainResultsHasData(rs);
    }

    /**
     * Tests that <code>CallableStatement.executeQuery()</code> fails
     * when multiple result sets are returned.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteQueryWithMoreThanOneDynamicResultSet_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 2);
        try {
            cs.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that <code>CallableStatement.executeUpdate()</code>
     * succeeds when no result sets are returned.
     *
     * <p>Currently, this test fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithNoDynamicResultSets_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 0);
        assertUpdateCount(cs, 0);
        JDBC.assertNoMoreResults(cs);
    }

    /**
     * Tests that <code>CallableStatement.executeUpdate()</code> fails
     * when a result set is returned from a stored procedure.
     * @exception SQLException if a database error occurs
     */
    public void testExecuteUpdateWithOneDynamicResultSet_callable()
        throws SQLException
    {
        CallableStatement cs =
            prepareCall("CALL RETRIEVE_DYNAMIC_RESULTS(?)");
        cs.setInt(1, 1);
        try {
            cs.executeUpdate();
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when
     * <code>Connection.rollback()</code> is called.
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWithExecuteQuery() throws SQLException {

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(1)");
        rs.close();
        rollback();
        
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeUpdate()</code> are correctly rolled back when
     * <code>Connection.rollback()</code> is called.
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWithExecuteUpdate() throws SQLException {
        Statement stmt = createStatement();
        stmt.executeUpdate("CALL PROC_WITH_SIDE_EFFECTS(0)");
        rollback();
        
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when the
     * query fails because the number of returned result sets is zero.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteQueryReturnsNothing()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        Statement stmt = createStatement();
        try {
            stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(0)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }

        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when the
     * query fails because the number of returned result sets is more
     * than one.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteQueryReturnsTooMuch()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        Statement stmt = createStatement();
        try {
            stmt.executeQuery("CALL PROC_WITH_SIDE_EFFECTS(2)");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeUpdate()</code> are correctly rolled back when the
     * query fails because the stored procedure returned a result set.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteUpdateReturnsResults()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("CALL PROC_WITH_SIDE_EFFECTS(1)");
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when the
     * query fails because the number of returned result sets is zero.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteQueryReturnsNothing_prepared()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        PreparedStatement ps =
            prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
        ps.setInt(1, 0);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
        Statement stmt = createStatement();
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeQuery()</code> are correctly rolled back when the
     * query fails because the number of returned result sets is more
     * than one.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void testRollbackStoredProcWhenExecuteQueryReturnsTooMuch_prepared()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        PreparedStatement ps =
            prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
        ps.setInt(1, 2);
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertMultipleResultsFromExecuteQuery(sqle);
        }
        Statement stmt = createStatement();
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
     }

    /**
     * Tests that the effects of executing a stored procedure with
     * <code>executeUpdate()</code> are correctly rolled back when the
     * query fails because the stored procedure returned a result set.
     *
     * <p> This test case fails with JCC.
     *
     * @exception SQLException if a database error occurs
     */
    public void
        testRollbackStoredProcWhenExecuteUpdateReturnsResults_prepared()
        throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        PreparedStatement ps =
            prepareStatement("CALL PROC_WITH_SIDE_EFFECTS(?)");
        ps.setInt(1, 1);
        try {
            ps.executeUpdate();
            fail("executeUpdate() didn't fail.");
        } catch (SQLException sqle) {
            assertResultsFromExecuteUpdate(sqle);
        }
        Statement stmt = createStatement();
        // Expect Side effects from stored procedure to be rolled back.
        JDBC.assertEmpty(stmt.executeQuery("SELECT * FROM SIMPLE_TABLE"));
 
    }

    /**
     * Tests that closed result sets are not returned when calling
     * <code>executeQuery()</code>.
     * @exception SQLException if a database error occurs
     */
    public void testClosedDynamicResultSetsFromExecuteQuery()
        throws SQLException
    {
        Statement stmt = createStatement();
        try {
            ResultSet rs = stmt.executeQuery("CALL RETRIEVE_CLOSED_RESULT()");
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that closed result sets are ignored when calling
     * <code>executeUpdate()</code>.
     * @exception SQLException if a database error occurs
     */
    public void testClosedDynamicResultSetsFromExecuteUpdate()
        throws SQLException
    {
        Statement stmt = createStatement();
        stmt.executeUpdate("CALL RETRIEVE_CLOSED_RESULT()");
        JDBC.assertNoMoreResults(stmt);
    }

    /**
     * Tests that dynamic result sets from other connections are
     * ignored when calling <code>executeQuery</code>.
     * @exception SQLException if a database error occurs
     */
    public void testDynamicResultSetsFromOtherConnectionWithExecuteQuery()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_EXTERNAL_RESULT(?,?,?)");
        
        ps.setString(1, getTestConfiguration().getDefaultDatabaseName());
        ps.setString(2, getTestConfiguration().getUserName());
        ps.setString(3, getTestConfiguration().getUserPassword());
        try {
            ps.executeQuery();
            fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            assertNoResultSetFromExecuteQuery(sqle);
        }
    }

    /**
     * Tests that dynamic result sets from other connections are
     * ignored when calling <code>executeUpdate</code>.
     * @exception SQLException if a database error occurs
     */
    public void testDynamicResultSetsFromOtherConnectionWithExecuteUpdate()
        throws SQLException
    {
        PreparedStatement ps =
            prepareStatement("CALL RETRIEVE_EXTERNAL_RESULT(?,?,?)");
        
        ps.setString(1, getTestConfiguration().getDefaultDatabaseName());
        ps.setString(2, getTestConfiguration().getUserName());
        ps.setString(3, getTestConfiguration().getUserPassword());
        
        ps.executeUpdate();
        
        JDBC.assertNoMoreResults(ps);
    }

    /**
     * Test that a call to getBlob() to retrieve the value of a non-BLOB
     * parameter fails with the expected SQLException. Used to throw
     * ClassCastException, see DERBY-4970.
     */
    public void testGetBlobFromIntParameter() throws SQLException {
        CallableStatement cs = prepareCall("call int_out(?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        try {
            cs.getBlob(1);
            fail("getBlob() on int parameter expected to fail");
        } catch (SQLException sqle) {
            assertSQLState("22005", sqle);
        }
    }

    /**
     * Test that a call to getClob() to retrieve the value of a non-CLOB
     * parameter fails with the expected SQLException. Used to throw
     * ClassCastException, see DERBY-4970.
     */
    public void testGetClobFromIntParameter() throws SQLException {
        CallableStatement cs = prepareCall("call int_out(?)");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        try {
            cs.getClob(1);
            fail("getClob() on int parameter expected to fail");
        } catch (SQLException sqle) {
            assertSQLState("22005", sqle);
        }
    }

    /**
     * Test that INOUT args are preserved over procedure invocations.
     * See DERBY-2515.
     */
    public  void    test_2515()   throws Exception
    {
        Connection  conn = getConnection();
        
        PreparedStatement ps = conn.prepareStatement
            (
             "create type price_2515 external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java\n"
             );
        ps.execute();
        ps.close();
        
        ps = conn.prepareStatement
            (
             "create procedure proc_2515\n" +
             "(\n" +
             "\tin passNumber int,\n" +
             "\tout returnMessage varchar( 32672 ),\n" +
             "\tinout bigintArg bigint,\n" +
             "\tinout blobArg blob,\n" +
             "inout booleanArg boolean,\n" +
             "inout charArg char( 6 ),\n" +
             "inout charForBitDataArg char( 3 ) for bit data,\n" +
             "inout clobArg clob,\n" +
             "inout dateArg date,\n" +
             "inout decimalArg decimal,\n" +
             "inout doubleArg double,\n" +
             "inout intArg int,\n" +
             "inout longVarcharArg long varchar,\n" +
             "inout longVarcharForBitDataArg long varchar for bit data,\n" +
             "inout realArg real,\n" +
             "inout smallintArg smallint,\n" +
             "inout timeArg time,\n" +
             "inout timestampArg timestamp,\n" +
             "inout priceArg price_2515,\n" +
             "inout varcharArg varchar( 20 ),\n" +
             "inout varcharForBitDataArg varchar( 3 ) for bit data\n" +
            ")\n" +
            "parameter style java language java no sql\n" +
            "external name '" + ProcedureTest.class.getName() + ".proc_2515'"
             );
        ps.execute();
        ps.close();

        CallableStatement   cs = conn.prepareCall
            ( "call proc_2515( ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ? )" );
        AllTypesTuple   firstArgs = makeFirstAllTypesTuple();

        int     idx = 2;
        
        cs.registerOutParameter( idx++, Types.VARCHAR );

        cs.registerOutParameter( idx, Types.BIGINT );
        cs.setLong( idx++, firstArgs.get_bigintArg().longValue() );

        cs.registerOutParameter( idx, Types.BLOB );
        cs.setBlob( idx++, firstArgs.get_blobArg() );

        cs.registerOutParameter( idx, Types.BOOLEAN );
        cs.setBoolean( idx++, firstArgs.get_booleanArg().booleanValue() );

        cs.registerOutParameter( idx, Types.CHAR );
        cs.setString( idx++, firstArgs.get_charArg() );

        cs.registerOutParameter( idx, Types.BINARY );
        cs.setBytes( idx++, firstArgs.get_charForBitDataArg() );

        cs.registerOutParameter( idx, Types.CLOB );
        cs.setClob( idx++, firstArgs.get_clobArg() );

        cs.registerOutParameter( idx, Types.DATE );
        cs.setDate( idx++, firstArgs.get_dateArg() );

        cs.registerOutParameter( idx, Types.DECIMAL );
        cs.setBigDecimal( idx++, firstArgs.get_decimalArg() );

        cs.registerOutParameter( idx, Types.DOUBLE );
        cs.setDouble( idx++, firstArgs.get_doubleArg().doubleValue() );

        cs.registerOutParameter( idx, Types.INTEGER );
        cs.setInt( idx++, firstArgs.get_intArg().intValue() );

        cs.registerOutParameter( idx, Types.LONGVARCHAR );
        cs.setString( idx++, firstArgs.get_longVarcharArg() );

        cs.registerOutParameter( idx, Types.LONGVARBINARY );
        cs.setBytes( idx++, firstArgs.get_longVarcharForBitDataArg() );

        cs.registerOutParameter( idx, Types.REAL );
        cs.setFloat( idx++, firstArgs.get_realArg().floatValue() );

        cs.registerOutParameter( idx, Types.SMALLINT );
        cs.setShort( idx++, firstArgs.get_smallintArg().shortValue() );

        cs.registerOutParameter( idx, Types.TIME );
        cs.setTime( idx++, firstArgs.get_timeArg() );

        cs.registerOutParameter( idx, Types.TIMESTAMP );
        cs.setTimestamp( idx++, firstArgs.get_timestampArg() );

        cs.registerOutParameter( idx, Types.JAVA_OBJECT );
        cs.setObject( idx++, firstArgs.get_priceArg() );

        cs.registerOutParameter( idx, Types.VARCHAR );
        cs.setString( idx++, firstArgs.get_varcharArg() );

        cs.registerOutParameter( idx, Types.VARBINARY );
        cs.setBytes( idx++, firstArgs.get_varcharForBitDataArg() );

        cs.setInt( 1, 0 );
        cs.execute();
        assertEquals( "", cs.getString( 2 ) );  // the return message should be empty, meaning the call args were what the procedure expected
        assertEquals( "", makeSecondAllTypesTuple().compare( getActualReturnArgs( cs ) ) );

        cs.setInt( 1, 1 );
        cs.execute();
        assertEquals( "", cs.getString( 2 ) );  // the return message should be empty, meaning the call args were what the procedure expected
        assertEquals( "", makeThirdAllTypesTuple().compare( getActualReturnArgs( cs ) ) );

        cs.setInt( 1, 2 );
        cs.execute();
        assertEquals( "", cs.getString( 2 ) );  // the return message should be empty, meaning the call args were what the procedure expected
        assertEquals( "", makeFourthAllTypesTuple().compare( getActualReturnArgs( cs ) ) );

        ps = conn.prepareStatement( "drop procedure proc_2515" );
        ps.execute();
        ps.close();

        ps = conn.prepareStatement( "drop type price_2515 restrict" );
        ps.execute();
        ps.close();
    }
    private AllTypesTuple   getActualReturnArgs( CallableStatement cs )
        throws Exception
    {
        int idx = 3;
        
        return new AllTypesTuple
            (
             (Long) cs.getObject( idx++ ),
             (Blob) cs.getObject( idx++ ),
             (Boolean) cs.getObject( idx++ ),
             (String) cs.getObject( idx++ ),
             (byte[]) cs.getObject( idx++ ),
             (Clob) cs.getObject( idx++ ),
             (Date) cs.getObject( idx++ ),
             (BigDecimal) cs.getObject( idx++ ),
             (Double) cs.getObject( idx++ ),
             (Integer) cs.getObject( idx++ ),
             (String) cs.getObject( idx++ ),
             (byte[]) cs.getObject( idx++ ),
             (Float) cs.getObject( idx++ ),
             (Integer) cs.getObject( idx++ ),
             (Time) cs.getObject( idx++ ),
             (Timestamp) cs.getObject( idx++ ),
             (Price) cs.getObject( idx++ ),
             (String) cs.getObject( idx++ ),
             (byte[]) cs.getObject( idx++ )
             );
    }

    // UTILITY METHODS

    /**
     * Raises an exception if the exception is not caused by
     * <code>executeQuery()</code> returning no result set.
     *
     * @param sqle a <code>SQLException</code> value
     */
    private void assertNoResultSetFromExecuteQuery(SQLException sqle) {
        if (usingDB2Client()) {
            assertNull("Unexpected SQL state.", sqle.getSQLState());
        } else {
            assertSQLState("Unexpected SQL state.", "X0Y78", sqle);
        }
    }

    /**
     * Raises an exception if the exception is not caused by
     * <code>executeQuery()</code> returning multiple result sets.
     *
     * @param sqle a <code>SQLException</code> value
     */
    private void assertMultipleResultsFromExecuteQuery(SQLException sqle)
    {
        if (usingDB2Client()) {
            assertNull("Unexpected SQL state.", sqle.getSQLState());
        } else {
            assertSQLState("Unexpected SQL state.", "X0Y78", sqle);
        }
    }

    /**
     * Raises an exception if the exception is not caused by
     * <code>executeUpdate()</code> returning result sets.
     *
     * @param sqle a <code>SQLException</code> value
     */
    private void assertResultsFromExecuteUpdate(SQLException sqle) {
        if (usingDB2Client()) {
            assertNull("Unexpected SQL state.", sqle.getSQLState());
        } else {
            assertSQLState("Unexpected SQL state.", "X0Y79", sqle);
        }

    }

    // SETUP

    /**
     * Runs the test fixtures in embedded and client.
     * @return test suite
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("ProcedureTest");

        suite.addTest(baseSuite("ProcedureTest:embedded"));

        suite.addTest(
                TestConfiguration.clientServerDecorator(
                        baseSuite("ProcedureTest:client")));    
        return suite;
    }

    /**
     * Creates the test suite and wraps it in a <code>TestSetup</code>
     * instance which sets up and tears down the test environment.
     * @return test suite
     */
    private static Test baseSuite(String name)
    {
        TestSuite suite = new TestSuite(name);
        
        // Need JDBC DriverManager to run these tests
        if (!JDBC.vmSupportsJDBC3())
            return suite;
        
        suite.addTestSuite(ProcedureTest.class);
        
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the tables and the stored procedures used in the test
             * cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement s) throws SQLException
            {
                for (int i = 0; i < PROCEDURES.length; i++) {
                    s.execute(PROCEDURES[i]);
                }
                for (int i = 0; i < TABLES.length; i++) {
                    s.execute(TABLES[i][1]);
                }
            }
        };
    }

    /**
     * Sets up the connection for a test case and clears all tables
     * used in the test cases.
     * @exception SQLException if a database error occurs
     */
    public void setUp() throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement s = createStatement();
        for (int i = 0; i < TABLES.length; i++) {
            s.execute("DELETE FROM " + TABLES[i][0]);
        }
        commit();
    }

    /**
     * Procedures that should be created before the tests are run and
     * dropped when the tests have finished. First element in each row
     * is the name of the procedure, second element is SQL which
     * creates it.
     */
    private static final String[] PROCEDURES = {
       
          "CREATE PROCEDURE RETRIEVE_DYNAMIC_RESULTS(number INT) " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".retrieveDynamicResults' " +
          "DYNAMIC RESULT SETS 4",


          "CREATE PROCEDURE RETRIEVE_CLOSED_RESULT() LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".retrieveClosedResult' " +
          "DYNAMIC RESULT SETS 1",

          "CREATE PROCEDURE RETRIEVE_EXTERNAL_RESULT(" +
          "DBNAME VARCHAR(128), DBUSER VARCHAR(128), DBPWD VARCHAR(128)) LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".retrieveExternalResult' " +
          "DYNAMIC RESULT SETS 1",

          "CREATE PROCEDURE PROC_WITH_SIDE_EFFECTS(ret INT) LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".procWithSideEffects' " +
          "DYNAMIC RESULT SETS 2",
          
          "CREATE PROCEDURE NESTED_RESULT_SETS(proctext VARCHAR(128)) LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".nestedDynamicResultSets' " +
          "DYNAMIC RESULT SETS 6",

          "CREATE PROCEDURE INT_OUT(OUT X INTEGER) LANGUAGE JAVA " +
          "PARAMETER STYLE JAVA EXTERNAL NAME '" +
          ProcedureTest.class.getName() + ".intOut'",
    };

    /**
     * Tables that should be created before the tests are run and
     * dropped when the tests have finished. The tables will be
     * cleared before each test case is run. First element in each row
     * is the name of the table, second element is the SQL text which
     * creates it.
     */
    private static final String[][] TABLES = {
        // SIMPLE_TABLE is used by PROC_WITH_SIDE_EFFECTS
        { "SIMPLE_TABLE", "CREATE TABLE SIMPLE_TABLE (id INT)" },
    };

    // PROCEDURES

    /**
     * Stored procedure which returns 0, 1, 2, 3 or 4 <code>ResultSet</code>s.
     *
     * @param number the number of <code>ResultSet</code>s to return
     * @param rs1 first <code>ResultSet</code>
     * @param rs2 second <code>ResultSet</code>
     * @param rs3 third <code>ResultSet</code>
     * @param rs4 fourth <code>ResultSet</code>
     * @exception SQLException if a database error occurs
     */
    public static void retrieveDynamicResults(int number,
                                              ResultSet[] rs1,
                                              ResultSet[] rs2,
                                              ResultSet[] rs3,
                                              ResultSet[] rs4)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        if (number > 0) {
            rs1[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        if (number > 1) {
            rs2[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        if (number > 2) {
            rs3[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        if (number > 3) {
            rs4[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        c.close();
    }

    /**
     * Stored procedure which produces a closed result set.
     *
     * @param closed holder for the closed result set
     * @exception SQLException if a database error occurs
     */
    public static void retrieveClosedResult(ResultSet[] closed)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        closed[0] = c.createStatement().executeQuery("VALUES(1)");
        closed[0].close();
        c.close();
    }

    /**
     * Stored procedure which produces a result set in another
     * connection.
     *
     * @param external result set from another connection
     * @exception SQLException if a database error occurs
     */
    public static void retrieveExternalResult(String dbName, 
            String user, String password, ResultSet[] external)
        throws SQLException
    {
        // Use a server-side connection to the same database.
        String url = "jdbc:derby:" + dbName;
        
        Connection conn = DriverManager.getConnection(url, user, password);
        
        external[0] =
            conn.createStatement().executeQuery("VALUES(1)");
    }

    /**
     * Stored procedure which inserts a row into SIMPLE_TABLE and
     * optionally returns result sets.
     *
     * @param returnResults if one, return one result set; if greater
     * than one, return two result sets; otherwise, return no result
     * set
     * @param rs1 first result set to return
     * @param rs2 second result set to return
     * @exception SQLException if a database error occurs
     */
    public static void procWithSideEffects(int returnResults,
                                           ResultSet[] rs1,
                                           ResultSet[] rs2)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement stmt = c.createStatement();
        stmt.executeUpdate("INSERT INTO SIMPLE_TABLE VALUES (42)");
        if (returnResults > 0) {
            rs1[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        if (returnResults > 1) {
            rs2[0] = c.createStatement().executeQuery("VALUES(1)");
        }
        c.close();
    }
    
    /**
     * Method for a Java procedure that calls another procedure
     * and just passes on the dynamic results from that call.
     */
    public static void nestedDynamicResultSets(String procedureText,
            ResultSet[] rs1, ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4,
            ResultSet[] rs5, ResultSet[] rs6)
    throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        
        CallableStatement cs = c.prepareCall("CALL " + procedureText);
        
        cs.execute();
        
        // Mix up the order of the result sets in the returned
        // parameters, ensures order is defined by creation
        // and not parameter order.
        rs6[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs3[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs4[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs2[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs1[0] = cs.getResultSet();
        if (!cs.getMoreResults(Statement.KEEP_CURRENT_RESULT))
            return;
        rs5[0] = cs.getResultSet();
    
    }

    /**
     * Stored procedure with an integer output parameter.
     * @param out an output parameter
     */
    public static void intOut(int[] out) {
        out[0] = 42;
    }

    /**
     * Procedure to test that INOUT args preserve their value when the
     * procedure is re-executed (DERBY-2515). If you add a new datatype
     * to Derby, you will need to add a new argument at the end of this
     * procedure's signature.
     */
    public static  AllTypesTuple   makeFirstAllTypesTuple() throws Exception
    {
        return new AllTypesTuple
            (
             new Long( 1L ),
             new HarmonySerialBlob( new byte[] { (byte) 1, (byte) 1, (byte) 1 } ),
             Boolean.TRUE,
             "firstt",
             new byte[] { (byte) 1, (byte) 1, (byte) 1 },
             new HarmonySerialClob( "firstt" ),
             new Date( 1L ),
             new BigDecimal( 1 ),
             new Double( 1.0 ),
             new Integer( 1 ),
             new String( "firstt" ),
             new byte[] { (byte) 1, (byte) 1, (byte) 1 },
             new Float( 1.0F ),
             new Integer( 1 ),
             new Time( 1L ),
             new Timestamp( 1L ),
             new Price( "USD", new BigDecimal( 1 ), new Timestamp( 1 ) ),
             "firstt",
             new byte[] { (byte) 1, (byte) 1, (byte) 1 }
             );
    }
    public static  AllTypesTuple   makeSecondAllTypesTuple() throws Exception
    {
        return new AllTypesTuple
            (
             new Long( 2L ),
             new HarmonySerialBlob( new byte[] { (byte) 2, (byte) 2, (byte) 2 } ),
             Boolean.FALSE,
             "second",
             new byte[] { (byte) 2, (byte) 2, (byte) 2 },
             new HarmonySerialClob( "second" ),
             new Date( 2L ),
             new BigDecimal( 2 ),
             new Double( 2.0 ),
             new Integer( 2 ),
             new String( "second" ),
             new byte[] { (byte) 2, (byte) 2, (byte) 2 },
             new Float( 2.0F ),
             new Integer( 2 ),
             new Time( 2L ),
             new Timestamp( 2L ),
             new Price( "USD", new BigDecimal( 2 ), new Timestamp( 2 ) ),
             "second",
             new byte[] { (byte) 2, (byte) 2, (byte) 2 }
             );
    }
    public static  AllTypesTuple   makeThirdAllTypesTuple() throws Exception
    {
        return new AllTypesTuple
            (
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null,
             null
             );
    }
    public static  AllTypesTuple   makeFourthAllTypesTuple() throws Exception
    {
        return makeFirstAllTypesTuple();
    }
    public  static  void    proc_2515
        (
         int passNumber,
         String[] message,
         
         Long[] bigintArg,
         Blob[] blobArg,
         Boolean[] booleanArg,
         String[] charArg,
         byte[][] charForBitDataArg,
         Clob[] clobArg,
         Date[] dateArg,
         BigDecimal[] decimalArg,
         Double[] doubleArg,
         Integer[] intArg,
         String[] longVarcharArg,
         byte[][] longVarcharForBitDataArg,
         Float[] realArg,
         Integer[] smallintArg,
         Time[] timeArg,
         Timestamp[] timestampArg,
         Price[] priceArg,
         String[] varcharArg,
         byte[][] varcharForBitDataArg
         )
        throws Exception
    {
        AllTypesTuple   actualCallSignature = new AllTypesTuple
            (
             bigintArg[ 0 ],
             blobArg[ 0 ],
             booleanArg[ 0 ],
             charArg[ 0 ],
             charForBitDataArg[ 0 ],
             clobArg[ 0 ],
             dateArg[ 0 ],
             decimalArg[ 0 ],
             doubleArg[ 0 ],
             intArg[ 0 ],
             longVarcharArg[ 0 ],
             longVarcharForBitDataArg[ 0 ],
             realArg[ 0 ],
             smallintArg[ 0 ],
             timeArg[ 0 ],
             timestampArg[ 0 ],
             priceArg[ 0 ],
             varcharArg[ 0 ],
             varcharForBitDataArg[ 0 ]
             );
        AllTypesTuple   expectedCallSignature;
        AllTypesTuple   returnSignature;

        switch( passNumber )
        {
        case 0:
            expectedCallSignature = makeFirstAllTypesTuple();
            returnSignature = makeSecondAllTypesTuple();
            break;
        case 1:
            expectedCallSignature = makeSecondAllTypesTuple();
            returnSignature = makeThirdAllTypesTuple();
            break;
        case 2:
        default:
            expectedCallSignature = makeThirdAllTypesTuple();
            returnSignature = makeFourthAllTypesTuple();
            break;
        }

        message[ 0 ] = expectedCallSignature.compare( actualCallSignature );
        
        bigintArg[ 0 ] = returnSignature.get_bigintArg();
        blobArg[ 0 ] = returnSignature.get_blobArg();
        booleanArg[ 0 ] = returnSignature.get_booleanArg();
        charArg[ 0 ] = returnSignature.get_charArg();
        charForBitDataArg[ 0 ] = returnSignature.get_charForBitDataArg();
        clobArg[ 0 ] = returnSignature.get_clobArg();
        dateArg[ 0 ] = returnSignature.get_dateArg();
        decimalArg[ 0 ] = returnSignature.get_decimalArg();
        doubleArg[ 0 ] = returnSignature.get_doubleArg();
        intArg[ 0 ] = returnSignature.get_intArg();
        longVarcharArg[ 0 ] = returnSignature.get_longVarcharArg();
        longVarcharForBitDataArg[ 0 ] = returnSignature.get_longVarcharForBitDataArg();
        realArg[ 0 ] = returnSignature.get_realArg();
        smallintArg[ 0 ] = returnSignature.get_smallintArg();
        timeArg[ 0 ] = returnSignature.get_timeArg();
        timestampArg[ 0 ] = returnSignature.get_timestampArg();
        priceArg[ 0 ] = returnSignature.get_priceArg();
        varcharArg[ 0 ] = returnSignature.get_varcharArg();
        varcharForBitDataArg[ 0 ] = returnSignature.get_varcharForBitDataArg();
    }
    
        /**
         * Test various combinations of getMoreResults
         * 
         * @throws SQLException
         */
        public void testGetMoreResults() throws SQLException {

                Statement s = createStatement();
                

                s.executeUpdate("create table MRS.FIVERS(i integer)");
                PreparedStatement ps = prepareStatement("insert into MRS.FIVERS values (?)");
                for (int i = 1; i <= 20; i++) {
                        ps.setInt(1, i);
                        ps.executeUpdate();
                }

                // create a procedure that returns 5 result sets.
                        
                s.executeUpdate("create procedure MRS.FIVEJP() parameter style JAVA READS SQL DATA dynamic result sets 5 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.fivejp'");


                CallableStatement cs = prepareCall("CALL MRS.FIVEJP()");
                ResultSet[] allRS = new ResultSet[5];

                defaultGetMoreResults(cs, allRS);
                java.util.Arrays.fill(allRS, null);
                closeCurrentGetMoreResults(cs, allRS);
                java.util.Arrays.fill(allRS, null);
                keepCurrentGetMoreResults(cs, allRS);                              
                java.util.Arrays.fill(allRS, null);
                mixedGetMoreResults(cs, allRS);
                java.util.Arrays.fill(allRS, null);
                checkExecuteClosesResults(cs, allRS);
                java.util.Arrays.fill(allRS, null);
                checkCSCloseClosesResults(cs,allRS);
                java.util.Arrays.fill(allRS, null);
                
                // a procedure that calls another procedure that returns
                // dynamic result sets, see if the result sets are handled
                // correctly through the nesting.
                CallableStatement nestedCs = prepareCall(
                        "CALL NESTED_RESULT_SETS('MRS.FIVEJP()')");
                defaultGetMoreResults(nestedCs, allRS);
                
        }

        
        /**
         * Check that CallableStatement.execute() closes results
         * @param cs
         * @param allRS
         * @throws SQLException
         */
        private void checkExecuteClosesResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            //Fetching result sets with getMoreResults(Statement.KEEP_CURRENT_RESULT) and checking that cs.execute() closes them");          
            cs.execute();
            int pass = 0;
            do {

                    allRS[pass++] = cs.getResultSet();      
                    assertSame(cs, allRS[pass-1].getStatement());
                    // expect everything to stay open.                        

            } while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            //fetched all results
            // All should still be open.
            for (int i = 0; i < 5; i++)
                JDBC.assertDrainResults(allRS[i]);                
            
            cs.execute();
            // all should be closed.
            for (int i = 0; i < 5; i++)
                JDBC.assertClosed(allRS[i]);
        }

        /**
         * Check that CallableStatement.close() closes results
         * @param cs
         * @param allRS
         * @throws SQLException
         */
        private void checkCSCloseClosesResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            cs.execute();
            int pass = 0;
            do {

                    allRS[pass++] = cs.getResultSet();         
                    assertSame(cs, allRS[pass-1].getStatement());
                    // expect everything to stay open.                        

            } while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            //fetched all results
            // All should still be open.
            for (int i = 0; i < 5; i++)
                JDBC.assertDrainResults(allRS[i]);                
            
            cs.close();
            // all should be closed.
            for (int i = 0; i < 5; i++)
                JDBC.assertClosed(allRS[i]);
        }

        private void mixedGetMoreResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            //Fetching result sets with getMoreResults(<mixture>)"
            cs.execute();

            //first two with KEEP_CURRENT_RESULT"
            allRS[0] = cs.getResultSet();
            assertSame(cs, allRS[0].getStatement());
            boolean moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            if (!moreRS)
                    fail("FAIL - no second result set");
            allRS[1] = cs.getResultSet();   
            assertSame(cs, allRS[1].getStatement());
            // two open
            allRS[0].next();
            assertEquals(2,allRS[0].getInt(1));
            allRS[1].next();
            assertEquals(3,allRS[1].getInt(1));
            
            //third with CLOSE_CURRENT_RESULT"
            moreRS = cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
            if (!moreRS)
                    fail("FAIL - no third result set");
            // first and third open
            allRS[2] = cs.getResultSet();
            assertSame(cs, allRS[2].getStatement());
            assertEquals(2,allRS[0].getInt(1));
            JDBC.assertClosed(allRS[1]);
            allRS[2].next();
            assertEquals(4,allRS[2].getInt(1));

            
            //fourth with KEEP_CURRENT_RESULT"
            moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            if (!moreRS)
                    fail("FAIL - no fourth result set");
            allRS[3] = cs.getResultSet();
            assertSame(cs, allRS[3].getStatement());
            allRS[3].next();
            // first, third and fourth open, second closed
            assertEquals(2,allRS[0].getInt(1));
            JDBC.assertClosed(allRS[1]);
            assertEquals(4,allRS[2].getInt(1));
            assertEquals(5,allRS[3].getInt(1));
            
            //fifth with CLOSE_ALL_RESULTS"
            moreRS = cs.getMoreResults(Statement.CLOSE_ALL_RESULTS);
            if (!moreRS)
                   fail("FAIL - no fifth result set");
            allRS[4] = cs.getResultSet();
            assertSame(cs, allRS[4].getStatement());
            allRS[4].next();
            // only fifth open
            JDBC.assertClosed(allRS[0]);
            JDBC.assertClosed(allRS[1]);
            JDBC.assertClosed(allRS[2]);
            JDBC.assertClosed(allRS[3]);
            assertEquals(6,allRS[4].getInt(1));
            
            //no more results with with KEEP_CURRENT_RESULT"
            moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            if (moreRS)
                    fail("FAIL - too many result sets");
            // only fifth open
            JDBC.assertClosed(allRS[0]);
            JDBC.assertClosed(allRS[1]);
            JDBC.assertClosed(allRS[2]);
            JDBC.assertClosed(allRS[3]);
            assertEquals(6,allRS[4].getInt(1));
            
            allRS[4].close();
        }

        /**
         * Check getMoreResults(Statement.KEEP_CURRENT_RESULT)  
         * 
         * @param cs
         * @param allRS
         * @throws SQLException
         */
        private void keepCurrentGetMoreResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            cs.execute();
            
            for (int i = 0; i < 5; i++)
            {
                allRS[i] = cs.getResultSet();
                assertSame(cs, allRS[i].getStatement());
                allRS[i].next();
                assertEquals(2+i, allRS[i].getInt(1));
                
                if (i < 4)
                    assertTrue(cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
                else
                    assertFalse(cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
            }            
            
            // resultSets should still be open
            for (int i = 0; i < 5; i++)
                JDBC.assertDrainResults(allRS[i]);
        }

        private void closeCurrentGetMoreResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            cs.execute();
            
            for (int i = 0; i < 5; i++)
            {
                allRS[i] = cs.getResultSet();
                assertSame(cs, allRS[i].getStatement());
                allRS[i].next();
                assertEquals(2+i, allRS[i].getInt(1));
                
                if (i < 4)
                    assertTrue(cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
                else
                    assertFalse(cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
            }
            
            // verify resultSets are closed
            for (int i = 0; i < 5; i++)
                JDBC.assertClosed(allRS[i]);
        }

        /**
         * Test default getMoreResults() closes result set.
         * @param cs
         * @param allRS
         * @throws SQLException
         */
        private void defaultGetMoreResults(CallableStatement cs, ResultSet[] allRS) throws SQLException {
            // execute the procedure that returns 5 result sets and then use the various
            // options of getMoreResults().

            cs.execute();
            
            for (int i = 0; i < 5; i++)
            {
                allRS[i] = cs.getResultSet();
                assertSame(cs, allRS[i].getStatement());
                allRS[i].next();
                assertEquals(2+i, allRS[i].getInt(1));
                
                if (i < 4)
                    assertTrue(cs.getMoreResults());
                else
                    assertFalse(cs.getMoreResults());
            } 
                        
            // verify resultSets are closed
            for (int i = 0; i < 5; i++)
                JDBC.assertClosed(allRS[i]);
        }

    ////////////////////////////////////////////
    //
    // Nested classes.
    //
    ////////////////////////////////////////////

    public  static  final   class   AllTypesTuple
    {
        private Long _bigintArg;
        private Blob _blobArg;
        private Boolean _booleanArg;
        private String _charArg;
        private byte[] _charForBitDataArg;
        private Clob _clobArg;
        private Date _dateArg;
        private BigDecimal _decimalArg;
        private Double _doubleArg;
        private Integer _intArg;
        private String _longVarcharArg;
        private byte[] _longVarcharForBitDataArg;
        private Float _realArg;
        private Integer _smallintArg;
        private Time _timeArg;
        private Timestamp _timestampArg;
        private Price _priceArg;
        private String _varcharArg;
        private byte[] _varcharForBitDataArg;

        public AllTypesTuple
            (
             Long  bigintArg,
             Blob  blobArg,
             Boolean  booleanArg,
             String  charArg,
             byte[]   charForBitDataArg,
             Clob  clobArg,
             Date  dateArg,
             BigDecimal  decimalArg,
             Double  doubleArg,
             Integer  intArg,
             String  longVarcharArg,
             byte[]   longVarcharForBitDataArg,
             Float  realArg,
             Integer  smallintArg,
             Time  timeArg,
             Timestamp  timestampArg,
             Price  priceArg,
             String  varcharArg,
             byte[]   varcharForBitDataArg
             )
        {
            _bigintArg = bigintArg;
            _blobArg = blobArg;
            _booleanArg = booleanArg;
            _charArg = charArg;
            _charForBitDataArg = charForBitDataArg;
            _clobArg = clobArg;
            _dateArg = dateArg;
            _decimalArg = decimalArg;
            _doubleArg = doubleArg;
            _intArg = intArg;
            _longVarcharArg = longVarcharArg;
            _longVarcharForBitDataArg = longVarcharForBitDataArg;
            _realArg = realArg;
            _smallintArg = smallintArg;
            _timeArg = timeArg;
            _timestampArg = timestampArg;
            _priceArg = priceArg;
            _varcharArg = varcharArg;
            _varcharForBitDataArg = varcharForBitDataArg;
        }

        public Long get_bigintArg() { return _bigintArg; }
        public Blob get_blobArg() { return _blobArg; }
        public Boolean get_booleanArg() { return _booleanArg; }
        public String get_charArg() { return _charArg; }
        public byte[] get_charForBitDataArg() { return _charForBitDataArg; }
        public Clob get_clobArg() { return _clobArg; }
        public Date get_dateArg() { return _dateArg; }
        public BigDecimal get_decimalArg() { return _decimalArg; }
        public Double get_doubleArg() { return _doubleArg; }
        public Integer get_intArg() { return _intArg; }
        public String get_longVarcharArg() { return _longVarcharArg; }
        public byte[] get_longVarcharForBitDataArg() { return _longVarcharForBitDataArg; }
        public Float get_realArg() { return _realArg; }
        public Integer get_smallintArg() { return _smallintArg; }
        public Time get_timeArg() { return _timeArg; }
        public Timestamp get_timestampArg() { return _timestampArg; }
        public Price get_priceArg() { return _priceArg; }
        public String get_varcharArg() { return _varcharArg; }
        public byte[] get_varcharForBitDataArg() { return _varcharForBitDataArg; }

        public  String  compare( AllTypesTuple that ) throws Exception
        {
            String  message = "";

            message = message + compare( "_bigintArg", this._bigintArg, that._bigintArg );
            message = message + compare( "_blobArg", this.getBlobBytes(), that.getBlobBytes() );
            message = message + compare( "_booleanArg", this._booleanArg, that._booleanArg );
            message = message + compare( "_charArg", this._charArg, that._charArg );
            message = message + compare( "_charForBitDataArg", this._charForBitDataArg, that._charForBitDataArg );
            message = message + compare( "_clobArg", this.getClobString(), that.getClobString() );
            message = message + compare( "_dateArg", this.getDateString(), that.getDateString() );
            message = message + compare( "_decimalArg", this._decimalArg, that._decimalArg );
            message = message + compare( "_doubleArg", this._doubleArg, that._doubleArg );
            message = message + compare( "_intArg", this._intArg, that._intArg );
            message = message + compare( "_longVarcharArg", this._longVarcharArg, that._longVarcharArg );
            message = message + compare( "_longVarcharForBitDataArg", this._longVarcharForBitDataArg, that._longVarcharForBitDataArg );
            message = message + compare( "_realArg", this._realArg, that._realArg );
            message = message + compare( "_smallintArg", this._smallintArg, that._smallintArg );
            message = message + compare( "_timeArg", this.getTimeString(), that.getTimeString() );
            message = message + compare( "_timestampArg", this._timestampArg, that._timestampArg );
            message = message + compare( "_priceArg", this._priceArg, that._priceArg );
            message = message + compare( "_varcharArg", this._varcharArg, that._varcharArg );
            message = message + compare( "_varcharForBitDataArg", this._varcharForBitDataArg, that._varcharForBitDataArg );

            return message;
        }
        private byte[]  getBlobBytes() throws Exception
        {
            if ( _blobArg == null ) { return null; }
            else { return _blobArg.getBytes( 1, (int) _blobArg.length() ); }
        }
        private String  getClobString() throws Exception
        {
            if ( _clobArg == null ) { return null; }
            else { return _clobArg.getSubString( 1, (int) _clobArg.length() ); }
        }
        private String  getDateString()
        {
            if ( _dateArg ==  null ) { return null; }
            else { return _dateArg.toString(); }
        }
        private String  getTimeString()
        {
            if ( _timeArg == null ) { return null; }
            else { return _timeArg.toString(); }
        }
        private String  compare( String argName, Object left, Object right )
        {
            if ( left == null )
            {
                if ( right == null ) { return ""; }
                return (argName + ": left was null but right was " + right);
            }
            if ( right == null ) { return (argName + ": left = " + left + " but right is null" ); }
            if ( left instanceof byte[] ) { return compareBytes( argName, (byte[]) left, (byte[]) right ); }

            if ( left.equals( right ) ) { return ""; }

            return (argName + ": left = " + left + " but right = " + right);
        }
        private String  compareBytes( String argName, byte[] left, byte[] right )
        {
            int count = left.length;

            if ( count != right.length )
            {
                return (argName + ": left count = " + count + " but right count = " + right.length );
            }
            for ( int i = 0; i < count; i++ )
            {
                if ( left[ i ] != right[ i ] )
                {
                    return (argName + ": left[ " + i + " ] = " + left[ i ] + " but right[ " + i + " ] = " + right[ i ] );
                }
            }

            return "";
        }
    }

    

}
