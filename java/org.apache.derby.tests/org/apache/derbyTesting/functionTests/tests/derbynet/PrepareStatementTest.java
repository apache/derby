/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.PrepareStatementTest

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.DataTruncation;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This Junit test class tests the JDBC PreparedStatement.  This test
 * is a Junit version of the old prepStmt.java test.  That test tested
 * prepared statements in client/server context, and many of the test
 * cases is specifically testing corner cases in client/server
 * communication.  However, this Junit test is set up to run as part
 * of both the embedded and client/server test suites.
 */

public class PrepareStatementTest extends BaseJDBCTestCase
{

    /**
     * Creates a new instance of PrepareStatementTest
     *
     * @param name name of the test.
     */
    public PrepareStatementTest(String name)
    {
        super(name);
    }


    /**
     * Adds this class to the default suite.  That is, all test cases will be
     * run in both embedded and client/server.
     */
    public static Test suite()
    {
        if ( JDBC.vmSupportsJSR169())
            // see DERBY-2233 for details
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
                        return new BaseTestSuite(
                "empty PrepareStatementTest - client not supported on JSR169");
        else
        {
            BaseTestSuite suite = new BaseTestSuite("PrepareStatementTest");
            suite.addTest(TestConfiguration.defaultSuite(PrepareStatementTest.class));
            suite.addTest(TestConfiguration.clientServerDecorator(
                TestConfiguration.connectionCPDecorator(new CleanDatabaseTestSetup(
                    new BaseTestSuite(PrepareStatementTest.class)))));
            return suite;
        }
    }


    /**
     * Test basic prepare mechanism with executeUpdate and executeQuery with
     * and without simple parameters.
     */
    public void testBasicPrepare() throws Exception
    {
        // executeUpdate() without parameters
        PreparedStatement pSt
            = prepareStatement("create table t1(c1 int, c2 int, c3 int)");
        assertUpdateCount(pSt, 0);
        pSt.close();

        // Rows to be inserted in table t1 for this test
        final Integer[][] t1_rows = {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            {99, 5, 9},
            {2, 6, 10},
            {7, 5, 8}
        };

        // executeUpdate() with parameters
        pSt = prepareStatement("insert into t1 values (?, " + t1_rows[0][1]
                               + ", ?)");
        pSt.setInt(1, t1_rows[0][0].intValue());
        pSt.setInt(2, t1_rows[0][2].intValue());
        assertUpdateCount(pSt, 1);
        pSt.close();

        // execute() with parameters, no result set returned
        pSt = prepareStatement("insert into t1 values (" + t1_rows[1][0] + ", "
                               + t1_rows[1][1] + ", ?), (?, " + t1_rows[2][1]
                               + ", " + t1_rows[2][2] + ")");
        pSt.setInt(1, t1_rows[1][2].intValue());
        pSt.setInt(2, t1_rows[2][0].intValue());
        boolean hasResultSet = pSt.execute();
        while (hasResultSet)
        {
            ResultSet rs = pSt.getResultSet();
            assertFalse(rs.next());
            rs.close();
            hasResultSet = pSt.getMoreResults();
        }
        assertEquals(2, pSt.getUpdateCount());
        pSt.close();

        // executeQuery() without parameters
        pSt = prepareStatement("select * from t1");
        ResultSet rs = pSt.executeQuery();
        JDBC.assertFullResultSet(rs, t1_rows, false);
        rs.close();
        pSt.close();

        // Create table with subset of rows to be selected in query below
        Integer[][] t1filter_rows = new Integer[2][];
        for (int i=0, j=0; i < t1_rows.length; ++i) {
            if (t1_rows[i][1].intValue() == 5) {
                t1filter_rows[j++] = t1_rows[i];
            }
        }

        // executeQuery() with parameters
        pSt = prepareStatement("select * from t1 where c2 = ?");
        pSt.setInt(1, 5);
        rs = pSt.executeQuery();
        JDBC.assertFullResultSet(rs, t1filter_rows, false);
        rs.close();
        pSt.close();

        // execute() with parameters, with result set returned
        pSt = prepareStatement("select * from t1 where c2 = ?");
        pSt.setInt(1, 5);
        assertTrue(pSt.execute());
        rs = pSt.getResultSet();
        JDBC.assertFullResultSet(rs, t1filter_rows, false);
        rs.close();
        assertFalse(pSt.getMoreResults());
        assertEquals(-1, pSt.getUpdateCount());
        pSt.close();
    }


    /**
     * Tests different data types for input parameters of a PreparedStatement.
     */
    public void testParameterTypes() throws Exception
    {
        PreparedStatement pSt = prepareStatement(
            "create table t2(si smallint,i int, bi bigint, r real, f float, "
            + "d double precision, n5_2 numeric(5,2), dec10_3 decimal(10,3), "
            + "ch20 char(20),vc varchar(20), lvc long varchar, "
            + "b20 char(23) for bit data, vb varchar(23) for bit data, "
            + "lvb long varchar for bit data,  dt date, tm time, "
            + "ts timestamp not null)");
        assertUpdateCount(pSt, 0);
        pSt.close();

        // byte array for binary values.
        byte[] ba = new byte[] { 0x0,0x1,0x2,0x3,0x4,0x5,0x6,0x7,0x8,0x9,0xa,
                                 0xb,0xc,0xd,0xe,0xf,0x10,0x11,0x12,0x13 };

        byte[] bapad = new byte[23];  // For padded byte data
        System.arraycopy(ba, 0, bapad, 0, ba.length);
        // Pad with space!!!
        Arrays.fill(bapad, ba.length, bapad.length, (byte )0x20);

        // Values to be inserted
        Object[][] t2_rows = {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            {1, 2, 3L, 4.0F,
             5.0, 6.0, new BigDecimal("77.77"),
             new BigDecimal("8.100"), "column9string       ",
             "column10vcstring", "column11lvcstring", bapad, ba, ba,
             Date.valueOf("2002-04-12"), Time.valueOf("11:44:30"),
             Timestamp.valueOf("2002-04-12 11:44:30.000000000")},
            {1, 2, 3L, 4.0F,
             5.0, 6.0, new BigDecimal("77.77"),
             new BigDecimal("8.100"), "column11string      ",
             "column10vcstring", "column11lvcstring", bapad, ba, ba,
             Date.valueOf("2002-04-12"), Time.valueOf("11:44:30"),
             Timestamp.valueOf("2002-04-12 11:44:30.000000000")},
            {null, null, null, null, null, null, null, null, null, null, null,
             null, null, null, null, null,
             Timestamp.valueOf("2002-04-12 11:44:31.000000000")}
        };


        pSt = prepareStatement(
            "insert into t2 values (?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            + "?, ? ,? , ?)");
        pSt.setShort(1, ((Integer )t2_rows[0][0]).shortValue());
        pSt.setInt(2, ((Integer )t2_rows[0][1]).intValue());
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        pSt.setLong(3, ((Long)t2_rows[0][2]).longValue());
        pSt.setFloat(4, ((Float )t2_rows[0][3]).floatValue());
        pSt.setDouble(5, ((Double )t2_rows[0][4]).doubleValue());
        pSt.setDouble(6, ((Double )t2_rows[0][5]).doubleValue());
        pSt.setBigDecimal(7, (BigDecimal )t2_rows[0][6]);
        pSt.setBigDecimal(8, new BigDecimal("8.1")); // Diff. precision
        pSt.setString(9, "column9string");  // Without padding
        byte[] c10ba = ((String )t2_rows[0][9]).getBytes("UTF-8");
        int len = c10ba.length;
        pSt.setAsciiStream(10, new ByteArrayInputStream(c10ba), len);
        byte[] c11ba = ((String )t2_rows[0][10]).getBytes("UTF-8");
        len = c11ba.length;
        pSt.setCharacterStream(11, new InputStreamReader
                               (new ByteArrayInputStream(c11ba),"UTF-8"),len);
        pSt.setBytes(12, ba);
        pSt.setBinaryStream(13, new ByteArrayInputStream(ba), ba.length);
        pSt.setBytes(14, ba);
        pSt.setDate(15, ((Date )t2_rows[0][14]));
        pSt.setTime(16, ((Time )t2_rows[0][15]));
        pSt.setTimestamp(17, ((Timestamp )t2_rows[0][16]));
        assertUpdateCount(pSt, 1);

        // test setObject on different datatypes of the input parameters of
        // PreparedStatement
        for (int i=0; i<17; ++i) {
            pSt.setObject(i+1, t2_rows[1][i]);
        }
        assertUpdateCount(pSt, 1);

        // test setNull on different datatypes of the input parameters of
        // PreparedStatement
        pSt.setNull(1, java.sql.Types.SMALLINT);
        pSt.setNull(2, java.sql.Types.INTEGER);
        pSt.setNull(3, java.sql.Types.BIGINT);
        pSt.setNull(4, java.sql.Types.REAL);
        pSt.setNull(5, java.sql.Types.FLOAT);
        pSt.setNull(6, java.sql.Types.DOUBLE);
        pSt.setNull(7, java.sql.Types.NUMERIC);
        pSt.setNull(8, java.sql.Types.DECIMAL);
        pSt.setNull(9, java.sql.Types.CHAR);
        pSt.setNull(10, java.sql.Types.VARCHAR);
        pSt.setNull(11, java.sql.Types.LONGVARCHAR);
        pSt.setNull(12, java.sql.Types.BINARY);
        pSt.setNull(13, java.sql.Types.VARBINARY);
        pSt.setNull(14, java.sql.Types.LONGVARBINARY);
        pSt.setNull(15, java.sql.Types.DATE);
        pSt.setNull(16, java.sql.Types.TIME);

        pSt.setTimestamp(17, ((Timestamp )t2_rows[2][16]));
        assertFalse(pSt.execute());
        assertEquals(1, pSt.getUpdateCount());
        pSt.close();

        pSt = prepareStatement("select * from t2");
        ResultSet rs = pSt.executeQuery();
        JDBC.assertFullResultSet(rs, t2_rows, false);
        rs.close();
        pSt.close();

        // negative test cases with no parameters set
        try {
            pSt = prepareStatement("select * from t2 where i = ?");
            rs = pSt.executeQuery();
            fail("Exception expected above!");
        } catch (SQLException e) {
            assertSQLState("07000", e);
        }
        rs.close();
        pSt.close();


        try {
            pSt = prepareStatement(
                "insert into t2 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                + "?, ?, ?, ?, ?)");
            pSt.executeUpdate();
            fail("Exception expected above!");
        } catch (SQLException e) {
            assertSQLState("07000", e);
        }
        pSt.close();

        // Some test cases for DERBY-2558, involving validation of the
        // parameterIndex argument to the 4-argument overload of setObject
        //
        pSt = prepareStatement("create table d2558 (i int)");
        assertUpdateCount(pSt, 0);
        pSt.close();
        pSt = prepareStatement("insert into d2558 values (3), (4)");
        assertUpdateCount(pSt, 2);
        pSt.close();
        pSt = prepareStatement("select * from d2558 where i = ?");
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        pSt.setObject(1,3,java.sql.Types.INTEGER,0);
        try {
            // There's only 1 parameter marker, so this should fail:
            pSt.setObject(2,4, java.sql.Types.INTEGER,0);
            rs = pSt.executeQuery();
            rs.close();
            fail("Exception expected above!");
        } catch (SQLException e) {
            assertSQLState("XCL13", e);
        }
        pSt.close();

    }


    /**
     * Test prepared statements with a large number of input parameters.
     */
    public void testBigTable() throws Exception
    {
        int tabSize = 1000;
        StringBuffer createBigTabSql
            = new StringBuffer("create table bigtab (");
        for (int i = 1; i <= tabSize; ++i) {
            createBigTabSql.append("c");
            createBigTabSql.append(i);
            createBigTabSql.append(" int");
            createBigTabSql.append((i != tabSize) ? ", " : " )");
        }

        PreparedStatement pSt = prepareStatement(createBigTabSql.toString());
        assertUpdateCount(pSt, 0);
        pSt.close();

        insertTab("bigtab", 50);
        insertTab("bigtab", 200);
        insertTab("bigtab", 300);
        insertTab("bigtab", 500);
        // prepared Statement with many  params (bug 4863)
        insertTab("bigtab", 1000);
        selectFromBigTab();

        // Negative Cases
        try {
            insertTab("bigtab", 1001);
            fail("Exception expected above!");
        } catch (SQLException e) {
            assertSQLState("42X14", e);
        }
        // this one will give a sytax error
        try {
            insertTab("bigtab", 0);
            fail("Exception expected above!");
        } catch (SQLException e) {
            assertSQLState("42X01", e);
        }
        // table doesn't exist
        try {
            insertTab("wrongtab",1000);
            fail("Exception expected above!");
        } catch (SQLException e) {
            assertSQLState("42X05", e);
        }
    }

    /**
     * Insert 1 row with the given number of columns into the given table.
     * It is assumed that the table has numCols, named c1, c2, ...
     * This is a helper method for the testBigTable test case.
     *
     * @param tabName Table name
     * @param numCols Number of columns in table.
     */
    private void insertTab(String tabName, int numCols)
        throws SQLException
    {
        StringBuffer insertSql
            = new StringBuffer("insert into " + tabName + "(");
        for (int i = 1; i <= numCols; ++i)
        {
            insertSql.append(" c");
            insertSql.append(i);
            insertSql.append((i != numCols) ? ", " : ")");
        }
        insertSql.append("  values (");
        for (int i = 1; i <= numCols; ++i)
        {
            insertSql.append("?");
            insertSql.append((i != numCols) ? ", " : " )");
        }

        PreparedStatement pSt = prepareStatement(insertSql.toString());
        for (int i = 1; i <= numCols; ++i) {
            pSt.setInt(i, i);
        }
        assertUpdateCount(pSt, 1);
        pSt.close();
    }

    /**
     * Test that the table bigtab contains the expected tuples for the test
     * case testBigTable.
     */
    private void selectFromBigTab() throws SQLException
    {
        String selectSQL = "select * from bigtab";
        PreparedStatement pSt = prepareStatement(selectSQL);
        ResultSet rs = pSt.executeQuery();

        int i = 0;
        while (rs.next())
        {
            switch(++i) {
                case 1:
                case 2:
                case 3:
                    assertNull(rs.getObject(500));
                    assertNull(rs.getObject(1000));
                    break;
                case 4:
                    assertEquals(rs.getInt(500), 500);
                    assertNull(rs.getObject(1000));
                    break;
                case 5:
                    assertEquals(rs.getInt(500), 500);
                    assertEquals(rs.getInt(1000), 1000);
                    break;
                default:
                    fail("Too many rows in bigTab");
            }
        }
        assertEquals(i, 5);

        rs.close();
        pSt.close();

    }


    /**
     * Check that values are preserved when BigDecimal values
     * which have more than 31 digits are converted to Double
     * with setObject.
     */
    public void testBigDecimalSetObject() throws SQLException
    {
        getConnection().setAutoCommit(false);
        String sql = "CREATE TABLE doubletab (i int, doubleVal DOUBLE)";
        Statement stmt = createStatement();
        assertUpdateCount(stmt, 0, sql);
        stmt.close();
        commit();

        // Insert various double values
        double[] doubleVals = {1.0E-130, 1.0E125, 0, -1.0E124};
        BigDecimal[] bigDecimalVals =
            { new BigDecimal(1.0E-130),
              new BigDecimal(1.0E125),
              new BigDecimal(-1.0E124),
              new BigDecimal("12345678901234567890123456789012"),
              new BigDecimal("1.2345678901234567890123456789012")
        };

        String isql = "INSERT INTO doubletab VALUES (?, ?)";
        PreparedStatement insPs = prepareStatement(isql);
        String ssql = "SELECT doubleVal FROM doubletab";
        PreparedStatement selPs = prepareStatement(ssql);
        String dsql = "DELETE FROM doubletab";
        PreparedStatement delPs = prepareStatement(dsql);
        for (int i = 0; i < bigDecimalVals.length; ++i)
        {
            BigDecimal bd = bigDecimalVals[i];
            insPs.setInt(1,i);
            insPs.setObject(2,bd,java.sql.Types.DOUBLE);
            assertUpdateCount(insPs, 1);
            // Check Value
            ResultSet rs = selPs.executeQuery();
            rs.next();
            assertEquals(bd.doubleValue(), rs.getDouble(1), 0.0);
            rs.close();
            // Clear out the table;
            assertUpdateCount(delPs, 1);
        }
        insPs.close();
        selPs.close();
        delPs.close();
        commit();
    }


    /**
     * Test BigDecimal with scale as parameter.
     */
    public void testBigDecimalSetObjectWithScale() throws Exception
    {
        getConnection().setAutoCommit(false);
        String sql = "CREATE TABLE numtab (num NUMERIC(10,6))";
        Statement stmt = createStatement();
        assertUpdateCount(stmt, 0, sql);
        stmt.close();
        commit();

        // make a big decimal from string
        BigDecimal bdFromString = new BigDecimal("2.33333333");

        sql = "INSERT INTO  numtab  VALUES(?)";
        PreparedStatement ps =  prepareStatement(sql);
        // setObject using the big decimal value
        int scale = 2;
        ps.setObject(1, bdFromString, java.sql.Types.DECIMAL, scale);
        assertUpdateCount(ps, 1);
        ps.close();
        // check the value
        sql = "SELECT num FROM numtab";
        stmt = createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        // Check that the correct scale was set
        BigDecimal expected
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            = bdFromString.setScale(scale, RoundingMode.DOWN);
        BigDecimal actual = (BigDecimal)rs.getObject(1);
        assertEquals("Actual value: " + actual
                     + "does not match expected value: " + expected,
                     expected.compareTo(actual), 0);
        rs.close();
        stmt.close();

        commit();
    }


    /**
     * Test execution of batch update where the type of
     * a parameter varies for difference entries in the batch.
     */
     public void testVaryingClientParameterTypeBatch() throws Exception
     {
         Statement stmt = createStatement();
         String createSql
             = "create table varcharclobtab (c1 varchar(100), c2 clob)";
         assertUpdateCount(stmt, 0, createSql);
         stmt.close();

         PreparedStatement pStmt
             = prepareStatement("insert into varcharclobtab VALUES(?,?)");

         pStmt.setNull(1, java.sql.Types.VARCHAR);
         pStmt.setString(2, "clob");
         pStmt.addBatch();

         pStmt.setString(1, "varchar");
         pStmt.setNull(2, java.sql.Types.CLOB);
         pStmt.addBatch();

         // The following statement should not throw an exception.
         pStmt.executeBatch();

         pStmt.close();
     }


    /**
     * Test small (close to 0) BigDecimal parameters.
     */
    public void testSmallBigDecimal() throws Exception
    {
        Statement stmt = createStatement();
        String createTableSQL
            = "create table Numeric_Tab (MAX_VAL NUMERIC(30,15), MIN_VAL "
            + "NUMERIC(30,15), NULL_VAL NUMERIC(30,15) DEFAULT NULL)";
        // to create the Numeric Table
        assertUpdateCount(stmt, 0, createTableSQL);

        String insertSQL
            = "insert into Numeric_Tab "
            + "values(999999999999999, 0.000000000000001, null)";
        assertUpdateCount(stmt, 1, insertSQL);

        //to extract the Maximum Value of BigDecimal to be Updated
        String sminBigDecimalVal = "0.000000000000001";
        BigDecimal minBigDecimalVal = new BigDecimal(sminBigDecimalVal);

        // to update Null value column with Minimum value
        String sPrepStmt = "update Numeric_Tab set NULL_VAL=?";

        // Uncomment and prepare the below statement instead to see JCC bug on
        // setObject for decimal
        // String sPrepStmt ="update Numeric_Tab set NULL_VAL="
        //                    + sminBigDecimalVal +" where 0.0 != ?";

        // get the PreparedStatement object
        PreparedStatement pstmt = prepareStatement(sPrepStmt);
        pstmt.setObject(1, minBigDecimalVal);
        pstmt.executeUpdate();
        pstmt.close();

        //to query from the database to check the call of pstmt.executeUpdate
        //to get the query string
        String Null_Val_Query = "Select NULL_VAL from Numeric_Tab";
        ResultSet rs = stmt.executeQuery(Null_Val_Query);
        rs.next();

        BigDecimal rBigDecimalVal = (BigDecimal )rs.getObject(1);
        assertEquals(rBigDecimalVal, minBigDecimalVal);
        rs.close();
        stmt.close();
    }


    /**
     * Test creation and execution of many Prepared Statements.
     * (Beetle 5130).
     */
    public void testManyPreparedStatements () throws Exception
    {
        int numOfPreparedStatement = 500;
        PreparedStatement[] tempPreparedStatement
            = new PreparedStatement[numOfPreparedStatement];

        for (int i = 0; i < numOfPreparedStatement; ++i) {
            tempPreparedStatement[i] = getConnection()
                .prepareStatement("SELECT COUNT(*) from SYS.SYSTABLES",
                                   ResultSet.TYPE_SCROLL_INSENSITIVE,
                                   ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = tempPreparedStatement[i].executeQuery();
            rs.close();
        }
        for (int i = 0; i < numOfPreparedStatement; ++i) {
            tempPreparedStatement[i].close();
        }
    }


    /**
     * Test invalid Timestamp parameters.
     */
    public void testInvalidTimestamp() throws Exception
    {
        Statement stmt = createStatement();
        assertUpdateCount(stmt, 0,
                          "CREATE TABLE TSTAB "
                          + "(I int, STATUS_TS  Timestamp, "
                          + " PROPERTY_TS Timestamp)" );
        assertUpdateCount(stmt, 1,
                          "INSERT INTO TSTAB "
                          + "VALUES(1 , '2003-08-15 21:20:00',"
                          + "       '2003-08-15 21:20:00')");
        assertUpdateCount(stmt, 1,
                          "INSERT INTO TSTAB "
                          + "VALUES(2 ,'1969-12-31 16:00:00.0',"
                          + "       '2003-08-15 21:20:00')");
        stmt.close();

        String timestamp = "20";
        String query =
            "select STATUS_TS from TSTAB "
            + "where  (STATUS_TS >= ? or PROPERTY_TS < ?)";

        PreparedStatement ps = prepareStatement(query);
        try {
            // Embedded will fail in setString
            // Client/server will fail in executeQuery
            ps.setString(1, timestamp);
            ps.setString(2, timestamp);
            ResultSet rs = ps.executeQuery();
            rs.close();
            fail("Exception expected above!");
        }
        catch (SQLException e) {
            assertSQLState("22007", e);
        }
        ps.close();
    }

    /**
     * <p>
     * Regression test for DERBY-614. The test consists of two parts:
     * </p>
     *
     * <p>
     * <b>Part 1:</b>
     * Test how the server responds when the client closes the statement in
     * between split QRYDTA blocks. We have to cause a split QRYDTA block,
     * which we can do by having a bunch of moderately-sized rows which mostly
     * fill a 32K block followed by a single giant row which overflows the
     * block. Then, we fetch some of the rows, then close the result set.
     * </p>
     *
     * <p>
     * <b>Part 2:</b>
     * Verifies that the server-side statement state is cleaned up when a
     * statement is re-used. Specifically, we set up a statement which has a
     * non-null splitQRYDTA value, then we close that statement and re-use it
     * for a totally unrelated query. If the splitQRYDTA wasn't cleaned up
     * properly, it comes flooding back as the response to that unrelated
     * query, causing a protocol parsing exception on the client.
     * </p>
     */
    public void testSplitQRYDTABlock() throws Exception
    {
        // Part 1:

        PreparedStatement ps
            = prepareStatement("create table jira614 (c1 varchar(10000))");
        assertUpdateCount(ps, 0);
        ps.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-5099
        String workString = Formatters.repeatChar("a", 150);
        ps = prepareStatement("insert into jira614 values (?)");
        ps.setString(1, workString);
        for (int row = 0; row < 210; ++row) ps.executeUpdate();

        workString = Formatters.repeatChar("b", 10000);
        ps.setString(1, workString);
        ps.executeUpdate();
        ps.close();

        ps = prepareStatement("select * from jira614");
        ResultSet rs = ps.executeQuery();

        int rowNum = 0;
        while (rs.next()) {
            if (++rowNum == 26) break;
        }
        rs.close(); // This statement actually triggers the bug.
        ps.close();

        // Part 2:

        // 1: set up a second table to use for an unrelated query:
        Statement stmt = createStatement();
        stmt.execute("create table jira614_a (c1 int)");

//IC see: https://issues.apache.org/jira/browse/DERBY-5099
        ps = prepareStatement("insert into jira614_a values (?)");
        for (int row = 1; row <= 5; ++row)
        {
            ps.setInt(1, row);
            ps.executeUpdate();
        }

        // 2: get the first statement into a splitQRYDTA state:
//IC see: https://issues.apache.org/jira/browse/DERBY-5099
        rs = stmt.executeQuery("select * from jira614");
        rowNum = 0;
        while (rs.next())
        {
            if (++rowNum == 26) break;
        }

        // 3: Now re-use the statement for some totally different purpose:
        stmt.close();
        stmt = createStatement();
        rs = stmt.executeQuery("select * from jira614_a");
        while (rs.next());
        ps.close();
        rs.close();
        stmt.close();
    }


    /**
     * Test how the server handles re-synchronization of
     * the data stream when an enormous parameter value follows a failed
     * prepare statement. Note that it is deliberate here that we are
     * preparing a statement referring to a non-existing table.
     * (This is a test case for Jira-170)
     */
    public void testExcpetionWithBigParameter() throws Exception
    {
        // Create a huge array of chars to be used as the input parameter
        char []cData = new char[1000000];
        for (int i = 0; i < cData.length; ++i) {
            cData[i] = Character.forDigit(i%10, 10);
        }

        // The behavior of this test program depends on how the JDBC driver
        // handles statement prepares. The DB2 Universal JDBC driver
        // implements something called "deferred prepares" by default. This
        // means that it doesn't do the prepare of the statement until the
        // statement is actually executed. Other drivers, such as the
        // standard Derby client driver, do the prepare at the time of the
        // prepare. This means that, depending on which driver we're using
        // and what the driver's configuration is, we'll get the "table not
        // found" error either on the prepare or on the execute. It doesn't
        // really matter for the purposes of the test, because the whole
        // point is that we *dont* get a DRDA Protocol Exception, but rather
        // a table-not-found exception.
        PreparedStatement ps = null ;
        try {
            ps = prepareStatement("insert into jira170 values (?)");
            ps.setString(1, new String(cData));
            ps.execute();
            ps.close();
            fail("No exception when executing a failed prepare with "
                 + "an enormous parameter");
        } catch (SQLException e) { // Should get "Table not Found"
            assertSQLState("42X05", e);
        }
    }

    /**
     * Test the proper use of continuation headers for very large reply
     * messages, such as the SQLDARD which is returned for a prepared
     * statement with an enormous number of parameter markers. This test
     * generates a multi-segment SQLDARD response message from the server, to
     * verify that the code in DDMWriter.finalizeDSSLength is executed.
     *
     * Repro for DERBY-125 off-by-one error.  This repro runs in
     * two iterations.  The first iteration, we use a table name
     * and a column name that are extra long, so that the server-
     * side buffer has more data in it.  The second iteration, we
     * use simpler names for the table and column, which take up
     * less space in the server buffer.  Then, since the server-
     * side bytes array was previously used for a larger amount of
     * data, then the unused bytes contain old data.  Since we
     * intentionally put the "larger amount of data" into the buffer
     * during the first iteration, we know what the old data bytes
     * are going to be.  Thus, by using specific lengths for the
     * table and column names, we can 'shift' the old data until we
     * reach a point where the off-by-one error manifests itself:
     * namely, we end up incorrectly leaving a non-zero data byte
     * in the last position of the current server buffer, which
     * is wrong.
     */
    public void testLargeReplies() throws Exception
    {
        jira125Test_a();
        jira125Test_b();
    }

    /**
     * First iteration of testLargeReplies test case.
     */
    private void jira125Test_a() throws Exception
    {
        // Build a column name that is 99 characters long;
        // the length of the column name and the length of
        // the table name are important to the repro--so
        // do not change these unless you can confirm that
        // the new values will behave in the same way.
        StringBuffer id = new StringBuffer();
        for (int i = 0; i < 49; ++i) id.append("id");
        id.append("i");

        // Build a table name that is 97 characters long;
        // the length of the column name and the length of
        // the table name are important to the repro--so
        // do not change these unless you can confirm that
        // the new values will behave in the same way.
        StringBuffer tabName = new StringBuffer("jira");
        for (int i = 0; i < 31; ++i) tabName.append("125");

        Statement stmt = createStatement();
        stmt.execute("create table " + tabName.toString() + " (" +
                     id.toString() + " integer)");
        stmt.execute("insert into " + tabName.toString() + " values 1, 2, 3");
        stmt.close();

        StringBuffer buf = new StringBuffer();
        buf.append("SELECT " + id.toString() + " FROM " +
                   tabName.toString() + " WHERE " + id.toString() + " IN ( ");

        // Must have at least 551 columns here, in order to force
        // server buffer beyond 32k.  NOTE: Changing this number
        // could cause the test to "pass" even if a regression
        // occurs--so only change it if needed!
        int nCols = 554;
        for (int i = 0; i < nCols; ++i) buf.append("?,");
        buf.append("?)");
        PreparedStatement ps = prepareStatement(buf.toString());
        // Note that we actually have nCols+1 parameter markers
        for (int i = 0; i <= nCols; i++) ps.setInt(i+1, 1);
        ResultSet rs = ps.executeQuery();
        while (rs.next());
        rs.close();
        ps.close();
    }

    /**
     * Second iteration of testLargeReplies test case.
     */
    private void jira125Test_b() throws Exception
    {
        Statement stmt = createStatement();
        stmt.execute("create table jira125 (id integer)");
        stmt.execute("insert into jira125 values 1, 2, 3");

        StringBuffer buf = new StringBuffer();
        buf.append("SELECT id FROM jira125 WHERE id IN ( ");

        // Must have at least 551 columns here, in order to force
        // server buffer beyond 32k.  NOTE: Changing this number
        // could cause the test to "pass" even if a regression
        // occurs--so only change it if needed!
        int nCols = 556;
        for (int i = 0; i < nCols; i++) buf.append("?,");
        buf.append("?)");
        PreparedStatement ps = prepareStatement(buf.toString());
        // Note that we actually have nCols+1 parameter markers
        for (int i = 0; i <= nCols; i++) ps.setInt(i+1, 1);
        ResultSet rs = ps.executeQuery();
        while (rs.next());
        rs.close();
        ps.close();
    }


    /**
     * This test case ensures that the bug introduced by the first patch for
     * Jira-815 has not been re-introduced.  The bug resulted in a hang if a
     * prepared statement was first executed with a lob value, and then
     * re-executed with a null-value in place of the lob.
     */
    public void testAlternatingLobValuesAndNull()  throws Exception
    {
        getConnection().setAutoCommit(false);
        Statement st = createStatement();
        st.execute("create table tt1 (CLICOL01 smallint not null)");
        st.execute("alter table tt1 add clicol02 smallint");
        st.execute("alter table tt1 add clicol03 int not null default 1");
        st.execute("alter table tt1 add clicol04 int");
        st.execute("alter table tt1 add clicol05 decimal(10,0) not null default 1");
        st.execute("alter table tt1 add clicol51 blob(1G)");
        st.execute("alter table tt1 add clicol52 blob(50)");
        st.execute("alter table tt1 add clicol53 clob(2G) not null default ''");
        st.execute("alter table tt1 add clicol54 clob(60)");
        commit();

        PreparedStatement pSt =
            prepareStatement("insert into tt1 values (?,?,?,?,?,?,?,?,?)");
        pSt.setShort(1, (short)500);
        pSt.setShort(2, (short)501);
        pSt.setInt(3, 496);
        pSt.setInt(4, 497);
        pSt.setDouble(5, 484);
        pSt.setBytes(6, "404 bit".getBytes());
        pSt.setBytes(7, "405 bit".getBytes());
        pSt.setString(8, "408 bit");
        pSt.setString(9, "409 bit");

        // Inserting first row
        assertUpdateCount(pSt, 1);

        pSt.setNull(2, java.sql.Types.SMALLINT);
        pSt.setNull(4, java.sql.Types.DOUBLE);
        pSt.setNull(7, java.sql.Types.BLOB);
        pSt.setNull(9, java.sql.Types.CLOB);

        // Inserting second row
        assertUpdateCount(pSt, 1);

        // Now inserting 3rd row, using lobs from 1st row
        ResultSet rs = st.executeQuery("select * from tt1");
        rs.next();
        pSt.setShort(1, rs.getShort(1));
        pSt.setShort(2, rs.getShort(2));
        pSt.setInt(3, rs.getInt(3));
        pSt.setInt(4, rs.getInt(4));
        pSt.setDouble(5, rs.getDouble(5));
        pSt.setBlob(6, rs.getBlob(6));
        pSt.setBlob(7, rs.getBlob(7));
        pSt.setClob(8, rs.getClob(8));
        pSt.setClob(9, rs.getClob(9));
        pSt.execute();

        // Now inserting 4th row, using lobs from 2nd row
        rs.next();
        pSt.setNull(2, java.sql.Types.SMALLINT);
        pSt.setNull(4, java.sql.Types.DOUBLE);
        pSt.setBlob(6, rs.getBlob(6));
        pSt.setNull(7, java.sql.Types.BLOB);
        pSt.setClob(8, rs.getClob(8));
        pSt.setNull(9, java.sql.Types.CLOB);
        pSt.execute();

        rs.close();
        pSt.close();

        commit();
    }


    /**
     * Test large batch sizes for Statement.addBatch and
     * Statement.executeBatch.  (This is a test for Jira 428.) Currently,
     * there is a hard DRDA limit of 65535 statements per batch (prior to
     * DERBY-428, the server failed at around 9000 statements). The different
     * JDBC clients support slightly lower limits: the Network Client supports
     * 65534 statements in a single batch, the DB2JCC driver v2.4 supports
     * 65532 statements, the DB2JCC driver v2.6 supports 32765 statements.
     * This test just verifies that a batch of 32765 statements works, and
     * that a batch of 100000 statements gets a BatchUpdateException from the
     * Network Client.
     */
    public void testLargeBatch() throws Exception
    {
        Statement stmt = createStatement();
        stmt.execute("create table jira428 (i integer)");
        getConnection().setAutoCommit(false);

        PreparedStatement ps
            = prepareStatement("insert into jira428 values (?)");
        for (int i = 0; i < 32765; ++i) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
        commit();

        // The below cannot be run as part of the test for the JCC client
        // because the exception forces the connection closed. For
        // DerbyNetClient, it's a clean exception that we can catch and
        // recover from, so we test that code path:
        ps = prepareStatement("insert into jira428 values (?)");
        for (int i = 0; i < 100000; ++i) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        try {
            ps.executeBatch();
            assertFalse("Expected exception when >65534 statements per batch",
                        usingDerbyNetClient());
        } catch (BatchUpdateException bue) {
            assertSQLState("XJ116", bue);
            assertFalse("Unexpected exception in embedded framework",
                        usingEmbedded());
        }
        ps.close();
        commit();
    }


    /**
     * Test for an off-by-one bug in the splitQRYDTA processing in the Network
     * Server writeQRYDTA code (Jira-1454), and is related to previous bugs
     * 614, 170, 491, and 492. The issue is that if the DSS block is exactly
     * the maximum DSS length (32767), then the writeQRYDTA code erroneously
     * thinks the DSS needs to be split when in fact it doesn't.
     *
     * The repro case sets up the boundary scenario; we run the case three
     * times, once with the value 1 less than the max DSS, once with the
     * value 1 greater than the max DSS, and once with the exact DSS length.
     * Only the third case triggers the JIRA-1454 bug; the other two tests
     * are for completeness.
     */
    public void testDSSLength() throws Exception
    {
        // Create table to be used in this test case
        Statement st = createStatement();
        st.execute(
            "create table jira1454(c1 varchar(20000), c2 varchar(30000))");
        st.close();

        tickleDSSLength(12748);
        tickleDSSLength(12750);
        tickleDSSLength(12749);
    }

    /**
     * Helper method for testDSSLength test case.  Inserts a record into a
     * table, and reads the content of the table before the content is
     * deleted.  (I.e., the table will empty when the method returns.  Use the
     * given length for the second parameter to the insert statement to test
     * different sizes for DSS blocks.
     *
     * @param c2Len Length to be used for the second
     */
    private void tickleDSSLength(int c2Len) throws Exception
    {
        char[] c1 = new char[20000];
        for (int i = 0; i < c1.length; ++i) {
            c1[i] = Character.forDigit(i%10, 10);
        }
        char[] c2 = new char[30000];
        for (int i = 0; i < c2Len; ++i) {
            c2[i] = Character.forDigit(i%10, 10);
        }

        PreparedStatement pSt =
            prepareStatement("insert into jira1454 values (?,?)");
        pSt.setString(1, new String(c1));
        pSt.setString(2, new String(c2, 0, c2Len));
        pSt.execute();
        pSt.close();

        Statement st = createStatement();
        ResultSet rs = st.executeQuery("select * from jira1454");
        while (rs.next()) {
            assertEquals(rs.getString("c2").length(), c2Len);
        }
        rs.close();

        // Clean up so table can be reused
        st.execute("delete from jira1454");
        st.close();
    }

    /**
     * A test case for DERBY-3046
     * We were running into null pointer exception if the parameter count
     * for PreparedStatement was 0 and the user tried doing setObject
     * 
     * @throws Exception
     */
    public void testVariationOfSetObject() throws Exception
    {
        Statement stmt = createStatement();
        String createString = "CREATE TABLE WISH_LIST  "
        	+  "(WISH_ID INT NOT NULL GENERATED ALWAYS AS IDENTITY " 
        	+  "   CONSTRAINT WISH_PK PRIMARY KEY, " 
        	+  " ENTRY_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        	+  " WISH_ITEM VARCHAR(32) NOT NULL) " ;
        
        stmt.executeUpdate(createString);
        PreparedStatement ps = prepareStatement("insert into WISH_LIST(WISH_ITEM) values (?)");
        //this won't raise any errors because there is one parameter in ps
        ps.setString(1, "aaa");
        ps.executeUpdate();
        
        //Negative test case. There are no parameter in the following ps
        ps = prepareStatement("insert into WISH_LIST(WISH_ITEM) values ('bb')");
        //Try setString when no parameters in ps
        try {
        	ps.setString(1, "aaa");
            fail("Exception expected above!");
        } catch (SQLException e)  {  
        	if (usingDerbyNetClient())
        		//note that SQLState is XCL14. For setObject below, the 
        		//SQLState is XCL13. I have entered DERBY-3139 for this
        		//difference in SQLState.
        		assertSQLState("XCL14", e);
        	else
        		assertSQLState("07009", e);
        }
        //Try setObject when no parameters in ps
        try {
        	ps.setObject(1,"cc",java.sql.Types.VARCHAR); 
            fail("Exception expected above!");
        } catch (SQLException e)  {   
    		assertSQLState("07009", e);
        }
    }

    /**
     * Test two different bugs regarding the handling of large
     * amounts of parameter data: first, the Network Server was incorrectly
     * handling the desegmentation of continued DSS segments, and second,
     * the Network Server was using the wrong heuristic to determine whether
     * long string data was being flowed in-line or externalized (Jira 1533).
     *
     * Tests "a" and "b" provoke two different forms of this problem, one
     * with just a single continued segment, and one with several continuations
     */
    public void testLargeParameters_a() throws Exception
    {
        Statement stmt = createStatement();
        stmt.execute("create table jira1533_a ("
                     + "aa BIGINT NOT NULL, "
                     + "bbbbbb BIGINT DEFAULT 0 NOT NULL,"
                     + "cccc  VARCHAR(40), ddddddddddd BIGINT, "
                     + "eeeeee VARCHAR(128), ffffffffffffffffff VARCHAR(128),"
                     + "ggggggggg  BLOB(2G), hhhhhhhhh VARCHAR(128), "
                     + "iiiiiiii VARCHAR(128), jjjjjjjjjjjjjj BIGINT,"
                     + "kkkkkkkk CHAR(1) DEFAULT 'f', "
                     + "llllllll CHAR(1) DEFAULT 'f', "
                     + "mmmmmmmmmmmmm  CHAR(1) DEFAULT 'f')");
         stmt.close();

         PreparedStatement ps = prepareStatement(
             "INSERT INTO jira1533_a (aa, bbbbbb, cccc, ddddddddddd, eeeeee,"
             + "                      ffffffffffffffffff,"
             + "                      ggggggggg, hhhhhhhhh, iiiiiiii, "
             + "                      jjjjjjjjjjjjjj, kkkkkkkk,"
             + "                      llllllll,mmmmmmmmmmmmm)"
             + "          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)");
         String blobStr = makeString(32584);
         ps.setLong(1, 5);
         ps.setLong(2, 1);
         ps.setString(3, "AAAAAAAAAAA");
         ps.setLong(4, 30000);
         ps.setString(5, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
         ps.setString(6, "AAAAAAAAAAA");
         ps.setBytes(7, blobStr.getBytes());
         ps.setString(8, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
         ps.setString(9, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
         ps.setLong(10, 1);
         ps.setString(11, "1");
         ps.setString(12, "1");
         ps.setString(13, "1");
         ps.execute();
         ps.close();
    }

    /**
     *  @see #testLargeParameters_a()
     */
    public void testLargeParameters_b() throws Exception
    {
        Statement stmt = createStatement();
        stmt.execute("create table jira1533_b ("
                     + "aa BIGINT NOT NULL, bbbbbb BIGINT DEFAULT 0 NOT NULL, "
                     + "cccc VARCHAR(40), ddddddddddd BIGINT, "
                     + "eeeeee VARCHAR(128), ffffffffffffffffff VARCHAR(128), "
                     + "g1 BLOB(2G), g2 BLOB(2G), g3 BLOB(2G), g4 BLOB(2G), "
                     + "ggggggggg  BLOB(2G), hhhhhhhhh VARCHAR(128), "
                     + "iiiiiiii VARCHAR(128), jjjjjjjjjjjjjj BIGINT,"
                     + "kkkkkkkk CHAR(1) DEFAULT 'f', "
                     + "llllllll CHAR(1) DEFAULT 'f', "
                     + "mmmmmmmmmmmmm  CHAR(1) DEFAULT 'f')");
        stmt.close();

        PreparedStatement ps = prepareStatement(
            "INSERT INTO jira1533_b (aa, bbbbbb, cccc, ddddddddddd, eeeeee,"
            + "                      ffffffffffffffffff,"
            + "                      g1, g2, g3, g4,"
            + "                      ggggggggg, hhhhhhhhh, iiiiiiii,"
            + "                      jjjjjjjjjjjjjj, kkkkkkkk,"
            + "                      llllllll,mmmmmmmmmmmmm)"
            + "          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        String blobStr = makeString(32584);
        ps.setLong(1, 5);
        ps.setLong(2, 1);
        ps.setString(3, "AAAAAAAAAAA");
        ps.setLong(4, 30000);
        ps.setString(5, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        ps.setString(6, "AAAAAAAAAAA");
        ps.setBytes(7, blobStr.getBytes());
        ps.setBytes(8, blobStr.getBytes());
        ps.setBytes(9, blobStr.getBytes());
        ps.setBytes(10 ,blobStr.getBytes());
        ps.setBytes(11 ,blobStr.getBytes());
        ps.setString(12, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        ps.setString(13, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        ps.setLong(14, 1);
        ps.setString(15, "1");
        ps.setString(16, "1");
        ps.setString(17, "1");
        ps.execute();
        ps.close();
    }

    /**
     * Test fix for protocol error if splitQRYDTA occurs during DRDAConnThread.doneData()
     * DERBY-3230
     * @throws SQLException
     */
    public void testDerby3230() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE TAB (col1 varchar(32672) NOT NULL)");
        PreparedStatement ps = prepareStatement("INSERT INTO TAB VALUES(?)");
        ps.setString(1,makeString(15000));
        ps.executeUpdate();
        ps.setString(1,makeString(7500));
        ps.executeUpdate();
        ps.setString(1,makeString(5000));
        ps.executeUpdate();
        ps.setString(1,makeString(2000));
        ps.executeUpdate();
        ps.setString(1,makeString(1600));
        ps.executeUpdate();
        ps.setString(1,makeString(800));
        ps.executeUpdate();
        ps.setString(1,makeString(400));
        ps.executeUpdate();
        ps.setString(1,makeString(200));
        ps.executeUpdate();
        ps.setString(1,makeString(100));
        ps.executeUpdate();
        ps.setString(1,makeString(56));
        ps.executeUpdate();
            
        ResultSet rs = s.executeQuery("SELECT * from tab");
        // drain the resultset
        JDBC.assertDrainResults(rs);
                   
    }
    /**
     * Return a string of the given length.  The string will contain just 'X'
     * characters.
     *
     * @param length Length of string to be returned.
     * @return String of given length.
     */
    private static String makeString(int length)
    {
        return makeString(length, 'X');
    }

    /**
     * Return a string of the given length filled with the specified character.
     */
    private static String makeString(int length, char ch) {
        char[] buf = new char[length];
        Arrays.fill(buf, ch);
        return new String(buf);
    }

    /**
     * Test fix for DERBY-4088 where an ArrayIndexOutOfBoundsException was
     * thrown by DDMReader.readBytes() when reading a BLOB value whose length
     * was close to the maximum length of a DSS.
     */
    public void testReadBlobCloseToMaxDssLength() throws Exception {
        final int length = 32766; // max DSS length is 32767

        // Create test data with the requested length
        DataInputStream stream1 =
                new DataInputStream(new LoopingAlphabetStream(length));
        byte[] bytes = new byte[length];
        stream1.readFully(bytes);

        // See if the test data can be sent to the server and back with
        // no errors.
        PreparedStatement ps = prepareStatement("values cast(? as blob)");
        ps.setBytes(1, bytes);
        ResultSet rs = ps.executeQuery();
        assertTrue("empty result set", rs.next());
        InputStream stream2 = rs.getBinaryStream(1);
        assertEquals(new LoopingAlphabetStream(length), stream2);
        assertFalse("too many rows", rs.next());
        rs.close();
    }

    /**
     * Verify that string values aren't truncated when their UTF-8 encoded
     * representation exceeds 32KB. DERBY-5236.
     */
    public void testLongColumn() throws Exception {
        PreparedStatement ps = prepareStatement(
                "values cast(? as varchar(32672))");

        String s1 = makeString(20000, '\u4e10');
        ps.setString(1, s1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), s1);

        // 64K-1 bytes, should be OK.
        String s2 =
                s1 + makeString(64 * 1024 - s1.getBytes("UTF-8").length - 1);
        ps.setString(1, s2);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), s2);

        // 64K bytes, will be truncated to 64K-1 by the client driver because
        // of limitation in the protocol.
        String s3 = s2 + 'X';
        ps.setString(1, s3);
        if (usingDerbyNetClient()) {
            String expected = s3.substring(0, s3.length() - 1);
            ResultSet rs = ps.executeQuery();
            assertTrue("Empty result", rs.next());
            assertDataTruncation(
                    new String[] { expected },
                    new String[] { rs.getString(1) },
                    1, false, true, s3.length(), expected.length(),
                    rs.getWarnings());
            assertFalse("Too many rows", rs.next());
            rs.close();
        } else {
            // Embedded is OK. No truncation.
            JDBC.assertSingleValueResultSet(ps.executeQuery(), s3);
        }

        // 64K+1 bytes, will be truncated by the client driver because of
        // limitation in the protocol. Should be truncated to to 64K-2 to
        // match the character boundary.
        String s4 = s3.substring(0, s3.length() - 2) + '\u4e10';
        ps.setString(1, s4);
        if (usingDerbyNetClient()) {
            String expected = s4.substring(0, s4.length() - 1);
            ResultSet rs = ps.executeQuery();
            assertTrue("Empty result", rs.next());
            assertDataTruncation(
                    new String[] { expected },
                    new String[] { rs.getString(1) },
                    1, false, true, s4.length(), expected.length(),
                    rs.getWarnings());
            assertFalse("Too many rows", rs.next());
            rs.close();
        } else {
            // Embedded is OK. No truncation.
            JDBC.assertSingleValueResultSet(ps.executeQuery(), s4);
        }

        // Try two columns at 64K+1 bytes. Expect same result as above.
        PreparedStatement ps2 = prepareStatement(
                "values (cast(? as varchar(32672)), " +
                "cast(? as varchar(32672)))");
        ps2.setString(1, s4);
        ps2.setString(2, s4);
        if (usingDerbyNetClient()) {
            String expected = s4.substring(0, s4.length() - 1);
            ResultSet rs = ps2.executeQuery();
            assertTrue("Empty result", rs.next());
            // We should actually have received two warnings here, but the
            // network client driver currently only supports one warning.
            assertDataTruncation(
                    new String[] { expected, expected },
                    new String[] { rs.getString(1), rs.getString(2) },
                    1, false, true, s4.length(), expected.length(),
                    rs.getWarnings());
            assertFalse("Too many rows", rs.next());
            rs.close();
        } else {
            String[][] expectedRow = {{s4, s4}};
            JDBC.assertFullResultSet(ps2.executeQuery(), expectedRow);
        }

        // Now test 64KB in a procedure call. Will be truncated to 64KB-1 on
        // the network client.
        Statement s = createStatement();
        s.execute("create procedure derby_5236_proc" +
                  "(in x varchar(32672), out y varchar(32672))" +
                  "language java parameter style java external name '" +
                  getClass().getName() + ".copyString'");
        CallableStatement cs = prepareCall("call derby_5236_proc(?,?)");
        cs.setString(1, s3);
        cs.registerOutParameter(2, Types.VARCHAR);
        cs.execute();
        if (usingDerbyNetClient()) {
            assertDataTruncation(
                    new String[] { s3.substring(0, s3.length() - 1) },
                    new String[] { cs.getString(2) },
                    2, true, true, s3.length(), s3.length() - 1,
                    cs.getWarnings());
        } else {
            assertEquals(s3, cs.getString(2));
        }
    }

    /**
     * Copy a string value from {@code in} to {@code out[0}}. Used as a
     * stored procedure in {@link #testLongColumn()}.
     *
     * @param in stored procedure input parameter
     * @param out stored procedure output parameter
     */
    public static void copyString(String in, String[] out) {
        out[0] = in;
    }

    /**
     * Assert that data returned from the server was truncated, and that the
     * proper warning came with the result.
     *
     * @param expectedRow the expected values
     * @param actualRow   the actual values returned
     * @param index       the expected column/parameter index in the warning
     * @param parameter   whether the values came from a procedure parameter
     * @param read        whether the values came from a read operation
     * @param dataSize    the expected full size of the truncated value
     * @param transferSize the expected size of the value after truncation
     * @param warning     the received warning
     */
    private static void assertDataTruncation(
            String[] expectedRow, String[] actualRow,
            int index, boolean parameter, boolean read,
            int dataSize, int transferSize, SQLWarning warning) {
        assertEquals("Wrong number of columns",
                     expectedRow.length, actualRow.length);
        assertNotNull("Expected data truncation warning", warning);
        for (int i = 0; i < expectedRow.length; i++) {
            assertEquals("column #" + (i + 1), expectedRow[i], actualRow[i]);

            if (warning instanceof DataTruncation) {
                DataTruncation dt = (DataTruncation) warning;
                assertEquals("index", index, dt.getIndex());
                assertEquals("parameter", parameter, dt.getParameter());
                assertEquals("read", read, dt.getRead());
                assertEquals("dataSize", dataSize, dt.getDataSize());
                assertEquals("transferSize", transferSize, dt.getTransferSize());
            } else {
                fail("Unexpected warning", warning);
            }

            assertNull("Chained warnings not expected on network client",
                       warning.getNextWarning());
        }
    }
}
