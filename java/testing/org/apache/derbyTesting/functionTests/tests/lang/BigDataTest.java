/**
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.BigDataTest
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * A test case for big.sql.
 */
public class BigDataTest extends BaseJDBCTestCase {
    private static final String BIG_TABLE_NAME = "big";

    /**
     * Constructor.
     * 
     * @param name
     * @throws SQLException
     */
    public BigDataTest(String name) throws SQLException {
        super(name);
    }

    /**
     * Get a String to select all records from the table defined.
     * 
     * @param tableName
     *            The table to fetch records from.
     * @return "select * from " + tableName.
     */
    public static String getSelectSql(String tableName) {
        return "select * from " + tableName;
    }

    /**
     * Create table by the name defined, and each column of the table has the type of
     * varchar or clob, which is determined by the element in useClob. At the same time,
     * each column is as big as identified by lengths. i.e. to call createTable(conn,
     * "big", {1000, 1000, }, {true, false,}) equals calling the sql sentence "create
     * table big(c1 clob(1000), c2 varchar(1000));"
     * 
     * @param tableName
     * @param lengths
     * @param useClob
     *            for element of useClob, if true, use clob as an element for a column;
     *            false, use varchar as an element for a column.
     * @throws SQLException
     */
    private void createTable(String tableName, int[] lengths, boolean[] useClob)
            throws SQLException {
        StringBuffer sqlSb = new StringBuffer();
        sqlSb.append("create table ");
        sqlSb.append(tableName);
        sqlSb.append(" (");
        for (int i = 0; i < lengths.length - 1; i++) {
            sqlSb.append("c" + (i + 1) + (useClob[i] ? " clob(" : " varchar(")
                    + lengths[i] + "),");
        }
        sqlSb.append("c" + lengths.length
                + (useClob[lengths.length - 1] ? " clob(" : " varchar(")
                + lengths[lengths.length - 1] + ")");
        sqlSb.append(")");
        String sql = sqlSb.toString();

        createTable(sql);
    }

    /**
     * Create a new table with defined sql sentence.
     * 
     * @param sql
     *            a sql sentence a create a table, which should use BIG_TABLE_NAME as new
     *            table's name.
     * @throws SQLException
     */
    private void createTable(String sql) throws SQLException {
        Statement ps = createStatement();
        ps.executeUpdate(sql);
        ps.close();
    }

    /**
     * Generate String array according to String array and int array defined. i.e. calling
     * getStringArray({"a", "b",}, {3, 4}) returns {"aaa", "bbbb",}.
     * 
     * @param sa
     *            the sort string array to use.
     * @param timesArray
     *            stores repeated times of String constructed by elements in sa.
     * @return A String array, whose elements is constructed by elements in sa, and each
     *         element repeated times defined by ia.
     */
    private String[] getStringArray(String[] sa, int[] timesArray) {
        String[] result = new String[sa.length];
        for (int i = 0; i < sa.length; i++) {
            result[i] = new String(Formatters.repeatChar(sa[i], timesArray[i]));
        }

        return result;
    }

    /**
     * Generate String array with two dimensions according to String array and int array
     * defined. i.e. calling getStringArray({"a", "b",}, {3, 4}) returns {"aaa", "bbbb",}.
     * 
     * @param sa
     *            the sort string array to use.
     * @param timesArray
     *            stores repeated times of String constructed by elements in sa.
     * @return A String array with two dimensions, whose elements is constructed by
     *         elements in sa, and each element repeated times defined by ia, for each
     *         row, it has only one column.
     */
    private String[][] getRowsWithOnlyOneColumn(String[] sa, int[] timesArray) {
        String[][] result = new String[sa.length][1];
        for (int i = 0; i < sa.length; i++) {
            result[i][0] = new String(Formatters.repeatChar(sa[i], timesArray[i]));
        }

        return result;
    }

    /**
     * Insert one row into a table named by tableName, with defined table, and String
     * array and int array to construct params.
     * 
     * @param tableName
     *            can not be null.
     * @param sa
     *            the string array to use.
     * @param timesArray
     *            stores repeated times of String constructed by elements in sa.
     * @throws SQLException
     */
    private void insertOneRow(String tableName, String[] sa, int[] timesArray)
            throws SQLException {
        String[] params = getStringArray(sa, timesArray);
        insertOneRow(tableName, params);
    }

    /**
     * Insert a row into a table named by tableName, with defined table and params.
     * 
     * @param tableName
     *            can not be null.
     * @param columns
     *            can not be null, and has a length bigger than 0.
     * @throws SQLException
     *             if SQLException occurs.
     */
    private void insertOneRow(String tableName, String[] columns) throws SQLException {
        StringBuffer sqlSb = new StringBuffer();
        sqlSb.append("insert into ");
        sqlSb.append(tableName);
        sqlSb.append(" values (");
        for (int i = 0; i < columns.length - 1; i++)
            sqlSb.append("?, ");
        sqlSb.append("?)");
        String sql = sqlSb.toString();

        PreparedStatement ps = prepareStatement(sql);
        for (int i = 1; i <= columns.length; i++)
            ps.setString(i, columns[i - 1]);
        ps.executeUpdate();
        ps.close();
    }

    /**
     * Insert multiple rows into one table.
     * 
     * @param tableName
     *            the table will receive new rows.
     * @param rows
     *            new rows for the table. Each row has only one column.
     * @throws SQLException
     */
    private void insertMultipleRows(String tableName, String[][] rows)
            throws SQLException {
        for (int i = 0; i < rows.length; i++) {
            String[] row = rows[i];
            insertOneRow(tableName, row);
        }
    }

    /**
     * Valid content in defined table.
     * 
     * @param expected
     *            the values expected, it has the same order with the table.
     *            i.e.expected[0] means the expected values for the first row in rs.
     * @param tableName
     *            whose content will be compared.
     * @throws SQLException
     *             means invalid.
     */
    private void validTable(String[][] expected, String tableName) throws SQLException {
        String sql = getSelectSql(tableName);
        Statement st = createStatement();
        ResultSet rs = st.executeQuery(sql);
        JDBC.assertFullResultSet(rs, expected);
        st.close();
    }

    /**
     * Valid the current row record of passed ResultSet.
     * 
     * @param exected
     *            the values expected, it has the same order with the table.
     * @param useClob
     *            for each element of useColb, true means the column is Clob, false means
     *            varchar.
     * @param rs
     *            whose current row will be compared.
     * @throws SQLException
     *             means invalid.
     */
    private void validSingleRow(String[] exected, boolean[] useClob, ResultSet rs)
            throws SQLException {
        for (int i = 0; i < exected.length; i++) {
            String real;
            if (useClob[i]) {
                Clob c = rs.getClob(i + 1);
                real = c.getSubString(1, (int) c.length());
            } else {
                real = rs.getString(i + 1);
            }

            assertEquals("Compare column " + (i + 1), exected[i], real);
        }
    }

    public void tearDown() throws SQLException {
        dropTable(BIG_TABLE_NAME);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("BigDataTest");
        suite.addTest(TestConfiguration.defaultSuite(BigDataTest.class));
        return suite;
    }

    /**
     * Mix clob and varchar in the table. The commented part in big.sql has been revived
     * without DRDAProtocolException thrown.
     * 
     * @throws SQLException
     */
    public void testMixture() throws SQLException {
        int[] ia = { 32672, 32672, 32672, 32672, };
        boolean[] useClob = { true, false, false, true, };
        createTable(BIG_TABLE_NAME, ia, useClob);

        String[] sa = { "a", "b", "c", "d", };
        insertOneRow(BIG_TABLE_NAME, sa, ia);

        String[] row = getStringArray(sa, ia);
        String[][] expected = { row, };
        validTable(expected, BIG_TABLE_NAME);

        insertOneRow(BIG_TABLE_NAME, sa, ia);
        insertOneRow(BIG_TABLE_NAME, sa, ia);

        expected = new String[][] { row, row, row, };
        validTable(expected, BIG_TABLE_NAME);

        String sql1 = getSelectSql(BIG_TABLE_NAME);
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = st.executeQuery(sql1);
        assertEquals("Before operation, row No. is 0.", 0, rs.getRow());
        rs.first();
        assertEquals("After calling first(), row No. is 1.", 1, rs.getRow());
        validSingleRow(row, useClob, rs);
        rs.next();
        assertEquals("After calling next(), row No. is 2.", 2, rs.getRow());
        validSingleRow(row, useClob, rs);
        rs.previous();
        assertEquals("After calling previous(), row No. is 1.", 1, rs.getRow());
        validSingleRow(row, useClob, rs);
        rs.last();
        assertEquals("After calling last(), row No. is 3.", 3, rs.getRow());
        validSingleRow(row, useClob, rs);
        rs.close();

        st.close();
    }

    /**
     * let's try scrolling.
     * 
     * @throws SQLException
     */
    public void testScrolling() throws SQLException {
        int[] lens = { 10000, 10000, 10000, 10000, };
        boolean[] useClob = { false, false, false, false, };
        createTable(BIG_TABLE_NAME, lens, useClob);

        String[] sa1 = { "a", "b", "c", "d", };
        insertOneRow(BIG_TABLE_NAME, sa1, lens);
        String[] sa2 = new String[] { "e", "f", "g", "h", };
        insertOneRow(BIG_TABLE_NAME, sa2, lens);
        String[] sa3 = new String[] { "i", "j", "k", "l", };
        insertOneRow(BIG_TABLE_NAME, sa3, lens);
        String[] sa4 = new String[] { "m", "n", "o", "p", };
        insertOneRow(BIG_TABLE_NAME, sa4, lens);

        String[] row1 = getStringArray(sa1, lens);
        String[] row2 = getStringArray(sa2, lens);
        String[] row3 = getStringArray(sa3, lens);
        String[] row4 = getStringArray(sa4, lens);
        String[][] expected = { row1, row2, row3, row4, };
        validTable(expected, BIG_TABLE_NAME);

        String sql = getSelectSql(BIG_TABLE_NAME);
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = st.executeQuery(sql);

        rs.first();
        validSingleRow(row1, useClob, rs);
        rs.next();
        validSingleRow(row2, useClob, rs);
        rs.previous();
        validSingleRow(row1, useClob, rs);
        rs.last();
        validSingleRow(row4, useClob, rs);
        rs.close();

        rs = st.executeQuery(sql);
        rs.last();
        validSingleRow(row4, useClob, rs);
        rs.close();

        st.close();
    }

    /**
     * try a column which is > 32767
     * 
     * @throws SQLException
     */
    public void testBigColumn() throws SQLException {
        int[] ia = { 40000, };
        boolean[] useClob = { true, };
        createTable(BIG_TABLE_NAME, ia, useClob);

        String[] sa = { "a", };
        insertOneRow(BIG_TABLE_NAME, sa, ia);

        String[][] expected = { getStringArray(sa, ia), };
        validTable(expected, BIG_TABLE_NAME);
    }

    /**
     * try several columns > 32767.
     * 
     * @throws SQLException
     */
    public void testSeveralBigColumns() throws SQLException {
        int[] ia = { 40000, 40000, 40000, };
        boolean[] useClob = { true, true, true, };
        createTable(BIG_TABLE_NAME, ia, useClob);

        String[] sa = { "a", "b", "c", };
        insertOneRow(BIG_TABLE_NAME, sa, ia);

        String[] sa1 = new String[] { "d", "e", "f", };
        insertOneRow(BIG_TABLE_NAME, sa1, ia);

        String[][] expected = { getStringArray(sa, ia), getStringArray(sa1, ia), };
        validTable(expected, BIG_TABLE_NAME);
    }

    /**
     * create table with row greater than 32K.
     * 
     * @throws SQLException
     */
    public void testBigRow() throws SQLException {
        int[] ia = { 10000, 10000, 10000, 10000, };
        boolean[] useClob = { false, false, false, false, };
        createTable(BIG_TABLE_NAME, ia, useClob);

        String[] sa = { "a", "b", "c", "d", };
        insertOneRow(BIG_TABLE_NAME, sa, ia);

        String[][] expected = { getStringArray(sa, ia), };
        validTable(expected, BIG_TABLE_NAME);

        String[] sa1 = new String[] { "e", "f", "g", "h", };
        insertOneRow(BIG_TABLE_NAME, sa1, ia);

        expected = new String[][] { expected[0], getStringArray(sa1, ia), };
        validTable(expected, BIG_TABLE_NAME);
    }

    /**
     * the overhead for DSS on QRYDTA is 15 bytes let's try a row which is exactly 32767
     * (default client query block size).
     * 
     * @throws SQLException
     */
    public void testDefaultQueryBlock() throws SQLException {
        int[] lens = { 30000, 2752, };
        boolean[] useClob = { false, false, };
        createTable(BIG_TABLE_NAME, lens, useClob);

        String[] sa = { "a", "b", };
        insertOneRow(BIG_TABLE_NAME, sa, lens);

        String[][] expected = { getStringArray(sa, lens), };
        validTable(expected, BIG_TABLE_NAME);
    }

    /**
     * Various tests for JIRA-614: handling of rows which span QRYDTA blocks. What happens
     * when the SplitQRYDTA has to span 3+ blocks.
     * 
     * @throws SQLException
     */
    public void testSpanQRYDTABlocks() throws SQLException {
        int[] lens = { 32672, 32672, 32672, 32672, };
        boolean[] useClob = { false, false, false, false, };
        createTable(BIG_TABLE_NAME, lens, useClob);

        String[] sa = { "a", "b", "c", "d", };
        insertOneRow(BIG_TABLE_NAME, sa, lens);

        String[] row = getStringArray(sa, lens);
        String[][] expected = { row, };
        validTable(expected, BIG_TABLE_NAME);

        insertOneRow(BIG_TABLE_NAME, sa, lens);
        insertOneRow(BIG_TABLE_NAME, sa, lens);

        expected = new String[][] { row, row, row, };
        validTable(expected, BIG_TABLE_NAME);

        String sql1 = getSelectSql(BIG_TABLE_NAME);
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = st.executeQuery(sql1);
        assertEquals("Before operation, row No. is 0.", 0, rs.getRow());
        rs.first();
        assertEquals("After calling first(), row No. is 1.", 1, rs.getRow());
        validSingleRow(row, useClob, rs);
        rs.next();
        assertEquals("After calling next(), row No. is 2.", 2, rs.getRow());
        validSingleRow(row, useClob, rs);
        rs.previous();
        assertEquals("After calling previous(), row No. is 1.", 1, rs.getRow());
        validSingleRow(row, useClob, rs);
        rs.last();
        assertEquals("After calling last(), row No. is 3.", 3, rs.getRow());
        validSingleRow(row, useClob, rs);
        rs.close();
        st.close();
    }

    /**
     * What happens when the row + the ending SQLCARD is too big.
     * 
     * @throws SQLException
     */
    public void testTooBigSQLCARD() throws SQLException {
        int[] lens = { 30000, 2750, };
        boolean[] useClob = { false, false, };
        createTable(BIG_TABLE_NAME, lens, useClob);

        String[] sa = { "a", "b", };
        insertOneRow(BIG_TABLE_NAME, sa, lens);

        String[][] expected = { getStringArray(sa, lens), };
        validTable(expected, BIG_TABLE_NAME);
    }

    /**
     * Test a table just has only one column typed long varchar. This is a Test case
     * commented in big.sql, but revived partly now. When inserting a big row, a
     * SQLException is thrown with the prompt that 33000 is a invalid length.
     * 
     * @throws SQLException
     */
    public void testLongVarchar() throws SQLException {
        String sql = "create table " + BIG_TABLE_NAME + "(lvc long varchar )";
        createTable(sql);

        String[] sa = { "a", "a", "a", "a", "a", };
        int[] timesArray = { 1000, 2000, 3000, 32000, 32700, };
        String[][] rows = getRowsWithOnlyOneColumn(sa, timesArray);

        insertMultipleRows(BIG_TABLE_NAME, rows);
        validTable(rows, BIG_TABLE_NAME);
    }

    /**
     * Test a table just has only one column typed varchar. This is a Test case commented
     * in big.sql, but revived partly now.
     * 
     * @throws SQLException
     */
    public void testVarchar() throws SQLException {
        String sql = "create table " + BIG_TABLE_NAME + "(vc varchar(32672))";
        createTable(sql);

        String[] sa = { "a", "a", "a", "a", "a", };
        int[] timesArray = { 1000, 2000, 3000, 32000, 32672, };
        String[][] rows = getRowsWithOnlyOneColumn(sa, timesArray);

        insertMultipleRows(BIG_TABLE_NAME, rows);
        validTable(rows, BIG_TABLE_NAME);
    }
}
