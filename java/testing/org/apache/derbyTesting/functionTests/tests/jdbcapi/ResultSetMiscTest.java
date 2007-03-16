/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.ResultSetMiscTest

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.SecurityCheck;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.Utilities;

public class ResultSetMiscTest extends BaseJDBCTestCase {

    /**
     * Hang onto the SecurityCheck class while running the tests so that it is
     * not garbage collected during the test and lose the information it has
     * collected.
     */
    private final Object nogc = SecurityCheck.class;

    public ResultSetMiscTest(String name) {
        super(name);
    }

    /**
     * Test resultset metadata on columns of various types
     * 
     * @throws SQLException
     */
    public void testResultSetMetaData() throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs = stmt
                .executeQuery("select i, s, r, d, dt, t, ts, c, v, dc, bi, cbd, vbd, lvbd, cl, bl from t");
        ResultSetMetaData met = rs.getMetaData();
        rs = stmt
                .executeQuery("select i, s, r, d, dt, t, ts, c, v, dc, bi, cbd, vbd, lvbd, cl, bl from t");
        met = rs.getMetaData();

        int colCount = met.getColumnCount();
        assertEquals(16, colCount);

        // Column 1 INTEGER
        assertFalse(met.isAutoIncrement(1));
        assertFalse(met.isCaseSensitive(1));
        assertTrue(met.isSearchable(1));
        assertFalse(met.isCurrency(1));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(1));
        assertTrue(met.isSigned(1));
        assertEquals(11, met.getColumnDisplaySize(1));
        assertEquals("I", met.getColumnLabel(1));
        assertEquals("I", met.getColumnName(1));
        // beetle 5323
        assertEquals("T", met.getTableName(1));
        assertEquals("APP", met.getSchemaName(1));
        assertEquals("", met.getCatalogName(1));
        assertEquals(java.sql.Types.INTEGER, met.getColumnType(1));
        assertEquals(10, met.getPrecision(1));
        assertEquals(0, met.getScale(1));
        assertEquals("INTEGER", met.getColumnTypeName(1));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(1));
        assertFalse(met.isWritable(1));
        assertFalse(met.isDefinitelyWritable(1));

        // Column 2 SMALLINT
        assertFalse(met.isAutoIncrement(2));
        assertFalse(met.isCaseSensitive(2));
        assertTrue(met.isSearchable(2));
        assertFalse(met.isCurrency(2));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(2));
        assertTrue(met.isSigned(2));
        assertEquals(6, met.getColumnDisplaySize(2));
        assertEquals("S", met.getColumnLabel(2));
        assertEquals("S", met.getColumnName(2));
        // beetle 5323
        assertEquals("T", met.getTableName(2));
        assertEquals("APP", met.getSchemaName(2));
        assertEquals("", met.getCatalogName(2));
        assertEquals(java.sql.Types.SMALLINT, met.getColumnType(2));
        assertEquals(5, met.getPrecision(2));
        assertEquals(0, met.getScale(2));
        assertEquals("SMALLINT", met.getColumnTypeName(2));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(2));
        assertFalse(met.isWritable(2));
        assertFalse(met.isDefinitelyWritable(2));

        // Column 3 REAL
        assertFalse(met.isAutoIncrement(3));
        assertFalse(met.isCaseSensitive(3));
        assertTrue(met.isSearchable(3));
        assertFalse(met.isCurrency(3));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(3));
        assertTrue(met.isSigned(3));
        assertEquals(13, met.getColumnDisplaySize(3));
        assertEquals("R", met.getColumnLabel(3));
        assertEquals("R", met.getColumnName(3));
        // beetle 5323
        assertEquals("T", met.getTableName(3));
        assertEquals("APP", met.getSchemaName(3));
        assertEquals("", met.getCatalogName(3));
        assertEquals(java.sql.Types.REAL, met.getColumnType(3));
        assertEquals(7, met.getPrecision(3));
        assertEquals(0, met.getScale(3));
        assertEquals("REAL", met.getColumnTypeName(3));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(3));
        assertFalse(met.isWritable(3));
        assertFalse(met.isDefinitelyWritable(3));

        // Column 4 DOUBLE
        assertFalse(met.isAutoIncrement(4));
        assertFalse(met.isCaseSensitive(4));
        assertTrue(met.isSearchable(4));
        assertFalse(met.isCurrency(4));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(4));
        assertTrue(met.isSigned(4));
        assertEquals(22, met.getColumnDisplaySize(4));
        assertEquals("D", met.getColumnLabel(4));
        assertEquals("D", met.getColumnName(4));
        // beetle 5323
        assertEquals("T", met.getTableName(4));
        assertEquals("APP", met.getSchemaName(4));
        assertEquals("", met.getCatalogName(4));
        assertEquals(java.sql.Types.DOUBLE, met.getColumnType(4));
        assertEquals(15, met.getPrecision(4));
        assertEquals(0, met.getScale(4));
        assertEquals("DOUBLE", met.getColumnTypeName(4));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(4));
        assertFalse(met.isWritable(4));
        assertFalse(met.isDefinitelyWritable(4));

        // Column 5 DATE
        assertFalse(met.isAutoIncrement(5));
        assertFalse(met.isCaseSensitive(5));
        assertTrue(met.isSearchable(5));
        assertFalse(met.isCurrency(5));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(5));
        assertFalse(met.isSigned(5));
        assertEquals(10, met.getColumnDisplaySize(5));
        assertEquals("DT", met.getColumnLabel(5));
        assertEquals("DT", met.getColumnName(5));
        // beetle 5323
        assertEquals("T", met.getTableName(5));
        assertEquals("APP", met.getSchemaName(5));
        assertEquals("", met.getCatalogName(5));
        assertEquals(java.sql.Types.DATE, met.getColumnType(5));
        assertEquals(10, met.getPrecision(5));
        assertEquals(0, met.getScale(5));
        assertEquals("DATE", met.getColumnTypeName(5));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(1));
        assertFalse(met.isWritable(1));
        assertFalse(met.isDefinitelyWritable(1));

        // COLUMN 7 TIMESTAMP
        assertFalse(met.isAutoIncrement(7));
        assertFalse(met.isCaseSensitive(7));
        assertTrue(met.isSearchable(7));
        assertFalse(met.isCurrency(7));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(7));
        assertFalse(met.isSigned(7));
        assertEquals(26, met.getColumnDisplaySize(7));
        assertEquals("TS", met.getColumnLabel(7));
        assertEquals("TS", met.getColumnName(7));
        // beetle 5323
        assertEquals("T", met.getTableName(7));
        assertEquals("APP", met.getSchemaName(7));
        assertEquals("", met.getCatalogName(7));
        assertEquals(java.sql.Types.TIMESTAMP, met.getColumnType(7));
        assertEquals(26, met.getPrecision(7));
        assertEquals(6, met.getScale(7));
        assertEquals("TIMESTAMP", met.getColumnTypeName(7));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(7));
        assertFalse(met.isWritable(7));
        assertFalse(met.isDefinitelyWritable(7));

        // COLUMN 8 CHAR
        assertFalse(met.isAutoIncrement(8));
        assertTrue(met.isCaseSensitive(8));
        assertTrue(met.isSearchable(8));
        assertFalse(met.isCurrency(8));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(8));
        assertFalse(met.isSigned(8));
        assertEquals(10, met.getColumnDisplaySize(8));
        assertEquals("C", met.getColumnLabel(8));
        assertEquals("C", met.getColumnName(8));
        // beetle 5323
        assertEquals("T", met.getTableName(8));
        assertEquals("APP", met.getSchemaName(8));
        assertEquals("", met.getCatalogName(8));
        assertEquals(java.sql.Types.CHAR, met.getColumnType(8));
        assertEquals(10, met.getPrecision(8));
        assertEquals(0, met.getScale(8));
        assertEquals("CHAR", met.getColumnTypeName(8));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(8));
        assertFalse(met.isWritable(8));
        assertFalse(met.isDefinitelyWritable(8));

        // COLUMN 9 VARCHAR
        assertFalse(met.isAutoIncrement(9));
        assertTrue(met.isCaseSensitive(9));
        assertTrue(met.isSearchable(9));
        assertFalse(met.isCurrency(9));
        assertEquals(ResultSetMetaData.columnNoNulls, met.isNullable(9));
        assertFalse(met.isSigned(9));
        assertEquals(40, met.getColumnDisplaySize(9));
        assertEquals("V", met.getColumnLabel(9));
        assertEquals("V", met.getColumnName(9));
        // beetle 5323
        assertEquals("T", met.getTableName(9));
        assertEquals("APP", met.getSchemaName(9));
        assertEquals("", met.getCatalogName(9));
        assertEquals(java.sql.Types.VARCHAR, met.getColumnType(9));
        assertEquals(40, met.getPrecision(9));
        assertEquals(0, met.getScale(9));
        assertEquals("VARCHAR", met.getColumnTypeName(9));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(9));
        assertFalse(met.isWritable(9));
        assertFalse(met.isDefinitelyWritable(9));

        // COLUMN 10 DECIMAL
        assertFalse(met.isAutoIncrement(10));
        assertFalse(met.isCaseSensitive(10));
        assertTrue(met.isSearchable(10));
        // DERBY-2423 Embedded and client differ on isCurrency() for
        // DECIMAL and NUMERIC columns. Enable for embedded once the
        // issue is fixed
        if (usingDerbyNetClient())
            assertFalse(met.isCurrency(10));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(10));
        assertTrue(met.isSigned(10));
        assertEquals(12, met.getColumnDisplaySize(10));
        assertEquals("DC", met.getColumnLabel(10));
        assertEquals("DC", met.getColumnName(10));
        // beetle 5323
        assertEquals("T", met.getTableName(10));
        assertEquals("APP", met.getSchemaName(10));
        assertEquals("", met.getCatalogName(10));
        assertEquals(java.sql.Types.DECIMAL, met.getColumnType(10));
        assertEquals(10, met.getPrecision(10));
        assertEquals(2, met.getScale(10));
        assertEquals("DECIMAL", met.getColumnTypeName(10));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(10));
        assertFalse(met.isWritable(10));
        assertFalse(met.isDefinitelyWritable(10));

        // COLUMN 11 BIGINT
        assertFalse(met.isAutoIncrement(11));
        assertFalse(met.isCaseSensitive(11));
        assertTrue(met.isSearchable(11));
        assertFalse(met.isCurrency(11));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(11));
        assertTrue(met.isSigned(11));
        assertEquals(20, met.getColumnDisplaySize(11));
        assertEquals("BI", met.getColumnLabel(11));
        assertEquals("BI", met.getColumnName(11));
        // beetle 5323
        assertEquals("T", met.getTableName(11));
        assertEquals("APP", met.getSchemaName(11));
        assertEquals("", met.getCatalogName(11));
        assertEquals(java.sql.Types.BIGINT, met.getColumnType(11));
        assertEquals(19, met.getPrecision(11));
        assertEquals(0, met.getScale(11));
        assertEquals("BIGINT", met.getColumnTypeName(11));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(11));
        assertFalse(met.isWritable(11));
        assertFalse(met.isDefinitelyWritable(11));

        // COLUMN 12 CHAR FOR BIT DATA
        assertFalse(met.isAutoIncrement(12));
        assertFalse(met.isCaseSensitive(12));
        assertTrue(met.isSearchable(12));
        assertFalse(met.isCurrency(12));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(12));
        assertFalse(met.isSigned(12));
        assertEquals(20, met.getColumnDisplaySize(12));
        assertEquals("CBD", met.getColumnLabel(12));
        assertEquals("CBD", met.getColumnName(12));
        // beetle 5323
        assertEquals("T", met.getTableName(12));
        assertEquals("APP", met.getSchemaName(12));
        assertEquals("", met.getCatalogName(12));
        assertEquals(java.sql.Types.BINARY, met.getColumnType(12));
        assertEquals(10, met.getPrecision(12));
        assertEquals(0, met.getScale(12));
        // client and embedded differ in name, but stil a rose.
        if (usingEmbedded())
            assertEquals("CHAR () FOR BIT DATA", met.getColumnTypeName(12));
        else
            assertEquals("CHAR FOR BIT DATA", met.getColumnTypeName(12));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(12));
        assertFalse(met.isWritable(12));
        assertFalse(met.isDefinitelyWritable(12));

        // COLUMN 13 VARCHAR FOR BIT DATA
        assertFalse(met.isAutoIncrement(13));
        assertFalse(met.isCaseSensitive(13));
        assertTrue(met.isSearchable(13));
        assertFalse(met.isCurrency(13));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(13));
        assertFalse(met.isSigned(13));
        assertEquals(20, met.getColumnDisplaySize(13));
        assertEquals("VBD", met.getColumnLabel(13));
        assertEquals("VBD", met.getColumnName(13));
        // beetle 5323
        assertEquals("T", met.getTableName(13));
        assertEquals("APP", met.getSchemaName(13));
        assertEquals("", met.getCatalogName(13));
        assertEquals(java.sql.Types.VARBINARY, met.getColumnType(13));
        assertEquals(10, met.getPrecision(13));
        assertEquals(0, met.getScale(13));
        // client and embedded differ in name, but stil a rose.
        if (usingEmbedded())
            assertEquals("VARCHAR () FOR BIT DATA", met.getColumnTypeName(13));
        else
            assertEquals("VARCHAR FOR BIT DATA", met.getColumnTypeName(13));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(13));
        assertFalse(met.isWritable(13));
        assertFalse(met.isDefinitelyWritable(13));

        // COLUMN 14 LONGVARCHAR FOR BIT DATA
        assertFalse(met.isAutoIncrement(14));
        assertFalse(met.isCaseSensitive(14));
        assertTrue(met.isSearchable(14));
        assertFalse(met.isCurrency(14));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(14));
        assertFalse(met.isSigned(14));
        assertEquals(65400, met.getColumnDisplaySize(14));
        assertEquals("LVBD", met.getColumnLabel(14));
        assertEquals("LVBD", met.getColumnName(14));
        // beetle 5323
        assertEquals("T", met.getTableName(14));
        assertEquals("APP", met.getSchemaName(14));
        assertEquals("", met.getCatalogName(14));
        assertEquals(java.sql.Types.LONGVARBINARY, met.getColumnType(14));
        assertEquals(32700, met.getPrecision(14));
        assertEquals(0, met.getScale(14));
        assertEquals("LONG VARCHAR FOR BIT DATA", met.getColumnTypeName(14));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(14));
        assertFalse(met.isWritable(14));
        assertFalse(met.isDefinitelyWritable(14));

        // COLUMN 15 CLOB
        assertFalse(met.isAutoIncrement(15));
        assertTrue(met.isCaseSensitive(15));
        assertTrue(met.isSearchable(15));
        assertFalse(met.isCurrency(15));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(15));
        assertFalse(met.isSigned(15));
        assertEquals(2147483647, met.getColumnDisplaySize(15));
        assertEquals("CL", met.getColumnLabel(15));
        assertEquals("CL", met.getColumnName(15));
        // beetle 5323
        assertEquals("T", met.getTableName(15));
        assertEquals("APP", met.getSchemaName(15));
        assertEquals("", met.getCatalogName(15));
        assertEquals(java.sql.Types.CLOB, met.getColumnType(15));
        assertEquals(2147483647, met.getPrecision(15));
        assertEquals(0, met.getScale(15));
        assertEquals("CLOB", met.getColumnTypeName(15));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(15));
        assertFalse(met.isWritable(15));
        assertFalse(met.isDefinitelyWritable(15));

        // COLUMN 16 BLOB
        assertFalse(met.isAutoIncrement(16));
        assertFalse(met.isCaseSensitive(16));
        assertTrue(met.isSearchable(16));
        assertFalse(met.isCurrency(16));
        assertEquals(ResultSetMetaData.columnNullable, met.isNullable(16));
        assertFalse(met.isSigned(16));
        // DERBY-2425 Client returns negative value for getColumnDisplaySize()
        // enable for client once fixed.
        if (usingEmbedded())
            assertEquals(2147483647, met.getColumnDisplaySize(16));
        assertEquals("BL", met.getColumnLabel(16));
        assertEquals("BL", met.getColumnName(16));
        // beetle 5323
        assertEquals("T", met.getTableName(16));
        assertEquals("APP", met.getSchemaName(16));
        assertEquals("", met.getCatalogName(16));
        assertEquals(java.sql.Types.BLOB, met.getColumnType(16));
        assertEquals(1073741824, met.getPrecision(16));
        assertEquals(0, met.getScale(16));
        assertEquals("BLOB", met.getColumnTypeName(16));
        // DERBY-142 client incorrectly returns true. Enable on client
        // once DERBY-142 is fixed
        if (usingEmbedded())
            assertFalse(met.isReadOnly(16));
        assertFalse(met.isWritable(16));
        assertFalse(met.isDefinitelyWritable(16));
    }

    /**
     * Test fix for Bug4810 -Connection.commit() & rollback() do not
     * commit/rollback in auto-commit mode.
     */
    public void testBug4810() throws SQLException {
        Connection con = getConnection();

        CallableStatement cs = con
                .prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        cs.setString(1, "derby.locks.deadlockTimeout");
        cs.setString(2, "3");
        cs.execute();
        cs.setString(1, "derby.locks.waitTimeout");
        cs.setString(2, "3");
        cs.close();
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table bug4810(i int, b int)");
        stmt
                .executeUpdate("insert into bug4810 values (1,1), (1,2), (1,3), (1,4)");
        stmt
                .executeUpdate("insert into bug4810 values (1,1), (1,2), (1,3), (1,4)");
        con.commit();
        con.setAutoCommit(true);
        con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        // Just autocommit
        checkLocksForAutoCommitSelect(con, stmt, 0);
        // commit with autocommit
        checkLocksForAutoCommitSelect(con, stmt, 1);
        // rollback with autocommit
        checkLocksForAutoCommitSelect(con, stmt, 2);

        stmt.execute("drop table bug4810");
        con.commit();
        stmt.close();
    }

    /**
     * Setup up and run the auto-commit tests.
     * 
     * 
     *         
     * @throws SQLException
     */
    public void testAutoCommit() throws SQLException {
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("create table AutoCommitTable (num int)");

        s.executeUpdate("insert into AutoCommitTable values (1)");
        s.executeUpdate("insert into AutoCommitTable values (2)");
        int isolation = conn.getTransactionIsolation();
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        checkSingleRSAutoCommit(conn);
        checkSingleRSCloseCursorsAtCommit(conn);
        conn.setTransactionIsolation(isolation);
        s.executeUpdate("drop table AutoCommitTable");
        s.close();
    }

    // JIRA-1136: LossOfPrecisionConversionException fetching Float.MAX_VALUE.
    // This test proves that we can successfully fetch that value from the DB

    public void testCorrelationNamesAndMetaDataCalls() throws SQLException {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt
                .executeUpdate("create table s (a int, b int, c int, d int, e int, f int)");
        stmt.executeUpdate("insert into s values (0,1,2,3,4,5)");
        stmt.executeUpdate("insert into s values (10,11,12,13,14,15)");
        // System.out.println("Run select * from s ss (f, e, d, c, b, a) where f
        // = 0 and then try getTableName and getSchemaName on columns");
        ResultSet rs = stmt
                .executeQuery("select * from s ss (f, e, d, c, b, a) where f = 0");
        rs.next();
        ResultSetMetaData met = rs.getMetaData();
        assertEquals("S", met.getTableName(1));
        assertEquals("APP", met.getSchemaName(1));
        // System.out.println("Run select * from (select * from s) a and then
        // try getTableName and getSchemaName on columns");
        rs = stmt.executeQuery("select * from (select * from s) a");
        rs.next();
        met = rs.getMetaData();
        assertEquals("S", met.getTableName(1));
        assertEquals("APP", met.getSchemaName(1));
        stmt.executeUpdate("create schema s1");
        stmt.executeUpdate("create table s1.t1 (c11 int, c12 int)");
        stmt.executeUpdate("insert into s1.t1 values (11, 12), (21, 22)");
        // System.out.println("Run select * from s1.t1 as abc and then try
        // getTableName and getSchemaName on columns");
        rs = stmt.executeQuery("select * from s1.t1 as abc");
        met = rs.getMetaData();
        assertEquals("T1", met.getTableName(1));
        assertEquals("S1", met.getSchemaName(1));
        assertEquals("T1", met.getTableName(2));
        assertEquals("S1", met.getSchemaName(2));

        // System.out.println("Run select abc.c11 from s1.t1 as abc and then try
        // getTableName and getSchemaName on columns");
        rs = stmt.executeQuery("select abc.c11 from s1.t1 as abc");
        met = rs.getMetaData();
        assertEquals("T1", met.getTableName(1));
        assertEquals("S1", met.getSchemaName(1));
        // System.out.println("Run select bcd.a, abc.c11 from s1.t1 as abc, s as
        // bcd and then try getTableName and getSchemaName on columns");
        rs = stmt
                .executeQuery("select bcd.a, abc.c11 from s1.t1 as abc, s as bcd");
        met = rs.getMetaData();
        assertEquals("S", met.getTableName(1));
        assertEquals("APP", met.getSchemaName(1));
        assertEquals("T1", met.getTableName(2));
        assertEquals("S1", met.getSchemaName(2));

        stmt.executeUpdate("create schema app1");
        stmt.executeUpdate("create table app1.t1 (c11 int, c12 int)");
        stmt.executeUpdate("insert into app1.t1 values (11, 12), (21, 22)");
        stmt.executeUpdate("create schema app2");
        stmt.executeUpdate("create table app2.t1 (c11 int, c12 int)");
        stmt.executeUpdate("insert into app2.t1 values (11, 12), (21, 22)");
        // System.out.println("Run select app1.t1.c11, app2.t1.c11 from app1.t1,
        // app2.t1 and then try getTableName and getSchemaName on columns");
        rs = stmt
                .executeQuery("select app1.t1.c11, app2.t1.c11 from app1.t1, app2.t1");
        met = rs.getMetaData();
        assertEquals("T1", met.getTableName(1));
        assertEquals("APP1", met.getSchemaName(1));
        assertEquals("T1", met.getTableName(2));
        assertEquals("APP2", met.getSchemaName(2));

        stmt.execute("drop table s");
        stmt.execute("drop table s1.t1");
        stmt.execute("drop schema s1 restrict");
        stmt.execute("drop table app1.t1");
        stmt.execute("drop table app2.t1");
        stmt.execute("drop schema app2 restrict");
        stmt.execute("drop schema app1 restrict");
    }

    public void testFloatMAX_VALUE(Connection conn) throws SQLException {
        Statement stmt = createStatement();
        try {
            stmt.execute("drop table jira1136");
        } catch (Throwable t) {
        }
        stmt.execute("create table jira1136 (f float)");
        stmt.execute("insert into jira1136 values (3.4028235E38)");
        PreparedStatement ps = conn.prepareStatement("select * from jira1136");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            assertEquals(3.4028235E38, rs.getFloat(1), .00001);

        }
    }

    /**
     * Tests for two things:
     * 
     * 1) The ResultSet does not close implicitly when the ResultSet completes
     * and holdability == HOLD_CURSORS_OVER_COMMIT
     * 
     * 2) The ResultSet auto-commits when it completes and auto-commit is on.
     * 
     * @param conn
     *            The Connection
     * @throws SQLException
     */
    private void checkSingleRSAutoCommit(Connection conn) throws SQLException {
        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        // Single RS auto-commit test:
        Statement s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = s.executeQuery("select * from AutoCommitTable");
        // drain results but don't close
        while (rs.next())
            ;
        // test that resultset was not implicitly closed but autoCommit occured.
        assertFalse("Fail Auto-commit unsuccessful", locksHeld());

        assertFalse("Final call of rs.next() should return false", rs.next());
        rs.close();
        // check that next() on closed ResultSet throws an exception
        try {
            rs.next();
            fail("FAIL Error should have occured with rs.next() on a closed ResultSet");
        } catch (SQLException se) {
            assertEquals("XCL16", se.getSQLState());
        }
    }

    /**
     * Check to see that ResultSet closes implicitly when holdability is set to
     * CLOSE_CURORS_AT_COMMIT.
     * 
     * @param conn
     *            The Connection
     * @throws SQLException
     */
    private void checkSingleRSCloseCursorsAtCommit(Connection conn)
            throws SQLException {
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        Statement s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = s.executeQuery("select * from AutoCommitTable");
        // drain but do not close resultset.
        while (rs.next())
            ;
        assertFalse("Fail Auto-commit unsuccessful", locksHeld());
        try {
            rs.next();
            fail("FAIL. ResultSet not closed implicitly");
        } catch (SQLException e) {
            assertEquals("XCL16", e.getSQLState());

        }
    }

    /**
     * Check locks with various commit sequences.
     * 
     * @param conn
     *            Initialized connection
     * @param stmt
     *            stmt to use for select from table
     * @param action
     *            0 = autocommit only 1 = commit with ResultSet open 2 =
     *            rollback with ResultSet open
     * @throws SQLException
     */
    private void checkLocksForAutoCommitSelect(Connection conn, Statement stmt,
            int action) throws SQLException {

        ResultSet rs = stmt.executeQuery("select i,b from bug4810");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));

        if (action == 1) {
            conn.commit();
            assertFalse("Should not hold locks after commit", locksHeld());
        } else if (action == 2) {
            conn.rollback();
            assertFalse("Should not hold locks after rollback", locksHeld());
        } else if (action == 0) {
            // autocommit only
            assertTrue("Locks should be held with autocommit only", locksHeld());

            try {

                rs.next();
                assertEquals(1, rs.getInt(1));
                assertEquals(3, rs.getInt(2));

            } catch (SQLException sqle) {
                fail("Unexpected exception" + sqle.getSQLState() + ":" + sqle.getMessage());
            }

        }
        rs.close();
    }

    /**
     * 
     * 
     * @return true if locks are held.
     * @throws SQLException
     */
    private boolean locksHeld() throws SQLException {

        boolean hasLocks = false;
        Connection con2 = openDefaultConnection();
        PreparedStatement ps2 = con2
                .prepareStatement("select XID, count(*) from SYSCS_DIAG.LOCK_TABLE as L group by XID");
        ResultSet rs2 = ps2.executeQuery();

        while (rs2.next()) {
            if (rs2.getInt(2) > 0) {
                hasLocks = true;
            } else {
                // 0 locks held
                hasLocks = false;
            }
        }

        rs2.close();
        ps2.close();
        con2.close();
        return hasLocks;
    }

    /**
     * Runs the test fixtures in embedded and client.
     * 
     * @return test suite
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("ResultSetTest2");

        suite.addTest(baseSuite("ResultSetTest2:embedded"));

        suite.addTest(TestConfiguration
                .clientServerDecorator(baseSuite("ResultSetTest2:client")));
        return suite;
    }

    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(ResultSetMiscTest.class);

        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the table used in the test cases.
             * 
             */
            protected void decorateSQL(Statement s) throws SQLException {

                s
                        .execute("create table t (i int, s smallint, r real, "
                                + "d double precision, dt date, t time, ts timestamp, "
                                + "c char(10), v varchar(40) not null, dc dec(10,2),"
                                + "bi bigint, cbd char(10) for bit data,"
                                + "vbd varchar(10) for bit data,lvbd long varchar for bit data,"
                                + "cl clob(2G), bl blob(1G) )");
                s
                        .execute("insert into t values(1,2,3.3,4.4,date('1990-05-05'),"
                                + "time('12:06:06'),timestamp('1990-07-07 07:07:07.000007'),"
                                + "'eight','nine', 10.1, 11,"
                                + Utilities.stringToHexLiteral("twelv")
                                + ","
                                + Utilities.stringToHexLiteral("3teen")
                                + ","
                                + Utilities.stringToHexLiteral("4teen")
                                + ", null, null)");

            }
        };
    }

}
