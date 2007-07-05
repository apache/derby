/*
 *
 * Derby - Classs org.apache.derbyTesting.functionTests.tests.jdbcapi.PrepStmtMetaDataTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.TestConfiguration;

public class PrepStmtMetaDataTest extends BaseJDBCTestCase {

    public PrepStmtMetaDataTest(String name) {
        super(name);
    }

    /**
     * Test getMetaData calls on prepared statements that do not return a
     * ResultSet
     */
    public void testNoResultSetMeta() throws SQLException {

        // test MetaData with statements that
        // do not return a ResultSet
        checkEmptyMetaData("create table ab(a int)", true);
        checkEmptyMetaData("alter table ab add column b int", true);

        Statement s = createStatement();
        s
                .execute("create procedure testproc() language java external name "
                        + "'org.apache.derbyTesting.functionTests.tests.jdbcapi.PrepStmtMetaDataTest.tstmeth'"
                        + " parameter style java");

        // test call statement - shouldn't have meta data
        checkEmptyMetaData("call testproc()", false);

        // test drop procedure - meta data should be null
        checkEmptyMetaData("drop procedure testproc", true);

        // test create schema - meta data should be null
        checkEmptyMetaData("create schema myschema", true);

        // test drop schema - meta data should be null
        checkEmptyMetaData("drop schema myschema restrict", true);

        s.execute("CREATE TABLE TRIGTAB (i int)");
        // test create trigger - meta data should be null
        checkEmptyMetaData(
                "create trigger mytrig after insert on ab for each row insert into trigtab values(1)",
                true);

        // test drop trigger - meta data should be null
        checkEmptyMetaData("drop trigger mytrig", true);

        // test create view - meta data should be null
        checkEmptyMetaData("create view myview as select * from ab", true);

        // test drop view - meta data should be null
        checkEmptyMetaData("drop view myview", true);

        // test drop table - meta data should be null
        checkEmptyMetaData("drop table ab", false);

        // test create index - meta data should be null
        checkEmptyMetaData("create index aindex on ab(a)", true);

        // test drop index - meta data should be null
        checkEmptyMetaData("drop index aindex", false);

        // test insert - meta data should be null
        checkEmptyMetaData("insert into ab values(1,1)", true);

        // test update - meta data should be null
        checkEmptyMetaData("update ab set a = 2", false);

        // test delete - meta data should be null
        checkEmptyMetaData("delete from ab", false);
        s.executeUpdate("drop table ab");
        s.close();
    }

    public void testAlterTableMeta() throws SQLException {

       Statement s = createStatement();
        s.executeUpdate("create table bug4579 (c11 int)");
        s.executeUpdate("insert into bug4579 values (1)");
        PreparedStatement ps = prepareStatement("select * from bug4579");
        ResultSetMetaData rsmd = ps.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(java.sql.Types.INTEGER, rsmd.getColumnType(1));
        assertEquals("C11", rsmd.getColumnName(1));

        // DERBY-2402 Client does not report added columns.
        // Take out check when DERBY-2402 is fixed
        if (usingDerbyNetClient())
            return;

        s.executeUpdate("alter table bug4579 add column c12 int");
        rsmd = ps.getMetaData();
        assertEquals(2, rsmd.getColumnCount());
        assertEquals(java.sql.Types.INTEGER, rsmd.getColumnType(1));
        assertEquals("C11", rsmd.getColumnName(1));
        assertEquals(java.sql.Types.INTEGER, rsmd.getColumnType(2));
        assertEquals("C12", rsmd.getColumnName(2));

        // ResultSetMetaData for select * after alter table and
        // executeQuery.
        s.executeUpdate("alter table bug4579 add column c13 int");
        ResultSet rs = ps.executeQuery();
        rsmd = ps.getMetaData();
        assertEquals(3, rsmd.getColumnCount());
        assertEquals(java.sql.Types.INTEGER, rsmd.getColumnType(1));
        assertEquals("C11", rsmd.getColumnName(1));
        assertEquals(java.sql.Types.INTEGER, rsmd.getColumnType(2));
        assertEquals("C12", rsmd.getColumnName(2));
        assertEquals(java.sql.Types.INTEGER, rsmd.getColumnType(3));
        assertEquals("C13", rsmd.getColumnName(3));
        rs.close();
        ps.close();
        s.executeUpdate("drop table bug4579");
        s.close();

    }

    
    /**
     * Check the metatdata for a prepared statement that does not return a
     * ResultSet is empty
     * 
     * @param sql
     *            sql for prepared statement
     * @param execute
     *            execute PreparedStatement if true
     * @throws SQLException
     */
    private void checkEmptyMetaData(String sql, boolean execute)
            throws SQLException {
        PreparedStatement ps = prepareStatement(sql);
        ResultSetMetaData rsmd = ps.getMetaData();
        assertEmptyResultSetMetaData(rsmd);
        if (execute)
            ps.executeUpdate();
        ps.close();
    }

    public void testAllDataTypesMetaData()  throws SQLException
    {
        Statement s = createStatement();
        SQLUtilities.createAndPopulateAllDataTypesTable(s);
        s.close();
        PreparedStatement ps = prepareStatement("SELECT * from AllDataTypesTable");
        ResultSetMetaData rsmd = ps.getMetaData();
        int colCount = rsmd.getColumnCount();
        assertEquals(17, colCount);
        // Column 1 SMALLINT
        assertEquals("",rsmd.getCatalogName(1));
        assertEquals("java.lang.Integer", rsmd.getColumnClassName(1));
        assertEquals(6, rsmd.getColumnDisplaySize(1));
        assertEquals("SMALLINTCOL", rsmd.getColumnLabel(1));
        assertEquals(java.sql.Types.SMALLINT,rsmd.getColumnType(1));
        assertEquals("SMALLINT", rsmd.getColumnTypeName(1));
        assertEquals(5,rsmd.getPrecision(1));
        assertEquals(0, rsmd.getScale(1));
        assertEquals("APP", rsmd.getSchemaName(1));
        assertEquals("ALLDATATYPESTABLE",rsmd.getTableName(1));
        assertFalse(rsmd.isAutoIncrement(1));
        assertFalse(rsmd.isCurrency(1));
        assertFalse(rsmd.isDefinitelyWritable(1));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(1));
        // DERBY-142 client returns incorrect value for isReadOnly
        if (usingEmbedded())
            assertFalse(rsmd.isReadOnly(1));
        assertTrue(rsmd.isSearchable(1));
        assertTrue(rsmd.isSigned(1));
        assertFalse(rsmd.isWritable(1));
        // COLUMN 2 INTEGER
        assertEquals("java.lang.Integer", rsmd.getColumnClassName(2));
        assertEquals(11, rsmd.getColumnDisplaySize(2));
        assertEquals("INTEGERCOL", rsmd.getColumnLabel(2));
        assertEquals(java.sql.Types.INTEGER,rsmd.getColumnType(2));
        assertEquals("INTEGER", rsmd.getColumnTypeName(2));
        assertEquals(10,rsmd.getPrecision(2));
        assertEquals(0, rsmd.getScale(2));       
        // COLUMN 3 BIGINT
        assertEquals("java.lang.Long", rsmd.getColumnClassName(3));
        assertEquals(20, rsmd.getColumnDisplaySize(3));
        assertEquals("BIGINTCOL", rsmd.getColumnLabel(3));
        assertEquals(java.sql.Types.BIGINT,rsmd.getColumnType(3));
        assertEquals("BIGINT", rsmd.getColumnTypeName(3));
        assertEquals(19,rsmd.getPrecision(3));
        assertEquals(0, rsmd.getScale(3));
        // COLUMN 4 DECIMAL
        assertEquals("java.math.BigDecimal", rsmd.getColumnClassName(4));
        assertEquals(12, rsmd.getColumnDisplaySize(4));
        assertEquals("DECIMALCOL", rsmd.getColumnLabel(4));
        assertEquals(java.sql.Types.DECIMAL,rsmd.getColumnType(4));
        assertEquals("DECIMAL", rsmd.getColumnTypeName(4));
        assertEquals(10,rsmd.getPrecision(4));
        assertEquals(5, rsmd.getScale(4));
        // COLUMN 5 REAL
        assertEquals("java.lang.Float", rsmd.getColumnClassName(5));
        assertEquals(13, rsmd.getColumnDisplaySize(5));
        assertEquals("REALCOL", rsmd.getColumnLabel(5));
        assertEquals(java.sql.Types.REAL,rsmd.getColumnType(5));
        assertEquals("REAL", rsmd.getColumnTypeName(5));
        assertEquals(7,rsmd.getPrecision(5));
        assertEquals(0, rsmd.getScale(5));
        
        // COLUMN 6 DOUBLE
        assertEquals("java.lang.Double", rsmd.getColumnClassName(6));
        assertEquals(22, rsmd.getColumnDisplaySize(6));
        assertEquals("DOUBLECOL", rsmd.getColumnLabel(6));
        assertEquals(java.sql.Types.DOUBLE,rsmd.getColumnType(6));
        assertEquals("DOUBLE", rsmd.getColumnTypeName(6));
        assertEquals(15,rsmd.getPrecision(6));
        assertEquals(0, rsmd.getScale(6));
        
        // COLUMN 7 CHAR (60)
        assertEquals("java.lang.String", rsmd.getColumnClassName(7));
        assertEquals(60, rsmd.getColumnDisplaySize(7));
        assertEquals("CHARCOL", rsmd.getColumnLabel(7));
        assertEquals(java.sql.Types.CHAR,rsmd.getColumnType(7));
        assertEquals("CHAR", rsmd.getColumnTypeName(7));
        assertEquals(60,rsmd.getPrecision(7));
        assertEquals(0, rsmd.getScale(7));
        
        // COLUMN 8 VARCHAR (60)
        assertEquals("java.lang.String", rsmd.getColumnClassName(8));
        assertEquals(60, rsmd.getColumnDisplaySize(8));
        assertEquals("VARCHARCOL", rsmd.getColumnLabel(8));
        assertEquals(java.sql.Types.VARCHAR,rsmd.getColumnType(8));
        assertEquals("VARCHAR", rsmd.getColumnTypeName(8));
        assertEquals(60,rsmd.getPrecision(8));
        assertEquals(0, rsmd.getScale(8));
        
        // COLUMN 9  LONG VARCHAR
        assertEquals("java.lang.String", rsmd.getColumnClassName(9));
        assertEquals(32700, rsmd.getColumnDisplaySize(9));
        assertEquals("LONGVARCHARCOL", rsmd.getColumnLabel(9));
        assertEquals(java.sql.Types.LONGVARCHAR,rsmd.getColumnType(9));
        assertEquals("LONG VARCHAR", rsmd.getColumnTypeName(9));
        assertEquals(32700,rsmd.getPrecision(9));
        assertEquals(0, rsmd.getScale(9));
        
        // COLUMN 10 CHAR FOR BIT DATA
        assertEquals("byte[]", rsmd.getColumnClassName(10));
        assertEquals(120, rsmd.getColumnDisplaySize(10));
        assertEquals("CHARFORBITCOL", rsmd.getColumnLabel(10));
        assertEquals(java.sql.Types.BINARY,rsmd.getColumnType(10));
        if (usingEmbedded())
            assertEquals("CHAR () FOR BIT DATA", rsmd.getColumnTypeName(10));
        else
            assertEquals("CHAR FOR BIT DATA", rsmd.getColumnTypeName(10));
        assertEquals(60,rsmd.getPrecision(10));
        assertEquals(0, rsmd.getScale(10));
        
        // COLUMN 11 VARCHAR FOR BIT DATA
        assertEquals("byte[]", rsmd.getColumnClassName(11));
        assertEquals(120, rsmd.getColumnDisplaySize(11));
        assertEquals("VARCHARFORBITCOL", rsmd.getColumnLabel(11));
        assertEquals(java.sql.Types.VARBINARY,rsmd.getColumnType(11));
        if (usingEmbedded())
            assertEquals("VARCHAR () FOR BIT DATA", rsmd.getColumnTypeName(11));
        else
            assertEquals("VARCHAR FOR BIT DATA", rsmd.getColumnTypeName(11));
        assertEquals(60,rsmd.getPrecision(11));
        assertEquals(0, rsmd.getScale(11));
    
        // COLUMN 12 LONG VARCHAR FOR BIT DATA
        assertEquals("byte[]", rsmd.getColumnClassName(12));
        assertEquals(65400, rsmd.getColumnDisplaySize(12));
        assertEquals("LVARCHARFORBITCOL", rsmd.getColumnLabel(12));
        assertEquals(java.sql.Types.LONGVARBINARY,rsmd.getColumnType(12));
        assertEquals("LONG VARCHAR FOR BIT DATA", rsmd.getColumnTypeName(12));
        assertEquals(32700,rsmd.getPrecision(12));
        assertEquals(0, rsmd.getScale(12));
        
        // COLUMN 13 CLOB
        assertEquals("java.sql.Clob", rsmd.getColumnClassName(13));
        assertEquals(1024, rsmd.getColumnDisplaySize(13));
        assertEquals("CLOBCOL", rsmd.getColumnLabel(13));
        assertEquals(java.sql.Types.CLOB,rsmd.getColumnType(13));
        assertEquals("CLOB", rsmd.getColumnTypeName(13));
        assertEquals(1024,rsmd.getPrecision(13));
        assertEquals(0, rsmd.getScale(13));
        
        // COLUMN 14 DATE
        assertEquals("java.sql.Date", rsmd.getColumnClassName(14));
        assertEquals(10, rsmd.getColumnDisplaySize(14));
        assertEquals("DATECOL", rsmd.getColumnLabel(14));
        assertEquals(java.sql.Types.DATE,rsmd.getColumnType(14));
        assertEquals("DATE", rsmd.getColumnTypeName(14));
        assertEquals(10,rsmd.getPrecision(14));
        assertEquals(0, rsmd.getScale(14));

        // COLUMN 15 TIME
        assertEquals("java.sql.Time", rsmd.getColumnClassName(15));
        assertEquals(8, rsmd.getColumnDisplaySize(15));
        assertEquals("TIMECOL", rsmd.getColumnLabel(15));
        assertEquals(java.sql.Types.TIME,rsmd.getColumnType(15));
        assertEquals("TIME", rsmd.getColumnTypeName(15));
        assertEquals(8,rsmd.getPrecision(15));
        assertEquals(0, rsmd.getScale(15));
        
        // COLUMN 16 TIMESTAMP
        assertEquals("java.sql.Timestamp", rsmd.getColumnClassName(16));
        assertEquals(26, rsmd.getColumnDisplaySize(16));
        assertEquals("TIMESTAMPCOL", rsmd.getColumnLabel(16));
        assertEquals(java.sql.Types.TIMESTAMP,rsmd.getColumnType(16));
        assertEquals("TIMESTAMP", rsmd.getColumnTypeName(16));
        assertEquals(26,rsmd.getPrecision(16));
        assertEquals(6, rsmd.getScale(16));

        // COLUMN 17 BLOB
        assertEquals("java.sql.Blob", rsmd.getColumnClassName(17));
        assertEquals(2048, rsmd.getColumnDisplaySize(17));
        assertEquals("BLOBCOL", rsmd.getColumnLabel(17));
        assertEquals(java.sql.Types.BLOB,rsmd.getColumnType(17));
        assertEquals("BLOB", rsmd.getColumnTypeName(17));
        assertEquals(1024,rsmd.getPrecision(17));
        assertEquals(0, rsmd.getScale(17));
        
        ps.close();
    
    }
    
    /**
     * Assert that ResultSetMetaData is null or empty
     * 
     * @param rsmd
     *            ResultSetMetaData to check
     * @throws SQLException
     */
    private void assertEmptyResultSetMetaData(ResultSetMetaData rsmd)
            throws SQLException {
        if (rsmd != null)
            assertEquals(0, rsmd.getColumnCount());

    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(PrepStmtMetaDataTest.class);
    }

    public static void tstmeth() {
        // for purpose of test, method may do nothing
    }

}
