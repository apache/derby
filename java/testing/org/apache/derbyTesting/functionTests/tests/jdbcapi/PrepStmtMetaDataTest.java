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
        Connection conn = getConnection();
        conn.setAutoCommit(true);

        // test MetaData with statements that
        // do not return a ResultSet
        checkEmptyMetaData(conn, "create table ab(a int)", true);
        checkEmptyMetaData(conn, "alter table ab add column b int", true);

        Statement s = conn.createStatement();
        s
                .execute("create procedure testproc() language java external name "
                        + "'org.apache.derbyTesting.functionTests.tests.jdbcapi.PrepStmtMetaDataTest.tstmeth'"
                        + " parameter style java");

        // test call statement - shouldn't have meta data
        checkEmptyMetaData(conn, "call testproc()", false);

        // test drop procedure - meta data should be null
        checkEmptyMetaData(conn, "drop procedure testproc", true);

        // test create schema - meta data should be null
        checkEmptyMetaData(conn, "create schema myschema", true);

        // test drop schema - meta data should be null
        checkEmptyMetaData(conn, "drop schema myschema restrict", true);

        s.execute("CREATE TABLE TRIGTAB (i int)");
        // test create trigger - meta data should be null
        checkEmptyMetaData(
                conn,
                "create trigger mytrig after insert on ab for each row insert into trigtab values(1)",
                true);

        // test drop trigger - meta data should be null
        checkEmptyMetaData(conn, "drop trigger mytrig", true);

        // test create view - meta data should be null
        checkEmptyMetaData(conn, "create view myview as select * from ab", true);

        // test drop view - meta data should be null
        checkEmptyMetaData(conn, "drop view myview", true);

        // test drop table - meta data should be null
        checkEmptyMetaData(conn, "drop table ab", false);

        // test create index - meta data should be null
        checkEmptyMetaData(conn, "create index aindex on ab(a)", true);

        // test drop index - meta data should be null
        checkEmptyMetaData(conn, "drop index aindex", false);

        // test insert - meta data should be null
        checkEmptyMetaData(conn, "insert into ab values(1,1)", true);

        // test update - meta data should be null
        checkEmptyMetaData(conn, "update ab set a = 2", false);

        // test delete - meta data should be null
        checkEmptyMetaData(conn, "delete from ab", false);
        s.executeUpdate("drop table ab");
        s.close();
    }

    public void testAlterTableMeta() throws SQLException {

        Connection conn = getConnection();
        Statement s = conn.createStatement();
        s.executeUpdate("create table bug4579 (c11 int)");
        s.executeUpdate("insert into bug4579 values (1)");
        PreparedStatement ps = conn.prepareStatement("select * from bug4579");
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
     * @param conn
     *            connection to use
     * @param sql
     *            sql for prepared statement
     * @param execute
     *            execute PreparedStatement if true
     * @throws SQLException
     */
    private void checkEmptyMetaData(Connection conn, String sql, boolean execute)
            throws SQLException {
        PreparedStatement ps;
        ResultSetMetaData rsmd;
        ps = conn.prepareStatement(sql);
        rsmd = ps.getMetaData();
        assertEmptyResultSetMetaData(rsmd);
        if (execute)
            ps.executeUpdate();
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
