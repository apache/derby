/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.UpdateLocksTest
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
package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;

import java.util.Properties;
import java.util.Arrays;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test lock setting for updates for the cartesian product of:
 * <p/>
 * all four ISO isolation levels <b>X</b>  <br>
 * {unpadded varchar column, padded varchar column} <b>X</b>  <br>
 * {unique, non-unique indexes} <b>X</b>   <br>
 * {index on unpadded varchar column, index on padded varchar column}
 * <p/>
 * This test started out as <code>updatelocks.sql</code> in the old
 * harness. The structure of this JUnit test mirrors the old test closely.
 * It contains a test fixture for each isolation level, e.g. four.
 */

public class UpdateLocksTest extends BaseJDBCTestCase {

    public UpdateLocksTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        getLocksQuery = prepareStatement(lock_table_query);
    }


    public void tearDown() throws Exception {
        getLocksQuery = null;

        try {
            dropTable("a");
        } catch (SQLException e) {
            assertSQLState("42Y55", e);
        }

        super.tearDown();
    }


    public static Test suite() {

        Test suite = TestConfiguration.embeddedSuite(UpdateLocksTest.class);

        Properties p = new Properties();
        p.put("derby.storage.pageSize", "4096");

        return new CleanDatabaseTestSetup(
            new SystemPropertyTestSetup(suite, p, false)) {

            /**
             * Creates the views and procedures used by the test cases.
             */
            protected void decorateSQL(Statement s) throws SQLException {
                s.executeUpdate(
                    "create function PADSTRING (data varchar(32000), " +
                    "                           length integer) " +
                    "    returns varchar(32000) " +
                    "    external name " +
                    "    'org.apache.derbyTesting.functionTests." +
                    "util.Formatters.padString' " +
                    "    language java parameter style java");

                s.executeUpdate(
                    "create view LOCK_TABLE as " +
                    "select  " +
                    "    cast(username as char(8)) as username, " +
                    "    cast(t.type as char(8)) as trantype, " +
                    "    cast(l.type as char(8)) as type, " +
                    "    cast(lockcount as char(3)) as cnt, " +
                    "    mode, " +
                    "    cast(tablename as char(12)) as tabname, " +
                    "    cast(lockname as char(10)) as lockname, " +
                    "    state, " +
                    "    status " +
                    "from  " +
                    "    syscs_diag.lock_table l  right outer join " +
                    "    syscs_diag.transaction_table t " +
                    "on l.xid = t.xid " +
                    "where l.tableType <> 'S' and " +
                    "      t.type='UserTransaction'");
            }
        };
    }


    private final static int UNIQUE_INDEX = 0;
    private final static int NON_UNIQUE_INDEX = 1;
    private final static int NO_IDX_1 = 2;
    private final static int NO_IDX_2 = 3;
    private final static String _g = "GRANT";
    private final static String _a = "ACTIVE";
    private final static String _app = "APP";
    private final static String _ut = "UserTran";
    private final static String _t = "TABLE";
    private final static String _r = "ROW";
    private final static String _tl = "Tablelock";
    private final static String _A = "A";
    private final static String _X = "X";
    private final static String _IX = "IX";
    private final static String _U = "U";


    /**
     * Should be the same as SERIALIZABLE results except no previous key locks.
     */
    public void testRepeatableRead () throws Exception {
        doRunTests(Connection.TRANSACTION_REPEATABLE_READ);
    }

    public void testReadCommitted() throws Exception {
        doRunTests(Connection.TRANSACTION_READ_COMMITTED);
    }

    public void testSerializable() throws Exception {
        doRunTests(Connection.TRANSACTION_SERIALIZABLE);
    }

    public void testReadUncommitted() throws Exception {
        doRunTests(Connection.TRANSACTION_READ_UNCOMMITTED);
    }

    private void insertValuesUnpaddedVarchar(Statement s) throws SQLException {
        s.executeUpdate("insert into a values (1, 10, 'one')");
        s.executeUpdate("insert into a values (2, 20, 'two')");
        s.executeUpdate("insert into a values (3, 30, 'three')");
        s.executeUpdate("insert into a values (4, 40, 'four')");
        s.executeUpdate("insert into a values (5, 50, 'five')");
        s.executeUpdate("insert into a values (6, 60, 'six')");
        s.executeUpdate("insert into a values (7, 70, 'seven')");
    }

    private void doRunTests (int isolation) throws Exception {
        setAutoCommit(false);
        getConnection().setTransactionIsolation(
            isolation);
        commit();

        // Run each test with rows on one page in the interesting conglomerate
        // (heap in the non-index tests, and in the index in the index based
        // tests).

        // cursor, no index run
        // To create tables of page size 4k and still keep the following tbl
        // create table a (a int, b int, c varchar(1900));
        Statement s = createStatement();
        s.executeUpdate("create table a(a int, b int)");
        s.executeUpdate("alter table a add column c varchar(1900)");
        insertValuesUnpaddedVarchar(s);
        commit();
        updatecursorlocks(getConnection(), isolation, 0, NO_IDX_1);

        // non cursor, no index run
        // to create tables of page size 4k and still keep the following tbl
        // create table a (a int, b int, c varchar(1900));

        s.executeUpdate("create table a(a int, b int)");
        s.executeUpdate("alter table a add column c varchar(1900)");
        insertValuesUnpaddedVarchar(s);
        commit();
        updatesetlocks(getConnection(), isolation, 0, NO_IDX_1);

        // cursor, unique index run
        // to create tables of page size 4k and still keep the following tbl
        // create table a (a int, b int, c varchar(1900));
        s.executeUpdate("create table a(a int, b int)");
        s.executeUpdate("alter table a add column c varchar(1900)");
        insertValuesUnpaddedVarchar(s);
        s.executeUpdate("create unique index a_idx on a (a)");
        commit();
        updateBtreeCursorLocks1(getConnection(),
                                isolation,
                                UNIQUE_INDEX,
                                true, 0, 0);
        updateBtreeCursorLocks2(getConnection(),
                                isolation,
                                UNIQUE_INDEX,
                                true, 0, 0);

        // cursor, non-unique index run
        // to create tables of page size 4k and still keep the following tbl
        // create table a (a int, b int, c varchar(1900));
        s.executeUpdate("create table a(a int, b int)");
        s.executeUpdate("alter table a add column c varchar(1900)");
        insertValuesUnpaddedVarchar(s);
        s.executeUpdate("create index a_idx on a (a)");
        commit();
        updateBtreeCursorLocks1(getConnection(), isolation,
                                NON_UNIQUE_INDEX,
                                true, 0, 0);
        updateBtreeCursorLocks2(getConnection(), isolation,
                                NON_UNIQUE_INDEX,
                                true, 0, 0);

        // non cursor, unique index run
        // to create tables of page size 4k and still keep the following tbl
        // create table a (a int, b int, c varchar(1900));

        s.executeUpdate("create table a(a int, b int)");
        s.executeUpdate("alter table a add column c varchar(1900)");
        insertValuesUnpaddedVarchar(s);
        s.executeUpdate("create unique index a_idx on a (a)");
        commit();
        updateBtreeSetLocks(
            getConnection(), isolation, UNIQUE_INDEX, true, 0, 0);

        // non cursor, non-unique index run
        // to create tables of page size 4k and still keep the following tbl
        // create table a (a int, b int, c varchar(1900));

        s.executeUpdate("create table a(a int, b int)");
        s.executeUpdate("alter table a add column c varchar(1900)");
        insertValuesUnpaddedVarchar(s);
        s.executeUpdate("create index a_idx on a (a)");
        commit();
        updateBtreeSetLocks(
            getConnection(), isolation, NON_UNIQUE_INDEX, true, 0, 0);

        // run each test with rows across multiple pages in the interesting
        // conglomerate (heap in the non-index tests, and in the index in the
        // index based tests).

        // cursor, no index run
        s.executeUpdate("create table a (a int, b int, c varchar(1900))");
        s.executeUpdate("insert into a values (1, 10, " +
                        "    PADSTRING('one',1900))");
        s.executeUpdate("insert into a values (2, 20, " +
                        "    PADSTRING('two',1900))");
        s.executeUpdate("insert into a values (3, 30, " +
                        "    PADSTRING('three',1900))");
        s.executeUpdate("insert into a values (4, 40, " +
                        "    PADSTRING('four',1900))");
        s.executeUpdate("insert into a values (5, 50, " +
                        "    PADSTRING('five',1900))");
        s.executeUpdate("insert into a values (6, 60, " +
                        "    PADSTRING('six',1900))");
        s.executeUpdate("insert into a values (7, 70, " +
                        "    PADSTRING('seven',1900))");
        commit();
        updatecursorlocks(getConnection(), isolation, 1900, NO_IDX_2);

        // non cursor, no index run
        s.executeUpdate("create table a (a int, b int, c varchar(1900))");
        s.executeUpdate("insert into a values (1, 10, " +
                        "    PADSTRING('one',1900))");
        s.executeUpdate("insert into a values (2, 20, " +
                        "    PADSTRING('two',1900))");
        s.executeUpdate("insert into a values (3, 30, " +
                        "    PADSTRING('three',1900))");
        s.executeUpdate("insert into a values (4, 40, " +
                        "    PADSTRING('four',1900))");
        s.executeUpdate("insert into a values (5, 50, " +
                        "    PADSTRING('five',1900))");
        s.executeUpdate("insert into a values (6, 60, " +
                        "    PADSTRING('six',1900))");
        s.executeUpdate("insert into a values (7, 70, " +
                        "    PADSTRING('seven',1900))");
        commit();
        updatesetlocks(getConnection(), isolation, 1900, NO_IDX_2);

        // cursor, unique index run
        s.executeUpdate("create table a (a int, b int, c varchar(1900)," +
                        "    index_pad varchar(600) )");
        s.executeUpdate("insert into a values (1, 10, " +
                        "    PADSTRING('one',1900), " +
                        "    PADSTRING('index pad 1',600))");
        s.executeUpdate("insert into a values (2, 20, " +
                        "    PADSTRING('two',1900), " +
                        "    PADSTRING('index pad 2',600))");
        s.executeUpdate("insert into a values (3, 30, " +
                        "    PADSTRING('three',1900), " +
                        "    PADSTRING('index pad 3',600))");
        s.executeUpdate("insert into a values (4, 40, " +
                        "    PADSTRING('four',1900), " +
                        "    PADSTRING('index pad 4',600))");
        s.executeUpdate("insert into a values (5, 50, " +
                        "    PADSTRING('five',1900), " +
                        "    PADSTRING('index pad 5',600))");
        s.executeUpdate("insert into a values (6, 60, " +
                        "    PADSTRING('six',1900), " +
                        "    PADSTRING('index pad 6',600))");
        s.executeUpdate("insert into a values (7, 70, " +
                        "    PADSTRING('seven',1900), " +
                        "    PADSTRING('index pad 7',600))");
        s.executeUpdate("create unique index a_idx on a (a, index_pad)");
        commit();
        updateBtreeCursorLocks1(getConnection(), isolation,
                                UNIQUE_INDEX, false, 1900, 600);
        updateBtreeCursorLocks2(getConnection(), isolation,
                                UNIQUE_INDEX, false, 1900, 600);

        // cursor, non-unique index run
        s.executeUpdate("create table a (a int, b int, c varchar(1900)," +
                        "    index_pad varchar(700) )");
        s.executeUpdate("insert into a values (1, 10, " +
                        "    PADSTRING('one',1900), " +
                        "    PADSTRING('index pad 1',700))");
        s.executeUpdate("insert into a values (2, 20, " +
                        "    PADSTRING('two',1900), " +
                        "    PADSTRING('index pad 2',700))");
        s.executeUpdate("insert into a values (3, 30, " +
                        "    PADSTRING('three',1900), " +
                        "    PADSTRING('index pad 3',700))");
        s.executeUpdate("insert into a values (4, 40, " +
                        "    PADSTRING('four',1900), " +
                        "    PADSTRING('index pad 4',700))");
        s.executeUpdate("insert into a values (5, 50, " +
                        "    PADSTRING('five',1900), " +
                        "    PADSTRING('index pad 5',700))");
        s.executeUpdate("insert into a values (6, 60, " +
                        "    PADSTRING('six',1900), " +
                        "    PADSTRING('index pad 6',700))");
        s.executeUpdate("insert into a values (7, 70, " +
                        "    PADSTRING('seven',1900), " +
                        "    PADSTRING('index pad 7',700))");
        s.executeUpdate("create index a_idx on a (a, index_pad)");
        commit();
        updateBtreeCursorLocks1(getConnection(), isolation,
                                NON_UNIQUE_INDEX, false, 1900, 700);
        updateBtreeCursorLocks2(getConnection(), isolation,
                                NON_UNIQUE_INDEX, false, 1900, 700);

        // non cursor, unique index run
        s.executeUpdate("create table a (a int, b int, c varchar(1900)," +
                        "    index_pad varchar(800) )");
        s.executeUpdate("insert into a values (1, 10, " +
                        "    PADSTRING('one',1900), " +
                        "    PADSTRING('index pad 1',800))");
        s.executeUpdate("insert into a values (2, 20, " +
                        "    PADSTRING('two',1900), " +
                        "    PADSTRING('index pad 2',800))");
        s.executeUpdate("insert into a values (3, 30, " +
                        "    PADSTRING('three',1900), " +
                        "    PADSTRING('index pad 3',800))");
        s.executeUpdate("insert into a values (4, 40, " +
                        "    PADSTRING('four',1900), " +
                        "    PADSTRING('index pad 4',800))");
        s.executeUpdate("insert into a values (5, 50, " +
                        "    PADSTRING('five',1900), " +
                        "    PADSTRING('index pad 5',800))");
        s.executeUpdate("insert into a values (6, 60, " +
                        "    PADSTRING('six',1900), " +
                        "    PADSTRING('index pad 6',800))");
        s.executeUpdate("insert into a values (7, 70, " +
                        "    PADSTRING('seven',1900), " +
                        "    PADSTRING('index pad 7',800))");
        s.executeUpdate("create unique index a_idx on a (a, index_pad)");
        commit();
        updateBtreeSetLocks(getConnection(), isolation,
                            UNIQUE_INDEX, false, 1900, 800);

        // non cursor, non-unique index run
        s.executeUpdate("create table a (a int, b int, c varchar(1900), " +
                        "    index_pad varchar(900) )");
        s.executeUpdate("insert into a values (1, 10, " +
                        "    PADSTRING('one',1900), " +
                        "    PADSTRING('index pad 1',900))");
        s.executeUpdate("insert into a values (2, 20, " +
                        "    PADSTRING('two',1900), " +
                        "    PADSTRING('index pad 2',900))");
        s.executeUpdate("insert into a values (3, 30, " +
                        "    PADSTRING('three',1900), " +
                        "    PADSTRING('index pad 3',900))");
        s.executeUpdate("insert into a values (4, 40, " +
                        "    PADSTRING('four',1900), " +
                        "    PADSTRING('index pad 4',900))");
        s.executeUpdate("insert into a values (5, 50, " +
                        "    PADSTRING('five',1900), " +
                        "    PADSTRING('index pad 5',900))");
        s.executeUpdate("insert into a values (6, 60, " +
                        "    PADSTRING('six',1900), " +
                        "    PADSTRING('index pad 6',900))");
        s.executeUpdate("insert into a values (7, 70, " +
                        "    PADSTRING('seven',1900), " +
                        "    PADSTRING('index pad 7',900))");
        s.executeUpdate("create index a_idx on a (a, index_pad)");
        commit();
        updateBtreeSetLocks(getConnection(), isolation,
                            NON_UNIQUE_INDEX, false, 1900, 900);

        commit();
    }


    private void updatecursorlocks(
        Connection c, int isolation, int pad, int mode) throws SQLException {

        Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = s.executeQuery(
            "select a, b, c from a for update");

        ResultSet ltrs = getLocks();

        verifyRsMetaData(ltrs);

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));

        rs.next();

        assertRow(rs, new String[] {"1", "10", pad2("one", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"2", "20", pad2("two", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a}}
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"3", "30", pad2("three", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                ( mode == NO_IDX_1 ?
                  new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                  :
                  new String[][]{
                      {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                      {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                      {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                      {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
            ));

        rs.next();
        assertRow(rs, new String[] {"4", "40", pad2("four", pad)});
        ltrs = getLocks();
        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a}}
                )
            ));

        rs.next();
        assertRow(rs, new String[] {"5", "50", pad2("five", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"6", "60", pad2("six", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"7", "70", pad2("seven", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);

        commit();

        /* ------------------------------------------------------------
         * Test full cursor scan which deletes "even" rows.  Will claim an U
         * lock as it visits each row.  Will claim an X lock on any row it
         * actually deletes.
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a for update");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"1", "10", pad2("one", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"2", "20", pad2(pad2("two", pad), pad)});
        rs.deleteRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"3", "30", pad2("three", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"4", "40", pad2("four", pad)});
        rs.deleteRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"5", "50", pad2("five", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"6", "60", pad2("six", pad)});
        rs.deleteRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"7", "70", pad2("seven", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                )
            ));
        rs.close();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                )
            ));
        commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        commit();

        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select * from a"),
            new String[][] {
                {"1", "10", pad2("one", pad)},
                {"3", "30", pad2("three", pad)},
                {"5", "50", pad2("five", pad)},
                {"7", "70", pad2("seven", pad)}},
            false);
        commit();

        /* ------------------------------------------------------------
         * Test full cursor scan which does no updates, now there are committed
         * deleted rows in the heap, make sure there are no locks on the
         * committed deleted rows.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a for update");

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"1", "10", pad2("one", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"3", "30", pad2("three", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"5", "50", pad2("five", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"7", "70", pad2("seven", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        rs.close();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);

        commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"1", "10", pad2("one", pad)},
                {"3", "30", pad2("three", pad)},
                {"5", "50", pad2("five", pad)},
                {"7", "70", pad2("seven", pad)}},
            false);
        commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which updates the middle 2 rows, now there are
         * committed deleted rows in the heap.  Will get X locks only on the
         * rows which are updated.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a for update");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"1", "10", pad2("one", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"3", "30", pad2("three", pad)});

        rs.updateInt(_A, -3);
        rs.updateInt("B", -30);
        rs.updateString("C", pad2("-three", pad));
        rs.updateRow();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"5", "50", pad2("five", pad)});

        rs.updateInt(_A,-5);
        rs.updateInt("B",-50);
        rs.updateString("C", pad2("-five", pad));
        rs.updateRow();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[] {"7", "70", pad2("seven", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);

        commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"1", "10", pad2("one", pad)},
                {"-3", "-30", pad2("-three", pad)},
                {"-5", "-50", pad2("-five", pad)},
                {"7", "70", pad2("seven", pad)}},
            false);
        commit();

        /* ------------------------------------------------------------
         * Test qualified full cursor scan which does no updates.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * -3, -30, '-three'
         * -5, -50, '-five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a < 0 for update");

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"-3", "-30", pad2("-three", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                mode == NO_IDX_1 ?
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"-5", "-50", pad2("-five", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        rs.close();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);

        commit();

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"1", "10", pad2("one", pad)},
                {"-3", "-30", pad2("-three", pad)},
                {"-5", "-50", pad2("-five", pad)},
                {"7", "70", pad2("seven", pad)}},
            false);
        commit();

        /* ------------------------------------------------------------
         * Test qualified full cursor scan which deletes the positive rows.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * -3, -30, '-three'
         * -5, -50, '-five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a > 0 for update");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"1", "10", pad2("one", pad)});
        rs.deleteRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"7", "70", pad2("seven", pad)});
        rs.deleteRow();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,13)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,13)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,13)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                )
            ));
        commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);

        commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"-3", "-30", pad2("-three", pad)},
                {"-5", "-50", pad2("-five", pad)}},
            false);
        commit();

        /* ------------------------------------------------------------
         * Test qualified full cursor scan which updates a row.
         *
         * At this point the table should look like:
         * -3, -30, '-three'
         * -5, -50, '-five'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a >= -5 for update");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"-3", "-30", pad2("-three", pad)});
        rs.updateInt(_A, 3);
        rs.updateInt("B", 30);
        rs.updateString("C", pad2("three", pad));
        rs.updateRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                mode == NO_IDX_1 ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"-5", "-50", pad2("-five", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
            ));
        commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);

        commit();
        rs = s.executeQuery(
            "select * from a");
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"3", "30", pad2("three", pad)},
                {"-5", "-50", pad2("-five", pad)}},
            true); // need trim here because pad2("three", pad) is shorter
                   // than pad2("-three", pad)..

        commit();

        /* ------------------------------------------------------------
         * Test qualified full cursor scan which updates a row and deletes a
         * row.
         *
         * At this point the table should look like:
         * 3, 30, 'three'
         * -5, -50, '-five'
         * ------------------------------------------------------------
         */
        rs= s.executeQuery(
            "select a, b, c from a where a >= -5 for update");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"3", "30", pad2("three", pad)});
        rs.updateInt(_A, 33);
        rs.updateInt("B", 3030);
        rs.updateString("C", "threethree");
        rs.updateRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                mode == NO_IDX_1 ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[] {"-5", "-50", pad2("-five", pad)});
        rs.deleteRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);

        commit();

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"33", "3030", "threethree"}});
        commit();

        s.executeUpdate("drop table a");

        rs.close();
        ltrs.close();
        s.close();
    }

    /**
     * Assumes that calling routine has set up the following simple dataset,
     * a heap, no indexes with following initial values:
     * create table (a int, b int, c somesortofchar);
     * 1, 10, 'one'
     * 2, 20, 'two'
     * 3, 30, 'three'
     * 4, 40, 'four'
     * 5, 50, 'five'
     * 6, 60, 'six'
     * 7, 70, 'seven'
     */
    private void updatesetlocks(
        Connection c, int isolation, int pad, int mode) throws SQLException {

        c.setAutoCommit(false);

        Statement s = c.createStatement();

        /* ------------------------------------------------------------
         * Assumes that calling routine has set up the following simple dataset,
         * a heap, no indexes with following initial values:
         *     create table (a int, b int, c somesortofchar);
         * 1, 10, 'one'
         * 2, 20, 'two'
         * 3, 30, 'three'
         * 4, 40, 'four'
         * 5, 50, 'five'
         * 6, 60, 'six'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"1", "10", "one"},
                {"2", "20", "two"},
                {"3", "30", "three"},
                {"4", "40", "four"},
                {"5", "50", "five"},
                {"6", "60", "six"},
                {"7", "70", "seven"}});
        c.commit();
        /* ------------------------------------------------------------
         * Test full heap scan which does a delete which does not qualify.
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a where a = -42");
        ResultSet ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        /* ------------------------------------------------------------
         * Test full heap scan which does an update which does not qualify.
         * This can be done with a single call to store pushing the qualifier
         * down.
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a set b = -b where a = -42");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"1", "10", "one"},
                {"2", "20", "two"},
                {"3", "30", "three"},
                {"4", "40", "four"},
                {"5", "50", "five"},
                {"6", "60", "six"},
                {"7", "70", "seven"}});
        c.commit();
        /* ------------------------------------------------------------
         * Test full heap scan which does an update which does not qualify.
         * This has to be deferred as the update may change the values in the
         * qualifer.
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a set a = -a, b = -b where a = -42");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"1", "10", "one"},
                {"2", "20", "two"},
                {"3", "30", "three"},
                {"4", "40", "four"},
                {"5", "50", "five"},
                {"6", "60", "six"},
                {"7", "70", "seven"}});

        c.commit();
        /* ------------------------------------------------------------
         * Test full scan which deletes "even" rows.
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a where a = 2 or a = 4 or a = 6");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,7)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"1", "10", "one"},
                {"3", "30", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}});

        c.commit();
        /* ------------------------------------------------------------
         * Test full heap scan which does a delete which does not qualify.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a where a = -42");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        /* ------------------------------------------------------------
         * Test full heap scan which does an update which does not qualify.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a set b = -b where a = -42");

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"1", "10", "one"},
                {"3", "30", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}});

        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which updates the middle 2 rows, now there are
         * committed deleted rows in the heap.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a set b = -b where a = 3 or a = 5");

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NO_IDX_1 ?
             new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _t, "1", _X, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"1", "10", "one"},
                {"3", "-30", "three"},
                {"5", "-50", "five"},
                {"7", "70", "seven"}});

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which does no updates.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, -30, 'three'
         * 5, -50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a set b = 4000 where a < 0");

        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which deletes the positive rows.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, -30, 'three'
         * 5, -50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a where b > 0");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,13)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"3", "-30", "three"},
                {"5", "-50", "five"}});

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which updates a row.
         *
         * At this point the table should look like:
         * 3, -30, 'three'
         * 5, -50, 'five'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a set b = -b, c = 'three' where a > 2 and a < 5");

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"3", "30", "three"},
                {"5", "-50", "five"}});

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which updates a row and deletes a
         * row.
         *
         * can't be done is one set statement, do 2 statements.
         *
         * At this point the table should look like:
         * 3, 30, 'three'
         * 5, -50, 'five'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a set a=33,b=3030,c='threethree' where a = 3");

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
                )
            ));
        s.executeUpdate(
            "delete from a where a = 5");

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "4", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (mode == NO_IDX_1 ?
                 new String[][]{
                    {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    mode == NO_IDX_1 ?
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            new String[][] {
                {"33", "3030", "threethree"}});

        c.commit();
        s.executeUpdate(
            "drop table a");

        ltrs.close();
        s.close();
    }

    /*
     * Very basic single user testing of btree cursor update locks.  This is
     * test is meant to be run from another test such that it gets run under
     * multiple isolation levels.  This is important for update locks as they
     * behave differently, depending on isolation levels.
     *
     * This test concentrates on updates which use a primary index for the
     * cursor, and then update a non-key field, or delete a row.
     */
    private void updateBtreeCursorLocks1(
        Connection c,
        int isolation,
        int mode,
        boolean unPadded,
        int pad,
        int idxPad) throws SQLException {

        c.setAutoCommit(false);
        Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_UPDATABLE);

        ResultSet rs = s.executeQuery(
            "select a, b, c from a for update");

        ResultSet ltrs = getLocks();

        verifyRsMetaData(ltrs);
        /* ------------------------------------------------------------
         * Assumes that calling routine has set up the following simple dataset,
         * a heap, no indexes with following initial values:
         *     create table (a int, b int, c somesortofchar, d somesortofpad);
         *     create index a_idx on (a, somesortofpad)
         * 1, 10, 'one'
         * 2, 20, 'two'
         * 3, 30, 'three'
         * 4, 40, 'four'
         * 5, 50, 'five'
         * 6, 60, 'six'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"2", "20", "two"},
                {"3", "30", "three"},
                {"4", "40", "four"},
                {"5", "50", "five"},
                {"6", "60", "six"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"2", "20", pad2("two", pad), pad2("index pad 2", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"4", "40", pad2("four", pad), pad2("index pad 4", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"6", "60", pad2("six", pad), pad2("index pad 6", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}},
            false);

        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which does no updates.
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where " +
            "    a >= 1 and a < 20 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"1", "10", pad2("one", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"2", "20", pad2("two", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"3", "30", pad2("three", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"4", "40", pad2("four", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"5", "50", pad2("five", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"6", "60", pad2("six", pad)});
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"7", "70", pad2("seven", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        rs.close();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));

        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which does no updates, which exits in middle
         * of scan.
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a >= 1 and a < 20 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"1", "10", pad2("one", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"2", "20", pad2("two", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"3", "30", pad2("three", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"4", "40", pad2("four", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
            ));
        rs.close();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which deletes "even" rows.
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a >= 1 and a < 20 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"1", "10", pad2("one", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"2", "20", pad2("two", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.deleteRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             (mode == UNIQUE_INDEX ?
              new String[][]{
                 {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"3", "30", pad2("three", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             (mode == UNIQUE_INDEX ?
              new String[][]{
                 {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"4", "40", pad2("four", pad)});

        rs.deleteRow();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             (mode == UNIQUE_INDEX ?
              new String[][]{
                 {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"5", "50", pad2("five", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             (mode == UNIQUE_INDEX ?
              new String[][]{
                 {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "5", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
                )
            ));

        rs.next();
        assertRow(rs, new String[]{"6", "60", pad2("six", pad)});

        rs.deleteRow();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "6", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             (mode == UNIQUE_INDEX ?
              new String[][]{
                 {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"7", "70", pad2("seven", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "6", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             (mode == UNIQUE_INDEX ?
              new String[][]{
                 {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "6", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             (mode == UNIQUE_INDEX ?
              new String[][]{
                 {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "6", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             (mode == UNIQUE_INDEX ?
              new String[][]{
                 {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "7", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select a from a"),
            new String[][] {
                {"1"},
                {"3"},
                {"5"},
                {"7"}});

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "30", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}},
            false);
        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which does no updates, now there are committed
         * deleted rows in the heap.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a >= 1 and a < 20 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"1", "10", pad2("one", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"3", "30", pad2("three", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"5", "50", pad2("five", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"7", "70", pad2("seven", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        rs.close();

    }

    private void updateBtreeCursorLocks2(
        Connection c,
        int isolation,
        int mode,
        boolean unPadded,
        int pad,
        int idxPad) throws SQLException {

        Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = null;

        ResultSet ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        c.commit();

        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "30", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}},
            false);

        c.commit();
        /* ------------------------------------------------------------
         * Test exact match cursor scan does one update,
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a = 3 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}});
        rs.next();
        assertRow(rs, new String[]{"3", "30", pad2("three", pad)});

        rs.updateInt("B", -3000);
        rs.updateRow();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            (isolation == Connection.TRANSACTION_SERIALIZABLE &&
             mode == NON_UNIQUE_INDEX && unPadded) ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, "Tablelock ", _g, _a},
                {_app, _ut, _r, "1  ", _U, _A, "(1,8)", _g, _a},
                {_app, _ut, _r, "1  ", _U, _A, "(1,9)", _g, _a},
                {_app, _ut, _r, "1  ", _X, _A, "(1,9)", _g, _a}}
            :
            (isolation == Connection.TRANSACTION_SERIALIZABLE &&
             mode == NON_UNIQUE_INDEX && !unPadded) ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, "Tablelock ", _g, _a},
                {_app, _ut, _r, "1  ", _U, _A, "(2,6)", _g, _a},
                {_app, _ut, _r, "1  ", _U, _A, "(3,6)", _g, _a},
                {_app, _ut, _r, "1  ", _X, _A, "(3,6)", _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_SERIALIZABLE && !unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, "Tablelock ", _g, _a},
                    {_app, _ut, _r, "1  ", _U, _A, "(2,6)", _g, _a},
                    {_app, _ut, _r, "1  ", _U, _A, "(3,6)", _g, _a},
                    {_app, _ut, _r, "1  ", _X, _A, "(3,6)", _g, _a}}
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, "Tablelock ", _g, _a},
                 {_app, _ut, _r, "1  ", _U, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "1  ", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1  ", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, "Tablelock ", _g, _a},
                  {_app, _ut, _r, "1  ", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1  ", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1  ", _X, _A, "(3,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                unPadded ?
                (mode == UNIQUE_INDEX ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                )
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            ));


        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "-3000", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "-3000", pad2("three",pad), pad2("index pad 3", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}},
            false);

        c.commit();
        /* ------------------------------------------------------------
         * Test exact match cursor scan does one update, and bales early.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, -3000, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a = 3 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}});
        rs.next();
        assertRow(rs, new String[]{"3", "-3000", pad2("three", pad)});
        rs.updateInt("B", 30);
        rs.updateRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "30", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}},
            false);

        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which updates the middle 2 rows, now there are
         * committed deleted rows in the heap.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a >= 1 and a < 20 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"1", "10", pad2("one", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"3", "30", pad2("three", pad)});

        rs.updateInt("B", -30);
        rs.updateString("C", "-three");
        rs.updateRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"5", "50", pad2("five", pad)});

        rs.updateInt("B", -50);
        rs.updateString("C", "-five");
        rs.updateRow();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"7", "70", pad2("seven", pad)});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "-30", "-three"},
                {"5", "-50", "-five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "-30", "-three", pad2("index pad 3", idxPad)},
                {"5", "-50", "-five", pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}},
            false);

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which does no updates.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, -30, '-three'
         * 5, -50, '-five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a from a where a > 0 and b < 0 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            )
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));

        rs.next();
        assertRow(rs, new String[]{"3"});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.next();
        assertRow(rs, new String[]{"5"});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a}}
                )
            ));
        assertFalse(rs.next());
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(6,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    new String[][]{
                        {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "-30", "-three"},
                {"5", "-50", "-five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "-30", "-three", pad2("index pad 3", idxPad)},
                {"5", "-50", "-five", pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}},
            false);

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified exact match cursor scan which deletes one row.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, -30, '-three'
         * 5, -50, '-five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a from a where a = 1 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}});
        rs.next();
        assertRow(rs, new String[]{"1"});

        rs.deleteRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            (isolation == Connection.TRANSACTION_SERIALIZABLE &&
             mode == NON_UNIQUE_INDEX) ?
            new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                {_app, _ut, _r, "1", _X, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
            :
            ((isolation == Connection.TRANSACTION_SERIALIZABLE &&
              mode == UNIQUE_INDEX && !unPadded) ?
             new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
             :
             (
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
             )
            ));
        assertFalse(rs.next());

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            (isolation == Connection.TRANSACTION_SERIALIZABLE &&
             mode == NON_UNIQUE_INDEX) ?
            new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                {_app, _ut, _r, "1", _X, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
            :
            ((isolation == Connection.TRANSACTION_SERIALIZABLE &&
              mode == UNIQUE_INDEX && !unPadded) ?
             new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
             :
             (
                 isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                 (
                     unPadded ?
                     new String[][]{
                         {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                         {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                         {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
                     :
                     new String[][]{
                         {_app,_ut, _t, "3", _IX, _A, _tl, _g, _a},
                         {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                         {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
                 )
                 :
                 (
                     unPadded ?
                     (mode == UNIQUE_INDEX ?
                      new String[][]{
                         {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                         {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                         {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
                      :
                      new String[][]{
                          {_app,_ut, _t, "3", _IX, _A, _tl, _g, _a},
                          {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
                     )
                     :
                     new String[][]{
                         {_app,_ut, _t, "3", _IX, _A, _tl, _g, _a},
                         {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
                 )
             )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            (isolation == Connection.TRANSACTION_SERIALIZABLE &&
             mode == NON_UNIQUE_INDEX) ?
            new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                {_app, _ut, _r, "1", _X, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
            :
            ((isolation == Connection.TRANSACTION_SERIALIZABLE &&
              mode == UNIQUE_INDEX && !unPadded) ?
             new String[][]{
                {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
             :
             (
                 isolation == Connection.TRANSACTION_SERIALIZABLE ?
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
                 :
                 (
                     isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                     new String[][]{
                         {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                         {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                         {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
                     :
                     (new String[][]{
                         {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                         {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
                     )
                 )
             )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"3", "-30", "-three"},
                {"5", "-50", "-five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"3", "-30", "-three", pad2("index pad 3", idxPad)},
                {"5", "-50", "-five", pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}},
            false);

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which deletes the positive rows.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, -30, '-three'
         * 5, -50, '-five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a from a where a <> 3 and a <> 5 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                new String[][]{
                    {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"7"});

        rs.deleteRow();
        ltrs = getLocks();
        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,13)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(7,6)", _g, _a}}
            ));
        assertFalse(rs.next());
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,13)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,13)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(7,6)", _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _t, "1", _X, _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,13)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(7,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(7,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,13)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "3", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(7,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"3", "-30", "-three"},
                {"5", "-50", "-five"}}
            :
            new String[][] {
                {"3", "-30", "-three", pad2("index pad 3", idxPad)},
                {"5", "-50", "-five", pad2("index pad 5", idxPad)}},
            false);

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which updates a row.
         *
         * At this point the table should look like:
         * 3, -30, '-three'
         * 5, -50, '-five'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a,b,c from a where a > 2 and a <= 5 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}});
        rs.next();
        assertRow(rs, new String[]{"3", "-30", "-three"});

        rs.updateInt("B", 30);
        rs.updateString("C", "three");
        rs.updateRow();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"5", "-50", "-five"});

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        rs.close();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"3", "30", "three"},
                {"5", "-50", "-five"}}
            :
            new String[][] {
                {"3", "30", "three", pad2("index pad 3", idxPad)},
                {"5", "-50", "-five", pad2("index pad 5", idxPad)}},
            false);

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which updates a row and deletes a
         * row.
         *
         * At this point the table should look like:
         * 3, 30, 'three'
         * 5, -50, '-five'
         * ------------------------------------------------------------
         */
        rs = s.executeQuery(
            "select a, b, c from a where a > 0 for update of b, c");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            new String[][]{
                {_app, _ut, _t, "1", _IX, _A, _tl, _g, _a}});
        rs.next();
        assertRow(rs, new String[]{"3", "30", "three"});

        rs.updateInt("B", 3030);
        rs.updateString("C", "threethree");
        rs.updateRow();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (unPadded ?
             new String[][]{
                {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
             :
             new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a}}
            ));
        rs.next();
        assertRow(rs, new String[]{"5", "-50", "-five"});

        rs.deleteRow();
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a}}
                )
            ));
        rs.close();

        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a}}
             )
            )
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", _U, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", _U, _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a}}
                )
                :
                (
                    unPadded ?
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a}}
                    :
                    new String[][]{
                        {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                        {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                        {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a}}
                )
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        /* ------------------------------------------------------------
         * At this point the table should look like:
         * 3, 3030, 'threethree'
         * ------------------------------------------------------------
         */
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {{"3", "3030", "threethree"}}
            :
            new String[][] {{"3", "3030", "threethree",
                             pad2("index pad 3", idxPad)}},
            false);

        c.commit();
        s.executeUpdate(
            "drop table a");

        ltrs.close();
        s.close();
        rs.close();
    }


    /*
     * Very basic single user testing of update locks.  This test is meant to
     * be run from another test such that it gets run under multiple isolation
     * levels.  This is important for update locks as they behave differently,
     * depending on isolation levels.
     *
     * This test concentrates on updates which use a primary index for the
     * cursor,
     * and then update a non-key field, or delete a row.
     */
    private void updateBtreeSetLocks (
        Connection c,
        int isolation,
        int mode,
        boolean unPadded,
        int pad,
        int idxPad) throws SQLException {

        c.setAutoCommit(false);

        Statement s = c.createStatement();

        /* ------------------------------------------------------------
         * Assumes that calling routine has set up the following simple dataset,
         * a heap, no indexes with following initial values:
         *     create table (a int, b int, c somesortofchar, d somesortofpad);
         *     create index a_idx on (a, somesortofpad)
         * 1, 10, 'one'
         * 2, 20, 'two'
         * 3, 30, 'three'
         * 4, 40, 'four'
         * 5, 50, 'five'
         * 6, 60, 'six'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"2", "20", "two"},
                {"3", "30", "three"},
                {"4", "40", "four"},
                {"5", "50", "five"},
                {"6", "60", "six"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"2", "20", pad2("two", pad), pad2("index pad 2", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"4", "40", pad2("four", pad), pad2("index pad 4", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"6", "60", pad2("six", pad), pad2("index pad 6", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which does no updates.
         * ------------------------------------------------------------
         */
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"2", "20", "two"},
                {"3", "30", "three"},
                {"4", "40", "four"},
                {"5", "50", "five"},
                {"6", "60", "six"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"2", "20", pad2("two", pad), pad2("index pad 2", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"4", "40", pad2("four", pad), pad2("index pad 4", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"6", "60", pad2("six", pad), pad2("index pad 6", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});

        ResultSet ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            new String[][]{
                {_app, _ut, _t, "1", "S", _A, _tl, _g, _a}}
            :
            (
                isolation == Connection.TRANSACTION_REPEATABLE_READ ?
                (unPadded ?
                 new String[][]{
                    {_app, _ut, _t, "1", "IS", _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", "S", _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "1", "S", _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "1", "S", _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "1", "S", _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "1", "S", _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", "S", _A, "(1,8)", _g, _a},
                    {_app, _ut, _r, "1", "S", _A, "(1,9)", _g, _a}}
                 :
                 new String[][]{
                     {_app, _ut, _t, "1", "IS", _A, _tl, _g, _a},
                     {_app, _ut, _r, "1", "S", _A, "(1,7)", _g, _a},
                     {_app, _ut, _r, "1", "S", _A, "(2,6)", _g, _a},
                     {_app, _ut, _r, "1", "S", _A, "(3,6)", _g, _a},
                     {_app, _ut, _r, "1", "S", _A, "(4,6)", _g, _a},
                     {_app, _ut, _r, "1", "S", _A, "(5,6)", _g, _a},
                     {_app, _ut, _r, "1", "S", _A, "(6,6)", _g, _a},
                     {_app, _ut, _r, "1", "S", _A, "(7,6)", _g, _a}}
                )
                :
                (
                    new String[][]{})
            ));

        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which deletes exact match on a.
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a where a = 4");
        ltrs = getLocks();


        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"2", "20", "two"},
                {"3", "30", "three"},
                {"5", "50", "five"},
                {"6", "60", "six"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"2", "20", pad2("two", pad), pad2("index pad 2", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"6", "60", pad2("six", pad), pad2("index pad 6", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which deletes "even" rows.
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a where a = 2 or a = 4 or a = 6");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,11)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,9)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,12)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,8)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(2,6)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(6,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();

        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select a from a"),
            new String[][] {
                {"1"},
                {"3"},
                {"5"},
                {"7"}});

        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "30", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which does no updates, now there are committed
         * deleted rows in the heap.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a where (a = 2 or a = 4 or a = 6) and (b < 8)");
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "30", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test updates an exact match on a.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a  set b = 300 where a = 3");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "300", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "300", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test updates an exact match on a with base row qualification
         * necessary.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 300, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a  set b = 30 where a = 3 and b = 300");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "30", "three"},
                {"5", "50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test full cursor scan which updates the middle 2 rows, now there are
         * committed deleted rows in the heap.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, 30, 'three'
         * 5, 50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a  set b = -b where a >= 3 and a < 6");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,11)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(5,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,11)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(5,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(5,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "-30", "three"},
                {"5", "-50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "-30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "-50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});


        c.commit();
        /* ------------------------------------------------------------
         * Test exact match which does no updates.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, -30, 'three'
         * 5, -50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a  where a = 2");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
            ));
        c.commit();
        s.executeUpdate(
            "update a  set b = -b where a = 2");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "-30", "three"},
                {"5", "-50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "-30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "-50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which does no updates.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, -30, 'three'
         * 5, -50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a  where a > 0 and b < -1000");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(6,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(7,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _X, _A, _tl, _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(6,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(7,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(3,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(5,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(6,6)", _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(7,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"1", "10", "one"},
                {"3", "-30", "three"},
                {"5", "-50", "five"},
                {"7", "70", "seven"}}
            :
            new String[][] {
                {"1", "10", pad2("one", pad), pad2("index pad 1", idxPad)},
                {"3", "-30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "-50", pad2("five", pad), pad2("index pad 5", idxPad)},
                {"7", "70", pad2("seven", pad), pad2("index pad 7", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which deletes the positive rows.
         *
         * At this point the table should look like:
         * 1, 10, 'one'
         * 3, -30, '-three'
         * 5, -50, 'five'
         * 7, 70, 'seven'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "delete from a  where a = 1 or a = 7");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,12)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,3)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,13)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(6,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(7,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,13)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(1,3)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(6,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(7,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,13)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,7)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(7,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"3", "-30", "three"},
                {"5", "-50", "five"}}
            :
            new String[][] {
                {"3", "-30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "-50", pad2("five", pad), pad2("index pad 5", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which updates a row.
         *
         * At this point the table should look like:
         * 3, -30, '-three'
         * 5, -50, 'five'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a  set b = 30 where a > 2 and a < 5");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
             )
             :
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "2", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"3", "30", "three"},
                {"5", "-50", "five"}}
            :
            new String[][] {
                {"3", "30", pad2("three", pad), pad2("index pad 3", idxPad)},
                {"5", "-50", pad2("five", pad), pad2("index pad 5", idxPad)}});

        c.commit();
        /* ------------------------------------------------------------
         * Test qualified full cursor scan which updates a row and deletes a
         * row.
         *
         * At this point the table should look like:
         * 3, 30, 'three'
         * 5, -50, 'five'
         * ------------------------------------------------------------
         */
        s.executeUpdate(
            "update a  set b = 3030 where a > 2 and a < 5");
        s.executeUpdate(
            "delete from a where a = 5");
        ltrs = getLocks();

        JDBC.assertUnorderedResultSet(
            ltrs,
            isolation == Connection.TRANSACTION_SERIALIZABLE ?
            (mode == NON_UNIQUE_INDEX ?
             (unPadded ?
              new String[][]{
                 {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
             )
             :
             (unPadded?
              new String[][]{
                 {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                 {_app, _ut, _r, "1", _X, _A, "(1,8)", _g, _a},
                 {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a},
                 {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
              :
              new String[][]{
                  {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                  {_app, _ut, _r, "1", _X, _A, "(2,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(4,6)", _g, _a},
                  {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a},
                  {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
             )
            )
            :
            (
                unPadded ?
                new String[][]{
                    {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(1,10)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(1,11)", _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(1,9)", _g, _a}}
                :
                new String[][]{
                    {_app, _ut, _t, "4", _IX, _A, _tl, _g, _a},
                    {_app, _ut, _r, "1", _X, _A, "(4,6)", _g, _a},
                    {_app, _ut, _r, "2", _X, _A, "(5,6)", _g, _a},
                    {_app, _ut, _r, "3", _X, _A, "(3,6)", _g, _a}}
            ));
        c.commit();
        ltrs = getLocks();
        JDBC.assertEmpty(ltrs);
        c.commit();
        /* ------------------------------------------------------------
         * At this point the table should look like:
         * 3, 3030, 'threethree'
         * ------------------------------------------------------------
         */
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select * from a"),
            unPadded ?
            new String[][] {
                {"3", "3030", "three"}}
            :
            new String[][] {
                {"3", "3030", pad2("three", pad), pad2("index pad 3",idxPad)}});

        c.commit();
        s.executeUpdate(
            "drop table a");

        ltrs.close();
        s.close();
    }


    private void verifyRsMetaData (ResultSet s) throws SQLException {
        ResultSetMetaData rsmd = s.getMetaData();
        assertEquals(rsmd.getColumnCount(), 9);

        String[] colNames = {"USERNAME",
                             "TRANTYPE",
                             "TYPE",
                             "CNT",
                             "MODE",
                             "TABNAME",
                             "LOCKNAME",
                             "STATE",
                             "STATUS"};
        for (int i=0; i < colNames.length; i++) {
            assertTrue(colNames[i].equals(rsmd.getColumnName(i+1)));
        }
    }

    private void assertRow(ResultSet rs, String[] expected)
            throws SQLException {
        assertEquals(rs.getMetaData().getColumnCount(), expected.length);

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], rs.getString(i + 1));
        }

    }

    // /**
    //  * Useful for debugging result sets if the test fails.
    //  */
    // private void show(ResultSet rs, boolean unPadded) throws SQLException {
    //     System.err.println("Isolation:" +
    //                        rs.getStatement().
    //                        getConnection().
    //                        getTransactionIsolation());
    //     System.err.println("UnPadded == " + unPadded);
    //     System.err.println("        JDBC.assertUnorderedResultSet(\n" +
    //                        "            ltrs,\n" +
    //                        "            new String[][]{");
    //     int rowNo = 0;
    //     while (rs.next()) {
    //         if (rowNo++ != 0) {
    //             System.err.print(",\n");
    //         }
    //
    //         System.err.print(
    //             "                {\"" +
    //             rs.getString(1).trim() + "\", \"" +
    //             rs.getString(2).trim() + "\", \"" +
    //             rs.getString(3).trim() + "\", \"" +
    //             rs.getString(4).trim() + "\", \"" +
    //             rs.getString(5).trim() + "\", \"" +
    //             rs.getString(6).trim() + "\", \"" +
    //             rs.getString(7).trim() + "\", \"" +
    //             rs.getString(8).trim() + "\", \"" +
    //             rs.getString(9).trim() + "\"}");
    //     }
    //
    //     System.err.print("};\n");
    //     rs.close();
    // }

    private static String pad2(String s, int i) {
        if (i == 0) {
            return s;
        }

        char[] buff = new char[i];
        s.getChars(0, s.length(), buff, 0);
        Arrays.fill(buff, s.length(), i, ' ');
        buff[i-1] = 0; // to match bug in Formatters.padString
        return new String(buff);
    }

    private PreparedStatement getLocksQuery;

    private final static String lock_table_query =
        "select * from lock_table order by " +
        "    tabname, type desc, mode, cnt, lockname";


    private ResultSet getLocks() throws SQLException {
        return getLocksQuery.executeQuery();
    }
}
