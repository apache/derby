/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DeclareGlobalTempTableJava

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.DatabaseMetaData;
import java.sql.Connection;

import junit.framework.Test;


import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Test for declared global temporary tables introduced in Cloudscape 5.2 The
 * temp table tests with holdable cursor and savepoints are in
 * declareGlobalTempTableJavaJDBC30 class. The reason for a different test class
 * is that the holdability and savepoint support is under jdk14 and higher. But
 * we want to be able to run the non-jdk14 specific tests under all the jdks we
 * support and hence splitting the tests into 2 different classes Global
 * Temporary Tables are referenced as GTT to make it easy for Naming.
 */

public class DeclareGlobalTempTableJavaTest extends BaseJDBCTestCase {

    public DeclareGlobalTempTableJavaTest(String name) {
        super(name);
    }

    public static Test suite() {
	return TestConfiguration.embeddedSuite(DeclareGlobalTempTableJavaTest.class);
    }
    protected void setUp() throws Exception {
        super.setUp();
        dropSchemaTables();
        getConnection().setAutoCommit(false);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test switching to session schema (it doesn't yet exist because no create
     * schema session has been issued yet) and then try to create first persistent
     * object in it. This used to cause null pointer exception (DERBY-1706).
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testDerby1706() throws SQLException {
        Statement s = createStatement();
        // the try/catch block ensures to drop the SESSION schema if exists.
        try {
            s.executeUpdate("drop schema SESSION restrict");
        } catch (SQLException e) {
            assertSQLState("42Y07", e);
        }
        assertUpdateCount(s , 0 , "set schema SESSION");
        // This used to cause NullPointerException before.
        assertUpdateCount(s, 0, "create table DERBY1706(c11 int)");
 	assertUpdateCount(s, 0, "drop table DERBY1706");
        assertUpdateCount(s, 0, "set schema APP");
 	assertUpdateCount(s, 0, "drop schema SESSION restrict");
    }

    /**
     * Test the schema of Golabal Temporary Tables.
     * Global Temporary Tables can only be in SESSION schema. Declaring them in Other schemas Should give an  Error. 
     * Global Temporary Tables always goes into SESSION schema. Even if
     * the current schema is not SESSION.
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testGTTSchemaName() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "set schema APP");
        // Global Temporary Tables can only be created in SESSION schema
        assertStatementError("428EK",s,"DECLARE GLOBAL TEMPORARY TABLE APP.t2(c21 int) on commit delete rows not logged");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE t2(c21 int) on commit delete rows not logged");
        // temp table t2 is not in APP schema
        assertStatementError("42X05", s, "insert into APP.t2 values(7)");
        // temp table should be referred as SESSIO.t2
        assertStatementError("42X05", s, "insert into t2 values(7)");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(7)");
        // temp table t2 must be qualified with SESSION
        assertStatementError("42Y55", s, "drop table t2");
        assertUpdateCount(s , 0 , "drop table SESSION.t2");
    }

    /**
     * Some positive Grammar tests for the DECLARE GLOBAL TEMPORARY TABLE
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testPositiveGrammars() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s, 0,
                "DECLARE GLOBAL TEMPORARY TABLE tA(c1 int) not logged");
        assertUpdateCount(s, 0,
                "DECLARE GLOBAL TEMPORARY TABLE tB(c1 int) on commit delete rows not logged");
        assertUpdateCount(s, 0,
                "DECLARE GLOBAL TEMPORARY TABLE tC(c1 int) not logged on commit delete rows");
        assertUpdateCount(s, 0,
                "DECLARE GLOBAL TEMPORARY TABLE tD(c1 int) on commit preserve rows not logged");
        assertUpdateCount(s, 0,
                "DECLARE GLOBAL TEMPORARY TABLE tE(c1 int) not logged on commit preserve rows");
        assertUpdateCount(s, 0,
                "DECLARE GLOBAL TEMPORARY TABLE tF(c1 int) on rollback delete rows not logged");
        assertUpdateCount(s, 0,
                "DECLARE GLOBAL TEMPORARY TABLE tG(c1 int) not logged on rollback delete rows");
        assertUpdateCount(
                s,
                0,
                "DECLARE GLOBAL TEMPORARY TABLE tH(c1 int) on commit preserve rows not logged on rollback delete rows");
        assertUpdateCount(
                s,
                0,
                "DECLARE GLOBAL TEMPORARY TABLE tI(c1 int) not logged on commit preserve rows on rollback delete rows");
        assertUpdateCount(
                s,
                0,
                "DECLARE GLOBAL TEMPORARY TABLE tJ(c1 int) not logged on rollback delete rows on commit preserve rows");
        assertUpdateCount(
                s,
                0,
                "DECLARE GLOBAL TEMPORARY TABLE tK(c1 int) on commit delete rows not logged on rollback delete rows");
        assertUpdateCount(
                s,
                0,
                "DECLARE GLOBAL TEMPORARY TABLE tL(c1 int) not logged on commit delete rows on rollback delete rows");
        assertUpdateCount(
                s,
                0,
                "DECLARE GLOBAL TEMPORARY TABLE tM(c1 int) not logged on rollback delete rows on commit delete rows");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tA");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tB");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tC");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tD");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tE");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tF");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tG");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tH");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tI");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tJ");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tK");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tL");
        assertUpdateCount(s, 0, "DROP TABLE SESSION.tM");
    }

    /**
     * Some Negative Grammar tests for the DECLARE GLOBAL TEMPORARY TABLE
     * 
     * @exception SQLException
     * Thrown if some unexpected error happens
     */
    public void testNegativeGrammars() throws SQLException {
        Statement s = createStatement();
        assertStatementError("42X01", s,
                "DECLARE GLOBAL TEMPORARY TABLE t1(c11 int)");
        assertStatementError("42613", s,
                "DECLARE GLOBAL TEMPORARY TABLE t1(c11 int) NOT LOGGED NOT LOGGED");
        assertStatementError(
                "42613",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE t1(c11 int) NOT LOGGED ON COMMIT PRESERVE ROWS ON COMMIT DELETE ROWS");
        assertStatementError(
                "42613",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE t1(c11 int) NOT LOGGED ON ROLLBACK DELETE ROWS ON ROLLBACK DELETE ROWS");
        assertStatementError(
                "42X01",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE t1(c11 int) ON ROLLBACK DELETE ROWS ON COMMIT PRESERVE ROWS");
    }

    /**
     * Test some of the features that are not allowed on temp tables namely
     * ALTER TABLE , LOCK TABLE , RENAME TABLE , CREATE INDEX AND CREATE VIEW
     * CREATE VIEW is some what special in that it can't have reference to the
     * temp tables. The Other two features are generated always as identity and
     * Long datatype.
     * 
     * @throws SQLException
     */
    public void testFeaturesNotAllowedOnGTTs() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) not logged on commit delete rows");
        // Alter Table is not allowed on temp tables.
        assertStatementError("42995", s,"ALTER TABLE SESSION.t2 add column c22 int");
        // Lock Table is not allowed on temp tables.
        assertStatementError("42995", s, "LOCK TABLE SESSION.t2 IN SHARE MODE");
        // Rename Table is not allowed on temp tables.
        assertStatementError("42995", s, "RENAME TABLE SESSION.t2 TO t3");
        // Create Index is not allowed on temp tables.
        assertStatementError("42995", s, "CREATE index t2i1 on SESSION.t2 (c21)");
        // Create view that references temp tables is not allowed
        assertStatementError("XCL51", s, "CREATE VIEW t2v1 as select * from SESSION.t2");
        // generated always as identity not allowed on temp tables.
        assertStatementError("42995",s,"DECLARE GLOBAL TEMPORARY TABLE SESSION.t1(c21 int generated always as identity) on commit delete rows not logged");
        // Long datatypes are not supported.
        assertStatementError(
                "42962",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE SESSION.t1(c21 int, c22 blob(3k)) on commit delete rows not logged");
        assertStatementError(
                "42962",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE SESSION.t1(c21 int, c22 clob(3k)) on commit delete rows not logged");
        assertStatementError(
                "42962",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE SESSION.t1(c21 int, c22 long varchar) on commit delete rows not logged");
        s.executeUpdate("DROP TABLE SESSION.t2");
        assertStatementError("42Y55", s, "DROP TABLE SESSION.t1");
    }

    /**
     * Test some features that allowed on physical tables in SESSION Schema
     * 
     * @throws SQLException
     */
    public void testFeaturesAllowedOnPhysicalTablesOfSESSIONSchema()
            throws SQLException {
        Statement s = createStatement();
        try {
            s.executeUpdate("CREATE schema SESSION");
        } catch (SQLException e) {
            assertSQLState("X0Y68", e);
        }
        assertUpdateCount(s , 0 , "CREATE TABLE SESSION.t2(c21 int)");
        // Alter Table is allowed on physical tables in SESSION schema
        assertUpdateCount(s, 0, "ALTER TABLE SESSION.t2 add column c22 int");
        // Lock Table is allowed on physical tables in SESSION schema
  	assertUpdateCount(s , 0 , "LOCK TABLE SESSION.t2 IN EXCLUSIVE MODE");
        // Rename Table is allowed on physical tables in SESSION schema
        assertUpdateCount(s , 0 , "RENAME TABLE SESSION.t2 TO t3");
        // Lock column is allowed on physical tables in SESSION schema
        assertUpdateCount(s , 0 , "RENAME COLUMN SESSION.t3.c21 TO c23");
        // Create Index is allowed on physical tables in SESSION schema
        assertUpdateCount(s , 0 , "CREATE TABLE SESSION.t2 (c21 int)");
        assertUpdateCount(s , 0 , "CREATE index t2i1 on SESSION.t2 (c21)");
        // Create View referencing physical tables in SESSION schema is not
        // supported
        assertStatementError("XCL51", s,
                "CREATE VIEW t2v1 as select * from SESSION.t2");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t3");
    }
    /**
     * Test the various constraints not allowed on temp tables namely primary
     * key constraints , Unique constraints , check constraints and foreign key
     * constraints.
     * 
     * @throws SQLException
     */
    public void testConstraintsNotAllowedOnGTTs() throws SQLException {
        Statement s = createStatement();
        // primary key constraints are not allowed on temp tables.
        assertStatementError(
                "42995",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int not null, constraint pk primary key (c21)) on commit delete rows not logged");
        // Unique constraints are not allowed on temp tables.
        assertStatementError(
                "42995",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int not null unique) on commit delete rows not logged");
        // foreign key constraints are not allowed on temp tables.
        s.executeUpdate("CREATE TABLE t1(c11 int not null unique)");
        assertStatementError(
                "42995",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int references t1(c11)) on commit delete rows not logged");
        // check constraints are not allowed on temp tables.
        assertStatementError(
                "42995",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int check (c21 > 0)) on commit delete rows not logged");
        s.executeUpdate("DROP TABLE t1");
    }

    /**
     * Test the constraints allowed on physical tables in SESSION schema.
     * 
     * @throws SQLException
     */
    public void testConstraintsAllowedOnSESSIONPhysicalTables()
            throws SQLException {
        Statement s = createStatement();
        try {
            s.executeUpdate("CREATE SCHEMA SESSION");
        } catch (SQLException e) {
            assertSQLState("X0Y68", e);
        }
        // primary key constraints allowed on SESSION physical tables
        s
                .executeUpdate("CREATE TABLE SESSION.t1(c21 int not null, constraint pk primary key (c21))");
        // unique constraints allowed on SESSION physical tables
        s.executeUpdate("CREATE TABLE SESSION.t2(c21 int not null unique)");
        // check constraints allowed on SESSION physical tables
        s.executeUpdate("CREATE TABLE SESSION.t3(c21 int check (c21 > 0))");
        // foreign key constraints allowed on SESSION physical tables
        s.executeUpdate("CREATE TABLE t4(c11 int not null unique)");
        s.executeUpdate("CREATE TABLE SESSION.t5(c21 int references t4(c11))");
        // cleanUp
        s.executeUpdate("DROP TABLE SESSION.t1");
        s.executeUpdate("DROP TABLE SESSION.t2");
        s.executeUpdate("DROP TABLE SESSION.t3");
        s.executeUpdate("DROP TABLE SESSION.t5");
        s.executeUpdate("DROP TABLE t4");
    }

    /**
     * Test declared temporary table with ON COMMIT DELETE ROWS with and without
     * open cursors. Tests with holdable cursor are in a different class since
     * holdability support is only under jdk14 and higher.
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testOnCommiDeleteRowsWithAndWithoutOpenCursors()
            throws SQLException {
        Statement s = createStatement();
        // Temp table t2 with not holdable cursor open on it. Data should get
        // deleted from t2 at commit time
        assertUpdateCount(s, 0, "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(22, 22)");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(23, 23)");
        ResultSet rs2 = s.executeQuery("select count(*) from SESSION.t2");
        // Before commit t2 has 2 columns.
        JDBC.assertSingleValueResultSet(rs2 , "2");
        // eventhough this cursor is open, it is not a hold cursor. Commit
        // should delete the rows
        rs2 = s.executeQuery("select * from SESSION.t2");
        rs2.next();
        // Temp table t3 with no open cursors of any kind on it. Data should get
        // deleted from t3 at commit time
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c31 int, c32 int) on commit delete rows not logged");
        assertUpdateCount(s , 1 , "insert into SESSION.t3 values(32, 32)");
        assertUpdateCount(s , 1 , "insert into SESSION.t3 values(33, 33)");
        ResultSet rs3 = s.executeQuery("select count(*) from SESSION.t3");
        // Before commit t3 has 2 columns.
        JDBC.assertSingleValueResultSet(rs3 , "2");
        // commiting the above statements
	commit();
        // The 2 rows from t2 got deleted
        rs2 = s.executeQuery("select count(*) from SESSION.t2");
        JDBC.assertSingleValueResultSet(rs2 , "0");
        // The 2 rows from t3 got deleted
        rs3 = s.executeQuery("select count(*) from SESSION.t3");
        JDBC.assertSingleValueResultSet(rs3 , "0");
        s.executeUpdate("DROP TABLE SESSION.t2");
        s.executeUpdate("DROP TABLE SESSION.t3");
    }
    /**
     * Declare a temporary table with ON COMMIT PRESERVE ROWS with and without
     * open cursors. Tests with holdable cursor are in a different class since
     * holdability support is only under jdk14 and higher.
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testOnCommitPreserveRowsWithAndWithoutOpenCursors()
            throws SQLException {
        Statement s = createStatement();
        // Temp table t2 with not holdable cursor open on it. Data should be
        // preserved, holdability shouldn't matter
        s
                .executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");
        s.executeUpdate("insert into SESSION.t2 values(22, 22)");
        s.executeUpdate("insert into SESSION.t2 values(23, 23)");
        ResultSet rs2 = s.executeQuery("select count(*) from SESSION.t2");
        // t2 has 2 rows before commiting.
        JDBC.assertSingleValueResultSet(rs2 , "2");
        // eventhough this cursor is open, it is not a hold cursor.
        rs2 = s.executeQuery("select * from SESSION.t2");
        rs2.next();
        // Temp table t3 with no open cursors of any kind on it. Data should be
        // preserved, holdability shouldn't matter
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c31 int, c32 int) on commit preserve rows not logged");
        s.executeUpdate("insert into SESSION.t3 values(32, 32)");
        s.executeUpdate("insert into SESSION.t3 values(33, 33)");
        ResultSet rs3 = s.executeQuery("select count(*) from SESSION.t3");
        // t3 has 2 rows before commiting.
        JDBC.assertSingleValueResultSet(rs3 , "2");
        // commit point
        commit();
        rs2 = s.executeQuery("select count(*) from SESSION.t2");
        // The rows in t2 got preserved
        JDBC.assertSingleValueResultSet(rs2 , "2");
        rs3 = s.executeQuery("select count(*) from SESSION.t3");
        // The rows in t3 got preserved
        JDBC.assertSingleValueResultSet(rs3 , "2");

        s.executeUpdate("DROP TABLE SESSION.t2");
        s.executeUpdate("DROP TABLE SESSION.t3");
    }

    /**
     * Test that We can't create the temp table twice and we can'd drop a temp
     * table that doesn't wxist.
     * 
     * @throws SQLException
     */
    public void testDuplicateAndNullGTT() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
        // temp table t2 already exists.
        assertStatementError(
                "X0Y32",
                s,
                "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) not logged on commit preserve rows");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        // t2 has already been dropped.
        assertStatementError("42Y55", s, "DROP TABLE SESSION.t2");
    }

    /**
     * Test That Insert command allowed on temp tables in various conditions.
     * 
     * @throws SQLException
     */
    public void testInsertOnGTT() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 char(2)) on commit delete rows not logged");
        // Regular Insertion - Inserting with values.
        assertUpdateCount(s, 1, "insert into SESSION.t2 values (1, 'aa')");
        assertUpdateCount(s, 3, "insert into SESSION.t2 values (2, 'bb'),(3, 'cc'),(4, null)");
        assertUpdateCount(s, 0, "CREATE TABLE t1(c11 int, c22 char(2))");
        assertUpdateCount(s, 3, "insert into t1 values (5, null),(6, null),(7, 'gg')");
        // Insert into a table values selected from the Other table.
        assertUpdateCount(s, 3, "insert into SESSION.t2 (select * from t1 where c11>4)");
        assertUpdateCount(s, 7, "insert into SESSION.t2 select * from SESSION.t2");
        ResultSet rs1 = s.executeQuery("select sum(c21) from SESSION.t2");
	JDBC.assertSingleValueResultSet(rs1 , "56");
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c21 int, c22 char(2) not null) on commit delete rows not logged");
        assertUpdateCount(s , 1 , "insert into SESSION.t3 values (1, 'aa')");
        // test inserting a null value into a non null column
        assertStatementError("23502", s, "insert into SESSION.t3 values (2, null)");
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t4(c21 int, c22 char(2) default 'aa', c23 varchar(20) default user ) on commit delete rows not logged");
        assertUpdateCount(s, 1, "insert into SESSION.t4 values (1, 'aa', null)");
        // Inserting into a table of which some columns have default values.
        assertUpdateCount(s, 1, "insert into SESSION.t4(c21) values (2)");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t4");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t3");
        assertUpdateCount(s , 0 , "DROP TABLE t1");
    }

    /**
     * Test Delete Operation on temp tables.
     * 
     * @throws SQLException
     */
    public void testDeleteOnGTT() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 decimal) not logged");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(1, 1.1)");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(2, 2.2)");
        ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
        // Initially t2 has 2 rows
        JDBC.assertSingleValueResultSet(rs1 , "2");
        assertUpdateCount(s , 2 , "DELETE FROM SESSION.t2 where c21 > 0");
        rs1 = s.executeQuery("select count(*) from SESSION.t2");
        // After deletion t2 has nothing
        JDBC.assertSingleValueResultSet(rs1 , "0");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
    }

    /**
     * Test Update on temp tables in various ways.
     * 
     * @throws SQLException
     */
    public void testUpdateOnGTT() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(1, 1)");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(2, 1)");
        ResultSet rs2 = s.executeQuery("select count(*) from SESSION.t2 where c22 = 1");
        JDBC.assertSingleValueResultSet(rs2 , "2");
        assertUpdateCount(s , 2 , "UPDATE SESSION.t2 SET c22 = 2 where c21>0");
        rs2 = s.executeQuery("select count(*) from SESSION.t2 where c22 = 1");
        JDBC.assertSingleValueResultSet(rs2 , "0");
        rs2 = s.executeQuery("select count(*) from SESSION.t2 where c22 = 2");
        JDBC.assertSingleValueResultSet(rs2 , "2");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
    }

    /**
     * Test CREATE and DROP operations on SESSION schema
     * 
     * @throws SQLException
     */
    public void testSESSIONschema() throws SQLException {
        Statement s = createStatement();
        // SESSION schema can be created like any other schema
        try {

            s.executeUpdate("CREATE SCHEMA SESSION");
        } catch (SQLException e) {
            assertSQLState("X0Y68", e);
        }
        // SESSION schema can be dropped like any Other Schema
        assertUpdateCount(s , 0 , "DROP SCHEMA SESSION restrict");
        // We can't drop the In-Memory SESSION schema
        assertStatementError("42Y07", s, "DROP SCHEMA SESSION restrict");
    }
    /**
     * CREATE VIEW in SESSION schema referencing a table outside of SESSION
     * schema.
     * 
     * @throws SQLException
     */
    public void testCreateView() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "CREATE TABLE t2 (c28 int)");
        assertUpdateCount(s , 2 , "INSERT INTO t2 VALUES (280),(281)");
        // Create a VIEW in SESSION schema referencing a table outside of
        // SESSION schema
        assertUpdateCount(s, 0, "CREATE VIEW SESSION.t2v1 as select * from t2");
        // Drop the view.
        ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2v1");
        // Test the values in View as selected from Table.
        JDBC.assertSingleValueResultSet(rs1 , "2");
        assertUpdateCount(s , 0 , "DROP VIEW SESSION.t2v1");
        assertUpdateCount(s , 0 , "DROP TABLE t2");
    }
    /**
     * Multiple tests to make sure we do not do statement caching for statement
     * referencing SESSION schema tables. CREATE physical table and then DECLARE
     * GLOBAL TEMPORARY TABLE with the same name in session schema.
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testStatementCaching() throws SQLException {
        Statement s = createStatement();
        // Need to do following 3 in autocommit mode otherwise the data
        // dictionary will be in write mode and statements won't get
        // cached. I need to have statement caching enabled here to make sure
        // that tables with same names do not conflict
        getConnection().setAutoCommit(true);
        try {
            s.executeUpdate("CREATE schema SESSION");
        } catch (SQLException e) {
            assertSQLState("X0Y68", e);
        }
        assertUpdateCount(s , 0 , "CREATE TABLE SESSION.t2 (c21 int)");
        assertUpdateCount(s, 1, "INSERT into SESSION.t2 values(21)");
        getConnection().setAutoCommit(false);
        // select will return data from physical table t2
        ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
        JDBC.assertSingleValueResultSet(rs1 , "1");
        // declare temporary table with same name as a physical table in SESSION
        // schema
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
        assertUpdateCount(s , 1 , "INSERT into SESSION.t2 values(22, 22)");
        assertUpdateCount(s , 1 , "INSERT into SESSION.t2 values(23, 23)");
        // select will return data from temp table t2
        rs1 = s.executeQuery("select count(*) from SESSION.t2");
        JDBC.assertSingleValueResultSet(rs1 , "2");
        // drop the temp table t2
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        // select will return data from physical table t2 because temp table has
        // been deleted
        rs1 = s.executeQuery("select count(*) from SESSION.t2");
        JDBC.assertSingleValueResultSet(rs1 , "1");
	// cleanup
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        assertUpdateCount(s , 0 , "drop schema SESSION restrict");
    }

    /**
     * After creating SESSION schema and making it current schema, temporary
     * tables should not require SESSION qualification
     * 
     * @exception SQLException
     */
    public void testSESSIONQualifier() throws SQLException {
        Statement s = createStatement();
        // We have to qualify the temp tables with SESSION qualifier.
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(21, 21)");
        assertStatementError("42X05", s, "insert into t2 values(23, 23)");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(22, 22)");
        ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
        JDBC.assertSingleValueResultSet(rs1 , "2");
        // Create the SESSION Schema Manually
        try {
            s.executeUpdate("CREATE SCHEMA SESSION");
        } catch (SQLException e) {
            assertSQLState("X0Y68", e);
        }
        // Set the SESSION schema as current schema
        s.executeUpdate("SET SCHEMA SESSION");
        // we don't need the SESSION qualifier for referencing temp tables.
        rs1 = s.executeQuery("select count(*) from t2");
        JDBC.assertSingleValueResultSet(rs1 , "2");
        assertUpdateCount(s , 0 , "DROP TABLE t2");
        assertUpdateCount(s , 0 , "SET SCHEMA APP");
        assertUpdateCount(s , 0 , "drop schema SESSION restrict");
    }
    /**
     * Temporary table created in one connection should not be available in
     * another connection.
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testOneGTTInConnection() throws SQLException {
        Statement s1 = createStatement();
        // creating the temp table in connection 1
        assertUpdateCount(s1 , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
        assertUpdateCount(s1, 1, "insert into SESSION.t2 values(22, 22)");
        // Getting the Second Connection
        Connection con2 = openDefaultConnection();
        Statement s2 = con2.createStatement();
        // con2 should not find temp table declared in con1
        assertStatementError("42X05", s2, "select count(*) from SESSION.t2");
        // connection1 will be closed in tearDown() methos automatically.
        s2.close();
        con2.close();
    }

    /**
     * Temp table in one connection should not conflict with temp table with
     * same name in another connection.
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testSameGTTNameForTwoConnections() throws SQLException {
        Statement s1 = createStatement();
        // creating the temp table t2 in First Connection
        assertUpdateCount(s1 , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
        assertUpdateCount(s1, 1, "insert into SESSION.t2 values(22, 22)");
        // Getting the Second Connection.
        Connection con2 = openDefaultConnection();
        Statement s2 = con2.createStatement();
        // creating the temp table with same name (t2) in the Sesond Connection
	assertUpdateCount(s2 , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) not logged");
        assertUpdateCount(s2, 1, "insert into SESSION.t2 values(99)");
        // dropping temp table t2 defined for con1
        assertUpdateCount(s1 , 0 , "DROP TABLE SESSION.t2");
        // dropping temp table t2 defined for con2
        assertUpdateCount(s2 , 0 , "DROP TABLE SESSION.t2");
	s2.close();
        con2.close();
    }
    /**
     * Prepared statement test - drop the temp table underneath
     * 
     * @throws SQLException
     */
    public void testPreparedStatement1() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
        PreparedStatement pStmt = prepareStatement("insert into SESSION.t2 values (?, ?)");
        pStmt.setInt(1, 21);
        pStmt.setInt(2, 1);
        pStmt.execute();
        ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
        JDBC.assertSingleValueResultSet(rs1 , "1");
        // drop the temp table t2
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        // now try to insert into the table t2 which was dropped
        try {
            pStmt.setInt(1, 22);
            pStmt.setInt(2, 2);
            pStmt.execute();
            fail(" Table/View 'SESSION.T2' does not exist:");
        } catch (SQLException e) {
            assertSQLState("42X05", e);
        }
    }
    /**
     * Prepared statement test - drop and recreate the temp table with different
     * definition underneath
     * 
     * @throws SQLException
     */
    public void testPreparedStatement2() throws SQLException {
        Statement s = createStatement();
        // create the temporary table t2 with 2 columns.
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
        PreparedStatement pStmt = prepareStatement("insert into SESSION.t2 values (?, ?)");
        pStmt.setInt(1, 21);
        pStmt.setInt(2, 1);
        pStmt.execute();
        pStmt.close();
        ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
	rs1.next();
	ResultSetMetaData rsmd = rs1.getMetaData();
	assertEquals(2 , rsmd.getColumnCount());
 	//JDBC.assertSingleValueResultSet(rs1 , "1");
        // drop the temp table t2
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        // recreate the temp table t2 with 3 columns
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int, c23 int) not logged");
        pStmt = prepareStatement("insert into SESSION.t2 values (?, ? , ?)");
        pStmt.setInt(1, 22);
        pStmt.setInt(2, 2);
        pStmt.setNull(3, java.sql.Types.INTEGER);
        pStmt.execute();
        rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
	rsmd = rs1.getMetaData();
        assertEquals(3 , rsmd.getColumnCount());
        // drop the temp table t2
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        // recreate the temp table t2 with 4 columns.
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int, c23 int, c24 int not null) not logged");
        pStmt = prepareStatement("insert into SESSION.t2 values (?, ? , ? , ?)");
        // try to insert a null value into a non-null column
        try {
            pStmt.setInt(1, 22);
            pStmt.setInt(2, 2);
            pStmt.setNull(3, java.sql.Types.INTEGER);
            pStmt.setNull(4, java.sql.Types.INTEGER);
            pStmt.execute();
            fail("trying to Insert a null value into non null column:");
        } catch (SQLException e) {
            assertSQLState("23502", e);
        }
    }
    /**
     * Rollback behavior - declare temp table, rollback, select should fail.
     * 
     * @throws SQLException
     */
    public void testRollbackBehavior1() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
        PreparedStatement pStmt = prepareStatement("insert into SESSION.t2 values (?, ?)");
        pStmt.setInt(1, 21);
        pStmt.setInt(2, 1);
        pStmt.execute();
        pStmt.close();
        ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
        JDBC.assertSingleValueResultSet(rs1 , "1");
        // RollBack to the last committed point
        rollback();
        // Now select from SESSION.t2 should fail
        assertStatementError("42X05", s, "select * from SESSION.t2");
    }
    /**
     * Rollback behavior - declare temp table, commit, drop temp table,
     * rollback, select should pass
     * 
     * @throws SQLException
     */
    public void testRollbackBehavior2() throws SQLException {
        Statement s = createStatement();
        // create a temp table t2
    	assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");
        PreparedStatement pStmt = prepareStatement("insert into SESSION.t2 values (?, ?)");
        pStmt.setInt(1, 21);
        pStmt.setInt(2, 1);
        pStmt.execute();
        pStmt.close();
        // commit the changes
        commit();
        // After commiting drop the temp table t2
        s.executeUpdate("DROP TABLE SESSION.t2");
        // Rollback the last Operation that is the DROP TABBE SESSION.t2 operation
        rollback();
        // now select will pass
        ResultSet rs2 = s.executeQuery("select count(*) from SESSION.t2");
        JDBC.assertSingleValueResultSet(rs2 , "0");	
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        commit();
    }
    /**
     * Rollback behavior - create temp table , commit , drop it and create
     * another temp table with samename , rollback select will select from the
     * first temp table.
     * 
     * @throws SQLException
     */
    public void testRollbackBehavior3() throws SQLException {
        Statement s = createStatement();
        // create temp table t2 with 3 columns.
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int, c23 int) on commit preserve rows not logged");
        assertUpdateCount(s, 1, "insert into session.t2 values(1,1,1)");
        ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
	ResultSetMetaData rsmd = rs1.getMetaData();
        assertEquals(3 , rsmd.getColumnCount());
        // drop the temp table t2 with 3 columns.
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        assertStatementError("42X05", s, "select * from SESSION.t2");
        // create temp table with 2 columns.
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");
        assertUpdateCount(s, 1, "insert into session.t2 values(1,1)");
        rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
	rsmd = rs1.getMetaData();
        assertEquals(2 , rsmd.getColumnCount());
        // commit point
        commit();
        // drop the temp table with 2 columns.
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        // create the temp table with 1 column.
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
        assertUpdateCount(s, 1, "insert into session.t2 values(1)");
        rs1 = s.executeQuery("select * from SESSION.t2");
	rs1.next();
	rsmd = rs1.getMetaData();
        assertEquals(1 , rsmd.getColumnCount());
        rs1.close();
        // rollback to the last committed point
        rollback();
        // Now we have the temp table with 2 columns.
        rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
	rsmd = rs1.getMetaData();
        assertEquals(2 , rsmd.getColumnCount());
        rs1.close();
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
    }
    /**
     * Rollback behavior for tables touched with DML
     * 
     * @throws SQLException
     */
    public void testRollbackBehavior4() throws SQLException {
        Statement s = createStatement();
        // Declare temp table t2 & t3 & t4 & t5 with preserve rows, insert data
        // and commit
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged on rollback delete rows");
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c31 int, c32 int) not logged on commit preserve rows on rollback delete rows");
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t4(c41 int, c42 int) not logged on rollback delete rows on commit preserve rows");
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t5(c51 int, c52 int) on commit preserve rows not logged");
        s.executeUpdate("insert into session.t2 values(21,1)");
        s.executeUpdate("insert into session.t2 values(22,2)");
        s.executeUpdate("insert into session.t2 values(23,3)");
        s.executeUpdate("insert into session.t3 values(31,1)");
        s.executeUpdate("insert into session.t3 values(32,2)");
        s.executeUpdate("insert into session.t3 values(33,3)");
        s.executeUpdate("insert into session.t4 values(41,1)");
        s.executeUpdate("insert into session.t4 values(42,2)");
        s.executeUpdate("insert into session.t4 values(43,3)");
        s.executeUpdate("insert into session.t5 values(51,1)");
        s.executeUpdate("insert into session.t5 values(52,2)");
        s.executeUpdate("insert into session.t5 values(53,3)");
        // commit point
        commit();
        // create a temp table t6 with preserve rows , insert data.
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t6(c61 int, c62 int) on commit preserve rows not logged on rollback delete rows");
        s.executeUpdate("insert into session.t6 values(61,1)");
        s.executeUpdate("insert into session.t6 values(62,2)");
        s.executeUpdate("insert into session.t6 values(63,3)");
        // DML Operations.
        s.executeUpdate("DELETE FROM session.t2 WHERE c22> (select c52 from session.t5 where c52=2)");
        s.executeUpdate("DELETE FROM session.t3 WHERE c32>3");
        // rollback to the last commit point
        rollback();
        // After rollback t2 should have nothing.
        ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
	JDBC.assertSingleValueResultSet(rs1 , "0");
        // temp table t3 should have no rows because attempt was made to delete
        // from it even though nothing actually got deleted from it in the transaction
        rs1 = s.executeQuery("select count(*) from SESSION.t3");
	JDBC.assertSingleValueResultSet(rs1 , "0");
        // temp table t4 should have its data intact because it was not touched
        // in the transaction that got rolled back
        rs1 = s.executeQuery("select count(*) from SESSION.t4");
	JDBC.assertSingleValueResultSet(rs1 , "3");
        // temp table t5 should have its data intact because it was only used in
        // where clause and not touched in the transaction that got rolled back
        rs1 = s.executeQuery("select count(*) from SESSION.t5");
	JDBC.assertSingleValueResultSet(rs1 , "3");
        // temp table t6 got dropped as part of rollback of this transaction
        // since it was declared in this same transaction
        assertStatementError("42X05", s, "select * from SESSION.t6");
        // CleanUp
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t3");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t4");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t5");
    }
    /**
     * The Test Verifies that there is no entry in system catalogs for temporary
     * tables. while physical tables from SESSION schema have an entry.
     * 
     * @throws SQLException
     */
    public void testEntryForSESSIONTablesToSysCatalog() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
       ResultSet rs1 = s.executeQuery("select count(*) from sys.systables where CAST(tablename AS VARCHAR(128)) like 'T2'");
	JDBC.assertSingleValueResultSet(rs1 , "0");
        // drop the temp table t2
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        // create a physical table in SESSION schema
        try {
            s.executeUpdate("CREATE SCHEMA SESSION");
        } catch (SQLException e) {
            assertSQLState("X0Y68", e);
        }
        assertUpdateCount(s , 0 , "CREATE TABLE SESSION.t2(c21 int, c22 int)");
        rs1 = s.executeQuery("select count(*) from sys.systables where CAST(tablename AS VARCHAR(128)) like 'T2'");
        // System Catalog contains the physical tables from SESSION schema.
	JDBC.assertSingleValueResultSet(rs1 , "1");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        assertUpdateCount(s , 0 , "drop schema SESSION restrict");
    }
    /**
     * Verify that there is no entry in system catalogs for SESSION schmea after
     * declare table.
     * 
     * @throws SQLException
     */
    public void testEntryForSEESSIONtoSysCatalog() throws SQLException {
        Statement s = createStatement();
        try {
            s.executeUpdate("drop schema SESSION restrict");
        } catch (SQLException e) {
            assertSQLState("42Y07", e);
        }
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
        ResultSet rs1 = s.executeQuery("select count(schemaname) from sys.sysschemas where CAST(schemaname AS VARCHAR(128)) like 'SESSION'");
	JDBC.assertSingleValueResultSet(rs1 , "0");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
    }
    /**
     * DatabaseMetaData.getTables() should not return temporary tables
     * 
     * @throws SQLException
     */
    public void testDatabaseMetadata() throws SQLException {
        Statement s = createStatement();
        int count = 0;
        DatabaseMetaData databaseMetaData;
        databaseMetaData = getConnection().getMetaData();
        try {
            s.executeUpdate("CREATE SCHEMA SESSION");
        } catch (SQLException e) {
            assertSQLState("X0Y68", e);
        }
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
        assertUpdateCount(s , 0 , "CREATE TABLE SESSION.t3(c31 int, c32 int)");
        ResultSet rs1 = databaseMetaData.getTables("", null, "%", null);
        while (rs1.next()) {
            if (("T2" == rs1.getString(3)) && ("SESSION" == rs1.getString(2)))
                fail("Temporary table Found");
            count++;
        }
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t3");
        assertUpdateCount(s , 0 , "drop schema SESSION restrict");
    }
    /**
     * Test for delete where current of on temporary tables
     * 
     * @throws SQLException
     */
    public void testDeleteWhereCurrentOfOnGTT() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(21, 1)");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(22, 1)");
        ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
	JDBC.assertSingleValueResultSet(rs1 , "2");
        PreparedStatement pStmt1 = prepareStatement("select c21 from session.t2 for update");
        ResultSet rs2 = pStmt1.executeQuery();
        rs2.next();
        PreparedStatement pStmt2 = prepareStatement("delete from session.t2 where current of "+ rs2.getCursorName());
        pStmt2.executeUpdate();
        rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
        assertEquals(22, rs1.getInt(1));
        assertEquals(1, rs1.getInt(2));
        rs2.next();
        pStmt2.executeUpdate();
        rs1 = s.executeQuery("select count(*) from SESSION.t2");
        rs1.next();
        assertEquals(0, rs1.getInt(1));
        rs2.close();
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
    }
    /**
     * Test for update where current of on temporary tables
     * 
     * @throws SQLException
     */
    public void UpdateWhereCurrentOfOnGTT() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(21, 1)");
        assertUpdateCount(s , 1 , "insert into SESSION.t2 values(22, 1)");
        ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
	JDBC.assertSingleValueResultSet(rs1 , "2");
        PreparedStatement pStmt1 = prepareStatement("select c21 from session.t2 for update");
        ResultSet rs2 = pStmt1.executeQuery();
        rs2.next();
        PreparedStatement pStmt2 = prepareStatement("update session.t2 set c22 = 2 where current of "+ rs2.getCursorName());
        pStmt2.executeUpdate();
        rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
        assertEquals(21, rs1.getInt(1));
        assertEquals(2, rs1.getInt(2));
        rs1.next();
        assertEquals(22, rs1.getInt(1));
        assertEquals(1, rs1.getInt(2));
        rs2.next();
        pStmt2.executeUpdate();
        rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
        assertEquals(21, rs1.getInt(1));
        assertEquals(2, rs1.getInt(2));
        rs1.next();
        assertEquals(22, rs1.getInt(1));
        assertEquals(2, rs1.getInt(2));
        rs2.close();
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
    }
    /**
     * Prepared statement test - DML and rollback behavior
     * 
     * @throws SQLException
     */
    public void testDMLRollback1() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged on commit preserve rows");
        PreparedStatement pStmt = prepareStatement("insert into SESSION.t2 values (?, ?)");
        pStmt.setInt(1, 21);
        pStmt.setInt(2, 1);
        pStmt.execute();
        commit();
        ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
        assertEquals(21, rs1.getInt(1));
        assertEquals(1, rs1.getInt(2));
        pStmt.setInt(1, 22);
        pStmt.setInt(2, 2);
        pStmt.execute();
        rollback();
        rs1 = s.executeQuery("select count(*) from SESSION.t2");
        rs1.next();
        assertEquals(0, rs1.getInt(1));
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
    }
    /**
     * Prepared statement test - DML and rollback behavior.
     * 
     * @exception SQLException
     *                Thrown if some unexpected error happens
     */
    public void testDMLRollback2() throws SQLException {
        Statement s = createStatement();
        assertUpdateCount(s , 0 , "DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged on commit preserve rows");
        assertUpdateCount(s , 1 , "INSERT INTO SESSION.t2 VALUES(21, 1)");
        commit();
        ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
        assertEquals(21, rs1.getInt(1));
        assertEquals(1, rs1.getInt(2));
        prepareStatement("insert into SESSION.t2 values (?, ?)");
        rollback();
        rs1 = s.executeQuery("select * from SESSION.t2");
        rs1.next();
        assertEquals(21, rs1.getInt(1));
        assertEquals(1, rs1.getInt(2));
        assertUpdateCount(s , 0 , "DROP TABLE SESSION.t2");
    }
    /**
     * 
     * A Utility method that deletes all the SESSION schema tables before each fixture.
     *
     * @exception SQLException
     */
    public void dropSchemaTables() throws SQLException {
        Statement s = createStatement();
        try {
            s.executeUpdate("DROP TABLE SESSION.t1");
        } catch (SQLException e) {
        }
        try {
            s.executeUpdate("DROP TABLE SESSION.t2");
        } catch (SQLException e) {
        }
        try {
            s.executeUpdate("DROP TABLE SESSION.t3");
        } catch (SQLException e) {
        }
        try {
            s.executeUpdate("DROP TABLE SESSION.t4");
        } catch (SQLException e) {
        }
        try {
            s.executeUpdate("DROP TABLE SESSION.t5");
        } catch (SQLException e) {
        }
    }
}

