/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.InsertTest

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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This class contains test cases for the INSERT statement.
 */
public class InsertTest extends BaseJDBCTestCase {

    private static final String PARAMETER_IN_SELECT_LIST = "42X34";
    private static final String TOO_MANY_RESULT_COLUMNS = "42X06";

    public InsertTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(InsertTest.class);
    }

    //DERBY-6788(Wrong value inserted by INSERT INTO with multiple subselects)
    //Following test case has zz against its name and hence it won't run. 
    // This is because one INSERT with JOIN in the test can cause data .  
    // corruption. Once DERBY-6788 is fixed, following test should be enabled 
    // by removing zz. This bug might be related to 
    // DERBY-6786(NullPointerException in INSERT INTO statement with multiple 
    //  subselects)
    public void zztestDerby6788() throws SQLException {
        Statement s = createStatement();
        s.execute("CREATE TABLE M1 (K varchar(64), S decimal)");
        s.execute("CREATE TABLE M2 (K varchar(64), S decimal)");
        s.execute("CREATE TABLE V  (S DECIMAL)");
        s.execute("INSERT INTO M1 VALUES ('Bug', 2015)");
        s.execute("INSERT INTO M2 VALUES ('Bug', 1957)");
        JDBC.assertFullResultSet(
            s.executeQuery(
            "SELECT res.* FROM (SELECT d2.s FROM m1 "+
            "LEFT JOIN " +
            "(SELECT k,s FROM m2) AS d2 ON m1.k=d2.k) AS res"),
            new String[][]{{"1957"}});
        //The INSERT below will insert incorrect value into V because of
        // DERBY-6788. This bug might be related to 
        // DERBY-6786(NullPointerException in INSERT INTO statement with  
        //  multiple subselects)
        s.execute("INSERT INTO V "+
                "(SELECT res.* FROM (SELECT d2.s FROM m1 " +
        		"LEFT JOIN " +
                "(SELECT k,s FROM m2) AS d2 ON m1.k=d2.k) AS res)");
        JDBC.assertFullResultSet(
                s.executeQuery(
                "SELECT * FROM V"),
                new String[][]{{"1957"}});
        s.execute("INSERT INTO V "+
                "(SELECT res.* FROM (SELECT d2.s*1 FROM m1 " +
        		"LEFT JOIN " +
                "(SELECT k,s FROM m2) AS d2 ON m1.k=d2.k) AS res)");
        JDBC.assertFullResultSet(
                s.executeQuery(
                "SELECT * FROM V"),
                new String[][]{{"1957"},{"1957"}});
        s.execute("DROP   TABLE M1");
        s.execute("DROP   TABLE M2");
        s.execute("DROP   TABLE V");
    }
    
    public void testDerby6786Case1() throws SQLException {
        Statement s = createStatement();
        s.execute("CREATE TABLE K1 (K VARCHAR(64), S DECIMAL)");
        s.execute("CREATE TABLE K2 (K VARCHAR(64), S DECIMAL)");
        s.execute("CREATE TABLE T  (S DECIMAL)");
        s.execute("INSERT INTO K1 VALUES ('110007', 224)");
        s.execute("INSERT INTO K2 VALUES ('110007', 361)");
        //Run just plain select
        derby6786QueriesToRun(s, "");
        //Run insert into ... select
        derby6786QueriesToRun(s, "insert into t ");
        s.execute("DROP   TABLE K1");
        s.execute("DROP   TABLE K2");
        s.execute("DROP   TABLE T");
    }

    public void testDerby6786InsertIntoSelectCase2() throws SQLException {
        Statement s = createStatement();
        s.execute("CREATE TABLE K1 (K VARCHAR(64), S DECIMAL)");
        s.execute("CREATE TABLE K2 (K VARCHAR(64), S DECIMAL)");
        s.execute("CREATE TABLE T  (S DECIMAL)");
        s.execute("INSERT INTO K1 VALUES ('110007', 224)");
        s.execute("INSERT INTO K2 VALUES ('110007', null)");
        //Run just plain select
        derby6786QueriesToRun(s, "");
        //Run insert into ... select
        derby6786QueriesToRun(s, "insert into t ");
        s.execute("DROP   TABLE K1");
        s.execute("DROP   TABLE K2");
        s.execute("DROP   TABLE T");
    }

    //DERBY-6786(NullPointerException in INSERT INTO statement with multiple subselects)
    //Following test case has zz against its name and hence it won't run. 
    // This is because some queries in the test can cause NPE. Once 
    // DERBY-6786 is fixed, following test should be enabled by removing
    // zz.
    public void zztestDerby6786InsertIntoSelectCase3() throws SQLException {
        Statement s = createStatement();
        s.execute("CREATE TABLE K1 (K VARCHAR(64), S DECIMAL)");
        s.execute("CREATE TABLE K2 (K VARCHAR(64), S DECIMAL)");
        s.execute("CREATE TABLE T  (S DECIMAL)");
        s.execute("INSERT INTO K1 VALUES ('110007', 224)");
        s.execute("INSERT INTO K2 VALUES ('110019', null)");
        //Run just plain select
        derby6786QueriesToRun(s, "");
        //Run insert into ... select. Running insert will into can result into
        // NPE for some of the queries until DERBY-6786 is fixed.
        derby6786QueriesToRun(s, "insert into t ");
        s.execute("DROP   TABLE K1");
        s.execute("DROP   TABLE K2");
        s.execute("DROP   TABLE T");
    }
    
    private void derby6786QueriesToRun(Statement s, String insertInto) throws SQLException {
        //following left join works
        s.execute(insertInto +
            "select erg.* from ( " +
            "select d1.s from (select k,s from k1) as d1 " +
            "left join "+
            "(select k,s from k2) as d2 on d1.k=d2.k" +
            ") as erg " +
            "where s > 10");
        //DERBY-6786 : following left join can fail if the right table  
        // does not have a matching row
        s.execute(insertInto +
                "select erg.* from ( " +
                "select d2.s from (select k,s from k1) as d1 " +
                "left join "+
                "(select k,s from k2) as d2 on d1.k=d2.k" +
                ") as erg " +
                "where s > 10");
        //DERBY-6786 : following is another example of left join that can fail 
        // if the right table does not have a matching row
        s.execute(insertInto +
                "select erg.* from ( " +
                "select d2.s from k1 " +
                "left join "+
                "(select k,s from k2) as d2 on k1.k=d2.k" +
                ") as erg " +
                "where s > 10");
        //DERBY-6786 : following right join can fail if the left table  
        // does not have a matching row
        s.execute(insertInto +
            "select erg.* from ( " +
            "select d1.s from (select k,s from k1) as d1 " +
            "right join "+
            "(select k,s from k2) as d2 on d1.k=d2.k" +
            ") as erg " +
            "where s > 10");
        //following right join works
        s.execute(insertInto +
                "select erg.* from ( " +
                "select d2.s from (select k,s from k1) as d1 " +
                "right join "+
                "(select k,s from k2) as d2 on d1.k=d2.k" +
                ") as erg " +
                "where s > 10");

    }
    
    /**
     * Regression test case for DERBY-4348 where an INSERT INTO .. SELECT FROM
     * statement would result in a LONG VARCHAR column becoming populated with
     * the wrong values.
     */
    public void testInsertIntoSelectFromWithLongVarchar() throws SQLException {
        // Generate the data that we want table T2 to hold when the test
        // completes.
        String[][] data = new String[100][2];
        for (int i = 0; i < data.length; i++) {
            // first column should have integers 0,1,...,99
            data[i][0] = Integer.toString(i);
            // second column should always be -1
            data[i][1] = "-1";
        }

        // Turn off auto-commit so that the tables used in the test are
        // automatically cleaned up in tearDown().
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table t1(a long varchar)");

        // Fill table T1 with the values we want to see in T2's first column.
        PreparedStatement insT1 = prepareStatement("insert into t1 values ?");
        for (int i = 0; i < data.length; i++) {
            insT1.setString(1, data[i][0]);
            insT1.executeUpdate();
        }

        // Create table T2 and insert the contents of T1. Column B must have
        // a default value and a NOT NULL constraint in order to expose
        // DERBY-4348. The presence of NOT NULL makes the INSERT statement use
        // a NormalizeResultSet, and the bug was caused by a bug in the
        // normalization.
        s.execute("create table t2(a long varchar, b int default -1 not null)");
        s.execute("insert into t2(a) select * from t1");

        // Verify that T1 contains the expected values. Use an ORDER BY to
        // guarantee the same ordering as in data[][].
        JDBC.assertFullResultSet(s.executeQuery(
                    "select * from t2 order by int(cast (a as varchar(10)))"),
                data);
    }

    /**
     * INSERT used to fail with a NullPointerException if the source was an
     * EXCEPT operation or an INTERSECT operation. DERBY-4420.
     */
    public void testInsertFromExceptOrIntersect() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        // Create tables to fetch data from
        s.execute("create table t1(x int)");
        s.execute("insert into t1 values 1,2,3");
        s.execute("create table t2(x int)");
        s.execute("insert into t2 values 2,3,4");

        // Create table to insert into
        s.execute("create table t3(x int)");

        // INTERSECT (used to cause NullPointerException)
        s.execute("insert into t3 select * from t1 intersect select * from t2");
        JDBC.assertFullResultSet(
                s.executeQuery("select * from t3 order by x"),
                new String[][]{{"2"}, {"3"}});
        s.execute("delete from t3");

        // INTERSECT ALL (used to cause NullPointerException)
        s.execute("insert into t3 select * from t1 " +
                  "intersect all select * from t2");
        JDBC.assertFullResultSet(
                s.executeQuery("select * from t3 order by x"),
                new String[][]{{"2"}, {"3"}});
        s.execute("delete from t3");

        // EXCEPT (used to cause NullPointerException)
        s.execute("insert into t3 select * from t1 except select * from t2");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t3 order by x"),
                "1");
        s.execute("delete from t3");

        // EXCEPT ALL (used to cause NullPointerException)
        s.execute("insert into t3 select * from t1 " +
                  "except all select * from t2");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t3 order by x"),
                "1");
        s.execute("delete from t3");
    }

    /**
     * Regression test for DERBY-4671. Verify that dynamic parameters can be
     * used in the select list in an INSERT INTO ... SELECT FROM statement.
     * This used to work, but the fix for DERBY-4420 made it throw a
     * NullPointerException.
     */
    public void testInsertFromSelectWithParameters() throws SQLException {
        Statement s = createStatement();
        s.execute("create table derby4671(x int)");
        s.execute("insert into derby4671 values (1), (2)");

        // This call failed with a NullPointerException
        PreparedStatement ins1 = prepareStatement(
                "insert into derby4671 select ? from derby4671");

        ins1.setInt(1, 7);
        assertUpdateCount(ins1, 2);

        JDBC.assertFullResultSet(
                s.executeQuery("select * from derby4671 order by x"),
                new String[][] {{"1"}, {"2"}, {"7"}, {"7"}});

        // Also verify that it works when the ? is in an expression
        PreparedStatement ins2 = prepareStatement(
                "insert into derby4671 select (x+?)*10 from derby4671");

        ins2.setInt(1, 77);
        assertUpdateCount(ins2, 4);

        JDBC.assertFullResultSet(
                s.executeQuery("select * from derby4671 order by x"),
                new String[][] {
                    {"1"}, {"2"}, {"7"}, {"7"},
                    {"780"}, {"790"}, {"840"}, {"840"}});

        // We only accept ? in the top level select list, so these should
        // still fail
        assertCompileError(
                PARAMETER_IN_SELECT_LIST,
                "insert into derby4671 select ? from derby4671 "
                + "union select ? from derby4671");
        assertCompileError(
                PARAMETER_IN_SELECT_LIST,
                "insert into derby4671 select ? from derby4671 "
                + "except select ? from derby4671");
        assertCompileError(
                PARAMETER_IN_SELECT_LIST,
                "insert into derby4671 select ? from derby4671 "
                + "intersect select ? from derby4671");
    }

    /**
     * Regression test case for DERBY-4449. INSERT statements with an explicit
     * target column list used to fail with ArrayIndexOutOfBoundsException if
     * the table constructor had more columns than the target column list and
     * one of the extra columns was specified as DEFAULT.
     */
    public void testInsertTooManyDefaultColumns() throws SQLException {
        createStatement().execute("create table derby4449(x int)");
        // This statement has always failed gracefully (no explicit target
        // column list)
        assertCompileError(
                TOO_MANY_RESULT_COLUMNS,
                "insert into derby4449 values (default, default)");
        // This statement used to fail with ArrayIndexOutOfBoundsException
        assertCompileError(
                TOO_MANY_RESULT_COLUMNS,
                "insert into derby4449 (x) values (default, default)");
    }

    /**
     * Regression test case for DERBY-6443. INSERT statements bind the
     * source SELECT statement twice, and the second time it would miss
     * aggregates and subqueries if they were wrapped in a function call.
     * This led to inconsistencies in the query tree that caused errors
     * during execution (or assertion failures during compilation in sane
     * builds).
     */
    public void testDerby6443() throws SQLException {
        Statement s = createStatement();

        // Disable auto-commit for easy cleanup of test tables (automatically
        // rolled back in tearDown()), and create a separate schema to avoid
        // name conflicts with other test cases.
        setAutoCommit(false);
        s.execute("CREATE SCHEMA d6443");
        s.execute("SET SCHEMA d6443");

        // This is the original test case provided in the bug report. It
        // used to fail with an assert failure when compiling the trigger
        // (in sane builds), or with an ArrayIndexOutOfBoundsException when
        // the trigger fired (in insane builds).
        s.execute("CREATE TABLE foo (name VARCHAR(20), val DOUBLE)");
        s.execute("CREATE TABLE summary "
                + "(name VARCHAR(20), aver DOUBLE, size INT)");
        s.execute("CREATE TRIGGER trg_foo AFTER INSERT ON foo "
                + "REFERENCING NEW TABLE AS changed FOR EACH STATEMENT "
                + "INSERT INTO summary (name, aver, size) "
                + "SELECT name, FLOOR(AVG(LOG10(val))), COUNT(*) "
                + "FROM changed "
                + "GROUP BY name");
        s.execute("INSERT INTO foo (name, val) "
                + "VALUES ('A', 10), ('A', 20), ('B', 30), ('C', 40)");
        JDBC.assertFullResultSet(
                s.executeQuery("select * from foo order by val"),
                new String[][] {
                    { "A", "10.0" },
                    { "A", "20.0" },
                    { "B", "30.0" },
                    { "C", "40.0" },
                });
        JDBC.assertFullResultSet(
                s.executeQuery("select * from summary order by name"),
                new String[][] {
                    { "A", "1.0", "2" },
                    { "B", "1.0", "1" },
                    { "C", "1.0", "1" },
                });

        // Some other problematic queries...

        s.execute("create table t1(x int)");
        s.execute("insert into t1 values 1");
        s.execute("create table t2(x int)");

        // Used to fail with assert or ArrayIndexOutOfBoundsException.
        s.execute("insert into t2 select floor(avg(x)) from t1");

        // Same here...
        s.execute("create function f(x int) returns int language java "
                + "parameter style java external name 'java.lang.Math.abs'");
        s.execute("insert into t2 select f(avg(x)) from t1");

        // This query used to fail with a NullPointerException.
        s.execute("insert into t2 select f((select x from t1)) from t1");

        JDBC.assertFullResultSet(
                s.executeQuery("select * from t2"),
                new String[][] {{"1"}, {"1"}, {"1"}});
    }
}
