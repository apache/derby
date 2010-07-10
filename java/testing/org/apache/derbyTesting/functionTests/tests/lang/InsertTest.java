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
}
