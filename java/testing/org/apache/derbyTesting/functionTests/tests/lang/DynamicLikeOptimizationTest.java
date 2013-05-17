/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.DynamicLikeOptimizationTest
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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the dynamic like optimization.
 *
 * <p><b>NOTE:</b> the metadata test does a bunch of likes with parameters.
 */
public class DynamicLikeOptimizationTest extends BaseJDBCTestCase {
    /** All rows in the cei table. */
    private static final Object[][] CEI_ROWS = {
        { new Integer(0), "Alarms", "AlarmDisk999" },
        { new Integer(1), "Alarms", "AlarmFS-usr" },
        { new Integer(2), "Alarms", "AlarmPower" },
        { new Integer(3), "Alert", "AlertBattery" },
        { new Integer(4), "Alert", "AlertUPS" },
        { new Integer(5), "Warning", "WarnIntrusion" },
        { new Integer(6), "Warning", "WarnUnlockDoor" },
        { new Integer(7), "Warning", "Warn%Unlock%Door" },
        { new Integer(8), "Warning", "W_Unlock_Door" },
    };

    public DynamicLikeOptimizationTest(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite tests = new TestSuite("DynamicLikeOptimizationTest");
        tests.addTestSuite(DynamicLikeOptimizationTest.class);
        tests.addTest(TestConfiguration.clientServerDecorator(
                          new TestSuite(DynamicLikeOptimizationTest.class)));
        return new CleanDatabaseTestSetup(tests) {
            protected void decorateSQL(Statement stmt) throws SQLException {
                stmt.executeUpdate("create table t1(c11 int)");
                stmt.executeUpdate("insert into t1 values 1");

                stmt.executeUpdate("create table test(id char(10), " +
                                   "c10 char(10), vc10 varchar(10))");
                PreparedStatement insert = getConnection().prepareStatement(
                    "insert into test values (?,?,?)");
                String[] values = {
                    "asdf", "asdg", "aasdf", "%foobar", "foo%bar", "foo_bar"
                };
                for (int i = 0; i < values.length; i++) {
                    for (int j = 1; j <= 3; j++) {
                        insert.setString(j, values[i]);
                    }
                    insert.executeUpdate();
                }
                insert.setString(1, "V-NULL");
                insert.setString(2, null);
                insert.setString(3, null);
                insert.executeUpdate();
                insert.setString(1, "MAX_CHAR");
                insert.setString(2, "\uFA2D");
                insert.setString(3, "\uFA2D");
                insert.executeUpdate();
                insert.close();

                stmt.executeUpdate(
                    "create table likeable(match_me varchar(10), " +
                    "pattern varchar(10), esc varchar(1))");
                stmt.executeUpdate(
                    "insert into likeable values " +
                    "('foo%bar', 'fooZ%bar', 'Z'), " +
                    "('foo%bar', '%Z%ba_', 'Z')," +
                    "('foo%bar', 'fooZ%baZ', 'Z')");

                stmt.executeUpdate(
                    "create table cei(id int, name varchar(192) not null, " +
                    "source varchar(252) not null)");

                PreparedStatement cei = getConnection().prepareStatement(
                    "insert into cei values (?,?,?)");
                for (int i = 0; i < CEI_ROWS.length; i++) {
                    for (int j = 0; j < CEI_ROWS[i].length; j++) {
                        cei.setObject(j+1, CEI_ROWS[i][j]);
                    }
                    cei.executeUpdate();
                }
                cei.close();
            }
        };
    }

    protected void setUp() throws SQLException {
        getConnection().setAutoCommit(false);
    }

    public void testSimpleLikePredicates() throws SQLException {
        PreparedStatement ps =
            prepareStatement("select 1 from t1 where 'asdf' like ?");

        // queries that expect one row
        String[] one = { "%", "%f", "asd%", "_%", "%_", "%asdf" };
        for (int i = 0; i < one.length; i++) {
            ps.setString(1, one[i]);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
        }

        // queries that expect empty result set
        String[] empty = { "", "%g", "_asdf", null };
        for (int i = 0; i < empty.length; i++) {
            ps.setObject(1, empty[i], Types.VARCHAR);
            JDBC.assertEmpty(ps.executeQuery());
        }

        ps.close();
    }

    public void testEscapeSyntax() throws SQLException {
        PreparedStatement ps =
            prepareStatement("select 1 from t1 where '%foobar' " +
                             "like 'Z%foobar' escape ?");

        // match: optimize to LIKE and ==
        ps.setString(1, "Z");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");

        // invalid escape strings
        String[][] invalid = {
            { "raZ", "22019" },
            { "", "22019" },
            { null, "22501" },
        };
        for (int i = 0; i < invalid.length; i++) {
            ps.setObject(1, invalid[i][0], Types.VARCHAR);
            try {
                ps.executeQuery();
                fail();
            } catch (SQLException e) {
                assertSQLState(invalid[i][1], e);
            }
        }

        // no match, wrong char
        ps.setString(1, "%");
        JDBC.assertEmpty(ps.executeQuery());

        ps.close();
    }

    public void testWildcardAsEscape() throws SQLException {
        Statement s = createStatement();
        JDBC.assertSingleValueResultSet(
            s.executeQuery(
                "select 1 from t1 where '%foobar' like '%%foobar' escape '%'"),
            "1");
        JDBC.assertSingleValueResultSet(
            s.executeQuery(
                "select 1 from t1 where '_foobar' like '__foobar' escape '_'"),
            "1");
        s.close();
    }

    public void testEscapeSyntax2() throws SQLException {
        PreparedStatement ps = prepareStatement(
            "select 1 from t1 where '%foobar' like ? escape ?");

        ps.setString(1, "Z%foobar");
        ps.setString(2, "Z");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");

        ps.setString(2, "");
        try {
            ps.executeQuery();
            fail();
        } catch (SQLException e) {
            assertSQLState("22019", e);
        }

        ps.close();
    }

    public void testEscapeSyntax3() throws SQLException {
        PreparedStatement ps = prepareStatement(
            "select 1 from t1 where '%foobar' like ? escape 'Z'");

        ps.setString(1, "x%foobar");
        JDBC.assertEmpty(ps.executeQuery());

        ps.setString(1, "Z%foobar");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");

        ps.close();
    }

    public void testEscapeSyntax4() throws SQLException {
        PreparedStatement ps = prepareStatement(
            "select 1 from t1 where '%foobar' like ? escape '$'");

        ps.setString(1, "$%f%bar");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");

        ps.close();
    }

    public void testEscapeSyntax5() throws SQLException {
        PreparedStatement ps = prepareStatement(
            "select 1 from t1 where 'Z%foobar' like ? escape 'Z'");

        ps.setString(1, "ZZZ%foo%a_");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");

        ps.close();
    }

    public void testLikeWithHighestValidCharacter() throws SQLException {
        // \uFA2D - the highest valid character according to
        // Character.isDefined() of JDK 1.4;
        PreparedStatement ps =
            prepareStatement("select 1 from t1 where '\uFA2D' like ?");

        String[] match = { "%", "_", "\uFA2D" };
        for (int i = 0; i < match.length; i++) {
            ps.setString(1, match[i]);
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
        }

        ps.setString(1, "");
        JDBC.assertEmpty(ps.executeQuery());

        ps.close();
    }

    public void testGeneratedPredicatesCHAR() throws SQLException {
        PreparedStatement ps =
            prepareStatement("select id from test where c10 like ?");
        String[][][] expected = {
            /* null */ { },
            /* 1 */ { },
            /* "" */ { },
            /* % */  { {"MAX_CHAR"}, {"asdf"}, {"asdg"}, {"aasdf"},
                       {"%foobar"}, {"foo%bar"}, {"foo_bar"} },
            /* %f */ { },
            /* %g */ { },
            /* asd% */ { {"asdf"},{"asdg"} },
            /* _% */ { {"MAX_CHAR"}, {"asdf"}, {"asdg"}, {"aasdf"},
                       {"%foobar"}, {"foo%bar"}, {"foo_bar"} },
            /* %_ */ { {"MAX_CHAR"}, {"asdf"}, {"asdg"}, {"aasdf"},
                       {"%foobar"}, {"foo%bar"}, {"foo_bar"} },
            /* _asdf */ { },
            /* _asdf % */ { {"aasdf"} },
            /* %asdf */ { },
        };
        testGeneratedPredicates(ps, expected);
    }

    public void testGeneratedPredicatesVARCHAR() throws SQLException {
        PreparedStatement ps =
            prepareStatement("select id from test where vc10 like ?");
        String[][][] expected = {
            /* null */ { },
            /* 1 */ { },
            /* "" */ { },
            /* % */  { {"MAX_CHAR"}, {"asdf"}, {"asdg"}, {"aasdf"},
                       {"%foobar"}, {"foo%bar"}, {"foo_bar"} },
            /* %f */ { {"asdf"}, {"aasdf"} },
            /* %g */ { {"asdg"} },
            /* asd% */ { {"asdf"},{"asdg"} },
            /* _% */ { {"MAX_CHAR"}, {"asdf"}, {"asdg"}, {"aasdf"},
                       {"%foobar"}, {"foo%bar"}, {"foo_bar"} },
            /* %_ */ { {"MAX_CHAR"}, {"asdf"}, {"asdg"}, {"aasdf"},
                       {"%foobar"}, {"foo%bar"}, {"foo_bar"} },
            /* _asdf */ { {"aasdf"} },
            /* _asdf % */ { },
            /* %asdf */ { {"asdf"}, {"aasdf"} },
        };
        testGeneratedPredicates(ps, expected);
    }

    /**
     * Helper method for <code>testGeneratedPredicates*</code>. Executes a
     * prepared statement with different parameter values and compares result
     * to an array of expected rows.
     *
     * @param ps the prepared statement to execute
     * @param rows array of expected rows to be returned for the different
     * executions
     */
    private void testGeneratedPredicates(PreparedStatement ps,
                                         String[][][] rows)
            throws SQLException {
        Object[] args = {
            null, new Integer(1), "", "%", "%f", "%g", "asd%", "_%", "%_",
            "_asdf", "_asdf %", "%asdf"
        };
        assertEquals(args.length, rows.length);

        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                ps.setString(1, null);
            } else {
                ps.setObject(1, args[i]);
            }
            JDBC.assertUnorderedResultSet(ps.executeQuery(), rows[i]);
        }
        ps.close();
    }

    public void testStringAndPatternAndEscapeFromTable() throws SQLException {
        PreparedStatement ps =
            prepareStatement("select match_me from likeable " +
                             "where match_me like pattern escape esc");

        // In embedded, the first two should go fine, third one should fail
        // because the escape character is not followed by _ or %. In
        // client/server mode, executeQuery() should fail because of
        // pre-fetching. (This test only works correctly if the rows are
        // returned in the insert order, which happens to be the case but is
        // not guaranteed.)
        ResultSet rs = null;
        boolean twoSuccessful = false;
        try {
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertTrue(rs.next());
            twoSuccessful = true;
            rs.next();
            fail();
        } catch (SQLException e) {
            assertSQLState("22025", e);
            assertTrue((usingEmbedded() && twoSuccessful) ||
                       (usingDerbyNetClient() && (rs == null)));
        }
        if (rs != null) {
            rs.close();
        }

        PreparedStatement del = prepareStatement("delete from likeable");
        assertEquals(3, del.executeUpdate());

        PreparedStatement ins = prepareStatement("insert into likeable " +
                                                 "values (?, ?, ?)");
        ins.setString(1, "foo%bar");
        ins.setString(2, "foo%bar");
        ins.setString(3, null);
        ins.executeUpdate();

        try {
            JDBC.assertDrainResults(ps.executeQuery());
            fail();
        } catch (SQLException e) {
            assertSQLState("22501", e);
        }

        assertEquals(1, del.executeUpdate());

        ins.setString(3, "");
        ins.executeUpdate();

        try {
            JDBC.assertDrainResults(ps.executeQuery());
            fail();
        } catch (SQLException e) {
            assertSQLState("22019", e);
        }

        ps.close();
        del.close();
        ins.close();
    }

    /**
     * Test defect 6002/6039.
     */
    public void testEscapeWithBackslash() throws SQLException {
        PreparedStatement ps = prepareStatement(
            "select id, name, source from cei where " +
            "(name LIKE ? escape '\\') and (source like ? escape '\\') " +
            "order by source asc, name asc");

        HashMap<String[], Object[][]> inputOutput =
                new HashMap<String[], Object[][]>();
        inputOutput.put(
            new String[] {"%", "%"},
            new Object[][] {
                CEI_ROWS[0], CEI_ROWS[1], CEI_ROWS[2], CEI_ROWS[3], CEI_ROWS[4],
                CEI_ROWS[8], CEI_ROWS[7], CEI_ROWS[5], CEI_ROWS[6]
            });
        inputOutput.put(
            new String[] {"Alarms", "AlarmDisk%"},
            new Object[][] { CEI_ROWS[0] });
        inputOutput.put(
            new String[] {"A%", "%"},
            new Object[][] {
                CEI_ROWS[0], CEI_ROWS[1], CEI_ROWS[2], CEI_ROWS[3], CEI_ROWS[4],
            });
        inputOutput.put(
            new String[] {"%", "___rm%"},
            new Object[][] { CEI_ROWS[0], CEI_ROWS[1], CEI_ROWS[2] });
        inputOutput.put(
            new String[] {"Warning", "%oor"},
            new Object[][] { CEI_ROWS[8], CEI_ROWS[7], CEI_ROWS[6] });
        inputOutput.put(
            new String[] {"Warning", "Warn\\%Unlock\\%Door"},
            new Object[][] { CEI_ROWS[7] });
        inputOutput.put(
            new String[] {"Warning", "%\\%Unlo%"},
            new Object[][] { CEI_ROWS[7] });
        inputOutput.put(
            new String[] {"Warning", "W\\_Unloc%"},
            new Object[][] { CEI_ROWS[8] });
        inputOutput.put(
            new String[] {"Warning", "_\\_Unlock\\_Door"},
            new Object[][] { CEI_ROWS[8] });
        inputOutput.put(
            new String[] {"W%", "Warn\\%Unlock\\%Door"},
            new Object[][] { CEI_ROWS[7] });
        inputOutput.put(
            new String[] {"%ing", "W\\_Unlock\\_%Door"},
            new Object[][] { CEI_ROWS[8] });
        inputOutput.put(new String[] {"Bogus", "Name"}, new Object[][] {});

        for (Map.Entry<String[], Object[][]> entry : inputOutput.entrySet()) {
            String[] args = entry.getKey();
            Object[][] rows = entry.getValue();
            ps.setObject(1, args[0]);
            ps.setObject(2, args[1]);
            JDBC.assertFullResultSet(ps.executeQuery(), rows, false);
        }

        ps.close();
    }

    /**
     * Test that % matches tab characters (DERBY-1262).
     */
    public void testTabs() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("insert into test values " +
                        "('asd\tp', 'asd\tp', 'asd\tp'), " +
                        "('ase\tp', 'ase\tg', 'ase\tg')");

        String[][] expected = { {"asdf"}, {"asdg"}, {"asd\tp"} };
        JDBC.assertUnorderedResultSet(
            s.executeQuery("select c10 from test where c10 like 'asd%'"),
            expected);

        PreparedStatement ps =
            prepareStatement("select c10 from test where c10 like ?");
        ps.setString(1, "asd%");
        JDBC.assertUnorderedResultSet(ps.executeQuery(), expected);

        s.close();
        ps.close();
    }

    /**
     * Test that it is possible to escape an escape character that is before
     * the first wildcard (% or _) in the pattern (DERBY-1386).
     */
    public void testEscapedEscapeCharacterPrecedingFirstWildcard()
            throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("insert into test values " +
                        "('abc#def', 'abc#def', 'abc#def'), " +
                        "('abc\\def', 'abc\\def', 'abc\\def')");

        PreparedStatement[] ps = {
            prepareStatement("select id from test where c10 like ? escape ?"),
            prepareStatement("select id from test where vc10 like ? escape ?")
        };

        String[][] inputOutput = {
            { "abc##%", "#", "abc#def", "abc#def" },
            { "abc\\\\%", "\\", "abc\\def", "abc\\def" },
            { "abc##_ef", "#", null, "abc#def" },
            { "abc\\\\_ef", "\\", null, "abc\\def" },
        };

        for (int i = 0; i < inputOutput.length; i++) {
            for (int j = 0; j < ps.length; j++) {
                ps[j].setString(1, inputOutput[i][0]);
                ps[j].setString(2, inputOutput[i][1]);
                ResultSet rs = ps[j].executeQuery();
                String expected = inputOutput[i][2+j];
                if (expected == null) {
                    JDBC.assertEmpty(rs);
                } else {
                    JDBC.assertSingleValueResultSet(rs, expected);
                }
            }
        }

        s.close();
        ps[0].close();
        ps[1].close();
    }

    /**
     * Test that dynamic like optimization is performed. That is, the LIKE
     * predicate is rewritten to &gt;=, &lt; and LIKE.
     */
    public void testDynamicLikeOptimization() throws SQLException {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery( 
          		"VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collation')");
        if (rs.next()){
        	if (rs.getString(1).equals("TERRITORY_BASED")) {
        		rs.close();
        		s.close();
        		return;
        	}
        }
        s.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        PreparedStatement ps =
            prepareStatement("select id from test where vc10 like ?");
        ps.setString(1, "%");
        JDBC.assertDrainResults(ps.executeQuery());
        RuntimeStatisticsParser p = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(p.hasGreaterThanOrEqualQualifier());
        assertTrue(p.hasLessThanQualifier());
        s.close();
        ps.close();
    }

    public void testCast() throws SQLException {
        Statement s = createStatement();
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select 1 from t1 where 'asdf' like " +
                           "cast('%f' as varchar(2))"),
            "1");
        JDBC.assertEmpty(s.executeQuery("select 1 from t1 where 'asdf' like " +
                                        "cast(null as char)"));
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select 1 from t1 where '%foobar' like 'Z%foobar' " +
                           "escape cast('Z' as varchar(1))"),
            "1");
        // quoted values clause should not match anything
        JDBC.assertEmpty(s.executeQuery(
                "select vc10 from test where vc10 like " +
                "'values cast(null as varchar(1))'"));
        JDBC.assertEmpty(s.executeQuery(
                "select id from test where c10 like " +
                "cast ('%f' as varchar(2))"));
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select id from test where vc10 like " +
                "cast ('%f' as varchar(2))"),
                new String[][] { {"asdf"}, {"aasdf"} });
        s.close();
    }
}
