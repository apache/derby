/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CaseExpressionTest
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

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.TestConfiguration;


public class CaseExpressionTest extends BaseJDBCTestCase {

        // Results if the Case Expression evaluates to a column reference :
        //
        // 1. SELECT CASE WHEN 1 = 1 THEN <column reference> ELSE NULL END
        // 2. SELECT CASE WHEN 1 = 1 THEN
        //       (CASE WHEN 1 = 1 THEN <column reference> ELSE NULL END)
        //       ELSE NULL END
        //
        private static String[][] columnReferenceResults = {
            /*SMALLINT*/ {null,"0","1","2"},
            /*INTEGER*/ {null,"0","1","21"},
            /*BIGINT*/ {null,"0","1","22"},
            /*DECIMAL(10,5)*/ {null,"0.00000","1.00000","23.00000"},
            /*REAL*/ {null,"0.0","1.0","24.0"},
            /*DOUBLE*/ {null,"0.0","1.0","25.0"},
            /*CHAR(60)*/ {
                null,
                "0                                                           ",
                "aa                                                          ",
                "2.0                                                         "},
            /*VARCHAR(60)*/ {null,"0","aa","15:30:20"},
            /*LONG VARCHAR*/ {null,"0","aa","2000-01-01 15:30:20"},
            /*CHAR(60) FOR BIT DATA*/ {
                null,
                "10aa20202020202020202020202020202020202020202020202020202020" +
                "202020202020202020202020202020202020202020202020202020202020",
                null,
                "10aaaa202020202020202020202020202020202020202020202020202020" +
                "202020202020202020202020202020202020202020202020202020202020"},
            /*VARCHAR(60) FOR BIT DATA*/ {null,"10aa",null,"10aaba"},
            /*LONG VARCHAR FOR BIT DATA*/ {null,"10aa",null,"10aaca"},
            /*CLOB(1k)*/ {null,"13","14",null},
            /*DATE*/ {null,"2000-01-01","2000-01-01",null},
            /*TIME*/ {null,"15:30:20","15:30:20","15:30:20"},
            /*TIMESTAMP*/ {
                null,
                "2000-01-01 15:30:20.0",
                "2000-01-01 15:30:20.0",
                "2000-01-01 15:30:20.0"},
            /*BLOB(1k)*/ {null,null,null,null},
        };
        
       

        // Results if the Case Expression evaluates to a NULL value :
        //
        // 3. SELECT CASE WHEN 1 = 1 THEN NULL ELSE <column reference> END
        // 4. SELECT CASE WHEN 1 = 1 THEN
        //       (CASE WHEN 1 = 1 THEN NULL ELSE <column reference> END)
        //         ELSE NULL END
        // 5. SELECT CASE WHEN 1 = 1 THEN NULL ELSE
        //         (CASE WHEN 1 = 1 THEN <column reference> ELSE NULL END) END
        // 6. SELECT CASE WHEN 1 = 1 THEN NULL ELSE
        //         (CASE WHEN 1 = 1 THEN NULL ELSE <column reference> END) END
        //
        private static String[][] nullValueResults  = {
            /*SMALLINT*/ {null,null,null,null},
            /*INTEGER*/ {null,null,null,null},
            /*BIGINT*/ {null,null,null,null},
            /*DECIMAL(10,5)*/ {null,null,null,null},
            /*REAL*/ {null,null,null,null},
            /*DOUBLE*/ {null,null,null,null},
            /*CHAR(60)*/ {null,null,null,null},
            /*VARCHAR(60)*/ {null,null,null,null},
            /*LONG VARCHAR*/ {null,null,null,null},
            /*CHAR(60) FOR BIT DATA*/ {null,null,null,null},
            /*VARCHAR(60) FOR BIT DATA*/ {null,null,null,null},
            /*LONG VARCHAR FOR BIT DATA*/ {null,null,null,null},
            /*CLOB(1k)*/ {null,null,null,null},
            /*DATE*/ {null,null,null,null},
            /*TIME*/ {null,null,null,null},
            /*TIMESTAMP*/ {null,null,null,null},
            /*BLOB(1k)*/ {null,null,null,null},
        };

    public CaseExpressionTest(String name) {
        super(name);
    }
    
    /**
     * Test various statements that 
     *
     */
    public void testWhenNonBoolean() {
        
        // DERBY-2809: BOOLEAN datatype was forced upon
        // unary expressions that were not BOOLEAN, such
        // as SQRT(?)
        String[] unaryOperators = {
                "SQRT(?)", "SQRT(9)",
                "UPPER(?)", "UPPER('haight')",
                "LOWER(?)", "LOWER('HAIGHT')",
        };
        for (int i = 0; i < unaryOperators.length; i++)
        {
            assertCompileError("42X88",
               "VALUES CASE WHEN " + unaryOperators[i] +
               " THEN 3 ELSE 4 END");
        }
    }

    public void testAllDatatypesCombinationsForCaseExpressions()
    throws SQLException
    {
        Statement s = createStatement();

        /* 1. Column Reference in the THEN node, and NULL in
         * the ELSE node.
         */
        testCaseExpressionQuery(s, columnReferenceResults,
            "SELECT CASE WHEN 1 = 1 THEN ",
            " ELSE NULL END from AllDataTypesTable");

        /* 2. Test Column Reference nested in the THEN's node THEN node,
         * NULL's elsewhere.
         */
        testCaseExpressionQuery(s, columnReferenceResults,
            "SELECT CASE WHEN 1 = 1 THEN (CASE WHEN 1 = 1 THEN ",
            " ELSE NULL END) ELSE NULL END from AllDataTypesTable");

        /* 3. NULL in the THEN node, and a Column Reference in
         * the ELSE node.
         */
        testCaseExpressionQuery(s, nullValueResults,
            "SELECT CASE WHEN 1 = 1 THEN NULL ELSE ",
            " END from AllDataTypesTable");

        /* 4. Test Column Reference nested in the THEN's node ELSE node,
         * NULL's elsewhere.
         */
        testCaseExpressionQuery(s, nullValueResults,
            "SELECT CASE WHEN 1 = 1 THEN (CASE WHEN 1 = 1 THEN NULL ELSE ",
            " END) ELSE NULL END from AllDataTypesTable");

        /* 5. Test Column Reference nested in the ELSE's node THEN node,
         * NULL's elsewhere.
         */
        testCaseExpressionQuery(s, nullValueResults,
            "SELECT CASE WHEN 1 = 1 THEN NULL ELSE (CASE WHEN 1 = 1 THEN ",
            " ELSE NULL END) END from AllDataTypesTable");

        /* 6. Test Column Reference nested in the ELSE's node ELSE node,
         * NULL's elsewhere.
         */
        testCaseExpressionQuery(s, nullValueResults,
            "SELECT CASE WHEN 1 = 1 THEN NULL " +
            "ELSE (CASE WHEN 1 = 1 THEN NULL ELSE ",
            " END) END from AllDataTypesTable");

        s.close();
    }

    /**
     * Test a query that has many WHEN conditions in it.  This is mostly
     * checking for the performance regression filed as DERBY-2986.  That
     * regression may not be noticeable in the scope of the full regression
     * suite, but if this test is run standalone then this fixture could
     * still be useful.
     */
    public void testMultipleWhens() throws SQLException
    {
        Statement s = createStatement();
        JDBC.assertFullResultSet(
            s.executeQuery(
                "values CASE WHEN 10 = 1 THEN 'a' " +
                "WHEN 10 = 2 THEN 'b' " +
                "WHEN 10 = 3 THEN 'c' " +
                "WHEN 10 = 4 THEN 'd' " +
                "WHEN 10 = 5 THEN 'e' " +
                "WHEN 10 = 6 THEN 'f' " +
                "WHEN 10 = 7 THEN 'g' " +
                "WHEN 10 = 8 THEN 'h' " +
                "WHEN 10 = 11 THEN 'i' " +
                "WHEN 10 = 12 THEN 'j' " +
                "WHEN 10 = 15 THEN 'k' " +
                "WHEN 10 = 16 THEN 'l' " +
                "WHEN 10 = 23 THEN 'm' " +
                "WHEN 10 = 24 THEN 'n' " +
                "WHEN 10 = 27 THEN 'o' " +
                "WHEN 10 = 31 THEN 'p' " +
                "WHEN 10 = 41 THEN 'q' " +
                "WHEN 10 = 42 THEN 'r' " +
                "WHEN 10 = 50 THEN 's' " +
                "ELSE '*' END"),
            new String[][] {{"*"}});

        s.close();
    }

    /**
     * Before DERBY-6423, boolean expressions (such as A OR B, or A AND B)
     * were not accepted in THEN and ELSE clauses.
     */
    public void testBooleanExpressions() throws SQLException {
        Statement s = createStatement();

        // Test both with and without parentheses around the expressions.
        // Those with parentheses used to work, and those without used to
        // cause syntax errors. Now both should work.
        JDBC.assertFullResultSet(
            s.executeQuery(
                "select case when a or b then b or c else a or c end,\n" +
                "   case when a and b then b and c else a and c end,\n" +
                "   case when (a or b) then (b or c) else (a or c) end,\n" +
                "   case when (a and b) then (b and c) else (a and c) end\n" +
                "from (values (true, true, true), (true, true, false),\n" +
                "             (true, false, true), (true, false, false),\n" +
                "             (false, true, true), (false, true, false),\n" +
                "             (false, false, true), (false, false, false)\n" +
                "      ) v(a, b, c)\n" +
                "order by a desc, b desc, c desc"),
            new String[][] {
                { "true", "true", "true", "true" },
                { "true", "false", "true", "false" },
                { "true", "true", "true", "true" },
                { "false", "false", "false", "false" },
                { "true", "false", "true", "false" },
                { "true", "false", "true", "false" },
                { "true", "false", "true", "false" },
                { "false", "false", "false", "false" },
            });
    }

    /**
     * Runs the test fixtures in embedded.
     *
     * @return test suite
     */
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = (BaseTestSuite)
            TestConfiguration.embeddedSuite(CaseExpressionTest.class);

        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the table used in the test cases.
             */
            protected void decorateSQL(Statement s) throws SQLException {
                SQLUtilities.createAndPopulateAllDataTypesTable(s);
                
            }
        };
    }

    /**
     * Execute the received caseExpression on the received Statement
     * and check the results against the receieved expected array.
     */
    private void testCaseExpressionQuery(Statement st,
        String [][] expRS, String caseExprBegin, String caseExprEnd)
        throws SQLException
    {
        ResultSet rs;
        int colType;
        int row;

        for (colType = 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-3034
            colType < SQLUtilities.SQLTypes.length;
            colType++)
        {
            rs = st.executeQuery(
                caseExprBegin +
                SQLUtilities.allDataTypesColumnNames[colType] +
                caseExprEnd);

            row = 0;
            
            while (rs.next()) {
                String val = rs.getString(1);
                assertEquals(expRS[colType][row], val);
                row++;
            }
            rs.close();
        }
     
    }

    
    /**
     * Test fix for DERBY-3032. Fix ClassCastException if SQL NULL is returned from conditional.
     * 
     * @throws SQLException
     */
    public void testDerby3032() throws SQLException 
    {
        Statement s = createStatement();
        

        s.executeUpdate("create table t (d date, vc varchar(30))");
        s.executeUpdate("insert into t values(CURRENT_DATE, 'hello')");
        ResultSet rs = s.executeQuery("SELECT d from t where d = (SELECT CASE WHEN 1 = 1 THEN CURRENT_DATE ELSE NULL END from t)");
        JDBC.assertDrainResults(rs,1);
        
        // Make sure null gets cast properly to date type to avoid cast exception. DERBY-3032
        rs = s.executeQuery("SELECT d from t where d = (SELECT CASE WHEN 1 = 1 THEN NULL  ELSE CURRENT_DATE  END from t)");
        JDBC.assertEmpty(rs);
        
        rs = s.executeQuery("SELECT d from t where d = (SELECT CASE WHEN 1 = 0 THEN CURRENT_DATE  ELSE NULL END from t)");
        JDBC.assertEmpty(rs);
        
        // Make sure metadata has correct type for various null handling
        rs = s.executeQuery("SELECT CASE WHEN 1 = 1 THEN NULL  ELSE CURRENT_DATE  END from t");
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals(java.sql.Types.DATE, rsmd.getColumnType(1));
        // should be nullable since it returns NULL #:)
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(1));
        JDBC.assertSingleValueResultSet(rs, null);    
        
        rs = s.executeQuery("SELECT CASE WHEN 1 = 0 THEN CURRENT_DATE ELSE NULL END from t");
        rsmd = rs.getMetaData();
        assertEquals(java.sql.Types.DATE, rsmd.getColumnType(1));
        // should be nullable since it returns NULL #:)
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(1));
        JDBC.assertSingleValueResultSet(rs, null);  
        
        // and with an implicit NULL return.
        rs = s.executeQuery("SELECT CASE WHEN 1 = 0 THEN CURRENT_DATE END from t");
        rsmd = rs.getMetaData();
        assertEquals(java.sql.Types.DATE, rsmd.getColumnType(1));
        // should be nullable since it returns NULL #:)
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(1));
        JDBC.assertSingleValueResultSet(rs, null);  
        
        // and no possible NULL return.
        
        rs = s.executeQuery("SELECT CASE WHEN 1 = 0 THEN 6 ELSE 4 END from t");
        rsmd = rs.getMetaData();
        assertEquals(java.sql.Types.INTEGER, rsmd.getColumnType(1));
        // should be nullable since it returns NULL #:)
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
        JDBC.assertSingleValueResultSet(rs, "4"); 
        
        rs = s.executeQuery("SELECT CASE WHEN 1 = 1 THEN 6 ELSE 4 END from t");
        rsmd = rs.getMetaData();
        assertEquals(java.sql.Types.INTEGER, rsmd.getColumnType(1));
        // should be nullable since it returns NULL #:)
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
        JDBC.assertSingleValueResultSet(rs, "6");
        
    }

    /**
     * Verify that NOT elimination produces the correct results.
     * DERBY-6563.
     */
    public void testNotElimination() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table d6563(b1 boolean, b2 boolean, b3 boolean)");

        // Fill the table with all possible combinations of true/false/null.
        Boolean[] universe = { true, false, null };
        PreparedStatement insert = prepareStatement(
                "insert into d6563 values (?, ?, ?)");
        for (Boolean v1 : universe) {
            insert.setObject(1, v1);
            for (Boolean v2 : universe) {
                insert.setObject(2, v2);
                for (Boolean v3 : universe) {
                    insert.setObject(3, v3);
                    insert.executeUpdate();
                }
            }
        }

        // Truth table for
        // B1, B2, B3, WHEN B1 THEN B2 ELSE B3, NOT (WHEN B1 THEN B2 ELSE B3).
        Object[][] expectedRows = {
            { false, false, false, false, true  },
            { false, false, true,  true,  false },
            { false, false, null,  null,  null  },
            { false, true,  false, false, true  },
            { false, true,  true,  true,  false },
            { false, true,  null,  null,  null  },
            { false, null,  false, false, true  },
            { false, null,  true,  true,  false },
            { false, null,  null,  null,  null  },
            { true,  false, false, false, true  },
            { true,  false, true,  false, true  },
            { true,  false, null,  false, true  },
            { true,  true,  false, true,  false },
            { true,  true,  true,  true,  false },
            { true,  true,  null,  true,  false },
            { true,  null,  false, null,  null  },
            { true,  null,  true,  null,  null  },
            { true,  null,  null,  null,  null  },
            { null,  false, false, false, true  },
            { null,  false, true,  true,  false },
            { null,  false, null,  null,  null  },
            { null,  true,  false, false, true  },
            { null,  true,  true,  true,  false },
            { null,  true,  null,  null,  null  },
            { null,  null,  false, false, true  },
            { null,  null,  true,  true,  false },
            { null,  null,  null,  null,  null  },
        };

        // Verify the truth table. Since NOT elimination is not performed on
        // expressions in the SELECT list, this passed even before the fix.
        JDBC.assertFullResultSet(
            s.executeQuery(
                "select b1, b2, b3, case when b1 then b2 else b3 end, "
                        + "not case when b1 then b2 else b3 end "
                        + "from d6563 order by b1, b2, b3"),
            expectedRows, false);

        // Now take only those rows where the NOT CASE expression evaluated
        // to TRUE, and strip off the expression columns at the end.
        ArrayList<Object[]> rows = new ArrayList<Object[]>();
        for (Object[] row : expectedRows) {
            if (row[4] == Boolean.TRUE) {
                rows.add(Arrays.copyOf(row, 3));
            }
        }

        // Assert that those are the only rows returned if the NOT CASE
        // expression is used as a predicate. This query used to return a
        // different set of rows before the fix.
        expectedRows = rows.toArray(new Object[rows.size()][]);
        JDBC.assertFullResultSet(
                s.executeQuery("select * from d6563 where "
                        + "not case when b1 then b2 else b3 end "
                        + "order by b1, b2, b3"),
                expectedRows, false);
    }

    /**
     * Test that parameters can be used in CASE expressions.
     */
    public void testParameters() throws SQLException {
        // If all of the result expressions are untyped parameters, the
        // type cannot be determined, and an error should be raised.
        assertCompileError("42X87", "values case when true then ? else ? end");

        // If at least one result expression is typed, the parameter should
        // get its type from it.
        PreparedStatement ps = prepareStatement(
                "values case when true then ? else 1 end");

        // DERBY-6567: The result should be nullable, since the parameter
        // could be set to null. It used to be reported as not nullable.
        assertEquals(ResultSetMetaData.columnNullable,
                     ps.getMetaData().isNullable(1));

        ps.setNull(1, Types.INTEGER);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        ps.setInt(1, 1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");

        ps = prepareStatement(
                "values case when true then ? else cast(? as integer) end");
        ParameterMetaData params = ps.getParameterMetaData();
        assertEquals(Types.INTEGER, params.getParameterType(1));
        assertEquals(Types.INTEGER, params.getParameterType(2));
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");

        // Parameters in the WHEN clause can be untyped. They will
        // implicitly get the BOOLEAN type.
        ps = prepareStatement("values case when ? then 1 else 0 end");
        assertEquals(Types.BOOLEAN,
                     ps.getParameterMetaData().getParameterType(1));

        ps.setBoolean(1, true);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");

        ps.setBoolean(1, false);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "0");

        ps.setNull(1, Types.BOOLEAN);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "0");
    }

    /**
     * Test how untyped NULLs are handled.
     */
    public void testUntypedNulls() throws SQLException {
        Statement s = createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-6566

        // Before DERBY-2002, Derby accepted a CASE expression to have an
        // untyped NULL in all the result branches. Verify that an error
        // is raised.
        String[] allUntyped = {
            // The SQL standard says at least one result should not be an
            // untyped NULL, so expect these to fail.
            "values case when true then null end",
            "values case when true then null else null end",
            "values case when true then null "
                + "when false then null else null end",

            // We're not able to tell the type if we have a mix of untyped
            // NULLs and untyped parameters.
            "values case when true then ? end", // implicit ELSE NULL
            "values case when true then null else ? end",
            "values case when true then ? when false then ? else null end",

            // These ones failed even before DERBY-2002.
            "values case when true then ? else ? end",
            "values case when true then ? when false then ? else ? end",
        };
        for (String sql : allUntyped) {
            assertCompileError("42X87", sql);
        }

        // Check that expressions with untyped NULLs compile as long as
        // there is at least one typed expression.
        JDBC.assertFullResultSet(s.executeQuery(
                "select case when a then 1 when b then null end, "
                    + "case when a then null when b then 1 end, "
                    + "case when a then null when b then null else 1 end "
                    + "from (values (false, false), (false, true), "
                    + " (true, false), (true, true)) v(a, b) order by a, b"),
            new Object[][] {
                { null, null, 1    },
                { null, 1,    null },
                { 1,    null, null },
                { 1,    null, null },
            },
            false);

        // When there is a typed NULL, its type has to be compatible with
        // the types of the other expressions.
        assertCompileError("42X89",
            "values case when 1<>1 then 'abc' else cast(null as smallint) end");
    }

    /** Regression test case for DERBY-6577. */
    public void testQuantifiedComparison() throws SQLException {
        // This query used to return wrong results.
        JDBC.assertUnorderedResultSet(createStatement().executeQuery(
                "select c, case when c = all (values 'Y') then true end "
                + "from (values 'Y', 'N') v(c)"),
            new String[][] { { "N", null }, { "Y", "true" }});
    }

    /**
     * Tests for the simple case syntax added in DERBY-1576.
     */
    public void testSimpleCaseSyntax() throws SQLException {
        Statement s = createStatement();

        // Simplest of the simple cases. SQL:1999 syntax, which allows a
        // single operand per WHEN clause, and the operand is a value
        // expression.
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select i, case i when 0 then 'zero' "
                + "when 1 then 'one' when 1+1 then 'two' "
                + "else 'many' end from "
                + "(values 0, 1, 2, 3, cast(null as int)) v(i)"),
            new String[][] {
                {"0", "zero"},
                {"1", "one"},
                {"2", "two"},
                {"3", "many"},
                {null, "many"}
            });

        // SQL:2003 added feature F262 Extended CASE Expression, which
        // allows more complex WHEN operands. Essentially, it allows any
        // last part of a predicate (everything after the left operand).
        JDBC.assertFullResultSet(s.executeQuery(
                "select i, case i when < 0 then 'negative' "
                        + "when < 10 then 'small' "
                        + "when between 10 and 20 then 'medium' "
                        + "when in (19, 23, 29, 37, 41) then 'prime' "
                        + "when = some (values 7, 42) then 'lucky number' "
                        + "when >= 40 then 'big' end "
                        + "from (values -1, 0, 1, 2, 3, 8, 9, 10, 17, 19, "
                        + "29, 37, 38, 39, 40, 41, 42, 50) v(i) order by i"),
            new String[][] {
                { "-1", "negative" },
                { "0", "small" },
                { "1", "small" },
                { "2", "small" },
                { "3", "small" },
                { "8", "small" },
                { "9", "small" },
                { "10", "medium" },
                { "17", "medium" },
                { "19", "medium" },
                { "29", "prime" },
                { "37", "prime" },
                { "38", null },
                { "39", null },
                { "40", "big" },
                { "41", "prime" },
                { "42", "lucky number" },
                { "50", "big" },
            });

        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select c, case c "
                + "when like 'abc%' then 0 "
                + "when like 'x%%' escape 'x' then 1 "
                + "when = all (select ibmreqd from sysibm.sysdummy1) then 2 "
                + "when 'xyz' || 'zyx' then 3 "
                + "when is null then 4 "
                + "when is not null then 5 end "
                + "from (values 'abcdef', 'xyzzyx', '%s', 'hello', "
                + "cast(null as char(1)), 'Y', 'N') v(c)"),
            new String[][] {
                { "abcdef", "0" },
                { "xyzzyx", "3" },
                { "%s", "1" },
                { "hello", "5" },
                { null, "4" },
                { "Y", "2" },
                { "N", "5" },
            });

        // SQL:2011 added feature F263 Comma-separated predicates in simple
        // CASE expression, which allows multiple operands per WHEN clause.
        JDBC.assertFullResultSet(s.executeQuery(
                "select i, case i "
                + "when between 2 and 3, 5, =7 then 'prime' "
                + "when <1, >7 then 'out of range' "
                + "when is not null then 'small' end "
                + "from (values 0, 1, 2, 3, 4, 5, 6, 7, 8, cast(null as int)) "
                + "as v(i) order by i"),
            new String[][] {
                { "0", "out of range" },
                { "1", "small" },
                { "2", "prime" },
                { "3", "prime" },
                { "4", "small" },
                { "5", "prime" },
                { "6", "small" },
                { "7", "prime" },
                { "8", "out of range" },
                { null, null },
            });

        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select c, case c "
                + "when in ('ab', 'cd'), like '_' then 'matched' "
                + "else 'not matched' end "
                + "from (values cast('a' as varchar(1)), 'b', 'c', 'ab', "
                + "'cd', 'ac', 'abc') v(c)"),
            new String[][] {
                { "a",   "matched" },
                { "b",   "matched" },
                { "c",   "matched" },
                { "ab",  "matched" },
                { "cd",  "matched" },
                { "ac",  "not matched" },
                { "abc", "not matched" },
            });

        // Untyped null is not allowed as CASE operand. Use typed null instead.
        assertCompileError("42X01", "values case null when 1 then 'one' end");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values case cast(null as int) when 1 then 'one' end"),
            null);

        // Untyped null is not allowed as WHEN operand. Use IS NULL instead.
        assertCompileError("42X01", "values case 1 when null then 'null' end");
        JDBC.assertUnorderedResultSet(s.executeQuery(
                "select i, case i when is null then 1 when is not null "
                + "then 2 else 3 end from (values 1, cast(null as int)) v(i)"),
            new String[][] { { "1", "2" }, { null, "1" } });

        // Non-deterministic functions are not allowed in the case operand.
        assertCompileError("42Y98",
                "values case sysfun.random() when 1 then true else false end");
        assertCompileError("42Y98",
                "values case (values sysfun.random()) "
                + "when 1 then true else false end");

        // Deterministic functions, on the other hand, are allowed.
        JDBC.assertFullResultSet(s.executeQuery(
                "select case sysfun.sin(angle) when < 0 then 'negative' "
                        + "when > 0 then 'positive' end "
                        + "from (values -pi()/2, 0, pi()/2) v(angle) "
                        + "order by angle"),
            new String[][] { {"negative"}, {null}, {"positive"} });

        // Non-deterministic functions can be used outside of the case operand.
        JDBC.assertDrainResults(
            s.executeQuery(
                "values case 1 when sysfun.random() then sysfun.random() end"),
            1);

        // Scalar subqueries are allowed in the case operand.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("values case (values 1) when 1 then true end"),
                "true");

        // Non-scalar subqueries are not allowed.
        assertCompileError(
                "42X39", "values case (values (1, 2)) when 1 then true end");
        assertStatementError(
                "21000", s, "values case (values 1, 2) when 1 then true end");

        // The type of the CASE operand must be compatible with the types
        // of all the WHEN operands.
        assertCompileError("42818", "values case 1 when true then 'yes' end");
        assertCompileError("42818",
                "values case 1 when 1 then 'yes' when 2 then 'no' "
                + "when 'three' then 'maybe' end");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values case cast(1 as bigint)"
                + " when cast(1 as smallint) then 'yes' end"),
            "yes");

        // A sequence cannot be accessed anywhere in a CASE expression.
        s.execute("create sequence d1576_s start with 1");
        assertCompileError(
                "42XAH",
                "values case next value for d1576_s when 1 then 1 else 0 end");
        assertCompileError(
                "42XAH",
                "values case 1 when next value for d1576_s then 1 else 0 end");
        assertCompileError(
                "42XAH",
                "values case 1 when 1 then next value for d1576_s else 0 end");

        // Instead, access the sequence value in a nested query.
        JDBC.assertSingleValueResultSet(
                s.executeQuery(
                    "select case x when 1 then 1 else 0 end from "
                    + "(values next value for d1576_s) v(x)"),
                "1");

        s.execute("drop sequence d1576_s restrict");

        // Window functions are allowed.
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select case row_number() over () when 1 then 'one' "
                    + "when 2 then 'two' end from (values 1, 1, 1) v(x)"),
                new String[][] { {"one"}, {"two"}, {null} });

        // Test that you can have a typed parameter in the case operand.
        PreparedStatement ps = prepareStatement(
                "values case cast(? as integer) "
                + "when 1 then 'one' when 2 then 'two' end");
        ps.setInt(1, 1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "one");
        ps.setInt(1, 2);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "two");
        ps.setInt(1, 3);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        // This one fails to compile because an integer cannot be checked
        // with LIKE.
        assertCompileError("42884",
                "values case cast(? as integer) "
                + "when 1 then 1 when like 'abc' then 2 end");

        // Untyped parameter in the case operand. Should be able to infer
        // the type from the WHEN clauses.
        ps = prepareStatement("values case ? when 1 then 2 when 3 then 4 end");
        ParameterMetaData pmd = ps.getParameterMetaData();
        assertEquals(Types.INTEGER, pmd.getParameterType(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(1));

        ps.setInt(1, 1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "2");

        ps.setInt(1, 2);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        ps.setInt(1, 3);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "4");

        ps = prepareStatement(
                "values case ? when cast(1.1 as double) then true "
                + "when cast(1.2 as double) then false end");
        pmd = ps.getParameterMetaData();
        assertEquals(Types.DOUBLE, pmd.getParameterType(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(1));

        ps.setDouble(1, 1.1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "true");
        ps.setDouble(1, 1.2);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "false");
        ps.setDouble(1, 1.3);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        // Mixed types are accepted, as long as they are compatible.
        ps = prepareStatement(
                "values case ? when 1 then 'one' when 2.1 then 'two' end");
        pmd = ps.getParameterMetaData();
        assertEquals(Types.DECIMAL, pmd.getParameterType(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(1));

        ps.setInt(1, 1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "one");
        ps.setInt(1, 2);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        ps.setDouble(1, 1.1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.setDouble(1, 2.1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "two");

        ps = prepareStatement(
                "values case ? when 1 then 'one' when 2.1 then 'two'"
                + " when cast(3 as bigint) then 'three' end");
        assertEquals(Types.DECIMAL, pmd.getParameterType(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(1));

        ps.setInt(1, 1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "one");
        ps.setInt(1, 2);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.setInt(1, 3);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "three");

        ps.setDouble(1, 1.1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.setDouble(1, 2.1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "two");
        ps.setDouble(1, 3.1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        ps = prepareStatement(
                "values case ? when 'abcdef' then 1 "
                + "when cast('abcd' as varchar(4)) then 2 end");
        pmd = ps.getParameterMetaData();
        assertEquals(Types.VARCHAR, pmd.getParameterType(1));
        assertEquals(6, pmd.getPrecision(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(1));

        ps.setString(1, "abcdef");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "1");
        ps.setString(1, "abcd");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "2");
        ps.setString(1, "ab");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.setString(1, "abcdefghi");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        // The types in the WHEN clauses are incompatible, so the type of
        // the case operand cannot be inferred.
        assertCompileError("42818",
            "values case ? when 1 then true when like 'abc' then false end");

        assertCompileError("42818",
            "values case ? when like 'abc' then true when 1 then false end");

        // BLOB and CLOB are not comparable with anything.
        assertCompileError("42818",
                "values case ? when cast(x'abcd' as blob) then true end");
        assertCompileError("42818",
                "values case ? when cast('abcd' as clob) then true end");

        // Cannot infer type if both sides of the comparison are untyped.
        assertCompileError("42X35", "values case ? when ? then true end");
        assertCompileError("42X35", "values case ? when ? then true "
                                    + "when 1 then false end");

        // Should be able to infer type when the untyped parameter is prefixed
        // with plus or minus.
        ps = prepareStatement(
                "values (case +? when 1 then 1 when 2.1 then 2 end, "
                        + "case -? when 1 then 1 when 2.1 then 2 end)");
        pmd = ps.getParameterMetaData();
        assertEquals(Types.DECIMAL, pmd.getParameterType(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(1));
        assertEquals(Types.DECIMAL, pmd.getParameterType(2));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(2));

        ps.setInt(1, 1);
        ps.setInt(2, -1);
        JDBC.assertFullResultSet(ps.executeQuery(),
                                 new String[][] {{ "1", "1" }});

        ps.setInt(1, 2);
        ps.setInt(2, -2);
        JDBC.assertFullResultSet(ps.executeQuery(),
                                 new String[][] {{ null, null }});

        ps.setDouble(1, 1.1);
        ps.setDouble(2, -1.1);
        JDBC.assertFullResultSet(ps.executeQuery(),
                                 new String[][] {{ null, null }});

        ps.setDouble(1, 2.1);
        ps.setDouble(2, -2.1);
        JDBC.assertFullResultSet(ps.executeQuery(),
                                 new String[][] {{ "2", "2" }});

        // If the untyped parameter is part of an arithmetic expression, its
        // type is inferred from that expression and not from the WHEN clause.
        ps = prepareStatement(
                "values case 2*? when 2 then true when 3.0 then false end");
        pmd = ps.getParameterMetaData();
        assertEquals(Types.INTEGER, pmd.getParameterType(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(1));

        ps.setInt(1, 1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "true");
        ps.setDouble(1, 1.5);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "true");
        ps.setInt(1, 2);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        ps = prepareStatement(
                "values case 2.0*? when 2 then true when 3.0 then false end");
        pmd = ps.getParameterMetaData();
        assertEquals(Types.DECIMAL, pmd.getParameterType(1));
        assertEquals(ParameterMetaData.parameterNullable, pmd.isNullable(1));

        ps.setInt(1, 1);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "true");
        ps.setDouble(1, 1.5);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "false");
        ps.setInt(1, 2);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        // The EXISTS predicate can only be used in the WHEN operand if
        // the CASE operand is a BOOLEAN.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("values case true when exists" +
                               "(select * from sysibm.sysdummy1) then 1 end"),
                "1");
        assertCompileError("42818",
                "values case 1 when exists" +
                "(select * from sysibm.sysdummy1) then 1 end");

        // Scalar subqueries are allowed in the operands.
        JDBC.assertSingleValueResultSet(
                s.executeQuery(
                        "values case (select ibmreqd from sysibm.sysdummy1) "
                        + "when 'N' then 'no' when 'Y' then 'yes' end"),
                "yes");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("values case 'Y' when "
                                + "(select ibmreqd from sysibm.sysdummy1) "
                                + "then 'yes' end"),
                "yes");

        // Subquery returns two columns - fail.
        assertCompileError(
                "42X39",
                "values case (select ibmreqd, 1 from sysibm.sysdummy1)"
                + " when 'Y' then true end");
        assertCompileError(
                "42X39",
                "values case 'Y' when "
                + "(select ibmreqd, 1 from sysibm.sysdummy1) then true end");

        // Subquery returns multiple rows - fail.
        assertStatementError("21000", s,
            "values case (select 1 from sys.systables) when 1 then true end");
        assertStatementError("21000", s,
            "values case 1 when (select 1 from sys.systables) then true end");

        // Subquery returns zero rows, which is converted to NULL for scalar
        // subqueries.
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values case (select ibmreqd from sysibm.sysdummy1 where false)"
                + " when is null then 'yes' end"),
            "yes");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values case true when true then "
                + "(select ibmreqd from sysibm.sysdummy1 where false) end"),
            null);

        // Simple case expressions should work in join conditions.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select x from (values 1, 2, 3) v1(x) "
                                + "join (values 13, 14) v2(y) "
                                + "on case y-x when 10 then true end"),
                "3");
    }

    /**
     * Verify that the case operand expression is evaluated only once per
     * evaluation of the CASE expression.
     */
    public void testSingleEvaluationOfCaseOperand() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();

        s.execute("create function count_me(x int) returns int "
                + "language java parameter style java external name '"
                + getClass().getName() + ".countMe' no sql deterministic");

        callCount.set(0);

        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select case count_me(x) when 1 then 'one' when 2 then 'two' "
                + "when 3 then 'three' end from (values 1, 2, 3) v(x)"),
            new String[][] { {"one"}, {"two"}, {"three"} });

        // The CASE expression is evaluated once per row. There are three
        // rows. Expect that the COUNT_ME function was only invoked once
        // per row.
        assertEquals(3, callCount.get());
    }

    /** Count how many times countMe() has been called. */
    private static final AtomicInteger callCount = new AtomicInteger();

    /**
     * Stored function that keeps track of how many times it has been called.
     * @param i an integer
     * @return the integer {@code i}
     */
    public static int countMe(int i) {
        callCount.incrementAndGet();
        return i;
    }

    /**
     * Test that large objects can be used as case operands.
     */
    public void testLobAsCaseOperand() throws SQLException {
        Statement s = createStatement();

        // BLOB and CLOB are allowed in the case operand.
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values case cast(null as blob) when is null then 'yes' end"),
            "yes");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "values case cast(null as clob) when is null then 'yes' end"),
            "yes");

        // Comparisons between BLOB and BLOB, or between CLOB and CLOB, are
        // not allowed, so expect a compile-time error for these queries.
        assertCompileError("42818",
                "values case cast(null as blob) "
                + "when cast(null as blob) then true end");
        assertCompileError("42818",
                "values case cast(null as clob) "
                + "when cast(null as clob) then true end");

        // Now create a table with some actual LOBs in them.
        s.execute("create table lobs_for_simple_case("
                + "id int generated always as identity, b blob, c clob)");

        PreparedStatement insert = prepareStatement(
                "insert into lobs_for_simple_case(b, c) values (?, ?)");

        // A small one.
        insert.setBytes(1, new byte[] {1, 2, 3});
        insert.setString(2, "small");
        insert.executeUpdate();

        // And a big one (larger than 32K means it will be streamed
        // from store, instead of being returned as a materialized value).
        insert.setBinaryStream(1, new LoopingAlphabetStream(40000));
        insert.setCharacterStream(2, new LoopingAlphabetReader(40000));
        insert.executeUpdate();

        // And a NULL.
        insert.setNull(1, Types.BLOB);
        insert.setNull(2, Types.CLOB);
        insert.executeUpdate();

        // IS [NOT] NULL can be used on both BLOB and CLOB. LIKE can be
        // used on CLOB. Those are the only predicates supported on BLOB
        // and CLOB in simple case expressions currently. Test that they
        // all work.
        JDBC.assertUnorderedResultSet(
            s.executeQuery(
                "select id, case b when is null then 'yes'"
                + " when is not null then 'no' end, "
                + "case c when is null then 'yes' when like 'abc' then 'abc'"
                + " when like 'abc%' then 'abc...' when is not null then 'no'"
                + " end "
                + "from lobs_for_simple_case"),
            new String[][] {
                { "1", "no", "no" },
                { "2", "no", "abc..." },
                { "3", "yes", "yes" },
            });

        s.execute("drop table lobs_for_simple_case");
    }
}
