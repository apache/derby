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

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
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
        TestSuite suite = (TestSuite)
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

        // When all branches specify NULL, then Derby currently returns NULL
        // with type CHAR(1). It should have raised an error according to the
        // SQL standard. See DERBY-2002.
        String[] allNull = {
            "values case when true then null end",
            "values case when true then null else null end",
            "values case when true then null when false then null else null end"
        };
        for (String sql : allNull) {
            JDBC.assertSingleValueResultSet(s.executeQuery(sql), null);
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
}
