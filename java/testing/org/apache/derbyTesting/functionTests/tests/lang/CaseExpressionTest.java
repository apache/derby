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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

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
    
  
    
}
