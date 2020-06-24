/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ArithmeticTest

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

import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test case for arithmetic.sql. It tests the arithmetic operators. 
 */
public class ArithmeticTest extends BaseJDBCTestCase {

    private static final String BIGINT = "bigint";
    private static final String DECIMAL = "decimal(31,0)";

    public ArithmeticTest(String name) {
        super(name);
    }

    public static Test suite(){
        return TestConfiguration.defaultSuite(ArithmeticTest.class);
    }

    /**
     * Test arithmetic on different types.
     * i.e. int and bigint, or int and smallint.
     * @throws SQLException 
     */
    public void testTypes() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5986
        String[] tableNames = { "smallint_r", "t", "bigint_r", "decimal_r" };
        String[] types = { "smallint", "int", BIGINT, DECIMAL };
        BigInteger[][] boundaries = {
            { BigInteger.valueOf(Short.MIN_VALUE),
              BigInteger.valueOf(Short.MAX_VALUE) },
            { BigInteger.valueOf(Integer.MIN_VALUE),
              BigInteger.valueOf(Integer.MAX_VALUE) },
            { BigInteger.valueOf(Long.MIN_VALUE),
              BigInteger.valueOf(Long.MAX_VALUE) },
            { new BigInteger("-9999999999999999999999999999999", 10),
              new BigInteger("9999999999999999999999999999999", 10) },
        };

        for (int i = 0; i < types.length; i++) {
            doBasically(tableNames[i], types[i]);
            doOverflow(tableNames[i], types[i],
                    boundaries[i][0], boundaries[i][1]);

            dropTable(tableNames[i]);
        }
    }

    /**
     * Test basic arithmetic operations.
     * i.e. int and bigint, or int and smallint.
     * 
     * @param tableName
     *                the name of table to test. 
     *                If a table with the same name has existed,
     *                it will be dropped.
     * @param type
     *                the type to test. i.e. "smallint" or "bigint".
     * @throws SQLException 
     */
    private void doBasically(String tableName, String type)
//IC see: https://issues.apache.org/jira/browse/DERBY-5986
            throws SQLException {
        String sql = "create table " + tableName + "(i " + 
            type + ", j " + type + ")";
        Statement st = createStatement();
        st.addBatch(sql);
        sql = "insert into " + tableName + " values (null, null)";
        st.addBatch(sql);
        sql = "insert into " + tableName + " values (0, 100)";
        st.addBatch(sql);
        sql = "insert into " + tableName + " values (1, 101)";
        st.addBatch(sql);
        sql = "insert into " + tableName + " values (-2, -102)";
        st.addBatch(sql);

        st.executeBatch();

        sql = "select * from " + tableName;
        String[][] result = {
            {null, null}, {"0", "100"}, {"1", "101"}, {"-2", "-102"},
        };
        JDBC.assertFullResultSet(st.executeQuery(sql), result);

        sql = "select i + j from " + tableName;
        result = new String[][]{
            {null}, {"100"}, {"102"}, {"-104"},
        };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        sql = "select i, i + 10 + 20, j, j + 100 + 200" +
                " from " + tableName;
        result = new String [][]{
                    {null, null, null, null},
                    {"0", "30", "100", "400"},
                    {"1", "31", "101", "401"},
                    {"-2", "28", "-102", "198"}
                };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        sql = "select i - j, j - i from " + tableName;
        result = new String [][]{
                    {null, null},
                    {"-100", "100"},
                    {"-100", "100"},
                    {"100", "-100"}
            };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        sql = "select i, i - 10 - 20, 20 - 10 - i, j, " +
                "j - 100 - 200, 200 - 100 - j from " 
                + tableName;
        result = new String [][]{
                {null, null, null, null, null, null},
                {"0", "-30", "10", "100", "-200", "0"},
                {"1", "-29", "9", "101", "-199", "-1"},
                {"-2", "-32", "12", "-102", "-402", "202"}
            };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        sql = "select i, j, i * j, j * i from " + tableName;
        result = new String [][]{
                    {null, null, null, null},
                    {"0", "100", "0", "0"},
                    {"1", "101", "101", "101"},
                    {"-2", "-102", "204", "204"}
            };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        sql = "select i, j, i * 10 * -20, j * 100 * -200 " +
                "from " + tableName;
        result = new String [][]{
                    {null, null, null, null},
                    {"0", "100", "0", "-2000000"},
                    {"1", "101", "-200", "-2020000"},
                    {"-2", "-102", "400", "2040000"}
            };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        // try unary minus on some expressions
        sql = "select -i, -j, -(i * 10 * -20), " +
                "-(j * 100 * -200) from " + tableName;
        result = new String[][]{
                {null, null, null, null},
                {"0", "-100", "0", "2000000"},
                {"-1", "-101", "200", "2020000"},
                {"2", "102", "-400", "-2040000"}
        };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        //unary plus doesn't do anything
        sql = "select +i, +j, +(+i * +10 * -20), " +
                "+(+j * +100 * -200) from " + tableName;
        result = new String[][]{
                    {null, null, null, null},
                    {"0", "100", "0", "-2000000"},
                    {"1", "101", "-200", "-2020000"},
                    {"-2", "-102", "400", "2040000"}
                };

        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        //test null/null, constant/null, null/constant
        sql = "select i, j, i / j, 10 / j, j / 10 " +
                "from " + tableName;
        result = new String[][]{
                    {null, null, null, null, null},
                    {"0", "100", "0", "0", "10"},
                    {"1", "101", "0", "0", "10"},
                    {"-2", "-102", "0", "0", "-10"}
                };
//IC see: https://issues.apache.org/jira/browse/DERBY-5986
        if (type.equals(DECIMAL)) {
            // With DECIMAL, the fraction part won't be truncated from i/j.
            result[1][3] = "0.100000000000000000000";
            result[2][3] = "0.099009900990099009900";
            result[3][3] = "-0.098039215686274509803";
        }
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        //test for divide by 0
        sql = "select j / i from " + tableName;
        assertStatementError("22012", st, sql);

        sql = "select (j - 1) / (i + 4), 20 / 5 / 4, " +
                "20 / 4 / 5 from " + tableName;
        result = new String[][]{
                    {null, "1", "1"},
                    {"24", "1", "1"},
                    {"20", "1", "1"},
                    {"-51", "1", "1"}
                };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        //-- test positive/negative, negative/positive 
        //and negative/negative
        sql = "select j, j / (0 - j), (0 - j) / j, " +
                "(0 - j) / (0 - j) from " + tableName;
        result = new String [][] {
                     {null, null, null, null},
                     {"100", "-1", "-1", "1"},
                     {"101", "-1", "-1", "1"},
                     {"-102", "-1", "-1", "1"}
        };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        //test some "more complex" expressions
        sql = "select i, i + 10, i - (10 - 20), i - 10, " +
                "i - (20 - 10) from " + tableName;
        result = new String [][] {
                    {null, null, null, null, null},
                    {"0", "10", "10", "-10", "-10"},
                    {"1", "11", "11", "-9", "-9"},
                    {"-2", "8", "8", "-12", "-12"}
        };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        sql = "select  'The next 2 columns should agree', " +
                "2 + 3 * 4 + 5, 2 + (3 * 4) + 5 " +
                "from " + tableName;
        result = new String [][] {
                {"The next 2 columns should agree", "19", "19"},
                {"The next 2 columns should agree", "19", "19"},
                {"The next 2 columns should agree", "19", "19"},
                {"The next 2 columns should agree", "19", "19"}
            };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);

        sql = "select 'The next column should be 45', " +
                "(2 + 3) * (4 + 5) from " + tableName;
        result = new String [][] {
                {"The next column should be 45", "45"},
                {"The next column should be 45", "45"},
                {"The next column should be 45", "45"},
                {"The next column should be 45", "45"}
        };
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            result);
    }

    /**
     * Test overflow on some types.
     * i.e. int and bigint, or int and smallint.
     * 
     * @param tableName
     *                the name of table to test. 
     *                If a table with the same name has existed,
     *                it will be dropped.
     * @param type
     *                the type to test. i.e. "smallint" or "bigint".
     * @param negativeBoundary
     *                the negative boundary for the data type
     * @param positiveBoundary
     *                the positive boundary for the data type
     * @throws SQLException 
     */
    private void doOverflow(String tableName, String type,
//IC see: https://issues.apache.org/jira/browse/DERBY-5986
            BigInteger negativeBoundary, BigInteger positiveBoundary)
        throws SQLException
    {
        dropTable(tableName);
        String sql = "create table " + tableName
                + " (i " + type + ", j " + type + ")";
        Statement st = createStatement();
        st.executeUpdate(sql);

        long i = 1L;
        sql = "insert into " + tableName + " values (" 
                + i + "," + positiveBoundary + ")";
        assertUpdateCount(st, 1, sql);
//IC see: https://issues.apache.org/jira/browse/DERBY-5986

        sql = "select i + j from " + tableName;
        assertStatementError("22003", st, sql);

        sql = "select i - j - j from " + tableName;
        assertStatementError("22003", st, sql);

        sql = "select j + j from " + tableName;
        assertStatementError("22003", st, sql);

        sql = "select j * j from " + tableName;
        assertStatementError("22003", st, sql);

        sql = "insert into " + tableName + " values "
//IC see: https://issues.apache.org/jira/browse/DERBY-5986
                + "(" + negativeBoundary + ", 0)";
        assertUpdateCount(st, 1, sql);

        // Check if the boundaries of the data type are asymmetric and
        // allow more negative values than positive values.
        final boolean asymmetricTowardsNegative =
                negativeBoundary.negate().compareTo(positiveBoundary) > 0;

        sql = "select -i from " + tableName;
        if (asymmetricTowardsNegative) {
            // For most numeric data types, the legal range is not symmetric
            // around zero, so negating the negative boundary will make the
            // resulting value out of range. Expect failure.
            assertStatementError("22003", st, sql);
        } else {
            // For DECIMAL, the boundaries are symmetric around zero, so
            // expect the query to succeed.
            JDBC.assertFullResultSet(st.executeQuery(sql),
                    new String[][] {
                        { "-1" },
                        { negativeBoundary.negate().toString() },
                    });
        }

        sql = "select -j from " + tableName;
        JDBC.assertFullResultSet(st.executeQuery(sql),
                new String[][] {
                { positiveBoundary.negate().toString() }, { "0" }, }
        );

        sql = "select j / 2 * 2 from " + tableName;
        JDBC.assertFullResultSet(st.executeQuery(sql), 
                new String[][] {
                    { positiveBoundary.subtract(BigInteger.ONE).toString() },
                    { "0" },
                }
        );

        // When type is not BIGINT or DECIMAL, it won't overflow.
        // Just like testMixedType().
        if (type.equals(BIGINT) || type.equals(DECIMAL)) {
            sql = "select 2 * (" + positiveBoundary + " / 2 + 1) from "
                    + tableName;
            assertStatementError("22003", st, sql);

            sql = "select -2 * (" + positiveBoundary + " / 2 + 2) from " 
                    + tableName;
            assertStatementError("22003", st, sql);

            sql = "select 2 * ((-" + positiveBoundary + " - 1) / 2 - 1) from "
                    + tableName;
            assertStatementError("22003", st, sql);

            sql = "select -2 * ((-" + positiveBoundary + " - 1) / 2 - 1) from "
                    + tableName;
            assertStatementError("22003", st, sql);

            // Check if the negative boundary is even, in which case the
            // arithmetic operation below won't lose precision when dividing
            // by two.
            final boolean negativeBoundaryIsEven = !negativeBoundary.testBit(0);
//IC see: https://issues.apache.org/jira/browse/DERBY-5986

            //different from arithmetic. This can support better test.
            sql = "select i / 2 * 2 - 1 from " + tableName;
            if (negativeBoundaryIsEven) {
                // Since the negative boundary is even, dividing by two and
                // subsequently multiplying by two will result in the same
                // value, and subtracting one from that value will make the
                // result out of range (less than negative boundary).
                assertStatementError("22003", st, sql);
            } else {
                // If the negative boundary is odd, dividing by two and
                // subsequently multiplying by two will result in a value
                // that is one above the negative boundary, because the
                // fraction part is lost in the intermediate result. Result
                // will still be in valid range, also after the subtraction.
                JDBC.assertFullResultSet(st.executeQuery(sql),
                        new String[][] {
                            { "-1" },
                            { negativeBoundary.toString() },
                        });
            }

            // Same test case as above, but subtract two to force error
            // also when the negative boundary is odd.
            sql = "select i / 2 * 2 - 2 from " + tableName;
            assertStatementError("22003", st, sql);
        }
    }

    /**
     * Test mixed types.i.e. int and bigint, or int and smallint.
     * 
     * @param tableName
     *                the name of table to test.
     * @param type
     *                the type to test. i.e. "smallint" or "bigint".
     * @param i
     *                an integer number to test with.
     * @throws SQLException 
     */
    private void doMixedTypeImpl(String tableName, String type,
//IC see: https://issues.apache.org/jira/browse/DERBY-5986
        BigInteger i) throws SQLException{
        String sql = "create table " + tableName 
            + " (y "  + type+ ")" ;
        Statement st = createStatement();
        st.executeUpdate(sql);

        BigInteger y = BigInteger.valueOf(2);
        sql = "insert into " + tableName + " values (" + y + ")";
        assertEquals(1, st.executeUpdate(sql));

        sql = "select " + i + " + y from " + tableName;
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
                i.add(y).toString());

        sql = "select y + " + i + " from " + tableName;
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
                y.add(i).toString());

        sql = "select y - " + i + " from " + tableName;
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
                y.subtract(i).toString());

        sql = "select " + i + " - y from " + tableName;
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
                i.subtract(y).toString());

        sql = "select " + i + " * y from " + tableName;
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
                i.multiply(y).toString());

        sql = "select y * " + i + " from "+ tableName;
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
                y.multiply(i).toString());

        sql = "select " + i + " / y from " + tableName;
        String fraction = type.equals(DECIMAL) ? ".000000000000" : "";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
                i.divide(y) + fraction);

        sql = "select y / " + i + " from " + tableName;
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
                y.divide(i).toString());

        st.close();
    }

    /**
     * Test mixed types.i.e. int and bigint, or int and smallint.
     * @throws SQLException
     */
    public void testMixedType() throws SQLException{
//IC see: https://issues.apache.org/jira/browse/DERBY-5986
        String[] types = {"smallint", BIGINT, DECIMAL};
        String[] tableNames = {"smallint_r", "bigint_r", "decimal_r"};
        BigInteger[] testValues = {
                BigInteger.valueOf(65535),
                BigInteger.valueOf(Integer.MAX_VALUE),
                BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
        };

        for(int i = 0; i < types.length; i++){
            doMixedTypeImpl(tableNames[i], types[i], testValues[i]);
            dropTable(tableNames[i]);
        }
    }

    /**
     * Test the arithmetic operators on a type we know they don't work on.
     * @throws SQLException 
     */
    public void testWrongType() throws SQLException{
        String sql = "create table s (x char(10), y char(10))";
        Statement st = createStatement();
        st.executeUpdate(sql);
        st.close();

        sql ="select x + y from s";
        assertCompileError("42Y95", sql);

        sql = "select x - y from s";
        assertCompileError("42Y95", sql);

        sql = "select x * y from s";
        assertCompileError("42Y95", sql);

        sql = "select x / y from s";
        assertCompileError("42Y95", sql);

        sql = "select -x from s";
        assertCompileError("42X37", sql);

        dropTable("s");
    }

    /**
     * Arithmetic on a numeric data type.
     * @throws SQLException 
     */
    public void testNumericDataType() throws SQLException{
        String sql = "create table u (c1 int, c2 char(10))";    
        Statement st = createStatement();
        st.addBatch(sql);
        sql = "insert into u (c2) values 'asdf'";
        st.addBatch(sql);
        sql = "insert into u (c1) values null";
        st.addBatch(sql);
        sql = "insert into u (c1) values 1";
        st.addBatch(sql);
        sql = "insert into u (c1) values null";
        st.addBatch(sql);
        sql = "insert into u (c1) values 2";
        st.addBatch(sql);
        st.executeBatch();

        sql = "select * from u";
        String[][] result = {
            {null, "asdf",},
            {null, null,},
            {"1", null,},
            {null, null,},
            {"2", null,},
        };
        JDBC.assertFullResultSet(st.executeQuery(sql), result);

        sql = "select c1 + c1 from u";
        result = new String[][]{
            {null,}, {null,}, {"2",}, {null,}, {"4",},
        };
        JDBC.assertFullResultSet(st.executeQuery(sql), result);

        sql = "select c1 / c1 from u";
        result = new String[][]{
            {null,}, {null,}, {"1",}, {null,}, {"1",},
        };
        JDBC.assertFullResultSet(st.executeQuery(sql), result);

        //arithmetic between a numeric and a string data type fails.
        sql = "select c1 + c2 from u";
        assertStatementError("22018", st, sql);

        st.close();

        dropTable("u");
    }

    public void testPrecedenceAndAssociativity() throws SQLException{
        String sql = "create table r (x int)";
        Statement st = createStatement();
        st.executeUpdate(sql);

        sql = "insert into r values (1)";
        assertEquals(1, st.executeUpdate(sql));

        sql = "select 2 + 3 * 4 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            (2 + 3 * 4) + "");

        sql = "select (2 + 3) * 4 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            ((2 + 3) * 4) + "");

        sql = "select 3 * 4 + 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            ( 3 * 4 + 2) + "");

        sql = "select 3 * (4 + 2) from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            ( 3 * (4 + 2)) + "");

        sql = "select 2 - 3 * 4 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            (2 - 3 * 4) + "");

        sql = "select (2 - 3) * 4 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            ((2 - 3) * 4) + "");

        sql = "select  3 * 4 - 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            ( 3 * 4 - 2) + "");

        sql = "select 3 * (4 - 2) from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            (3 * (4 - 2)) + "");

        sql = "select 4 + 3 / 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            (4 + 3 / 2) + "");

        sql = "select (4 + 3) / 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            ((4 + 3) / 2) + "");

        sql = "select 3 / 2 + 4 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            (3 / 2 + 4) + "");

        sql = "select 3 / (2 + 4) from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            (3 / (2 + 4)) + "");

        sql = "select 4 - 3 / 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            (4 - 3 / 2) + "");

        sql = "select (4 - 3) / 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql),
            ((4 - 3) / 2) + "");

        sql = "select 1 + 2147483647 - 2 from r";
        assertStatementError("22003", st, sql);

        sql = "select 1 + (2147483647 - 2) from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), 
            (1 + (2147483647 - 2)) + "");

        sql = "select 4 * 3 / 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), 
            (4 * 3 / 2) + "");

        sql = "select 4 * (3 / 2) from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), 
            (4 * (3 / 2)) + "");

        //Test associativity of unary - versus the binary operators
        sql = "select -1 + 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), 
            (-1 + 2) + "");

        sql = "select -(1 + 2) from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), 
            (-(1 + 2)) + "");

        sql = "select -1 - 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), 
            (-1 - 2) + "");

        sql = "select -(1 - 2) from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), 
            (-(1 - 2)) + "");

    //The test the associativity of unary - with respect to binary *,
      //we must use a trick.  The value -1073741824 is the minimum integer 
       //divided by 2. So, 1073741824 * 2 will overflow, but (-1073741824) * 2
       //will not (because of two's complement arithmetic.
        sql = "select -1073741824 * 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), 
            (-1073741824 * 2) + "");

        sql = "select -(1073741824 * 2) from r";
        assertStatementError("22003", st, sql);

        //-- This should not get an overflow
        sql = "select -2147483648 / 2 from r";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), 
            (-2147483648 / 2) + "");

        dropTable("r");
    }
}
