/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GroupByTest

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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Many of these test cases were converted from the old groupBy.sql
 * using the DERBY-2151 SQLToJUnit conversion utility.
 */
public class GroupByTest extends BaseJDBCTestCase {

	public GroupByTest(String name) {
		super(name);
	}
	
	public static Test suite() {
		return new CleanDatabaseTestSetup(
				new TestSuite(GroupByTest.class, "GroupByTest")) {
			protected void decorateSQL(Statement s)
				throws SQLException
			{
				createSchemaObjects(s);
			}
		};
	}

	String [][] expRS;
	String [] expColNames;

	/**
	 * Creates a variety of tables used by the various GroupBy tests
	 * @throws SQLException
	 */
	private static void createSchemaObjects(Statement st)
		throws SQLException
	{
		st.executeUpdate("create table bug280 (a int, b int)");
		st.executeUpdate("insert into bug280 (a, b) " +
						"values (1,1), (1,2), (1,3), (2,1), (2,2)");
        st.executeUpdate("CREATE TABLE A2937 (C CHAR(10) NOT NULL, " +
                "D DATE NOT NULL, DC DECIMAL(6,2))");
        st.executeUpdate("INSERT INTO A2937 VALUES ('aaa', " +
                "DATE('2007-07-10'), 500.00)");
		st.executeUpdate("create table yy (a double, b double)");
		st.executeUpdate("insert into yy values (2,4), (2, 4), " +
			"(5,7), (2,3), (2,3), (2,3), (2,3), (9,7)");
        
        st.executeUpdate("create table t1 (a int, b int, c int)");
        st.executeUpdate("create table t2 (a int, b int, c int)");
        st.executeUpdate("insert into t2 values (1,1,1), (2,2,2)");
        
        st.executeUpdate("create table unmapped(c1 long varchar)");
        st.executeUpdate("create table t_twoints(c1 int, c2 int)");
        st.executeUpdate("create table t5920(c int, d int)");
        st.executeUpdate("insert into t5920(c,d) values "
            + "(1,10),(2,20),(2,20),(3,30),(3,30),(3,30)");
        st.executeUpdate("create table t5653 (c1 float)");
        st.executeUpdate("insert into t5653 values 0.0, 90.0");

        st.executeUpdate("create table d3613 (a int, b int, c int, d int)");
        st.executeUpdate("insert into d3613 values (1,2,1,2), (1,2,3,4), " +
                "(1,3,5,6), (2,2,2,2)");

        st.executeUpdate("create table d2085 (a int, b int, c int, d int)");
        st.executeUpdate("insert into d2085 values (1,1,1,1), (1,2,3,4), " +
                "(4,3,2,1), (2,2,2,2)");

        st.execute("create table d3219 (a varchar(10), b varchar(1000))");

        st.executeUpdate("create table d2457_o (name varchar(20), ord int)");
        st.executeUpdate("create table d2457_a (ord int, amount int)");
        st.executeUpdate("insert into d2457_o values ('John', 1)," +
                " ('Jerry', 2), ('Jerry', 3), ('John', 4), ('John', 5)");
        st.executeUpdate("insert into d2457_a values (1, 12), (2, 23), " +
                "(3, 34), (4, 45), (5, 56)");

        // create an all types tables
        
        st.executeUpdate(
            "create table t (i int, s smallint, l bigint, c "
            + "char(10), v varchar(50), lvc long varchar, d double "
            + "precision, r real, dt date, t time, ts timestamp, b "
            + "char(2) for bit data, bv varchar(2) for bit data, "
            + "lbv long varchar for bit data)");
        
        st.executeUpdate(
            " create table tab1 ( i integer, s smallint, l "
            + "bigint, c char(30), v varchar(30), lvc long "
            + "varchar, d double precision, r real, dt date, t "
            + "time, ts timestamp)");
        
        // populate tables
        
        st.executeUpdate("insert into t (i) values (null)");
        st.executeUpdate("insert into t (i) values (null)");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (1, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 200, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 2000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'goodbye', "
            + "'everyone is here', 'adios, muchachos', 200.0e0, "
            + "200.0e0, date('1992-01-01'), time('12:30:30'), "
            + "timestamp('1992-01-01 12:30:30'), X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'noone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "100.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 100.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-09-09'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:55:55'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:55:55'), "
            + "X'12af', X'0f0f', X'ABCD')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'ffff', X'0f0f', X'1234')");
        
        st.executeUpdate(
            " insert into t values (0, 100, 1000000, 'hello', "
            + "'everyone is here', 'what the heck do we care?', "
            + "200.0e0, 200.0e0, date('1992-01-01'), "
            + "time('12:30:30'), timestamp('1992-01-01 12:30:30'), "
            + "X'12af', X'ffff', X'ABCD')");
        
        // bit maps to Byte[], so can't test for now
        
        st.executeUpdate(
            "insert into tab1 select i, s, l, c, v, lvc, d, r, "
            + "dt, t, ts from t");
	}

	/**
	 * Test various invalid GROUP BY statements.
	 * @throws Exception
	 */
    public void testGroupByErrors()
		throws Exception
    {
		Statement st = createStatement();

        // group by constant. should compile but fail because it 
        // is not a valid grouping expression.
        
        assertStatementError("42Y36", st,
            "select * from t1 group by 1");
        
        // column in group by list not in from list
        
        assertStatementError("42X04", st,
            "select a as d from t1 group by d");
        
        // column in group by list not in select list
        
        assertStatementError("42Y36", st,
            "select a as b from t1 group by b");
        
        assertStatementError("42Y36", st,
            " select a from t1 group by b");
        
        assertStatementError("42Y36", st,
            " select a, char(b) from t1 group by a");
        
        // cursor with group by is not updatable
        
        assertCompileError("42Y90",
                "select a from t1 group by a for update");
        
        // noncorrelated subquery that returns too many rows
        
        assertStatementError("21000", st,
            "select a, (select a from t2) from t1 group by a");
        
        // correlation on outer table
        
        assertStatementError("42Y30", st,
            "select t2.a, (select b from t1 where t1.b = t2.b) "
            + "from t1 t2 group by t2.a");
        
        // having clause cannot contain column references which 
        // are not grouping columns
        
        assertStatementError("42X24", st,
            "select a from t1 group by a having c = 1");
        
        assertStatementError("42X04", st,
            " select a from t1 o group by a having a = (select a "
            + "from t1 where b = b.o)");
        
        // ?s in group by
        
        assertStatementError("42X01", st,
            "select a from t1 group by ?");

        // group by on long varchar type
        
        assertStatementError("X0X67", st,
            " select c1, max(1) from unmapped group by c1");
		st.close();
	}

	/**
	 * Verify the correct behavior of GROUP BY with various datatypes.
	 * @throws Exception
	 */
	public void testGroupByWithVariousDatatypes()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        // Test group by and having clauses with no aggregates 
        
        // simple grouping
        
        rs = st.executeQuery(
            "select i from t group by i order by i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"},
            {"1"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select s from t group by s order by s");
        
        expColNames = new String [] {"S"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"100"},
            {"200"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select l from t group by l order by l");
        
        expColNames = new String [] {"L"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1000000"},
            {"2000000"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select c from t group by c order by c");
        
        expColNames = new String [] {"C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"goodbye"},
            {"hello"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select v from t group by v order by v");
        
        expColNames = new String [] {"V"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"everyone is here"},
            {"noone is here"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select d from t group by d order by d");
        
        expColNames = new String [] {"D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"100.0"},
            {"200.0"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select r from t group by r order by r");
        
        expColNames = new String [] {"R"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"100.0"},
            {"200.0"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select dt from t group by dt order by dt");
        
        expColNames = new String [] {"DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1992-01-01"},
            {"1992-09-09"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t from t group by t order by t");
        
        expColNames = new String [] {"T"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"12:30:30"},
            {"12:55:55"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select ts from t group by ts order by ts");
        
        expColNames = new String [] {"TS"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1992-01-01 12:30:30.0"},
            {"1992-01-01 12:55:55.0"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select b from t group by b order by b");
        
        expColNames = new String [] {"B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"12af"},
            {"ffff"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select bv from t group by bv order by bv");
        
        expColNames = new String [] {"BV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0f0f"},
            {"ffff"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // grouping by long varchar [for bit data] cols should 
        // fail in db2 mode
        
        assertStatementError("X0X67", st,
            "select lbv from t group by lbv order by lbv");
		st.close();
	}

	/**
	 * Test multicolumn grouping.
	 * @throws Exception
	 */
    public void testMulticolumnGrouping()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            "select i, dt, b from t where 1=1 group by i, dt, b "
            + "order by i,dt,b");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"0", "1992-01-01", "ffff"},
            {"0", "1992-09-09", "12af"},
            {"1", "1992-01-01", "12af"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i, dt, b from t group by i, dt, b order by i,dt,b");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"0", "1992-01-01", "ffff"},
            {"0", "1992-09-09", "12af"},
            {"1", "1992-01-01", "12af"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i, dt, b from t group by b, i, dt order by i,dt,b");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"0", "1992-01-01", "ffff"},
            {"0", "1992-09-09", "12af"},
            {"1", "1992-01-01", "12af"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i, dt, b from t group by dt, i, b order by i,dt,b");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"0", "1992-01-01", "ffff"},
            {"0", "1992-09-09", "12af"},
            {"1", "1992-01-01", "12af"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}

	/**
	 * Test the use of a subquery to group by an expression.
	 * @throws Exception
	 */
	public void testGroupByExpression()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            "select expr1, expr2 from (select i * s, c || v from "
            + "t) t (expr1, expr2) group by expr2, expr1 order by "
            + "expr2,expr1");
        
        expColNames = new String [] {"EXPR1", "EXPR2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "goodbye   everyone is here"},
            {"0", "hello     everyone is here"},
            {"100", "hello     everyone is here"},
            {"0", "hello     noone is here"},
            {null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}

	/**
	 * Test using GROUP BY in a correlated subquery.
	 * @throws Exception
	 */
	public void testGroupByCorrelatedSubquery()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            "select i, expr1 from (select i, (select distinct i "
            + "from t m where m.i = t.i) from t) t (i, expr1) "
            + "group by i, expr1 order by i,expr1");
        
        expColNames = new String [] {"I", "EXPR1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "0"},
            {"1", "1"},
            {null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}

	/**
	 * Test GROUP BY and DISTINCT.
	 * @throws Exception
	 */
	public void testGroupByDistinct()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            "select distinct i, dt, b from t group by i, dt, b "
            + "order by i,dt,b");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"0", "1992-01-01", "ffff"},
            {"0", "1992-09-09", "12af"},
            {"1", "1992-01-01", "12af"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}

	/**
	 * Test combinations of GROUP BY and ORDER BY.
	 * @throws Exception
	 */
	public void testGroupByOrderBy()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        // order by and group by same order
        
        rs = st.executeQuery(
            "select i, dt, b from t group by i, dt, b order by i, dt, b");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"0", "1992-01-01", "ffff"},
            {"0", "1992-09-09", "12af"},
            {"1", "1992-01-01", "12af"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // subset in same order
        
        rs = st.executeQuery(
            "select i, dt, b from t group by i, dt, b order by i, dt");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"0", "1992-01-01", "ffff"},
            {"0", "1992-09-09", "12af"},
            {"1", "1992-01-01", "12af"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // different order
        
        rs = st.executeQuery(
            "select i, dt, b from t group by i, dt, b order by b, dt, i");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"1", "1992-01-01", "12af"},
            {"0", "1992-09-09", "12af"},
            {"0", "1992-01-01", "ffff"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // subset in different order
        
        rs = st.executeQuery(
            "select i, dt, b from t group by i, dt, b order by b, dt");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"1", "1992-01-01", "12af"},
            {"0", "1992-09-09", "12af"},
            {"0", "1992-01-01", "ffff"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}

	/**
	 * group by without having in from subquery.
	 * @throws Exception
	 */
	public void testGroupByInSubquery()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        
        rs = st.executeQuery(
            "select * from (select i, dt from t group by i, dt) "
            + "t (t_i, t_dt), (select i, dt from t group by i, dt) "
            + "m (m_i, m_dt) where t_i = m_i and t_dt = m_dt order "
            + "by t_i,t_dt,m_i,m_dt");
        
        expColNames = new String [] {"T_I", "T_DT", "M_I", "M_DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "0", "1992-01-01"},
            {"0", "1992-09-09", "0", "1992-09-09"},
            {"1", "1992-01-01", "1", "1992-01-01"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from (select i, dt from t group by i, dt) "
            + "t (t_i, t_dt), (select i, dt from t group by i, dt) "
            + "m (m_i, m_dt) group by t_i, t_dt, m_i, m_dt order "
            + "by t_i,t_dt,m_i,m_dt");
        
        expColNames = new String [] {"T_I", "T_DT", "M_I", "M_DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "0", "1992-01-01"},
            {"0", "1992-01-01", "0", "1992-09-09"},
            {"0", "1992-01-01", "1", "1992-01-01"},
            {"0", "1992-01-01", null, null},
            {"0", "1992-09-09", "0", "1992-01-01"},
            {"0", "1992-09-09", "0", "1992-09-09"},
            {"0", "1992-09-09", "1", "1992-01-01"},
            {"0", "1992-09-09", null, null},
            {"1", "1992-01-01", "0", "1992-01-01"},
            {"1", "1992-01-01", "0", "1992-09-09"},
            {"1", "1992-01-01", "1", "1992-01-01"},
            {"1", "1992-01-01", null, null},
            {null, null, "0", "1992-01-01"},
            {null, null, "0", "1992-09-09"},
            {null, null, "1", "1992-01-01"},
            {null, null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from (select i, dt from t group by i, dt) "
            + "t (t_i, t_dt), (select i, dt from t group by i, dt) "
            + "m (m_i, m_dt) where t_i = m_i and t_dt = m_dt group "
            + "by t_i, t_dt, m_i, m_dt order by t_i,t_dt,m_i,m_dt");
        
        expColNames = new String [] {"T_I", "T_DT", "M_I", "M_DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "0", "1992-01-01"},
            {"0", "1992-09-09", "0", "1992-09-09"},
            {"1", "1992-01-01", "1", "1992-01-01"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t.*, m.* from (select i, dt from t group by "
            + "i, dt) t (t_i, t_dt), (select i, dt from t group by "
            + "i, dt) m (t_i, t_dt) where t.t_i = m.t_i and t.t_dt "
            + "= m.t_dt group by t.t_i, t.t_dt, m.t_i, m.t_dt "
            + "order by t.t_i,t.t_dt,m.t_i,m.t_dt");
        
        expColNames = new String [] {"T_I", "T_DT", "T_I", "T_DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "0", "1992-01-01"},
            {"0", "1992-09-09", "0", "1992-09-09"},
            {"1", "1992-01-01", "1", "1992-01-01"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t.t_i, t.t_dt, m.* from (select i, dt from "
            + "t group by i, dt) t (t_i, t_dt), (select i, dt from "
            + "t group by i, dt) m (t_i, t_dt) where t.t_i = m.t_i "
            + "and t.t_dt = m.t_dt group by t.t_i, t.t_dt, m.t_i, "
            + "m.t_dt order by t.t_i,t.t_dt,m.t_i,m.t_dt");
        
        expColNames = new String [] {"T_I", "T_DT", "T_I", "T_DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "0", "1992-01-01"},
            {"0", "1992-09-09", "0", "1992-09-09"},
            {"1", "1992-01-01", "1", "1992-01-01"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}

	/**
	 * additional columns in group by list not in select list.
	 * @throws Exception
	 */
	public void testGroupByWithAdditionalColumns()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            "select i, dt, b from t group by i, dt, b order by i,dt,b");
        
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"0", "1992-01-01", "ffff"},
            {"0", "1992-09-09", "12af"},
            {"1", "1992-01-01", "12af"},
            {null, null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t.i from t group by i, dt, b order by i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"},
            {"0"},
            {"0"},
            {"1"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t.dt from t group by i, dt, b order by dt");
        
        expColNames = new String [] {"DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1992-01-01"},
            {"1992-01-01"},
            {"1992-01-01"},
            {"1992-09-09"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t.b from t group by i, dt, b order by b");
        
        expColNames = new String [] {"B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"12af"},
            {"12af"},
            {"12af"},
            {"ffff"},
            {null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t.t_i, m.t_i from (select i, dt from t "
            + "group by i, dt) t (t_i, t_dt), (select i, dt from t "
            + "group by i, dt) m (t_i, t_dt) where t.t_i = m.t_i "
            + "and t.t_dt = m.t_dt group by t.t_i, t.t_dt, m.t_i, "
            + "m.t_dt order by t.t_i,m.t_i");
        
        expColNames = new String [] {"T_I", "T_I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "0"},
            {"0", "0"},
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}

	/**
	 * Test parameter markers in the having clause.
	 * @throws Exception
	 */
	public void testParameterMarkersInHavingClause()
		throws Exception
	{
		ResultSet rs = null;

		PreparedStatement pSt = prepareStatement(
            "select i, dt, b from t group by i, dt, b having i = "
            + "? order by i,dt,b");
        
		pSt.setInt(1, 0);
        
        rs = pSt.executeQuery();
        expColNames = new String [] {"I", "DT", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "12af"},
            {"0", "1992-01-01", "ffff"},
            {"0", "1992-09-09", "12af"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		pSt.close();
	}

	/**
	 * group by with having in from subquery.
	 * @throws Exception
	 */
	public void testHavingClauseInSubquery()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            "select * from (select i, dt from t group by i, dt "
            + "having 1=1) t (t_i, t_dt), (select i, dt from t "
            + "group by i, dt having i = 0) m (m_i, m_dt) where "
            + "t_i = m_i and t_dt = m_dt order by t_i,t_dt,m_i,m_dt");
        
        expColNames = new String [] {"T_I", "T_DT", "M_I", "M_DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "0", "1992-01-01"},
            {"0", "1992-09-09", "0", "1992-09-09"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from (select i, dt from t group by i, dt "
            + "having 1=1) t (t_i, t_dt), (select i, dt from t "
            + "group by i, dt having i = 0) m (m_i, m_dt) group by "
            + "t_i, t_dt, m_i, m_dt order by t_i,t_dt,m_i,m_dt");
        
        expColNames = new String [] {"T_I", "T_DT", "M_I", "M_DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "0", "1992-01-01"},
            {"0", "1992-01-01", "0", "1992-09-09"},
            {"0", "1992-09-09", "0", "1992-01-01"},
            {"0", "1992-09-09", "0", "1992-09-09"},
            {"1", "1992-01-01", "0", "1992-01-01"},
            {"1", "1992-01-01", "0", "1992-09-09"},
            {null, null, "0", "1992-01-01"},
            {null, null, "0", "1992-09-09"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from (select i, dt from t group by i, dt "
            + "having 1=1) t (t_i, t_dt), (select i, dt from t "
            + "group by i, dt having i = 0) m (m_i, m_dt) where "
            + "t_i = m_i and t_dt = m_dt group by t_i, t_dt, m_i, "
            + "m_dt having t_i * m_i = m_i * t_i order by t_i,t_dt,m_i,m_dt");
        
        expColNames = new String [] {"T_I", "T_DT", "M_I", "M_DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01", "0", "1992-01-01"},
            {"0", "1992-09-09", "0", "1992-09-09"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}

	/**
	 * correlated subquery in having clause.
	 * @throws Exception
	 */
	public void testCorrelatedSubqueryInHavingClause()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            "select i, dt from t group by i, dt having i = "
            + "(select distinct i from tab1 where t.i = tab1.i) "
            + "order by i,dt");
        
        expColNames = new String [] {"I", "DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01"},
            {"0", "1992-09-09"},
            {"1", "1992-01-01"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select i, dt from t group by i, dt having i = "
            + "(select i from t m group by i having t.i = m.i) "
            + "order by i,dt");
        
        expColNames = new String [] {"I", "DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01"},
            {"0", "1992-09-09"},
            {"1", "1992-01-01"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}

	/**
	 * column references in having clause match columns in group by list.
	 * @throws Exception
	 */
	public void testHavingClauseColumnRef()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            "select i as outer_i, dt from t group by i, dt "
            + "having i = (select i from t m group by i having t.i "
            + "= m.i) order by outer_i,dt");
        
        expColNames = new String [] {"OUTER_I", "DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01"},
            {"0", "1992-09-09"},
            {"1", "1992-01-01"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
		st.close();
	}

	/**
	 * additional columns in group by list not in select list.
	 * @throws Exception
	 */
	public void testGroupByColumnsNotInSelectList()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            "select i, dt from t group by i, dt order by i,dt");
        
        expColNames = new String [] {"I", "DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0", "1992-01-01"},
            {"0", "1992-09-09"},
            {"1", "1992-01-01"},
            {null, null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t.dt from t group by i, dt having i = 0 "
            + "order by t.dt");
        
        expColNames = new String [] {"DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1992-01-01"},
            {"1992-09-09"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t.dt from t group by i, dt having i <> 0 "
            + "order by t.dt");
        
        expColNames = new String [] {"DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1992-01-01"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select t.dt from t group by i, dt having i != 0 "
            + "order by t.dt");
        
        expColNames = new String [] {"DT"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1992-01-01"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
	}
        
	/**
	 * negative tests for selects with a having clause without a group by.
	 * @throws Exception
	 */
	public void testInvalidHavingClauses()
		throws Exception
	{
		Statement st = createStatement();

        // binding of having clause
        
        assertStatementError("42X19", st,
            "select 1 from t_twoints having 1");
        
        // column references in having clause not allowed if no 
        // group by
        
        assertStatementError("42Y35", st,
            "select * from t_twoints having c1 = 1");
        
        assertStatementError("42X24", st,
            " select 1 from t_twoints having c1 = 1");
        
        // correlated subquery in having clause
        
        assertStatementError("42Y35", st,
            "select * from t_twoints t1_outer having 1 = (select 1 from "
            + "t_twoints where c1 = t1_outer.c1)");
		st.close();
	}

	/**
	 * Tests for Bug 5653 restrictions.
	 * @throws Exception
	 */
	public void testHavingClauseRestrictions5653()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        // bug 5653 test (almost useful) restrictions on a having 
        // clause without a group by clause create the table
        
        // this is the only query that should not fail filter out 
        // all rows
        
        rs = st.executeQuery(
            "select 1 from t5653 having 1=0");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // all 6 queries below should fail after bug 5653 is fixed 
        // select *
        
        assertStatementError("42Y35", st,
            "select * from t5653 having 1=1");
        
        // select column
        
        assertStatementError("42Y35", st,
            "select c1 from t5653 having 1=1");
        
        // select with a built-in function sqrt
        
        assertStatementError("42Y35", st,
            "select sqrt(c1) from t5653 having 1=1");
        
        // non-correlated subquery in having clause
        
        assertStatementError("42Y35", st,
            "select * from t5653 having 1 = (select 1 from t5653 where "
            + "c1 = 0.0)");
        
        // expression in select list
        
        assertStatementError("42Y35", st,
            "select (c1 * c1) / c1 from t5653 where c1 <> 0 having 1=1");
        
        // between
        
        assertStatementError("42Y35", st,
            "select * from t5653 having 1 between 1 and 2");
		st.close();
	}

	/**
	 * bug 5920 test that HAVING without GROUPBY makes one group.
	 * @throws Exception
	 */
	public void testHavingWithoutGroupBy5920()
		throws Exception
	{
		Statement st = createStatement();
		ResultSet rs = null;

        rs = st.executeQuery(
            " select avg(c) from t5920 having 1 < 2");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // used to give several rows, now gives only one
        
        rs = st.executeQuery(
            "select 10 from t5920 having 1 < 2");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"10"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // ok, gives one row
        
        rs = st.executeQuery(
            "select 10,avg(c) from t5920 having 1 < 2");
        
        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"10", "2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
		st.close();
    }

	/**
	 * DERBY-578: select with group by on a temp table caused NPE
	 * @throws SQLException
	 */
	public void testGroupByWithTempTable() throws SQLException {
		Statement s = createStatement();
		s.execute("declare global temporary table session.ztemp ( orderID varchar( 50 ) ) not logged");
		JDBC.assertEmpty(s.executeQuery("select orderID from session.ztemp group by orderID"));
	}

	/**
	 * DERBY-280: Wrong result from select when aliasing to same name as used
	 * in group by
	 */
	public void testGroupByWithAliasToSameName() throws SQLException {

		Statement s = createStatement();

		String[][] expected1 = {{"1", "3"}, {"2", "2"}};
		JDBC.assertUnorderedResultSet(
			s.executeQuery("select a, count(a) from bug280 group by a"),
			expected1);
		// The second query should return the same results as the first. Would
		// throw exception before DERBY-681.
		JDBC.assertUnorderedResultSet(
			s.executeQuery("select a, count(a) as a from bug280 group by a"),
			expected1);

		// should return same results as first query (but with extra column)
		String[][] expected2 = {{"1", "3", "1"}, {"2", "2", "2"}};
		JDBC.assertUnorderedResultSet(
			s.executeQuery("select a, count(a), a from bug280 group by a"),
			expected2);

		// different tables with same column name ok
		String[][] expected3 = {
			{"1","1"}, {"1","1"}, {"1","1"}, {"2","2"}, {"2","2"} };
		JDBC.assertFullResultSet(
			s.executeQuery("select t.t_i, m.t_i from " +
						   "(select a, b from bug280 group by a, b) " +
						   "t (t_i, t_dt), " +
						   "(select a, b from bug280 group by a, b) " +
						   "m (t_i, t_dt) " +
						   "where t.t_i = m.t_i and t.t_dt = m.t_dt " +
						   "group by t.t_i, t.t_dt, m.t_i, m.t_dt " +
						   "order by t.t_i,m.t_i"),
			expected3);

		// should be allowed
		String[][] expected4 = { {"1", "1"}, {"2", "2"} };
		JDBC.assertUnorderedResultSet(
			s.executeQuery("select a, a from bug280 group by a"),
			expected4);
		JDBC.assertUnorderedResultSet(
			s.executeQuery("select bug280.a, a from bug280 group by a"),
			expected4);
		JDBC.assertUnorderedResultSet(
			s.executeQuery("select bug280.a, bug280.a from bug280 group by a"),
			expected4);
		JDBC.assertUnorderedResultSet(
			s.executeQuery("select a, bug280.a from bug280 group by a"),
			expected4);

		s.close();
	}
    
    /**
     * DERBY-2397 showed incorrect typing of aggregate nodes
     * that lead to a SUBSTR throwing an exception that its
     * position/length were out of range.
     * @throws SQLException
     */
    public void testDERBY2937() throws SQLException {
        Statement s = createStatement();
        
        ResultSet rs = s.executeQuery("SELECT A.C, SUBSTR (MAX(CAST(A.D AS CHAR(10)) || " +
                "CAST(A.DC AS CHAR(8))), 11, 8) AS BUG " +
                "FROM A2937 A GROUP BY A.C");
        JDBC.assertFullResultSet(rs,
                new String[][] {{"aaa","500.00"}});
    }

	public void testDerbyOrderByOnAggregate() throws SQLException
	{
		Statement s = createStatement();

		ResultSet rs = s.executeQuery(
			"select b, count(*) from yy where a=5 or a=2 " +
			"group by b order by count(*) desc");

		JDBC.assertFullResultSet(
				rs,
				new Object[][]{
						{new Double(3), new Integer(4)},
						{new Double(4), new Integer(2)},
						{new Double(7), new Integer(1)}},
				false);

		rs = s.executeQuery(
			"select b, count(*) from yy where a=5 or a=2 " +
			"group by b order by count(*) asc");

		JDBC.assertFullResultSet(
				rs,
				new Object[][]{
						{new Double(7), new Integer(1)},
						{new Double(4), new Integer(2)},
						{new Double(3), new Integer(4)}},
				false);
	}
    
    
    /**
     * DERBY-3257 check for correct number of rows returned with
     * or in having clause.
     *  
     * @throws SQLException
     */
    public void testOrNodeInHavingClause() throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE TAB ( ID VARCHAR(20), INFO VARCHAR(20))");
        s.executeUpdate("insert into TAB values  ('1', 'A')");
        s.executeUpdate("insert into TAB values  ('2', 'A')");
        s.executeUpdate("insert into TAB values  ('3', 'B')");
        s.executeUpdate("insert into TAB values  ('4', 'B')");
        ResultSet rs = s.executeQuery("SELECT t0.INFO, COUNT(t0.ID) FROM TAB t0 GROUP BY t0.INFO HAVING (t0.INFO = 'A' OR t0.INFO = 'B') AND t0.INFO IS NOT NULL");
        String [][] expectedRows = {{"A","2"},{"B","2"}};
        JDBC.assertFullResultSet(rs, expectedRows);
        s.executeUpdate("DROP TABLE TAB");
    }

    /**
      * DERBY-3613 check combinations of DISTINCT and GROUP BY
      */
    public void testDistinctGroupBy() throws SQLException
    {
        Statement s = createStatement();
        ResultSet rs;
        // First, a number of queries without aggregates:
        rs = s.executeQuery("select distinct a from d3613 group by a");
        JDBC.assertUnorderedResultSet(rs, new String[][] {{"2"},{"1"}});
        rs = s.executeQuery("select distinct a from d3613 group by a,b");
        JDBC.assertUnorderedResultSet(rs, new String[][] {{"2"},{"1"}});
        rs = s.executeQuery("select a,b from d3613");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","2"},{"1","2"},{"1","3"},{"2","2"}});
        rs = s.executeQuery("select distinct a,b from d3613 group by a,b,c");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","2"},{"1","3"},{"2","2"}});
        rs = s.executeQuery("select distinct a,b from d3613 group by a,b");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","2"},{"1","3"},{"2","2"}});
        rs = s.executeQuery("select distinct a,b from d3613 group by a,c,b");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","2"},{"1","3"},{"2","2"}});
        // Second, a number of similar queries, with aggregates:
        rs = s.executeQuery("select a,sum(b) from d3613 group by a,b");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","4"},{"1","3"},{"2","2"}});
        rs = s.executeQuery("select distinct a,sum(b) from d3613 group by a,b");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","4"},{"1","3"},{"2","2"}});
        rs = s.executeQuery("select a,sum(b) from d3613 group by a,c");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","2"},{"1","2"},{"1","3"},{"2","2"}});
        rs = s.executeQuery("select distinct a,sum(b) from d3613 group by a,c");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","2"},{"1","3"},{"2","2"}});
        rs = s.executeQuery(
                "select a,sum(b) from d3613 group by a,b,c");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","2"},{"1","2"},{"1","3"},{"2","2"}});
        rs = s.executeQuery(
                "select distinct a,sum(b) from d3613 group by a,b,c");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","2"},{"1","3"},{"2","2"}});
        rs = s.executeQuery(
                "select distinct a,sum(b) from d3613 group by a");
        JDBC.assertUnorderedResultSet(rs,
                new String[][] {{"1","7"},{"2","2"}});
        // A few error cases:
        assertStatementError("42Y36", s,
            "select distinct a,b from d3613 group by a");
        assertStatementError("42Y36", s,
            "select distinct a,b from d3613 group by a,c");
        assertStatementError("42Y36", s,
            "select distinct a,b,sum(b) from d3613 group by a");
        
        // A few queries from other parts of this suite, with DISTINCT added:
        JDBC.assertFullResultSet(
            s.executeQuery("select distinct t.t_i, m.t_i from " +
                           "(select a, b from bug280 group by a, b) " +
                           "t (t_i, t_dt), " +
                           "(select a, b from bug280 group by a, b) " +
                           "m (t_i, t_dt) " +
                           "where t.t_i = m.t_i and t.t_dt = m.t_dt " +
                           "group by t.t_i, t.t_dt, m.t_i, m.t_dt " +
                           "order by t.t_i,m.t_i"),
            new String[][] {  {"1","1"}, {"2","2"} } );

        JDBC.assertFullResultSet(
            s.executeQuery(
                " select distinct t.i from t group by i, dt, b order by i"),
            new String [][] { {"0"}, {"1"}, {null} });
        JDBC.assertFullResultSet(
            s.executeQuery(
                " select distinct t.dt from t group by i, dt, b order by dt"),
            new String [][] { {"1992-01-01"}, {"1992-09-09"}, {null} });
    }
    /**
      * DERBY-2085 check message on order by of non-grouped column
      */
    public void testOrderByNonGroupedColumn() throws SQLException
    {
        Statement s = createStatement();
        ResultSet rs;
        assertStatementError("42Y36", s,
            "select a from d2085 group by a order by b");
        assertStatementError("42Y36", s,
            "select a from d2085 group by a,b order by c");
        assertStatementError("42Y36", s,
            "select a,b from d2085 group by a,b order by c*2");
    }

    /**
     * DERBY-3219: Check that MaxMinAggregator's external format works
     * properly with a string of length 0.
     */
    public void testGroupByMaxWithEmptyString() throws SQLException
    {
        // Force all sorts to be external sorts, for DERBY-3219. This
        // property only takes effect for debug sane builds, but that
        // should be adequate since at least some developers routinely
        // run tests in that configuration.
        boolean wasSet = false;
        if (SanityManager.DEBUG)
        {
            wasSet = SanityManager.DEBUG_ON("testSort");
            SanityManager.DEBUG_SET("testSort");
        }
            
        Statement st = createStatement();
        loadRows();
        ResultSet rs = st.executeQuery("select b,max(a) from d3219 group by b");
        while (rs.next());
        // If we can read the results, the test passed. DERBY-3219 resulted
        // in an externalization data failure during the group by processing.
        if (SanityManager.DEBUG)
        {
            if (! wasSet)
                SanityManager.DEBUG_CLEAR("testSort");
        }
    }

    /**
     * Load enough rows into the table to get some externalized
     * MaxMinAggregator instances. To ensure that the values are externalized,
     * we set up this test to run with -Dderby.debug.true=testSort. Note that
     * we load the column 'a' with a string of length 0, because DERBY-3219
     * occurs when the computed MAX value is a string of length 0.
     */
    private void loadRows()
        throws SQLException
    {
        PreparedStatement ps = prepareStatement(
                "insert into d3219 (a, b) values ('', ?)");

        for (int i = 0; i < 2000; i++)
        {
            ps.setString(1, genString(1000));
            ps.executeUpdate();
        }
    }
    private static String genString(int len)
    {
        StringBuffer buf = new StringBuffer(len);

        for (int i = 0; i < len; i++)
            buf.append(chars[(int) (chars.length * Math.random())]);
        return buf.toString();
    }
    private static char []chars = {
        'q','w','e','r','t','y','u','i','o','p',
        'a','s','d','f','g','h','j','k','l',
        'z','x','c','v','b','n','m'
    };

    /**
      * DERBY-2457: Derby does not support column aliases in the
      * GROUP BY and HAVING clauses.
      */
    public void testColumnAliasInGroupByAndHaving() throws SQLException
    {
        Statement s = createStatement();
        // 1) Using the underlying column names works fine, with or
        // without a table alias:
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select name, count(ord) from d2457_o " +
                    " group by name having count(ord) > 2"),
            new String[][] {  {"John","3"} } );
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select name as col1, count(ord) as col2 " +
                    " from d2457_o group by name having count(ord) > 2"),
            new String[][] {  {"John","3"} } );
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select name as col1, count(ord) as col2 " +
                    " from d2457_o ordertable group by name " +
                    " having count(ord) > 2"),
            new String[][] {  {"John","3"} } );
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select ordertable.name as col1, " +
                    " count(ord) as col2 from d2457_o ordertable " +
                    " group by name having count(ord) > 2"),
            new String[][] {  {"John","3"} } );
        // 2) References to column aliases in GROUP BY and HAVING are
        // rejected with an error message:
        assertStatementError("42X04", s,
                "select name as col1, count(ord) as col2 from d2457_o " +
                " group by name having col2 > 2");
        assertStatementError("42X04", s,
                "select name as col1, count(ord) as col2 from d2457_o " +
                " group by col1 having col2 > 2");
        assertStatementError("42X04", s,
                "select name as col1, count(ord) as col2 from d2457_o " +
                " group by col1 having count(ord) > 2");
        assertStatementError("42X04", s,
                "select name as col1, sum(amount) as col2 " +
                " from d2457_o, d2457_a where d2457_o.ord = d2457_a.ord " +
                " group by col1 having col2 > 2");
        assertStatementError("42X04", s,
                "select name as col1, sum(amount) as col2 " +
                " from d2457_o t1, d2457_a t2 where t1.ord = t2.ord " +
                " group by col1 having col2 > 2");
        assertStatementError("42X04", s,
                "select name as col1, sum(amount) as col2 " +
                " from d2457_o t1, d2457_a t2 where t1.ord = t2.ord " +
                " group by col1 having sum(amount) > 2");
        assertStatementError("42X04", s,
                "select name as col1, sum(amount) as col2 " +
                " from d2457_o t1, d2457_a t2 where t1.ord = t2.ord " +
                " group by col1 having col2 > 2");
        assertStatementError("42X04", s,
                "select * from (select t1.name as col, sum(amount) " +
                " from d2457_o t1, d2457_a t2 where t1.ord = t2.ord " +
                " group by col) as t12(col1, col2) where col2 > 2");
        // 3) Demonstrate that column aliasing works correctly when the
        // GROUP BY is packaged as a subquery:
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from " +
                    "(select name, count(ord) from d2457_o ordertable " +
                    " group by ordertable.name) as ordertable(col1, col2) " +
                    " where col2 > 2"),
            new String[][] {  {"John","3"} } );
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from " +
                    "(select name as col, sum(amount) " +
                    " from d2457_o t1, d2457_a t2 where t1.ord = t2.ord " +
                    " group by name) as t12(col1, col2) where col2 > 2"),
            new String[][] {  {"Jerry", "57"}, {"John","113"} } );
        // 4) Demonatrate that table aliases can be used in GROUP BY and
        // HAVING clauses
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select t1.name as col1, " +
                    " sum(t2.amount) as col2 from d2457_o t1, d2457_a t2 " +
                    " where t1.ord = t2.ord group by t1.name " +
                    " having sum(t2.amount) > 2"),
            new String[][] {  {"Jerry", "57"}, {"John","113"} } );
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select name as col1, sum(amount) as col2 " +
                    " from d2457_o t1, d2457_a t2 where t1.ord = t2.ord " +
                    " group by name having sum(amount) > 2"),
            new String[][] {  {"Jerry", "57"}, {"John","113"} } );
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select t1.name as col1, sum(amount) as col2 " +
                    " from d2457_o t1, d2457_a t2 where t1.ord = t2.ord " +
                    " group by name having sum(amount) > 2"),
            new String[][] {  {"Jerry", "57"}, {"John","113"} } );
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select name as col1, sum(t2.amount) as col2 " +
                    " from d2457_o t1, d2457_a t2 where t1.ord = t2.ord " +
                    " group by name having sum(amount) > 2"),
            new String[][] {  {"Jerry", "57"}, {"John","113"} } );
    }
}

