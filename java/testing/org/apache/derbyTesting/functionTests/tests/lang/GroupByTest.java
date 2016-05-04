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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import junit.framework.Test;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;

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
                new BaseTestSuite(GroupByTest.class, "GroupByTest")) {
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

		st.execute("CREATE TABLE d3904_T1( " +
				"D1 DATE NOT NULL PRIMARY KEY, N1 VARCHAR( 10 ))");
		st.execute("CREATE TABLE d3904_T2( " +
				"D2 DATE NOT NULL PRIMARY KEY, N2 VARCHAR( 10 ))");
		st.execute("INSERT INTO d3904_T1 VALUES "+
				"( DATE( '2008-10-01' ), 'something' ), "+
				"( DATE( '2008-10-02' ), 'something' )" );
		st.execute("INSERT INTO d3904_T2 VALUES" +
				"( DATE( '2008-10-01' ), 'something' )" ); 

        st.executeUpdate("create table d2457_o (name varchar(20), ord int)");
        st.executeUpdate("create table d2457_a (ord int, amount int)");
        st.executeUpdate("insert into d2457_o values ('John', 1)," +
                " ('Jerry', 2), ('Jerry', 3), ('John', 4), ('John', 5)");
        st.executeUpdate("insert into d2457_a values (1, 12), (2, 23), " +
                "(3, 34), (4, 45), (5, 56)");

        st.executeUpdate("create table d4071(i int, v char(10))");
        st.executeUpdate("insert into d4071 " +
                         "       values (1, '0123456789')," +
                         "              (1, '1234567890')," +
                         "              (3, '2345678901')," +
                         "              (4, '0123456789')," +
                         "              (5, '1234567890')");


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
        
        //Following setup is for DERBY-3872
        st.executeUpdate(
        		"CREATE TABLE EMPTAB (EMPID INTEGER NOT NULL, "
        		+ "SALARY DECIMAL(10, 4), DEPT_DEPTNO INTEGER)"); 

        st.executeUpdate(
        		"ALTER TABLE EMPTAB ADD CONSTRAINT " +
        		"PK_EMPTAB PRIMARY KEY (EMPID)"); 

        st.executeUpdate(
        		"CREATE TABLE DEPTTAB (DEPTNO INTEGER NOT NULL)");

      st.executeUpdate(
    		  "ALTER TABLE DEPTTAB ADD CONSTRAINT "+
    		  "PK_DEPTTAB PRIMARY KEY (DEPTNO)");

      st.executeUpdate(
    		  "insert into DEPTTAB values( 1 )");

      st.executeUpdate(
    		  "insert into EMPTAB values( 1, 1000, 1 )"); 
	
      // tables for DERBY-3880 testing
      st.executeUpdate("CREATE TABLE T1_D3880(i int, c varchar(20))");
      st.executeUpdate("create table t2_D3880(i int, c2 varchar(20), i2 int)");
      st.executeUpdate("insert into t1_D3880 values(1, 'abc')");
      st.executeUpdate("insert into t1_D3880 values(2, 'abc')");
      st.executeUpdate("insert into t2_D3880 values(1, 'xyz', 10)");
      st.executeUpdate("insert into t2_D3880 values(1, 'aaa', 20)");
      st.executeUpdate("insert into t2_D3880 values(2, 'xxx', 30)");
      
      // for DERBY-3631 testing
      st.executeUpdate("CREATE FUNCTION MAXOF2(ONE DOUBLE, TWO DOUBLE) RETURNS DOUBLE PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'java.lang.Math.max'");
      st.executeUpdate("CREATE TABLE Testd3631( GroupCol INT, Value1 INT, Value2 INT )");
      st.executeUpdate("INSERT INTO Testd3631 VALUES (1, 1, 5)");
      st.executeUpdate("INSERT INTO Testd3631 VALUES (2, -7, 2)");
      st.executeUpdate("INSERT INTO Testd3631 VALUES (2, 1, -5)");
      
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

        // group by on aggregate
        assertCompileError("42Y26", "select sum(a) from t1 group by sum(a)");

        // group by on subquery (DERBY-4403)
        assertCompileError("42Y26",
                "select sum(a) from t1 group by (select a from t1)");
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

        //Test case for DERBY-3872 Prior to fix for DERBY-3872, following
        //query resulted in NPE because of missing chain of
        //VirtualColumn-to-ResultColumn nodes for the where clause in
        //the HAVING clause. The reason for this that we didn't overwrite 
        //the method "accept()" in IndexToBaseRowNode. This missing code
        //caused Derby to associate the ResultColumn for the HAVING
        //clause incorrectly with the ResultColumn used for the join
        //clause. More info can be found in the jira
        rs = st.executeQuery(
        		"select  q1.DEPTNO from DEPTTAB q1, EMPTAB q2 where "+ 
        		"( integer (1.1) = 1)  and  ( q2.DEPT_DEPTNO = q1.DEPTNO) "+
        		" GROUP BY q1.DEPTNO HAVING  max( q2.SALARY) >=  "+
        		"( select  q3.SALARY from EMPTAB q3 where  "+
        		"(q3.EMPID =  q1.DEPTNO) )");
        
        expRS = new String [][]
        {
            {"1"}
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
	 * DERBY-5613: Queries with group by column not included in the column list
	 * 	for JOIN(INNER or OUTER) with NATURAL or USING does not fail
	 *  
	 * Derby does not replace join columns in the select list with coalesce as
	 *  suggested by SQL spec, instead it binds the join column with the left 
	 *  table when working with natural left outer join or inner join and it 
	 *  binds the join column with the right table when working with natural 
	 *  right outer join. This causes incorrect GROUP BY and HAVING queries
	 *  to pass as shown below.
	 *  
	 * The tests below show that GROUP BY and HAVING clauses are able to use a 
	 *  column which is not part of the SELECT list. This happens for USING
	 *  clause & NATURAL joins with queries using INNER JOINS and OUTER JOINS.
	 *  When using the JOIN with ON clause, we do not run into this problem 
	 *  because we are expected to qualify the JOIN column with table name 
	 *  in the SELECT list when using thw ON clause.
	 * 
	 * @throws SQLException
	 */
	public void testGroupByWithUsingClause() throws SQLException {
		Statement s = createStatement();
		//JOIN queries with ON clause do not cause ambiguity on join columns
		// because such queries require that join columns in SELECT query
		// should be qualified with left or right table name. Just a note
		// that ON clause is not allowed on CROSS and NATURAL JOINS.
		//
        //The join queries with ON clause are not impacted by DERBY-5613 and 
        // hence following tests are showing the correct behavior.
		//
		//Try INNER JOIN with ON clause.
        assertStatementError("42X03", s,
                "select i from t1_D3880 " +
				"inner join t2_D3880 ON t1_D3880.i = t2_D3880.i " +
                "group by t1_D3880.i");
		ResultSet rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"inner join t2_D3880 ON t1_D3880.i = t2_D3880.i " +
                "group by t1_D3880.i");
		String[][] expRs = new String[][] {{"1"},{"2"}};
		JDBC.assertFullResultSet(rs,expRs);
		//Try LEFT OUTER JOIN with ON clause.
        assertStatementError("42X03", s,
                "select i from t1_D3880 " +
				"LEFT OUTER JOIN t2_D3880 ON t1_D3880.i = t2_D3880.i " +
                "group by t1_D3880.i");
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"LEFT OUTER JOIN t2_D3880 ON t1_D3880.i = t2_D3880.i " +
                "group by t1_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		//Try RIGHT OUTER JOIN with ON clause.
        assertStatementError("42X03", s,
                "select i from t1_D3880 " +
				"RIGHT OUTER JOIN t2_D3880 ON t1_D3880.i = t2_D3880.i " +
                "group by t1_D3880.i");
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"RIGHT OUTER JOIN t2_D3880 ON t1_D3880.i = t2_D3880.i " +
                "group by t1_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);

		//Test group by on a column which is not part of SELECT query (query 
		// uses USING clause). We see the incorrect behavior where the group 
		// by does not raise an error for using 
		// leftTable(orRightTable).joinColumn even though that column is not 
		// part of the SELECT list. Just a note that ON clause is not allowed 
		// on CROSS and NATURAL JOINS.
		//
		//Try INNER JOIN with USING clause.
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t1_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"inner join t2_D3880 USING(i) group by t1_D3880.i");
		expRs = new String[][] {{"1"},{"2"}};
		JDBC.assertFullResultSet(rs,expRs);
		//Following query does not allow t2_D3880.i in group by clause
		// because join column i the select query gets associated with
		// left table in case of INNER JOIN.
        assertStatementError("42Y36", s,
        		"select i from t1_D3880 " +
				"inner join t2_D3880 USING(i) group by t2_D3880.i");

        //Test the GROUP BY problem with LEFT OUTER JOIN and USING clause.
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t1_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"LEFT OUTER JOIN t2_D3880 USING(i) GROUP BY t1_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		//Following query does not allow t2_D3880.i in group by clause
		// because join column i the select query gets associated with
		// left table in case of LEFT OUTER JOIN.
        assertStatementError("42Y36", s,
        		"select i from t1_D3880 " +
				"LEFT OUTER JOIN t2_D3880 USING(i) GROUP BY t2_D3880.i");

        //Test the GROUP BY problem with RIGHT OUTER JOIN and USING clause.
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t2_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"RIGHT OUTER JOIN t2_D3880 USING(i) GROUP BY t2_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		//Following query does not allow t1_D3880.i in group by clause
		// because join column i the select query gets associated with
		// right table in case of RIGHT OUTER JOIN.
        assertStatementError("42Y36", s,
        		"select i from t1_D3880 " +
				"RIGHT OUTER JOIN t2_D3880 USING(i) GROUP BY t1_D3880.i");
		
		//The correct queries for GROUP BY and USING clause
		//
		//INNER JOIN with USING clause.
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"inner join t2_D3880 USING(i) group by t1_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		//GROUP BY with LEFT OUTER JOIN and USING clause.
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"LEFT OUTER JOIN t2_D3880 USING(i) GROUP BY t1_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		//GROUP BY with RIGHT OUTER JOIN and USING clause.
		rs = s.executeQuery("select t2_D3880.i from t1_D3880 " +
				"RIGHT OUTER JOIN t2_D3880 USING(i) GROUP BY t2_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		
		//Test group by on a column which is not part of SELECT query (query 
		// uses NATURAL JOIN). We see the incorrect behavior where the group 
		// by does not raise an error for using 		
		// leftTable(orRightTable).joinColumn even though that column is not
		// part of the SELECT list. Just a note that a CROSS JOIN can't be a 
		// NATURAL JOIN.
		//
		//Try the GROUP BY problem with NATURAL INNER JOIN
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t1_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"NATURAL inner join t2_D3880 group by t1_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		//Test the GROUP BY problem with NATURAL LEFT OUTER JOIN
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t1_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"NATURAL LEFT OUTER JOIN t2_D3880 GROUP BY t1_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		//Test the GROUP BY problem with NATURAL RIGHT OUTER JOIN
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t2_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"NATURAL RIGHT OUTER JOIN t2_D3880 GROUP BY t2_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		
		//The correct queries for GROUP BY and NATURAL JOIN
		//
		//NATURAL INNER JOIN
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"NATURAL inner join t2_D3880 group by t1_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		//NATURAL LEFT OUTER JOIN
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"NATURAL LEFT OUTER JOIN t2_D3880 GROUP BY t1_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);
		//NATURAL RIGHT OUTER JOIN
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t2_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select t2_D3880.i from t1_D3880 " +
				"NATURAL RIGHT OUTER JOIN t2_D3880 GROUP BY t2_D3880.i");
		JDBC.assertFullResultSet(rs,expRs);

		//Similar query for HAVING clause. HAVING clause should not be able
		// to use a column which is not part of the SELECT column list.
		// Doing this testing with USING clause
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t1_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"inner join t2_D3880 USING(i) group by t1_D3880.i " +
				"HAVING t1_D3880.i > 1");
		expRs = new String[][] {{"2"}};
		JDBC.assertFullResultSet(rs,expRs);
		// Doing the same test as above with NATURAL JOIN
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t1_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"NATURAL inner join  t2_D3880 group by t1_D3880.i " +
				"HAVING t1_D3880.i > 1");
		expRs = new String[][] {{"2"}};
		JDBC.assertFullResultSet(rs,expRs);
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t1_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"LEFT OUTER join t2_D3880 USING(i) group by t1_D3880.i " +
				"HAVING t1_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t1_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"NATURAL LEFT OUTER join t2_D3880 group by t1_D3880.i " +
				"HAVING t1_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t2_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"RIGHT OUTER join t2_D3880 USING(i) group by t2_D3880.i " +
				"HAVING t2_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
		//Following query should have given compile time error. 
		//Once DERBY-5613 is fixed, this query will run into compile time
		// error for using t2_D3880.i in group by clause because that column
		// is not part of the SELECT list. 
		rs = s.executeQuery("select i from t1_D3880 " +
				"NATURAL RIGHT OUTER join t2_D3880 group by t2_D3880.i " +
				"HAVING t2_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
		
		//The correct query for HAVING should be written as follows
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"inner join t2_D3880 USING(i) group by t1_D3880.i " +
				"HAVING t1_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"NATURAL inner join  t2_D3880 group by t1_D3880.i " +
				"HAVING t1_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"LEFT OUTER join t2_D3880 USING(i) group by t1_D3880.i " +
				"HAVING t1_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
		rs = s.executeQuery("select t1_D3880.i from t1_D3880 " +
				"NATURAL LEFT OUTER join t2_D3880 group by t1_D3880.i " +
				"HAVING t1_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
		rs = s.executeQuery("select t2_D3880.i from t1_D3880 " +
				"RIGHT OUTER join t2_D3880 USING(i) group by t2_D3880.i " +
				"HAVING t2_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
		rs = s.executeQuery("select t2_D3880.i from t1_D3880 " +
				"NATURAL RIGHT OUTER join t2_D3880 group by t2_D3880.i " +
				"HAVING t2_D3880.i > 1");
		JDBC.assertFullResultSet(rs,expRs);
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

	public void testHavingWithInnerJoinDerby3880() throws SQLException {
		Statement s = createStatement();
		ResultSet rs = s.executeQuery("select   t1_D3880.i, avg(t2_D3880.i2)  from t1_D3880 " +
				"inner join t2_D3880 on (t1_D3880.i = t2_D3880.i) group by t1_D3880.i having "  +
						"avg(t2_D3880.i2) > 0");
		String[][] expRs = new String[][] {{"1","15"},{"2","30"}};
		JDBC.assertFullResultSet(rs,expRs);

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
						{3.0, 4},
						{4.0, 2},
						{7.0, 1}},
				false);

		rs = s.executeQuery(
			"select b, count(*) from yy where a=5 or a=2 " +
			"group by b order by count(*) asc");

		JDBC.assertFullResultSet(
				rs,
				new Object[][]{
						{7.0, 1},
						{4.0, 2},
						{3.0, 4}},
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

    /**
     * Test that GROUP BY can be used in the sub-expressions of set operations
     * (DERBY-3764).
     */
    public void testSetOperationsAndGroupBy() throws SQLException {
        // turn off auto-commit to clean up test data automatically
        getConnection().setAutoCommit(false);

        Statement s = createStatement();

        s.execute("CREATE TABLE D3764A (A1 INTEGER, A2 VARCHAR(10))");

        int[] valuesA = { 1, 2, 3, 4, 5, 6, 5, 3, 1 };

        PreparedStatement insertA =
            prepareStatement("INSERT INTO D3764A (A1) VALUES (?)");

        for (int i = 0; i < valuesA.length; i++) {
            insertA.setInt(1, valuesA[i]);
            insertA.executeUpdate();
        }

        s.execute("CREATE TABLE D3764B (B1 INTEGER, B2 VARCHAR(10))");

        int[] valuesB = { 1, 2, 3, 3, 7, 9 };

        PreparedStatement insertB =
            prepareStatement("INSERT INTO D3764B (B1) VALUES (?)");

        for (int i = 0; i < valuesB.length; i++) {
            insertB.setInt(1, valuesB[i]);
            insertB.executeUpdate();
        }

        // Define some queries with one result column and a varying number of
        // grouping columns.
        String[] singleColumnQueries = {
            "SELECT A1 FROM D3764A",
            "SELECT COUNT(A1) FROM D3764A",
            "SELECT COUNT(A1) FROM D3764A GROUP BY A1",
            "SELECT COUNT(A1) FROM D3764A GROUP BY A2",
            "SELECT COUNT(A1) FROM D3764A GROUP BY A1, A2",
            "SELECT B1 FROM D3764B",
            "SELECT COUNT(B1) FROM D3764B",
            "SELECT COUNT(B1) FROM D3764B GROUP BY B1",
            "SELECT COUNT(B1) FROM D3764B GROUP BY B2",
            "SELECT COUNT(B1) FROM D3764B GROUP BY B1, B2",
            "VALUES 1, 2, 3, 4",
        };

        // The expected results from the queries above.
        String[][][] singleColumnExpected = {
            { {"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"5"}, {"3"}, {"1"} },
            { { Integer.toString(valuesA.length) } },
            { {"2"}, {"1"}, {"2"}, {"1"}, {"2"}, {"1"} },
            { { Integer.toString(valuesA.length) } },
            { {"2"}, {"1"}, {"2"}, {"1"}, {"2"}, {"1"} },
            { {"1"}, {"2"}, {"3"}, {"3"}, {"7"}, {"9"} },
            { { Integer.toString(valuesB.length) } },
            { {"1"}, {"1"}, {"2"}, {"1"}, {"1"} },
            { { Integer.toString(valuesB.length) } },
            { {"1"}, {"1"}, {"2"}, {"1"}, {"1"} },
            { {"1"}, {"2"}, {"3"}, {"4"} },
        };

        // Test all the set operations with all the combinations of the queries
        // above.
        doAllSetOperations(s, singleColumnQueries, singleColumnExpected);

        // Define some queries with two result columns and a varying number of
        // grouping columns.
        String[] twoColumnQueries = {
            "SELECT A1-1, A1+1 FROM D3764A",
            "SELECT COUNT(A1), A1 FROM D3764A GROUP BY A1",
            "SELECT COUNT(A1), LENGTH(A2) FROM D3764A GROUP BY A2",
            "SELECT COUNT(A1), A1 FROM D3764A GROUP BY A1, A2",
            "SELECT B1-1, B1+1 FROM D3764B",
            "SELECT COUNT(B1), B1 FROM D3764B GROUP BY B1",
            "SELECT COUNT(B1), LENGTH(B2) FROM D3764B GROUP BY B2",
            "SELECT COUNT(B1), B1 FROM D3764B GROUP BY B1, B2",
            "VALUES (1, 2), (3, 4)",
        };

        // The expected results from the queries above.
        String[][][] twoColumnExpected = {
            { {"0","2"}, {"1","3"}, {"2","4"}, {"3","5"}, {"4","6"},
              {"5","7"}, {"4","6"}, {"2","4"}, {"0","2"} },
            { {"2","1"}, {"1","2"}, {"2","3"}, {"1","4"}, {"2","5"},
              {"1","6"} },
            { { Integer.toString(valuesA.length), null } },
            { {"2","1"}, {"1","2"}, {"2","3"}, {"1","4"}, {"2","5"},
              {"1","6"} },
            { {"0","2"}, {"1","3"}, {"2","4"}, {"2","4"}, {"6","8"},
              {"8","10"} },
            { {"1","1"}, {"1","2"}, {"2","3"}, {"1","7"}, {"1","9"} },
            { { Integer.toString(valuesB.length), null } },
            { {"1","1"}, {"1","2"}, {"2","3"}, {"1","7"}, {"1","9"} },
            { {"1","2"}, {"3","4"} },
        };

        // Test all the set operations with all the combinations of the queries
        // above.
        doAllSetOperations(s, twoColumnQueries, twoColumnExpected);

        // Test that set operations cannot be used on sub-queries with
        // different number of columns.
        assertSetOpErrors("42X58", s, singleColumnQueries, twoColumnQueries);
        assertSetOpErrors("42X58", s, twoColumnQueries, singleColumnQueries);
    }

    /**
     * Try all set operations (UNION [ALL], EXCEPT [ALL], INTERSECT [ALL]) on
     * all combinations of the specified queries.
     *
     * @param s the statement used to execute the queries
     * @param queries the different queries to use, all of which must be union
     * compatible
     * @param expectedResults the expected results from the different queries
     */
    private static void doAllSetOperations(Statement s, String[] queries,
                                           String[][][] expectedResults)
            throws SQLException {

        assertEquals(queries.length, expectedResults.length);

        for (int i = 0; i < queries.length; i++) {
            final String query1 = queries[i];
            final List<List<String>> rows1 =
                    resultArrayToList(expectedResults[i]);

            for (int j = 0; j < queries.length; j++) {
                final String query2 = queries[j];
                final List<List<String>> rows2 =
                        resultArrayToList(expectedResults[j]);

                String query = query1 + " UNION " + query2;
                String[][] rows = union(rows1, rows2, false);
                JDBC.assertUnorderedResultSet(s.executeQuery(query), rows);

                query = query1 + " UNION ALL " + query2;
                rows = union(rows1, rows2, true);
                JDBC.assertUnorderedResultSet(s.executeQuery(query), rows);

                query = query1 + " EXCEPT " + query2;
                rows = except(rows1, rows2, false);
                JDBC.assertUnorderedResultSet(s.executeQuery(query), rows);

                query = query1 + " EXCEPT ALL " + query2;
                rows = except(rows1, rows2, true);
                JDBC.assertUnorderedResultSet(s.executeQuery(query), rows);

                query = query1 + " INTERSECT " + query2;
                rows = intersect(rows1, rows2, false);
                JDBC.assertUnorderedResultSet(s.executeQuery(query), rows);

                query = query1 + " INTERSECT ALL " + query2;
                rows = intersect(rows1, rows2, true);
                JDBC.assertUnorderedResultSet(s.executeQuery(query), rows);
            }
        }
    }

    /**
     * Try all set operations with queries from {@code queries1} in the left
     * operand and queries from {@code queries2} in the right operand. All the
     * set operations are expected to fail with the same SQLState.
     *
     * @param sqlState the expected SQLState
     * @param s the statement used to execute the queries
     * @param queries1 queries to use as the left operand
     * @param queries2 queries to use as the right operand
     */
    private static void assertSetOpErrors(String sqlState,
                                          Statement s,
                                          String[] queries1,
                                          String[] queries2)
            throws SQLException {

        final String[] operators = {
            " UNION ", " UNION ALL ", " EXCEPT ", " EXCEPT ALL ",
            " INTERSECT ", " INTERSECT ALL "
        };

        for (int i = 0; i < queries1.length; i++) {
            for (int j = 0; j < queries2.length; j++) {
                for (int k = 0; k < operators.length; k++) {
                    assertStatementError(
                        sqlState, s, queries1[i] + operators[k] + queries2[j]);
                }
            }
        }
    }

    /**
     * Find the union between two collections of rows (each row is a list of
     * strings). Return the union as an array of string arrays.
     *
     * @param rows1 the first collection of rows
     * @param rows2 the second collection of rows
     * @param all whether or not bag semantics (as in UNION ALL) should be used
     * instead of set semantics
     * @return the union of {@code rows1} and {@code rows2}, as a {@code
     * String[][]}
     */
    private static String[][] union(Collection<List<String>> rows1,
                                    Collection<List<String>> rows2,
                                    boolean all) {
        Collection<List<String>> bagOrSet = newBagOrSet(all);
        bagOrSet.addAll(rows1);
        bagOrSet.addAll(rows2);
        return toResultArray(bagOrSet);
    }

    /**
     * Find the difference between two collections of rows (each row is a list
     * of strings). Return the difference as an array of string arrays.
     *
     * @param rows1 the first operand to the set difference operator
     * @param rows2 the second operand to the set difference operator
     * @param all whether or not bag semantics (as in EXCEPT ALL) should be
     * used instead of set semantics
     * @return the difference between {@code rows1} and {@code rows2}, as a
     * {@code String[][]}
     */
    private static String[][] except(Collection<List<String>> rows1,
                                     Collection<List<String>> rows2,
                                     boolean all) {
        Collection<List<String>> bagOrSet = newBagOrSet(all);
        bagOrSet.addAll(rows1);
        // could use removeAll() for sets, but need other behaviour for bags
        for (List<String> row : rows2) {
            bagOrSet.remove(row);
        }
        return toResultArray(bagOrSet);
    }

    /**
     * Find the intersection between two collections of rows (each row is a
     * list of strings). Return the intersection as an array of string arrays.
     *
     * @param rows1 the first collection of rows
     * @param rows2 the second collection of rows
     * @param all whether or not bag semantics (as in INTERSECT ALL) should be
     * used instead of set semantics
     * @return the intersection between {@code rows1} and {@code rows2}, as a
     * {@code String[][]}
     */
    private static String[][] intersect(Collection<List<String>> rows1,
                                        Collection<List<String>> rows2,
                                        boolean all) {
        Collection<List<String>> bagOrSet = newBagOrSet(all);
        List<List<String>> copyOfRows2 = new ArrayList<List<String>>(rows2);
        // could use retainAll() for sets, but need other behaviour for bags
        for (List<String> x : rows1) {
            if (copyOfRows2.remove(x)) {
                // x is present in both of the collections, add it
                bagOrSet.add(x);
            }
        }
        return toResultArray(bagOrSet);
    }

    /**
     * Create a {@code Collection} that can be used as a bag or a set.
     *
     * @param bag tells whether or not the collection should be a bag
     * @return a {@code List} if a bag is requested, or a {@code Set} otherwise
     */
    private static Collection<List<String>> newBagOrSet(boolean bag) {
        if (bag) {
            return new ArrayList<List<String>>();
        } else {
            return new HashSet<List<String>>();
        }
    }

    /**
     * Convert a {@code Collection} of rows to an array of string arrays that
     * can be passed as an argument with expected results to
     * {@link JDBC#assertUnorderedResultSet(ResultSet,String[][])}.
     *
     * @param rows a collection of rows, where each row is a list of strings
     * @return a {@code String[][]} containing the same values as {@code rows}
     */
    private static String[][] toResultArray(Collection<List<String>> rows) {
        String[][] results = new String[rows.size()][];
        Iterator<List<String>> it = rows.iterator();
        for (int i = 0; i < results.length; i++) {
            List<String> row = it.next();
            results[i] = row.toArray(new String[row.size()]);
        }
        return results;
    }

    /**
     * Return a list of lists containing the same values as the specified array
     * of string arrays. This method can be used to make it easier to perform
     * set operations on a two-dimensional array of strings.
     *
     * @param results a two dimensional array of strings (typically expected
     * results from a query}
     * @return the values of {@code results} in a list of lists
     */
    private static List<List<String>> resultArrayToList(String[][] results) {
        ArrayList<List<String>> rows =
                new ArrayList<List<String>>(results.length);
        for (int i = 0; i < results.length; i++) {
            rows.add(Arrays.asList(results[i]));
        }
        return rows;
    }

    /**
      * DERBY-3904: Min/Max optimization needs to be aware of joins.
      */
    public void testDerby3904MinMaxOptimization() throws SQLException
    {
        Statement s = createStatement();

        JDBC.assertFullResultSet(
                s.executeQuery("SELECT d3904_T1.D1 " +
					"FROM d3904_T1 LEFT JOIN d3904_T2 " +
				    "ON d3904_T1.D1 = d3904_T2.D2 " +
					"WHERE d3904_T2.D2 IS NULL"), 
            new String[][] {  {"2008-10-02"} } );
        JDBC.assertFullResultSet(
                s.executeQuery("SELECT MAX( d3904_T1.D1 ) as D " +
					"FROM d3904_T1 WHERE d3904_T1.D1 NOT IN " +
					"( SELECT d3904_T2.D2 FROM d3904_T2 )"), 
            new String[][] {  {"2008-10-02"} } );
		//
		// In DERBY-3904, this next query fails with a null pointer
		// exception because GroupByNode doesn't realize that there
		// is a join involved here
		//
        JDBC.assertFullResultSet(
                s.executeQuery("SELECT MAX( d3904_T1.D1 ) AS D " +
					"FROM d3904_T1 LEFT JOIN d3904_T2 " +
					"ON d3904_T1.D1 = d3904_T2.D2 " +
					"WHERE d3904_T2.D2 IS NULL"),
            new String[][] {  {"2008-10-02"} } );

		// Verify that the min/max optimization still works for the
		// simple query SELECT MAX(D1) FROM T1:
		s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        JDBC.assertFullResultSet(
                s.executeQuery("SELECT MAX(D1) FROM D3904_T1"),
            new String[][] {  {"2008-10-02"} } );
		RuntimeStatisticsParser rtsp =
			SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedLastKeyIndexScan());
		assertFalse(rtsp.usedIndexRowToBaseRow());

		// A form of the Beetle 4423 query:
        JDBC.assertFullResultSet(
                s.executeQuery("SELECT MAX(D1) " +
					"FROM d3904_T1, D3904_T2 WHERE d3904_T1.D1='2008-10-02'"),
            new String[][] {  {"2008-10-02"} } );
	}


    /**
     * Test aggregate used in group by query.
     * @throws SQLException
     */
    public void testDerby3631AggregateInGroupByQuery() throws SQLException {
         Statement s = createStatement();
         ResultSet rs = s.executeQuery("SELECT GroupCol, MAXOF2(CAST(SUM(Value1) AS DOUBLE), CAST(SUM(Value2) AS DOUBLE)) AS MaxOf2 FROM Testd3631 GROUP BY GroupCol ");
         JDBC.assertFullResultSet(rs, new String[][] {{"1","5.0"},{"2","-3.0"}});
         rs = s.executeQuery("SELECT GroupCol, MAXOF2(SUM(Value1), SUM(Value2)) AS MaxOf2 FROM Testd3631 GROUP BY GroupCol");
         JDBC.assertFullResultSet(rs, new String[][] {{"1","5.0"},{"2","-3.0"}});
    }


    /**
     * Test aggregate function on a GROUP BY column also present in a HAVING
     * clause.  Iff the GROUP BY column is not the first column in the table,
     * this would fail before DERBY-4071 was fixed.
     *
     * @throws SQLException
     */
    public void testDerby4071AggregateOnGroupByColumnInHaving() throws SQLException {
         Statement s = createStatement();
         ResultSet rs = s.executeQuery("SELECT MAX(i), COUNT(T.V) FROM d4071 T " +
                                       "    GROUP BY T.V HAVING COUNT(T.V) > 1");
         
         JDBC.assertFullResultSet(rs, new String[][] {{"4","2"},{"5","2"}});
    }

    /**
     * GROUP BY in an IN-subquery inside HAVING clause whose select list is
     * subset of group by columns.
     *
     * @throws SQLException
     */
    public void testDerby4450() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        ResultSet rs;

        s.executeUpdate(
            "create table tt(i int not null," +
            "               j int, k int)");
        s.executeUpdate(
            "insert into tt values " +
            "    (1,10,1), (1,40,1),(3,45,1),(4,46,1),(5,90,1)");

        rs = s.executeQuery(
            "select sum(j) from tt group by i having i " +
            "                     in (select i from tt group by i,j )");

        JDBC.assertFullResultSet(rs, new String[][] {
                {"50"},{"45"},{"46"},{"90"}});
        rollback();
    }


    /**
     * HAVING with an aggregate function in presence of join flattening
     * DERBY-4698. See also DERBY-3880.
     * @throws SQLException
     */
    public void testDerby4698() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        ResultSet rs;

        s.executeUpdate(
            "create table usr ( user_id  bigint primary key," +
            "                  deleted  char(1) not null)");
        s.executeUpdate(
            "create table  user_account ( user_account_id bigint primary key," +
            "                             user_id  bigint not null," +
            "                             account_id  bigint not null)");

        s.executeUpdate(
            "CREATE TABLE  ACCOUNT  (ACCOUNT_ID  BIGINT PRIMARY KEY," +
            "                        ACCOUNT_TYPE  VARCHAR(10) NOT NULL," +
            "                        BALANCE  DOUBLE NOT NULL)");

        s.executeUpdate(
            "insert into usr values " +
            "   (3003, 'Y'), (3004, 'N'), (1001, 'N'), (1002, 'Y')," +
            "   (1003, 'N'), (1004, 'N'), (1005, 'N'), (1006, 'N')," +
            "   (1007, 'N'), (1008, 'N'), (2002, 'N')");

        s.executeUpdate(
            "insert into user_account values " +
            "  (4004, 3003, 9009), (4005, 3004, 9010), (5005, 1001, 10010)," +
            "  (5006, 3004, 10011), (5007, 2002, 10012), (5008, 1003, 10013)," +
            "  (5009, 1004, 10014), (5010, 1005, 10015), (5011, 1006, 10016)," +
            "  (5012, 1007, 10017), (5013, 1008, 10018), (6006, 1001, 11011)," +
            "  (6007, 3004, 11012), (6008, 2002, 11013), (6009, 1003, 11014)," +
            "  (6010, 1004, 11015), (6011, 1005, 11016), (6012, 1006, 11017)," +
            "  (6013, 1007, 11018), (6014, 1008, 11019), (1001, 1001, 1002)," +
            "  (1002, 1002, 1003), (1003, 1003, 1004), (1004, 1004, 1005)," +
            "  (1005, 1005, 1006), (1006, 1006, 1007), (1007, 1007, 1008)," +
            "  (1008, 1008, 1009), (1009, 1004, 1010), (2002, 1004, 6006)," +
            "  (3003, 2002, 7007)");

        s.executeUpdate(
            "insert into account values " +
            " (9009, 'USER', 12.5), (9010, 'USER', 12.5)," +
            " (10010, 'USER-01', 0.0), (10011, 'USER-01', 0.0)," +
            " (10012, 'USER-01', 0.0), (10013, 'USER-01', 0.0)," +
            " (10014, 'USER-01', 99.0), (10015, 'USER-01', 0.0)," +
            " (10016, 'USER-01', 0.0), (10017, 'USER-01', 0.0)," +
            " (10018, 'USER-01', 0.0), (11011, 'USER-02', 0.0)," +
            " (11012, 'USER-02', 0.0), (11013, 'USER-02', 0.0)," +
            " (11014, 'USER-02', 0.0), (11015, 'USER-02', 0.0)," +
            " (11016, 'USER-02', 0.0), (11017, 'USER-02', 0.0)," +
            " (11018, 'USER-02', 0.0), (11019, 'USER-02', 0.0)," +
            " (1002, 'USER', 10.0), (1003, 'USER', 80.31)," +
            " (1004, 'USER', 10.0), (1005, 'USER', 161.7)," +
            " (1006, 'USER', 10.0), (1007, 'USER', 10.0)," +
            " (1008, 'USER', 10.0), (1009, 'USER', 10.0)," +
            " (7007, 'USER', 11.0)");

        rs = s.executeQuery(
            "SELECT user0_.user_id AS col_0_0_," +
            "   SUM(account2_.balance) AS col_1_0_ " +
            "   FROM usr user0_ " +
            "   INNER JOIN user_account accountlin1_ " +
            "   ON user0_.user_id = accountlin1_.user_id " +
            "   INNER JOIN account account2_ " +
            "   ON accountlin1_.account_id = account2_.account_id " +
            "WHERE user0_.deleted = 'N' " +
            "      AND ( account2_.account_type IN ( 'USER-01', 'USER' ) )" +
            "GROUP BY user0_.user_id " +
            "HAVING SUM(account2_.balance) >= 100.0 ");

        JDBC.assertFullResultSet(rs, new String[][] {
                {"1004", "260.7"} });

            rollback();
    }


    /**
     * DISTINCT aggregates in result sets which are opened multiple times.
     * DERBY-5584.
     * @throws SQLException
     */
    public void testDerby5584()
	throws SQLException
    {
        setAutoCommit(false);
        Statement s = createStatement();
        ResultSet rs;

        s.executeUpdate(
		"CREATE TABLE TEST_5 (" +
		"       profile_id INTEGER NOT NULL," +
		"       group_ref INTEGER NOT NULL," +
		"       matched_count INTEGER NOT NULL )"); 

        s.executeUpdate(
		"CREATE TABLE TEST_6 ( " +
		"       profile_id INTEGER NOT NULL, " +
		"       group_ref INTEGER NOT NULL, " +
		"       matched_count INTEGER NOT NULL )"); 

        s.executeUpdate( "insert into test_5 values (1, 10000, 1)" ); 
        s.executeUpdate( "insert into test_5 values (2, 10000, 2)" ); 

        s.executeUpdate( "insert into test_6 values (1, 10000, 1)" ); 
        s.executeUpdate( "insert into test_6 values (2, 10000, 2)" );

        rs = s.executeQuery( "SELECT ps1.group_ref," +
		"COUNT(DISTINCT ps1.matched_count) AS matched_count" +
		" FROM test_5 ps1 " +
		" GROUP BY ps1.group_ref, ps1.profile_id" );
        JDBC.assertFullResultSet(rs, new String[][] {
                {"10000", "1"},
                {"10000", "1"}
	});

        rs = s.executeQuery( "SELECT ps1.group_ref," +
		"COUNT(ps1.matched_count) AS matched_count" +
		" FROM test_5 ps1 " +
		" GROUP BY ps1.group_ref, ps1.profile_id" );
        JDBC.assertFullResultSet(rs, new String[][] {
                {"10000", "1"},
                {"10000", "1"}
	});

	String cartProdWithDISTINCTsubqueries = " SELECT *" +
		" FROM " +
		" (SELECT ps1.group_ref, ps1.profile_id, " +
		"         COUNT(DISTINCT ps1.matched_count) AS matched_count " +
		"  FROM test_5 ps1" +
		"  GROUP BY ps1.group_ref, ps1.profile_id " +
		" ) a, " +
		" (SELECT ps2.group_ref, ps2.profile_id, " +
		"         COUNT( DISTINCT ps2.matched_count) AS matched_count" +
		"  FROM test_6 ps2" +
		"  GROUP BY ps2.group_ref, ps2.profile_id " +
		") b ";

	String cartProdWithSubqueries = " SELECT * " +
		" FROM " +
		" (SELECT ps1.group_ref, ps1.profile_id, " +
		"         COUNT(ps1.matched_count) AS matched_count " +
		"  FROM test_5 ps1 " +
		"  GROUP BY ps1.group_ref, ps1.profile_id " +
		") a, " +
		" (SELECT ps2.group_ref, ps2.profile_id, " +
		"         COUNT( ps2.matched_count) AS matched_count " +
		"  FROM test_6 ps2 " +
		"  GROUP BY ps2.group_ref, ps2.profile_id " +
		") b ";

	String cartProdWithOrderBySubqueries = "SELECT * " +
		" FROM " +
		" (SELECT ps1.group_ref, ps1.profile_id " +
		"  FROM test_5 ps1 ORDER BY profile_id fetch first 3 rows only) a, " +
		" (SELECT ps2.group_ref, ps2.profile_id " +
		"  FROM test_6 ps2 ORDER BY PROFILE_ID fetch first 2 rows only) b "; 


	rs = s.executeQuery( cartProdWithDISTINCTsubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "1"},
		{"10000", "1", "1", "10000", "2", "1"},
		{"10000", "2", "1", "10000", "1", "1"},
		{"10000", "2", "1", "10000", "2", "1"}
	});

	rs = s.executeQuery( cartProdWithSubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "1"},
		{"10000", "1", "1", "10000", "2", "1"},
		{"10000", "2", "1", "10000", "1", "1"},
		{"10000", "2", "1", "10000", "2", "1"}
	});

        s.executeUpdate( "insert into test_5 values (3, 10000, 3)" ); 

	rs = s.executeQuery( cartProdWithDISTINCTsubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "1"},
		{"10000", "1", "1", "10000", "2", "1"},
		{"10000", "2", "1", "10000", "1", "1"},
		{"10000", "2", "1", "10000", "2", "1"},
		{"10000", "3", "1", "10000", "1", "1"},
		{"10000", "3", "1", "10000", "2", "1"}
	});

	rs = s.executeQuery( cartProdWithSubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "1"},
		{"10000", "1", "1", "10000", "2", "1"},
		{"10000", "2", "1", "10000", "1", "1"},
		{"10000", "2", "1", "10000", "2", "1"},
		{"10000", "3", "1", "10000", "1", "1"},
		{"10000", "3", "1", "10000", "2", "1"}
	});

        s.executeUpdate( "insert into test_5 values (4, 10000, 4) "); 
        s.executeUpdate( "insert into test_6 values (3, 10000, 3) "); 

	// NOTE: At this point,
	//   test_5 contains:		test_6 contains:
	//	1, 10000, 1			1, 10000, 1
	//	2, 10000, 2			2, 10000, 2
	//	3, 10000, 3			3, 10000, 3
	//	4, 10000, 4

	rs = s.executeQuery( cartProdWithDISTINCTsubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "1"},
		{"10000", "1", "1", "10000", "2", "1"},
		{"10000", "1", "1", "10000", "3", "1"},
		{"10000", "2", "1", "10000", "1", "1"},
		{"10000", "2", "1", "10000", "2", "1"},
		{"10000", "2", "1", "10000", "3", "1"},
		{"10000", "3", "1", "10000", "1", "1"},
		{"10000", "3", "1", "10000", "2", "1"},
		{"10000", "3", "1", "10000", "3", "1"},
		{"10000", "4", "1", "10000", "1", "1"},
		{"10000", "4", "1", "10000", "2", "1"},
		{"10000", "4", "1", "10000", "3", "1"}
	});

	rs = s.executeQuery( cartProdWithSubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "1"},
		{"10000", "1", "1", "10000", "2", "1"},
		{"10000", "1", "1", "10000", "3", "1"},
		{"10000", "2", "1", "10000", "1", "1"},
		{"10000", "2", "1", "10000", "2", "1"},
		{"10000", "2", "1", "10000", "3", "1"},
		{"10000", "3", "1", "10000", "1", "1"},
		{"10000", "3", "1", "10000", "2", "1"},
		{"10000", "3", "1", "10000", "3", "1"},
		{"10000", "4", "1", "10000", "1", "1"},
		{"10000", "4", "1", "10000", "2", "1"},
		{"10000", "4", "1", "10000", "3", "1"}
	});

        s.executeUpdate( "insert into test_6 values (2, 10000, 1) "); 

	rs = s.executeQuery( cartProdWithDISTINCTsubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "1"},
		{"10000", "1", "1", "10000", "2", "2"},
		{"10000", "1", "1", "10000", "3", "1"},
		{"10000", "2", "1", "10000", "1", "1"},
		{"10000", "2", "1", "10000", "2", "2"},
		{"10000", "2", "1", "10000", "3", "1"},
		{"10000", "3", "1", "10000", "1", "1"},
		{"10000", "3", "1", "10000", "2", "2"},
		{"10000", "3", "1", "10000", "3", "1"},
		{"10000", "4", "1", "10000", "1", "1"},
		{"10000", "4", "1", "10000", "2", "2"},
		{"10000", "4", "1", "10000", "3", "1"}
	});

	rs = s.executeQuery( cartProdWithSubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "1"},
		{"10000", "1", "1", "10000", "2", "2"},
		{"10000", "1", "1", "10000", "3", "1"},
		{"10000", "2", "1", "10000", "1", "1"},
		{"10000", "2", "1", "10000", "2", "2"},
		{"10000", "2", "1", "10000", "3", "1"},
		{"10000", "3", "1", "10000", "1", "1"},
		{"10000", "3", "1", "10000", "2", "2"},
		{"10000", "3", "1", "10000", "3", "1"},
		{"10000", "4", "1", "10000", "1", "1"},
		{"10000", "4", "1", "10000", "2", "2"},
		{"10000", "4", "1", "10000", "3", "1"}
	});

	// Now introduce some duplicate values so that the DISTINCT
	// aggregates have some work to do


        s.executeUpdate( "insert into test_6 values (1, 10000, 1) "); 
        s.executeUpdate( "insert into test_6 values (2, 10000, 2) "); 

	// NOTE: At this point,
	//   test_5 contains:		test_6 contains:
	//	1, 10000, 1			1, 10000, 1 (2 vals, 1 distinct)
	//	2, 10000, 2			1, 10000, 1 
	//	3, 10000, 3			2, 10000, 1 (3 vals, 2 distinct)
	//	4, 10000, 4			2, 10000, 2
	//	 				2, 10000, 2
	//	 				3, 10000, 2 (1 val, 1 distinct)

	rs = s.executeQuery( cartProdWithDISTINCTsubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "1"},
		{"10000", "1", "1", "10000", "2", "2"},
		{"10000", "1", "1", "10000", "3", "1"},
		{"10000", "2", "1", "10000", "1", "1"},
		{"10000", "2", "1", "10000", "2", "2"},
		{"10000", "2", "1", "10000", "3", "1"},
		{"10000", "3", "1", "10000", "1", "1"},
		{"10000", "3", "1", "10000", "2", "2"},
		{"10000", "3", "1", "10000", "3", "1"},
		{"10000", "4", "1", "10000", "1", "1"},
		{"10000", "4", "1", "10000", "2", "2"},
		{"10000", "4", "1", "10000", "3", "1"}
	});

	rs = s.executeQuery( cartProdWithSubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "1", "10000", "1", "2"},
		{"10000", "1", "1", "10000", "2", "3"},
		{"10000", "1", "1", "10000", "3", "1"},
		{"10000", "2", "1", "10000", "1", "2"},
		{"10000", "2", "1", "10000", "2", "3"},
		{"10000", "2", "1", "10000", "3", "1"},
		{"10000", "3", "1", "10000", "1", "2"},
		{"10000", "3", "1", "10000", "2", "3"},
		{"10000", "3", "1", "10000", "3", "1"},
		{"10000", "4", "1", "10000", "1", "2"},
		{"10000", "4", "1", "10000", "2", "3"},
		{"10000", "4", "1", "10000", "3", "1"}
	});

	rs = s.executeQuery( cartProdWithOrderBySubqueries );
        JDBC.assertFullResultSet(rs, new String[][] {
		{"10000", "1", "10000", "1"},
		{"10000", "1", "10000", "1"},
		{"10000", "2", "10000", "1"},
		{"10000", "2", "10000", "1"},
		{"10000", "3", "10000", "1"},
		{"10000", "3", "10000", "1"}
	});

            rollback();
    }

    /**
     * GROUP BY on an expression in a JOIN used to trigger an assert failure.
     * See DERBY-5313.
     */
    public void testDerby5313() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table d5313_1(a int, b int)");
        s.execute("create table d5313_2(b int, c int)");
        s.execute("insert into d5313_1 values (3, 1), (2, 2), (3, 3)");
        s.execute("insert into d5313_2 values (0, 1), (1, 2), (2, 3), (3, 4)");

        JDBC.assertUnorderedResultSet(
                s.executeQuery("select a+b, sum(c) from "
                             + "d5313_1 natural join d5313_2 group by a+b"),
                new String[][] { { "4", "5" }, { "6", "4" } });

        JDBC.assertUnorderedResultSet(
                s.executeQuery("select case when a=2 then 1 else 2 end, sum(c) "
                             + "from d5313_1 natural join d5313_2 group by "
                             + "case when a=2 then 1 else 2 end"),
                new String[][] { { "1", "3" }, { "2", "6" } });
    }
}
