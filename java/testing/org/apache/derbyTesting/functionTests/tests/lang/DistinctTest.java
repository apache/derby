/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DistinctTest

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests for DISTINCT. These tests mostly assume: no indexes, no order by, no grouping
 *
 */
public class DistinctTest extends BaseJDBCTestCase {

	public DistinctTest(String name) {
		super(name);
	}
	
	public static Test suite() {
		Test s = new TestSuite(DistinctTest.class);
		Properties p = new Properties();
		p.put("derby.optimizer.noTimeout", "true");
		Test t = new SystemPropertyTestSetup(s, p);
		
		return new CleanDatabaseTestSetup(t) {
			protected void decorateSQL(Statement s) throws SQLException {
				s.execute("create table t (i int, s smallint, r real, f float, d date, t time, ts timestamp, c char(10), v varchar(20))");
 
				// for tests from distinctElimination
				s.execute("create table one(c1 int, c2 int, c3 int, c4 int, c5 int)");
				s.execute("create unique index one_c1 on one(c1)");
				s.execute("create table two(c1 int, c2 int, c3 int, c4 int, c5 int)");
				s.execute("create unique index two_c1c3 on two(c1, c3)");
				s.execute("create table three(c1 int, c2 int, c3 int, c4 int, c5 int)");
				s.execute("create unique index three_c1 on three(c1)");
				s.execute("create table four(c1 int, c2 int, c3 int, c4 int, c5 int)");
				s.execute("create unique index four_c1c3 on four(c1, c3)");
				s.execute("CREATE TABLE \"APP\".\"IDEPT\" (\"DISCRIM_DEPT\" VARCHAR(32), \"NO1\" INTEGER NOT NULL, " +
						"\"NAME\" VARCHAR(50), \"AUDITOR_NO\" INTEGER, \"REPORTTO_NO\" INTEGER, \"HARDWAREASSET\"" +
						" VARCHAR(15), \"SOFTWAREASSET\" VARCHAR(15))");
				s.execute("ALTER TABLE \"APP\".\"IDEPT\" ADD CONSTRAINT \"PK_IDEPT\" PRIMARY KEY (\"NO1\")");

				s.execute("insert into one values (1, 1, 1, 1, 1)");
				s.execute("insert into one values (2, 1, 1, 1, 1)");
				s.execute("insert into one values (3, 1, 1, 1, 1)");
				s.execute("insert into one values (4, 1, 1, 1, 1)");
				s.execute("insert into one values (5, 1, 1, 1, 1)");
				s.execute("insert into one values (6, 1, 1, 1, 1)");
				s.execute("insert into one values (7, 1, 1, 1, 1)");
				s.execute("insert into one values (8, 1, 1, 1, 1)");

				s.execute("insert into two values (1, 1, 1, 1, 1)");
				s.execute("insert into two values (1, 1, 2, 1, 1)");
				s.execute("insert into two values (1, 1, 3, 1, 1)");
				s.execute("insert into two values (2, 1, 1, 1, 1)");
				s.execute("insert into two values (2, 1, 2, 1, 1)");
				s.execute("insert into two values (2, 1, 3, 1, 1)");
				s.execute("insert into two values (3, 1, 1, 1, 1)");
				s.execute("insert into two values (3, 1, 2, 1, 1)");
				s.execute("insert into two values (3, 1, 3, 1, 1)");

				s.execute("insert into three values (1, 1, 1, 1, 1)");
				s.execute("insert into three values (2, 1, 1, 1, 1)");
				s.execute("insert into three values (3, 1, 1, 1, 1)");
				s.execute("insert into three values (4, 1, 1, 1, 1)");
				s.execute("insert into three values (5, 1, 1, 1, 1)");
				s.execute("insert into three values (6, 1, 1, 1, 1)");
				s.execute("insert into three values (7, 1, 1, 1, 1)");
				s.execute("insert into three values (8, 1, 1, 1, 1)");

				s.execute("insert into four values (1, 1, 1, 1, 1)");
				s.execute("insert into four values (1, 1, 2, 1, 1)");
				s.execute("insert into four values (1, 1, 3, 1, 1)");
				s.execute("insert into four values (2, 1, 1, 1, 1)");
				s.execute("insert into four values (2, 1, 2, 1, 1)");
				s.execute("insert into four values (2, 1, 3, 1, 1)");
				s.execute("insert into four values (3, 1, 1, 1, 1)");
				s.execute("insert into four values (3, 1, 2, 1, 1)");
				s.execute("insert into four values (3, 1, 3, 1, 1)");
				
				s.execute("insert into idept values ('Dept', 1, 'Department1', null, null, null, null)");
				s.execute("insert into idept values ('HardwareDept', 2, 'Department2', 25, 1, 'hardwareaset2', null)");
				s.execute("insert into idept values ('HardwareDept', 3, 'Department3', 25, 2, 'hardwareaset3', null)");
				s.execute("insert into idept values ('SoftwareDept', 4, 'Department4', 25, 1, null, 'softwareasset4')");
				s.execute("insert into idept values ('SoftwareDept', 5, 'Department5', 30, 4, null, 'softwareasset5')");
			}
		};
	}
	
	public void testNoData() throws SQLException {
        Statement s = createStatement();
        s.execute("delete from t");
		
		int[] expectedRows = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				               0, 0, 0, 0, 0, 0, 0 };
		
		checkDistinctRows(expectedRows);
	}
	
	public void testOneRow() throws SQLException {
        Statement s = createStatement();
        s.execute("delete from t");
        s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
		
		int[] expectedRows = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0,
				               0, 1, 0, 1, 1, 1, 0, -1, 1, 1, 1, 1, 1, 1,
				               1, 1, 1, 2, 1, 1, 1, 1 };
		
		checkDistinctRows(expectedRows);
	}
	
	public void testIdenticalRows() throws SQLException {
        Statement s = createStatement();
        s.execute("delete from t");
        s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
        s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
		
		int[] expectedRows = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 0,
				               0, 2, 0, 2, 2, 2, 0, -1, 1, 1, 1, 2, 2, 1,
				               2, 2, 1, 2, 1, 1, 1, 1 };
		
		checkDistinctRows(expectedRows);
	}
	
	public void testDistinctIdenticalAndDifferingRows() throws SQLException {
        Statement s = createStatement();
        s.execute("delete from t");
        s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
        s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
        s.execute("insert into t values (2, 1, 4, 3, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
		
		int[] expectedRows = { 2, 2, 2, 2, 1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 0,
				               3, 3, 3, 3, 3, 3, 0, -1, 2, 2, 2, 3, 3, 2,
				               3, 3, 2, 4, 2, 2, 2, 2 };
		
		checkDistinctRows(expectedRows);
	}
	
	public void testDistinctTwoVaryingRows() throws SQLException {
        Statement s = createStatement();
        s.execute("delete from t");
        s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
        s.execute("insert into t values (1, 1, 3, 4, '1992-01-02', '19:01:01', '1992-01-01 19:01:01.000', 'goodbye', 'planet')");
		
		int[] expectedRows = { 1, 2, 1, 1, 2, 1, 1, 2, 1, 2, 2, 2, 2, 2, 0,
				               2, 2, 2, 2, 2, 2, 0, -2, 2, 2, 2, 4, 4, 2,
				               2, 2, 2, 2, 1, 2, 2, 4 };
		
		checkDistinctRows(expectedRows);
	}
	
	public void testDistinctIdenticalNullRows() throws SQLException {
        Statement s = createStatement();
        s.execute("delete from t");
        // defaults are null, get two null rows using defaults
        s.execute("insert into t (i) values (null)");
        s.execute("insert into t (i) values (null)");
		
		int[] expectedRows = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 0,
				               0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 1,
				               0, 0, 0, 2, 1, 1, 1, 0 };
		
		checkDistinctRows(expectedRows);
	}
	
	public void testDistinctSomeNullRows() throws SQLException {
        Statement s = createStatement();
        s.execute("delete from t");
        s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
        s.execute("insert into t values (1, 1, 3, 4, '1992-01-02', '19:01:01', '1992-01-01 19:01:01.000', 'goodbye', 'planet')");
        s.execute("insert into t (i) values (null)");
		
		int[] expectedRows = { 2, 3, 2, 2, 3, 2, 2, 3, 2, 3, 3, 3, 3, 3, 0,
				               2, 0, 2, 2, 2, 0, -2, -2, 3, 3, 3, 4, 4, 3,
				               2, 2, 2, 4, 2, 3, 3, 4 };
		
		checkDistinctRows(expectedRows);
	}
	
	public void testDistinctManyNullRows() throws SQLException {
        Statement s = createStatement();
        s.execute("delete from t");
        s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
        s.execute("insert into t values (1, 1, 3, 4, '1992-01-02', '19:01:01', '1992-01-01 19:01:01.000', 'goodbye', 'planet')");
        s.execute("insert into t (i) values (null)");
        s.execute("insert into t (i) values (null)");
        s.execute("insert into t (i) values (null)");
		
		int[] expectedRows = { 2, 3, 2, 2, 3, 2, 2, 3, 2, 3, 3, 3, 5, 5, 0,
				               2, 0, 2, 2, 2, 0, -2, -2, 3, 3, 3, 4, 4, 3,
				               2, 2, 2, 4, 2, 3, 3, 4 };
		
		checkDistinctRows(expectedRows);
	}
	
	public void testDistinctMixedNullRows() throws SQLException {
        Statement s = createStatement();
        s.execute("delete from t");
        s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
        s.execute("insert into t values (1, 1, 3, 4, '1992-01-02', '19:01:01', '1992-01-01 19:01:01.000', 'goodbye', 'planet')");
        s.execute("insert into t values (null, 1, null, 4, null, '19:01:01', null, 'goodbye', null)");
        s.execute("insert into t values (1, null, 3, null, '1992-01-02', null, '1992-01-01 19:01:01.000', null, 'planet')");
		
		int[] expectedRows = { 2, 3, 2, 2, 3, 2, 2, 3, 2, 4, 4, 4, 4, 4, 0,
				               3, 0, 3, 3, 3, 0, -2, -2, 4, 4, 4, 9, 9, 4,
				               2, 2, 2, 4, 2, 4, 4, 9 };
		
		checkDistinctRows(expectedRows);
	}

	public void testDistinctInValuesClause() throws SQLException {
		Statement s = createStatement();
		
		assertRowCount(3, s.executeQuery("select distinct * from (values (1,2),(1,3),(1,2),(2,3)) as t(a,b)"));
		assertRowCount(2, s.executeQuery("select distinct a from (values (1,2),(1,3),(1,2),(2,3)) as t(a,b)"));
		
		s.close();
	}
	
	public void testDistinctSyntaxErrors() throws SQLException{
		Statement s = createStatement();
		try {
			s.executeQuery("select distinct from t");
		} catch (SQLException e) {
			assertSQLState("42X01", e);
		}
		
		try {
			s.executeQuery("select i as distinct from t");
		} catch (SQLException e) {
			assertSQLState("42X01", e);
		}
		
		try {
			s.executeQuery("select i, v from t distinct");
		} catch (SQLException e) {
			assertSQLState("42X01", e);
		}
		
		s.close();
	}
	
	public void testBasicDistinct() throws SQLException {
		Statement s = createStatement();
		
		s.execute("create table userInt (u integer)");
		s.execute("insert into userInt values (123)");
		s.execute("insert into userInt values (123)");
		s.execute("insert into userInt values (456)");
		s.execute("insert into userInt values (null)");
		s.execute("create table sqlInt (i int not null)");
		s.execute("insert into sqlInt values(123)");
		
		assertRowCount(2, s.executeQuery("select distinct u from userInt where u is not null"));
		assertRowCount(3, s.executeQuery("select u from userInt where u is not null"));
		try {
			s.executeQuery("select distinct i from sqlInt where i = (select distinct u from userInt)");
		} catch (SQLException e) {
			assertSQLState("21000", e);
		}
		
		s.execute("drop table userInt");
		s.execute("drop table sqlInt");
		s.close();
	}
	
	public void testDistinctPaddingInVarcharIgnored() throws SQLException{
		Statement s = createStatement();
		
		s.execute("create table v (v varchar(40))");
		s.execute("insert into v values ('hello')");
		s.execute("insert into v values ('hello   ')");
		s.execute("insert into v values ('hello      ')");
		
		assertRowCount(1, s.executeQuery("select distinct v from v"));
		JDBC.assertSingleValueResultSet(s.executeQuery("select {fn length(c)} from (select distinct v from v) as t(c)"), "5");
		
		s.execute("drop table v");
		s.close();
	}
	
	public void testDistinctWithBigInt() throws SQLException {
		Statement s = createStatement();
		
		s.execute("create table li (l bigint, i int)");
		s.execute("insert into li values(1, 1)");
		s.execute("insert into li values(1, 1)");
		s.execute("insert into li values(9223372036854775807, 2147483647)");

		assertRowCount(2, s.executeQuery("select distinct l from li"));
		assertRowCount(4, s.executeQuery("(select distinct l from li) union all (select distinct i from li) order by 1"));
		assertRowCount(3, s.executeQuery("select distinct l from li union select distinct i from li"));
		assertRowCount(3, s.executeQuery("select distinct l from (select l from li union all select i from li) a(l)"));
		
		s.execute("drop table li");
		s.close();

	}
	
	public void testDistinctWithUpdatedRows() throws SQLException {
		
		Connection c = getConnection();
		
		c.setAutoCommit(false);
		Statement s = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		
		s.execute("create table u (d date)");
		s.execute("insert into u values ('1997-09-09'),('1997-09-09')");
		s.execute("insert into u values (null)");
		
		ResultSet rs = s.executeQuery("select distinct d from u");
		assertTrue(rs.next());
		assertTrue(rs.next());
		assertFalse(rs.next());
		rs.close();
		
		try {
			rs = s.executeQuery("select distinct d from u for update");
			fail("Distinct: for update test should have thrown exception");
		} catch (SQLException e) {
			assertSQLState("42Y90", e);
		}
		
		try {
			rs = s.executeQuery("select distinct d from u for update of d");
			fail("Distinct: for update test should have thrown exception");
		} catch (SQLException e) {
			assertSQLState("42Y90", e);
		}
		
		s.setCursorName("C1");
		rs = s.executeQuery("select distinct d from u");
		assertTrue(rs.next());
        Statement s2 = createStatement();
		try {
			s2.executeUpdate("update u set d='1992-01-01' where current of C1");
			fail("Distinct: update test should have thrown exception");
		} catch (SQLException e) {
			assertSQLState("42X23", e);
		}
		try {
			s2.executeUpdate("delete from u where current of C1");
			fail("Distinct: update test should have thrown exception");
		} catch (SQLException e) {
			assertSQLState("42X23", e);
		}
		//should be able to keep going.
		assertTrue(rs.next());
		assertFalse(rs.next());
		rs.close();
		
		try {
			s2.executeUpdate("update u set d='1992-01-01' where current of C1");
			fail("Distinct: update test should have thrown exception");
		} catch (SQLException e) {
			assertSQLState("42X30", e);
		}
		try {
			s2.executeUpdate("delete from u where current of c1");
			fail("Distinct: update test should have thrown exception");
		} catch (SQLException e) {
			assertSQLState("42X30", e);
		}

		s2.close();
		s.close();
		c.rollback();
		c.setAutoCommit(true);
	}
	
	public void testDistinctInInsert() throws SQLException {

		Statement s = createStatement();
		//create a table similar to t
		s.execute("delete from t");
		s.execute("create table insert_test (i int, s smallint, r real, f float, d date, t time, ts timestamp, c char(10), v varchar(20))");
		s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
		s.execute("insert into t values (1, 2, 3, 4, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
		s.execute("insert into t values (2, 1, 4, 3, '1992-01-01', '19:01:01', '1992-01-01 19:01:01.000', 'hello', 'planet')");
		
		s.execute("insert into insert_test select distinct * from t");
		assertRowCount(2, s.executeQuery("select * from insert_test"));
		s.execute("delete from insert_test");

		s.execute("insert into insert_test select distinct * from t union select * from t");
		assertRowCount(2, s.executeQuery("select * from insert_test"));
		s.execute("delete from insert_test");
		s.execute("drop table insert_test");
		
		s.execute("delete from t");
		s.close();
		
	}
	
	/**
	 * This test demonstrates that using distinct in a query for insert
	 * generates gaps in numbering in autoincremented columns.
	 * 
	 * See DERBY-3. If that bug is fixed, the first query after the comment
	 * below will fail.
	 * 
	 * @throws SQLException
	 */
	public void testDistinctInsertWithGeneratedColumn() throws SQLException {

		Statement s = createStatement();
		
		s.execute("create table destWithAI(c11 int generated always as identity, c12 int)");
		s.execute("alter table destWithAI alter c11 set increment by 1");
		s.execute("create table destWithNoAI(c21 int, c22 int)");
		s.execute("create table source(c31 int, c32 int, c33 int)");
		
		s.execute("insert into source values(1,1,1)");
		s.execute("insert into source values(1,2,1)");
		s.execute("insert into source values(2,1,1)");
		s.execute("insert into source values(2,2,1)");
		
		assertRowCount(2, s.executeQuery("select distinct(c31) from source"));
		assertEquals(2, s.executeUpdate("insert into destWithAI(c12) select distinct(c31) from source"));
		
		//we will see gaps in the autoincrement column for all the duplicate rows from source
		String [][] expected = { {"1", "1"}, 
				                 {"3", "2"} };
		JDBC.assertFullResultSet(s.executeQuery("select * from destWithAI"), expected);
		
		assertEquals(2, s.executeUpdate("insert into destWithNoAI(c22) select distinct(c31) from source"));
		expected = new String [][] { {null, "1"}, 
				                     {null, "2"} };
		JDBC.assertFullResultSet(s.executeQuery("select * from destWithNoAI"), expected);
		
		s.execute("drop table source");
		s.execute("drop table destWithNoAI");
		s.execute("drop table destWithAI");
		s.close();
	}
	
	/*
	 * Following test case fails in the prepareStatement call,
	 * with an ASSERT related to the DERBY-47 in list changes.
	 *
	public void testResultSetInOrderWhenUsingIndex() throws SQLException{
		Statement s = createStatement();
		
		s.execute("CREATE TABLE netbutton1 (lname varchar(128) not null, name varchar(128), summary varchar(256)," +
				 " lsummary varchar(256), description varchar(2000), ldescription varchar(2000), publisher_username" +
				 " varchar(256), publisher_lusername varchar(256), version varchar(16), source long varchar for bit data," +
				 " updated timestamp, created timestamp DEFAULT current_timestamp, primary key (lname))");	
		s.execute("insert into netbutton1 values('lname1','name1','sum2','lsum1', 'des1','ldes1','pubu1', 'publu1', 'ver1', null, current_timestamp, default)");
		s.execute("insert into netbutton1 values('lname2','name2','sum2','lsum2', 'des2','ldes2','pubu2', 'publu2', 'ver2', null, current_timestamp, default)");
		s.execute("CREATE TABLE library_netbutton (netbuttonlibrary_id int not null, lname varchar(128) not null, primary key (netbuttonlibrary_id, lname))");
		s.execute("insert into library_netbutton values(1, 'lname1')");
		s.execute("insert into library_netbutton values(2, 'lname2')");
		// this is the index that causes the bug to be exposed.
		s.execute("create unique index ln_library_id on library_netbutton(netbuttonlibrary_id)");
		s.execute("ALTER TABLE library_netbutton ADD CONSTRAINT ln_lname_fk FOREIGN KEY (lname) REFERENCES netbutton1(lname)");
		s.execute("CREATE TABLE netbuttonlibraryrole1 (lusername varchar(512) not null, netbuttonlibrary_id int not null," +
				  " username varchar(512), role varchar(24), created timestamp DEFAULT current_timestamp, primary key (lusername, netbuttonlibrary_id))");
		s.execute("insert into netbuttonlibraryrole1 values('lusername1', 1,'user1', 'role1', default)");
		s.execute("insert into netbuttonlibraryrole1 values('lusername2', 2,'user2', 'role2', default)");
		
		PreparedStatement p = prepareStatement("SELECT DISTINCT nb.name AS name, nb.summary AS summary FROM netbutton1 nb, netbuttonlibraryrole1 nlr, library_netbutton ln" +
		" WHERE nb.lname = ln.lname AND (nlr.lusername = ? OR nlr.lusername =?)");

		p = prepareStatement("SELECT DISTINCT nb.name AS name, nb.summary AS summary FROM netbutton1 nb, netbuttonlibraryrole1 nlr, library_netbutton ln" +
				" WHERE nlr.netbuttonlibrary_id = ln.netbuttonlibrary_id AND nb.lname = ln.lname AND (nlr.lusername = ? OR nlr.lusername = ?) AND nb.lname = ? ORDER BY summary");
		
		p.setString(1, "lusername1");
		p.setString(2, "lusername2");
		//p.setString(3, "lname1");
		assertTrue(p.execute());

	
		String [][] expected = { {"name1", "sum2" } };
    	ResultSet rs = p.getResultSet();
		JDBC.assertFullResultSet(rs, expected);
		rs.close();
		p.close();
		
		s.execute("drop table library_netbutton");
		s.execute("drop table netbutton1");
		s.close();
	}
	*/
	
	public void testDistinctStoreSort() throws SQLException {
		Statement s = createStatement();
		
		s.execute("create table td (x int)");
		s.execute("insert into td values (1)");
		s.execute("insert into td values (1)");
		s.execute("insert into td values (2)");
		
		// distinct in subquery where the store does not perform the sort
        String [][] expected = { {"1", "1"}, 
        		                 {"1", "1"}, 
        		                 {"2", "1"} };
		JDBC.assertFullResultSet(s.executeQuery("select * from td, (select distinct 1 from td) as sub(x)"), expected);
		
		// get the storage system to do the sort
        expected = new String [][] { {"1", "2"},
                                     {"1", "1"}, 
                                     {"1", "2"},
                                     {"1", "1"},
        		                     {"2", "2"}, 
        		                     {"2", "1"} };
		JDBC.assertUnorderedResultSet(s.executeQuery(
			"select * from td, (select distinct x from td) as sub(x)"),
			expected);
		
		s.execute("drop table td");
		s.close();
	}
	
	/**
	 * Tests for DERBY-504 (select distinct from a subquery)
	 * 
	 * @throws SQLException
	 */
	public void testDistinctScanForSubquery() throws SQLException {
		
		Statement s = createStatement();
		
		s.execute("create table names (id int, name varchar(10), age int)");
		s.execute("insert into names (id, name, age) values" +
				" (1, 'Anna', 23), (2, 'Ben', 24), (3, 'Carl', 25)," +
				" (4, 'Anna', 23), (5, 'Ben', 24), (6, 'Carl', 25)");
		s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
		
		// distinct names should be returned
		// runtime statistics should not have Distinct Scan in it
		assertRowCount(3, s.executeQuery("select distinct name from (select name, id from names) as n"));
		RuntimeStatisticsParser rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedTableScan());
		assertFalse(rtsp.usedDistinctScan());
		
		// distinct names should be returned
		// runtime statistics should have Distinct Scan in it
		assertRowCount(3, s.executeQuery("select distinct name from (select name from names) as n"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedTableScan());
		assertTrue(rtsp.usedDistinctScan());
		
		// runtime statistics should have Distinct Scan in it
		assertRowCount(6, s.executeQuery("select distinct a, b, b, a from (select y as a, x as b from (select id as x, name as y from names) as n) as m"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedTableScan());
		assertTrue(rtsp.usedDistinctScan());
		
		// runtime statistics should not have Distinct Scan in it
		assertRowCount(3, s.executeQuery("select distinct a, a from (select y as a from (select id as x, name as y from names) as n) as m"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedTableScan());
		assertFalse(rtsp.usedDistinctScan());
		
		s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
		s.execute("drop table names");
		s.close();
	}
	
	
	/**
	 * Tests queries where distinct scan is eliminated. 
	 */
	public void testDistinctElimination() throws SQLException {
		Statement s = createStatement();
		s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
		
		assertRowCount(1, s.executeQuery("select distinct c2 from one"));
		RuntimeStatisticsParser rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedDistinctScan());
		
		// Derby251 Distinct should not get eliminated for following query
		// because there is no equality condition on unique column of table
		// in the outside query
		assertRowCount(2, s.executeQuery("select distinct q1.\"NO1\",  q1.\"NAME\",  q1.\"AUDITOR_NO\",  q1.\"REPORTTO_NO\",  q1.\"DISCRIM_DEPT\",  q1.\"SOFTWAREASSET\" from IDEPT q1, IDEPT q2" +
				" where ( q2.\"DISCRIM_DEPT\" = 'HardwareDept') and ( q1.\"DISCRIM_DEPT\" = 'SoftwareDept') and ( q1.\"NO1\" <> ALL ( " +
				"select q3.\"NO1\" from IDEPT q3 where ( ( q3.\"DISCRIM_DEPT\" = 'Dept') or ( q3.\"DISCRIM_DEPT\" = 'HardwareDept')  or  " +
				"( q3.\"DISCRIM_DEPT\" = 'SoftwareDept') ) and ( q3.\"REPORTTO_NO\" =  q2.\"NO1\") ) ) "));
		
		// Another test case of Derby251 where the exists table column is embedded in an expression.
		assertRowCount(2, s.executeQuery("select  distinct  q1.\"NO1\" from IDEPT q1, IDEPT q2 where ( q2.\"DISCRIM_DEPT\" = 'HardwareDept')	and " +
				"( q1.\"DISCRIM_DEPT\" = 'SoftwareDept') and ( q1.\"NO1\" <> ALL (select  q3.\"NO1\" from IDEPT q3 where  ( ABS(q3.\"REPORTTO_NO\") =  q2.\"NO1\")))"));

		//result ordering is not guaranteed, but order by clause will change how
		// distinct is executed.  So test by retrieving data into a temp table and
		// return results ordered after making sure the query was executed as expected
		s.execute("create table temp_result (c2 int, c3 int)");
		s.execute("insert into temp_result select distinct c2, c3 from two");
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedDistinctScan());
		
		// Try same query, but with an order by at the end.  This will use the sort for
		// the "order by" to do the distinct and not do a "DISTINCT SCAN".
		assertRowCount(3, s.executeQuery("select distinct c2, c3 from two order by c2, c3"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		
		// more than one table in the select list
		// Following runtime statistics output should have Eliminate duplicates = true
		assertRowCount(3, s.executeQuery("select distinct a.c1, b.c1 from one a, two b where a.c1 = b.c1 and b.c2 =1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.eliminatedDuplicates());	
		
		// cross product join
		assertRowCount(8, s.executeQuery("select distinct a.c1 from one a, two b"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.eliminatedDuplicates());	
		
		// no single table will yield at most 1 row
		assertRowCount(9, s.executeQuery("select distinct a.c1, a.c3, a.c2 from two a, two b where a.c1 = b.c1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.eliminatedDuplicates());
		assertRowCount(9, s.executeQuery("select distinct a.c1, a.c3, a.c2 from two a, two b where a.c1 = b.c1 and a.c2 = 1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.eliminatedDuplicates());
		
		// both keys from unique index in where clause but joined to different tables
		assertRowCount(1, s.executeQuery("select distinct a.c1 from one a, two b, three c where a.c1 = b.c1 and c.c1 = b.c3 and a.c1 = 1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.eliminatedDuplicates());
		
		// join between two tables using one columns of unique key
		assertRowCount(3, s.executeQuery("select distinct a.c1 from two a, four b where a.c1 = b.c1 and b.c3 = 1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.eliminatedDuplicates());
		
		// join between two tables with no join predicate
		assertRowCount(9, s.executeQuery("select distinct a.c1, a.c3 from two a, one b"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.eliminatedDuplicates());

		// join between three tables with two tables joined uniquely
		assertRowCount(1, s.executeQuery("select distinct a.c1 from one a, two b, three c where a.c1 = c.c1 and a.c1 = 1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.eliminatedDuplicates());
		
		// queries that should eliminate the distinct
		// Following runtime statistics output should NOT have Eliminate duplicates = true
		// single table queries
		// unique columns in select list
		assertRowCount(8, s.executeQuery("select distinct c1 from one"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());
		
		assertRowCount(8, s.executeQuery("select distinct c1, c2 + c3 from one"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());

		assertRowCount(9, s.executeQuery("select distinct c3, c1 from two"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());

		// query returns single row
		assertRowCount(1, s.executeQuery("select distinct c2 from one where c1 = 3"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());
		
		assertRowCount(1, s.executeQuery("select distinct c3 from one where c1 = 3"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());

		// super-set in select list
		assertRowCount(8, s.executeQuery("select distinct c2, c5, c1 from one"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());

		assertRowCount(9, s.executeQuery("select distinct c2, c3, c1 from two"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());
		
		// multi-table queries

		// 1 to 1 join, select list is superset
		assertRowCount(8, s.executeQuery("select distinct a.c1 from one a, one b where a.c1 = b.c1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());

		assertRowCount(8, s.executeQuery("select distinct a.c1, 3 from one a, one b where a.c1 = b.c1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());

		assertRowCount(9, s.executeQuery("select distinct a.c1, a.c3, a.c2 from two a, one b where a.c1 = b.c1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());

		assertRowCount(9, s.executeQuery("select distinct a.c1, a.c3, a.c2 from two a, two b where a.c1 = b.c1 and b.c3 = 1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());

		//join between two tables using both columns of unique key
		assertRowCount(3, s.executeQuery("select distinct a.c1 from two a, four b where a.c1 = b.c1 and a.c3 = b.c3 and b.c3 = 1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());
	
		s.execute("drop table temp_result");
		s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
		s.close();
	}
	
	public void testDistinctFiltering() throws SQLException {
		Statement s = createStatement();
		// t1 gets non-unique indexes, t2 gets unique indexes
		s.execute("create table t1(c1 int, c2 char(50), c3 char(50))");
		s.execute("create table t2(c1 int, c2 char(50), c3 char(50))");
		s.execute("create index t11 on t1(c1)");
		s.execute("create index t12 on t1(c1, c2)");
		s.execute("create index t13 on t1(c1, c3, c2)");
		s.execute("create unique index t21 on t2(c1, c2)");
		s.execute("create unique index t22 on t2(c1, c3)");
		s.execute("insert into t1 values (1, '1', '1'), (1, '1', '1'), (1, '11', '11'), (1, '11', '11'), (2, '2', '2'), (2, '2', '3'), (2, '3', '2'), (3, '3', '3'), (null, null, null)");
	    s.execute("insert into t2 values (1, '1', '1'), (1, '2', '2'), (2, '1', '1'), (2, '2', '2'), (null, 'null', 'null')");
		s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
	    
		// first column of an index
		assertRowCount(4, s.executeQuery("select distinct c1 from t1 where 1=1"));
		RuntimeStatisticsParser rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertTrue(rtsp.eliminatedDuplicates());

		// equality predicate on preceding key columns
		assertRowCount(1, s.executeQuery("select distinct c2 from t1 where c1 = 1 and c3 = '1'"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertTrue(rtsp.eliminatedDuplicates());
		
		// equality predicate on all key columns, non unique
		assertRowCount(1, s.executeQuery("select distinct c3 from t1 where c1 = 1 and c2 = '1'"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertTrue(rtsp.eliminatedDuplicates());
		
		// equality predicate on all key columns, non unique
		assertRowCount(1, s.executeQuery("select distinct c3 from t2 where c1 = 1 and c2 = '1'"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertFalse(rtsp.eliminatedDuplicates());
		
		// different orderings
		assertRowCount(6, s.executeQuery("select distinct c2, c1 from t1 where 1=1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertTrue(rtsp.eliminatedDuplicates());

		assertRowCount(2, s.executeQuery("select distinct c2 from t1 where c1 = 1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertTrue(rtsp.eliminatedDuplicates());

		assertRowCount(1, s.executeQuery("select distinct c2, c1 from t1 where c3 = '1'"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertTrue(rtsp.eliminatedDuplicates());

		assertRowCount(1, s.executeQuery("select distinct c2 from t1 where c3 = '1' and c1 = 1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertTrue(rtsp.eliminatedDuplicates());

		// ordered, but no where clause - uses distinct scan
		// the following approach is used because the ordering of the results from
		// the distinct is not guaranteed (it varies depending on the JVM hash 
		// implementation), but adding an order by to the query may
		// change how we execute the distinct and we want to test the code path without
		// the order by.  By adding the temp table, we can maintain a single master
		// file for all JVM's.
		
		s.execute("create table temp_result (result_column int)");
		s.execute("insert into temp_result (select distinct c1 from t1)");
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedDistinctScan());
		assertRowCount(4, s.executeQuery("select * from temp_result order by result_column"));
		
		// test distinct with an order by
		assertRowCount(4, s.executeQuery("select distinct c1 from t1 order by c1"));
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertFalse(rtsp.usedDistinctScan());
		assertTrue(rtsp.eliminatedDuplicates());		
		
	    s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
	    s.execute("drop table temp_result");
		s.execute("drop table t1");
		s.execute("drop table t2");
		s.close();
	}
		
	/**
	 * Runs a set of 37 SQL queries on the current data in table t, testing
	 * a number of different column combinations, predicates, and subqueries.
	 * 
	 * @param rowcounts an array of integers with the expected row count for
	 *                  each query.
	 * @throws SQLException
	 */
	private void checkDistinctRows(int[] rowcounts) throws SQLException {
		
		assertEquals("DistinctTest: rowcounts array is not the right length", 37, rowcounts.length);
		
		Statement s = createStatement();
		assertRowCount(rowcounts[0], s.executeQuery("select distinct i from t"));
		assertRowCount(rowcounts[1], s.executeQuery("select distinct s from t"));
		assertRowCount(rowcounts[2], s.executeQuery("select distinct r from t"));
		assertRowCount(rowcounts[3], s.executeQuery("select distinct f from t"));
		assertRowCount(rowcounts[4], s.executeQuery("select distinct d from t"));
		assertRowCount(rowcounts[5], s.executeQuery("select distinct t from t"));
		assertRowCount(rowcounts[6], s.executeQuery("select distinct ts from t"));
		assertRowCount(rowcounts[7], s.executeQuery("select distinct c from t"));
		assertRowCount(rowcounts[8], s.executeQuery("select distinct v from t"));

		// select distinct multiple columns, each data type
		// select distinct all or just some columns of the table
		assertRowCount(rowcounts[9], s.executeQuery("select distinct t,i,s,f,d from t"));
		assertRowCount(rowcounts[10], s.executeQuery("select distinct * from t"));
		assertRowCount(rowcounts[11], s.executeQuery("select distinct t.*,ts from t"));
        // select distinct in an exists subquery
		assertRowCount(rowcounts[12], s.executeQuery("select * from t where exists (select distinct i from t)"));
		assertRowCount(rowcounts[13], s.executeQuery("select * from t where exists (select distinct * from t)"));
		assertRowCount(rowcounts[14], s.executeQuery("select * from t where not exists (select distinct t from t)"));
		// select distinct in an in subquery
		assertRowCount(rowcounts[15], s.executeQuery("select * from t where i in (select distinct s from t)"));
		assertRowCount(rowcounts[16], s.executeQuery("select * from t where s not in (select distinct r from t)"));

		// select distinct in a quantified subquery
		// same result as i in distinct s above
		assertRowCount(rowcounts[17], s.executeQuery("select * from t where i =any (select distinct s from t)"));
		// same result as s not in distinct r above
		assertRowCount(rowcounts[18], s.executeQuery("select * from t where s <>any (select distinct r from t)"));
		assertRowCount(rowcounts[19], s.executeQuery("select * from t where d >=any (select distinct d from t)"));
		assertRowCount(rowcounts[20], s.executeQuery("select * from t where t <=all (select distinct t from t)"));

		// select distinct in a scalar subquery
		// in some cases, the value that is returned is not valid for the where
		try {
			assertRowCount(rowcounts[21], s.executeQuery("select * from t where c = (select distinct v from t)"));	
		} catch (SQLException se1) {
			if (rowcounts[21] == -2) {
				//Scalar subquery is only allowed to return a single row.
				assertSQLState("21000", se1);
			} else {
				fail("Distinct: expected SQLException was not thrown.");
			}
		}
		
		try {
			assertRowCount(rowcounts[22], s.executeQuery("select * from t where v < (select distinct d from t)"));
		} catch (SQLException se2) {
			if (rowcounts[22] == -1) {
				//The syntax of the string representation of a datetime value is incorrect.
				assertSQLState("22007", se2);
			} else if (rowcounts[22] == -2) {
				//Scalar subquery is only allowed to return a single row.
				assertSQLState("21000", se2);
			} else {
				fail("Distinct: expected SQLException was not thrown.");
			}
		}

		// select distinct in a from subquery
		assertRowCount(rowcounts[23], s.executeQuery("select * from (select distinct t,i,s,f,d from t) as s(a,b,c,d,e)"));
		assertRowCount(rowcounts[24], s.executeQuery("select * from (select distinct * from t) as s"));
		assertRowCount(rowcounts[25], s.executeQuery("select * from (select distinct t.*,ts as tts from t) as s"));

		// select distinct in a from subquery joining with another table
		assertRowCount(rowcounts[26], s.executeQuery("select * from t, (select distinct t.*,ts as tts from t) as s where t.i=s.i"));
		assertRowCount(rowcounts[27], s.executeQuery("select * from (select distinct t.*,ts as tts from t) as s, t where t.i=s.i"));

		// multiple select distincts -- outer & sqs, just sqs, outer & from(s)
		assertRowCount(rowcounts[28], s.executeQuery("select distinct * from (select distinct t,i,s,f,d from t) as s(a,b,c,d,e)"));
		assertRowCount(rowcounts[29], s.executeQuery("select i, s from t as touter where touter.i in (select distinct i from t)	and exists (select distinct s from t as ti where touter.s=ti.s)"));

        // same result as exists above
		assertRowCount(rowcounts[30], s.executeQuery("select i, s from t as touter where touter.i in (select distinct i from t)	and touter.s =any (select distinct s from t)"));
		assertRowCount(rowcounts[31], s.executeQuery("select distinct i, s from t where t.i in (select distinct i from t) and t.s in (select distinct s from t)"));

		// select distinct under a union all/ over a union all
		// expect 2 rows of any value
		assertRowCount(rowcounts[32], s.executeQuery("select distinct i from t union all select distinct i from t"));

		// at most 1 row of any value
		assertRowCount(rowcounts[33], s.executeQuery("select distinct * from (select i from t union all select i from t) as s"));

		// select distinct over a from subquery (itself distinct/not)
		assertRowCount(rowcounts[34], s.executeQuery("select distinct * from (select t,i,s,f,d from t) as s(a,b,c,d,e)"));
		assertRowCount(rowcounts[35], s.executeQuery("select distinct * from (select distinct t,i,s,f,d from t) as s(a,b,c,d,e)"));

		// select distinct over a join
		assertRowCount(rowcounts[36], s.executeQuery("select distinct * from t t1, t t2 where t1.i = t2.i"));
		
		s.close();
	}
	
	/**
	 * Assert that the number of rows in the result set matches what we are expecting.
	 * We close the result set here, because we are only interested in the row count here
	 * and we assume that the caller is not checking any other aspect of the ResultSet.
	 * 
	 * @param count the number of rows we expect to find
	 * @param rs the result set to check 
	 * 
	 * @throws SQLException
	 */
	public void assertRowCount(int count, ResultSet rs) throws SQLException {
		int rowcount = 0;
		while (rs.next()) {
			rowcount++;
		}
		rs.close();
		assertEquals(count, rowcount);
	}
} 
