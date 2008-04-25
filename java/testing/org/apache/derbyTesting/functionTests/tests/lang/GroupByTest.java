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

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;

public class GroupByTest extends BaseJDBCTestCase {

	public GroupByTest(String name) {
		super(name);
	}
	
	public static Test suite() {
		return new TestSuite(GroupByTest.class);
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
		// disable auto-commit so that no tear-down code is needed
		getConnection().setAutoCommit(false);

		Statement s = createStatement();
		s.executeUpdate("create table bug280 (a int, b int)");
		s.executeUpdate("insert into bug280 (a, b) " +
						"values (1,1), (1,2), (1,3), (2,1), (2,2)");

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
		rollback();
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

	public void testDerbyOrderByOnAggregate() throws SQLException
	{
		Statement s = createStatement();
		s.executeUpdate("create table yy (a double, b double)");
		s.executeUpdate("insert into yy values (2,4), (2, 4), " +
			"(5,7), (2,3), (2,3), (2,3), (2,3), (9,7)");

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

		s.executeUpdate("drop table yy");
	}


    /**
      * DERBY-3613 check combinations of DISTINCT and GROUP BY
      */
    public void testDistinctGroupBy() throws SQLException
    {
        Statement s = createStatement();
        ResultSet rs;
        s.executeUpdate("create table d3613 (a int, b int, c int, d int)");
        s.executeUpdate("insert into d3613 values (1,2,1,2), (1,2,3,4), " +
                "(1,3,5,6), (2,2,2,2)");

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
        assertStatementError("42Y30", s,
            "select distinct a,b from d3613 group by a");
        assertStatementError("42Y30", s,
            "select distinct a,b from d3613 group by a,c");
        assertStatementError("42Y30", s,
            "select distinct a,b,sum(b) from d3613 group by a");
        
    }

}

