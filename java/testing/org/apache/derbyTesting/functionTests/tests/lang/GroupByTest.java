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
import java.sql.PreparedStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

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
            final List rows1 = resultArrayToList(expectedResults[i]);

            for (int j = 0; j < queries.length; j++) {
                final String query2 = queries[j];
                final List rows2 = resultArrayToList(expectedResults[j]);

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
    private static String[][] union(Collection rows1,
                                    Collection rows2,
                                    boolean all) {
        Collection bagOrSet = newBagOrSet(all);
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
    private static String[][] except(Collection rows1,
                                     Collection rows2,
                                     boolean all) {
        Collection bagOrSet = newBagOrSet(all);
        bagOrSet.addAll(rows1);
        // could use removeAll() for sets, but need other behaviour for bags
        for (Iterator it = rows2.iterator(); it.hasNext(); ) {
            bagOrSet.remove(it.next());
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
    private static String[][] intersect(Collection rows1,
                                        Collection rows2,
                                        boolean all) {
        Collection bagOrSet = newBagOrSet(all);
        List copyOfRows2 = new ArrayList(rows2);
        // could use retainAll() for sets, but need other behaviour for bags
        for (Iterator it = rows1.iterator(); it.hasNext(); ) {
            Object x = it.next();
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
    private static Collection newBagOrSet(boolean bag) {
        if (bag) {
            return new ArrayList();
        } else {
            return new HashSet();
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
    private static String[][] toResultArray(Collection rows) {
        String[][] results = new String[rows.size()][];
        Iterator it = rows.iterator();
        for (int i = 0; i < results.length; i++) {
            List row = (List) it.next();
            results[i] = (String[]) row.toArray(new String[row.size()]);
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
    private static List resultArrayToList(String[][] results) {
        ArrayList rows = new ArrayList(results.length);
        for (int i = 0; i < results.length; i++) {
            rows.add(Arrays.asList(results[i]));
        }
        return rows;
    }
}

