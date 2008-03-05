/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.OLAPTest
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * OLAP functionality test.
 * 
 * Please refer to DERBY-581 for more details.
 */ 
public class OLAPTest extends BaseJDBCTestCase {

	public OLAPTest(String name) {
		super(name);    
	}

	/**
	 * Main test body
	 * 
	 * @throws SQLException
	 */
	public void testBasicOperations()
		throws SQLException {
		Statement s = createStatement();

		s.executeUpdate("create table t1 (a int, b int)");
		s.executeUpdate("create table t2 (x int)");
		s.executeUpdate("create table t3 (y int)");
		s.executeUpdate("create table t4 (a int, b int)");

		s.executeUpdate("insert into t1 values (10,100),(20,200),(30,300),(40,400),(50,500)");
		s.executeUpdate("insert into t2 values (1),(2),(3),(4),(5)");
		s.executeUpdate("insert into t3 values (4),(5),(6),(7),(8)");		
		s.executeUpdate("insert into t4 values (10,100),(20,200)");

		/*
		 * Positive testing of Statements
		 *
		 * Simple queries
		 */		
		ResultSet rs = s.executeQuery("select row_number() over (), t1.* from t1");
		String[][] expectedRows = {{"1", "10", "100"}, {"2", "20", "200"}, {"3", "30", "300"}, {"4", "40", "400"}, {"5", "50", "500"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		rs = s.executeQuery("select row_number() over (), t1.* from t1 where a > 30");
		expectedRows = new String[][]{{"1", "40", "400"}, {"2", "50", "500"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		rs = s.executeQuery("select row_number() over (), a from t1 where b > 300");
		expectedRows = new String[][]{{"1", "40"}, {"2", "50"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		rs = s.executeQuery("select row_number() over () as r, a from t1 where b > 300");
		expectedRows = new String[][]{{"1", "40"}, {"2", "50"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Two instances of row_number columns in the same RCL */
		rs = s.executeQuery("select row_number() over (), row_number() over (), b from t1 where b <= 300");
		expectedRows = new String[][]{{"1", "1", "100"}, {"2", "2", "200"}, {"3", "3", "300"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Two instances of row_number columns in the same RCL, reorder columns */
		rs = s.executeQuery("select row_number() over (), b, row_number() over (), a from t1 where b < 300 ");
		expectedRows = new String[][]{{"1", "100", "1", "10"}, {"2", "200", "2", "20"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Pushing predicates (... where r ... ) too far cause this join to fail */
		rs = s.executeQuery("select row_number() over(),x from t2,t3 where x=y");
		expectedRows = new String[][]{{"1", "4"}, {"2", "5"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Ordering */
		rs = s.executeQuery("select row_number() over () as r, t1.* from t1 order by b desc");
		expectedRows = new String[][]{{"1", "50", "500"}, {"2", "40", "400"}, {"3", "30", "300"}, {"4", "20", "200"}, {"5", "10", "100"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Ordering on a column dropped in projection */
		rs = s.executeQuery("select row_number() over () as r, t1.a from t1 order by b desc");
		expectedRows = new String[][]{{"1", "50"}, {"2", "40"}, {"3", "30"}, {"4", "20"}, {"5", "10"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Only expressions in RCL */
		rs = s.executeQuery("select row_number() over (), row_number() over (), 2*t1.a from t1");
		expectedRows = new String[][]{{"1", "1", "20"}, {"2", "2","40"}, {"3", "3","60"}, {"4", "4", "80"}, {"5", "5", "100"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		/*
		 * Subquerys 
		 */ 
			
		/* This query returned no rows at one time */
		rs = s.executeQuery("select * from (select row_number() over () as r,x from t2,t3 where x=y) s(r,x) where r < 3");
		expectedRows = new String[][]{{"1", "4"}, {"2", "5"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		rs = s.executeQuery("select * from (select row_number() over () as r, t1.* from t1) as tr where r < 3");
		expectedRows = new String[][]{{"1", "10", "100"}, {"2", "20", "200"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		rs = s.executeQuery("select * from (select row_number() over () as r, t1.* from t1) as tr where r > 3");
		expectedRows = new String[][]{{"4", "40", "400"}, {"5", "50", "500"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Two instances of row_number columns */
		rs = s.executeQuery("select row_number() over(), tr.* from (select row_number() over () as r, t1.* from t1) as tr where r > 2 and r < 5");
		expectedRows = new String[][]{{"1", "3", "30", "300"}, {"2", "4", "40", "400"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Two instances of row_number columns, with projection */
		rs = s.executeQuery("select row_number() over(), tr.b from (select row_number() over () as r, t1.* from t1) as tr where r > 2 and r < 5");
		expectedRows = new String[][]{{"1", "300"}, {"2", "400"}};
		JDBC.assertFullResultSet(rs, expectedRows);		

		/* Column ordering */
		rs = s.executeQuery("select * from (select t1.b, row_number() over () as r from t1) as tr where r > 3");
		expectedRows = new String[][]{{"400", "4"}, {"500", "5"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Column ordering with projection*/
		rs = s.executeQuery("select b from (select t1.b, row_number() over () as r from t1) as tr where r > 3");
		expectedRows = new String[][]{{"400"}, {"500"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		/*
		 * Aggregates over window functions once failed
		 */
		rs = s.executeQuery("select count(*) from (select row_number() over() from t1) x");
		expectedRows = new String[][]{{"5"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		rs = s.executeQuery("select count(*) from (select row_number() over () as r from t1) as t(r) where r <=3");
		expectedRows = new String[][]{{"3"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		/*
		 * Some other joins with window functions.
		 * Run off a smaller table t4 to reduce expected row count.
		 */
		rs = s.executeQuery("select row_number() over () from t1 union all select row_number() over () from t1");
		expectedRows = new String[][]{{"1"},{"2"},{"3"},{"4"},{"5"},{"1"},{"2"},{"3"},{"4"},{"5"}};
		JDBC.assertFullResultSet(rs, expectedRows);	
		
		rs = s.executeQuery("select 2 * r from (select row_number() over () from t1) x(r)");
		expectedRows = new String[][]{{"2"},{"4"},{"6"},{"8"},{"10"},};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		rs = s.executeQuery("select c3, c1, c2 from " + 
							"(select a, b, row_number() over() as r from t4) x1 (c1, c2, r1), " +
							"(select row_number() over() as r, b, a from t4) x2 (r2, c3, c4)");
		expectedRows = new String[][]{{"100", "10", "100"},
										{"200", "10", "100"},																				
										{"100", "20", "200"},
										{"200", "20", "200"}};										
		JDBC.assertFullResultSet(rs, expectedRows);
					
		rs = s.executeQuery("select c3, c1, c2 from " + 
							"(select a, b, row_number() over() as r from t4) x1 (c1, c2, r1), " +
							"(select row_number() over() as r, b, a from t4) x2 (r2, c3, c4), " +
							"t4");
		expectedRows = new String[][]{{"100", "10", "100"},
										{"100", "10", "100"},																				
										{"200", "10", "100"},
										{"200", "10", "100"},
										{"100", "20", "200"},
										{"100", "20", "200"},
										{"200", "20", "200"},										
										{"200", "20", "200"}};										
		JDBC.assertFullResultSet(rs, expectedRows);

		rs = s.executeQuery("select c3, c1, c2 from "+
							"(select a, b, row_number() over() as r from t4) x1 (c1, c2, r1), "+
							"(select row_number() over() as r, b, a from t4) x2 (r2, c3, c4), "+
							"t4 "+
							"where x1.r1 = 2 * x2.r2");
		expectedRows = new String[][]{{"100", "20", "200"}, {"100", "20", "200"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		rs = s.executeQuery("select c3, c1, c2 from "+
							"(select a, b, row_number() over() as r from t4) x1 (c1, c2, r1), "+
							"(select row_number() over() as r, b, a from t4) x2 (r2, c3, c4), "+
							"t4 "+
							"where x1.r1 = 2 * x2.r2");
		expectedRows = new String[][]{{"100", "20", "200"}, {"100", "20", "200"}};
		JDBC.assertFullResultSet(rs, expectedRows);
				
		/* Two problematic joins reported during development */
		rs = s.executeQuery("select c3, c1, c2 from "+
							"(select a, b, row_number() over() as r from t4) x1 (c1, c2, r1), "+
							"(select row_number() over() as r, b, a from t4) x2 (r2, c3, c4), "+
							"t4 "+
							"where x2.c4 = t4.a");
		expectedRows = new String[][]{{"100", "10", "100"}, 
										{"100", "20", "200"},
										{"200", "10", "100"},
										{"200", "20", "200"}};			
		JDBC.assertFullResultSet(rs, expectedRows);
		
		rs = s.executeQuery("select c3, c1, c2 from "+
							"(select a, b, row_number() over() as r from t1) x1 (c1, c2, r1), "+
							"(select row_number() over() as r, b, a from t1) x2 (r2, c3, c4), "+
							"t1 "+
							"where x1.r1 = 2 * x2.r2 and x2.c4 = t1.a");
		expectedRows = new String[][]{{"100", "20", "200"}, {"200", "40", "400"}};
		JDBC.assertFullResultSet(rs, expectedRows);

		/* Group by and having */
		rs = s.executeQuery("select r from (select a, row_number() over() as r, b from t1) x group by r");
		expectedRows = new String[][]{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		rs = s.executeQuery("select * from (select a, row_number() over() as r, b from t1) x group by a, b, r");
		expectedRows = new String[][]{{"10", "1", "100"}, 
										{"20", "2", "200"},
										{"30", "3", "300"},
										{"40", "4", "400"},
										{"50", "5", "500"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		rs = s.executeQuery("select * from (select a, row_number() over() as r, b from t1) x group by b, r, a");
		expectedRows = new String[][]{{"10", "1", "100"}, 
										{"20", "2", "200"},
										{"30", "3", "300"},
										{"40", "4", "400"},
										{"50", "5", "500"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		rs = s.executeQuery("select * from "+
							"(select a, row_number() over() as r, b from t1) x "+
							"group by b, r, a "+
							"having r > 2");
		expectedRows = new String[][]{{"30", "3", "300"},
										{"40", "4", "400"}, 
										{"50", "5", "500"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		rs = s.executeQuery("select * from "+
							"(select a, row_number() over() as r, b from t1) x "+
							"group by b, r, a "+
							"having r > 2 and a >=30 "+
							"order by a desc");
		expectedRows = new String[][]{{"50", "5", "500"},
										{"40", "4", "400"}, 
										{"30", "3", "300"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		 
		rs = s.executeQuery("select * from "+
							"(select a, row_number() over() as r, b from t1) x "+
							"group by b, r, a "+
							"having r > 2 and a >=30 "+
							"order by r desc");
		expectedRows = new String[][]{{"50", "5", "500"},
										{"40", "4", "400"}, 
										{"30", "3", "300"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		rs = s.executeQuery("select * from "+
							"(select a, row_number() over() as r, b from t1) x "+
							"group by b, r, a "+
							"having r > 2 and a >=30 "+
							"order by a asc, r desc");
		expectedRows = new String[][]{{"30", "3", "300"},
										{"40", "4", "400"}, 
										{"50", "5", "500"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		/* A couple of distinct queries */
		rs = s.executeQuery("select distinct row_number() over (), 'ABC' from t1");
		expectedRows = new String[][]{{"1", "ABC"},
										{"2", "ABC"},
										{"3", "ABC"},
										{"4", "ABC"},
										{"5", "ABC"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		rs = s.executeQuery("select * from (select distinct row_number() over (), 'ABC' from t1) tmp");
		expectedRows = new String[][]{{"1", "ABC"},
										{"2", "ABC"},
										{"3", "ABC"},
										{"4", "ABC"},
										{"5", "ABC"}};
		JDBC.assertFullResultSet(rs, expectedRows);
		
		/*
		 * Negative testing of Statements
		 */

		// Missing required OVER () 
		assertStatementError("42X01", s, "select row_number() as r, * from t1 where t1.a > 2");

		// Illegal where clause, r not a named column of t1.        
		assertStatementError("42X04", s, "select row_number() over () as r, a from t1 where r < 3");

		// Illegal use of asterix with another column identifier.        
		assertStatementError("42X01", s, "select row_number() over () as r, * from t1 where t1.a > 2");

		/*
		 * Clean up the tables used.
		 */
		s.executeUpdate("drop table t1");
		s.executeUpdate("drop table t2");
		s.executeUpdate("drop table t3");

		s.close();
	}

	public static Test suite() {
		return TestConfiguration.defaultSuite(OLAPTest.class);
	}
}
