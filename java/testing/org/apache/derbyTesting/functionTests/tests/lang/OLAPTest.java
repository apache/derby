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
 * OLAP functionallity test.
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
		throws SQLException 
	{   
		Statement s = createStatement();

		s.executeUpdate("create table t1 (a int, b int)");
		s.executeUpdate("create table t2 (x int)");
		s.executeUpdate("create table t3 (y int)");      

		s.executeUpdate("insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)");
		s.executeUpdate("insert into t2 values (1),(2),(3),(4),(5)");
		s.executeUpdate("insert into t3 values (4),(5),(6),(7),(8)");
		/*
		 * Positive testing of Statements
		 */
		ResultSet rs = s.executeQuery("select row_number(), t1.* from t1");
		String [][] expectedRows = {{"1","1","1"},{"2","2","2"},{"3","3","3"},{"4","4","4"},{"5","5","5"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		rs = s.executeQuery("select row_number(), t1.* from t1 where a > 3");
		expectedRows = new String [][] {{"1","4","4"},{"2","5","5"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		rs = s.executeQuery("select row_number(), a from t1 where b > 3");
		expectedRows = new String [] [] {{"1","4"},{"2","5"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		rs = s.executeQuery("select row_number() as r, a from t1 where b > 3");
		expectedRows = new String [] [] {{"1","4"},{"2","5"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		rs = s.executeQuery("select * from (select row_number() as r, t1.* from t1) as tr where r < 3");
		expectedRows = new String [] [] {{"1","1","1"},{"2","2","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);        

		rs = s.executeQuery("select * from (select row_number() as r, t1.* from t1) as tr where r > 3");
		expectedRows = new String [] [] {{"4","4","4"},{"5","5","5"}};
		JDBC.assertFullResultSet(rs,expectedRows);        

		rs = s.executeQuery("select * from (select row_number() over () as r, t1.* from t1) as tr where r < 3");
		expectedRows = new String [] [] {{"1","1","1"},{"2","2","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);        

		// Pushing predicates (... where r ... ) too far cause this join to fail 
		rs = s.executeQuery("select row_number(),x from t2,t3 where x=y");
		expectedRows = new String [] [] {{"1","4"},{"2","5"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		/*
		 * Negative testing of Statements
		 */

		// Illegal where clause, r not a named column of t1.        
		assertStatementError("42X04",s,"select row_number() as r, a from t1 where r < 3");

		// Illegal use of asterix with another column identifier.        
		assertStatementError("42X01",s,"select row_number() as r, * from t1 where t1.a > 2");

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
