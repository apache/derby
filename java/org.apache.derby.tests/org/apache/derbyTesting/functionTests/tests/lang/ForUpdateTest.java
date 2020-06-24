/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ForUpdateTest

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
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for forupdate. 
 *
 */
public class ForUpdateTest extends BaseJDBCTestCase {

	/* Public constructor required for running test as standalone JUnit. */    
	public ForUpdateTest(String name) {
		super(name);
	}
    
    /**
     * Sets the auto commit to false.
     */
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

	/**
         * Create a suite of tests.
         **/
        public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            BaseTestSuite suite = new BaseTestSuite("ForUpdateTest");
        	suite.addTest(baseSuite("ForUpdateTest:embedded"));
        	suite.addTest(TestConfiguration.clientServerDecorator(baseSuite("ForUpdateTest:client")));
        	return suite;
    	}

	protected static Test baseSuite(String name) {
            BaseTestSuite suite = new BaseTestSuite(name);
        	suite.addTestSuite(ForUpdateTest.class);	
		return new CleanDatabaseTestSetup(suite) 
        	{
            		protected void decorateSQL(Statement s) throws SQLException
            		{
                		s.execute("create table t1 ( i int, v varchar(10), d double precision, t time )");
				s.execute("create table t2 ( s smallint, c char(10), r real, ts timestamp )");
                                s.execute("create table t3 (i int not null constraint t3pk primary key, b char(10))");
				s.execute("create table t4 (c1 int)");	
//IC see: https://issues.apache.org/jira/browse/DERBY-3224
//IC see: https://issues.apache.org/jira/browse/DERBY-3176
                                s.execute("create index t3bi on t3(b)");
                                s.execute("insert into t3 values (1, 'hhhh'), (2, 'uuuu'), (3, 'yyyy'), (4, 'aaaa'), (5, 'jjjj'), (6, 'rrrr')");
                                s.execute("insert into t3 values (7, 'iiii'), (8, 'wwww'), (9, 'rrrr'), (10, 'cccc'), (11, 'hhhh'), (12, 'rrrr')");
                                s.execute("insert into t4 (c1) values (1),(2),(3)");
            		}
        	};
    	} 

        public void testNegative() throws SQLException {    
		assertCompileError("42X01", "select i, v from t1 for");
        assertCompileError("42X01", "select i, v from t1 for read");
        assertCompileError("42X01", "select i, v from t1 for only");
        assertCompileError("42X01", "select i, v from t1 for update of");
        assertCompileError("42X01", "select i, v from t1 update");
        assertCompileError("42X01", "select i, v from t1 only");
        assertCompileError("42X01", "select i, v from t1 read");
        
        Statement stmt = createStatement();
		JDBC.assertEmpty(stmt.executeQuery("select i, v from t1 for update"));
        stmt.close();
        }


	public void testCursor() throws SQLException {	
        Statement stmt = createStatement();
 		stmt.setCursorName("C");
		stmt.executeQuery("select i, v from t1, t2");
                Statement stmt2 = createStatement();
		try {
			stmt2.executeUpdate("delete from t1 where current of C");
			fail("ForUpdateTest: should have thrown exception");
		} catch (SQLException e) {
			if (usingEmbedded())
				assertSQLState("42X23", e);
                        else
				assertSQLState("42X30", e);

		}
                
                try {
                     stmt2.executeQuery("select i, v from t1, t2");
                } catch (SQLException e ) {
                     assertSQLState("X0X60", e);
                }
                stmt2.close();
                stmt.close();
        }

         
        public void testCursor1() throws SQLException {	
            Statement stmt = createStatement();
		stmt.setCursorName("C1");
		ResultSet rs = stmt.executeQuery("select i, v from t1 where i is not null");
		Statement stmt2 = createStatement();
		try {
			stmt2.executeUpdate("delete from t1 where current of C1");
			fail("ForUpdateTest: should have thrown exception");
		} catch (SQLException e) {
			if (usingEmbedded())
			    assertSQLState("42X23", e);
			else
			    assertSQLState("42X30", e);
		}
		stmt2.close();
                rs.close();
        }


        public void testCursor2() throws SQLException {	
            Statement stmt = createStatement();
		stmt.setCursorName("C2");
		ResultSet rs = stmt.executeQuery("select i, v from t1, t2 for read only");
		Statement stmt2 = createStatement();
		try {
			stmt2.executeUpdate("delete from t1 where current of C2");
			fail("ForUpdateTest: should have thrown exception");
		} catch (SQLException e) {
			if (usingEmbedded())
			    assertSQLState("42X23", e);
			else
			    assertSQLState("42X30", e);
		}
		stmt2.close();
                rs.close();
                stmt.close();
        }



	public void testCursor3() throws SQLException {	
        Statement stmt = createStatement();
		stmt.setCursorName("C3");
		ResultSet rs = stmt.executeQuery("select i, v from t1 where i is not null for read only");
		Statement stmt2 = createStatement();
		try {
			stmt2.executeUpdate("delete from t1 where current of C3");
			fail("ForUpdateTest: should have thrown exception");
		} catch (SQLException e) {
			if (usingEmbedded())
			    assertSQLState("42X23", e);
			else
			    assertSQLState("42X30", e);
		}
		stmt2.close();
                rs.close();
                stmt.close();
        }

 
        
	public void testUpdates() throws SQLException {	
        Statement stmt = createStatement();
		JDBC.assertEmpty(stmt.executeQuery("select i, v from t1 for update of t"));
		JDBC.assertEmpty(stmt.executeQuery("select i, v from t1 for update of i"));

		assertStatementError("42X04", stmt, "select i, v from t1 for update of g");
		assertStatementError("42X04", stmt, "select i+10 as iPlus10, v from t1 for update of iPlus10");
		assertStatementError("42Y90", stmt, "select i from t1, t2 for update");

		assertStatementError("42Y90", stmt, "select i from t1 where i=(select i from t1) for update");
		assertStatementError("42Y90", stmt, "select i from t1 where i in (select i from t1) for update");
		assertStatementError("42Y90", stmt, "select i from t1 where exists (select i from t1) for update");
		assertStatementError("42Y90", stmt, "select i from t1 where exists (select s from t2) for update");
		assertStatementError("42Y90", stmt, "select i from t1 where exists (select s from t2 where i=s) for update");
		assertStatementError("42Y90", stmt, "select (select s from t2) from t1 where exists (select i from t1) for update");
		assertStatementError("42Y90", stmt, "select (select s from t2 where i=s) from t1 where exists (select i from t1) for update");
		assertStatementError("42Y90", stmt, "select * from (select i, d from t1) a for update");
		assertStatementError("42Y90", stmt, "select * from (select i+10, d from t1) a for update");
		assertStatementError("42Y90", stmt, "select * from (values (1, 2, 3)) a for update");
		assertStatementError("42Y90", stmt, "values (1, 2, 3) for update");
		assertStatementError("42Y90", stmt, "select * from t1 union all select * from t1 for update");
        stmt.close();
        }

 
        public void testUpdates2() throws SQLException {	
            Statement stmt = createStatement();
		stmt.executeUpdate("insert into t1 (i) values (1)");
		stmt.setCursorName("C4");
		ResultSet rs = stmt.executeQuery("select i from t1 s1 for update");
		rs.next();
                assertEquals(rs.getString("I"), "1");

		Statement stmt2 = createStatement();
		try {
			stmt2.executeUpdate("delete from s1 where current of C4");
			fail("ForUpdateTest: should have thrown exception");
		} catch (SQLException e) {
			assertSQLState("42X28", e);
		}

		Statement stmt3 = createStatement();
		stmt3.executeUpdate("delete from t1 where current of C4");
		rs.close();
		
		JDBC.assertEmpty(stmt.executeQuery("select i from t1 for update of i, v, d, t"));
		JDBC.assertEmpty(stmt.executeQuery("select i from t1 for update of v, i, t, d"));
		JDBC.assertEmpty(stmt.executeQuery("select i from t1 for update of i, d"));
		JDBC.assertEmpty(stmt.executeQuery("select i from t1 for update of t, v"));
		JDBC.assertEmpty(stmt.executeQuery("select i from t1 for update of d"));
		assertStatementError("42X04", stmt, "select i as z from t1 for update of z");
        stmt.close();
         }
		

	 public void testCursor5() throws SQLException {
         Statement stmt = createStatement();
		stmt.setCursorName("C5");
		stmt.executeQuery("select i as v from t1 for update of v");
		try {
			stmt.executeUpdate("update t1 set v='hello' where current of C5");
			fail("ForUpdateTest: should have thrown exception");
		} catch (SQLException e) {
			assertSQLState("42X30", e);
		}
                
		JDBC.assertEmpty(stmt.executeQuery("select i from t1 for update of i, v, v, t"));		
		assertStatementError("42X01", stmt, "select i from t1 for update of t1.v, t1.i, t1.d");
		JDBC.assertEmpty(stmt.executeQuery("select a.i+10, d, d from t1 a for update"));
        stmt.close();
         }

 
	public void testStatistics() throws SQLException {
        Statement stmt = createStatement();

		String [][] expectedValues = { {"1", "hhhh"}, 
				               {"2", "uuuu"}, 
					       {"3", "yyyy"}, 
					       {"4", "aaaa"}, 
				               {"5", "jjjj"},
					       {"6", "rrrr"}, 
				               {"7", "iiii"},
				               {"8", "wwww"}, 
				               {"9", "rrrr"},
			 		       {"10", "cccc"}, 
				               {"11", "hhhh"},
					       {"12", "rrrr"} };			               
		stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");		  
		JDBC.assertFullResultSet(stmt.executeQuery("select i, b from t3 FOR UPDATE"), expectedValues);
		RuntimeStatisticsParser rtsp = SQLUtilities.getRuntimeStatisticsParser(stmt);
		assertTrue(rtsp.usedTableScan());
		assertFalse(rtsp.usedDistinctScan());
		commit();

		PreparedStatement p = prepareStatement("select i, b from t3  where i = ? FOR UPDATE");
                p.setString(1, "7");
                p.executeQuery();
		String [][] expectedValues1 = { {"7", "iiii" } };
		JDBC.assertFullResultSet(p.getResultSet(), expectedValues1);
		RuntimeStatisticsParser rtsp2 = SQLUtilities.getRuntimeStatisticsParser(stmt);
		assertFalse(rtsp2.usedTableScan());
		assertFalse(rtsp2.usedDistinctScan());
		p.close();
		commit();


		p = prepareStatement("select i, b from t3 where i < ? FOR UPDATE");
                p.setString(1, "7");
                p.executeQuery();
		String[][] expectedValues2 =  { {"1", "hhhh" },
						{"2", "uuuu" },
						{"3", "yyyy" },
						{"4", "aaaa" },
						{"5", "jjjj" },
						{"6", "rrrr" } };
		JDBC.assertFullResultSet(p.getResultSet(), expectedValues2);
		RuntimeStatisticsParser rtsp3 = SQLUtilities.getRuntimeStatisticsParser(stmt);
		assertFalse(rtsp3.usedTableScan());
		assertFalse(rtsp3.usedDistinctScan());              
		p.close();
		commit();


		p = prepareStatement("select i, b from t3  where b = ? FOR UPDATE");
                p.setString(1, "cccc");
                p.executeQuery();
		String[][] expectedValues3 = { {"10", "cccc" } };
		JDBC.assertFullResultSet(p.getResultSet(), expectedValues3);
		RuntimeStatisticsParser rtsp4 = SQLUtilities.getRuntimeStatisticsParser(stmt);
		assertFalse(rtsp4.usedTableScan());
		assertFalse(rtsp4.usedDistinctScan());
		p.close();
		commit();

	        stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");	
            stmt.close();
        }

        public void testCursors() throws SQLException {  
		ResultSet rs;
		String actualValue = null;

		Statement stmt2 = createStatement();
		stmt2.setCursorName("T3C1");
		rs = stmt2.executeQuery("select i,b from t3 where i = 4 for update");
                rs.next();
		assertEquals(rs.getInt("I") + " " + rs.getString("B"), "4 aaaa      ");
		try {
			rs.next();
			actualValue = rs.getInt("I") + " " + rs.getString("B");
		} catch (SQLException e) {
      			if (usingEmbedded())
				assertSQLState("24000", e);
			else 
				assertSQLState("XJ121", e);
		}
		rs.close();
		stmt2.close();


		stmt2 = createStatement();
		stmt2.setCursorName("T3C2");
		rs = stmt2.executeQuery("select i,b from t3 where i = 4 for update");
		rs.next();
		assertEquals(rs.getInt("I") + " " + rs.getString("B"), "4 aaaa      ");
                Statement stmt3 = createStatement();
		stmt3.executeUpdate("update t3 set i = 13 where current of T3C2");
		try {
			rs.next();
			actualValue = rs.getInt("I") + " " + rs.getString("B");
		} catch (SQLException e) {
      			if (usingEmbedded())
				assertSQLState("24000", e);
			else 
				assertSQLState("XJ121", e);
		}
		rs.close();
		stmt2.close();
		stmt3.close();


		stmt2 = createStatement();
		stmt2.setCursorName("T3C3");
		rs = stmt2.executeQuery("select i,b from t3 where i = 6 for update");
		rs.next();
		assertEquals(rs.getInt("I") + " " + rs.getString("B"), "6 rrrr      ");
		stmt3 = createStatement();
		stmt3.executeUpdate("update t3 set i = 14 where current of T3C3");
                stmt3.execute("insert into t3 values (6, 'new!')");	
		try {
			rs.next();
			actualValue = rs.getInt("I") + " " + rs.getString("B");
		} catch (SQLException e) {
      			if (usingEmbedded())
				assertSQLState("24000", e);
			else 
				assertSQLState("XJ121", e);
		}
                rs.close();
		stmt2.close();
		stmt3.close();


		stmt2 = createStatement();
		stmt2.execute("insert into t4 (c1) values (1),(2),(3)");
		stmt2.setCursorName("T3C4");
		rs = stmt2.executeQuery("select * from t4 for update of c1");
                rs.next();
		assertEquals(rs.getInt("C1"),1);
		stmt3 = createStatement();
                try { 
		   stmt3.executeUpdate("update t4 set c1=c1 where current of T3C4");
                } catch (SQLException sqle) {
		    assertSQLState("42X30", sqle);
                }
		rs.close();
                stmt2.close(); 
		stmt3.close();
	}              

	

}
