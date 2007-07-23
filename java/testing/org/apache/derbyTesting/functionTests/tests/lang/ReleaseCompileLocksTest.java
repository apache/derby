/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ReleaseCompileLocksTest

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

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;


/**
 * Tests for forupdate. 
 *
 */
public class ReleaseCompileLocksTest extends BaseJDBCTestCase {


	/* Public constructor required for running test as standalone JUnit. */    
	public ReleaseCompileLocksTest(String name) {
		super(name);
	}
    
    /**
     * Sets the auto commit to false.
     */
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }


	/* Set up fixture */ 
	protected void setUp() throws SQLException {
	    Statement stmt = createStatement();

	    stmt.execute("create function dmlstatic() returns INT " +
	    	"parameter style java language java external name " +
	    	"'org.apache.derbyTesting.functionTests.util.StaticInitializers." +
	    	"DMLInStaticInitializer.getANumber' no sql");
                  
	    stmt.execute("create function insertstatic() returns INT " +
	    	"parameter style java language java external name " +
	    	"'org.apache.derbyTesting.functionTests.util.StaticInitializers." +
	    	"InsertInStaticInitializer.getANumber' no sql");
        
        stmt.close();
        commit();
	}



	/**
         * Create a suite of tests.
         **/
        public static Test suite() {
        	TestSuite suite = new TestSuite("ReleasecompileLocksTest");
        	suite.addTest(baseSuite("ReleaseCompileLocksTest:embedded"));
        	suite.addTest(TestConfiguration.clientServerDecorator(baseSuite("ReleaseCompileLocksTest:client")));
        	return suite;
    	}

	protected static Test baseSuite(String name) {
        	TestSuite suite = new TestSuite(name);
        	suite.addTestSuite(ReleaseCompileLocksTest.class);
        	
		return new CleanDatabaseTestSetup(suite) 
        	{
            		protected void decorateSQL(Statement s) throws SQLException
            		{
				s.execute("create table t1 (s int)");	
            		}
        	};
    	} 

        /*Fixtures*/
        public void testReleaseCompileLocks() throws Exception {
            
            Statement stmt = createStatement();

        	JDBC.assertFullResultSet(stmt.executeQuery(
        		"select (dmlstatic()) from sys.systables where " +
        		"CAST(tablename AS VARCHAR(128))= 'SYSCONGLOMERATES'"), new String[][] {{"1"}});

                JDBC.assertEmpty(stmt.executeQuery("select TYPE, MODE, TABLENAME, LOCKNAME, STATE from syscs_diag.lock_table order by 1"));
		commit();

		stmt.execute("drop table t1");
		stmt.execute("create table t1 (s int)");
		commit();

		JDBC.assertFullResultSet(stmt.executeQuery(
        		"select (insertstatic()) from sys.systables where " +
        		"CAST(tablename AS VARCHAR(128)) = 'SYSCONGLOMERATES'"), new String[][] {{"1"}});

                JDBC.assertEmpty(stmt.executeQuery("select TYPE, MODE, TABLENAME, LOCKNAME, STATE from syscs_diag.lock_table order by 1"));

		JDBC.assertEmpty(stmt.executeQuery("select * from t1"));
		stmt.execute("drop table t1");
		commit();
	
		JDBC.assertEmpty(stmt.executeQuery("select TYPE, MODE, TABLENAME, LOCKNAME, STATE from syscs_diag.lock_table order by 1"));
		commit();

		stmt.execute("create table test_tab (x int)");
		stmt.executeUpdate("insert into test_tab values (1)");
		commit();

		JDBC.assertSingleValueResultSet(stmt.executeQuery("select count(*) from syscs_diag.lock_table"), "0");
		JDBC.assertSingleValueResultSet(stmt.executeQuery("select count(*) from sys.sysviews"), "0");
		JDBC.assertSingleValueResultSet(stmt.executeQuery("select count(*) from syscs_diag.lock_table"), "0");
		stmt.execute("insert into test_tab values (2)");

                ResultSet rs = stmt.executeQuery("select TYPE, MODE, TABLENAME, LOCKNAME, STATE from syscs_diag.lock_table order by 1");
		
		String expectedValues[][] = {{"ROW", "X", "TEST_TAB", "(1,8)", "GRANT" }, {"TABLE", "IX", "TEST_TAB", "Tablelock","GRANT"}};
                JDBC.assertFullResultSet(rs, expectedValues);

		try { 
		  	stmt.execute("drop table t1");
		  	fail ("expected SQLException; table t should not exist");
		} catch (SQLException e) {
			assertSQLState("42Y55", e);
		}
		stmt.execute("create table t1 (x int)");
		commit();
		
		JDBC.assertEmpty(stmt.executeQuery("select * from t1"));

		Connection conn1 = openDefaultConnection();
                Statement stmt2 = conn1.createStatement();
		stmt2.execute("create table t2 (x int)");
		stmt2.execute("drop table t2");
		stmt2.close();
        conn1.commit();
		conn1.close();

		stmt.execute("drop table test_tab");
		stmt.execute("create table test_tab (x int)");
		stmt.execute("insert into test_tab values (1)");
		commit();

		PreparedStatement ps = prepareStatement("update test_tab set x=2 where x=?");
		ps.setCursorName("cursor1");
		ps.setInt(1, 1);

		JDBC.assertSingleValueResultSet(stmt.executeQuery("select count(*) from syscs_diag.lock_table"), "0");
		ps.executeUpdate();
		
		rs = stmt.executeQuery("select TYPE, MODE, TABLENAME, LOCKNAME, STATE from syscs_diag.lock_table order by 1");
		String expectedValues1[][] = {{"ROW", "X", "TEST_TAB", "(1,7)", "GRANT" }, {"TABLE", "IX", "TEST_TAB", "Tablelock","GRANT"}};
                JDBC.assertFullResultSet(rs, expectedValues1);
		commit();

		
		stmt.execute("create table t (c1 int not null primary key, c2 int references t)");
		stmt.executeUpdate("insert into t values (1,1)");
		stmt.executeUpdate("insert into t values (2,1)");
		commit();

		ps = prepareStatement("select * from t where c1 = ? and c2 = ?");
		ps.setCursorName("ps");
		JDBC.assertEmpty(stmt.executeQuery("select * from syscs_diag.lock_table"));

		
		stmt.execute("create table x(c1 int)");
		stmt.execute("drop table x");
		commit();

		ps = prepareStatement("insert into t values (3,2)");
		ps.setCursorName("pi");
		JDBC.assertEmpty(stmt.executeQuery("select * from syscs_diag.lock_table"));
		commit();


		stmt.execute("create table x(c1 int)");
		stmt.execute("drop table x");
		commit();

		ps = prepareStatement("update t set c2 = c1, c1 = c2");
		ps.setCursorName("p1");
		JDBC.assertEmpty(stmt.executeQuery("select * from syscs_diag.lock_table"));
		commit();

		
		stmt.execute("create table x(c1 int)");
		stmt.execute("drop table x");
		commit();

		ps = prepareStatement("delete from t");
		ps.setCursorName("p1");
		JDBC.assertEmpty(stmt.executeQuery("select * from syscs_diag.lock_table"));
		commit();
		
		stmt.execute("create trigger update_of_t after update on t for each row values 2");
		stmt.execute("create trigger insert_of_t after insert on t for each row values 3");
		commit();
	
		ps = prepareStatement("update t set c2=2 where c1=2");
		ps.setCursorName("pu");
		JDBC.assertEmpty(stmt.executeQuery("select * from syscs_diag.lock_table"));
		commit();

		rs.close();
		ps.close();
		stmt.close();
       }
       
}
