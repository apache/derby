/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GrantRevokeTest

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Locale;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test SQL GRANT and REVOKE statements
 */
public class GrantRevokeTest extends BaseJDBCTestCase {

	public GrantRevokeTest(String name) {
		super(name);
	}
	
	/**
	 * The set of users available for grant/revoke testing
	 */
	public final static String[] users = new String[] { "TEST_DBO","U1","U2","U3","U4"};
	
	/**
	 * Most tests run in embedded only, since they are only checking DDL
	 * statements. Metadata methods test also runs in client/server mode.
	 */
	public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite();
		
		// following is useful for debugging the lock timeout seen in rollback tests,
		// can connect via network server and look at the lock table.
		//suite.addTest(TestConfiguration.clientServerDecorator(basesuite()));
		
		suite.addTest(basesuite());
		suite.addTest(TestConfiguration.clientServerDecorator(new GrantRevokeTest("testGrantDatabaseMetaDataMethods")));

		return suite;
	}
	
	/**
	 * One set of grant/revoke tests for either client/server or embedded.
	 */
	public static Test basesuite() {
        Test test = new BaseTestSuite(GrantRevokeTest.class);
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        // DERBY-6238: Dump the contents of the lock table on lock timeout.
        // Helps debug intermittent lock timeouts seen in the test.
        test = DatabasePropertyTestSetup.singleProperty(
                test, "derby.locks.deadlockTrace", "true");

        test = new CleanDatabaseTestSetup(test) {
	    	protected void decorateSQL(Statement s) throws SQLException {
	    		s.execute("create schema s1");
	    		s.execute("create schema s2");
	    		s.execute("create table s1.t1(c1 int, c2 int, c3 int)");
	    		s.execute("create table s2.t1(c1 int, c2 int, c3 int)");
	    		s.execute("create table s2.t2(c1 int, c2 int, c3 int)");
	    		s.execute("create table s2.t3(c1 int, c2 int, c3 int)");
	    	    s.execute("create table s2.noPerms(c1 int, c2 int, c3 int)");
	    	    s.execute("create function s1.f1() returns int" +
	    	        "  language java parameter style java" +
	    	        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.GrantRevokeTest.s1F1'" +
	    	        "  no sql called on null input");
	    	    /*
	    	     * RESOLVE Derby does not implement SPECIFIC names
	    	       
	    	       s.execute("create function s2.f1() returns int" +
                             "  specific s2.s2sp1" +
	    	                 "  language java parameter style java" +
	    	                 "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F1a'" +
	    	                 "  no sql called on null input");
	    	     * RESOLVE Derby doesn't seem to support function overloading. It doesn't allow us to create two
	    	     * functions with the same name but different signatures. (Though the StaticMethodCallNode.bindExpression
	    	     * method does have code to handle overloaded methods). So we cannot throughly test
	    	     * grant/revoke on overloaded procedures.
	    	        	         
	    	       s.execute("create function s2.f1( p1 char(8)) returns int" +
	    	                 "  language java parameter style java" +
	    	                 "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F1b'" +
	    	                 "  no sql called on null input");
	    	       s.execute("create function s2.f1( char(8), char(8)) returns int" +
	    	                 "  language java parameter style java" +
	    	                 "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F1c'" +
	    	                 "  no sql called on null input");
	    	       s.execute("create function s2.f1( int) returns int" +
	    	                 "  language java parameter style java" +
	    	                 "  external name 'org.apache.derbyTesting.functionTests.tests.lang.grantRevoke.s2F1d'" +
	    	                 "  no sql called on null input");
	    	      
	    	       NOTE: This function definition does not match the s2F2() definition
	    	             in this class, and thus is not used.
	    	       s.execute("create function s2.f2( p1 char(8), p2 integer) returns int" +
	    	                 "  language java parameter style java" +
	    	                 "  external name 'org.apache.derbyTesting.functionTests.tests.lang.GrantRevokeTest.s2F2'" +
	    	                 "  no sql called on null input");
                 */
	    	   
	    	   // functions and procedures are supposed to have separate name spaces. Make sure that this does
	    	   // not confuse grant/revoke.
	    	   s.execute("create procedure s1.f1( )" +
	    	        "  language java parameter style java" +
	    	        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.GrantRevokeTest.s1F1P'" +
	    	        "  no sql called on null input");
	    	   s.execute("create procedure s1.p1( )" +
	    	        "  language java parameter style java" +
	    	        "  external name 'org.apache.derbyTesting.functionTests.tests.lang.GrantRevokeTest.s1P1'" +
	    	        "  no sql called on null input");


               // DERBY-5292: Definer's rights in views
               s.execute("create schema appl");
               s.execute(
                   "CREATE TABLE appl.\"TBL_Tasks\"" +
                   "(\"TaskID\" integer NOT NULL " +
                   "                    GENERATED ALWAYS AS IDENTITY," +
                   " \"Task\" varchar(64) NOT NULL," +
                   " \"AssignedTo\" varchar(64) NOT NULL," +
                   "CONSTRAINT \"PK_Tasks\" PRIMARY KEY (\"TaskID\"))");
               s.execute(
                   "CREATE TABLE appl.\"TBL_Priorities\"" +
                   "(\"TaskID\" integer NOT NULL," +
                   " \"Priority\" integer NOT NULL," +
                   " \"SeqNbr\" integer NOT NULL," +
                   "CONSTRAINT \"PK_Priorities\" PRIMARY KEY " +
                   "  (\"TaskID\", \"Priority\"))");
               s.execute(
                   "CREATE VIEW appl.\"VW_MyTasks\" AS " +
                   "    SELECT * FROM appl.\"TBL_Tasks\" " +
                   "WHERE \"AssignedTo\" = SESSION_USER");
               s.execute(
                   "CREATE VIEW appl.\"VW_MyPriorityTasks\" AS " +
                   "    SELECT t.\"TaskID\", t.\"Task\", p.\"Priority\"" +
                   "    FROM appl.\"TBL_Tasks\" AS t," +
                   "         appl.\"TBL_Priorities\" AS p " +
                   "    WHERE p.\"TaskID\" = t.\"TaskID\" " +
                   "          AND t.\"AssignedTo\" = SESSION_USER");
               s.execute(
                   "CREATE VIEW appl.\"VW2_MyPriorityTasks\" AS " +
                   "    SELECT t.\"TaskID\", t.\"Task\", p.\"Priority\" " +
                   "    FROM appl.\"TBL_Tasks\" AS t INNER JOIN " +
                   "         appl.\"TBL_Priorities\" AS p ON " +
                   "         p.\"TaskID\" = t.\"TaskID\" " +
                   "    WHERE t.\"AssignedTo\" = SESSION_USER");
               s.execute(
                   "CREATE VIEW appl.\"VW3_MyPriorityTasks\" AS " +
                   "    SELECT t.\"TaskID\", t.\"Task\" " +
                   "    FROM appl.\"TBL_Tasks\" AS t " +
                   "    WHERE t.\"AssignedTo\" = SESSION_USER " +
                   "    AND EXISTS " +
                   "        (SELECT * FROM appl.\"TBL_Priorities\" AS p " +
                   "         WHERE p.\"TaskID\" = t.\"TaskID\")");
	    	}
	    };
        test = DatabasePropertyTestSetup.builtinAuthentication(
                test, users, "grantrevoke");
        test = TestConfiguration.sqlAuthorizationDecorator(test);
	    
	    return test;
	}
	
	public void testSimpleGrant() throws Exception {
		grant("select", "s1", "t1", users[1]);
		assertSelectPrivilege(true, users[1], "s1", "t1", null);
		assertSelectPrivilege(false, users[2], "s1", "t1", null);
//IC see: https://issues.apache.org/jira/browse/DERBY-3661
		assertSelectPrivilege(false, users[2], "s2", "t1", null);
		assertSelectPrivilege(false, users[2], "s2", "t2", null);
		revoke("select", "s1", "t1", users[1]);
	}
	
	public void testAllPrivileges() throws Exception {
		grant("all privileges", "s2", "t1", new String[] {users[2], users[3]});
		assertAllPrivileges(false, users[1], "S2", "T1", null);
		assertAllPrivileges(true, users[2], "S2", "T1", null);
		assertAllPrivileges(true, users[3], "S2", "T1", null);
		assertSelectPrivilege(false, users[1], "s1", "t1", null);
		assertSelectPrivilege(false, users[1], "s2", "t2", null);
		revoke("all privileges", "s2", "t1", new String[] {users[2], users[3]});
	}
	
	public void testColumnPrivileges() throws Exception {
		grant("select(c1),update(c3,c2),references(c3,c1,c2)", "s1", "t1", users[4]);
		assertSelectPrivilege(true, users[4], "s1", "t1", new String[] {"c1"});
		assertSelectPrivilege(false, users[4], "s1", "t1", new String[] {"c2"});
		assertSelectPrivilege(false, users[4], "s1", "t1", new String[] {"c3"});
		assertSelectPrivilege(false, users[4], "s1", "t1", null);
		assertUpdatePrivilege(false, users[4], "S1", "T1", new String[] {"C1"});
//IC see: https://issues.apache.org/jira/browse/DERBY-2893
		assertUpdatePrivilege(true, users[4], "S1", "T1", new String[] {"C2", "C3"});
		assertReferencesPrivilege(true, users[4], "s1", "t1", new String[] {"c1","c2","c3"});
		revoke("select(c1),update(c3,c2),references(c3,c1,c2)", "s1", "t1", users[4]);
	}
	
	public void testFunctionWithSameProcedureName() throws Exception {
		grant("execute", "function s1", "f1", users[1]);
		assertFunctionPrivilege( true, users[1], "S1", "F1", false);
		assertProcedurePrivilege( false, users[1], "S1", "F1");
		assertFunctionPrivilege( false, users[2], "S1", "F1", false);
		revoke("execute", "function s1", "f1", users[1]);
	}
	
	public void testGrantOnProcedure() throws Exception {
		grant("execute", "procedure s1", "p1", users[1]);
		assertProcedurePrivilege( true, users[1], "S1", "P1");
		assertFunctionPrivilege( false, users[1], "S1", "P1", true);
		assertProcedurePrivilege( false, users[2], "S1", "P1");
		assertFunctionPrivilege( false, users[2], "S1", "P1", true);
		revoke("execute", "procedure s1", "p1", users[1]);
	}
	
    public void testPublicTablePrivileges() throws Exception {
    	grant("select, references(c1)", "s2", "t2", "public");
    	assertSelectPrivilege(true, users[4], "S2", "T2", null);
        assertSelectPrivilege(true, users[1], "S2", "T2", null);
        assertSelectPrivilege(false, users[4], "S2", "NOPERMS", null);
        assertUpdatePrivilege(false, users[4], "S2", "T2", null);
        assertReferencesPrivilege(true, users[4], "S2", "T2",
                                 new String[] {"C1"});
        assertReferencesPrivilege(false, users[4], "S2", "T2", null);
    	revoke("select, references(c1)", "s2", "t2", "public");
    }
    
    public void testPublicRoutinePrivileges() throws Exception {
        grant("execute", "procedure s1", "p1", "public");
        grant("execute", "procedure s1", "p1", users[1]);        
		assertProcedurePrivilege(true, users[1], "S1", "P1");
		assertProcedurePrivilege(true, users[4], "S1", "P1");
        revoke("execute", "procedure s1", "p1", "public");
        // user1 should still have execute privilege
        assertProcedurePrivilege(true, users[1], "S1", "P1");
        assertProcedurePrivilege(false, users[4], "S1", "P1");
        revoke("execute", "procedure s1", "p1", users[1]);
        assertProcedurePrivilege(false, users[1], "S1", "P1");
    }

    /**
     * Test grant statements before, during, and after a rolled-back
     * transaction
     */
    public void testGrantRollbackAndCommit() throws SQLException {
    
    	// NOTE: grantrevoke.java originally used S2.F2 for the function
    	// below, but the signature on the function didn't match the
    	// declaration, so was not used properly. Have substituted
    	// function S1.F1 here to get the testcase to pass.
    	
    	// NOTE 2: executing the grant statements on the owner connection
    	//         leads to a lock timeout when asserting any privilege?
    	
    	Connection oc = openUserConnection(users[0]);
    	oc.setAutoCommit(false);
    	
    	// set up some privileges and check them
    	grant(oc, "select", "s2", "t2", "public");
    	oc.commit();

        assertSelectPrivilege(true, users[3], "S2", "T2", null);
        assertUpdatePrivilege(false, users[3], "S2", "T2", null);
        assertSelectPrivilege(false, users[1], "S2", "T3", new String[] {"C2"});
        assertDeletePrivilege(false, users[1], "S2", "T3");
        assertTriggerPrivilege(false, users[2], "S2", "T2");
        assertFunctionPrivilege(false, users[1], "S1", "F1", false);
    	
    	// alter some privileges, assert that they were granted.
        grant(oc, "select(c2),delete", "s2", "t3", users[1]);
        grant(oc, "trigger", "s2", "t2", "public");
    	grant(oc, "execute", "function s1", "f1", users[1]);
        // the following asserts fail due to lock timeout
    	//assertSelectPrivilege(true, users[1], "s2", "t3", new String[] {"C2"});
        //assertDeletePrivilege(true, users[1], "s2", "t3");
        //assertTriggerPrivilege(true, users[2], "S2", "T2");
        //assertFunctionPrivilege(true, users[1], "S1", "F1", false);
        
        // roll it back and assert the privileges were not granted.
        oc.rollback();
        assertSelectPrivilege(false, users[1], "S2", "T3", new String[] {"C2"});
        assertDeletePrivilege(false, users[1], "S2", "T3");
        assertTriggerPrivilege(false, users[2], "S2", "T2");
        assertFunctionPrivilege(false, users[1], "S1", "F1", false);
        
        // do it again... 
        grant(oc, "select(c2),delete", "s2", "t3", users[1]);
        grant(oc, "trigger", "s2", "t2", "public");
    	grant(oc, "execute", "function s1", "f1", users[1]);
        // the following asserts fail due to lock timeout
    	//assertSelectPrivilege(true, users[1], "S2", "T3", new String[] {"C2"});
        //assertDeletePrivilege(true, users[1], "S2", "T3");
        //assertTriggerPrivilege(true, users[2], "S2", "T2");
        //assertFunctionPrivilege(true, users[1], "S1", "F1", false);
        
        // commit and ensure the permissions are correct
        oc.commit();
        assertSelectPrivilege(true, users[1], "S2", "T3", new String[] {"C2"});
        assertDeletePrivilege(true, users[1], "S2", "T3");
        assertTriggerPrivilege( true, users[2], "S2", "T2");
        assertFunctionPrivilege( true, users[1], "S1", "F1", false);
       
    	// remove any permissions we granted
    	revoke(oc, "select", "s2", "t2", "public");
    	revoke(oc, "select(c2),delete", "s2", "t3", users[1]);
        revoke(oc, "trigger", "s2", "t2", "public");
    	revoke(oc, "execute", "function s1", "f1", users[1]);
    	oc.commit();
    	oc.setAutoCommit(false);
        assertSelectPrivilege(false, users[3], "S2", "T2", null);
        assertUpdatePrivilege(false, users[3], "S2", "T2", null);
        assertSelectPrivilege(false, users[1], "S2", "T3", new String[] {"C2"});
        assertDeletePrivilege(false, users[1], "S2", "T3");
        assertTriggerPrivilege(false, users[2], "S2", "T2");
        assertFunctionPrivilege(false, users[1], "S1", "F1", false);
    	
    	oc.close();
    	
    }
	/**
	 * Test Grant/Revoke related DatabaseMetaData methods.
	 */
    public void testGrantDatabaseMetaDataMethods() throws Exception{
    	DatabaseMetaData dm = getConnection().getMetaData();
    	assertFalse("GrantRevoke: DatabaseMetaData.supportsCatalogsInPrivilegeDefinitionSupport", dm.supportsCatalogsInPrivilegeDefinitions());
    	assertTrue("GrantRevoke: DatabaseMetaData.supportsSchemasInPrivilegeDefinitions", dm.supportsSchemasInPrivilegeDefinitions());
    }
    
    /* Revoke test methods begin here */
    
    /**
     * Test revoke statements when user already has no permissions.
     */
    public void testRevokeWithNoPermissions() throws Exception {
        // assert users don't already have these privileges.
    	assertSelectPrivilege(false, users[1], "S1", "T1", null);
        assertSelectPrivilege(false, users[2], "S1", "T1", new String[] {"C2"});
        assertUpdatePrivilege(false, users[2], "S1", "T1", new String[] {"C1", "C3"});
        assertProcedurePrivilege(false, users[1], "S1", "P1");
        
        // no unexpected exception should be thrown revoking these privileges.
        revoke("all privileges", "s1", "t1", users[1]);
    	assertSelectPrivilege(false, users[1], "S1", "T1", null);
        assertSelectPrivilege(false, users[1], "S1", "T1", new String[] {"C2"});
        revoke("execute", "procedure s1", "p1", users[1]);
        assertProcedurePrivilege(false, users[1], "S1", "P1");
        revoke("select(c2), update(c1,c3)", "s1", "t1", users[2]);
        assertSelectPrivilege(false, users[2], "S1", "T1", new String[] {"C2"});
        assertUpdatePrivilege(false, users[2], "S1", "T1", new String[] {"C1", "C3"});  
    }
    
    public void testRevokeSingleTableSingleUser() throws Exception {
    	grant("all privileges", "s2", "t1", users[1]);
        grant("update(c3)", "s2", "t1", users[1]);
        assertSelectPrivilege(true, users[1], "S2", "T1", null);
        assertUpdatePrivilege(true, users[1], "S2", "T1", new String[] {"C3"});  
        
        revoke("update", "S2", "t1", users[1]);
        assertSelectPrivilege( true, users[1], "S2", "T1", null);
        assertUpdatePrivilege( false, users[1], "S2", "T1", null);
        assertUpdatePrivilege( false, users[1], "S2", "T1", new String[] {"C3"});
        assertInsertPrivilege( true, users[1], "S2", "T1", null);
        assertDeletePrivilege( true, users[1], "S2", "T1");
        assertReferencesPrivilege( true, users[1], "S2", "T1", null);
        assertTriggerPrivilege( true, users[1], "S2", "T1");
        
        revoke("all privileges", "s2", "t1", users[1]);
        assertAllPrivileges(false, users[1], "S2", "T1", null);
    }
    
    public void testRevokeMultiplePermissionsMultipleUsers() throws SQLException {
    	grant("select", "s1", "t1", new String[] {users[1], users[2], users[3]});
    	grant("update(c1,c2,c3)", "s1", "t1", users[1]);
    	grant("update(c3)", "s1", "t1", users[2]);
    	grant("trigger", "s1", "t1", users[1]);
        assertSelectPrivilege(true, users[1], "S1", "T1", null);
        assertSelectPrivilege(true, users[2], "S1", "T1", null);
        assertSelectPrivilege(true, users[3], "S1", "T1", null);
        // DatabaseMetaData.getTablePrivileges() returns false for the following
        // due to column privileges for table being used, so assert
        // with null for columns is disabled
        //assertUpdatePrivilege(true, users[1], "S1", "T1", null);  
        assertUpdatePrivilege(true, users[1], "S1", "T1", new String[] {"C1", "C2", "C3" }); 
        assertUpdatePrivilege(false, users[2], "S1", "T1", new String[] {"C1", "C2"});
        assertUpdatePrivilege(true, users[2], "S1", "T1", new String[] {"C3"});
        assertTriggerPrivilege(true, users[1], "S1", "T1");
        assertTriggerPrivilege(false, users[2], "S1", "T1");
        
        revoke("select, update(c2,c3)", "s1", "t1", new String[] {users[1], users[2], users[3]});
        assertSelectPrivilege(false, users[1], "S1", "T1", null);
        assertSelectPrivilege(false, users[2], "S1", "T1", null);
        assertSelectPrivilege(false, users[3], "S1", "T1", null);
        assertUpdatePrivilege(true, users[1], "S1", "T1", new String[] {"C1"});  
        assertUpdatePrivilege(false, users[1], "S1", "T1", new String[] {"C2", "C3"});
        assertUpdatePrivilege(false, users[2], "S1", "T1", null);
        assertTriggerPrivilege(true, users[1], "S1", "T1");
        assertTriggerPrivilege(false, users[2], "S1", "T1");
       
        revoke("update", "s1", "t1", users[1]);
        assertUpdatePrivilege(false, users[1], "S1", "T1", new String[] {"C1"});  
        assertUpdatePrivilege(false, users[1], "S1", "T1", null);
     
        revoke("all privileges", "s1", "t1", users[1]);
        assertAllPrivileges(false, users[1], "S1", "T1", null);
    }
    
    public void testRevokeExecutePrivileges() throws Exception {
    	grant("execute", "function s1", "f1", new String[] {users[1], users[2]});
        grant("execute", "procedure s1", "f1", users[1]);
        assertFunctionPrivilege(true, users[1], "S1", "F1", false);
        assertFunctionPrivilege(true, users[2], "S1", "F1", false);
        assertProcedurePrivilege(true, users[1], "S1", "F1");
        
        revoke("execute", "function s1", "f1", users[1]);
        assertFunctionPrivilege(false, users[1], "S1", "F1", false);
        assertFunctionPrivilege(true, users[2], "S1", "F1", false);
        assertProcedurePrivilege(true, users[1], "S1", "F1");
        
        grant("execute", "function s1", "f1", users[1]);
        revoke("execute", "procedure s1", "f1", users[1]);
        assertFunctionPrivilege(true, users[1], "S1", "F1", false);
        assertFunctionPrivilege(true, users[2], "S1", "F1", false);
        assertProcedurePrivilege(false, users[1], "S1", "F1");
        
    	revoke("execute", "function s1", "f1", new String[] {users[1], users[2]});
        assertFunctionPrivilege(false, users[1], "S1", "F1", false);
        assertFunctionPrivilege(false, users[2], "S1", "F1", false);
        assertProcedurePrivilege(false, users[1], "S1", "F1");
    }
    
    public void testRevokeWithPublicPrivilege() throws Exception {
        grant("select, delete", "s2", "t1", "public");
        grant("select, delete", "s2", "t1", new String[] {users[1], users[2]});
        grant("update(c1,c3)", "s2", "t1", "public");
        grant("update(c1,c3)", "s2", "t1", new String[] {users[1], users[2]});
        assertSelectPrivilege( true, users[1], "S2", "T1", null);
        assertSelectPrivilege( true, users[2], "S2", "T1", null);
        assertSelectPrivilege( true, users[4], "S2", "T1", null);
        assertDeletePrivilege( true, users[1], "S2", "T1");
        assertDeletePrivilege( true, users[2], "S2", "T1");
        assertDeletePrivilege( true, users[4], "S2", "T1");
        assertUpdatePrivilege( true, users[1], "S2", "T1", new String[] {"C1", "C3"});
        assertUpdatePrivilege( true, users[2], "S2", "T1", new String[] {"C1", "C3"});
        assertUpdatePrivilege( true, users[4], "S2", "T1", new String[] {"C1", "C3"});

        // revoke from user, should still be able to access via public privilege
        revoke("select, update(c1,c3), delete", "S2", "T1", users[1]);
        assertSelectPrivilege( true, users[1], "S2", "T1", null);
        assertSelectPrivilege( true, users[2], "S2", "T1", null);
        assertSelectPrivilege( true, users[4], "S2", "T1", null);
        assertDeletePrivilege( true, users[1], "S2", "T1");
        assertDeletePrivilege( true, users[2], "S2", "T1");
        assertDeletePrivilege( true, users[4], "S2", "T1");
        assertUpdatePrivilege( true, users[1], "S2", "T1", new String[] {"C1", "C3"});
        assertUpdatePrivilege( true, users[2], "S2", "T1", new String[] {"C1", "C3"});
        assertUpdatePrivilege( true, users[4], "S2", "T1", new String[] {"C1", "C3"});
       
        // now, revoke public permissions
        revoke("select, update(c1,c3), delete", "S2", "t1", "public");
        assertSelectPrivilege(false, users[1], "S2", "T1", null);
        assertSelectPrivilege(true, users[2], "S2", "T1", null);
        assertSelectPrivilege(false, users[4], "S2", "T1", null);
        assertDeletePrivilege(false, users[1], "S2", "T1");
        assertDeletePrivilege(true, users[2], "S2", "T1");
        assertDeletePrivilege(false, users[4], "S2", "T1");
        assertUpdatePrivilege(false, users[1], "S2", "T1", new String[] {"C1", "C3"});
        assertUpdatePrivilege(true, users[2], "S2", "T1", new String[] {"C1", "C3"});
        assertUpdatePrivilege(false, users[4], "S2", "T1", new String[] {"C1", "C3"});

        // clean up
        revoke("all privileges", "S2", "t1", users[2]);
        assertAllPrivileges(false, users[2], "S2", "T1", null);
     }
    
    public void testRevokeExecuteWithPublicPrivilege() throws Exception {
        grant("execute", "function s1", "f1", "public");
        grant("execute", "function s1", "f1", new String[] {users[1], users[2]});
        assertFunctionPrivilege(true, users[1], "S1", "F1", false);
        assertFunctionPrivilege(true, users[2], "S1", "F1", false);
        assertFunctionPrivilege(true, users[4], "S1", "F1", false);
        
        //revoke from user, should still be able to execute through public privilege
        revoke("execute", "function s1", "f1", users[1]);
        assertFunctionPrivilege(true, users[1], "S1", "F1", false);
        assertFunctionPrivilege(true, users[2], "S1", "F1", false);
        assertFunctionPrivilege(true, users[4], "S1", "F1", false);
   
        revoke("execute", "function s1", "f1", "public");
        assertFunctionPrivilege(false, users[1], "S1", "F1", false);
        assertFunctionPrivilege(true, users[2], "S1", "F1", false);
        assertFunctionPrivilege(false, users[4], "S1", "F1", false);
        
        // clean up
        revoke("execute", "function s1", "f1", users[2]);
        assertFunctionPrivilege(false, users[2], "s1", "F1", false);
    }
    
    public void testRevokeRollbackAndCommit() throws Exception {
        
    	// open a connection as database owner.
    	Connection oc = openUserConnection(users[0]);
    	oc.setAutoCommit(false);
   	
    	//set up some permissions
        grant(oc, "select(c1,c2), update(c1), insert, delete", "s2", "t3", users[1]);
        grant(oc, "select, references", "s2", "t3", users[2]);
        grant(oc, "select", "s2", "t3", users[3]);
        grant(oc, "execute", "procedure s1", "p1", users[1]);
        oc.commit();
        assertSelectPrivilege(true, users[1], "S2", "T3", new String[] { "C1", "C2"});
        assertUpdatePrivilege(true, users[1], "S2", "T3", new String[] { "C1"});
        assertInsertPrivilege(true, users[1], "S2", "T3", null);
        assertDeletePrivilege(true, users[1], "S2", "T3");
        assertSelectPrivilege(true, users[2], "S2", "T3", null);
        assertReferencesPrivilege(true, users[2], "S2", "T3", null);
        assertSelectPrivilege(true, users[3], "S2", "T3", null);
        assertProcedurePrivilege(true, users[1], "S1", "P1");
    	
    	// revoke the privileges and verify they were revoked.
        revoke(oc, "select(c2), update(c1), delete", "s2", "t3", users[1]);
        revoke(oc, "select, references", "s2", "t3", users[2]);
        revoke(oc, "select", "s2", "t3", users[3]);
        revoke(oc, "execute", "procedure s1", "p1", users[1]);
        // these asserts fail before rollback due to lock timeout
        //assertSelectPrivilege(true, users[1], "S2", "T3", new String[] {"C1"});
        //assertSelectPrivilege(false, users[1], "S2", "T3", new String[] {"C2", "C3"});
        //assertUpdatePrivilege(false, users[1], "S2", "T3", new String[] {"C1"});
        //assertInsertPrivilege(false, users[1], "S2", "T3", null);
        //assertDeletePrivilege(false, users[1], "S2", "T3");
        //assertSelectPrivilege(false, users[2], "S2", "T3", null);
        //assertReferencesPrivilege(false, users[2], "S2", "T3", null);
        //assertSelectPrivilege(false, users[3], "S2", "T3", null);
        //assertProcedurePrivilege(false, users[1], "S1", "P1");

        // rollback and verify that we have them again.
        oc.rollback();
        assertSelectPrivilege(true, users[1], "S2", "T3", new String[] {"C1", "C2"});
        assertUpdatePrivilege(true, users[1], "S2", "T3", new String[] {"C1"});
        assertInsertPrivilege(true, users[1], "S2", "T3", null);
        assertDeletePrivilege(true, users[1], "S2", "T3");
        assertSelectPrivilege(true, users[2], "S2", "T3", null);
        assertReferencesPrivilege(true, users[2], "S2", "T3", null);
        assertSelectPrivilege(true, users[3], "S2", "T3", null);
        assertProcedurePrivilege(true, users[1], "S1", "P1");
        
    	// revoke again, verify they were revoked.
        revoke(oc, "select(c2), update(c1), delete", "s2", "t3", users[1]);
        revoke(oc, "select, references", "s2", "t3", users[2]);
        revoke(oc, "select", "s2", "t3", users[3]);
        revoke(oc, "execute", "procedure s1", "p1", users[1]);
        // these asserts fail before commit due to lock timeout
        //assertSelectPrivilege(false, users[1], "S2", "T3", new String[] {"C1", "C2"});
        //assertUpdatePrivilege(false, users[1], "S2", "T3", new String[] {"C1"});
        //assertInsertPrivilege(false, users[1], "S2", "T3", null);
        //assertDeletePrivilege(false, users[1], "S2", "T3");
        //assertSelectPrivilege(false, users[2], "S2", "T3", null);
        //assertReferencesPrivilege(false, users[2], "S2", "T3", null);
        //assertSelectPrivilege(false, users[3], "S2", "T3", null);
        //assertProcedurePrivilege(false, users[1], "S1", "P1");
        
        //commit and verify again
        oc.commit();
        oc.setAutoCommit(true);
        assertSelectPrivilege(false, users[1], "S2", "T3", new String[] {"C1", "C2"});
        assertUpdatePrivilege(false, users[1], "S2", "T3", new String[] {"C1"});
        assertInsertPrivilege(true, users[1], "S2", "T3", null);
        assertDeletePrivilege(false, users[1], "S2", "T3");
        assertSelectPrivilege(false, users[2], "S2", "T3", null);
        assertReferencesPrivilege(false, users[2], "S2", "T3", null);
        assertSelectPrivilege(false, users[3], "S2", "T3", null);
        assertProcedurePrivilege(false, users[1], "S1", "P1");
        
    }
    
    /*
     * TODO - write tests for abandoned views / triggers / constraints
     * 
    public void testAbandonedView() {
    	//TODO
    }
    
    public void testAbandonedTrigger() {
        //TODO	
    }

    public void testAbandonedConstraint() {
    	//TODO
    }
    */
    
    /* Begin standard error cases */
    
    public void testInvalidGrantAction() throws Exception {
    	try {
            grant("xx", "s1", "t1", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42X01", e);
        }
    }

    public void testInvalidReservedWordAction() throws Exception {
    	try {
            grant("between", "s1", "t1", users[1]);
    	} catch (SQLException e) {
            	assertSQLState("42X01", e);
    	}
        
        assertCompileError("42X01", "grant select on schema t1 to " + users[1]);

        assertCompileError("42X01",  "grant select on decimal t1 to " + users[1]);
    }

    public void testGrantOnNonexistantColumn() throws Exception {
    	try {
            grant("select(nosuchCol)", "s1", "t1", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42X14", e);
        }
    }
    
    public void testGrantOnNonexistantSchema() throws Exception {
    	try {
            grant("select", "nosuch", "t1", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42Y07", e);
        }
    }
    
    public void testGrantOnNonexistantTable() throws Exception {
    	try {
            grant("select(nosuchCol)", "s1", "nosuch", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42X05", e);
        }
    }

    public void testGrantOnFunctionWithBadSchema() throws Exception {
    	try {
            grant("execute", "function nosuch", "f0", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42Y07", e);
        }
    }
    
    public void testGrantOnNonexistantFunction() throws Exception {
    	try {
            grant("execute", "function s1", "nosuch", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42Y03", e);
//IC see: https://issues.apache.org/jira/browse/DERBY-5252
        	if ( Locale.getDefault().getLanguage().equals("en") ) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3610
        	    assertEquals("'S1.NOSUCH' is not recognized as a function.", e.getMessage());
        	}
        }
    }
    
    public void testGrantOnNonexistantFunctionForProcedure() throws Exception {
    	try {
            grant("execute", "function s1", "p1", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42Y03", e);
//IC see: https://issues.apache.org/jira/browse/DERBY-5252
            if ( Locale.getDefault().getLanguage().equals("en") ) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3610
                assertEquals("'S1.P1' is not recognized as a function.", e.getMessage());
            }
        }
    }
    
    public void testGrantOnProcedureWithBadSchema() throws Exception {
    	try {
            grant("execute", "procedure nosuch", "f0", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42Y07", e);
        }
    }
    
    public void testGrantOnNonexistantProcedure() throws Exception {
    	try {
            grant("execute", "procedure s1", "nosuch", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42Y03", e);
//IC see: https://issues.apache.org/jira/browse/DERBY-5252
        	if ( Locale.getDefault().getLanguage().equals("en") ) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3610
        	    assertEquals("'S1.NOSUCH' is not recognized as a procedure.", e.getMessage());
        	}
        }
    }
    
    public void testGrantOnNonexistantProcedureForFunction() throws Exception {
    	try {
            grant("execute", "procedure s1", "f2", users[1]);
    	} catch (SQLException e) {
        	assertSQLState("42Y03", e);
//IC see: https://issues.apache.org/jira/browse/DERBY-5252
        	if ( Locale.getDefault().getLanguage().equals("en") ) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3610
        	    assertEquals("'S1.F2' is not recognized as a procedure.", e.getMessage());
        	}
        }
    }
    
    public void testGrantExecuteOnTable() throws Exception {
        assertCompileError("42X01",
                "grant execute on table s1.t1 to " + users[1]);
     }
    
    public void testGrantSelectOnRoutine() throws Exception {
        assertCompileError("42X01",
                "grant select on function s1.f1 to " + users[1]);
   	
        assertCompileError("42X01",
                "grant select on procedure s1.p1 to " + users[1]);
   }
    
    public void testGrantExecuteWithRestrict() throws Exception {
    	// restrict invalid in grant statement
        assertCompileError("42X01",
                "grant execute on function s1.f1 to " + users[1] + " restrict");
    }
    
    public void testGrantRevokeWithoutRestrict() throws Exception {
    	// restrict invalid in grant statement
        assertCompileError("42X01",
               "revoke execute on function s1.f1 from " + users[0]);
    }
    
    public void testGrantRevokeSelectWithRestrict() throws Exception {
    	// restrict invalid in grant statement
        assertCompileError("42X01",
    	     "revoke select on s1.t1 from " + users[0] + " restrict");
    }
    
    public void testGrantDeleteWithColumnList() throws Exception {
    	try {
    		grant("delete(c1)", "s1", "t1", users[1]);
    	} catch (SQLException e) {
    		assertSQLState("42X01", e);
    	}
    }
    
    public void testGrantTriggerWithColumnList() throws Exception {
    	try {
    		grant("trigger(c1)", "s1", "t1", users[1]);
    	} catch (SQLException e) {
    		assertSQLState("42X01", e);
    	}
    }
    
    /* End standard error cases */
    
    /* Begin testcases from grantRevokeDDL */

    public void testOtherUserCannotRevokeOwnerPrivileges() throws SQLException {
    	grant("select", "s1", "t1", "public");
    	grant("insert", "s1", "t1", users[1]);
    	grant("update", "s1", "t1", users[1]);
    	grant("delete", "s1", "t1", users[1]);
    	grant("update(c1)", "s1", "t1", users[2]);
        try {
        	revoke(users[2], "select", "s1", "t1", "public");
        } catch (SQLException e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
            assertSQLState("42506", e);
        }
        try {
        	revoke(users[2], "select", "s1", "t1", users[0]);
        } catch (SQLException e) {
            assertSQLState("42509", e);
        }
        try {
        	revoke(users[2], "insert", "s1", "t1", users[1]);
        } catch (SQLException e) {
            assertSQLState("42506", e);
        }
        try {
        	revoke(users[2], "update(c1)", "s1", "t1", users[2]);
        } catch (SQLException e) {
            assertSQLState("42506", e);
        }
        
        // clean up
//IC see: https://issues.apache.org/jira/browse/DERBY-3224
//IC see: https://issues.apache.org/jira/browse/DERBY-3176
        revoke("select", "s1", "t1", "public");
        revoke("all privileges", "s1", "t1", users[1]);
        revoke("all privileges", "s1", "t1", users[2]);
        assertAllPrivileges(false, users[1], "S1", "T1", null);
        assertAllPrivileges(false, users[2], "S1", "T1", null);
        
    }

    /**
     * DERBY-5292
     */
    public void testViewDefinersRights () throws Exception {

        grant("select", "appl", "\"VW_MyTasks\"", users[1]);
        grant("select", "appl", "\"VW_MyPriorityTasks\"", users[1]);
        grant("select", "appl", "\"VW2_MyPriorityTasks\"", users[1]);
        grant("select", "appl", "\"VW3_MyPriorityTasks\"", users[1]);

        // OK before fix
        assertSelectPrivilege(
            true, users[1], "appl", "\"VW_MyTasks\"", null);
        assertSelectPrivilege(
            true, users[1], "appl", "\"VW_MyPriorityTasks\"", null);

        // Failed before fix
        assertSelectPrivilege(
            true, users[1], "appl", "\"VW2_MyPriorityTasks\"", null);
        assertSelectPrivilege(
            true, users[1], "appl", "\"VW3_MyPriorityTasks\"", null);
    }

    /* End testcases from grantRevokeDDL */
    
    /* Begin utility methods specific to grant / revoke */
    
    /**
     * Grant a single permission to a single user.
     * Utility method that takes a single string for user instead
     * of an array of Strings.
     * 
     * @param perm Permission to grant
     * @param schema Schema on which to grant permission
     * @param table Table on which to grant permission
     * @param user User to grant permission to
     * @throws Exception throws all exceptions
     */
	void grant(String perm, String schema, String table, String user) throws SQLException {
		grant(perm, schema, table, new String[] {user});
	}
	
    /**
     * Grant a single permission from a specific user to a single user.
     * Utility method that takes a single string for user instead
     * of an array of Strings.
     * 
     * @param grantor Grantor of permission
     * @param perm Permission to grant
     * @param schema Schema on which to grant permission
     * @param table Table on which to grant permission
     * @param user User to grant permission to
     * @throws Exception throws all exceptions
     */
	void grant(String grantor, String perm, String schema, String table, String user) throws SQLException {
		Connection c = openUserConnection(grantor);
		grant(c, perm, schema, table, user);
		c.close();
	}
	
	/**
	 * Grant a SQL permission to a set of users.
	 * 
	 * @param perm The permission to grant
	 * @param schema the schema on which to grant the permission
	 * @param table the table on which to grant the permission
	 * @param users an Array of users to grant the permission
	 * @throws Exception throws all exceptions
	 */
	void grant(String perm, String schema, String table, String[] users) throws SQLException {
		StringBuffer command = new StringBuffer("grant " + perm + " on " + schema + "." + table + " to " + users[0]);
		for (int i = 1; i < users.length; i++ ) {
		    command.append("," + users[i]);
		}
        
		// default connection is for database owner.
		Statement s = getConnection().createStatement();
		s.executeUpdate(command.toString());
		s.close();
	}
	
    /**
     * Grant a single permission to a single user for a given connection.
     * Callers of this method should ensure that they close the Connection
     * that is passed in. Used primarily in rollback tests where we want to ensure
     * the grant/revoke statements are being called by the database owner.
     * 
     * @param c the Connection used to execute the grant statement
     * @param perm Permission to grant
     * @param schema Schema on which to grant permission
     * @param table Table on which to grant permission
     * @param user User to grant permission to
     * @throws Exception throws all exceptions
     */
	void grant(Connection c, String perm, String schema, String table, String user) throws SQLException {
		Statement s = c.createStatement();
		s.executeUpdate("grant " + perm + " on " + schema + "." + table + " to " + user);
		s.close();
	}
	
    /**
     * Revoke a single permission from a single user.
     * Utility method that takes a single string for user instead
     * of an array of Strings.
     * 
     * @param perm Permission to revoke
     * @param schema Schema on which to revoke permission
     * @param table Table on which to revoke permission
     * @param user User to revoke permissions
     * @throws Exception throws all exceptions
     */
	void revoke(String perm, String schema, String table, String user) throws SQLException {
		revoke(perm, schema, table, new String[] {user});
	}
	

    /**
     * Revoke a single permission from a specific user to a single user.
     * Utility method that takes a single string for user instead
     * of an array of Strings.
     * 
     * @param revoker Grantor of permission
     * @param perm Permission to revoke
     * @param schema Schema on which to revoke permission
     * @param table Table on which to revoke permission
     * @param user User to revoke permission to
     * @throws Exception throws all exceptions
     */
	void revoke(String revoker, String perm, String schema, String table, String user) throws SQLException {
		Connection c = openUserConnection(revoker);
		revoke(c, perm, schema, table, user);
		c.close();
	}
	
	/**
	 * Revoke a SQL permission from a set of users.
	 * 
	 * @param perm The permission to revoke
	 * @param schema the schema on which to revoke the permission
	 * @param table the table on which to revoke the permission
	 * @param users an array of users to revoke the permission
	 * @throws Exception throws all exceptions
	 */
	void revoke(String perm, String schema, String table, String[] users) throws SQLException {
		StringBuffer command = new StringBuffer("revoke " + perm + " on " + schema + "." + table + " from " + users[0]);
		for (int i = 1; i < users.length; i++ ) {
		    command.append("," + users[i]);
		}
		//add restrict to revoke execute... 
		if (perm.equalsIgnoreCase("execute"))
			command.append(" restrict");
        
        Statement s = createStatement();
        s.executeUpdate(command.toString());
        s.close();
	}

    /**
     * Revoke a single permission to a single user for a given connection.
     * Callers of this method should ensure that they close the Connection
     * that is passed in. Used primarily in rollback tests where we want to ensure
     * the grant/revoke statements are being called by the database owner.
     * 
     * @param c the connection to execute the revoke statement
     * @param perm Permission to revoke
     * @param schema Schema on which to revoke permission
     * @param table Table on which to revoke permission
     * @param user User to revoke permission
     * @throws Exception throws all exceptions
     */
	void revoke(Connection c, String perm, String schema, String table, String user) throws SQLException {
		Statement s = c.createStatement();
		s.execute("revoke " + perm + " on " + schema + "." + table + " from " + user + (perm.equalsIgnoreCase("execute") ? " restrict" : ""));
		s.close();
	}
	
    /* End utility methods specific to grant / revoke */
	
	/**
	 * Utility function to test grant/revoke
	 * @return 1
	 */
    public static int s1F1()
    {
        return 1;
    }
    
    /**
     * Another utility function to test grant/revoke - placeholder for
     * future if SPECIFIC names are implemented.
     * @return 1
     */
    public static int s2F1a()
    {
        return 1;
    }
    
    /**
     * Another utility function to test grant/revoke
     * @return 1
     */
    public static int s2F2()
    {
        return 1;
    }
    
    /**
     * Utility method to test procedure with identical name to a
     * function
     */
    public static void s1F1P( )
    {
    }
    
    /**
     * A utility method to test procedures with test grant/revoke
     */
    public static void s1P1( )
    {
    }
    
    /* 
     * public methods for asserting privileges begin here
     * May move to BaseJDBCTestCase if appropriate.
     */
    

    /**
     * Assert all privileges for a given user / schema / table / column set
     * 
     * @param hasPrivilege whether we expect the given user to have the privilege
     * @param user the user to check
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check for the user
     */
    public void assertAllPrivileges(boolean hasPrivilege, String user, String schema, String table, String[] columns) throws SQLException {
    	assertSelectPrivilege(hasPrivilege, user, schema, table, columns);
    	assertDeletePrivilege(hasPrivilege, user, schema, table);
    	assertInsertPrivilege(hasPrivilege, user, schema, table, columns);
    	assertUpdatePrivilege(hasPrivilege, user, schema, table, columns);
    	assertReferencesPrivilege(hasPrivilege, user, schema, table, columns);
       	assertTriggerPrivilege(hasPrivilege, user, schema, table);
    }
    
    
    /**
     * Assert that a user has select privilege on a given table / column
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check
     * @throws SQLException throws all exceptions
     */
    public void assertSelectPrivilege(boolean hasPrivilege, String user, String schema, String table, String[] columns) throws SQLException{
    	Connection c = openUserConnection(user);
    	
    	Statement s = c.createStatement();
    	try {
    	    boolean b = s.execute("select " + columnListAsString(columns) + " from " + schema + "." + table);
            assertTrue("expected no SELECT permission on table", hasPrivilege);
    	} catch (SQLException e) {
    		if (!hasPrivilege) {
    			assertSQLState("42502", e);
    		} else {
                printStackTrace(e);
                fail("Unexpected lack of select privilege", e);
    		}
    	}
        s.close();
    	c.close();
    	
    	assertPrivilegeMetadata(hasPrivilege, "SELECT", user, schema, table, columns);
    }
    
    /**
     * Assert that a user has delete privilege on a given table / column
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param schema the schema to check
     * @param table the table to check
     * @throws SQLException throws all exceptions
     */
    public void assertDeletePrivilege(boolean hasPrivilege, String user, String schema, String table) throws SQLException {
    	Connection c = openUserConnection(user);
    	
    	Statement s = c.createStatement();
    	try {
    	    boolean b = s.execute("delete from " + schema + "." + table);
            assertTrue("expected no DELETE permission on table", hasPrivilege);

    	} catch (SQLException e) {
    		if (!hasPrivilege) {
    			assertSQLState("42500", e);
    		} else {
                printStackTrace(e);
                fail("Unexpected lack of delete privilege", e);
    		}
    	}
    	s.close();
    	c.close();
    	
    	assertPrivilegeMetadata(hasPrivilege, "DELETE", user, schema, table, null);
    }
    
    /**
     * Assert that a user has insert privilege on a given table / column
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check
     * @throws SQLException throws all exceptions
     */
    public void assertInsertPrivilege(boolean hasPrivilege, String user, String schema, String table, String[] columns) throws SQLException {
   	
    	// NOTE - getColumns returns empty result set if schema / table names not capitalized.
        // TODO - should implement asserting insert privilege on a subset of columns at some point
    	
    	Connection c = openUserConnection(user);

    	Statement s = c.createStatement();
    	try {
    		StringBuffer command = new StringBuffer("insert into " + schema + "." + table + " values (");
    		ResultSet rs = c.getMetaData().getColumns( null, schema, table, null);
            boolean first = true;
            while(rs.next())
            {
                if(first)
                    first = false;
                else
                    command.append(",");
                appendColumnValue(command, rs.getInt(5));
            }
            rs.close();
            command.append(")");
    	    int i = s.executeUpdate(command.toString());
            assertTrue("expected no INSERT permission on table", hasPrivilege);

    	} catch (SQLException e) {
    		if (!hasPrivilege) {
    			assertSQLState("42500", e);
    		} else {
                fail("Unexpected lack of insert privilege on " +
                     JDBC.escape(schema, table) + " by " + user, e);
    		}
    	}
    	s.close();
    	c.close();
    	
    	assertPrivilegeMetadata(hasPrivilege, "INSERT", user, schema, table, columns);
    }
    
    /**
     * Assert that a user has update privilege on a given table / column
     * 
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check
     * @throws SQLException throws all exceptions
     */
    public void assertUpdatePrivilege(boolean hasPrivilege, String user, String schema, String table, String[] columns) throws SQLException {
 
    	String[] checkColumns = (columns == null) ? getAllColumns(schema, table) : columns;
    	Connection c = openUserConnection(user);
    	
    	Statement s = c.createStatement();
    	int columnCount = 0;
    	boolean checkCount;
    	for (int i = 0; i < checkColumns.length; i++) {
    		checkCount = false;
  
    		try {
    			// if possible, get count of rows to verify update rows
    			try {
    				ResultSet countRS = s.executeQuery("select count(" + checkColumns[i] +") from " + schema + "." + table);
    			    if (!countRS.next()) {
    				    fail("Could not get count on " + checkColumns[i] + " to verify update");
    			    }
    			    columnCount = countRS.getInt(1);
    			    checkCount = true;
    			} catch (SQLException e) {
                    // may not have select privilege on the column, in
    				// which case, we simply don't verify the count.
                    assertSQLState("42502", e);
    			}
    			
    			StringBuffer command = new StringBuffer("update " + schema + "." + table + " set " + checkColumns[i] + "=");
        		ResultSet rs = c.getMetaData().getColumns( (String) null, schema, table, checkColumns[i]);
                if (!rs.next())
                {
                	fail("Could not get column metadata for " + checkColumns[i]);
                }
                appendColumnValue(command, rs.getInt( 5));
                rs.close();
        	    int actualCount = s.executeUpdate(command.toString());
        	    if (hasPrivilege && checkCount)
        	    {
        	    	// update count should equal select count
        	        assertEquals(columnCount, actualCount);
        	    }

                assertTrue("expected no UPDATE permission on table",
                           hasPrivilege);

    		} catch (SQLException e) {
        		if (!hasPrivilege) {
        			assertSQLState("42502", e);
        		} else {
                    printStackTrace(e);
                    fail("Unexpected lack of privilege to update on " +
                         JDBC.escape(schema, table) + " by " + user, e);
        		}
        	}
        }

        s.close();
    	c.close();
    	
    	assertPrivilegeMetadata(hasPrivilege, "UPDATE", user, schema, table, columns);
    }
    
    /**
     * Assert that a user has references privilege on a given table / column
     * 
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check
     * @throws SQLException throws all exceptions
     */
    public void assertReferencesPrivilege(boolean hasPrivilege, String user, String schema, String table, String[] columns) throws SQLException {

    	assertPrivilegeMetadata(hasPrivilege, "REFERENCES", user, schema, table, columns);
    	
    	/* no way to empirically test any arbitrary column can be
    	 * referenced, as the column that is to be referenced must be
    	 * a primary key or a unique constraint. Leaving this here, as it
    	 * might form the useful basis of another assert method for cases
    	 * where we know this to be certain.
    	
    	Connection c = openUserConnection(user);
    	Statement s = c.createStatement();
        for (int i = 0; i < columns.length; i++) {
            // if it works, need to assert this as false.
        	boolean b = true;
        	try {
        	    if (columns == null) {
            	    b = s.execute("create table referencestest (c1 " + getColumnDataType(schema, table, columns[i]) + " references " + schema + "." + table + ")" );
        	    } else {
        	        b = s.execute("create table referencestest (c1 " + getColumnDataType(schema, table, columns[i]) + " references " + schema + "." + table + "(" + column + "))" );
        	    }
        	} catch (SQLException e) {
        		if (!hasPrivilege) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        			assertSQLState("42502", e);
        		} else {
                    printStackTrace(e);
                    fail("Unexpected lack of references privilege", e);
        		}
        	}
        	// no rows updated, so false.
        	assertFalse(b);
        	s.execute("drop table referencestest");   	
        }
        s.close();
        c.close();
        */
    }
    
    /**
     * Assert that a user has trigger execute privilege on a given table / column
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param schema the schema to check
     * @param table the table to check
     * @throws SQLException throws all exceptions
     */
    public void assertTriggerPrivilege(boolean hasPrivilege, String user, String schema, String table) throws SQLException {
    	
//IC see: https://issues.apache.org/jira/browse/DERBY-2893
    	Connection c = openUserConnection(user);
    	c.setAutoCommit(false);
    	
    	Statement s = c.createStatement();
    	try {
    	    int i = s.executeUpdate("create trigger \"" + table + "Trig\" after insert on " +
    	    		              schema + "." + table + " for each row values 1");
    	    if (hasPrivilege)
    	    {
    	        assertEquals(0, i); 
    	    }
            assertTrue("expected no TRIGGER permission on table", hasPrivilege);

    	} catch (SQLException e) {
    		if (!hasPrivilege) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
    			assertSQLState("42500", e);
    		} else {
                printStackTrace(e);
                fail("Unexpected lack of trigger privilege on " +
                     JDBC.escape(schema, table) + " by " + user, e);
    		}
    	}
    	
    	c.rollback();
        s.close();
    	c.close();

    	assertPrivilegeMetadata(hasPrivilege, "TRIGGER", user, schema, table, null);
 
    }
    
    /**
     * Assert that a user has function execute privilege on a given table / column
     * 
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param schema the schema to check
     * @param function the function to check
     * @param forProcedure true if checking for lack of function execute privilege against procedure of same name.
     * @throws SQLException throws all exceptions
     */
    public void assertFunctionPrivilege(boolean hasPrivilege, String user, String schema, String function, boolean forProcedure) throws SQLException {
        Connection c = openUserConnection(user);
        
        String functioncall = "values " + schema + "." + function + "()";
 
	    PreparedStatement ps = null;
	    ResultSet rs = null;
		try {
		    ps = c.prepareStatement(functioncall);
		    rs = ps.executeQuery();
            assertTrue("expected no EXECUTE permission on function",
                       hasPrivilege);

		} catch (SQLException e) {
			if (!hasPrivilege){
				if (forProcedure) 
					assertSQLState("42Y03", e);
				else 
					assertSQLState("42504", e);
			} else {
                printStackTrace(e);
                fail("Unexpected lack of function execute privilege", e);
			}
		}
		if (ps != null)
			ps.close();
		if (rs != null)
			rs.close();

        c.close();
    }
    
    /**
     * Assert that a user has procedure execute privilege on a given table / column
     * 
     * @param hasPrivilege whether or not the user has the privilege
     * @param user the user to check
     * @param schema the schema to check
     * @param procedure the name of the procedure to check
     * @throws SQLException throws all exceptions
     */
    public void assertProcedurePrivilege(boolean hasPrivilege, String user, String schema, String procedure) throws SQLException {
        Connection c = openUserConnection(user);
        
        String procedurecall = "call " + schema + "." + procedure + "()";
        
		CallableStatement ps = c.prepareCall(procedurecall);
		ResultSet rs = null;
		try {
			ps.execute();
			rs = ps.getResultSet();
            assertTrue("expected no EXECUTE permission on procedure",
                       hasPrivilege);
		} catch (SQLException e) {
			if (!hasPrivilege)
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
				assertSQLState("42504", e);
			else {
                printStackTrace(e);
                fail("Unexpected lack of procedure execute privilege", e);
			}
		}
		ps.close();
		if (rs != null)
		{
				rs.close();
		}
        c.close();
    }
    
    /**
     * Assert that a specific privilege exists by checking the
     * database metadata available to a user
     * 
     * @param hasPrivilege true if we expect the caller to have the privilege
     * @param type type of privilege, e.g. SELECT, INSERT, DELETE, etc.
     * @param user the user to check
     * @param schema the schema to check
     * @param table the table to check
     * @param columns the set of columns to check, or all columns if null
     * @throws SQLException
     */
    public void assertPrivilegeMetadata(boolean hasPrivilege, String type, String user, String schema, String table, String[] columns) throws SQLException {
        
    	Connection c = openUserConnection(user);
    	DatabaseMetaData dm = c.getMetaData();
        schema = JDBC.identifierToCNF(schema);
        table  = JDBC.identifierToCNF(table);
        ResultSet rs = dm.getTablePrivileges(null, schema, table);
     	boolean found = false;
    	// check getTablePrivileges
    	if (columns == null) {
        	while (rs.next())
        	{
        	// also verify that grantor and is_grantable can be obtained
        	// Derby doesn't currently support the for grant option, the
        	// grantor is always the object owner - in this test, TEST_DBO,
        	// and is_grantable is always 'NO'
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
          	    assertEquals(rs.getString(4),"TEST_DBO");
          	    assertEquals(rs.getString(7),"NO");
          	    if (rs.getString(6).equals(type)) {
        	    	String privUser = rs.getString(5);
        	    	if (privUser.equals(user) || privUser.equals("PUBLIC")) {
        	    		found = true;
        	    	}
        	    }
        	}
        	assertEquals(hasPrivilege, found);
        	rs.close();
    	}

    	// check getColumnPrivileges()
    	ResultSet cp = null;
    	if (columns == null) {
    		/*
    		 * Derby does not record table level privileges in SYSCOLPERMS,
    		 * so the following does not work. If it is ever changed so that
    		 * getColumnPrivileges returns proper results for table level privileges,
    		 * this can be reenabled.
    		 * 
        	ResultSet cols = dm.getColumns(null, schema.toUpperCase(), table.toUpperCase(), null);
			int foundCount = 0;
			int colCount = 0;
        	while (cols.next())
        	{
				colCount++;
    			String col = cols.getString(4);
    			//System.out.println("getting column privs for " + col);
    			cp = dm.getColumnPrivileges(null, schema.toUpperCase(), table.toUpperCase(), col);

    			while (cp.next()) {

					//System.out.println(schema + "." + table + ": "
					//		+ cp.getString(4) + ", " + cp.getString(5) + ", "
					//		+ cp.getString(6) + ", " + cp.getString(7));
					if (cp.getString(7).equals(type)) {
						String privUser = cp.getString(6);
						if (privUser.equals(user) || privUser.equals("PUBLIC")) {
							foundCount++;
						}
					}
				}
        	}
			if (hasPrivilege) {
				assertEquals(colCount, foundCount);
			} else {
			    assertFalse(colCount == foundCount);
			}
			*/
    	} else {
    		// or, check the given columns
    		for (int i = 0; i < columns.length; i++) {
    			cp = dm.getColumnPrivileges(null, schema.toUpperCase(), table.toUpperCase(), columns[i].toUpperCase());
    			found = false;
    			while (cp.next()) {
    			// also verify that grantor and is_grantable are valid
    			// Derby doesn't currently support for grant, so
    			// grantor is always the object owner - in this test, 
    			// TEST_DBO, and getColumnPrivileges casts 'NO' for 
    			// is_grantable for supported column-related privileges
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
					assertEquals("TEST_DBO", cp.getString(5));
					assertEquals("NO", cp.getString(8));
					if (cp.getString(7).equals(type)) {
						String privUser = cp.getString(6);
						if (privUser.equals(user) || privUser.equals("PUBLIC")) {
							found = true;
						}
					}
    			}
    			if (hasPrivilege)
    				assertTrue(found);
    		}        	
    	}
		if (cp != null)
			cp.close();
    	
    	c.close();
    }
    
    /* End assert methods */
    
    /* Begin helper methods */
    
    /**
     * Append a particular SQL datatype value to the given StringBuffer
     * 
     * @param sb the StringBuffer to append the value
     * @param type the java.sql.Types value to append
     */
    static void appendColumnValue(StringBuffer sb, int type)
    {
        switch(type)
        {
        case Types.BIGINT:
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.INTEGER:
        case Types.NUMERIC:
        case Types.REAL:
        case Types.SMALLINT:
        case Types.TINYINT:
            sb.append("0");
            break;

        case Types.CHAR:
        case Types.VARCHAR:
            sb.append("' '");
            break;

        case Types.DATE:
            sb.append("CURRENT_DATE");
            break;

        case Types.TIME:
            sb.append("CURRENT_TIME");
            break;

        case Types.TIMESTAMP:
            sb.append("CURRENT_TIMESTAMP");
            break;

        default:
            sb.append("null");
            break;
        }
    }
    
    /**
     * Return the given String array as a comma separated String
     * 
     * @param columns an array of columns to format
     * @return a comma separated String of the column names
     */
    static String columnListAsString(String[] columns) {
    	if (columns == null) {
    		return "*";
    	}
    	
    	StringBuffer sb = new StringBuffer(columns[0]);
		for (int i = 1; i < columns.length; i++ ) {
		    sb.append("," + columns[i]);
		}
		return sb.toString();
    }
    
    /**
     * Get all the columns in a given schema / table
     * 
     * @return an array of Strings with the column names
     * @throws SQLException
     */    
    String[] getAllColumns(String schema, String table) throws SQLException
    {
    	DatabaseMetaData dbmd = getConnection().getMetaData();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        ArrayList<String> columnList = new ArrayList<String>();
        ResultSet rs = dbmd.getColumns( (String) null, schema, table, (String) null);
        while(rs.next())
        {
            columnList.add(rs.getString(4));
        }
          
        return columnList.toArray(new String[columnList.size()]);
    }
    
    /**
     * Given a schema, table, and column as Strings, return the datatype of
     * the column as a String.
     * 
     * @param schema the schema for the table in which the column resides
     * @param table the table containing the column to check
     * @param column the column to get the data type as a String
     * @return the Type of the column as a String
     * @throws SQLException
     */
    String getColumnDataType(String schema, String table, String column) throws SQLException {
    	DatabaseMetaData dm = getConnection().getMetaData();
    	ResultSet rs = dm.getColumns(null, schema, table, column);
    	
    	int type = 0;
    	while (rs.next()) {
    		type = rs.getInt(5);
    	}
    	rs.close();
    	return JDBC.sqlNameFromJdbc(type);
    }
}
