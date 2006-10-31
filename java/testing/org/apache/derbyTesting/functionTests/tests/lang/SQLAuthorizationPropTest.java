/*
*
* Derby - Class org.apache.derbyTesting.functionTests.lang.SQLAuthorizationPropTest
*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, 
* software distributed under the License is distributed on an 
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
* either express or implied. See the License for the specific 
* language governing permissions and limitations under the License.
*/

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;

public class SQLAuthorizationPropTest extends BaseJDBCTestCase {

	public SQLAuthorizationPropTest(String name) {
		super(name);
	}

	public static Test suite() {
		TestSuite suite = new TestSuite(SQLAuthorizationPropTest.class,
                "SQLAuthorizationPropTest");
		
		// Use DatabasePropertyTestSetup decorator to set the property
		// required by this test. 
		Properties props = new Properties();
	    props.setProperty("derby.database.sqlAuthorization", "true");
	    Test test = new SQLAuthorizationPropTest("grantRevokeAfterSettingSQLAuthProperty");
	    suite.addTest(new DatabasePropertyTestSetup (test, props));
	    
	    // This test has to be run after SQL authorization property has been 
	    // set to true. 
	    suite.addTest(new SQLAuthorizationPropTest("resetSQLAuthProperty"));
	    
        // This test needs to run in a new single use database as upon entry
        // the test expects SQL authorization to be off and then sets it
        // which cannot be undone.
	    return TestConfiguration.singleUseDatabaseDecorator(suite);
	}
	
    /**
     * Create a table to test grant/revoke statements
     */
    protected void setUp() throws SQLException {
        Statement stmt = createStatement();
        stmt.execute("create table GR_TAB (id int)");
        stmt.close();
    }

    /**
     * Drop the table created during setup.
     * @throws Exception 
     */
    protected void tearDown()
        throws Exception {
        Statement stmt = createStatement();
        stmt.execute("drop table GR_TAB");
        stmt.close();
        super.tearDown();
    }
    
	/**
	 * This method tests that grant/revoke is not available if 
	 * derby.database.sqlAuthorization property is not set.
	 * 
	 * @throws SQLException
	 */
	public void testGrantRevokeWithoutSQLAuthProperty() throws SQLException{
		Statement stmt = createStatement();
		
    	try {
    		stmt.execute("grant select on GR_TAB to some_user");
    		fail("FAIL: Grant statement should have failed when SQL authorization is not set");
    	} catch(SQLException sqle) {
    		assertSQLState(SQLStateConstants.LANG_GRANT_REVOKE_WITH_LEGACY_ACCESS, sqle);
    	}
    	
    	try {
    		stmt.execute("revoke select on GR_TAB from some_user");
    		fail("FAIL: Revoke statement should have failed when SQL authorization is not set");
    	} catch(SQLException sqle) {
    		assertSQLState(SQLStateConstants.LANG_GRANT_REVOKE_WITH_LEGACY_ACCESS, sqle);
    	}
    	stmt.close();
	}
	
	/**
	 * This method tests that grant/revoke is available 
	 * once derby.database.sqlAuthorization property is set to true.
	 * 
	 * @throws SQLException
	 */
	public void grantRevokeAfterSettingSQLAuthProperty() throws SQLException{
		// Shutdown the database for derby.database.sqlAuthorization property 
		// to take effect. This was set by DatabasePropertyTestSetup decorator.
		try{ 
			getDefaultConnection("shutdown=true");
			fail("FAIL: Failed to shutdown database");
		} catch (SQLException sqle) {
			assertSQLState(SQLStateConstants.SHUTDOWN_DATABASE, sqle);
		}
		
		Statement stmt = createStatement();
		stmt.execute("grant select on GR_TAB to some_user");
    	stmt.execute("revoke select on GR_TAB from some_user");
    	stmt.close();
	}
	
	/**
	 * This method tests that once derby.database.sqlAuthorization property 
	 * has been set to true, it cannot be reset to any other value. For the 
	 * test to be valid, it must follow the test method which sets 
	 * derby.database.sqlAuthorization property to true.
	 * 
	 * @throws SQLException
	 */
	public void resetSQLAuthProperty() throws SQLException {
		Connection conn = getConnection();
        conn.setAutoCommit(false);
        
        CallableStatement setDBP =  conn.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        setDBP.setString(1, "derby.database.sqlAuthorization");
        // Resetting to any value other than true should fail
        testPropertyReset(setDBP, "false");
        testPropertyReset(setDBP, null);
        testPropertyReset(setDBP, "some_value");
        // This should work
        testPropertyReset(setDBP, "true");
        
        setDBP.close();
	}
	
	/**
	 * This method executes a callable statement to set the database property
	 * to a given value. It checks that reset to any value other than "true" 
	 * fails.
	 * 
	 * @param cs CallableStatement object used to set database property
	 * @param value value of database property
	 * @throws SQLException
	 */
	private void testPropertyReset(CallableStatement cs, String value) throws SQLException {

		cs.setString(2, value);
        
		try {
        	cs.executeUpdate();
        	if(value.compareToIgnoreCase("true") != 0)
        		fail("FAIL: Should not be possible to reset sql authorization once it has been turned on");
        } catch (SQLException sqle) {
        	assertSQLState(SQLStateConstants.PROPERTY_UNSUPPORTED_CHANGE, sqle);
        }
        
	}
}
