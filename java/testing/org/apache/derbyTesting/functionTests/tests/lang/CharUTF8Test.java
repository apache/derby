/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CharUTF8Test
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;

/**
 * Test all characters written through the UTF8 format.
 */
public class CharUTF8Test extends BaseJDBCTestCase {
	
	private PreparedStatement psSet;
	private PreparedStatement psGet;
	
	/**
	 * Basic constructor.
	 */	
	public CharUTF8Test(String name) {
		super(name);
	}
	
	/**
	 * Sets the auto commit to false.
	 */
	protected void initializeConnection(Connection conn) throws SQLException {
		conn.setAutoCommit(false);
	}
	
	/**
	 * Returns the implemented tests.
	 * 
	 * @return An instance of <code>Test</code> with the implemented tests to
	 *         run.
	 */
	public static Test suite() {
		return new CleanDatabaseTestSetup(new TestSuite(CharUTF8Test.class)) {
			protected void decorateSQL(Statement stmt) throws SQLException {
				stmt.execute("CREATE TABLE TEST(id int not null primary key, body varchar(60))");
			}
		};
	}

	protected void setUp() throws Exception {
		super.setUp();
		psSet = prepareStatement("insert into test values(?,?)");
		psGet = prepareStatement("select body from test where id=?");
	}
	
	protected void tearDown() throws Exception {
        // Forget the statements to allow them to be gc'ed. They will be
        // closed in super.tearDown().
        psSet = null;
        psGet = null;
		super.tearDown();
	}
	
	/**
	 * Tests the correct handling of UTF8 char sequence. 
	 * 
	 * This test iteratively writes on a test table a sequence of 60 UTF8 chars; next, in the same iteration, it 
	 * reads the string and checks if the written sequence is correct.
	 * 
	 * @throws SQLException
	 */
	public void testUTF8() throws SQLException {
		for (int i = Character.MIN_VALUE; i <= Character.MAX_VALUE; i++) {
			StringBuffer buff = new StringBuffer();
			buff.append((char) i);

			if ((buff.length() == 60) || (i == Character.MAX_VALUE)) {
				String text = buff.toString();
				//System.out.println("Testing with last char value " + i + " length=" + text.length());
				
				setBody(i, text); // set the text
				
				String res = getBody(i); // now read the text
				assertEquals("Fetched string is incorrect (length = " + buff.length() + ")", text, res);

				buff.setLength(0);
			}
		}		
	}
	
	/**
	 * Checks if an empty UTF8 string is correctly handled.
	 * 
	 * @throws SQLException
	 */
	public void testEmptyStringUTF8() throws SQLException {
		setBody(-1, "");
		assertEquals("Empty string incorrect!", "", getBody(-1));
	}
	
	private void setBody(int key, String body) throws SQLException {       
		psSet.setInt(1, key);
		psSet.setString(2, body);
		psSet.executeUpdate();
	}
        
    private String getBody(int key) throws SQLException {
		String result = "NO RESULT";

		psGet.setInt(1, key);
		ResultSet rs = psGet.executeQuery();
		if (rs.next()) {
			result = rs.getString(1);
		}
		rs.close();
		return result;
	}
}
