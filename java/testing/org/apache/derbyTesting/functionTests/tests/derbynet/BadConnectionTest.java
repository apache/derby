/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.BadConnectionTest

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
package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.sql.*;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 *	This tests some bad attempts at a client connection:
 *		- non-existant database
 *		- lack of user / password attributes
 *		- bad values for valid connection attributes
 */

public class BadConnectionTest extends BaseJDBCTestCase
{
	
	public void setUp() throws SQLException
	{
		// get the default connection so the driver is loaded.
		Connection c = getConnection();
		c.close();
	}
	
	/**
	 * Try to connect without a username or password.
	 * Should fail with SQLState 08004.
	 */
	public void testNoUserOrPassword()
	{
		try {
			Connection c = DriverManager.getConnection("jdbc:derby://localhost:1527/testbase");
		} catch (SQLException e) {
			assertSQLState("08004", e);
			assertEquals(-4499, e.getErrorCode());
		}
	}
	
	/**
	 * Try to connect to a non-existent database without create=true.
	 * Should fail with SQLState 08004.
	 */
	public void testDatabaseNotFound()
	{
		try {
			Properties p = new Properties();
			p.put("user", "admin");
			p.put("password", "admin");
			Connection c = DriverManager.getConnection("jdbc:derby://localhost:1527/testbase", p);
		} catch (SQLException e)
		{
			assertSQLState("08004", e);
			assertEquals(-4499, e.getErrorCode());
		}
	}
	
	/**
	 * Check that only valid values for connection attributes are accepted.
	 * For this check, we attempt to connect using the upgrade attribute
	 * with an invalid value.
	 * 
     * Should fail with SQLState XJ05B.
	 */
	public void testBadConnectionAttribute()
	{
		try {
			Connection c = DriverManager.getConnection("jdbc:derby://localhost:1527/badAttribute;upgrade=notValidValue");
		} catch (SQLException e)
		{
			assertSQLState("XJ05B", e);
			assertEquals(-1, e.getErrorCode());
		}
	}

	public BadConnectionTest(String name)
	{
		super(name);
	}
	
	public static Test suite()
	{
		return TestConfiguration.clientServerDecorator(
			new TestSuite(BadConnectionTest.class, "BadConnection"));
	}
}
