/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.nullSQLText

   Copyright 2001, 2005 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.JDBCTestDisplayUtil;

/**
 * Test of null strings in prepareStatement and execute 
 * result set
 *
 * @author peachey
 */

public class nullSQLText { 
	public static void main(String[] args) {
		Connection con;
		PreparedStatement  ps;
		Statement s;
		String nullString = null;
	
		System.out.println("Test nullSQLText starting");
    
		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();
			con.setAutoCommit(true); // make sure it is true
			s = con.createStatement();

			try
			{
				// test null String in prepared statement
				System.out.println("Test prepareStatement with null argument");
				ps = con.prepareStatement(nullString);
			}
			catch (SQLException e) {
				System.out.println("FAIL -- expected exception");
				dumpSQLExceptions(e);
			}
			try
			{
				// test null String in execute statement
				System.out.println("Test execute with null argument");
				s.execute(nullString);
			}
			catch (SQLException e) {
				System.out.println("FAIL -- expected exception");
				dumpSQLExceptions(e);
			}
			try
			{
				// test null String in execute query statement
				System.out.println("Test executeQuery with null argument");
				s.executeQuery(nullString);
			}
			catch (SQLException e) {
				System.out.println("FAIL -- expected exception");
				dumpSQLExceptions(e);
			}
			try
			{
				// test null String in execute update statement
				System.out.println("Test executeUpdate with null argument");
				s.executeUpdate(nullString);
			}
			catch (SQLException e) {
				System.out.println("FAIL -- expected exception");
				dumpSQLExceptions(e);
			}
			con.close();
		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
			e.printStackTrace(System.out);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}
		
		System.out.println("Test nullSQLText finished");
    }
	static private void dumpSQLExceptions (SQLException se) {
		while (se != null) {
            JDBCTestDisplayUtil.ShowCommonSQLException(System.out, se);			
	         se = se.getNextException();
		}
	}

}
