/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.statementJdbc30

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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;
import org.apache.derby.iapi.reference.JDBC30Translation;

/**
 * Test of additional methods in JDBC3.0  methods in statement class.
 *
 */

public class statementJdbc30 { 

	public static void main(String[] args) {
		Connection con;
		ResultSet rs;
		Statement stmt;

		System.out.println("Test statementJdbc30 starting");

		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();


			stmt = con.createStatement();

			//create a table, insert a row, do a select from the table,
			stmt.execute("create table tab1 (i int, s smallint, r real)");
			stmt.executeUpdate("insert into tab1 values(1, 2, 3.1)");

			// read the data just for the heck of it
			rs = stmt.executeQuery("select * from tab1");
			rs.next();

      System.out.println("trying stmt.getMoreResults(int) :");
			stmt.getMoreResults(JDBC30Translation.CLOSE_CURRENT_RESULT);

			System.out.println("trying stmt.executeUpdate(String, int) :");
			stmt.executeUpdate("insert into tab1 values(2, 3, 4.1)", JDBC30Translation.NO_GENERATED_KEYS);

			System.out.println("trying stmt.executeUpdate(String, int[]) :");
			int[] columnIndexes = new int[2];
			columnIndexes[0] = 1;
			columnIndexes[1] = 2;
			try {
				stmt.executeUpdate("insert into tab1 values(2, 3, 4.1)", columnIndexes);
			} catch (SQLException ex) {
				dumpExpectedSQLExceptions(ex);
			}

			System.out.println("trying stmt.executeUpdate(String, String[]) :");
			String[] columnNames = new String[2];
			columnNames[0] = "I";
			columnNames[1] = "S";
			try {
				stmt.executeUpdate("insert into tab1 values(2, 3, 4.1)", columnNames);
			} catch (SQLException ex) {
				dumpExpectedSQLExceptions(ex);
			}

			System.out.println("trying stmt.execute(String, int) :");
			stmt.execute("select * from tab1", JDBC30Translation.NO_GENERATED_KEYS);

			System.out.println("trying stmt.execute(String, int[]) :");
			try {
				stmt.execute("insert into tab1 values(2, 3, 4.1)", columnIndexes);
			} catch (SQLException ex) {
				dumpExpectedSQLExceptions(ex);
			}

			System.out.println("trying stmt.execute(String, String[]) :");
			try {
				stmt.execute("insert into tab1 values(2, 3, 4.1)", columnNames);
			} catch (SQLException ex) {
				dumpExpectedSQLExceptions(ex);
			}

			System.out.println("trying stmt.getResultSetHoldability() :");
			stmt.getResultSetHoldability();

			System.out.println("trying stmt.getGeneratedKeys() :");
			stmt.getGeneratedKeys();

			rs.close();
			stmt.close();
			con.close();

		}
		catch (SQLException e) {
			System.out.println("Expected : " + e.getMessage());
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception: "+e);
			e.printStackTrace();
		}

		System.out.println("Test statementJdbc30 finished");
    }

	public static void dumpExpectedSQLExceptions (SQLException se) {
		System.out.println("PASS -- expected exception");
		while (se != null)
		{
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
        }
    }
}
