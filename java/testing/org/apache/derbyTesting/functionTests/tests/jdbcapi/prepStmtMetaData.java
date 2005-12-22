/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.prepStmtMetaData

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
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.tools.ij;

/**
 * Test of prepared statement getMetaData for statements that don't produce a
 * result set
 *
 * @author peachey
 */

public class prepStmtMetaData { 
    
	public static void main(String[] args) {
		ResultSetMetaData met;
		Connection con;
		PreparedStatement  ps;
		System.out.println("Test prepStmtMetaData starting");
    
		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();
			con.setAutoCommit(true); // make sure it is true
			
			// test create table - meta data should be null
			testMetaData(con, "create table ab(a int)", true);

			// test alter table - meta data should be null
			testMetaData(con, "alter table ab add column b int", true);

			// for test call statement 
			Statement s = con.createStatement();	
			s.execute("create procedure testproc() language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.prepStmtMetaData.tstmeth'"+
				" parameter style java");

			// test call statement - shouldn't have meta data
			testMetaData(con, "call testproc()", false);
			
			// test drop procedure - meta data should be null
			testMetaData(con, "drop procedure testproc", true);

			// test create schema - meta data should be null
			testMetaData(con, "create schema myschema", true);

			// test drop schema - meta data should be null
			testMetaData(con, "drop schema myschema restrict", true);

			// test create trigger - meta data should be null
			//testMetaData(con, "create trigger mytrig after insert on ab for each row mode db2sql create table yy(a int)", true);

			// test drop trigger - meta data should be null
			//testMetaData(con, "drop trigger mytrig", true);

			// test create view - meta data should be null
			testMetaData(con, "create view myview as select * from ab", true);

			// test drop view - meta data should be null
			testMetaData(con, "drop view myview", true);

			// test drop table - meta data should be null
			testMetaData(con, "drop table ab", false);

			// test create index - meta data should be null
			testMetaData(con, "create index aindex on ab(a)", true);

			// test drop index - meta data should be null
			testMetaData(con, "drop index aindex", false);

			// test insert - meta data should be null
			testMetaData(con, "insert into ab values(1,1)", true);

			// test update - meta data should be null
			testMetaData(con, "update ab set a = 2", false);

			// test delete - meta data should be null
			testMetaData(con, "delete from ab", false);

			// test select - meta data should be provided
			ps = con.prepareStatement("select * from ab");
			met = ps.getMetaData();
			System.out.println("Result meta data for select");
			dumpRSMetaData(met);
			ps.close();

			// test set 
			testMetaData(con, "set schema rock", false);

			// bug 4579 : PreparedStatement.getMetaData should not return stale data 
			executeStmt(con,"create table bug4579 (c11 int)");
			executeStmt(con,"insert into bug4579 values 1");
			testMetaData(con, "set schema rick", false);
			ps = con.prepareStatement("select * from bug4579");
			met = ps.getMetaData();
			System.out.println("bug 4579 and 5338 : Result meta data for select *");
			dumpRSMetaData(met);
			executeStmt(con,"alter table bug4579 add column c12 int");
			met = ps.getMetaData();
			System.out.println("bug 4579 and 5338 : Result meta data for select * after alter table but w/o execute query");
			dumpRSMetaData(met);
			executeStmt(con,"alter table bug4579 add column c13 int");
			ps.execute();
			met = ps.getMetaData();
			System.out.println("bug 4579 and 5338 : Result meta data for select * after alter table and execute query");
			dumpRSMetaData(met);
			ps.close();

			// clean up
			executeStmt(con,"drop table ab");
			executeStmt(con,"drop table bug4579");

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

		System.out.println("Test prepStmtMetaData finished");
    }
	static private void testMetaData(Connection con, String statement, boolean execute) {
		try {
			PreparedStatement ps = con.prepareStatement(statement);
			ResultSetMetaData met = ps.getMetaData();
			dumpRSMetaData(met);
			if (execute)
				ps.execute();
			ps.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}
	}
	static private void executeStmt(Connection con, String statement) {
		try {
			PreparedStatement ps = con.prepareStatement(statement);
			ps.execute();
			ps.close();
		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}
	}
	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se.printStackTrace(System.out);
			se = se.getNextException();
		}
	}
	static void dumpRSMetaData(ResultSetMetaData rsmd) throws SQLException {
		if (rsmd == null || rsmd.getColumnCount() == 0)
		{
			System.out.println("ResultSetMetaData is Null or empty");
			return;
		}

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount ();

		if (numCols <= 0) {
			System.out.println("(no columns!)");
			return;
		}
		
		// Display column headings
		for (int i=1; i<=numCols; i++) {
			if (i > 1) System.out.print(",");
			System.out.print(rsmd.getColumnLabel(i));
		}
		System.out.println();
	}

	public static void tstmeth()
	{
		// for purpose of test, method may do nothing
	}

}
