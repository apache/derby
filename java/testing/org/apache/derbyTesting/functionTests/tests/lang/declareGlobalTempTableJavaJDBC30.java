/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.declareGlobalTempTableJavaJDBC30

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Test for declared global temporary tables (introduced in Cloudscape 5.2) and pooled connection close and jdbc 3.0 specific features
 * The jdbc3.0 specific featuers are holdable cursors, savepoints.
 * The rest of the temp table test are in declareGlobalTempTableJava class. The reason for a different test
 * class is that the holdability and savepoint support is under jdk14 and higher. But we want to be able to run the non-holdable
 * tests under all the jdks we support and hence splitting the tests into two separate tests. Also, the reason for pooled connection close
 * is because DRDA doesn't yet have support for pooled connection and hence can't pull this test into other temp table test which runs under
 * both DRDA and plain Cloudscape.
 */


public class declareGlobalTempTableJavaJDBC30 {

	static private boolean isDerbyNet = false;

	/*
	** There is a small description prior to each sub-test describing what is being tested.
	*/
	public static void main(String[] args) {
		boolean		passed = true;

		Connection con = null;
		Statement  s = null;

		/* Run all parts of this test, and catch any exceptions */
		try {
			System.out.println("Test declaredGlobalTempTableJava starting");

			/* Load the JDBC Driver class */
			// use the ij utility to read the property file and
			// make the initial connection.
      
			ij.getPropertyArg(args);
			con = ij.startJBMS();
			String framework = System.getProperty("framework");
			if (framework != null && framework.toUpperCase().equals("DERBYNET"))
				isDerbyNet = true;

			con.setAutoCommit(false);
			s = con.createStatement();

			/* Test temp tables with holdable cursors and with ON COMMIT DELETE ROWS and ON COMMIT PRESERVE ROWS */
			/* Test temp tables rollback behavior in combination with savepoints */
			passed = testHoldableCursorsAndSavepoints(con, s) && passed;

			/* Test pooled connection close behavior */
			passed = testPooledConnectionClose() && passed;

			con.close();

		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception "+e);
			JDBCDisplayUtil.ShowException(System.out, e);
			e.printStackTrace();
			passed = false;
		}

		if (passed)
			System.out.println("PASS");

		System.out.println("Test declaredGlobalTempTable finished");
	}

	/**
	 * Test temp tables with holdable cursors and with ON COMMIT DELETE ROWS and ON COMMIT PRESERVE ROWS
	 * Test temp tables rollback behavior in combination with savepoints
	 *
	 * @param conn	The Connection
	 * @param s		A Statement on the Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean testHoldableCursorsAndSavepoints(Connection con, Statement s)
					throws SQLException {
		boolean passed = true;

		try
		{
			System.out.println("TEST1 : Test declared temporary table with ON COMMIT DELETE ROWS and holdable cursors");
                     
			System.out.println("Temp table t1 with held open cursors on it. Data should be preserved in t1 at commit time");
      //create a statement with hold cursors over commit
      Statement s1 = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
      ResultSet.HOLD_CURSORS_OVER_COMMIT );
			s1.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit delete rows not logged");
			s1.executeUpdate("insert into session.t1 values(11, 1)");
			s1.executeUpdate("insert into session.t1 values(12, 2)");
			ResultSet rs1 = s1.executeQuery("select count(*) from SESSION.t1"); //should return count of 2
      dumpRS(rs1);

			rs1 = s1.executeQuery("select * from SESSION.t1"); //hold cursor open on t1. Commit should preserve the rows
			rs1.next();

			System.out.println("Temp tables t2 & t3 with one held open cursor on them together. Data should be preserved in t2 & t3 at commit time");
      Statement s2 = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
      ResultSet.HOLD_CURSORS_OVER_COMMIT );
			s2.executeUpdate("declare global temporary table SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
			s2.executeUpdate("insert into session.t2 values(21, 1)");
			s2.executeUpdate("insert into session.t2 values(22, 2)");
			ResultSet rs23 = s2.executeQuery("select count(*) from SESSION.t2"); //should return count of 2
      dumpRS(rs23);

			s2.executeUpdate("declare global temporary table SESSION.t3(c31 int, c32 int) on commit delete rows not logged");
			s2.executeUpdate("insert into session.t3 values(31, 1)");
			s2.executeUpdate("insert into session.t3 values(32, 2)");
			rs23 = s2.executeQuery("select count(*) from SESSION.t3"); //should return count of 2
      dumpRS(rs23);

			rs23 = s2.executeQuery("select * from SESSION.t2, SESSION.t3 where c22=c32"); //hold cursor open on t2 & t3. Commit should preseve the rows
			rs23.next(); 

			System.out.println("Temp table t4 with one held cursor but it is closed before commit. Data should be deleted from t4 at commit time");
      Statement s3 = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
      ResultSet.HOLD_CURSORS_OVER_COMMIT );
			s3.executeUpdate("declare global temporary table SESSION.t4(c41 int, c42 int) on commit delete rows not logged");
			s3.executeUpdate("insert into session.t4 values(41, 1)");
			s3.executeUpdate("insert into session.t4 values(42, 2)");
			ResultSet rs4 = s3.executeQuery("select count(*) from SESSION.t4"); //should return count of 2
      dumpRS(rs4);

			rs4 = s3.executeQuery("select * from SESSION.t4"); //hold cursor open on t4 but close it before commit.
			rs4.next();
			rs4.close();

			con.commit();

			System.out.println("After commit, verify all the 4 tables");

			System.out.println("Temp table t1 will have the data intact after commit");
			rs1 = s1.executeQuery("select count(*) from SESSION.t1"); //should return count of 2
      dumpRS(rs1);

			System.out.println("Temp table t2 will have the data intact after commit");
			rs23 = s2.executeQuery("select count(*) from SESSION.t2"); //should return count of 2
      dumpRS(rs23);

			System.out.println("Temp table t3 will have the data intact after commit");
			rs23 = s2.executeQuery("select count(*) from SESSION.t3"); //should return count of 2
      dumpRS(rs23);

			System.out.println("Temp table t4 will have no data after commit");
			rs4 = s3.executeQuery("select count(*) from SESSION.t4"); //should return count of 0
      dumpRS(rs4);

			s.executeUpdate("drop table SESSION.t1");
			s.executeUpdate("drop table SESSION.t2");
			s.executeUpdate("drop table SESSION.t3");   
			s.executeUpdate("drop table SESSION.t4");

			con.commit();
			System.out.println("TEST1 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			e.printStackTrace(System.out);
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST1 FAILED");
		}

		try
		{
			System.out.println("TEST1a : Test declared temporary table with ON COMMIT DELETE ROWS and holdable cursors on prepared statement");

			System.out.println("Temp table t1 with held open cursors on it. Data should be preserved in t1 at commit time");
			Statement s1 = con.createStatement();
			s1.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit delete rows not logged");
			s1.executeUpdate("insert into session.t1 values(11, 1)");
			s1.executeUpdate("insert into session.t1 values(12, 2)");
      
			//create a prepared statement with hold cursors over commit
			PreparedStatement ps1 = con.prepareStatement("select count(*) from SESSION.t1",
			ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT );
			ResultSet rs1 = ps1.executeQuery(); //should return count of 2
			dumpRS(rs1);

			PreparedStatement ps2 = con.prepareStatement("select * from SESSION.t1",
			ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT );
			ResultSet rs11 = ps2.executeQuery(); //hold cursor open on t1. Commit should preserve the rows
			rs11.next(); //notice that we didn't close rs11 with hold cursor on commit

			System.out.println("Temp table t2 with one held cursor but it is closed before commit. Data should be deleted from t2 at commit time");
			s1.executeUpdate("declare global temporary table SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
			s1.executeUpdate("insert into session.t2 values(21, 1)");
			s1.executeUpdate("insert into session.t2 values(22, 2)");
      
			//create a prepared statement with hold cursors over commit
			PreparedStatement ps3 = con.prepareStatement("select count(*) from SESSION.t2",
			ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT );
			ResultSet rs2 = ps3.executeQuery(); //should return count of 2
			dumpRS(rs2);

			PreparedStatement ps4 = con.prepareStatement("select * from SESSION.t2",
			ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT );
			rs2 = ps4.executeQuery(); //hold cursor open on t2 but close it before commit.
			rs2.next();
			rs2.close();

			con.commit();

			System.out.println("After commit, verify both the tables");

			System.out.println("Temp table t1 will have the data intact after commit");
			rs1 = s1.executeQuery("select count(*) from SESSION.t1"); //should return count of 2
			dumpRS(rs1);
			//Need to close the held cursor on t1 before t1 can be dropped
			rs11.close();

			System.out.println("Temp table t2 will have no data after commit");
			rs2 = s1.executeQuery("select count(*) from SESSION.t2"); //should return count of 0
			dumpRS(rs2);

			s.executeUpdate("drop table SESSION.t1");
			s.executeUpdate("drop table SESSION.t2");

			con.commit();
			System.out.println("TEST1a PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST1a FAILED");
		}

		try
		{
			System.out.println("TEST2 : Declare a temporary table with ON COMMIT PRESERVE ROWS and various combinations of holdability");

			System.out.println("Temp table t1 with held open cursors on it. Data should be preserved, holdability shouldn't matter");
      //create a statement with hold cursors over commit
      Statement s1 = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
      ResultSet.HOLD_CURSORS_OVER_COMMIT );
			s1.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");
			s1.executeUpdate("insert into session.t1 values(11, 1)");
			s1.executeUpdate("insert into session.t1 values(12, 2)");
			ResultSet rs1 = s1.executeQuery("select count(*) from SESSION.t1"); //should return count of 2
      dumpRS(rs1);

			rs1 = s1.executeQuery("select * from SESSION.t1"); //hold cursor open on t1.
			rs1.next();

			con.commit();

			System.out.println("After commit, verify the table");

			System.out.println("Temp table t1 will have data after commit");
			rs1 = s1.executeQuery("select count(*) from SESSION.t1"); //should return count of 2
      dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			con.commit();
			System.out.println("TEST2 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST2 FAILED");
		}

		try
		{
			System.out.println("TEST3A : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  Create savepoint1 and declare temp table t1");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");
			PreparedStatement pStmt = con.prepareStatement("insert into SESSION.t1 values (?, ?)");
      pStmt.setInt(1, 11);
      pStmt.setInt(2, 1);
      pStmt.execute();
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			System.out.println("  Create savepoint 2, drop temp table t1, rollback savepoint 2");
			Savepoint savepoint2 = con.setSavepoint();
			s.executeUpdate("drop table SESSION.t1");
			try {
				rs1 = s.executeQuery("select * from SESSION.t1");
			} catch (Throwable e)
			{
				System.out.println("Expected message: "+ e.getMessage());
			}
      con.rollback(savepoint2);

			System.out.println("  select should pass, rollback savepoint 1, select should fail");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
      con.rollback(savepoint1);
			rs1 = s.executeQuery("select * from SESSION.t1");

			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con.commit();
			System.out.println("TEST3A PASSED");
		}

		try
		{
			System.out.println("TEST3B : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  Create savepoint1 and declare temp table t1");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");

			System.out.println("  Create savepoint2 and declare temp table t2");
			Savepoint savepoint2 = con.setSavepoint();
			s.executeUpdate("declare global temporary table SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");

			System.out.println("  Release savepoint 1 and select from temp table t1 & t2");
      con.releaseSavepoint(savepoint1);
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Drop temp table t2(explicit drop), rollback transaction(implicit drop of t1)");
			s.executeUpdate("drop table SESSION.t2");
      con.rollback();

			System.out.println("  Select from temp table t1 and t2 will fail");
			try {
				rs1 = s.executeQuery("select * from SESSION.t1");
			} catch (Throwable e)
			{
				System.out.println("Expected message: "+ e.getMessage());
			}
			try {
				rs1 = s.executeQuery("select * from SESSION.t2");
			} catch (Throwable e)
			{
				System.out.println("Expected message: "+ e.getMessage());
			}
			con.commit();
			System.out.println("TEST3B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
      con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3B FAILED");
		}

		try
		{
			System.out.println("TEST3C : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  Create savepoint1 and declare temp table t1");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");

			System.out.println("  Create savepoint2 and declare temp table t2");
			Savepoint savepoint2 = con.setSavepoint();
			s.executeUpdate("declare global temporary table SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");

			System.out.println("  Release savepoint 1 and select from temp table t1 and t2");
      con.releaseSavepoint(savepoint1);
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Create savepoint3 and rollback savepoint3(should not touch t1 and t2)");
			Savepoint savepoint3 = con.setSavepoint();
      con.rollback(savepoint3);

			System.out.println("  select from temp tables t1 and t2 should pass");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Rollback transaction and select from temp tables t1 and t2 should fail");
      con.rollback();
			try {
				rs1 = s.executeQuery("select * from SESSION.t1");
			} catch (Throwable e)
			{
				System.out.println("Expected message: "+ e.getMessage());
			}
			try {
				rs1 = s.executeQuery("select * from SESSION.t2");
			} catch (Throwable e)
			{
				System.out.println("Expected message: "+ e.getMessage());
			}

			con.commit();
			System.out.println("TEST3C PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
      con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3C FAILED");
		}

		try
		{
			System.out.println("TEST3D : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  Create savepoint1 and declare temp table t1");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");

			System.out.println("  Create savepoint2 and drop temp table t1");
			Savepoint savepoint2 = con.setSavepoint();
			s.executeUpdate("drop table SESSION.t1");

			System.out.println("  Rollback savepoint2 and select temp table t1");
      con.rollback(savepoint2);
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			System.out.println(" Commit transaction and select temp table t1");
			con.commit();
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			con.commit();
			System.out.println("TEST3D PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3D FAILED");
		}

		try
		{
			System.out.println("TEST3E : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  Create savepoint1 and declare temp table t1");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");

			System.out.println("  Create savepoint2 and drop temp table t1");
			Savepoint savepoint2 = con.setSavepoint();
			s.executeUpdate("drop table SESSION.t1");

			System.out.println("  Rollback savepoint 1 and select from temp table t1 should fail");
      con.rollback(savepoint1);
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");

			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3E FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con.commit();
			System.out.println("TEST3E PASSED");
		}

		try
		{
			System.out.println("TEST3F : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 and drop temp table t1");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			s.executeUpdate("drop table SESSION.t1");
			System.out.println("  rollback, select on t1 should fail");
			con.rollback();
			rs1 = s.executeQuery("select * from SESSION.t1");

			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3F FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con.commit();
			System.out.println("TEST3F PASSED");
		}

		try
		{
			System.out.println("TEST3G : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 and commit");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			con.commit();
			System.out.println(" In the transaction:");
			System.out.println("  drop temp table t1 and rollback, select on t1 should pass");
			s.executeUpdate("drop table SESSION.t1");
			con.rollback();
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			con.commit();
			System.out.println("TEST3G PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3G FAILED");
		}

		try
		{
			System.out.println("TEST3H : Savepoint and commit behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 and commit");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			con.commit();
			System.out.println(" In the transaction:");
			System.out.println("  drop temp table t1 and commit, select on t1 should fail");
			s.executeUpdate("drop table SESSION.t1");
			con.commit();
			rs1 = s.executeQuery("select * from SESSION.t1");

			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3H FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con.commit();
			System.out.println("TEST3H PASSED");
		}

		try
		{
			System.out.println("TEST3I : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 and rollback");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			con.rollback();
			rs1 = s.executeQuery("select * from SESSION.t1");

			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3I FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con.commit();
			System.out.println("TEST3I PASSED");
		}

		try
		{
			System.out.println("TEST3J : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 with 2 columns and commit");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");
			s.executeUpdate("insert into SESSION.t1 values(11, 11)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			con.commit();
			System.out.println("  Create savepoint1 and drop temp table t1 with 2 columns");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("drop table SESSION.t1");
			System.out.println("  declare temp table t1 but this time with 3 columns");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int, c13 int not null) on commit preserve rows not logged");
			s.executeUpdate("insert into SESSION.t1 values(22, 22, 22)");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			System.out.println("  Create savepoint2 and drop temp table t1 with 3 columns");
			Savepoint savepoint2 = con.setSavepoint();
			s.executeUpdate("drop table SESSION.t1");
			con.rollback();
			System.out.println("  select from temp table t1 here should have 2 columns");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			s.executeUpdate("drop table SESSION.t1");

			con.commit();
			System.out.println("TEST3J PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3J FAILED");
		}

		try
		{
			System.out.println("TEST3K : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 & t2, insert few rows and commit");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");
			s.executeUpdate("insert into SESSION.t1 values(11, 1)");
			s.executeUpdate("insert into session.t2 values(21, 1)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			con.commit();

			System.out.println(" In the next transaction, insert couple more rows in t1 & t2 and ");
			s.executeUpdate("insert into SESSION.t1 values(12, 2)");
			s.executeUpdate("insert into SESSION.t2 values(22, 2)");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Create savepoint1 and update some rows in t1 and inspect the data");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("UPDATE SESSION.t1 SET c12 = 3 where c12>1");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			System.out.println("  update t2 with where clause such that no rows get modified in t2 and inspect the data");
			s.executeUpdate("UPDATE SESSION.t2 SET c22 = 3 where c22>2");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Rollback to savepoint1 and we should loose all the rows in t1");
			con.rollback(savepoint1);
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			System.out.println("  temp table t2 should also have no rows because attempt was made to modify it (even though nothing actually got modified in t2 in the savepoint)");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Commit the transaction and should see no data in t1 and t2");
			con.commit();
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			s.executeUpdate("drop table SESSION.t2");
			con.commit();
			System.out.println("TEST3K PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3K FAILED");
		}

		try
		{
			System.out.println("TEST3L : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 & t2, insert few rows and commit");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("insert into SESSION.t1 values(11, 1)");
			s.executeUpdate("insert into session.t2 values(21, 1)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			con.commit();

			System.out.println(" In the next transaction, insert couple more rows in t1 & t2 and ");
			s.executeUpdate("insert into SESSION.t1 values(12, 2)");
			s.executeUpdate("insert into session.t2 values(22, 2)");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Create savepoint1 and update some rows in t1 and inspect the data");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("UPDATE SESSION.t1 SET c12 = 3 where c12>1");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			System.out.println("  update t2 with where clause such that no rows get modified in t2 and inspect the data");
			s.executeUpdate("UPDATE SESSION.t2 SET c22 = 3 where c22>3");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Rollback to savepoint1 and we should loose all the rows in t1");
			con.rollback(savepoint1);
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			System.out.println("  temp table t2 should also have no rows because attempt was made to modfiy it (even though nothing actually got modified in t2 in the savepoint)");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Rollback the transaction and should see no data in t1 and t2");
			con.rollback();
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			s.executeUpdate("drop table SESSION.t2");
			con.commit();
			System.out.println("TEST3L PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3L FAILED");
		}

		try
		{
			System.out.println("TEST3M : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 & t2 & t3 & t4, insert few rows and commit");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c31 int, c32 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t4(c41 int, c42 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("insert into SESSION.t1 values(11, 1)");
			s.executeUpdate("insert into SESSION.t2 values(21, 1)");
			s.executeUpdate("insert into SESSION.t3 values(31, 1)");
			s.executeUpdate("insert into SESSION.t4 values(41, 1)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t4");
			dumpRS(rs1);
			con.commit();

			System.out.println(" In the next transaction, insert couple more rows in t1 & t2 & t3 and ");
			s.executeUpdate("insert into SESSION.t1 values(12, 2)");
			s.executeUpdate("insert into session.t2 values(22, 2)");
			s.executeUpdate("insert into session.t3 values(32, 2)");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);

			System.out.println("  Create savepoint1 and delete some rows from t1 and inspect the data in t1");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("DELETE FROM SESSION.t1 where c12>1");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			System.out.println("  Create savepoint2 and delete some rows from t2 this time and inspect the data in t2");
			Savepoint savepoint2 = con.setSavepoint();
			s.executeUpdate("DELETE FROM SESSION.t2 where c22>1");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Release savepoint2 and now savepoint1 should keep track of changes made to t1 and t2, inspect the data in t1 & t2");
			con.releaseSavepoint(savepoint2);
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Rollback savepoint1 and should see no data in t1 and t2, inspect the data");
			con.rollback(savepoint1);
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Should see data in t3 since it was not touched in the savepoint that was rolled back");
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);

			System.out.println("  Rollback the transaction and should see no data in t1 and t2 and t3");
			con.rollback();
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);

			System.out.println("  Should see data in t4 since it was not touched in the transaction that was rolled back");
			rs1 = s.executeQuery("select * from SESSION.t4");
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			s.executeUpdate("drop table SESSION.t2");
			s.executeUpdate("drop table SESSION.t3");
			s.executeUpdate("drop table SESSION.t4");
			con.commit();
			System.out.println("TEST3M PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3M FAILED");
		}

		try
		{
			System.out.println("TEST3N : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 & t2 & t3 & t4, insert few rows and commit");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c31 int, c32 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t4(c41 int, c42 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("insert into SESSION.t1 values(11, 1)");
			s.executeUpdate("insert into SESSION.t1 values(12, 2)");
			s.executeUpdate("insert into SESSION.t2 values(21, 1)");
			s.executeUpdate("insert into SESSION.t2 values(22, 2)");
			s.executeUpdate("insert into SESSION.t3 values(31, 1)");
			s.executeUpdate("insert into SESSION.t4 values(41, 1)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t4");
			dumpRS(rs1);
			con.commit();

			System.out.println(" In the next transaction, insert couple more rows in t3 ");
			s.executeUpdate("insert into SESSION.t3 values(31, 2)");
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);

			System.out.println("  Create savepoint1 and delete some rows from t1 and inspect the data in t1");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("DELETE FROM SESSION.t1 where c12>1");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			System.out.println("  delete from t2 with where clause such that no rows are deleted from t2 and inspect the data in t2");
			s.executeUpdate("DELETE FROM SESSION.t2 where c22>3");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Create savepoint2 and delete some rows from t2 this time and inspect the data in t2");
			Savepoint savepoint2 = con.setSavepoint();
			s.executeUpdate("DELETE FROM SESSION.t2 where c22>1");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Rollback the transaction and should see no data in t1 and t2 and t3");
			con.rollback();
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);

			System.out.println("  Should see data in t4 since it was not touched in the transaction that was rolled back");
			rs1 = s.executeQuery("select * from SESSION.t4");
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			s.executeUpdate("drop table SESSION.t2");
			s.executeUpdate("drop table SESSION.t3");
			s.executeUpdate("drop table SESSION.t4");
			con.commit();
			System.out.println("TEST3N PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3N FAILED");
		}

		try
		{
			System.out.println("TEST3O : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1 & t2, insert few rows and commit");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("insert into SESSION.t1 values(11, 1)");
			s.executeUpdate("insert into SESSION.t2 values(21, 1)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			con.commit();

			System.out.println(" In the next transaction, insert couple more rows in t1 ");
			s.executeUpdate("insert into SESSION.t1 values(12, 2)");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			System.out.println("  Create savepoint1 and insert one row in t2 and inspect the data in t2");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("insert into SESSION.t2 values(22, 2)");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Rollback savepoint1 and should see no data in t2 but t1 should have data, inspect the data");
			con.rollback(savepoint1);
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Commit the transaction and should see no data in t2 but t1 should have data");
			con.commit();
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			s.executeUpdate("drop table SESSION.t2");
			con.commit();
			System.out.println("TEST3O PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3O FAILED");
		}

		try
		{
			System.out.println("TEST3P : Savepoint and Rollback behavior");

			System.out.println(" In the transaction:");
			System.out.println("  declare temp table t1, insert few rows and commit");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");
			s.executeUpdate("insert into SESSION.t1 values(11, 1)");
			s.executeUpdate("insert into SESSION.t1 values(12, 2)");
			con.commit();

			System.out.println(" In the transaction:");
			System.out.println("  Create savepoint1 and insert some rows into t1 and inspect the data in t1");
			Savepoint savepoint1 = con.setSavepoint();
			s.executeUpdate("insert into SESSION.t1 values(13, 3)");

			System.out.println("  Release savepoint1 and now transaction should keep track of changes made to t1, inspect the data in t1");
			con.releaseSavepoint(savepoint1);
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			System.out.println("  Rollback the transaction and should still see no data in t1");
			con.rollback();
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			con.commit();
			System.out.println("TEST3P PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3P FAILED");
		}

		try
		{
			System.out.println("TEST3Q : Prepared statement test - DML and rollback behavior");
			System.out.println(" In the transaction:");
			System.out.println("  Declare temp table t2, insert / update / delete data using various prepared statements and commit");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged on commit preserve rows");
			PreparedStatement pStmtInsert = con.prepareStatement("insert into SESSION.t2 values (?, ?)");
			pStmtInsert.setInt(1, 21);
			pStmtInsert.setInt(2, 1);
			pStmtInsert.execute();
			pStmtInsert.setInt(1, 22);
			pStmtInsert.setInt(2, 2);
			pStmtInsert.execute();
			pStmtInsert.setInt(1, 23);
			pStmtInsert.setInt(2, 2);
			pStmtInsert.execute();
			PreparedStatement pStmtUpdate = con.prepareStatement("UPDATE SESSION.t2 SET c22 = 3 where c21=?");
			pStmtUpdate.setInt(1, 23);
			pStmtUpdate.execute();
			PreparedStatement pStmtDelete = con.prepareStatement("DELETE FROM SESSION.t2 where c21 = ?");
			pStmtDelete.setInt(1, 23);
			pStmtDelete.execute();

			con.commit();
			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println(" In the next transaction:");
			System.out.println("  Create savepoint1 and insert some rows into t2 using prepared statement and inspect the data in t2");
			Savepoint savepoint1 = con.setSavepoint();
			pStmtInsert.setInt(1, 23);
			pStmtInsert.setInt(2, 2);
			pStmtInsert.execute();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Create savepoint2 and update row inserted in savepoint1 using prepared statement and inspect the data in t2");
			Savepoint savepoint2 = con.setSavepoint();
			pStmtUpdate.setInt(1, 23);
			pStmtUpdate.execute();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  rollback savepoint2 and should loose all the data from t2");
			con.rollback(savepoint2);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Create savepoint3 and insert some rows into t2 using prepared statement and inspect the data in t2");
			Savepoint savepoint3 = con.setSavepoint();
			pStmtInsert.setInt(1, 21);
			pStmtInsert.setInt(2, 1);
			pStmtInsert.execute();
			pStmtInsert.setInt(1, 22);
			pStmtInsert.setInt(2, 2);
			pStmtInsert.execute();
			pStmtInsert.setInt(1, 23);
			pStmtInsert.setInt(2, 333);
			pStmtInsert.execute();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Create savepoint4 and update row inserted in savepoint3 using prepared statement and inspect the data in t2");
			Savepoint savepoint4 = con.setSavepoint();
			pStmtUpdate.setInt(1, 23);
			pStmtUpdate.execute();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  Release savepoint4 and inspect the data in t2, then delete a row from t2");
			con.releaseSavepoint(savepoint4);
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			pStmtDelete.setInt(1, 23);
			pStmtDelete.execute();

			System.out.println("  Commit transaction and should see data data in t2");
			con.commit();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t2");
			con.commit();
			System.out.println("TEST3Q PASSED");
		} catch (Throwable e)
		{
			System.out.println("FAIL " + e.getMessage());
			e.printStackTrace(System.out);
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3Q FAILED");
		}

		try
		{
			System.out.println("TEST4 : Test declared temporary table with ON COMMIT DELETE ROWS and holdable cursors and temp table as part of subquery");

			System.out.println("Temp table t1 with no direct held cursor open on it. Data should be deleted from t1 at commit time");
			Statement s1 = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
			ResultSet.HOLD_CURSORS_OVER_COMMIT );
			s1.executeUpdate("create table t1(c11 int, c12 int)");
			s1.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit delete rows not logged");
			s1.executeUpdate("insert into session.t1 values(11, 1)");
			s1.executeUpdate("insert into session.t1 values(12, 2)");
			ResultSet rs1 = s1.executeQuery("select count(*) from SESSION.t1"); //should return count of 2
			dumpRS(rs1);
			rs1 = s1.executeQuery("select count(*) from t1"); //should return count of 0
			dumpRS(rs1);
			System.out.println("Insert into real table using temporary table data on a statement with holdability set to true");
			s1.executeUpdate("INSERT INTO T1 SELECT * FROM SESSION.T1");
			con.commit();

			System.out.println("After commit, verify both the tables");

			System.out.println("Temp table t1 will have no data after commit");
			rs1 = s1.executeQuery("select count(*) from SESSION.t1"); //should return count of 0
			dumpRS(rs1);

			System.out.println("Physical table t1 will have 2 rows after commit");
			rs1 = s1.executeQuery("select count(*) from t1"); //should return count of 2
			dumpRS(rs1);

			s.executeUpdate("drop table SESSION.t1");
			s.executeUpdate("drop table t1");

			con.commit();
			System.out.println("TEST4 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST4 FAILED");
		}

		return passed;
	}

	/**
	 * Test that global temporary tables declared in a connection handle to pooled connection are dropped at connection handle close time
	 * and are not available to next connection handle to the same pooled connection 
	 *
	 * @param conn	The Connection
	 * @param s		A Statement on the Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
	static boolean testPooledConnectionClose()
					throws SQLException {
		boolean passed = true;
		Connection con1 = null, con2 = null;

		try
		{
			System.out.println("TEST5 : Temporary tables declared in a pooled connection should get dropped when that pooled connection is closed");
			ConnectionPoolDataSource dsp;
			if (isDerbyNet) {
			/* following would require the IBM universal jdbc driver to be available during build...This section needs to be reworked for networkserver
				com.ibm.db2.jcc.DB2ConnectionPoolDataSource ds = new com.ibm.db2.jcc.DB2ConnectionPoolDataSource();
				ds.setDatabaseName("wombat");
				ds.setUser("cs");
				ds.setPassword("cs");

				ds.setServerName("localhost");
				ds.setPortNumber(1527);
				ds.setDriverType(4);
				dsp = ds;
			*/
				System.out.println("test will not build without universal driver");
				return passed;
			
			} else {
				EmbeddedConnectionPoolDataSource dscsp = new EmbeddedConnectionPoolDataSource();
				dscsp.setDatabaseName("wombat");
				//dscsp.setConnectionAttributes("unicode=true");
				dsp = dscsp;
			}

			PooledConnection pc = dsp.getPooledConnection();
			con1 = pc.getConnection();
			con1.setAutoCommit(false);
			Statement s = con1.createStatement();

			System.out.println(" In the first connection handle to the pooled connection, create physical session schema, create table t1 in it");
			System.out.println(" Insert some rows in physical SESSION.t1 table. Inspect the data.");
			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t1(c21 int)");
			s.executeUpdate("insert into SESSION.t1 values(11)");
			s.executeUpdate("insert into SESSION.t1 values(12)");
			s.executeUpdate("insert into SESSION.t1 values(13)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t1"); //should return 3 rows for the physical table
			dumpRS(rs1);

			System.out.println(" Next declare a temp table with same name as physical table in SESSION schema.");
			System.out.println(" Insert some rows in temporary table SESSION.t1. Inspect the data");
			s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit preserve rows not logged");
			s.executeUpdate("insert into SESSION.t1 values(11,1)");
			rs1 = s.executeQuery("select * from SESSION.t1"); //should return 1 row for the temporary table
			dumpRS(rs1);
			System.out.println(" Now close the connection handle to the pooled connection");
			con1.commit();
			con1.close();
			con1=null;

			System.out.println(" Do another getConnection() to get a new connection handle to the pooled connection");
			con2 = pc.getConnection();
			s = con2.createStatement();
			System.out.println(" In this new handle, a select * from SESSION.t1 should be looking at the physical session table");
			rs1 = s.executeQuery("select * from SESSION.t1");
			dumpRS(rs1);

			s.executeUpdate("DROP TABLE SESSION.t1");
			if (isDerbyNet)
				s.executeUpdate("DROP TABLE SESSION.t1");

			s.executeUpdate("DROP schema SESSION restrict");
			con2.commit();
			con2.close();
			System.out.println("TEST5 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			if (con1 != null) con1.rollback();
			if (con2 != null) con2.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST5 FAILED");
		}

		return passed;
	}

	static private void dumpExpectedSQLExceptions (SQLException se) {
		System.out.println("PASS -- expected exception");
		while (se != null)
		{
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
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

	// lifted from the metadata test
	private static void dumpRS(ResultSet s) throws SQLException
	{
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		ResultSetMetaData rsmd = s.getMetaData();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount();

		if (numCols <= 0) 
		{
			System.out.println("(no columns!)");
			return;
		}

		StringBuffer heading = new StringBuffer("\t ");
		StringBuffer underline = new StringBuffer("\t ");

		int len;
		// Display column headings
		for (int i=1; i<=numCols; i++) 
		{
			if (i > 1) 
			{
				heading.append(",");
				underline.append(" ");
			}
			len = heading.length();
			heading.append(rsmd.getColumnLabel(i));
			len = heading.length() - len;
			for (int j = len; j > 0; j--)
			{
				underline.append("-");
			}
		}
		System.out.println(heading.toString());
		System.out.println(underline.toString());
		
	
		StringBuffer row = new StringBuffer();
		// Display data, fetching until end of the result set
		while (s.next()) 
		{
			row.append("\t{");
			// Loop through each column, getting the
			// column data and displaying
			for (int i=1; i<=numCols; i++) 
			{
				if (i > 1) row.append(",");
				row.append(s.getString(i));
			}
			row.append("}\n");
		}
		System.out.println(row.toString());
		s.close();
	}
}
