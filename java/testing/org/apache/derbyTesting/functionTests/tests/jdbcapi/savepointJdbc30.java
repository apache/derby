/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import java.sql.Savepoint;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Test the new class Savepoint in jdbc 30.
 * Also, test some mix and match of defining savepoints through JDBC and sql
 * Testing both callable and prepared statements meta data
 *
 * @author mamta
 */


public class savepointJdbc30 {

	static private boolean isDerbyNet = false;

	public static void main(String[] args) {
		Connection con = null, con2 = null;
		Statement  s;
		System.out.println("Test savepointJdbc30 starting");

		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();
			String framework = System.getProperty("framework");
			if (framework != null && framework.toUpperCase().equals("DERBYNET"))
				isDerbyNet = true;

			con.setAutoCommit(true); // make sure it is true
			s = con.createStatement();
			con2 = ij.startJBMS();
			con2.setAutoCommit(false);
			/* Create the table and do any other set-up */
			setUpTest(s);

			//JCC translates the JDBC savepoint calls into equivalent SQL statements.
			//In addition, (in order to stay DB2 LUW compatible), we do not allow nested savepoints when
			//coming through SQL statements. Because of this restriction, we can't run most of the
			//JDBC savepoint tests under DRDA framework. The JDBC tests have nested JDBC savepoint
			//calls and they fail when run under JCC(because they get translated into nested SQL savepoints).
			//Hence, splitting the test cases into non-DRDA and more generic tests.
			System.out.println("Tests common to DRDA and embedded Cloudscape");
			genericTests(con, con2, s);

			System.out.println("Next try non-DRDA tests");
			if (!isDerbyNet)
				nonDRDATests(con, s);

			s.close();
			con.close();
			con2.close();
		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}

		System.out.println("Test savepointJdbc30 finished");
	}

	//The following tests have nested savepoints through JDBC calls. When coming through JCC,
	//these nested JDBC savepoint calls are translated into equivalent SQL savepoint statements.
	//But for DB2 LUW compatibility, we do not allow nested savepoints coming through SQL statments
	//and hence these tests can't be run under DRDA framework.
	static void nonDRDATests(Connection con, Statement s)
					throws SQLException {
		ResultSet rs1, rs2, rs1WithHold, rs2WithHold;
		Savepoint savepoint1, savepoint2, savepoint3, savepoint4;

		//Setting autocommit to false will allow savepoints
		con.setAutoCommit(false); // make sure it is false

		//Test40 - We internally generate a unique name for unnamed savepoints. If a
		//named savepoint uses the currently used internal savepoint name, we won't
		//get an exception thrown for it because we prepend external saves with "e."
		//to avoid name conflicts.
		System.out.println("Test40 - named savepoint can't conflict with internally generated name for unnamed savepoints");
		savepoint1 = con.setSavepoint();
		savepoint2 = con.setSavepoint("i.SAVEPT0");
		con.rollback();

		//Test41 - Rolling back to a savepoint will release all the savepoints created after that savepoint.
		System.out.println("Test41a - Rollback to a savepoint, then try to release savepoint created after that savepoint");

		savepoint1 = con.setSavepoint();
		s.executeUpdate("INSERT INTO T1 VALUES(1,1)");

		savepoint2 = con.setSavepoint("s1");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");

		savepoint3 = con.setSavepoint("s2");
		s.executeUpdate("INSERT INTO T1 VALUES(3,1)");

		//Rollback to first named savepoint s1. This will internally release the second named savepoint s2.
		con.rollback(savepoint2);
		rs1 = s.executeQuery("select count(*) from t1");
		rs1.next();
		if(rs1.getInt(1) != 1) {
			System.out.println("ERROR: There should have been 1 row in the table, but found " + rs1.getInt(1) + " rows");
			return;
		}

		//Trying to release second named savepoint s2 should throw exception.
		try
		{
			con.releaseSavepoint(savepoint3);
			System.out.println("FAIL 41a release of rolled back savepoint");
		}
		catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}

		//Trying to rollback second named savepoint s2 should throw exception.
		System.out.println("Test41b - Rollback to a savepoint, then try to rollback savepoint created after that savepoint");
		try
		{
			con.rollback(savepoint3);
			System.out.println("FAIL 41b release of rolled back savepoint");
		}
		catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}

		//Release the unnamed named savepoint.
		con.rollback(savepoint1);
		rs1 = s.executeQuery("select count(*) from t1");
		rs1.next();
		if(rs1.getInt(1) != 0) {
			System.out.println("ERROR: There should have been no rows in the table, but found " + rs1.getInt(1) + " rows");
			return;
		}
		con.rollback();

		//Test42 - Rollback/commit on a connection will release all the savepoints created for that transaction
		System.out.println("Test42 - Rollback/commit the transaction, then try to use savepoint from that transaction");
		savepoint1 = con.setSavepoint();
		savepoint2 = con.setSavepoint("s1");
		con.rollback();
		try {
			con.rollback(savepoint1);
			System.out.println("FAIL 42 release of rolled back savepoint");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		//Testing commit next
		savepoint1 = con.setSavepoint();
		savepoint2 = con.setSavepoint("s1");
		con.commit();
		try {
			con.rollback(savepoint1);
			System.out.println("FAIL 42 rollback of rolled back savepoint");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}

		//Test43 - After releasing a savepoint, should be able to reuse it.
		System.out.println("Test43 - Release and reuse a savepoint name");
		savepoint1 = con.setSavepoint("s1");
		try {
			savepoint2 = con.setSavepoint("s1");
			System.out.println("FAIL 43");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.releaseSavepoint(savepoint1);
		savepoint2 = con.setSavepoint("s1");
		con.rollback();

		// Test 45 reuse savepoint name after rollback - should not work
		System.out.println("Test 45 reuse savepoint name after rollback - should not work");
		savepoint1 = con.setSavepoint("MyName");
		con.rollback(savepoint1);
		try {
			savepoint2 = con.setSavepoint("MyName");
			System.out.println("FAIL 45 reuse of savepoint name after rollback should fail");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();

		// Test 46 bug 5145 Cursors declared before and within the savepoint unit will be closed when rolling back the savepoint
		System.out.println("Test 46 Cursors declared before and within the savepoint unit will be closed when rolling back the savepoint");
		Statement sWithHold = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT );
		con.setAutoCommit(false);
		s.executeUpdate("DELETE FROM T1");
		s.executeUpdate("INSERT INTO T1 VALUES(19,1)");
		s.executeUpdate("INSERT INTO T1 VALUES(19,2)");
		s.executeUpdate("INSERT INTO T1 VALUES(19,3)");
		rs1 = s.executeQuery("select * from t1");
		rs1.next();
		rs1WithHold = sWithHold.executeQuery("select * from t1");
		rs1WithHold.next();
		savepoint1 = con.setSavepoint();
		rs2 = s.executeQuery("select * from t1");
		rs2.next();
		rs2WithHold = sWithHold.executeQuery("select * from t1");
		rs2WithHold.next();
		con.rollback(savepoint1);
		try {//resultset declared outside the savepoint unit should be closed at this point after the rollback to savepoint
			rs1.next();
			System.out.println("FAIL 46 shouldn't be able to use a resultset (declared before the savepoint unit) after the rollback to savepoint");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		try {//holdable resultset declared outside the savepoint unit should be closed at this point after the rollback to savepoint
			rs1WithHold.next();
			System.out.println("FAIL 46 shouldn't be able to use a holdable resultset (declared before the savepoint unit) after the rollback to savepoint");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		try {//resultset declared within the savepoint unit should be closed at this point after the rollback to savepoint
			rs2.next();
			System.out.println("FAIL 46 shouldn't be able to use a resultset (declared within the savepoint unit) after the rollback to savepoint");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		try {//holdable resultset declared within the savepoint unit should be closed at this point after the rollback to savepoint
			rs2WithHold.next();
			System.out.println("FAIL 46 shouldn't be able to use a holdable resultset (declared within the savepoint unit) after the rollback to savepoint");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();

		// Test 47 multiple tests for getSavepointId()
		System.out.println("Test 47 multiple tests for getSavepointId()");
		savepoint1 = con.setSavepoint();
		savepoint2 = con.setSavepoint();
		System.out.println(savepoint1.getSavepointId());
		System.out.println(savepoint2.getSavepointId());
		con.releaseSavepoint(savepoint2);
		savepoint2 = con.setSavepoint();
		System.out.println(savepoint2.getSavepointId());
		con.commit();
		savepoint2 = con.setSavepoint();
		System.out.println(savepoint2.getSavepointId());
		con.rollback();
		savepoint2 = con.setSavepoint();
		System.out.println(savepoint2.getSavepointId());
		con.rollback();

		// Test 48
		System.out.println("Test 48 No nested SQL savepoints allowed.");
		System.out.println("This is to match DB2 LUW behavior");
		savepoint1 = con.setSavepoint();
		savepoint2 = con.setSavepoint();
		System.out.println("Following SQL savepoint will fail because we are trying to nest it inside JDBC savepoint");
		try {
			s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
			System.out.println("FAIL 48 shouldn't be able set SQL savepoint nested inside JDBC/SQL savepoints");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		//rollback JDBC savepoint but still can't have SQL savepoint because there is still one JDBC savepoint
		con.releaseSavepoint(savepoint2);
		try {
			s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
			System.out.println("FAIL 48 Should have gotten exception for nested SQL savepoint");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.releaseSavepoint(savepoint1); //rollback last JDBC savepoint and now try SQL savepoint again
		s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		con.rollback();
	}

	//These tests do not allow savepoint nesting and hence can be run under DRDA too
	static void genericTests(Connection con, Connection con2, Statement s)
					throws SQLException {

		ResultSet rs1, rs2, rs1WithHold, rs2WithHold;
		Savepoint savepoint1, savepoint2, savepoint3, savepoint4;

		//Test1 and Test1a fail under DRDA (bug 5384).
		//Test1 - No savepoint allowed when auto commit is true
		con.setAutoCommit(true); // make sure it is true
		try
		{
			System.out.println("Test1 - no unnamed savepoints allowed if autocommit is true");
			con.setSavepoint(); // will throw exception because auto commit is true
			System.out.println("FAIL 1 - auto commit on");
		}
		catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		//Test1a - No savepoint allowed when auto commit is true
		try {
			System.out.println("Test1a - no named savepoints allowed if autocommit is true");
			con.setSavepoint("notallowed"); // will throw exception because auto commit is true
			System.out.println("FAIL 1a - auto commit on");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}

		con.setAutoCommit(false); // make sure it is false

		//Test2 - After releasing a savepoint, should be able to reuse it.
		System.out.println("Test2 - Release and reuse a savepoint name");
		savepoint1 = con.setSavepoint("s1");
		con.releaseSavepoint(savepoint1);
		savepoint2 = con.setSavepoint("s1");
		con.rollback();

		//Test3 - Named savepoints can't pass null for name
		try {
			System.out.println("Test3 - null name not allowed for named savepoints");
			con.setSavepoint(null);
			System.out.println("FAIL 3 null savepoint ");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();

		//Test4 - Verify names/ids of named/unnamed savepoints
		//named savepoints don't have an id.
		//unnamed savepoints don't have a name (internally, all our savepoints have names,
		//but for unnamed savepoint, that is not exposed thro jdbc api)
		System.out.println("Test4 - Verify names/ids of named/unnamed savepoints");
		try {
			savepoint1 = con.setSavepoint();
			savepoint1.getSavepointId();
			//following should throw exception for un-named savepoint
			savepoint1.getSavepointName();
			System.out.println("FAIL 4 getSavepointName on id savepoint ");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();
		try {
			savepoint1 = con.setSavepoint("s1");
			savepoint1.getSavepointName();
			//following should throw exception for named savepoint
			savepoint1.getSavepointId();
			System.out.println("FAIL 4 getSavepointId on named savepoint ");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();

		// TEST 5a and 5b for bug 4465
		// test 5a - create two savepoints in two different transactions
		// and release the first one in the subsequent transaction
		System.out.println("Test5a - create two savepoints in two different transactions" +
			" and release the first one in the subsequent transaction");
		savepoint1 = con.setSavepoint("s1");
		con.commit();
		//The following savepoint was earlier named s1. Changed it to s2 while working on DRDA support
		//for savepoints. The reason for that is as follows
		//JCC translates all savepoint jdbc calls to equivalent sql and hence if the 2 savepoints in
		//different connections are named the same, then the release savepoint below will get converted to
		//RELEASE TO SAVEPOINT s1 and that succeeds because the 2nd connection does have a savepoint named s1.
		//Hence we don't really check what we intended to check which is trying to release a savepoint created
		//in a different transaction
		savepoint2 = con.setSavepoint("s2");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
		try	{
			con.releaseSavepoint(savepoint1);
			System.out.println("FAIL 5a - release savepoint from a different transaction did not raise error");
		}	catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.commit();

		// test 5b - create two savepoints in two different transactions
		// and rollback the first one in the subsequent transaction
		System.out.println("Test5b - create two savepoints in two different transactions" +
			" and rollback the first one in the subsequent transaction");
		savepoint1 = con.setSavepoint("s1");
		con.commit();
		//The following savepoint was earlier named s1. Changed it to s2 while working on DRDA support
		//for savepoints. The reason for that is as follows
		//JCC translates all savepoint jdbc calls to equivalent sql and hence if the 2 savepoints in
		//different connections are named the same, then the rollback savepoint below will get converted to
		//ROLLBACK TO SAVEPOINT s1 and that succeeds because the 2nd connection does have a savepoint named s1.
		//Hence we don't really check what we intended to check which is trying to rollback a savepoint created
		//in a different transaction
		savepoint2 = con.setSavepoint("s2");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
		try {
			con.rollback(savepoint1);
			System.out.println("FAIL 5b - rollback savepoint from a different transaction did not raise error");
		}	catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.commit();

		// test 6a - create a savepoint release it and then create another with the same name.
		// and release the first one
		System.out.println("Test6a - create a savepoint, release it, create another with" +
			" same name and release the first one");
		savepoint1 = con.setSavepoint("s1");
		con.releaseSavepoint(savepoint1);
		//The following savepoint was earlier named s1. Changed it to s2 while working on DRDA support
		//for savepoints. The reason for that is as follows
		//JCC translates all savepoint jdbc calls to equivalent sql and hence if the 2 savepoints in
		//a transaction are named the same, then the release savepoint below will get converted to
		//RELEASE TO SAVEPOINT s1 and that succeeds because there is a valid savepoint named s1.
		savepoint2 = con.setSavepoint("s2");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
		try {
			con.releaseSavepoint(savepoint1);
			System.out.println("FAIL 6a - releasing a released savepoint did not raise error");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.commit();

		// test 6b - create a savepoints release it and then create another with the same name.
		// and rollback the first one
		System.out.println("Test6b - create a savepoint, release it, create another with" +
			" same name and rollback the first one");
		savepoint1 = con.setSavepoint("s1");
		con.releaseSavepoint(savepoint1);
		//The following savepoint was earlier named s1. Changed it to s2 while working on DRDA support
		//for savepoints. The reason for that is as follows
		//JCC translates all savepoint jdbc calls to equivalent sql and hence if the 2 savepoints in
		//a transaction are named the same, then the rollback savepoint below will get converted to
		//ROLLBACK TO SAVEPOINT s1 and that succeeds because there is a valid savepoint named s1.
		savepoint2 = con.setSavepoint("s2");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
		try {
			con.rollback(savepoint1);
			System.out.println("FAIL 6b - rollback a released savepoint did not raise error");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.commit();

/* TEST case just for bug 4467
			// Test 10 - create a named savepoint with the a generated name 
			savepoint1 = con2.setSavepoint("SAVEPT0");

			// what exactly is the correct behaviour here?
			try {
				savepoint2 = con2.setSavepoint();
			} 
			catch (SQLException se) {
				System.out.println("Expected Exception is " + se.getMessage());
			}
			con2.commit();
*/

		System.out.println("Test6c - Try to use a savepoint from another connection for release");
		savepoint1 = con.setSavepoint("s1");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
		try {
			con2.releaseSavepoint(savepoint1);
			System.out.println("FAIL 6c - releasing another transaction's savepoint did not raise error");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.commit();
		con2.commit();

		/* BUG 4468 - should not be able to pass a savepoint from a different transaction for release/rollback */
		// Test 7a - swap savepoints across connections
		System.out.println("Test7a - swap savepoints across connections with release");
		savepoint1 = con2.setSavepoint("s1");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
		savepoint2 = con.setSavepoint("s1");
		try {
			con.releaseSavepoint(savepoint1);
			System.out.println("FAIL 7a - releasing a another transaction's savepoint did not raise error");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.commit();
		con2.commit();

		// Test 7b - swap savepoints across connections
		System.out.println("Test7b - swap savepoints across connections with rollback");
		savepoint1 = con2.setSavepoint("s1");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
		savepoint2 = con.setSavepoint("s1");
		try {
			con.rollback(savepoint1);
			System.out.println("FAIL 7b - rolling back a another transaction's savepoint did not raise error");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.commit();
		con2.commit();

		/*  
		 *  following section attempts to call statement in a method to do a negative test
		 *  because savepoints are not supported in a trigger
		 *  however, this cannot be done because a call is not supported in a trigger.
		 *  leaving the test here for later reference for when we support the SQL version
                 *
		// bug 4507 - Test 8 test all 4 savepoint commands inside the trigger code
		System.out.println("Test 8a set savepoint(unnamed) command inside the trigger code");
		s.executeUpdate("create trigger trig1 before insert on t1 for each statement call org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30::doConnectionSetSavepointUnnamed()");
		try {
	
			s.executeUpdate("insert into t1 values(1,1)");
			System.out.println("FAIL 8a set savepoint(unnamed) command inside the trigger code");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		s.executeUpdate("drop trigger trig1");

		System.out.println("Test 8b set savepoint(named) command inside the trigger code");
		s.executeUpdate("create trigger trig2 before insert on t1 for each statement call org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30::doConnectionSetSavepointNamed()");
		try {
			s.executeUpdate("insert into t1 values(1,1)");
			System.out.println("FAIL 8b set savepoint(named) command inside the trigger code");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		s.executeUpdate("drop trigger trig2");

		System.out.println("Test 8c release savepoint command inside the trigger code");
		s.executeUpdate("create trigger trig3 before insert on t1 for each statement call org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30::doConnectionReleaseSavepoint()");
		try {
			s.executeUpdate("insert into t1 values(1,1)");
			System.out.println("FAIL 8c release savepoint command inside the trigger code");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		s.executeUpdate("drop trigger trig3");

		System.out.println("Test 8d rollback savepoint command inside the trigger code");
		s.executeUpdate("create trigger trig4 before insert on t1 for each statement call org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30::doConnectionRollbackSavepoint()");
		try {
			s.executeUpdate("insert into t1 values(1,1)");
			System.out.println("FAIL 8d rollback savepoint command inside the trigger code");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		s.executeUpdate("drop trigger trig4");
		con.rollback();
		*/ //end commented out test 8

		// Test 9 test savepoint name and verify case sensitivity
		System.out.println("Test 9 test savepoint name");
		savepoint1 = con.setSavepoint("myname");
		String savepointName = savepoint1.getSavepointName();
		if (!savepointName.equals("myname"))
			System.out.println("fail - savepoint name mismatch");
		con.rollback();

		// Test 10 test savepoint name case sensitivity
		System.out.println("Test 10 test savepoint name case sensitivity");
		savepoint1 = con.setSavepoint("MyName");
		savepointName = savepoint1.getSavepointName();
		if (!savepointName.equals("MyName"))
			System.out.println("fail - savepoint name mismatch");
		con.rollback();

		// Test 11 rolling back a savepoint multiple times - should work
		System.out.println("Test 11 rolling back a savepoint multiple times - should work");
		savepoint1 = con.setSavepoint("MyName");
		con.rollback(savepoint1);
		try {
			con.rollback(savepoint1);
		} catch (SQLException se) {
			System.out.println("FAIL 11 second rollback failed");
			System.out.println("Exception is " + se.getMessage());
		}
		con.rollback();

		// Test 12 releasing a savepoint multiple times - should not work
		System.out.println("Test 12 releasing a savepoint multiple times - should not work");
		savepoint1 = con.setSavepoint("MyName");
		con.releaseSavepoint(savepoint1);
		try {
			con.releaseSavepoint(savepoint1);
			System.out.println("FAIL 12 releasing a savepoint multiple times should fail");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();

		// Test 13 shouldn't be able to use a savepoint from earlier transaction after setting autocommit on and off
		System.out.println("Test 13 shouldn't be able to use a savepoint from earlier transaction after setting autocommit on and off");
		savepoint1 = con.setSavepoint("MyName");
		con.setAutoCommit(true);
		con.setAutoCommit(false);
		savepoint2 = con.setSavepoint("MyName1");
		try {//shouldn't be able to use savepoint from earlier tranasaction after setting autocommit on and off
			con.releaseSavepoint(savepoint1);
			System.out.println("FAIL 13 shouldn't be able to use a savepoint from earlier transaction after setting autocommit on and off");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.releaseSavepoint(savepoint2);
		con.rollback();

		// Test 14 cause a transaction rollback and that should release the internal savepoint array
		System.out.println("Test 14 A non-user initiated transaction rollback should release the internal savepoint array");
		Statement s1, s2;
		s1 = con.createStatement();
		s1.executeUpdate("insert into t1 values(1,1)");
		s1.executeUpdate("insert into t1 values(2,0)");
		con.commit();
		s1.executeUpdate("update t1 set c11=c11+1 where c12 > 0");
		s2 = con2.createStatement();
		savepoint1 = con2.setSavepoint("MyName");
		try {//following will get lock timeout which will rollback transaction on c2
			s2.executeUpdate("update t1 set c11=c11+1 where c12 < 1");
			System.out.println("FAIL 14 should have gotten lock time out");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		try {//the transaction rollback above should have removed the savepoint MyName
			con2.releaseSavepoint(savepoint1);
			System.out.println("FAIL 14 A non-user initiated transaction rollback should release the internal savepoint array");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();
		con2.rollback();
		s.execute("delete from t1");
		con.commit();

		// Test 15 check savepoints in batch
		System.out.println("Test 15 check savepoints in batch");
		s.execute("delete from t1");
		s.addBatch("insert into t1 values(1,1)");
		s.addBatch("insert into t1 values(1,1)");
		savepoint1 = con.setSavepoint();
		s.addBatch("insert into t1 values(1,1)");
		s.executeBatch();
		con.rollback(savepoint1);
		int val = count(con,s);
		if (val != 0)
			System.out.println("FAIL 15 savepoint should have been set before batch");
		con.rollback();

		// Test 16 grammar check for savepoint sq1
		System.out.println("Test 16 grammar check for savepoint sq1");
		try {
			s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS");
			System.out.println("FAIL 16 Should have gotten exception for missing ON ROLLBACK RETAIN CURSORS");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		try {
			s.executeUpdate("SAVEPOINT s1 UNIQUE ON ROLLBACK RETAIN CURSORS ON ROLLBACK RETAIN CURSORS");
			System.out.println("FAIL 16 Should have gotten exception for multiple ON ROLLBACK RETAIN CURSORS");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		try {
			s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN LOCKS");
			System.out.println("FAIL 16 Should have gotten exception for multiple ON ROLLBACK RETAIN LOCKS");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		try {
			s.executeUpdate("SAVEPOINT s1 UNIQUE UNIQUE ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
			System.out.println("FAIL 16 Should have gotten exception for multiple UNIQUE keywords");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN CURSORS ON ROLLBACK RETAIN LOCKS");
		s.executeUpdate("RELEASE TO SAVEPOINT s1");
		con.rollback();

		// Test 17
		System.out.println("Test 17 No nested savepoints allowed when using SQL to set savepoints. This is to match DB2 LUW behavior");
		System.out.println("Test 17a Test with UNIQUE clause.");
		s.executeUpdate("SAVEPOINT s1 UNIQUE ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		try {
			s.executeUpdate("SAVEPOINT s2 UNIQUE ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
			System.out.println("FAIL 17a Should have gotten exception for nested savepoints");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		s.executeUpdate("RELEASE TO SAVEPOINT s1");
		s.executeUpdate("SAVEPOINT s2 UNIQUE ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		con.rollback();

		System.out.println("Test 17b Test without UNIQUE clause.");
		System.out.println("Since no nesting is allowed, skipping UNIQUE still gives error for trying to define another savepoint");
		s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		try {
			s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
			System.out.println("FAIL 17b Should have gotten exception for nested savepoints");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();

		// Test 18
		System.out.println("Test 18 No nested SQL savepoints allowed inside JDBC savepoint.");
		System.out.println("This is to match DB2 LUW behavior");
		savepoint1 = con.setSavepoint();
		System.out.println("Following SQL savepoint will fail because we are trying to nest it inside JDBC savepoint");
		try {
			s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
			System.out.println("FAIL 18 shouldn't be able set SQL savepoint nested inside JDBC savepoints");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		//rollback the JDBC savepoint. Now since there are no user defined savepoints, we can define SQL savepoint
		con.releaseSavepoint(savepoint1);
		s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		con.rollback();

		// Test 19
		System.out.println("Test 19 No nested SQL savepoints allowed inside SQL savepoint.");
		System.out.println("This is to match DB2 LUW behavior");
		s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		System.out.println("Following SQL savepoint will fail because we are trying to nest it inside SQL savepoint");
		try {
			s.executeUpdate("SAVEPOINT s2 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
			System.out.println("FAIL 19 shouldn't be able set SQL savepoint nested inside SQL savepoint");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		//rollback the SQL savepoint. Now since there are no user defined savepoints, we can define SQL savepoint
		s.executeUpdate("RELEASE TO SAVEPOINT s1");
		s.executeUpdate("SAVEPOINT s2 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		con.rollback();

		// Test 20
		System.out.println("Test 20 Rollback of SQL savepoint works same as rollback of JDBC savepoint.");
		s.executeUpdate("DELETE FROM T1");
		con.commit();
		s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		s.executeUpdate("INSERT INTO T1 VALUES(1,1)");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
		s.executeUpdate("INSERT INTO T1 VALUES(3,1)");
		//Rollback to SQL savepoint and should see changes rolledback
		s.executeUpdate("ROLLBACK TO SAVEPOINT s1");
		rs1 = s.executeQuery("select count(*) from t1");
		rs1.next();
		if(rs1.getInt(1) != 0) {
			System.out.println("ERROR: There should have been 0 rows in the table, but found " + rs1.getInt(1) + " rows");
			return;
		}
		con.rollback();

		// Test 21
		System.out.println("Test 21 After releasing the SQL savepoint, rollback the transaction and should see everything undone.");
		s.executeUpdate("SAVEPOINT s1 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		s.executeUpdate("INSERT INTO T1 VALUES(1,1)");
		s.executeUpdate("INSERT INTO T1 VALUES(2,1)");
		s.executeUpdate("INSERT INTO T1 VALUES(3,1)");
		//Release the SQL savepoint and then rollback the transaction and should see changes rolledback
		s.executeUpdate("RELEASE TO SAVEPOINT s1");
		con.rollback();
		rs1 = s.executeQuery("select count(*) from t1");
		rs1.next();
		if(rs1.getInt(1) != 0) {
			System.out.println("ERROR: There should have been 0 rows in the table, but found " + rs1.getInt(1) + " rows");
			return;
		}
		con.rollback();

		// Test 22
		System.out.println("Test 22 Should not be able to create a SQL savepoint starting with name SYS");
		try {
			s.executeUpdate("SAVEPOINT SYSs2 ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
			System.out.println("FAIL 22 shouldn't be able to create a SQL savepoint starting with name SYS");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();

		// Test 23 - bug 5817 - make savepoint and release non-reserved keywords
		System.out.println("Test 23 Should be able to use non-reserved keywords savepoint and release as identifiers");
		System.out.println("Create table with savepoint and release as identifiers");
		s.execute("create table savepoint (savepoint int, release int)");
		rs1 = s.executeQuery("select count(*) from savepoint");
		rs1.next();
		if(rs1.getInt(1) != 0) {
			System.out.println("ERROR: There should have been 0 rows in the table, but found " + rs1.getInt(1) + " rows");
			return;
		}
		System.out.println("Create a savepoint with name savepoint");
		s.execute("SAVEPOINT savepoint ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		s.executeUpdate("INSERT INTO savepoint VALUES(1,1)");
		System.out.println("Release the savepoint with name savepoint");
		s.execute("RELEASE SAVEPOINT savepoint");
		rs1 = s.executeQuery("select count(*) from savepoint");
		rs1.next();
		if(rs1.getInt(1) != 1) {
			System.out.println("ERROR: There should have been 1 rows in the table, but found " + rs1.getInt(1) + " rows");
			return;
		}
		System.out.println("Create a savepoint with name release");
		s.execute("SAVEPOINT release ON ROLLBACK RETAIN LOCKS ON ROLLBACK RETAIN CURSORS");
		s.executeUpdate("INSERT INTO savepoint VALUES(2,1)");
		System.out.println("Rollback to the savepoint with name release");
		s.execute("ROLLBACK TO SAVEPOINT release");
		rs1 = s.executeQuery("select count(*) from savepoint");
		rs1.next();
		if(rs1.getInt(1) != 1) {
			System.out.println("ERROR: There should have been 1 rows in the table, but found " + rs1.getInt(1) + " rows");
			return;
		}
		System.out.println("Release the savepoint with name release");
		s.execute("RELEASE SAVEPOINT release");
		con.rollback();

		// Test 24
		System.out.println("Test 24 Savepoint name can't exceed 128 characters");
		try {
			savepoint1 = con.setSavepoint("MyName1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890");
			System.out.println("FAIL 24 shouldn't be able to create a SQL savepoint with name exceeding 128 characters");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();

		// Test 25
		System.out.println("Test 25 Should not be able to create a SQL savepoint starting with name SYS through jdbc");
		try {
			savepoint1 = con.setSavepoint("SYSs2");
			System.out.println("FAIL 25 shouldn't be able to create a SQL savepoint starting with name SYS through jdbc");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		con.rollback();

		s1.close();
		s2.close();

		// bug 4451 - Test 26a pass Null value to rollback
		// bug 5374 - Passing a null savepoint to rollback or release method
		// used to give a npe in JCC
		// it should give a SQLException aying "Cannot rollback to a null savepoint" 
		System.out.println("Test 26a rollback of null savepoint");
		try {
			con.rollback((Savepoint) null);
			System.out.println("FAIL 26a rollback of null savepoint did not raise error ");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
		// Test 26b pass Null value to releaseSavepoint
		System.out.println("Test 26b release  of null savepoint");
		try {
			con.releaseSavepoint((Savepoint) null);
			System.out.println("FAIL 26b release of null savepoint did not raise error ");
		} catch (SQLException se) {
			System.out.println("Expected Exception is " + se.getMessage());
		}
	}

	//Set up the test by creating the table used by the rest of the test.
	static void setUpTest(Statement s)
					throws SQLException {
		/* Create a table */
		s.execute("create table t1 (c11 int, c12 smallint)");
		s.execute("create table t2 (c11 int)");

	}

	static private int count(Connection con, Statement s) throws SQLException {
		int count = 0;
		ResultSet rs = s.executeQuery("select count(*) from t1");
		rs.next();
		count = rs.getInt(1);
		rs.close();
		return count;
	}
  
	public static void doConnectionSetSavepointUnnamed() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Savepoint s1 = conn.setSavepoint();
		Statement s = conn.createStatement();
		s.executeUpdate("insert into t2 values(1)");
		conn.rollback(s1);
	}

	public static void doConnectionSetSavepointNamed() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Savepoint s1 = conn.setSavepoint("s1");
		Statement s = conn.createStatement();
		s.executeUpdate("insert into t2 values(1)");
		conn.rollback(s1);
	}

	public static void doConnectionRollbackSavepoint() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.rollback((Savepoint) null);
		Statement s = conn.createStatement();
		s.executeUpdate("insert into t2 values(1)");
	}

	public static void doConnectionReleaseSavepoint() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.releaseSavepoint((Savepoint) null);
		Statement s = conn.createStatement();
		s.executeUpdate("insert into t2 values(1)");
	}

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se.printStackTrace(System.out);
			se = se.getNextException();
		}
	}
}
