/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.declareGlobalTempTableJava

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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;                                                                                     
import java.sql.SQLException;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Test for declared global temporary tables introduced in Cloudscape 5.2
 * The temp table tests with holdable cursor and savepoints are in declareGlobalTempTableJavaJDBC30 class.
 * The reason for a different test class is that the holdability and savepoint support is under jdk14 and higher.
 * But we want to be able to run the non-jdk14 specific tests under all the jdks we support and hence splitting
 * the tests into 2 different classes
 */


public class declareGlobalTempTableJava {

	/*
	** There is a small description prior to each sub-test describing what is being tested.
	*/
	public static void main(String[] args) {
		boolean		passed = true;

		Connection con1 = null, con2 = null;
		Statement  s = null;

		/* Run all parts of this test, and catch any exceptions */
		try {
			System.out.println("Test declaredGlobalTempTableJava starting");

			/* Load the JDBC Driver class */
			// use the ij utility to read the property file and
			// make the initial connection.
      
			ij.getPropertyArg(args);
			con1 = ij.startJBMS();
			con2 = ij.startJBMS();

			s = con1.createStatement();
			con1.setAutoCommit(false);
			con2.setAutoCommit(false);

			/* Test various schema and grammar related cases */
			passed = testSchemaNameAndGrammar(con1, s) && passed;

			/* Test various unallowed operations */
			passed = testOtherOperations(con1, s, con2) && passed;

			con1.close();
			con2.close();

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
	 * Test various schema and grammar related cases
	 *
	 * @param conn	The Connection
	 * @param s		A Statement on the Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean testSchemaNameAndGrammar(Connection con1, Statement s)
					throws SQLException {
		boolean passed = true;

		try
		{
			System.out.println("TEST1 : global temporary tables can only be in SESSION schema");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE APP.t2(c21 int) on commit delete rows not logged");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST1 FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.commit();
			System.out.println("TEST1 PASSED");
		}

		try
		{
			System.out.print("TEST2A : Declaring a global temporary table while in SYS schema will pass ");
			System.out.println("because temp tables always go in SESSION schema and never in default schema");

			s.executeUpdate("set schema SYS");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE t2(c21 int) on commit delete rows not logged");

			con1.commit();
			System.out.println("TEST2A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: " + e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST2A FAILED");
		}

		try
		{
			System.out.println("TEST2B : Drop the declared global temporary table declared in TEST2A while in schema SYS");

			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("set schema APP");

			con1.commit();
			System.out.println("TEST2B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: " + e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST2B FAILED");
		}

		try
		{
			System.out.println("TEST3A : positive grammar tests for DECLARE command");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tA(c1 int) not logged");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tB(c1 int) on commit delete rows not logged");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tC(c1 int) not logged on commit delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tD(c1 int) on commit preserve rows not logged");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tE(c1 int) not logged on commit preserve rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tF(c1 int) on rollback delete rows not logged");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tG(c1 int) not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tH(c1 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tI(c1 int) not logged on commit preserve rows on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tJ(c1 int) not logged on rollback delete rows on commit preserve rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tK(c1 int) on commit delete rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tL(c1 int) not logged on commit delete rows on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE tM(c1 int) not logged on rollback delete rows on commit delete rows");

			s.executeUpdate("DROP TABLE SESSION.tA");
			s.executeUpdate("DROP TABLE SESSION.tB");
			s.executeUpdate("DROP TABLE SESSION.tC");
			s.executeUpdate("DROP TABLE SESSION.tD");
			s.executeUpdate("DROP TABLE SESSION.tE");
			s.executeUpdate("DROP TABLE SESSION.tF");
			s.executeUpdate("DROP TABLE SESSION.tG");
			s.executeUpdate("DROP TABLE SESSION.tH");
			s.executeUpdate("DROP TABLE SESSION.tI");
			s.executeUpdate("DROP TABLE SESSION.tJ");
			s.executeUpdate("DROP TABLE SESSION.tK");
			s.executeUpdate("DROP TABLE SESSION.tL");
			s.executeUpdate("DROP TABLE SESSION.tM");
			con1.commit();
			System.out.println("TEST3A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3A FAILED");
		}

		try
		{
			System.out.println("TEST3B : negative grammar tests for DECLARE command");

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE t1(c11 int)");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table without NOT LOGGED. " + e.getMessage());
			}

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE t1(c11 int) NOT LOGGED NOT LOGGED");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table with multiple NOT LOGGED. " + e.getMessage());
			}

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE t1(c11 int) NOT LOGGED ON COMMIT PRESERVE ROWS ON COMMIT DELETE ROWS");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table with multiple ON COMMIT. " + e.getMessage());
			}

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE t1(c11 int) NOT LOGGED ON ROLLBACK DELETE ROWS ON ROLLBACK DELETE ROWS");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table with multiple ON ROLLBACK. " + e.getMessage());
			}

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE t1(c11 int) NOT LOGGED ON ROLLBACK PRESERVE ROWS");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table with syntax error ON ROLLBACK PRESERVE ROWS. " + e.getMessage());
			}

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE t1(c11 int) ON ROLLBACK DELETE ROWS ON COMMIT PRESERVE ROWS");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table without NOT LOGGED. " + e.getMessage());
			}

			con1.commit();
			System.out.println("TEST3B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST3B FAILED");
		}

		return passed;
	}

	/**
	 * Test various other operations on declared global temporary tables
	 *
	 * @param con1	Connection to the database
	 * @param s		A Statement on the Connection
	 * @param con2	Another Connection to the database
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean testOtherOperations(Connection con1, Statement s, Connection con2)
					throws SQLException {
		boolean passed = true;

		try
		{
			System.out.println("TEST4A : ALTER TABLE not allowed on global temporary tables");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) not logged on commit delete rows");
			s.executeUpdate("ALTER TABLE SESSION.t2 add column c22 int");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST4A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST4A PASSED");
		}

		try
		{
			System.out.println("TEST4B : ALTER TABLE on physical table in SESSION schema should work");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2(c21 int)");
			s.executeUpdate("ALTER TABLE SESSION.t2 add column c22 int");
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST4B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST4B FAILED");
		}

		try
		{
			System.out.println("TEST5A : LOCK TABLE not allowed on global temporary tables");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			s.executeUpdate("LOCK TABLE SESSION.t2 IN SHARE MODE");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST5A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST5A PASSED");
		}

		try
		{
			System.out.println("TEST5B : LOCK TABLE on physical table in SESSION schema should work");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2(c21 int)");
			s.executeUpdate("LOCK TABLE SESSION.t2 IN EXCLUSIVE MODE");
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("DROP schema SESSION restrict");

			con1.commit();
			System.out.println("TEST5B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST5B FAILED");
		}

		try
		{
			System.out.println("TEST6A : RENAME TABLE not allowed on global temporary tables");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			s.executeUpdate("RENAME TABLE SESSION.t2 TO t3");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST6A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST6A PASSED");
		}

		try
		{
			System.out.println("TEST6B : RENAME TABLE on physical table in SESSION schema should work");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2(c21 int)");
			s.executeUpdate("RENAME TABLE SESSION.t2 TO t3");
			s.executeUpdate("DROP TABLE SESSION.t3");
			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST6B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST6B FAILED");
		}

		try
		{
			System.out.println("TEST6C : RENAME COLUMN on physical table in SESSION schema should work");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("SET schema SESSION");
			s.executeUpdate("CREATE TABLE t2(c21 int)");
			//s.executeUpdate("RENAME COLUMN t2.c21 TO c22");
			s.executeUpdate("SET schema APP");
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST6C PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST6C FAILED");
		}

		try
		{
			System.out.println("TEST8 : generated always as identity not supported for declared global temporary tables");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int generated always as identity) on commit delete rows not logged");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST8 FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.commit();
			System.out.println("TEST8 PASSED");
		}

		try
		{
			System.out.println("TEST9 : long datatypes not supported for declared global temporary tables");

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 blob(3k)) on commit delete rows not logged");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table with blob. " + e.getMessage());
			}

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 clob(3k)) on commit delete rows not logged");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table with clob. " + e.getMessage());
			}

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 long varchar) on commit delete rows not logged");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table with long varchar. " + e.getMessage());
			}

			try {
				s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 \"org.apache.derbyTesting.functionTests.util.ShortHolder\") on commit delete rows not logged");
			} catch (Throwable e)
			{
				System.out.println("  Expected exception. Attempted to declare a temp table with user defined type. " + e.getMessage());
			}

			con1.commit();
			System.out.println("TEST9 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST9 FAILED");
		}

		try
		{
			System.out.println("TEST10A : Primary key constraint not allowed on a declared global temporary table.");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int not null, constraint pk primary key (c21)) on commit delete rows not logged");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST10A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.commit();
			System.out.println("TEST10A PASSED");
		}

		try
		{
			System.out.println("TEST10B : Primary key constraint allowed on a physical table in SESSION schema.");

			s.executeUpdate("CREATE SCHEMA SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2(c21 int not null, constraint pk primary key (c21))");
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST10B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST10B FAILED");
		}

		try
		{
			System.out.println("TEST10C : Unique key constraint not allowed on a declared global temporary table.");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int not null unique) on commit delete rows not logged");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST10C FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.commit();
			System.out.println("TEST10C PASSED");
		}

		try
		{
			System.out.println("TEST10D : Foreign key constraint not allowed on a declared global temporary table.");

			s.executeUpdate("CREATE TABLE t1(c11 int not null unique)");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int references t1(c11)) on commit delete rows not logged");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST10D FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE t1");
			con1.commit();
			System.out.println("TEST10D PASSED");
		}

		try
		{
			System.out.println("TEST11 : Attempt to declare the same global temporary table twice will fail. Plan to support WITH REPLACE in future");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) not logged on commit preserve rows");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST11 FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST11 PASSED");
		}

		try
		{
			System.out.println("TEST12 : Try to drop a declared global temporary table that doesn't exist.");

			s.executeUpdate("DROP TABLE SESSION.t2");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST12 FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.commit();
			System.out.println("TEST12 PASSED");
		}

		try
		{
			System.out.println("TEST13A : insert into declared global temporary table will pass.");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 char(2)) on commit delete rows not logged");
			s.executeUpdate("insert into SESSION.t2 values (1, 'aa')");
			s.executeUpdate("insert into SESSION.t2 values (2, 'bb'),(3, 'cc'),(4, null)");
			s.executeUpdate("CREATE TABLE t1(c11 int, c22 char(2))");
			s.executeUpdate("insert into t1 values (5, null),(6, null),(7, 'gg')");
			s.executeUpdate("insert into SESSION.t2 (select * from t1 where c11>4)");
			s.executeUpdate("insert into SESSION.t2 select * from SESSION.t2");
			ResultSet rs1 = s.executeQuery("select sum(c21) from SESSION.t2");
      dumpRS(rs1);
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("DROP TABLE t1");

			con1.commit();
			System.out.println("TEST13A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST13A FAILED");
		}

		try
		{
			System.out.println("TEST13B : attempt to insert null into non-null column in declared global temporary table will fail.");
			System.out.println("Declare the table with non-null column, insert a row and commit");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 char(2) not null) on commit delete rows not logged");
			s.executeUpdate("insert into SESSION.t2 values (1, 'aa')");
			con1.commit();
			System.out.println("In the next transaction, attempt to insert a null value in the table will fail and we will loose all the rows from the table as part of internal rollback");
			s.executeUpdate("insert into SESSION.t2 values (2, null)");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST13B FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			System.out.println("should see no data in t2");

			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST13B PASSED");
		}

		try
		{
			System.out.println("TEST13C : declare a temporary table with default and then insert into it.");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 char(2) default 'aa', c23 varchar(20) default user ) on commit delete rows not logged");
			s.executeUpdate("insert into SESSION.t2 values (1, 'aa', null)");
			s.executeUpdate("insert into SESSION.t2(c21) values (2)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
      dumpRS(rs1);

			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST13C PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST13C FAILED");
		}

		try
		{
			System.out.println("TEST14 : Should be able to create Session schema manually.");

			s.executeUpdate("CREATE schema SESSION");

			con1.commit();
			System.out.println("TEST14 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST14 FAILED");
		}

		try
		{
			System.out.println("TEST15 : Session schema can be dropped like any other user-defined schema.");

			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST15 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST15 FAILED");
		}

		try
		{
			System.out.print("TEST16 : Create a physical SESSION schema, drop it. Next attempt to drop SESSION schema will throw ");
			System.out.println("an exception because now we are dealing with in-memory SESSION schema and it can not be dropped by drop schema.");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("drop schema SESSION restrict");

			System.out.println("In TEST16, now attempting to drop in-memory SESSION schema");
			s.executeUpdate("drop schema SESSION restrict"); //this should fail

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST16 FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.commit();
			System.out.println("TEST16 PASSED");
		}

		try
		{
			System.out.println("TEST17A : Check constraint not allowed on global temporary table");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int check (c21 > 0)) on commit delete rows not logged");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST17A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.commit();
			System.out.println("TEST17A PASSED");
		}

		try
		{
			System.out.println("TEST17B : Check constraint allowed on physical SESSION schema table");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2(c21 int check (c21 > 0))");
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST17B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST17B FAILED");
		}

		try
		{
			System.out.println("TEST18 : Test declared temporary table with ON COMMIT DELETE ROWS with and without open cursors");
			System.out.println("Tests with holdable cursor are in a different class since holdability support is only under jdk14 and higher");

			System.out.println("Temp table t2 with not holdable cursor open on it. Data should get deleted from t2 at commit time");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
			s.executeUpdate("insert into SESSION.t2 values(22, 22)");
			s.executeUpdate("insert into SESSION.t2 values(23, 23)");

			ResultSet rs2 = s.executeQuery("select count(*) from SESSION.t2"); 
      dumpRS(rs2);

			rs2 = s.executeQuery("select * from SESSION.t2"); //eventhough this cursor is open, it is not a hold cursor. Commit should delete the rows
			rs2.next();

			System.out.println("Temp table t3 with no open cursors of any kind on it. Data should get deleted from t3 at commit time");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c31 int, c32 int) on commit delete rows not logged");
			s.executeUpdate("insert into SESSION.t3 values(32, 32)");
			s.executeUpdate("insert into SESSION.t3 values(33, 33)");

			ResultSet rs3 = s.executeQuery("select count(*) from SESSION.t3"); 
      dumpRS(rs3);

			con1.commit();

			System.out.println("After commit, verify the 2 tables");
			System.out.println("Temp table t2 will have no data after commit");
			rs2 = s.executeQuery("select count(*) from SESSION.t2"); 
      dumpRS(rs2);

			System.out.println("Temp table t3 will have no data after commit");
			rs3 = s.executeQuery("select count(*) from SESSION.t3"); 
      dumpRS(rs3);

			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("DROP TABLE SESSION.t3");

			con1.commit();
			System.out.println("TEST18 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST18 FAILED");
		}

		try
		{
			System.out.println("TEST19 : Declare a temporary table with ON COMMIT PRESERVE ROWS with and without open cursors");
			System.out.println("Tests with holdable cursor are in a different class since holdability support is only under jdk14 and higher");

			System.out.println("Temp table t2 with not holdable cursor open on it. Data should be preserved, holdability shouldn't matter");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");
			s.executeUpdate("insert into SESSION.t2 values(22, 22)");
			s.executeUpdate("insert into SESSION.t2 values(23, 23)");

			ResultSet rs2 = s.executeQuery("select count(*) from SESSION.t2"); 
      dumpRS(rs2);

			rs2 = s.executeQuery("select * from SESSION.t2"); //eventhough this cursor is open, it is not a hold cursor.
			rs2.next();

			System.out.println("Temp table t3 with no open cursors of any kind on it. Data should be preserved, holdability shouldn't matter");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c31 int, c32 int) on commit preserve rows not logged");
			s.executeUpdate("insert into SESSION.t3 values(32, 32)");
			s.executeUpdate("insert into SESSION.t3 values(33, 33)");

			ResultSet rs3 = s.executeQuery("select count(*) from SESSION.t3"); 
      dumpRS(rs3);

			con1.commit();

			System.out.println("After commit, verify the 2 tables");
			System.out.println("Temp table t2 will have data after commit");
			rs2 = s.executeQuery("select count(*) from SESSION.t2"); 
      dumpRS(rs2);

			System.out.println("Temp table t3 will have data after commit");
			rs3 = s.executeQuery("select count(*) from SESSION.t3"); 
      dumpRS(rs3);

			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("DROP TABLE SESSION.t3");

			con1.commit();
			System.out.println("TEST19 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST19 FAILED");
		}

		try
		{
			System.out.println("TEST20A : CREATE INDEX not allowed on global temporary table.");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			s.executeUpdate("CREATE index t2i1 on SESSION.t2 (c21)");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST20A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST20A PASSED");
		}

		try
		{
			System.out.println("TEST21A : CREATE INDEX on physical table in SESSION schema should work");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t3 (c31 int)");
			s.executeUpdate("CREATE index t3i1 on SESSION.t3 (c31)");  
			s.executeUpdate("DROP TABLE SESSION.t3");
			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST21A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST21A FAILED");
		}
/*
		try
		{
			System.out.println("TEST22A : CREATE TRIGGER not allowed on global temporary table.");

			s.executeUpdate("CREATE TABLE t1(c11 int)");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			s.executeUpdate("CREATE TRIGGER t2tr1 before insert on SESSION.t2 for each statement insert into t1 values(1)");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST22A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("DROP TABLE t1");
			con1.commit();
			System.out.println("TEST22A PASSED");
		}

		try
		{
			System.out.println("TEST23A : CREATE TRIGGER not allowed on physical table in SESSION schema");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t3 (c31 int)");
			s.executeUpdate("CREATE TABLE SESSION.t4 (c41 int)");
			s.executeUpdate("CREATE TRIGGER t3tr1 before insert on SESSION.t3 for each statement insert into SESSION.t4 values(1)");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST23A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t3");
			s.executeUpdate("DROP TABLE SESSION.t4");
			s.executeUpdate("drop schema SESSION restrict");
			con1.commit();
			System.out.println("TEST23A PASSED");
		}

		try
		{
			System.out.println("TEST24A : Temporary tables can not be referenced in trigger action");

			s.executeUpdate("CREATE TABLE t3 (c31 int)");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t4 (c41 int) not logged");
			s.executeUpdate("CREATE TRIGGER t3tr1 before insert on t3 for each statement insert into SESSION.t4 values(1)");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST24A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE t3");
			s.executeUpdate("DROP TABLE SESSION.t4");
			con1.commit();
			System.out.println("TEST24A PASSED");
		}

		try
		{
			System.out.println("TEST24B : SESSION schema persistent tables can not be referenced in trigger action");

			s.executeUpdate("CREATE TABLE t3 (c31 int)"); //not a SESSION schema table
			s.executeUpdate("CREATE SCHEMA SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t4 (c41 int)");
			s.executeUpdate("CREATE TRIGGER t3tr1 before insert on t3 for each statement delete from SESSION.t4");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST24B FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE t3");
			s.executeUpdate("DROP TABLE SESSION.t4");
			s.executeUpdate("drop schema SESSION restrict");
			con1.commit();
			System.out.println("TEST24B PASSED");
		}
*/
		try
		{
			System.out.println("TEST26A : CREATE VIEW not allowed on global temporary table.");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			s.executeUpdate("CREATE VIEW t2v1 as select * from SESSION.t2");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST26A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST26A PASSED");
		}

		try
		{
			System.out.println("TEST27A : CREATE VIEW not allowed on physical table in SESSION schema");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t3 (c31 int)");
			s.executeUpdate("CREATE VIEW t3v1 as select * from SESSION.t3");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST27A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t3");
			s.executeUpdate("drop schema SESSION restrict");
			con1.commit();
			System.out.println("TEST27A PASSED");
		}

		try
		{
			System.out.println("TEST29A : DELETE FROM global temporary table allowed.");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 decimal) not logged");
			s.executeUpdate("insert into SESSION.t2 values(1, 1.1)");
			s.executeUpdate("insert into SESSION.t2 values(2, 2.2)");

			ResultSet rs2 = s.executeQuery("select count(*) from SESSION.t2");
			dumpRS(rs2);

			s.executeUpdate("DELETE FROM SESSION.t2 where c21 > 0");

			rs2 = s.executeQuery("select count(*) from SESSION.t2");
			dumpRS(rs2);

			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST29A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST29A FAILED");
		}

		try
		{
			System.out.println("TEST31A : UPDATE on global temporary table allowed.");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			s.executeUpdate("insert into SESSION.t2 values(1, 1)");
			s.executeUpdate("insert into SESSION.t2 values(2, 1)");

			ResultSet rs2 = s.executeQuery("select count(*) from SESSION.t2 where c22 = 1");
			rs2.next();
			if (rs2.getInt(1) != 2)
				System.out.println("TEST31A FAILED: count should have been 2.");

			s.executeUpdate("UPDATE SESSION.t2 SET c22 = 2 where c21>0");

			rs2 = s.executeQuery("select count(*) from SESSION.t2 where c22 = 1");
			rs2.next();
			if (rs2.getInt(1) != 0)
				System.out.println("TEST31A FAILED: count should have been 0.");

			rs2 = s.executeQuery("select count(*) from SESSION.t2 where c22 = 2");
			rs2.next();
			if (rs2.getInt(1) != 2)
				System.out.println("TEST31A FAILED: count should have been 2.");

			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST31A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST31A FAILED");
		}
/*
		try
		{
			System.out.println("TEST32A : SET TRIGGERS not allowed on global temporary tables");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			s.executeUpdate("SET TRIGGERS FOR SESSION.t2 ENABLED");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST32A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST32A PASSED");
		}
		try
		{
			System.out.println("TEST32C : SET TRIGGERS on physical table in SESSION schema should work");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2(c21 int)");
			s.executeUpdate("SET TRIGGERS FOR SESSION.t2 ENABLED");
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST32C PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST32C FAILED");
		}                            */

		System.out.println("Multiple tests to make sure we do not do statement caching for statement referencing SESSION schema tables");
		try
		{
			System.out.println("TEST34A : CREATE physical table and then DECLARE GLOBAL TEMPORARY TABLE with the same name in session schema.");

			con1.setAutoCommit(true);
			//Need to do following 3 in autocommit mode otherwise the data dictionary will be in write mode and statements won't get
			//cached. I need to have statement caching enabled here to make sure that tables with same names do not conflict
			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2 (c21 int)");
			s.executeUpdate("INSERT into SESSION.t2 values(21)");

			con1.setAutoCommit(false);
			//select will return data from physical table t2
			s.execute("select * from SESSION.t2");
			dumpRS(s.getResultSet());

			//declare temporary table with same name as a physical table in SESSION schema
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
			s.executeUpdate("INSERT into SESSION.t2 values(22, 22)");
			s.executeUpdate("INSERT into SESSION.t2 values(23, 23)");
			//select will return data from temp table t2
			s.execute("select c21,c22 from SESSION.t2");
			dumpRS(s.getResultSet());
			//select will return data from temp table t2
			s.execute("select * from SESSION.t2");
			dumpRS(s.getResultSet());

			//drop the temp table t2
			s.executeUpdate("DROP TABLE SESSION.t2");
			//select will return data from physical table t2 because temp table has been deleted
			s.execute("select * from SESSION.t2");
			dumpRS(s.getResultSet());

			//cleanup
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");
			con1.commit();
			System.out.println("TEST34A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: " + e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Return false to indicate failure
			System.out.println("TEST34A FAILED");
		}
		try
		{
			System.out.println("TEST34B : Physical table & TEMPORARY TABLE with the same name in session schema, try insert.");

			con1.setAutoCommit(true);
			//Need to do following 3 in autocommit mode otherwise the data dictionary will be in write mode and statements won't get
			//cached. I need to have statement caching enabled here to make sure that tables with same names do not conflict
			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2 (c21 int)");
			s.executeUpdate("INSERT into SESSION.t2 values(21)");

			con1.setAutoCommit(false);
			//select will return data from physical table t2
			s.execute("select * from SESSION.t2");
			dumpRS(s.getResultSet());

			//declare temporary table with same name as a physical table in SESSION schema
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			//select will return data from temp table t2
			s.execute("select * from SESSION.t2");
			dumpRS(s.getResultSet());
			s.executeUpdate("INSERT into SESSION.t2 values(99)");
			s.execute("select * from SESSION.t2");
			dumpRS(s.getResultSet());

			//drop the temp table t2
			s.executeUpdate("DROP TABLE SESSION.t2");
			//select will return data from physical table t2 because temp table has been deleted
			s.execute("select * from SESSION.t2");
			dumpRS(s.getResultSet());

			//cleanup
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");
			con1.commit();
			System.out.println("TEST34B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: " + e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Return false to indicate failure
			System.out.println("TEST34B FAILED");
		}

		try
		{
			System.out.println("TEST35A : Temporary table created in one connection should not be available in another connection");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			s.executeUpdate("insert into SESSION.t2 values(22, 22)");

			ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
      dumpRS(rs1);

			Statement s2 = con2.createStatement();
			ResultSet rs2 = s2.executeQuery("select count(*) from SESSION.t2"); //con2 should not find temp table declared in con1

      dumpRS(rs2);
			con1.rollback();
			con2.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST35A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			con2.commit();
			System.out.println("TEST35A PASSED");
		}

		try
		{
			System.out.println("TEST35B : Temp table in one connection should not conflict with temp table with same name in another connection");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			s.executeUpdate("insert into SESSION.t2 values(22, 22)");

			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
      dumpRS(rs1); //should return 22, 22

			Statement s2 = con2.createStatement();
			s2.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) not logged");
			s2.executeUpdate("insert into SESSION.t2 values(99)");
			ResultSet rs2 = s2.executeQuery("select * from SESSION.t2");
      dumpRS(rs2); //should return 99

			rs1 = s.executeQuery("select * from SESSION.t2");
      dumpRS(rs1); //should return 22, 22

			s.executeUpdate("DROP TABLE SESSION.t2"); //dropping temp table t2 defined for con1
			s2.executeUpdate("DROP TABLE SESSION.t2"); //dropping temp table t2 defined for con2
			con1.commit();
			con2.commit();
			System.out.println("TEST35B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			con2.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST35B FAILED");
		}

		try
		{
			System.out.println("TEST36 : After creating SESSION schema and making it current schema, temporary tables should not require SESSION qualification");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			s.executeUpdate("insert into SESSION.t2 values(21, 21)");
			s.executeUpdate("insert into SESSION.t2 values(22, 22)");

			ResultSet rs1 = s.executeQuery("select count(*) from SESSION.t2");
			rs1.next();
			if (rs1.getInt(1) != 2)
				System.out.println("TEST36 FAILED: count should have been 2.");

			s.executeUpdate("CREATE SCHEMA SESSION");
			s.executeUpdate("SET SCHEMA SESSION");

			rs1 = s.executeQuery("select count(*) from t2"); //no need to qualify temp table here because we are in SESSION schema
			rs1.next();
			if (rs1.getInt(1) != 2)
				System.out.println("TEST36 FAILED: count should have been 2.");

			s.executeUpdate("DROP TABLE t2");
			s.executeUpdate("SET SCHEMA APP");
			s.executeUpdate("drop schema SESSION restrict");
			con1.commit();
			System.out.println("TEST36 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST36 FAILED");
		}

		try
		{
			System.out.println("TEST37A : Prepared statement test - drop the temp table underneath");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			PreparedStatement pStmt = con1.prepareStatement("insert into SESSION.t2 values (?, ?)");
      pStmt.setInt(1, 21);
      pStmt.setInt(2, 1);
      pStmt.execute();

			ResultSet rs1 = s.executeQuery("select * from SESSION.t2"); 
			dumpRS(rs1);

			s.executeUpdate("DROP TABLE SESSION.t2");
      pStmt.setInt(1, 22);
      pStmt.setInt(2, 2);
      pStmt.execute();
			System.out.println("TEST37A : Should not reach here because SESSION.t2 has been dropped underneath the prepared statement");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST37A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.commit();
			System.out.println("TEST37A PASSED");
		}

		try
		{
			System.out.println("TEST37B : Prepared statement test - drop and recreate the temp table with different definition underneath");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			PreparedStatement pStmt = con1.prepareStatement("insert into SESSION.t2 values (?, ?)");
      pStmt.setInt(1, 21);
      pStmt.setInt(2, 1);
      pStmt.execute();

			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int, c23 int) not logged");
      pStmt.setInt(1, 22);
      pStmt.setInt(2, 2);
      pStmt.execute();

			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int, c23 int, c24 int not null) not logged");
      pStmt.setInt(1, 22);
      pStmt.setInt(2, 2);
      pStmt.execute();
			System.out.println("TEST37B : Should not reach here because SESSION.t2 has been recreated with not null column");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST37B FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST37B PASSED");
		}

		try
		{
			System.out.println("TEST38A : Rollback behavior - declare temp table, rollback, select should fail");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged");
			PreparedStatement pStmt = con1.prepareStatement("insert into SESSION.t2 values (?, ?)");
      pStmt.setInt(1, 21);
      pStmt.setInt(2, 1);
      pStmt.execute();

			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			con1.rollback();

			System.out.println("TEST38A : select should fail since temp table got dropped as part of rollback");
			rs1 = s.executeQuery("select * from SESSION.t2"); //no temp table t2, should fail

			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST38A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.commit();
			System.out.println("TEST38A PASSED");
		}

		try
		{
			System.out.println("TEST38B : Rollback behavior - declare temp table, commit, drop temp table, rollback, select should pass");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");
			PreparedStatement pStmt = con1.prepareStatement("insert into SESSION.t2 values (?, ?)");
      pStmt.setInt(1, 21);
      pStmt.setInt(2, 1);
      pStmt.execute();

			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			con1.commit();

			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			s.executeUpdate("DROP TABLE SESSION.t2");

			con1.rollback();
			System.out.println("TEST38B : select should pass since temp table drop was rolled back");
			rs1 = s.executeQuery("select * from SESSION.t2"); //no temp table t2, should fail
			dumpRS(rs1);

			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST38B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST38B FAILED");
		}

		try
		{
			System.out.println("TEST38C : Rollback behavior");
			System.out.println(" In the transaction:");
			System.out.println("  Declare temp table t2 with 3 columns");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int, c23 int) on commit preserve rows not logged");
			s.executeUpdate("insert into session.t2 values(1,1,1)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			System.out.println("  Drop temp table t2 (with 3 columns)");
			s.executeUpdate("DROP TABLE SESSION.t2");
			try {
				rs1 = s.executeQuery("select * from SESSION.t2");
			} catch (Throwable e)
			{
				System.out.println("  Attempted to select from temp table t2 but it failed as expected with exception " + e.getMessage());
			}
			System.out.println("  Declare temp table t2 again but this time with 2 columns");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			System.out.println(" Commit the transaction. Should have temp table t2 with 2 columns");
			con1.commit();

			System.out.println(" In the next transaction:");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			System.out.println("  Drop temp table t2 (with 2 columns)");
			s.executeUpdate("DROP TABLE SESSION.t2");
			System.out.println("  Declare temp table t2 again but this time with 1 column");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			System.out.println(" Rollback this transaction. Should have temp table t2 with 2 columns");
			con1.rollback();

			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			s.executeUpdate("DROP TABLE SESSION.t2");

			con1.commit();
			System.out.println("TEST38C PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST38C FAILED");
		}

		try
		{
			System.out.println("TEST38D : Rollback behavior for tables touched with DML");
			System.out.println(" In the transaction:");
			System.out.println("  Declare temp table t2 & t3 & t4 & t5 with preserve rows, insert data and commit");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c31 int, c32 int) not logged on commit preserve rows on rollback delete rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t4(c41 int, c42 int) not logged on rollback delete rows on commit preserve rows");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t5(c51 int, c52 int) on commit preserve rows not logged");
			s.executeUpdate("insert into session.t2 values(21,1)");
			s.executeUpdate("insert into session.t2 values(22,2)");
			s.executeUpdate("insert into session.t2 values(23,3)");
			s.executeUpdate("insert into session.t3 values(31,1)");
			s.executeUpdate("insert into session.t3 values(32,2)");
			s.executeUpdate("insert into session.t3 values(33,3)");
			s.executeUpdate("insert into session.t4 values(41,1)");
			s.executeUpdate("insert into session.t4 values(42,2)");
			s.executeUpdate("insert into session.t4 values(43,3)");
			s.executeUpdate("insert into session.t5 values(51,1)");
			s.executeUpdate("insert into session.t5 values(52,2)");
			s.executeUpdate("insert into session.t5 values(53,3)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t4");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t5");
			dumpRS(rs1);
			con1.commit();

			System.out.println(" In the next transaction:");
			System.out.println("  Declare temp table t6 with preserve rows, insert data and inspect data in all the tables");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t6(c61 int, c62 int) on commit preserve rows not logged on rollback delete rows");
			s.executeUpdate("insert into session.t6 values(61,1)");
			s.executeUpdate("insert into session.t6 values(62,2)");
			s.executeUpdate("insert into session.t6 values(63,3)");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t4");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t5");
			dumpRS(rs1);
			rs1 = s.executeQuery("select * from SESSION.t6");
			dumpRS(rs1);

			System.out.println("  delete from t2 with t5 in it's where clause, look at t2");
			s.executeUpdate("DELETE FROM session.t2 WHERE c22> (select c52 from session.t5 where c52=2)");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  delete with where clause from t3 so that no rows get deleted, look at the rows");
			s.executeUpdate("DELETE FROM session.t3 WHERE c32>3");
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);

			System.out.println("  do not touch t4");

			System.out.println("  rollback this transaction, should not see any rows in temp table t2 after rollback");
			con1.rollback();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println("  temp table t3 should have no rows because attempt was made to delete from it (even though nothing actually got deleted from it in the transaction)");
			rs1 = s.executeQuery("select * from SESSION.t3");
			dumpRS(rs1);

			System.out.println("  temp table t4 should have its data intact because it was not touched in the transaction that got rolled back");
			rs1 = s.executeQuery("select * from SESSION.t4");
			dumpRS(rs1);

			System.out.println("  temp table t5 should have its data intact because it was only used in where clause and not touched in the transaction that got rolled back");
			rs1 = s.executeQuery("select * from SESSION.t5");
			dumpRS(rs1);

			System.out.println("  temp table t6 got dropped as part of rollback of this transaction since it was declared in this same transaction");
			try {
				rs1 = s.executeQuery("select * from SESSION.t6");
			} catch (Throwable e)
			{
				System.out.println("  Attempted to select from temp table t6 but it failed as expected with exception " + e.getMessage());
			}

			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("DROP TABLE SESSION.t3");
			s.executeUpdate("DROP TABLE SESSION.t4");
			s.executeUpdate("DROP TABLE SESSION.t5");
			con1.commit();
			System.out.println("TEST38D PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST38D FAILED");
		}

		try
		{
			System.out.println("TEST39A : Verify that there is no entry in system catalogs for temporary tables");
			System.out.println(" Declare a temp table T2 and check system catalogs. Shouldn't find anything. Then drop the temp table");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			ResultSet rs1 = s.executeQuery("select * from sys.systables where tablename like 'T2'");
			dumpRS(rs1);
			rs1 = s.executeQuery("select tablename, schemaname from sys.systables t, sys.sysschemas s where t.tablename like 'T2' and t.schemaid=s.schemaid");
			dumpRS(rs1);
			s.executeUpdate("DROP TABLE SESSION.t2");
			System.out.println(" Create physical schema SESSION, create a physical table T2 in SESSION schema and check system catalogs. Should be there");
			s.executeUpdate("CREATE SCHEMA SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2(c21 int, c22 int)");
			rs1 = s.executeQuery("select * from sys.systables where tablename like 'T2'");
			dumpRS(rs1);
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST39A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST39A FAILED");
		}

		try
		{
			System.out.println("TEST39B : Verify that there is no entry in system catalogs for SESSION schmea after declare table");
			System.out.println(" Declare a temp table T2 and check system catalogs for SESSION schmea. Shouldn't find anything. Then drop the temp table");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			ResultSet rs1 = s.executeQuery("select schemaname from sys.sysschemas where schemaname like 'SESSION'");
			dumpRS(rs1);
			s.executeUpdate("DROP TABLE SESSION.t2");

			con1.commit();
			System.out.println("TEST39B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST39B FAILED");
		}

		try
		{
			System.out.println("TEST40 : DatabaseMetaData.getTables() should not return temporary tables");
			DatabaseMetaData databaseMetaData;
			databaseMetaData = con1.getMetaData();
			s.executeUpdate("CREATE SCHEMA SESSION");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			s.executeUpdate("CREATE TABLE SESSION.t3(c31 int, c32 int)");
			System.out.println("getTables() with no types:");
 			dumpRS(databaseMetaData.getTables("", null, "%", null));

			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("DROP TABLE SESSION.t3");
			s.executeUpdate("drop schema SESSION restrict");
			con1.commit();
			System.out.println("TEST40 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST40 FAILED");
		}   

		try
		{
			System.out.println("TEST41 : delete where current of on temporary tables");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
			s.executeUpdate("insert into SESSION.t2 values(21, 1)");
			s.executeUpdate("insert into SESSION.t2 values(22, 1)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			PreparedStatement pStmt1 = con1.prepareStatement("select c21 from session.t2 for update");
			ResultSet rs2 = pStmt1.executeQuery();
			rs2.next();
			PreparedStatement pStmt2 = con1.prepareStatement("delete from session.t2 where current of "+
									   rs2.getCursorName());
			pStmt2.executeUpdate();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			rs2.next();
			pStmt2.executeUpdate();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			rs2.close();
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST41 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST41 FAILED");
		}

		try
		{
			System.out.println("TEST42 : update where current of on temporary tables");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit delete rows not logged");
			s.executeUpdate("insert into SESSION.t2 values(21, 1)");
			s.executeUpdate("insert into SESSION.t2 values(22, 1)");
			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			PreparedStatement pStmt1 = con1.prepareStatement("select c21 from session.t2 for update");
			ResultSet rs2 = pStmt1.executeQuery();
			rs2.next();
			PreparedStatement pStmt2 = con1.prepareStatement("update session.t2 set c22 = 2 where current of "+
									   rs2.getCursorName());
			pStmt2.executeUpdate();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);
			rs2.next();
			pStmt2.executeUpdate();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			rs2.close();
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST42 PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST42 FAILED");
		}
/*
		try
		{
			System.out.println("TEST43A : SET CONSTRAINTS not allowed on global temporary tables");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) on commit delete rows not logged");
			s.executeUpdate("SET CONSTRAINTS FOR SESSION.t2 DISABLED");

			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST43A FAILED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST43A PASSED");
		}

		try
		{
			System.out.println("TEST43C : SET CONSTRAINTS FOR on physical table in SESSION schema should work");

			s.executeUpdate("CREATE schema SESSION");
			s.executeUpdate("CREATE TABLE SESSION.t2(c21 int)");
			s.executeUpdate("SET CONSTRAINTS FOR SESSION.t2 ENABLED");
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");

			con1.commit();
			System.out.println("TEST43C PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unexpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST43C FAILED");
		}
*/
		try
		{
			System.out.println("TEST44A : Prepared statement test - DML and rollback behavior");
			System.out.println(" In the transaction:");
			System.out.println("  Declare temp table t2, insert data using prepared statement and commit");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged on commit preserve rows");
			PreparedStatement pStmt = con1.prepareStatement("insert into SESSION.t2 values (?, ?)");
			pStmt.setInt(1, 21);
			pStmt.setInt(2, 1);
			pStmt.execute();

			con1.commit();
			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println(" In the next transaction:");
			System.out.println("  insert more data using same prepared statement and rollback. Should loose all the data in t2");
			pStmt.setInt(1, 22);
			pStmt.setInt(2, 2);
			pStmt.execute();
			con1.rollback();
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST44A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST44A FAILED");
		}

		try
		{
			System.out.println("TEST44B : Prepared statement test - DML and rollback behavior");
			System.out.println(" In the transaction:");
			System.out.println("  Declare temp table t2, insert data and commit");
			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) not logged on commit preserve rows");
			s.executeUpdate("INSERT INTO SESSION.t2 VALUES(21, 1)");

			con1.commit();
			ResultSet rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			System.out.println(" In the next transaction:");
			System.out.println("  prepare a statement for insert into table but do not execute it and rollback");
			PreparedStatement pStmt = con1.prepareStatement("insert into SESSION.t2 values (?, ?)");
			con1.rollback();
			System.out.println("  Should not loose the data from t2");
			rs1 = s.executeQuery("select * from SESSION.t2");
			dumpRS(rs1);

			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST44B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Expected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST44B FAILED");
		}

/*		try
		{
			System.out.println("TEST33A : CREATE STATEMENT attempting to reference physical SESSION table in USING clause should work??");

			s.executeUpdate("CREATE SCHEMA SESSION");
			s.executeUpdate("CREATE TABLE t1(c11 int)");
			s.executeUpdate("CREATE TABLE SESSION.t2(c21 int)");
			s.executeUpdate("INSERT INTO SESSION.t2(c21) VALUES(1)");
			s.executeUpdate("CREATE STATEMENT s2 as select * from t1 where c11 = ? using select c21 from SESSION.t2");

			s.executeUpdate("DROP STATEMENT s2");
			s.executeUpdate("DROP TABLE t1");
			s.executeUpdate("DROP TABLE SESSION.t2");
			s.executeUpdate("drop schema SESSION restrict");
			con1.commit();
			System.out.println("TEST33A PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unxpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST33A FAILED");
		}

		try
		{
			System.out.println("TEST33B : CREATE STATEMENT attempting to global temp table in USING clause should work??");

			s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int) not logged");
			s.executeUpdate("CREATE TABLE t1(c11 int)");
			s.executeUpdate("INSERT INTO SESSION.t2(c21) VALUES(1)");
			s.executeUpdate("CREATE STATEMENT s2 as select * from t1 where c11 = ? using select c21 from SESSION.t2");

			s.executeUpdate("DROP STATEMENT s2");
			s.executeUpdate("DROP TABLE t1");
			s.executeUpdate("DROP TABLE SESSION.t2");
			con1.commit();
			System.out.println("TEST33B PASSED");
		} catch (Throwable e)
		{
			System.out.println("Unxpected message: "+ e.getMessage());
			con1.rollback();
			passed = false; //we shouldn't have reached here. Set passed to false to indicate failure
			System.out.println("TEST33B FAILED");
		}  */

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
