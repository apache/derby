/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.dblook_test

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;

import org.apache.derby.tools.dblook;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derbyTesting.functionTests.util.TestUtil;



import java.util.HashMap;
import java.util.TreeMap;
import java.util.Set;
import java.util.ArrayList;

public class dblook_test {

	private static final int SERVER_PORT = 1527;
	private static final int FRONT = -1;
	private static final int REAR = 1;

	protected static final String dbCreationScript_1 = "dblook_makeDB.sql";
	protected static final String dbCreationScript_2 = "dblook_makeDB_2.sql";
	private static final char TEST_DELIMITER='#';

	protected static String testDirectory = "dblook_test";
	protected static String testDBName = "wombat";
	protected static String separator;

	private static String dbPath;
	private static int duplicateCounter = 0;
	private static int sysNameCount = 0;
	private static String jdbcProtocol;
	protected static String territoryBased = "";
	protected static String expectedCollation = "UCS_BASIC";

	/* **********************************************
	 * main:
	 ****/

	public static void main (String[] args) {

		separator = System.getProperty("file.separator");
		new dblook_test().doTest();
		System.out.println("\n[ Done. ]\n");
		renameDbLookLog("dblook_test");
//IC see: https://issues.apache.org/jira/browse/DERBY-3458

	}

	/* **********************************************
	 * doTest
	 * Run a full test of the dblook utility.
	 ****/

	protected void doTest() {

		try {

			// Test full dblook functionality.
			System.out.println("\n-= Start dblook Functional Tests. =-");
//IC see: https://issues.apache.org/jira/browse/DERBY-90
			createTestDatabase(dbCreationScript_1);
			runDBLook(testDBName);

			// Test dblook messages.
			System.out.println("\n-= Start dblook Message Tests =-");
			createTestDatabase(dbCreationScript_2);
			runMessageCheckTest(testDBName);

            // Test DERBY-6387 - wrong order of triggers
            System.out.println("\n-= Start DERBY-6387 test. =-");
            testDerby6387();

		} catch (SQLException se) {

			System.out.println("FAILED: to complete the test:");
			se.printStackTrace(System.out);
			for (se = se.getNextException(); se != null;
				se = se.getNextException())
			{
				se.printStackTrace(System.out);
			}
		
		} catch (Exception e) {

			System.out.println("FAILED: to complete the test:");
			e.printStackTrace(System.out);

		}

	}

	/* **********************************************
	 * createTestDatabase:
	 * Using the creation script created as part of
	 * the test package, create the database that
	 * will be used as the basis for all dblook
	 * tests.
	 * @param scriptName The name of the sql script
	 *  to use for creating the test database.
	 * @return The test database has been created
	 *  in the current test directory, which is
	 *  "./dblook/" (as created by the harness).
	 ****/

	protected void createTestDatabase(String scriptName)
		throws Exception
	{

		// Delete existing database, if it exists.
//IC see: https://issues.apache.org/jira/browse/DERBY-90
		try {
			deleteDB(testDBName);
		} catch (Exception e) {
			System.out.println("** Warning: failed to delete " +
				"old test db before creating a new one...");
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        Class<?> clazz = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        clazz.getConstructor().newInstance();
		jdbcProtocol = "jdbc:derby:";
		createDBFromDDL(testDBName, scriptName);

		// Figure out where our database directory is (abs path).
//IC see: https://issues.apache.org/jira/browse/DERBY-577
		String systemhome = System.getProperty("derby.system.home");
		dbPath = systemhome + File.separatorChar;
		return;

	}

	/* **********************************************
	 * runDBLook:
	 * Runs a series of tests using dblook on
	 * the received database.
	 * @param dbName The name of the database on which to
	 *   run the tests.
	 * @return A series of tests intended to verify
	 *  the full functionality of the dblook utility
	 *  has been run.
	 ****/

	private void runDBLook(String dbName)
		throws Exception
	{

		// Close the error stream, so that messages
		// printed to System.err aren't intermixed
		// with our output (otherwise, the order
		// of the System.out vs System.err is
		// arbitrary (because of the way the harness
		// works), and so we will get diffs with
		// the master.
		System.err.close();

		// First, we dump all system catalogs for
		// the original source database to file.
		dumpSysCatalogs(dbName);

		// Then, we run dblook on the source database
		// with no limitations (i.e. we generate the
		// DDL for the FULL database).
		lookOne(dbName);
		dumpFileToSysOut("dblook.log");

		// Now, create new db from the DDL that
		// was generated by dblook.
		String newDBName = dbName + "_new";
		createDBFromDDL(newDBName, dbName + ".sql");
		deleteFile(new File(dbName + ".sql"));

		// Dump all system catalogs for the database
		// that was created from the DDL generated
		// by dblook.
		dumpSysCatalogs(newDBName);

		// Delete the new database.
		deleteDB(newDBName);
		deleteFile(new File(newDBName + ".sql"));

		// Run dblook on the source database
		// with various parameter configurations,
		// to make sure they are all working as
		// planned.
		runAllTests(dbName, newDBName);

	}

	/* **********************************************
	 * runAllTests:
	 * Makes the call to execute each of the desired
	 * tests.
	 * @param dbName The name of the database on which to
	 *   run the tests.
	 * @param newDBName The name of the database to be
	 *  created from the DDL that is generated (by
	 *  dblook) for the source database.
	 ****/

	protected void runAllTests(String dbName,
		String newDBName) throws Exception
	{

		runTest(2, dbName, newDBName);

		// Test 3 is run as part of derbynet suite;
		// see derbynet/dblook_test_net.java.

		runTest(4, dbName, newDBName);
		runTest(5, dbName, newDBName);
		runTest(7, dbName, newDBName);
		runTest(6, dbName, newDBName);
		return;

	}

	/* **********************************************
	 * runTest:
	 * Runs dblook on the source database with a
	 * specific set of parameters, then uses the
	 * resultant DDL to create a new database, and
	 * dumps the system catalogs for that database
	 * to file.  Finally, the new database is deleted
	 * in preparation for subsequent calls to this
	 * method.
	 * @param whichTest An indication of which test to run;
	 *  each test number has a different set of
	 *  parameters.
	 * @param dbName The name of the source database.
	 * @param newDBName The name of the database to be
	 *  created from the DDL that is generated (by
	 *  dblook) for the source database.
	 * @return dblook has been executed using the
	 *  parameters associated with the given test,
	 *  and that DDL has been written to a ".sql"
	 *  file named after the source database;
	 *  a new database has been created from the
	 *  ".sql" generated by dblook; the system
	 *  catalogs for that new database have been
	 *  dumped to output; and the new database has
	 *  been deleted.
	 ****/

	protected void runTest(int whichTest, String dbName,
		String newDBName)
	{

		try {

			switch(whichTest) {
				case 2:		lookTwo(dbName); break;
				case 3:		lookThree(dbName); break;
				case 4:		lookFour(dbName); break;
				case 5:		lookFive(dbName); break;
				case 6:		lookSix(dbName); break;
				case 7:		lookSeven(dbName); break;
				default:	break;
			}

			dumpFileToSysOut("dblook.log");
			createDBFromDDL(newDBName, dbName + ".sql");
			dumpSysCatalogs(newDBName);
			deleteDB(newDBName);
			deleteFile(new File(dbName + ".sql"));

		} catch (SQLException e) {

			System.out.println("FAILED: Test # : " + whichTest);
//IC see: https://issues.apache.org/jira/browse/DERBY-90
			e.printStackTrace(System.out);
			for (e = e.getNextException(); e != null;
				e = e.getNextException())
			{
				e.printStackTrace(System.out);
			}

		} catch (Exception e) {

			System.out.println("FAILED: Test # : " + whichTest);
			e.printStackTrace(System.out);

		}

		return;

	}

	/* **********************************************
	 * lookOne:
	 * Use dblook to generate FULL DDL for a given
	 * database.
	 * @param dbName The name of the source database (i.e.
	 *  the database for which the DDL is generated).
	 * @return The full DDL for the source database
	 *  has been generated and written to a file
	 *  called <dbName + ".sql">.
	 ****/

	private void lookOne(String dbName)
		throws Exception
	{

		printAsHeader("\nDumping full schema for '" +
			dbName + "'\nto file '" + dbName + ".sql':\n");

		String [] args = new String[] {
			"-o", dbName + ".sql",
			"-td", ""
		};

		go(dbName, args);
		return;

	}

	/* **********************************************
	 * lookTwo:
	 * Use dblook to generate DDL for all objects 
	 * in the source database with schema 'BAR',
	 * excluding views:
	 *  -z bar -noview
	 * @param dbName The name of the source database (i.e.
	 *  the database for which the DDL is generated).
	 * @return The appropriate DDL has been generated
	 *  and written to a file called <dbName + ".sql">.
	 ****/

	private void lookTwo(String dbName)
		throws Exception
	{

		printAsHeader("\nDumping DDL for all objects " +
			"with schema\n'BAR', excluding views:\n");
 
		String [] args = new String[] {
			"-o", dbName + ".sql",
			"-td", "",
			"-z", "bar",
			"-noview"
		};

		go(dbName, args);
		return;

	}

	/* **********************************************
	 * lookThree:
	 * Use dblook to generate DDL for all objects
	 * in the source database, using Network
	 * Server.
	 * @param dbName The name of the source database (i.e.
	 *  the database for which the DDL is generated).
	 * @return The appropriate DDL has been generated
	 *  and written to a file called <dbName + ".sql">.
	 ****/

	private void lookThree(String dbName)
		throws Exception
	{

		printAsHeader("\nDumping DDL for all objects, " +
			"using\nNetwork Server:\n");
//IC see: https://issues.apache.org/jira/browse/DERBY-413
		String hostName = TestUtil.getHostName();
		jdbcProtocol = TestUtil.getJdbcUrlPrefix(hostName,SERVER_PORT);

		String sourceDBUrl;
		if (TestUtil.isJCCFramework())
			sourceDBUrl = jdbcProtocol + "\"" + dbPath +
//IC see: https://issues.apache.org/jira/browse/DERBY-3877
//IC see: https://issues.apache.org/jira/browse/DERBY-3884
				separator + dbName + "\":user=app;password=apppw;";
		else
			sourceDBUrl = jdbcProtocol + dbPath +
			separator + dbName + ";user=app;password=apppw";

		// Make sure we're not connected to the database
		// (we connected to it in embedded mode when we
		// created it, so we have to shut it down).
		try {
			DriverManager.getConnection(
				"jdbc:derby:" + dbName +
				";shutdown=true;user=app;password=apppw");
		} catch (SQLException e) {}

		// Run the test.
		try {

//IC see: https://issues.apache.org/jira/browse/DERBY-90
			new dblook(new String[] {
				"-d", sourceDBUrl,
				"-o", dbName + ".sql",
				"-td", "" }
			);

		} catch (Exception e) {
			System.out.println("FAILED: ");
			e.printStackTrace(System.out);
		}

		return;

	}

	/* **********************************************
	 * lookFour:
	 * Use dblook to generate DDL for all objects 
	 * in the source database with schema 'BAR'
	 * that are related to tables 'T3', 'tWithKeys',
	 * and 'MULTI WORD NAME'.
	 *  -z bar -t t3 "\"tWithKeys\"" "Multi word name"
	 * @param dbName The name of the source database (i.e.
	 *  the database for which the DDL is generated).
	 * @return The appropriate DDL has been generated
	 *  and written to a file called <dbName + ".sql">.
	 ****/

	private void lookFour(String dbName)
		throws Exception
	{

		printAsHeader("\nDumping DDL for all objects " +
			"with schema 'BAR'\nthat are related to tables " +
			"'T3', 'tWithKeys',\nand 'MULTI WORD NAME':\n");
 
		String [] args = new String [] {
			"-o", dbName + ".sql",
			"-td", "",
			"-z", "BAR",
			"-t", "t3", "\"tWithKeys\"", "Multi word name"
		};

		go(dbName, args);
		return;

	}

	/* **********************************************
	 * lookFive:
	 * Use dblook to generate DDL for all objects 
	 * in the source database (with any schema)
	 * that are related to table 'T1' and 'TWITHKEYS'
	 * (with no matches existing for the latter).
	 * 	-t t1 "tWithKeys"
	 * @param dbName The name of the source database (i.e.
	 *  the database for which the DDL is generated).
	 * @return The appropriate DDL has been generated
	 *  and written to a file called <dbName + ".sql">.
	 ****/

	private void lookFive(String dbName)
		throws Exception
	{

		printAsHeader("\nDumping DDL for all objects " +
			"related to 'T1'\nand 'TWITHKEYS':\n");
 
		String [] args = new String [] {
			"-o", dbName + ".sql",
			"-td", "",
			"-t", "t1", "tWithKeys"
		};

		go(dbName, args);
		return;

	}

	/* **********************************************
	 * lookSix:
	 * Call dblook with an invalid url, to make
	 * sure that errors are printed to log.
	 *   -d <dbName> // missing protocol.
	 * @param dbName The name of the source database (i.e.
	 *  the database for which the DDL is generated).
	 * @return The appropriate DDL has been generated
	 *  and written to a file called <dbName + ".sql">.
	 ****/

	private void lookSix(String dbName)
		throws Exception
	{

		printAsHeader("\nDumping DDL w/ invalid url, and " +
			"writing\nerror to the log:\n");
 
		// Url is intentionally incorrect; it will cause an error.
//IC see: https://issues.apache.org/jira/browse/DERBY-90
		new dblook(new String[] {
			"-o", dbName + ".sql",
			"-d", dbName }
		);

	}

	/* **********************************************
	 * lookSeven:
	 * Use dblook to generate DDL for all objects 
	 * in the source database with schema '"Quoted"Schema"'.
	 *  -z \"\"Quoted\"Schema\"\"
	 * @param dbName The name of the source database (i.e.
	 *  the database for which the DDL is generated).
	 * @return The appropriate DDL has been generated
	 *  and written to a file called <dbName + ".sql">.
	 ****/

	private void lookSeven(String dbName)
		throws Exception
	{

		printAsHeader("\nDumping DDL for all objects " +
			"with schema\n'\"Quoted\"Schema\"':\n");
 
		String [] args = new String[] {
			"-o", dbName + ".sql",
			"-td", "",
			"-z", "\"\"Quoted\"Schema\"\""
		};

		go(dbName, args);
		return;

	}

	/* **********************************************
	 * go:
	 * Makes the call to execute the dblook command
	 * using the received arguments.
	 * @param dbName The name of the source database (i.e.
	 *  the database for which the DDL is generated).
	 * @args The list of arguments with which to execute
	 *  the dblook command.
	 ****/

	private void go(String dbName, String [] args) {

		jdbcProtocol = "jdbc:derby:";
		String sourceDBUrl = jdbcProtocol + dbPath +
			separator + dbName + ";user=app;password=apppw";
//IC see: https://issues.apache.org/jira/browse/DERBY-3877
//IC see: https://issues.apache.org/jira/browse/DERBY-3884

//IC see: https://issues.apache.org/jira/browse/DERBY-90
		String [] fullArgs = new String[args.length+2];
		fullArgs[0] = "-d";
		fullArgs[1] = sourceDBUrl;
		for (int i = 2; i < fullArgs.length; i++)
			fullArgs[i] = args[i-2];

		try {
			new dblook(fullArgs);
		} catch (Exception e) {
			System.out.println("FAILED: to run dblook: ");
			e.printStackTrace(System.out);
		}

	}

	/* **********************************************
	 * runMessageCheckTest
	 * Run dblook and verify that all of the dblook
	 * messages are correctly displayed.
	 * @param dbName The name of the source database (i.e.
	 *  the database for which the DDL is generated).
	 * @return The DDL for a simple database, plus all
	 *  dblook messages, have been generated and written
	 *  to System.out.
	 ****/
	private void runMessageCheckTest(String dbName)
		throws Exception
	{

		// #1: First, run DB look standard to check for
		// all of the "header" messages that are printed
		// out along with DDL.
		System.out.println("\n************\n" +
			"Msg Test 1\n" +
			"************\n");
		lookOne(dbName);
		dumpFileToSysOut(dbName + ".sql");
		dumpFileToSysOut("dblook.log");

		// Now, we have to run some additional dblook commands
		// to get the "non-standard" messages.

		// #2: Specify a target table and target schema, to
		// make sure they are echoed correctly.  Also, specify
		// an output file to make sure the file creation header
		// is printed in the file.
		System.out.println(
			"\n************\n" +
			"Msg Test 2\n" +
			"************\n");
		go(dbName, new String [] {
				"-t", "t1",
				"-z", "bar",
				"-o", dbName + ".sql"
			});
		dumpFileToSysOut(dbName + ".sql");
		dumpFileToSysOut("dblook.log");

		// #3: Run without specifying a database, to make
		// sure the usage message is printed to System.out
		System.out.println(
			"\n************\n" +
			"Msg Test 3\n" +
			"************\n");
		try {
			new dblook(new String[] { "-verbose" });
		} catch (Exception e) {
			System.out.println("FAILED: to run dblook: ");
			e.printStackTrace(System.out);
		}

		// #4: Just to confirm, try once with a statement
		// delimiter, to make sure it's actually working
		// correctly (this isn't a "message" per se, but
		// still, it's worth verifying).
		System.out.println(
			"\n************\n" +
			"Msg Test 4\n" +
			"************\n");
		go(dbName, new String [] {
				"-td", " " + TEST_DELIMITER
			});

		// #5: Intentionally create an error while loading
		// a jar file, to make sure the resultant message is
		// printed correctly.
		System.out.println(
			"\n************\n" +
			"Msg Test 5\n" +
			"************\n");

		// We'll cause the error by going in and deleting
		// the jar file from the test database.  First,
		// get the jar path.
		String jarPath = (new
			File(dbPath + separator + dbName)).getAbsolutePath();

		// Have to shut db down before we can mess with it.
		try {
			Connection conn =
				DriverManager.getConnection("jdbc:derby:" + 
//IC see: https://issues.apache.org/jira/browse/DERBY-3877
//IC see: https://issues.apache.org/jira/browse/DERBY-3884
					jarPath + ";shutdown=true,user=app;password=apppw");
			conn.close();
		} catch (SQLException se) {
		// shutdown exception.
		}

		jarPath = jarPath + separator + "jar";
		deleteFile(new File(jarPath));

		// Now that we've deleted the jar file, run dblook
		// and check the error.
		go(dbName, new String [] { 
				"-verbose",
				"-o", dbName + ".sql"
			});
		dumpFileToSysOut("dblook.log");

		// Clean up.
		try {
			deleteFile(new File(dbName + ".sql"));
		} catch (Exception e) {
		// not too big of a deal if we fail; just ignore...
		}

	}

	/* **********************************************
	 * dumpSysCatalogs:
	 * Takes a database name and dumps ALL of the
	 * system catalogs for that database, with the
	 * exception of SYSSTATISTICS.  This allows us
	 * to look at the full contents of a database's
	 * schema (without using dblook, of course)
	 * so that we can see if the databases created
	 * from the DDL generated by dblook have been
	 * built correctly--if they have all of the
	 * correct system catalog information, then
	 * the databases themselves must be correct.
	 * @param dbName The name of the database for which
	 *  we are dumping the system catalogs.
	 * @return All of the system catalogs for
	 *  the received database have been dumped
	 *  to output.
	 ****/

	private void dumpSysCatalogs(String dbName)
		throws Exception
	{

		System.out.println("\nDumping system tables for '" + dbName + "'\n");

		writeOut("\n----------------=================---------------");
		writeOut("System Tables for: " + dbName);
		writeOut("----------------=================---------------\n");

		// Connect to the database.
		Connection conn = DriverManager.getConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-3877
//IC see: https://issues.apache.org/jira/browse/DERBY-3884
				"jdbc:derby:" + dbName + ";user=app;password=apppw");
		conn.setAutoCommit(false);

		// Set the system schema to ensure that UCS_BASIC collation is used.
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("SET SCHEMA SYS");
//IC see: https://issues.apache.org/jira/browse/DERBY-3458

		// Ensure that the database has the expected collation type. 
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery("VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collation')");
			rs.next();
			String collation = rs.getString(1); 
			if (collation == null || !collation.equals(expectedCollation)) {
				throw new SQLException("Collation doesn't match with the expected type " + 
						expectedCollation);
			}
		} catch (Exception e) {
			System.out.println("FAILED: incorrect database collation\n");
			System.out.println(e.getMessage());
		} finally {
			if (rs != null) {
				rs.close();
			}
		}

		// Load any id-to-name mappings that will be useful
		// when dumping the catalogs.
		HashMap<String, String> idToNameMap = loadIdMappings(stmt);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

		// Go through and dump all system catalog information,
		// filtering out database-dependent id's so that they
		// won't cause diffs.

		writeOut("\n========== SYSALIASES ==========\n");
//IC see: https://issues.apache.org/jira/browse/DERBY-3458
		rs =
			stmt.executeQuery("select schemaid, sys.sysaliases.* from sys.sysaliases");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSCHECKS ==========\n");
		rs = stmt.executeQuery("select c.schemaid, ck.* from " +
			"sys.syschecks ck, sys.sysconstraints c where " +
			"ck.constraintid = c.constraintid");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSCOLUMNS ==========\n");
		writeOut("--- Columns for Tables ---");
		rs = stmt.executeQuery("select t.schemaid, c.* from " +
			"sys.syscolumns c, sys.systables t where c.referenceid " +
			"= t.tableid" );
		dumpResultSet(rs, idToNameMap, null);
		writeOut("\n--- Columns for Statements ---");
		rs = stmt.executeQuery("select s.schemaid, c.* from " +
			"sys.syscolumns c, sys.sysstatements s where c.referenceid " +
			"= s.stmtid" );
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSCONGLOMERATES ==========\n");
		rs = stmt.executeQuery("select schemaid, sys.sysconglomerates.* " +
			"from sys.sysconglomerates");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSCONSTRAINTS ==========\n");
		rs = stmt.executeQuery("select schemaid, sys.sysconstraints.* " +
			"from sys.sysconstraints");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSDEPENDS ==========\n");
		rs = stmt.executeQuery("select dependentid, sys.sysdepends.* from sys.sysdepends");
		dumpResultSet(rs, idToNameMap, conn);

		writeOut("\n========== SYSFILES ==========\n");
		rs = stmt.executeQuery("select schemaid, sys.sysfiles.* from sys.sysfiles");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSFOREIGNKEYS ==========\n");
		rs = stmt.executeQuery("select c.schemaid, fk.* from " +
			"sys.sysforeignkeys fk, sys.sysconstraints c where " +
			"fk.constraintid = c.constraintid");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSKEYS ==========\n");
		rs = stmt.executeQuery("select c.schemaid, k.* from " +
			"sys.syskeys k, sys.sysconstraints c where " +
			"k.constraintid = c.constraintid");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSSCHEMAS ==========\n");
		rs = stmt.executeQuery("select schemaid, sys.sysschemas.* from sys.sysschemas");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSSTATEMENTS ==========\n");
		rs = stmt.executeQuery("select schemaid, sys.sysstatements.* from sys.sysstatements");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSTABLES ==========\n");
		rs = stmt.executeQuery("select schemaid, sys.systables.* from sys.systables");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSTRIGGERS ==========\n");
		rs = stmt.executeQuery("select schemaid, sys.systriggers.* from sys.systriggers");
		dumpResultSet(rs, idToNameMap, null);

		writeOut("\n========== SYSVIEWS ==========\n");
		rs = stmt.executeQuery("select compilationschemaid, sys.sysviews.* from sys.sysviews");
		dumpResultSet(rs, idToNameMap, null);

//IC see: https://issues.apache.org/jira/browse/DERBY-3877
//IC see: https://issues.apache.org/jira/browse/DERBY-3884
		writeOut("\n========== SYSROLES ==========\n");
		rs = stmt.executeQuery
			("select 'dummyFirstCol', " +
			 "roleid || '_' || grantee || '_' || grantor as rgd, " +
			 "roleid, grantee, grantor, withadminoption, isdef " +
			 "from sys.sysroles");
		dumpResultSet(rs, idToNameMap, null);

		stmt.close();
		rs.close();
		conn.commit();
		conn.close();
		return;

	}

	/* **********************************************
	 * isIgnorableSchema:
     * Returns true if the the schema is a "system" schema, vs. a user 
     * schema.  
	 * @param schemaName name of schema to check.
	 ****/
	private boolean isIgnorableSchema(String schemaName) {

        boolean ret = false;

        for (int i = ignorableSchemaNames.length - 1; i >= 0;)
        {
            if ((ret = ignorableSchemaNames[i--].equalsIgnoreCase(schemaName)))
                break;
        }

        return(ret);
	}

    private static final String[] ignorableSchemaNames = {
        "SYSIBM",
        "SYS",
        "SYSVISUAL",
        "SYSCAT",
        "SYSFUN",
        "SYSPROC",
        "SYSSTAT",
        "NULLID",
        "SYSCS_ADMIN",
        "SYSCS_DIAG",
        "SYSCS_UTIL",
        "SQLJ"};

	/* **********************************************
	 * dumpResultSet:
	 * Iterates through the received result set and
	 * dumps ALL columns in ALL rows of that result
	 * set to output.  Since no order is guaranteed
	 * in the received result set, we have to generate
	 * unique "ids" for each row in the result, and
	 * then use those ids to determine what order the
	 * rows will be output.  Failure to do so will
	 * lead to diffs in the test for rows that occur
	 * out of order.  The unique id's must NOT
	 * depend on system-generated id's, as the
	 * latter will vary for every run of the test,
	 * and thus will lead to different orderings
	 * every time (which we don't want).
	 *
	 * @param rs The result set that is being dumped.
	 * @param idToNameMap Mapping of various ids to
	 *  object names; used in forming unique ids.
	 * @param conn Connection from which the result set
	 *  originated.
	 ****/

	private void dumpResultSet (ResultSet rs,
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
		HashMap<String, String> idToNameMap, Connection conn)
		throws Exception
	{

		// We need to form unique names for the rows of the
		// result set so that we can preserve the order of
		// the output and avoid diffs with a master.  This is
		// because a "select *" doesn't order rows--and even
		// though the schema for two databases might be the
		// same (i.e. the system tables contain all of the same
		// information) there's nothing to say the various rows in
		// the respective system tables will be the same (they
		// usually are NOT).  While system id's automatically
		// give us uniqueness, we can NOT order on them because
		// they vary from database to database; so, we need
		// to use something constant across the databases,
		// which is why we use object names.
		StringBuffer uniqueName = new StringBuffer();

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
		TreeMap<String, ArrayList<String>> orderedRows =
                new TreeMap<String, ArrayList<String>>();
		ArrayList<String> rowValues = new ArrayList<String>();
		ArrayList<String> duplicateRowIds = new ArrayList<String>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int cols = rsmd.getColumnCount();
		while (rs.next()) {

			for (int i = 1; i <= cols; i++) {

				String colName = rsmd.getColumnName(i);
				String value = rs.getString(i);
				String mappedName = idToNameMap.get(value);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

				if ((colName.indexOf("SCHEMAID") != -1) &&
					(mappedName != null) &&
					((mappedName.indexOf("SYS") != -1) ||
                     (isIgnorableSchema(mappedName))))
                {
				// then this row of the result set is for a system
				// object, which will always be the same for the
				// source and new database, so don't bother dumping
				// them to the output file (makes the test less
				// like to require updates when changes to database
				// metadata for system objects are checked in).
					rowValues = null;
					break;
				}
//IC see: https://issues.apache.org/jira/browse/DERBY-335
				else if (colName.equals("JAVACLASSNAME") && (value != null) &&
					(value.indexOf("org.apache.derby") != -1) &&
					(value.indexOf(".util.") == -1)) {
				// this is a -- hack -- to see if the alias is a
				// a system alias, needed because aliases
				// (other than stored procedures) do not have
				// an associated schema).
					rowValues = null;
					break;
				}

				if (i == 1)
				// 1st column is just for figuring out whether
				// to dump this row; no need to actually include
				// it in the results.
					continue;


				String uniquePiece;
//IC see: https://issues.apache.org/jira/browse/DERBY-3877
//IC see: https://issues.apache.org/jira/browse/DERBY-3884

				if (colName.equals("RGD")) {
					// Role Grant Descriptor: synthetic unique column, see query
					// from SYS.SYSROLES.
					uniquePiece = value;
				} else {
					uniquePiece = dumpColumnData(colName,
												 value, mappedName, rowValues);
				}


				if (colName.equals("DEPENDENTID")) {
				// Special case: rows in the "DEPENDS" table
				// don't have unique ids or names; we have to
				// build one by extracting information indirectly.
					String hiddenInfo = getDependsData(rs, conn,
						idToNameMap);
					if (hiddenInfo.indexOf("SYS_OBJECT") != -1) {
					// this info is for a system object, so
					// ignore it.
						rowValues = null;
						break;
					}
					uniqueName.append(hiddenInfo);
					// Include the hidden data as part of the
					// output.
					rowValues.add(hiddenInfo);
				}

		 		if (uniquePiece != null)
					uniqueName.append(uniquePiece);

				if (colName.equals("STMTNAME") &&
				  (value.indexOf("TRIGGERACTN") != -1))
				// Special case: can't use statement name, because
				// the entire statement may be automatically generated
				// in each database (to back a trigger), so the name
				// in which case the generated name will be different
				// every time; but filtering out the name means
				// we have no other guaranteed unique 'id' for
				// ordering.  So, just take "text" field, and
				// design test db so that no two triggers have the
				// same text value.
				uniqueName.append(rs.getString(6));

			}

			if (rowValues != null) {

				if (duplicateRowIds.contains(uniqueName.toString()))
				// then we've already encountered this row id before;
				// to preserve ordering, use the entire row as an
				// id.
					handleDuplicateRow(rowValues, null, orderedRows);
				else {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
					ArrayList<String> oldRow = orderedRows.put(
						uniqueName.toString(), rowValues);
					if (oldRow != null) {
					// Duplicate row id.
						duplicateRowIds.add(uniqueName.toString());
						// Delete the row that has the duplicate row id.
							orderedRows.remove(uniqueName.toString());
						handleDuplicateRow(rowValues, oldRow, orderedRows);
					}
				}
			}

			uniqueName = new StringBuffer();
			rowValues = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

		}

		// Now, print out all of the data in this result set
		// using the order of the unique names that we created.
        Set<String> objectNames = orderedRows.keySet();
        for (String row : objectNames) {
            ArrayList<String> colData = orderedRows.get(row);
            for (int i = 0; i < colData.size(); i++)
                writeOut((String)colData.get(i));
            writeOut("----");
        }

		orderedRows = null;
		rs.close();

	}

	/* **********************************************
	 * dumpColumnData:
	 * Stores the value for a specific column of
	 * some result set.  If the value needs to
	 * be filtered (to remove system-generated ids
	 * that would otherwise cause diffs with the
	 * master), that filtering is done here.
	 * @param colName Name of the column whose value we're
	 *  writing.
	 * @param value Value that we're writing.
	 * @param mappedName: Name corresponding to the value,
	 *  for cases where the value is actually an
	 *  object id (then we want to write the name
	 *  instead).
	 * rowValues a list of column values for the
	 *  current row of the result set.
	 * @return The (possibly filtered) value of the
	 *  received column has been added to the
	 *  "rowVals" array list, and the corresponding
	 *  piece of the row's unique name has been
	 *  returned, if one exists.
	 ****/

	private String dumpColumnData(String colName,
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
		String value, String mappedName, ArrayList<String> rowVals)
	{

		if (mappedName == null) {
		// probably not an id.
			if (colName.equals("CONGLOMERATENUMBER") ||
				colName.equals("GENERATIONID"))
			// special case: these numbers aren't ids per
			// se, but they are still generated by the system,
			// and will cause diffs with the master; so, ignore
			// them.
				rowVals.add("<systemnumber>");
			else if (colName.equals("AUTOINCREMENTVALUE"))
			// special case: new database won't have any data,
			// old will, so unless we filter this out, we'll
			// get a diff.
				rowVals.add("<autoincval>");
			else if (colName.equals("VALID"))
			// special case: ignore whether or not stored
			// statements are valid (have been compiled)
			// since it depends on history of database,
			// which we can't duplicate.
				rowVals.add("<validityflag>");
			else if (value != null) {
				if (looksLikeSysGenName(value)) {
					if (columnHoldsObjectName(colName))
						rowVals.add("<systemname>");
					else {
					// looks like a sys gen name, but's actually a VALUE.
						rowVals.add(value);
						return value;
					}
				}
				else if (looksLikeSysGenId(value))
					rowVals.add("<systemid>");
				else {
					rowVals.add(value);
					if (columnHoldsObjectName(colName))
					// if it's a name, we need it as part of
					// our unique id.
						return value;
				}
			}
			else
			// null value.
				rowVals.add(value);
		}
		else {
		// it's an id, so write the corresponding name.
			if (!isSystemGenerated(mappedName)) {
			// Not an id-as-name, so use it as part of our unique id.
				rowVals.add(mappedName);
				return mappedName;
			}
			else
				rowVals.add("<systemname>");
		}

		// If we get here, we do NOT want the received value
		// to be treated as part of this row's unique name.
		return null;

	}

	/* **********************************************
	 * handleDuplicateRow:
	 * If we get here, then despite our efforts (while
	 * dumping the system catalogs for a database), we
	 * still have a duplicate row id.  So, as a last
	 * resort we just use the ENTIRE row as a 'row id'.
	 * In the rare-but-possible case that the entire
	 * row is a duplicate (as can happen with the
	 * SYSDEPENDS table), then we tag a simple number
	 * onto the latest row's id, so that the row will
	 * still show up multiple times--and since the rows
	 * are identical, it doesn't matter which comes
	 * 'first'.
	 * @param newRow The most recently-fetched row from
	 *  the database system catalogs.
	 * @param oldRow The row that was replaced when the
	 *  newRow was inserted (because they had the
	 *  same row id), or "null" if we were already
	 *  here once for this row id, and so just want
	 *  insert a new row.
	 * @param orderedRows The ordered set of rows, into
	 *  which oldRow and newRow need to be inserted.
	 * @return oldRow and newRow have been inserted
	 *  into orderedRows, and each has a (truly)
	 *  unique id with it.
	 ****/

	private void handleDuplicateRow(
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
		ArrayList<String> newRow, ArrayList<String> oldRow,
		TreeMap<String, ArrayList<String>> orderedRows)
	{

		// Add the received rows (old and new) with
		// unique row ids.

		StringBuffer newRowId = new StringBuffer();
		for (int i = 0; i < newRow.size(); i++)
			newRowId.append((String)newRow.get(i));

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
		ArrayList<String> obj = orderedRows.put(newRowId.toString(), newRow);
		if (obj != null)
		// entire row is a duplicate.
			orderedRows.put(newRowId.toString() + 
				duplicateCounter++, newRow);

		if (oldRow != null) {

			StringBuffer oldRowId = new StringBuffer();
			for (int i = 0; i < oldRow.size(); i++)
				oldRowId.append((String)oldRow.get(i));

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
			obj = orderedRows.put(oldRowId.toString(), oldRow);
			if (obj != null)
			// entire row is a duplicate.
				orderedRows.put(oldRowId.toString() +
					duplicateCounter++, oldRow);
		}

		return;

	}

	/* **********************************************
	 * createDBFromDDL:
	 * Read from the given script and use it to create
	 * a new database of the given name.
	 * @param newDBName Name of the database to be created.
	 * @param scriptName Name of the script containing the
	 *  DDL from which the new database will be created.
	 * @return New database has been created from
	 *   the script; any commands in the script that
	 *   failed to execute have been echoed to output.
	 ****/

	private void createDBFromDDL(String newDBName,
		String scriptName) throws Exception
	{

		System.out.println("\n\nCreating database '" + newDBName +
			"' from ddl script '" + scriptName + "'");

		Connection conn = DriverManager.getConnection(
//IC see: https://issues.apache.org/jira/browse/DERBY-3877
//IC see: https://issues.apache.org/jira/browse/DERBY-3884
				"jdbc:derby:" + newDBName +
				";create=true;user=app;password=apppw" + territoryBased);

        runDDL( conn, scriptName );
//IC see: https://issues.apache.org/jira/browse/DERBY-6661

		conn.close();

		return;
	}

	/* **********************************************
	 * runDDL:
	 * Run an sql script.
	 * @param conn database connection
	 * @param scriptName Name of the script
	 ****/

	static  void runDDL( Connection conn, String scriptName)
        throws Exception
	{
		Statement stmt = conn.createStatement();
		BufferedReader ddlScript =
			new BufferedReader( new FileReader( scriptName ) );

		for (String sqlCmd = ddlScript.readLine(); sqlCmd != null;
			sqlCmd = ddlScript.readLine()) {

			if (sqlCmd.indexOf("--") == 0)
			// then this is a script comment; ignore it;
				continue;
			else if (sqlCmd.trim().length() == 0)
			// blank line; ignore it.
				continue;

			// Execute the command.
			if ((sqlCmd.charAt(sqlCmd.length()-1) == TEST_DELIMITER)
			  || (sqlCmd.charAt(sqlCmd.length()-1) == ';'))
			// strip off the delimiter.
				sqlCmd = sqlCmd.substring(0, sqlCmd.length()-1);

			try {
				stmt.execute(sqlCmd);
			} catch (Exception e) {
				System.out.println("FAILED: to execute cmd " +
					"from DDL script:\n" + sqlCmd + "\n");
				System.out.println(e.getMessage());
			}
		}

		// Cleanup.
		ddlScript.close();
		stmt.close();
	}

	/* **********************************************
	 * writeOut:
	 * Write the received string to some output.
	 * @param str String to write.
	 ****/

	private static void writeOut(String str) {

		System.out.println(str);
		return;

	}

	/* **********************************************
	 * loadIdMappings:
	 * Load mappings of object ids to object names
	 * for purposes of having meaningful output
	 * and for creating unique ids on the rows of
	 * the system catalogs.
	 * @param stmt Statement on a connection to the
	 *  database being examined.
	 * @param conn Connection to the database being
	 *   examined.
	 * @return A HashMap with all relevant id-to-
	 *  name mappings has been returned.
	 ****/

    private HashMap<String, String> loadIdMappings(Statement stmt)
            throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

		HashMap<String, String> idToNameMap = new HashMap<String, String>();

		// Table ids.
		ResultSet rs = stmt.executeQuery(
			"select tableid, tablename from sys.systables");
		while (rs.next())
			idToNameMap.put(rs.getString(1), rs.getString(2));

		// Schema ids.
		rs = stmt.executeQuery(
			"select schemaid, schemaname from sys.sysschemas");
		while (rs.next())
			idToNameMap.put(rs.getString(1), rs.getString(2));

		// Constraint ids.
		rs = stmt.executeQuery(
			"select constraintid, constraintname from " +
			"sys.sysconstraints");
		while (rs.next())
			idToNameMap.put(rs.getString(1), rs.getString(2));

		return idToNameMap;

	}

	/* **********************************************
	 * getDependsData:
	 * Forms a string containing detailed information
	 * about a row in the SYSDEPENDS table, and returns
	 * that string.
	 * @param rs Result set with SYSDEPENDS rows; current
	 *  row is the one for which we're getting the
	 *  data.
	 * @param conn Connection to the database being
	 *   examined.
	 * @param idToNameMap mapping of object ids to names
	 *  for the database in question.
	 * @return Schema, type and name of both the Provider
	 *   and the Dependent for the current row of
	 *   SYSDEPENDS have been returned as a string.
	 ****/

	private String getDependsData(ResultSet rs,
		Connection conn, HashMap idToNameMap)
		throws Exception
	{

		DependableFinder dep =
			(DependableFinder)rs.getObject(3);

		DependableFinder prov =
			(DependableFinder)rs.getObject(5);

		String depType = dep.getSQLObjectType();
		String provType = prov.getSQLObjectType();

		Statement dependsStmt = conn.createStatement();
		StringBuffer dependsData = new StringBuffer();
		dependsData.append(getHiddenDependsData(depType,
			rs.getString(2), dependsStmt, idToNameMap));
		dependsData.append(" -> ");
		dependsData.append(getHiddenDependsData(provType,
			rs.getString(4), dependsStmt, idToNameMap));

		return dependsData.toString();

	}

	/* **********************************************
	 * getHiddenDependsData:
	 * Returns a string containing the schema and
	 * name of the object having the received id.
	 * All object ids received by this message come
	 * from rows of the SYSDEPENDS table.
	 * @param type Type of the object that has the received
	 *   object id.
	 * @param id Id of the object in question.
	 * @param stmt Statement from the database in question.
	 * @param idToNameMap mapping of ids to names for
	 *  the database in question.
	 * @isProvider True if we're getting data for a
	 *  Provider object; false if we're getting data for
	 *  a Dependent object.
	 * @return Schema, type, and name for the object with
	 *   the received id have been returned as a string.
	 ****/

	private String getHiddenDependsData(String type,
		String id, Statement pStmt, HashMap idToNameMap)
		throws Exception
	{

		ResultSet rs = null;
		if (type.equals("Constraint")) {
			rs = pStmt.executeQuery(
				"select schemaid, constraintname from " +
				"sys.sysconstraints where " +
				"constraintid = '" + id + "'");
		}
		else if (type.equals("StoredPreparedStatement")) {
			rs = pStmt.executeQuery(
				"select schemaid, stmtname from " +
				"sys.sysstatements where stmtid = '" +
				id + "'");
		}
		else if (type.equals("Trigger")) {
			rs = pStmt.executeQuery(
				"select schemaid, triggername from " +
				"sys.systriggers where triggerid = '" +
				id + "'");
		}
		else if (type.equals("View") || type.equals("Table")
		  || type.equals("ColumnsInTable")) {
			rs = pStmt.executeQuery(
				"select schemaid, tablename from " +
				"sys.systables where tableid = '" +
				id + "'");
		}
		else if (type.equals("Conglomerate")) {
			rs = pStmt.executeQuery(
				"select schemaid, conglomeratename from " +
				"sys.sysconglomerates where conglomerateid = '" +
				id + "'");
		}
		else {
			System.out.println("WARNING: Unexpected " +
				"dependent type: " + type);
			return "";
		}

		if (rs.next()) {
			String schema = (String)idToNameMap.get(rs.getString(1));
			if (isIgnorableSchema(schema))
			// system object (so we want to ignore it); indicate
			// this by returning the string "SYS_OBJECT".
				return "SYS_OBJECT";
			StringBuffer result = new StringBuffer();
			result.append("<");
			result.append(type);
			result.append(">");
			result.append(schema);
			result.append(".");
			if (isSystemGenerated(rs.getString(2)))
				result.append("<sysname>");
			else
				result.append(rs.getString(2));
			return result.toString();
		}

		return "";

	}

	/* **********************************************
	 * deleteDB:
	 * Deletes the database with the received name
	 * from the test directory.
	 * @param dbName Name of the database to be deleted.
	 * @return Database has been completely deleted;
	 *   if deletion failed for any reason, a message
	 *   saying so has been printed to output.
	 ****/

	private void deleteDB(String dbName)
		throws Exception
	{

		// Get the full path.
		String deletePath = (new
			File(dbPath + separator + dbName)).getAbsolutePath();

		// Have to shut it down before we can delete it.
		try {
			Connection conn =
				DriverManager.getConnection("jdbc:derby:" + 
//IC see: https://issues.apache.org/jira/browse/DERBY-3877
//IC see: https://issues.apache.org/jira/browse/DERBY-3884
					deletePath + ";shutdown=true;user=app;password=apppw");
			conn.close();
		} catch (SQLException se) {
		// shutdown exception.
		}

		File f = new File(deletePath);
		if (!f.exists()) 
		// nothing to do.
			return;

		File [] files = f.listFiles();
		for (int i = 0; i < files.length; i++)
			deleteFile(files[i]);

		if (!f.delete()) {
		// still failed.
			System.out.println("ERROR: deleting: " +
				f.getName());
		}

		// And finally, delete the CSJARS directory,
		// if there is one.
		deleteFile(new File(System.getProperty("user.dir") +
			separator + "CSJARS"));

		System.out.println("Database '" + dbName + "' deleted.");
		return;

	}

	/* **********************************************
	 * deleteFile:
	 * Delete everything in a given directory, then
	 * delete the directory itself (recursive).
	 * @param aFile File object representing the directory
	 *  to be deleted.
	 * @return the directory corresponding to aFile
	 *  has been deleted, as have all of its contents.
	 ****/

	private void deleteFile(File aFile)
		throws Exception
	{

		if (!aFile.exists())
		// don't bother.
			return;

		if (aFile.delete())
		// just a file; we're done.
			return;

		// Otherwise, have to descend and delete all
		// files in this directory.
		File [] files = aFile.listFiles();
		if (files != null) {
			for (int i = 0; i < files.length; i++)
				deleteFile(files[i]);
		}

		// Now try to delete.
		if (!aFile.delete()) {
		// still failed.
			System.out.println("ERROR: deleting: " +
				aFile.getName());
		}

		return;

	}

	/* **********************************************
	 * renameDbLookLog:
	 * Checks if the logfile of dblook exists and
	 * tries to rename it to prevent possible 
	 * next tests from failing. The log should not be 
	 * deleted because the output may be examined in 
	 * case a test fails.
	 * The new name of dblook.log should be dblook_testname#.log,
	 * where # is a 'version' number. The 'version' number is
	 * needed because the same test may be run multiple
	 * times with different parameters.
	 * @param nameOfTest Name of the finished test.
	 ****/

	protected static void renameDbLookLog(String nameOfTest)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3458
		File dbLookTestLog = new File("dblook.log");
		if (dbLookTestLog.exists()) {
			int i = 0;
			String renamedLogName = nameOfTest + i + ".log";
			File renamedLog = new File(renamedLogName);
			while (renamedLog.exists()) {
				i++;
				renamedLogName = nameOfTest + i + ".log";
				renamedLog = new File(renamedLogName);
			}
			if (!dbLookTestLog.renameTo(renamedLog)) {
				System.out.println("Failed to rename dblook.org to " + 
						renamedLogName);
			}
		}
	}

	/* **********************************************
	 * dumpFileToSysOut:
	 * Checks to see if the received file is empty,
	 * and prints a message saying so.
	 * @param fName Name of the file to be written to output.
	 * @return The contents of the specified file have
	 *   been written to System.out.
	 ****/

	private void dumpFileToSysOut(String fName) {

		try {

//IC see: https://issues.apache.org/jira/browse/DERBY-90
			BufferedReader dumpFile =
				new BufferedReader(new FileReader(fName));

			String line = dumpFile.readLine();
			if (line != null) {
				System.out.println("File " + fName + " was NOT " +
					"empty.  Contents are:\n" +
					"############## Begin File Contents ################\n");
				do {
					System.out.println(line);
					line = dumpFile.readLine();
				} while (line != null);
				System.out.println(
					"############## End File Contents ################");
			}
			else
				System.out.println("File " + fName + " was empty.");

			// Close the file.
			dumpFile.close();

		} catch (Exception e) {
			System.out.println("FAILED: to dump file '" + fName + "'");
			e.printStackTrace(System.out);
		}

		return;

	}

	/* **********************************************
	 * isSystemGenerated:
	 * Returns true if the received string looks like
	 * it is a system-generated string.  We assume
	 * it's system-generated if either 1) it starts
	 * with the letters "SQL", in which case it's a
	 * system-name, or 2) it has a dash in it, in which
	 * case it's a system id.
	 * @param str The string to check.
	 * @return True if we assume the string is system-
	 *  generated, false otherwise.
	 ****/

	private boolean isSystemGenerated(String str) {

		return (looksLikeSysGenName(str) ||
			looksLikeSysGenId(str));

	}

	/* **********************************************
	 * looksLikeSysGenName:
	 * See if the received string looks like it is
	 * a system-generated name.  There are two types
	 * of system-generated names: 1) visible names,
	 * which start with "SQL", and 2) hidden names,
	 * which exist for Stored Statements that are
	 * used to back triggers; these names start with
	 * "TRIGGERACTN_" and then have a UUID.
	 * NOTE: This test assumes that none of object names
	 * provided in "dblook_makeDB.sql" satisfy
	 * either of these conditions.  If they do, they
	 * will be filtered out of the test output.
	 * @param val The string value in question.
	 * @return True if the value looks like it is a system-
	 *  generated name; false otherwise.
	 ****/

	private boolean looksLikeSysGenName(String val) {

		return ((val != null) &&
			((val.trim().indexOf("SQL") == 0) || 			// case 1.
			((val.trim().indexOf("TRIGGERACTN_") == 0) &&	// case 2.
			(val.indexOf("-") != -1))));

	}

	/* **********************************************
	 * looksLikeSysGenId:
	 * See if the received string looks like it is
	 * a system-generated id (i.e. contains a dash (-)).
	 * NOTE: This test assumes that none of object names
	 * provided in "dblook_makeDB.sql" will contain
	 * dashes.  If they do, then they will be filtered out
	 * in the test output.
	 * @param val The string value in question.
	 * @return True if the value looks like it is a system-
	 *  generated id; false otherwise.
	 ****/

	private boolean looksLikeSysGenId(String val) {

		return ((val != null) && (val.indexOf("-") != -1));

	}

	/* **********************************************
	 * columnHoldsObjectName:
	 * Return true if the received column, which is from
	 * some system table, holds the _name_ of a database
	 * object (table, constraint, etc.).  Typically, we
	 * can just look for the keyword "NAME"; the exception
	 * is aliases, where the name is held in a column called
	 * ALIAS.
	 * @param colName Name of the column in question.
	 * @return True if the column name indicates that it
	 *  holds the _name_ of a database object; false if the
	 *  column name indicates that it holds something else.
	 ****/

	private boolean columnHoldsObjectName(String colName) {

		return (colName.equals("ALIAS") ||
				(colName.indexOf("NAME") != -1));

	}

	/* **********************************************
	 * printAsHeader:
	 * Print the received string to output as a
	 * header.
	 * @param str String to print.
	 ****/

	private void printAsHeader(String str) {

		writeOut("--\n*******************************************");
		writeOut(str);
		writeOut("*******************************************\n");
		return;

	}

    /**
     * Regression test case for DERBY-6387. Verify that triggers are returned
     * in the order in which they were created.
     */
    private void testDerby6387() throws Exception {
        // Create the test database.
        createTestDatabase("dblook_makeDB_derby6387.sql");

        // Run dblook on it.
        lookOne(testDBName);

        // Check that the error log was empty.
        dumpFileToSysOut("dblook.log");

        // Check the dblook output. Before DERBY-6387 was fixed, TR24 was
        // first in the output. It should be last.
        dumpFileToSysOut(testDBName + ".sql");
    }
}
