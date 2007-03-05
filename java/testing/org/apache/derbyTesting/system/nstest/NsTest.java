/*

 Derby - Class org.apache.derbyTesting.system.nstest.NsTest

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
package org.apache.derbyTesting.system.nstest;

import java.util.Properties;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.io.IOException;
import java.sql.Connection;

import org.apache.derbyTesting.system.nstest.init.DbSetup;
import org.apache.derbyTesting.system.nstest.init.Initializer;
import org.apache.derbyTesting.system.nstest.init.NWServerThread;
import org.apache.derbyTesting.system.nstest.tester.BackupRestoreReEncryptTester;
import org.apache.derbyTesting.system.nstest.tester.Tester1;
import org.apache.derbyTesting.system.nstest.tester.Tester2;
import org.apache.derbyTesting.system.nstest.tester.Tester3;
import org.apache.derbyTesting.system.nstest.utils.MemCheck;

/**
 * NsTest - the main class to start the tests The main test settings are as
 * follows: INIT_THREADS = Initializer threads MAX_INITIAL_ROWS = Initial set of
 * rows inserted before test begins MAX_ITERATIONS = Each client does these many
 * transactions in the test MAX_LOW_STRESS_ROWS = Num of rows worked over in a
 * transaction MAX_OPERATIONS_PER_CONN = Num of transaction batches made by a
 * client before closing the connection NUMTESTER1 = Number of Tester1 testers
 * NUMTESTER2 = Number of Tester2 testers NUMTESTER3 = Number of Tester3 testers
 * NUM_HIGH_STRESS_ROWS = Maximum rows to be selected NUM_UNTOUCHED_ROWS =
 * Number of rows on which Update/Delete operations are not performed
 *
 * START_SERVER_IN_SAME_VM= Set to true/false to start Network Server in the
 * same VM.
 *
 * The System property 'derby.nstest.backupRestore' can be set to false for
 * turning off Backup/Restore/Re-Encryption.
 */

public class NsTest extends Thread {

	public static final String dbName = "nstestdb";

	public static final String user = "nstest";

	public static final String password = "nstest";

	public static final String clientURL = "jdbc:derby://localhost:1900/";

	public static final String embedURL = "jdbc:derby:";

	public static final String dataEncypt = "dataEncryption=true";

	public static final String bootPwd = "bootPassword=12345678";

	public static final String clientDbURL = new String(clientURL + dbName
			+ ";create=true");

	public static final String retrieveMessagePart = "retrieveMessagesFromServerOnGetMessage=true;";

	public static final String embedDbURL = new String(embedURL + dbName
			+ ";create=true");

	public static boolean embeddedMode = false; // true is to run the test in

	// embedded mode

	public static final String driver = new String(
	"org.apache.derby.jdbc.ClientDriver");

	public static final String embedDriver = new String(
	"org.apache.derby.jdbc.EmbeddedDriver");

	public static Properties prop = new Properties();

	public static int INIT = 0;

	public static int TESTER1 = 1;

	public static int TESTER2 = 2;

	public static int TESTER3 = 3;

	public static int BACKUP = 4;

	public static String BACKUPDIR = "backupdir";

	public static String RESTOREDIR = "restoredir";

	public static boolean START_SERVER_IN_SAME_VM = false;// If the server

	// also needs to be
	// started as a
	// thread

	public static boolean AUTO_COMMIT_OFF = false; // true is autocommit off

	public static boolean CREATE_DATABASE_ONLY = false;

	public static boolean schemaCreated = false; // initially schema is

	// assumed to not exist

	// *********Uncomment this block for a small test scenario, comment it for
	// full testing
	/*
	 * public static int INIT_THREADS = 3; //keep this low to avoid deadlocks
	 * public static int MAX_INITIAL_ROWS = 150; //for a small test public
	 * static int MAX_ITERATIONS = 50; //for a small test public static int
	 * MAX_LOW_STRESS_ROWS = 10; //for a small test public static int
	 * MAX_OPERATIONS_PER_CONN = 10; //for a small test public static int
	 * NUMTESTER1 = 3; //for a small test public static int NUMTESTER2 = 4;
	 * //for a small test public static int NUMTESTER3 = 3; //for a small test
	 * public static int NUM_HIGH_STRESS_ROWS = 20; //for a small test public
	 * static int NUM_UNTOUCHED_ROWS = 50; //for a small test
	 */
	// ***End of small test scenario block
	// ****Comment this block for a small test scenario, uncomment it for full
	// testing
	public static int INIT_THREADS = 6; // keep this low to avoid deadlocks

	// public static int MAX_INITIAL_ROWS = 60000; //for network server mode
	public static int MAX_INITIAL_ROWS = 6000; // for network server mode

	public static int MAX_ITERATIONS = 2000; // Each client does these many

	// transactions in the test.

	// for network server mode
	public static int MAX_LOW_STRESS_ROWS = 30; // num of rows worked over in a

	// transaction

	// for network server mode
	public static int MAX_OPERATIONS_PER_CONN = 25; // num of transaction

	// batches made by a client

	// before closing the connection
	// for network server mode
	public static int NUMTESTER1 = 15; // for network server mode

	// ***public static int NUMTESTER1 = 45; //for embedded mode
	public static int NUMTESTER2 = 45; // for network server mode

	// ***public static int NUMTESTER2 = 135; //for embedded server mode
	public static int NUMTESTER3 = 10; // for network server mode

	// ***public static int NUMTESTER3 = 30; //for network server mode
	public static int NUM_HIGH_STRESS_ROWS = 25000; // for network server mode

	public static int NUM_UNTOUCHED_ROWS = 6000; // for network server mode

	// ***End of full test block

	// The following are to keep statistics of the number of
	// Insert/Updates/Deletes & Selects
	public static int numInserts = 0;

	public static int numUpdates = 0;

	public static int numDeletes = 0;

	public static int numSelects = 0;

	public static int numFailedInserts = 0;

	public static int numFailedUpdates = 0;

	public static int numFailedDeletes = 0;

	public static int numFailedSelects = 0;

	public static int numConnections = 0;

	public static int INSERT = 0;

	public static int UPDATE = 1;

	public static int DELETE = 2;

	public static int SELECT = 3;

	public static int FAILED_INSERT = 4;

	public static int FAILED_UPDATE = 5;

	public static int FAILED_DELETE = 6;

	public static int FAILED_SELECT = 7;

	public static int CONNECTIONS_MADE = 8;

	public static final String SUCCESS = " *** SUCCESS *** ";

	public static String driver_type = null;

	private int type;

	public static synchronized void addStats(int type, int addValue) {
		switch (type) {
		case 0:
			numInserts += addValue;
			break;
		case 1:
			numUpdates += addValue;
			break;
		case 2:
			numDeletes += addValue;
			break;
		case 3:
			numSelects += addValue;
			break;
		case 4:
			numFailedInserts += addValue;
			break;
		case 5:
			numFailedUpdates += addValue;
			break;
		case 6:
			numFailedDeletes += addValue;
			break;
		case 7:
			numFailedSelects += addValue;
			break;
		case 8:
			numConnections += addValue;
			break;
		}
	}

	NsTest(int ttype, int k) throws Exception {
		this.type = ttype; // INIT or TESTER1/2/3

		if (ttype == INIT)
			this.setName("InitThread " + k);
		else if ((ttype == TESTER1) || (ttype == TESTER2) || (ttype == TESTER3))
			this.setName("Thread " + k);
	}

	// ****************************************************************************
	//
	// main - will load the Derby embedded or client, invoke the
	// database setup, initialize the
	// tables and then kick off the test threads.
	//
	// ****************************************************************************
	public static void main(String[] args) throws SQLException, IOException,
	InterruptedException, Exception, Throwable {

		Connection conn = null;
		if (args.length == 1) {
			driver_type = args[0];
			if (!((driver_type.equalsIgnoreCase("DerbyClient"))
					|| (driver_type
							.equalsIgnoreCase("Embedded")))) {
				printUsage();
				return;
			}
			System.out.println("Test nstest starting....., using driver: "
					+ driver_type);
		} else {
			driver_type = "DerbyClient";
		}

		// Load the driver and get a connection to the database
		String jdbcUrl = "";
		try {
			if (driver_type.equalsIgnoreCase("Embedded")) {
				// System.out.println("Driver embedd : " + driver_type);
				System.out.println("Loading the embedded driver...");
				Class.forName(embedDriver).newInstance();
				jdbcUrl = embedDbURL + ";" + dataEncypt + ";" + bootPwd;
				embeddedMode = true;
			} else {
				System.out.println("Driver type : " + driver_type);
				System.out.println("Loading the Derby Client driver..."
						+ driver);
				Class.forName(driver).newInstance();
				System.out.println("Client Driver loaded");
				jdbcUrl = clientDbURL + ";" + dataEncypt + ";" + bootPwd;
			}
			if ((!embeddedMode) && START_SERVER_IN_SAME_VM) {
				startNetworkServer();
			}
			prop.setProperty("user", user);
			prop.setProperty("password", password);
			System.out
			.println("Getting a connection using the url: " + jdbcUrl);
			System.out.println("JDBC url= " + jdbcUrl);
			conn = DriverManager.getConnection(jdbcUrl, prop);

		} catch (SQLException sqe) {

			System.out.println("\n\n " + sqe + sqe.getErrorCode() + " "
					+ sqe.getSQLState());
			if ((sqe.getErrorCode() == -4499)
					|| sqe.getSQLState().equalsIgnoreCase("08001")) {
				System.out
				.println("\n Unable to connect, test cannot proceed. Please verify if the Network Server is started on port 1900.");
				// sqe.printStackTrace();
				return;
			}

		} catch (ClassNotFoundException cnfe) {
			System.out.println("Driver not found: " + cnfe.getMessage());
			cnfe.printStackTrace();
			return;

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Unexpected Failure");
			printException("nstest.main() method ==> ", e);
		}

		// create test schema if it does not already exist
		if (DbSetup.doIt(conn) == false) {
			System.out.println("Error in dbSetup, test will exit");
			System.exit(1);
		}

		// Note that the connection is still open, we can safely close it now
		try {
			conn.close();
		} catch (Exception e) {
			System.out
			.println("FAIL - Error closing the connection in nstest.main():");
			printException("Closing connection in nstest.main()", e);
		}

		// check memory in separate thread-- allows us to monitor usage during
		// database calls
		// 200,000 msec = 3min, 20 sec delay between checks
		System.out.println("Starting memory checker thread");
		MemCheck mc = new MemCheck(200000);
		mc.start();

		// Now populate the tables using INIT_THREADS number of threads only if
		// the schemaCreated
		// flag has not been set. If so, then we assume that some other thread
		// from possibly
		// another jvm reached here and has already created the schema and
		// loaded the tables.
		// Note that we kick off threads of this object type (nstest) and use
		// the run method to
		// do the work. The key to starting the init threads is the use of the
		// constructor
		// to indicate to the thread that it is an init thread. In this case, we
		// pass the
		// value INIT to the constructor and in the run method we go to the
		// right section of the
		// code based on what value is passed in. The other possible value that
		// a thread can get
		// is TESTER which indicates that these are the main test threads.

		if (NsTest.schemaCreated == false) {
			// Table was created by this object, so we need to load it
			System.out
			.println("Kicking off initialization threads that will populate the test table");
			NsTest initThreads[] = new NsTest[INIT_THREADS];

			for (int i = 0; i < INIT_THREADS; i++) {
				initThreads[i] = new NsTest(INIT, i);
				initThreads[i].start();
				sleep(3000);
			}

			// Wait for the init threads to finish and join back
			for (int i = 0; i < INIT_THREADS; i++) {
				initThreads[i].join();
			}
		}// end of if(nstest.schemaCreated==false)

		// For informational/debug purposes, print out whether this process
		// created the schema
		if (NsTest.schemaCreated) // true means that the schema was created by
			// another jvm
			System.out
			.println("Schema has already been created by another process!");

		// The following 2 lines are used when you want to only create the test
		// database that can be
		// used as a reference so that subsequent tests do not need to create
		// one of their own.
		// The CREATE_DATABASE_ONLY FLAG is set with the rest of the flags
		if (CREATE_DATABASE_ONLY) {
			System.out
			.println("Finished creating the database, TEST THREADS WILL NOT RUN!!");
			// Finally also stop the memory checker thread, else the test will
			// remain hung!
			mc.stopNow = true;
			mc.join();
			return;
		}

		// Table was created by some other object, so we assume it is already
		// loaded
		// Now kick off the actual test threads that will do the work for us.
		// Note that we use
		// the value TESTER when initializing the threads.
		// The total number of threads is NUMTESTER1+NUMTESTER2+NUMTESTER3
		System.out
		.println("Kicking off test threads that will work over the test table");

		int numTestThread = 0;
		int maxTestThreads = 1 + NUMTESTER1 + NUMTESTER2 + NUMTESTER3;
		NsTest testThreads[] = new NsTest[maxTestThreads];

		// This loop is made of 3 subloops that will initialize the required
		// amount of tester threads
		// It uses the numTestThread variable as the array index which gets
		// incremented in each subloop
		while (numTestThread < maxTestThreads) {
			String runBackup = System.getProperty("derby.nstest.backupRestore");
			// Check for property setting to decide the need for starting
			// BackupRestore thread
			if ((runBackup != null) && (runBackup.equalsIgnoreCase("false"))) {
				System.out.println("BackupRestore Thread not started...");
			} else {
				// Otherwise, start the BackupRestore Thread by default
				testThreads[numTestThread] = new NsTest(BACKUP, numTestThread);
				testThreads[numTestThread].start();
				numTestThread++;
			}

			for (int j = 0; j < NUMTESTER1; j++) {
				testThreads[numTestThread] = new NsTest(TESTER1, numTestThread);
				testThreads[numTestThread].start();
				sleep(3000);
				numTestThread++;
			}
			for (int j = 0; j < NUMTESTER2; j++) {
				testThreads[numTestThread] = new NsTest(TESTER2, numTestThread);
				testThreads[numTestThread].start();
				sleep(3000);
				numTestThread++;
			}
			for (int j = 0; j < NUMTESTER3; j++) {
				testThreads[numTestThread] = new NsTest(TESTER3, numTestThread);
				testThreads[numTestThread].start();
				sleep(3000);
				numTestThread++;
			}

		}

		// Wait for the init threads to finish and join back
		for (int j = 0; j < maxTestThreads; j++) {
			System.out.println("Waiting for thread " + j
					+ " to join back/finish");
			testThreads[j].join();
		}

		// Print statistics
		System.out.println("");
		System.out.println("STATISTICS OF OPERATIONS DONE");
		System.out.println("-----------------------------");
		System.out.println("");
		System.out.println("SUCCESSFUL: ");
		System.out.println("	Number of INSERTS = " + numInserts);
		System.out.println("	Number of UPDATES = " + numUpdates);
		System.out.println("	Number of DELETES = " + numDeletes);
		System.out.println("	Number of SELECTS = " + numSelects);
		System.out.println("");
		System.out.println("FAILED: ");
		System.out.println("	Number of failed INSERTS = " + numFailedInserts);
		System.out.println("	Number of failed UPDATES = " + numFailedUpdates);
		System.out.println("	Number of failed DELETES = " + numFailedDeletes);
		System.out.println("	Number of failed SELECTS = " + numFailedSelects);
		System.out.println("");
		System.out.println("  Note that this may not be the same as the server side connections made "
				+ "   to the database especially if connection pooling is employed");
		System.out.println("");
		System.out
		.println("NOTE: Failing operations could be because of locking issue that are "
				+ "directly related to the application logic.  They are not necessarily bugs.");

		// Finally also stop the memory checker thread
		mc.stopNow = true;
		mc.join();

		System.out
		.println("End of test nstest! Look for 'FAIL' messages in the output and derby.log");

	}// end of main

	// ****************************************************************************
	//
	// run() - the main workhorse method of the threads that will either
	// initialize
	// the table data or work over it as part of the test process.
	// Table data initialization threads are of the following type
	// Initializer -
	// Bulk Insert client type that deals with a large(stress)
	// number of rows with the connection being closed after the insert.
	// Max rows inserted is based on the parameter MAX_INITIAL_ROWS
	// Note that the run method will also instantiate tester objects of
	// different
	// types based on the following criteria
	// Tester1 -
	// The connection to the database is open forever. This client
	// will do Insert/Update/Delete and simple Select queries over
	// a small to medium set of data determined randomly as 1 row to
	// MAX_LOW_STRESS_ROWS. Autocommit is left on.
	// Tester2 -
	// The connection is frequently opened and closed based on
	// a random choice between 1 and MAX_OPERATIONS_PER_CONN number of
	// transaction batches committed by this client type. This client will
	// do Insert/Update/Delete and simple Select queries over a
	// small to medium set of data determined randomly as 1 row to
	// MAX_LOW_STRESS_ROWS.
	// Tester3 -
	// Query only kind of client that deals with a large result
	// set based on a select query that returns a large number of
	// rows (stress condition). Connection is closed after each
	// query. The query will need to run in a DIRTY_READ mode, i.e.
	// READ UNCOMMITTED isolation level. We work over the untouched
	// portion of rows in the table (i.e. serialkey 1 to NUM_UNTOUCHED_ROWS)
	//
	// The mix of the tester types is based on the parameters NUMTESTER1,
	// NUMTESTER2, NUMTESTER3.
	//
	//
	// ****************************************************************************
	public void run() {

		System.out.println(this.getName() + " is now running");

		if (this.type == INIT) {
			Initializer Init = new Initializer(this.getName());
			Init.startInserts(); // this method only returns when the thread
			// is done
		} else if (this.type == TESTER1) {
			Tester1 Tstr1 = new Tester1("Tester1" + this.getName());
			Tstr1.startTesting(); // this method only returns when the thread
			// is done
		} else if (this.type == TESTER2) {
			Tester2 Tstr2 = new Tester2("Tester2" + this.getName());
			Tstr2.startTesting(); // this method only returns when the thread
			// is done
		} else if (this.type == TESTER3) {
			Tester3 Tstr3 = new Tester3("Tester3" + this.getName());
			Tstr3.startTesting(); // this method only returns when the thread
			// is done
		} else if (this.type == BACKUP) {
			BackupRestoreReEncryptTester Tstr4 = null;
			try {
				Tstr4 = new BackupRestoreReEncryptTester(
						"BackupRestoreReEncrypt" + this.getName());
			} catch (IOException ioe) {
				System.out
				.println(ioe
						+ "=====> Unable to create backup log file, test cannot proceed ");
				ioe.printStackTrace();
				return;
			}
			Tstr4.startTesting();

		} else {
			System.out
			.println("FAIL: Invalid thread type, should be INIT or TESTERx or BACKUP");
			System.out.println("You should check the code and restart");
			return;
		}

		System.out.println(this.getName() + " finished and is now exiting");

	}// end of run()

	// ** This method abstracts exception message printing for all exception
	// messages. You may want to change
	// ****it if more detailed exception messages are desired.
	// ***Method is synchronized so that the output file will contain sensible
	// stack traces that are not
	// ****mixed but rather one exception printed at a time
	public static synchronized void printException(String where, Exception e) {
		if (e instanceof SQLException) {
			SQLException se = (SQLException) e;
			if (se.getSQLState() != null) { // SQLSTATE is NULL for a
				if (se.getSQLState().equals("40001"))
					System.out.println("deadlocked detected");
				if (se.getSQLState().equals("40XL1"))
					System.out.println(" lock timeout exception");
				if (se.getSQLState().equals("23500"))
					System.out.println(" duplicate key violation");
			}
			if (se.getNextException() != null) {
				String m = se.getNextException().getSQLState();
				System.out.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
			}
		}
		if (e.getMessage().equals(null)) {
			System.out.println("NULL error message detected");
			System.out.println("Here is the NULL exection - " + e.toString());
			System.out.println("Stack trace of the NULL exception - ");
			e.printStackTrace(System.out);
		}
		System.out.println("At this point - " + where
				+ ", exception thrown was : " + e.getMessage());

	}

	public static String getDriverURL() {
		if (driver_type.equalsIgnoreCase("DerbyClient")) {
			return clientURL;
		} else {
			return embedURL;
		}
	}

	public static void startNetworkServer() throws Exception {
		try {
			NWServerThread nsw = new NWServerThread("localhost", 1900);
			nsw.start();
			Thread.sleep(10000);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

	}

	public static void printUsage() {
		System.out.println("Usage:");
		System.out
		.println("java org.apache.derbyTesting.system.nstest.NsTest DerbyClient|Embedded");
		System.out.println("\nNo argument/Default value is 'DerbyClient'");
	}
}
