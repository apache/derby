/*

 Derby - Class org.apache.derbyTesting.system.nstest.tester.BackupRestoreReEncryptTester

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

package org.apache.derbyTesting.system.nstest.tester;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derbyTesting.system.nstest.NsTest;

/**
 * BackupRestoreReEncryptTester: The thread that invokes the
 * Backup/Restore/Re-Encrypt functions. By default one instance of this thread
 * is always started, unless the System property 'derby.nstest.backupRestore'
 * set to 'false'
 */
public class BackupRestoreReEncryptTester extends TesterObject {
	String logFile = "backup.log";

	PrintWriter logger = null;

	// *******************************************************************************
	//
	// Constructor. Get's the name of the thread running this for use in
	// messages
	//
	// *******************************************************************************
	public BackupRestoreReEncryptTester(String name) throws IOException {
		super(name);
		logger = new PrintWriter(new FileWriter(logFile));

	}

	// *********************************************************************************
	//
	// This starts the acutal test operations. Overrides the startTesting() of
	// parent.
	// Tester3 profile -
	// Query only kind of client that deals with a large result
	// set based on a select query that returns a large number of
	// rows (stress condition). Connection is closed after each
	// query. The query will need to run in a DIRTY_READ mode, i.e.
	// READ UNCOMMITTED isolation level. We work over the untouched
	// portion of rows in the table (i.e. serialkey 1 to NUM_UNTOUCHED_ROWS)
	//
	// *********************************************************************************
	public void startTesting() {
        
		// The following loop will be done nstest.MAX_ITERATIONS times after
		// which we exit the thread
		// Note that a different connection is used for each operation. The
		// purpose of this client is to work on a large set of data as defined 
        // by the parameter NUM_HIGH_STRESS_ROWS
		// This thread could be made to pause (sleep) for a bit between each
		// iteration.
		for (int i = 0; i < NsTest.MAX_ITERATIONS; i++) {
			log(getTimestamp() + " Thread " + getThread_id() + " starting");
			String message = "";
			// Get the connection. It will be closed at the end of the loop
			connex = getConnection();
			if (connex == null) {
				NsTest.logger.println("FAIL: " + getThread_id()
						+ " could not get the database connection");
				return; // quit
			}

			// set isolation level to Connection.TRANSACTION_READ_UNCOMMITTED to
			// reduce number of deadlocks/lock issues
			setIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED);

			// Now select nstest.NUM_HIGH_STRESS_ROWS number of rows
			try {
				doBackup();

			} catch (Exception e) {
				message = getTimestamp() + "FAILED - BackUp thread doBackup"
						+ getThread_id() + " threw " + e;
				NsTest.logger.println(message);
				log(message);
				printException("call to doBackup() in BackupThread ", e);
                if ( NsTest.justCountErrors() ) { NsTest.printException( BackupRestoreReEncryptTester.class.getName(), e ); }
				else { e.printStackTrace( NsTest.logger ); }
			}

			try {
				doRestoreandReEncrypt();

			} catch (SQLException e) {
                if ( NsTest.justCountErrors() ) { NsTest.printException( BackupRestoreReEncryptTester.class.getName(), e ); }
				else { e.printStackTrace( NsTest.logger ); }
				NsTest.logger
						.println("FAILED at doRestoreandReEncrypt() - BackUp thread "
								+ getThread_id() + " threw " + e);
				printException(
						"call to doRestoreandReEncrypt() in BackupThread ", e);
				log(getTimestamp()
						+ " call to doRestoreandReEncrypt() in BackupThread FAILED "
						+ e.getSQLState() + " " + e);

				e.printStackTrace( logger );
			}

			// close the connection
			closeConnection();
			try {
				log(getTimestamp() + " Thread " + getThread_id() + " sleeping");
				Thread.sleep(10 * 60000); // 10 minutes sleep before second
				// backup
			} catch (InterruptedException ie) {
				message = getTimestamp() + "FAILED - " + getThread_id()
						+ " Sleep interrupted " + ie;
				log(message);
			}

            // first check if there are still active tester threads, so 
            // we do not make backups on an unchanged db every 10 mins for
            // the remainder of MAX_ITERATIONS.
            if (NsTest.numActiveTestThreads() > 1)
            {
                log("active test threads > 1, backup will continue in 10 minutes");
                continue;
            }
            else
            {
                log("no more test threads, finishing backup also");
                break;
            }
    
		}// end of for (int i=0;...)

		NsTest.logger.println("Thread " + getThread_id() + " is now terminating");

	}// end of startTesting()

	public void doBackup() throws SQLException {
		log("--------------------- B A C K U P  S E C T I O N  B E G I N ------------------------");
		CallableStatement cs = connex
				.prepareCall("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT(?, ?)");
		cs.setString(1, NsTest.BACKUPDIR);
		cs.setInt(2, 1);
		long start = System.currentTimeMillis();
		cs.execute();
		cs.close();
		long end = System.currentTimeMillis();
		log(getTimestamp() + " Backup completed successfully in "
				+ (end - start) / 100 + " seconds");
		try {
			String backupDbURL = NsTest.getDriverURL() + NsTest.BACKUPDIR
					+ File.separator + NsTest.dbName + ";" + NsTest.bootPwd;
			// Consistency check not required everytime
			doConsistCheck(backupDbURL, "BACKUP");

		} catch (Exception e) {
			String message = getTimestamp()
					+ "FAILED - BackUp thread doConsistCheck() "
					+ getThread_id() + " threw " + e;
			log(message);
			printException("call to doConsistCheck() in BackupThread ", e);
			e.printStackTrace( logger );
		}
		log("--------------------- B A C K U P  S E C T I O N  E N D ----------------------------");
	}

	public void doConsistCheck(String dbURL, String dbType) throws SQLException {
		/*
		 * SELECT schemaname, tablename,
		 * SYSCS_UTIL.SYSCS_CHECK_TABLE(schemaname, tablename) FROM
		 * sys.sysschemas s, sys.systables t WHERE s.schemaid = t.schemaid;
		 */
		Connection conn = DriverManager.getConnection(dbURL);
		Statement stmt = conn.createStatement();
		long start = System.currentTimeMillis();
		ResultSet rs = stmt
				.executeQuery("SELECT schemaname, tablename,	SYSCS_UTIL.SYSCS_CHECK_TABLE(schemaname, tablename)	FROM sys.sysschemas s, sys.systables t	WHERE s.schemaid = t.schemaid");
		while (rs.next()) {
			// Iterate thru ResultSet
			rs.getString(1);
			rs.getString(2);
			rs.getString(3);
		}
		long end = System.currentTimeMillis();
		log(getTimestamp() + " Consistency Check on the " + dbType
				+ " database Completed successfully " + (end - start) / 100
				+ " seconds");
		stmt.close();
		conn.close();
		shutDownDB(dbURL, dbType);
	}

	public void doRestoreandReEncrypt() throws SQLException {
		log("--------------------- R E S T O R E   S E C T I O N  B E G I N ------------------------");
		String dbType = "RESTORED";
		Connection conn = null;

		long newKey = System.currentTimeMillis();
		String restoreDbURL = NsTest.getDriverURL() + NsTest.RESTOREDIR
				+ File.separator + NsTest.dbName;
		String dbUrl = restoreDbURL + ";" + NsTest.bootPwd + ";restoreFrom="
				+ NsTest.BACKUPDIR + File.separator + NsTest.dbName;
		try {
			conn = DriverManager.getConnection(dbUrl);
			log(getTimestamp() + " Database restored successfully " + dbUrl);
		} catch (SQLException e) {
			log(getTimestamp() + " FAILURE ! to restore database " + dbUrl);
			e.printStackTrace( logger );

		}

		// Consistency check not required everytime
		if ( conn != null ) { conn.close(); }
		dbUrl = restoreDbURL + ";" + NsTest.bootPwd;
		doConsistCheck(dbUrl, dbType);
		// DERBY-1737, hence create a new connection
		log("--------------------- R E S T O R E   S E C T I O N  E N D ----------------------------");

		conn = DriverManager.getConnection(dbUrl);
		// Disable log archival
		// call SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1);
		CallableStatement cs = conn
				.prepareCall("CALL SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(?)");
		cs.setInt(1, 1);
		cs.execute();
		conn.close();
		log(getTimestamp()
				+ " Disable log archival mode to enable re-encryption " + dbUrl);
		shutDownDB(restoreDbURL, dbType);
		log("--------------------- ENCRYPT AND RECONNECT  S E C T I O N  BEGIN ------------------------");
		String encryptDbURL = restoreDbURL + ";dataEncryption=true;";

		// Try Re-Encrypting Now
		encryptDbURL += ";" + NsTest.bootPwd + ";newBootPassword=" + newKey;

		long start = System.currentTimeMillis();
		log(getTimestamp() + " Encrypting database, url = " + encryptDbURL);
		conn = DriverManager.getConnection(encryptDbURL);
		conn.close();
		long end = System.currentTimeMillis();
		log(getTimestamp()
				+ " Re-encryption completed on restored database in "
				+ (end - start) / 100 + " seconds, url = " + encryptDbURL);
		// Shutdown the db
		dbType = "ENCRYPTED";
		shutDownDB(restoreDbURL, dbType);
		// Attempt to connect with old key should fail
		try {
			conn = DriverManager.getConnection(dbUrl);
			log(getTimestamp()
					+ " FAILURE ! - Attempt to boot with old password/url should have failed, url ="
					+ dbUrl);
		} catch (SQLException sqe) {
			if ((sqe.getSQLState().equalsIgnoreCase("XJ040"))
					|| (sqe.getSQLState().equalsIgnoreCase("XBM06"))) {
				log(getTimestamp()
						+ " PASS - Unsuccessful attempt to boot with old password/url, "
						+ dbUrl);
			} else {
				throw sqe;
			}
		}
		/*
		 * A Shutdown is not needed, since the part gets exected only when a
		 * unsuccessful attempt is made to boot the db with an old password
		 */
		// shutDownDB(restoreDbURL, dbType);
		log("--------------------- ENCRYPT AND RECONNECT  S E C T I O N  END --------------------------");
	}

	private void shutDownDB(String dbURL, String dbType) throws SQLException {
		Connection conn = null;
		dbURL = dbURL + ";shutdown=true";
		try {
			conn = DriverManager.getConnection(dbURL);

		} catch (SQLException sqe) {
			if (conn != null)
				conn.close();
			if (!sqe.getSQLState().equalsIgnoreCase("08006")) {
				throw sqe;
			} else {
				log(getTimestamp() + " " + dbType
						+ " database shutdown completed, url = " + dbURL);
			}
		}
	}

	public void log(String msg) {
		logger.write(msg + "\n");
		logger.flush();

		NsTest.logger.println(msg);
	}
}
