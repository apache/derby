/*

   Derby - Class org.apache.derby.impl.tools.dblook.Logs

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.tools.dblook;

import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.IOException;

import java.sql.SQLException;
import org.apache.derby.tools.dblook;

public class Logs {

	// Log file (for errors/warnings).
	private static PrintWriter logFile = null;

	// User-specified output file.
	private static PrintWriter ddlFile = null;

	// Statement delimiter.
	private static String stmtEnd;

	// Verbose mode?
	private static boolean verbose;

	// Did we write at least one message to the dblook file?
	private static boolean atLeastOneDebug;

	/* **********************************************
	 * initLogs:
	 * Prepare output streams and initialize state for
	 * handling dblook output.
	 * @param logFileName File for errors/warnings.
	 * @param ddlFileName File for generated DDL.
	 * @param appendLogs Whether or not to append to existing
	 *   log and ddl files.
	 * @param doVerbose verbose mode
	 * @param endOfStmt Statement delimiter.
	 * @return true if all state is initialized successfully;
	 *  false otherwise.
	 ****/

	public static boolean initLogs(String logFileName, String ddlFileName,
			boolean appendLogs, boolean doVerbose, String endOfStmt)
	{
		try {

			logFile = new PrintWriter(new FileOutputStream(logFileName, appendLogs));
			if (ddlFileName != null)
				ddlFile = new PrintWriter(new FileOutputStream(ddlFileName, appendLogs));
			verbose = doVerbose;
			stmtEnd = endOfStmt;
			atLeastOneDebug = false;
		}
		catch (IOException ioe)
		{
			System.out.println("Error initializing log file(s): " + ioe);
			return false;
		}

		return true;

	}


	/* **********************************************
	 * Method to report status info to the end-user.
	 * This information will be printed as SQL script
	 * comments, which means the messages must be
	 * preceded by a "--".  If the user specified a
	 * DDL file, then the message will be printed to
	 * that file; otherwise, it will be printed to
	 * the console.
	 * @param msg the information to print out.
	 ****/

	public static void report(String msg) {

		if (ddlFile == null)
			System.out.println("-- " + msg);
		else
			ddlFile.println("-- " + msg);

		return;

	}

	/* **********************************************
	 * Report a specific string to output.
	 * @param str The string to report.
	 ****/
 
	public static void reportString(String str) {
		report(str);
	}

	/* **********************************************
	 * Report a localized message to output.
	 * @param key Key for the message to report.
	 ****/
 
	public static void reportMessage(String key) {
		reportMessage(key, (String[])null);
	}

	/* **********************************************
	 * Report a localized message to output,
	 * substituting the received value where
	 * appropriate.
	 * @param key Key for the message to report.
	 * @param value Value to be inserted into the
	 *   message at the {0} marker.
	 ****/

	public static void reportMessage(String key,
		String value) {
		reportMessage(key, new String [] {value});
	}

	/* **********************************************
	 * Report a localized message to output,
	 * substituting the received values where
	 * appropriate.
	 * @param key Key for the message to report.
	 * @param values Array of Value to be inserted
	 *   into the message at the {0}, {1}, etc markers.
	 ****/

	public static void reportMessage(String key,
		String [] values) {

		String msg = dblook.lookupMessage(key, values);
		report(msg);

	}

	/* **********************************************
	 * Prints the received exception to the log
	 * file and, if the use has specified "verbose",
	 * the screen as well.
	 * @param e The exception to be printed.
	 ****/

	public static void debug(Exception e) {

		e.printStackTrace(logFile);
		if (verbose)
			e.printStackTrace(System.err);
		atLeastOneDebug = true;

	}

	/* **********************************************
	 * Prints the message for the received key to the log
	 * log file and, if the use has specified "verbose",
	 * the screen as well.
	 * @param key Key for the message to be printed.
	 * @param value Value to be substituted into the
	 *   message.
	 ****/
	
	public static void debug(String key,
		String value)
	{

		String msg = key;
		if (value != null) {
			msg = dblook.lookupMessage(key,
				new String [] {value});
		}

		logFile.println("-- **--> DEBUG: " + msg);
		if (verbose)
			System.err.println("-- **--> DEBUG: " + msg);
		atLeastOneDebug = true;

	}

	/* **********************************************
	 * Prints the message for the received key to the log
	 * log file and, if the use has specified "verbose",
	 * the screen as well.
	 * @param key Key for the message to be printed.
	 * @param value Value to be substituted into the
	 *   message.
	 ****/

	public static void debug(String key,
		String [] values)
	{

		String msg = key;
		if (values != null) {
			msg = dblook.lookupMessage(key, values);
		}

		logFile.println("-- **--> DEBUG: " + msg);
		if (verbose)
			System.err.println("-- **--> DEBUG: " + msg);
		atLeastOneDebug = true;

	}

	/* **********************************************
	 * Recursive method to unroll a chains of SQL exceptions.
	 * @param sqlE The SQL exception to unroll.
	 * @return A string representing the unrolled exception
	 *  is returned.
	 ****/

	public static String unRollExceptions(SQLException sqlE) {

		String rv = sqlE.getMessage() + "\n";
		if (sqlE.getNextException() != null) 
			return rv + unRollExceptions(sqlE.getNextException());
		else
			return rv;

	}

	/* **********************************************
	 * Write a string (piece of an SQL command) to
	 * the output DDL file.
	 * @param sql The string to write.
	 ****/

	public static void writeToNewDDL(String sql) {

		if (ddlFile == null)
			System.out.print(sql);
		else
			ddlFile.print(sql);

	}

	/* **********************************************
	 * Write the user-given statement delimiter to
	 * the output DDL file, followed by a newline.
	 ****/

	public static void writeStmtEndToNewDDL() {

		if (ddlFile == null)
			System.out.println(stmtEnd);
		else
			ddlFile.println(stmtEnd);

	}

	/* **********************************************
	 * Write a newline character to the output DDL
	 * file, followed by a newline.
	 ****/

	public static void writeNewlineToNewDDL() {

		if (ddlFile == null)
			System.out.println();
		else
			ddlFile.println();
	}

	/* **********************************************
	 * Close output streams and, if at least one
	 * message was printed to the log file, let
	 * the user know.
	 * @return true if all streams were closed
	 *  successfully; false otherwise.
	 ****/

	public static boolean cleanup() {

		try {
			if (atLeastOneDebug)
				dblook.writeVerboseOutput(
					"CSLOOK_AtLeastOneDebug", null);
			logFile.close();
			if (ddlFile != null)
				ddlFile.close();
			
		}
		catch (Exception e) {
			System.out.println("Error releasing resources: " + e);
			return false;
		}

		return true;

	}

}
