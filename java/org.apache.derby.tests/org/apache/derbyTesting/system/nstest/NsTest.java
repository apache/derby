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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Date;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Timestamp;

import org.apache.derbyTesting.system.nstest.init.DbSetup;
import org.apache.derbyTesting.system.nstest.init.Initializer;
import org.apache.derbyTesting.system.nstest.init.NWServerThread;
import org.apache.derbyTesting.system.nstest.tester.BackupRestoreReEncryptTester;
import org.apache.derbyTesting.system.nstest.tester.Tester1;
import org.apache.derbyTesting.system.nstest.tester.Tester2;
import org.apache.derbyTesting.system.nstest.tester.Tester3;
import org.apache.derbyTesting.system.nstest.utils.MemCheck;
import org.apache.derbyTesting.system.nstest.utils.SequenceReader;

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

public class NsTest extends Thread
{
    private static  final   String  BACKUP_FLAG = "derby.nstest.backupRestore";
    private static  final   String  OUTPUT_FILE = "derby.nstest.outputFile";
    private static  final   String  JUST_COUNT_ERRORS = "derby.nstest.justCountErrors";
    private static  final   String  QUIET = "derby.nstest.quiet";
    private static  final   String  DURATION = "derby.nstest.durationInMinutes";

    private static  final   long    MILLIS_PER_MINUTE = 1000L * 60L;
    
    private static  final   String  USAGE =
        "Usage:\n" +
        "\n" +
        "    java org.apache.derbyTesting.system.nstest.NsTest [ DerbyClient | Embedded [ small ] ]\n" +
        "\n" +
        "If no arguments are specified, the test defaults to a client/server configuration (DerbyClient)\n" +
        "\n" +
        "The following flags can be set:\n" +
        "\n" +
        "    -D" + BACKUP_FLAG + "=false    Turns off backup, restore, and re-encryption.\n" +
        "\n" +
        "    -D" + OUTPUT_FILE + "=fileName    Redirects output and errors to a file.\n" +
        "\n" +
        "    -D" + JUST_COUNT_ERRORS + "=true    Makes the test run quietly at steady-state, counting errors, and printing a summary at the end.\n" +
        "\n" +
        "    -D" + DURATION + "=$number    Run for this number of minutes.\n";

    private static  final   String  ERROR_BANNER1 = "//////////////////////////////////////////////////////////////\n";
    private static  final   String  ERROR_BANNER2 = "//    ";

    public  static  final   String  DEAD_CONNECTION = "08003";
    
    
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

    /* where to log messages */
    private static  PrintStream         statisticsLogger;
    public static NsTestPrintStream   logger;
    
	public static Properties prop = new Properties();

	public static int INIT = 0;

	public static int TESTER1 = 1;

	public static int TESTER2 = 2;

	public static int TESTER3 = 3;

	public static int BACKUP = 4;

	public static String BACKUPDIR = "backupdir";

	public static String RESTOREDIR = "restoredir";

	public static boolean START_SERVER_IN_SAME_VM = false;// If the server
	// also needs to be started as a thread

	public static boolean AUTO_COMMIT_OFF = false; // true is autocommit off

	public static boolean CREATE_DATABASE_ONLY = false;

	public static boolean schemaCreated = false; // initially schema is
	// assumed to not exist

	// Default configuration; can be overwritten by
	// adding the argument 'small' to the call of this class - 
	// see setSmallConfig()
	public static int INIT_THREADS = 6; // keep this low to avoid deadlocks

	public static int MAX_INITIAL_ROWS = 6000;

	public static int MAX_ITERATIONS = 2000; // Each client does these many
	// transactions in the test.

	// num of rows worked over in a transaction
	public static int MAX_LOW_STRESS_ROWS = 30; 

	// num of transaction batches
	public static int MAX_OPERATIONS_PER_CONN = 25; 

	public static int NUMTESTER1 = 15;

	public static int NUMTESTER2 = 45;

	public static int NUMTESTER3 = 10;

	public static int NUM_HIGH_STRESS_ROWS = 25000;

	public static int NUM_UNTOUCHED_ROWS = 6000;

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
    
	private static NsTest[] testThreads = null;

    private static  boolean _justCountErrors;
    private static  HashMap<String,NsTestError> _errors = new HashMap<String,NsTestError>();

    private static  long    _duration;
    
    private static  boolean _statisticsAlreadyPrinted = false;
    private static  long        _maxSequenceCounter;
    private static  long        _startTimestamp;
    private static  long        _endTimestamp;

    private static  long        _totalMemory;
    private static  long        _freeMemory;
    private static  Date        _lastMemoryCheckTime;

	public static int numActiveTestThreads() {
		int activeThreadCount=0;

        if ( testThreads != null )
        {
            for (int i = 0; i < testThreads.length ; i++)
            {
                if (testThreads[i] != null && testThreads[i].isAlive())
                    activeThreadCount++;
            }
        }
        
		return activeThreadCount;
	}

    public  static  void    updateMemoryTracker
        ( long newTotalMemory, long newFreeMemory, Date newTimestamp )
    {
        _totalMemory = newTotalMemory;
        _freeMemory = newFreeMemory;
        _lastMemoryCheckTime = newTimestamp;
    }

    public  static  void    updateSequenceTracker( long newValue )
    {
        _maxSequenceCounter = newValue;
    }

    public  static  boolean justCountErrors() { return _justCountErrors; }

	public static synchronized void addError( Throwable t )
    {
        String  key = getStackTrace( t );

        NsTestError error = _errors.get( key );
        if ( error != null ) { error.increment(); }
        else
        {
            error = new NsTestError( t );
            _errors.put( key, error );
        }
	}
    private static  String  getStackTrace( Throwable t )
    {
        StringWriter    sw = new StringWriter();
        PrintWriter     pw = new PrintWriter( sw );

        t.printStackTrace( pw );
        pw.flush();
        sw.flush();

        return sw.toString();
    }

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

	private static void setSmallConfig() {
        
		INIT_THREADS = 3; //keep this low to avoid deadlocks
		MAX_INITIAL_ROWS = 150; //for a small test
		MAX_ITERATIONS = 50; //for a small test
		MAX_LOW_STRESS_ROWS = 10; //for a small test
		MAX_OPERATIONS_PER_CONN = 10; //for a small test
		NUMTESTER1 = 3; //for a small test 
		NUMTESTER2 = 4;//for a small test
		NUMTESTER3 = 3; //for a small test
		NUM_HIGH_STRESS_ROWS = 20; //for a small test
		NUM_UNTOUCHED_ROWS = 50; //for a small test
	}
    
	// ****************************************************************************
	//
	// main - will load the Derby embedded or client, invoke the
	// database setup, initialize the
	// tables and then kick off the test threads.
	//
	// ****************************************************************************
	public static void main(String[] args) throws SQLException, IOException,
	InterruptedException, Exception, Throwable
    {
        _startTimestamp = System.currentTimeMillis();

		String outputFile = System.getProperty( OUTPUT_FILE );
        statisticsLogger = System.out;
        if ( outputFile != null )
        {
            statisticsLogger = new PrintStream( outputFile );
        }

		String duration = System.getProperty( DURATION );
        if ( duration != null )
        {
            _duration = Long.parseLong( duration ) * MILLIS_PER_MINUTE;
        }

        _justCountErrors = Boolean.getBoolean( JUST_COUNT_ERRORS );

        logger = new NsTestPrintStream( statisticsLogger, !_justCountErrors );

        // add a shutdown hook to print statistics if someone types control-c to kill the test
        Runtime.getRuntime().addShutdownHook( new Thread( new ShutdownHook() ) );

		Connection conn = null;
		if (args.length >= 1) {
			driver_type = args[0];
			if (!((driver_type.equalsIgnoreCase("DerbyClient"))
					|| (driver_type
							.equalsIgnoreCase("Embedded")))) {
				printUsage();
				return;
			}
			logger.println("Test nstest starting....., using driver: "
					+ driver_type);
		} else {
			driver_type = "DerbyClient";
		}
		if (args.length >= 2) {
			String testConfiguration = args [1];
			if (testConfiguration.equalsIgnoreCase("small"))
			{
				logger.println("using small config");
				setSmallConfig();
			}    
		}
        
		TimerThread timerThread = null;
        if ( _duration > 0L )
        {
            timerThread = new TimerThread( _duration );
            timerThread.start();
        }

		// Load the driver and get a connection to the database
		String jdbcUrl = "";
        Class<?> clazz;
		try {
			if (driver_type.equalsIgnoreCase("Embedded")) {
				// logger.println("Driver embedd : " + driver_type);
				logger.println("Loading the embedded driver...");
				clazz = Class.forName(embedDriver);
                clazz.getConstructor().newInstance();
				jdbcUrl = embedDbURL + ";" + dataEncypt + ";" + bootPwd;
				embeddedMode = true;
			} else {
				logger.println("Driver type : " + driver_type);
				logger.println("Loading the Derby Client driver..."
						+ driver);
				clazz = Class.forName(driver);
                clazz.getConstructor().newInstance();
				logger.println("Client Driver loaded");
				jdbcUrl = clientDbURL + ";" + dataEncypt + ";" + bootPwd;
			}
			if ((!embeddedMode) && START_SERVER_IN_SAME_VM) {
				startNetworkServer();
			}
			prop.setProperty("user", user);
			prop.setProperty("password", password);
			logger
			.println("Getting a connection using the url: " + jdbcUrl);
			logger.println("JDBC url= " + jdbcUrl);
			conn = DriverManager.getConnection(jdbcUrl, prop);

		} catch (SQLException sqe) {

			logger.println("\n\n " + sqe + sqe.getErrorCode() + " "
					+ sqe.getSQLState());
			if ((sqe.getErrorCode() == 40000)
					|| sqe.getSQLState().equalsIgnoreCase("08001")) {
				logger
				.println("\n Unable to connect, test cannot proceed. Please verify if the Network Server is started on port 1900.");
				// sqe.printStackTrace();
				return;
			}

		} catch (ClassNotFoundException cnfe) {
			logger.println("Driver not found: " + cnfe.getMessage());
			cnfe.printStackTrace( logger );
			return;

		} catch (Exception e) {
			e.printStackTrace( logger );
			logger.println("Unexpected Failure");
			printException("nstest.main() method ==> ", e);
		}

		// create test schema if it does not already exist
		if (DbSetup.doIt(conn) == false) {
			logger.println("Error in dbSetup, test will exit");
			System.exit(1);
		}

		// Note that the connection is still open, we can safely close it now
		try {
			conn.close();
		} catch (Exception e) {
			logger
			.println("FAIL - Error closing the connection in nstest.main():");
			printException("Closing connection in nstest.main()", e);
		}

		// check memory in separate thread-- allows us to monitor usage during
		// database calls
		// 200,000 msec = 3min, 20 sec delay between checks
		logger.println("Starting memory checker thread");
		MemCheck mc = new MemCheck(200000);
		mc.start();

		// Now populate the tables using INIT_THREADS number of threads only if
		// the schemaCreated flag has not been set. If so, then we assume that 
		// some other thread from possibly another jvm reached here and has
		// already created the schema and loaded the tables.
		// Note that we kick off threads of this object type (nstest) and use
		// the run method to do the work. The key to starting the init threads
		// is the use of the constructor to indicate to the thread that it is
		// an init thread. In this case, we pass the value INIT to the
		// constructor and in the run method we go to the right section of the
		// code based on what value is passed in. The other possible value that
		// a thread can get is TESTER which indicates that these are the main 
		// test threads.

		if (NsTest.schemaCreated == false) {
			// Table was created by this object, so we need to load it
			logger
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
			logger
			.println("Schema has already been created by another process!");

		// The following 2 lines are used when you want to only create the test
		// database that can be used as a reference so that subsequent tests do
		// not need to create one of their own.
		// The CREATE_DATABASE_ONLY FLAG is set with the rest of the flags
		if (CREATE_DATABASE_ONLY) {
			logger
			.println("Finished creating the database, TEST THREADS WILL NOT RUN!!");
			// Finally also stop the memory checker and sequence threads, else the test will
			// remain hung!
			mc.stopNow = true;
			mc.join();
			return;
		}

		// Table was created by some other object, so we assume it is already
		// loaded
		// Now kick off the actual test threads that will do the work for us.
		// Note that we use the value TESTER when initializing the threads.
		// The total number of threads is NUMTESTER1+NUMTESTER2+NUMTESTER3
		logger
		.println("Kicking off test threads that will work over the test table");

		int numTestThread = 0;
		int maxTestThreads = 0;
		String runBackup = System.getProperty( BACKUP_FLAG );
		if ((runBackup != null) && (runBackup.equalsIgnoreCase("false")))
				maxTestThreads = NUMTESTER1 + NUMTESTER2 + NUMTESTER3;
		else
				maxTestThreads = 1 + NUMTESTER1 + NUMTESTER2 + NUMTESTER3;
		testThreads = new NsTest[maxTestThreads];

		// This loop is made of 3 subloops that will initialize the required
		// amount of tester threads
		// It uses the numTestThread variable as the array index which gets
		// incremented in each subloop
		while (numTestThread < maxTestThreads) {
			// Check for property setting to decide the need for starting
			// BackupRestore thread
			if ((runBackup != null) && (runBackup.equalsIgnoreCase("false"))) {
				logger.println("BackupRestore Thread not started...");
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

		// check sequence value thread
		// 60,000 msec = 1 minute delay between checks
		logger.println("Starting sequence reader thread");
		SequenceReader  sequenceReader = new SequenceReader( DriverManager.getConnection( jdbcUrl, prop ), 60000 );
		sequenceReader.start();

		// Wait for the test threads to finish and join back
		for (int j = 0; j < maxTestThreads; j++)
        {
            logger.println("Waiting for thread " + j+ " to join back/finish");
			testThreads[j].join();
		}

        if ( timerThread != null )
        {
            timerThread.stopNow();
            timerThread.interrupt();
            timerThread.join();
        }

        // stop the sequence reader thread
		sequenceReader.stopNow = true;
		sequenceReader.interrupt();
		sequenceReader.join();

		// Print statistics
        printStatistics();

		// Finally also stop the memory checker thread
		mc.stopNow = true;
		mc.join();

		logger
		.println("End of test nstest! Look for 'FAIL' messages in the output and derby.log");

	}// end of main

    public  static  void    printStatistics()
    {
        if ( _statisticsAlreadyPrinted ) { return; }
        else { _statisticsAlreadyPrinted = true; }

        _endTimestamp = System.currentTimeMillis();

		statisticsLogger.println("");
		statisticsLogger.println("STATISTICS OF OPERATIONS DONE");
		statisticsLogger.println("-----------------------------");
		statisticsLogger.println("\n\n");
		statisticsLogger.println( "Start time = " + (new Timestamp( _startTimestamp )).toString() );
		statisticsLogger.println( "End time = " + (new Timestamp( _endTimestamp )).toString() );
		statisticsLogger.println( "Duration = " + ( (_endTimestamp - _startTimestamp) / MILLIS_PER_MINUTE ) + " minutes" );
		statisticsLogger.println("\n\n");
		statisticsLogger.println("SUCCESSFUL: ");
		statisticsLogger.println("	Number of INSERTS = " + numInserts);
		statisticsLogger.println("	Number of UPDATES = " + numUpdates);
		statisticsLogger.println("	Number of DELETES = " + numDeletes);
		statisticsLogger.println("	Number of SELECTS = " + numSelects);
		statisticsLogger.println("");
		statisticsLogger.println("FAILED: ");
		statisticsLogger.println("	Number of failed INSERTS = " + numFailedInserts);
		statisticsLogger.println("	Number of failed UPDATES = " + numFailedUpdates);
		statisticsLogger.println("	Number of failed DELETES = " + numFailedDeletes);
		statisticsLogger.println("	Number of failed SELECTS = " + numFailedSelects);
		statisticsLogger.println("");
		statisticsLogger.println("  Note that this may not be the same as the server side connections made\n"
				+ "   to the database especially if connection pooling is employed");
		statisticsLogger.println("");
		statisticsLogger
		.println("NOTE: Failing operations could be because of locking issue that are\n"
				+ "directly related to the application logic.  They are not necessarily bugs.");

        statisticsLogger.println( "\nMax sequence counter peeked at = " + _maxSequenceCounter + "\n" );
        
        statisticsLogger.println( "\nLast total memory = " + _totalMemory + ", last free memory = " + _freeMemory + " as measured at " + _lastMemoryCheckTime + "\n" );

        if ( _errors.size() > 0 )
        {
            // sort the errors by the timestamps of their first occurrences
            NsTestError[]   errors = new NsTestError[ _errors.size() ];
            _errors.values().toArray( errors );
            Arrays.sort( errors );
            
            countAndPrintSQLStates();
            for ( NsTestError error  : errors )
            {
                printError( error );
            }
        }
    }

    /** Count and print the number of times each SQLState was seen in an error */
    private static  void    countAndPrintSQLStates()
    {
        HashMap<String,int[]>   results = new HashMap<String,int[]>();

        // count the number of times each SQL state was seen
        for ( String key  : _errors.keySet() )
        {
            NsTestError error = _errors.get( key );
            int         count = error.count();
            Throwable   throwable = error.throwable();
            if ( throwable instanceof SQLException )
            {
                SQLException    se = (SQLException) throwable;
                String          sqlState = se.getSQLState();

                if ( sqlState != null )
                {
                    int[]   holder = results.get( sqlState );
                    if ( holder == null )
                    {
                        holder = new int[] { count };
                        results.put( sqlState, holder );
                    }
                    else { holder[ 0 ] += count; }
                }
            }
        }

        // now print the counts
        statisticsLogger.println( "\n" );
        for ( String sqlState : results.keySet() )
        {
            statisticsLogger.println("	Number of " + sqlState + " = " + results.get( sqlState )[ 0 ] );
        }
        statisticsLogger.println( "\n" );
    }

    private static  void    printError( NsTestError error )
    {
        Throwable   throwable = error.throwable();
        String          stackTrace = getStackTrace( throwable );
        int             count = error.count();
        Timestamp   firstOccurrenceTime = new Timestamp( error.getFirstOccurrenceTime() );
        Timestamp   lastOccurrenceTime = new Timestamp( error.getLastOccurrenceTime() );
        String      sqlState = (throwable instanceof SQLException) ? 
            ((SQLException) throwable).getSQLState() : null;

        StringBuilder   buffer = new StringBuilder();

        buffer.append( ERROR_BANNER1 );
        buffer.append( ERROR_BANNER2 );
        buffer.append( "\n" );
        buffer.append( ERROR_BANNER2 );
        buffer.append( "Count = " + count );
        if ( sqlState != null ) { buffer.append( ", SQLState = " + sqlState ); }
        buffer.append( ", Message = " + throwable.getMessage() );
        buffer.append( "\n" );
        buffer.append( ERROR_BANNER2 );
        buffer.append( "\n" );
        buffer.append( ERROR_BANNER2 );
        buffer.append( "First occurrence at " + firstOccurrenceTime );
        buffer.append( ", last occurrence at " + lastOccurrenceTime );
        buffer.append( "\n" );
        buffer.append( ERROR_BANNER2 );
        buffer.append( "\n" );
        buffer.append( ERROR_BANNER1 );
        buffer.append( "\n" );
        buffer.append( stackTrace );
        buffer.append( "\n" );

        statisticsLogger.println( buffer.toString() );
    }

	// ****************************************************************************
	//
	// run() - the main workhorse method of the threads that will either
	// initialize the table data or work over it as part of the test process.
	// Table data initialization threads are of the following type
	// Initializer -
	// Bulk Insert client type that deals with a large(stress)
	// number of rows with the connection being closed after the insert.
	// Max rows inserted is based on the parameter MAX_INITIAL_ROWS
	// Note that the run method will also instantiate tester objects of
	// different types based on the following criteria
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

		logger.println(this.getName() + " is now running");

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
				logger
				.println(ioe
						+ "=====> Unable to create backup log file, test cannot proceed ");
				ioe.printStackTrace( logger );
				return;
			}
			Tstr4.startTesting();

		} else {
			logger
			.println("FAIL: Invalid thread type, should be INIT or TESTERx or BACKUP");
			logger.println("You should check the code and restart");
			return;
		}

		logger.println(this.getName() + " finished and is now exiting");

	}// end of run()

	// ** This method abstracts exception message printing for all exception
	// messages. You may want to change
	// ****it if more detailed exception messages are desired.
	// ***Method is synchronized so that the output file will contain sensible
	// stack traces that are not
	// ****mixed but rather one exception printed at a time
    public static synchronized void printException(String where, Exception e)
    {
        if ( justCountErrors() )
        {
            addError( e );
            vetError( e );
            return;
        }
        
		if (e instanceof SQLException) {
			SQLException se = (SQLException) e;
			if (se.getSQLState() != null) { // SQLSTATE is NULL for a
				if (se.getSQLState().equals("40001"))
					logger.println("deadlocked detected");
				if (se.getSQLState().equals("40XL1"))
					logger.println(" lock timeout exception");
				if (se.getSQLState().equals("23500"))
					logger.println(" duplicate key violation");
			}
			if (se.getNextException() != null) {
				String m = se.getNextException().getSQLState();
				logger.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
			}
		}
		if (e.getMessage() == null) {
			logger.println("NULL error message detected");
			logger.println("Here is the NULL exection - " + e.toString());
			logger.println("Stack trace of the NULL exception - ");
			e.printStackTrace( logger );
		}
		logger.println("At this point - " + where
				+ ", exception thrown was : " + e.getMessage());

        vetError( e );
	}

    /**
     * Analyze an error which is being reported. Currently, all this
     * does is check for OutOfMemoryErrors. If we see an OutOfMemoryError,
     * we kill the JVM since we will just get cascading noise after we exhaust
     * memory.
     */
    private static  void    vetError( Throwable t )
    {
        if ( t == null ) { return; }
        
        if ( t instanceof OutOfMemoryError )
        {
            printStatistics();
            Runtime.getRuntime().halt( 0 );
        }

        vetError( t.getCause() );

        if ( t instanceof SQLException )
        {
            SQLException    sqlException = (SQLException) t;
            vetError( sqlException.getNextException() );
        }
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
			e.printStackTrace( logger );
			throw e;
		}

	}

    /** Return true if the connection is dead */
    public  static  boolean deadConnection( Throwable t )
    {
        if ( t instanceof SQLException )
        {
            SQLException    se = (SQLException) t;

            if ( DEAD_CONNECTION.equals( se.getSQLState() ) ) { return true; }
        }

        return false;
    }

	public static void printUsage()
    {
        _statisticsAlreadyPrinted = true;
        System.out.println( USAGE );
	}

    public  static  class   ShutdownHook    implements  Runnable
    {
        public  void    run()
        {
            NsTest.printStatistics();
        }
    }
    
}
