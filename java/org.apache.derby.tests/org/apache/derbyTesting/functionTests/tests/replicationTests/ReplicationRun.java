/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun
 
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
package org.apache.derbyTesting.functionTests.tests.replicationTests;


import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derby.drda.NetworkServerControl;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import java.sql.*;
import java.io.*;
import org.apache.derby.jdbc.ClientDriver;
import org.apache.derby.client.ClientDataSourceInterface;

import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.shared.common.reference.ModuleUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Framework to run replication tests.
 * Subclass to create specific tests as 
 * in ReplicationRun_Local and ReplicationRun_Distributed.
 */

public class ReplicationRun extends BaseTestCase
{
    
    /**
     * Name of properties file defining the test environment
     * and replication tests to be run.
     * Located in <CODE>${user.dir}</CODE>
     */
    final static String REPLICATIONTEST_PROPFILE = "replicationtest.properties";
    
//IC see: https://issues.apache.org/jira/browse/DERBY-4246
    final static String REPLICATION_MASTER_TIMED_OUT           = "XRE06";
    final static String REPLICATION_SLAVE_STARTED_OK           = "XRE08";
    final static String REPLICATION_DB_NOT_BOOTED              = "XRE11";
    final static String SLAVE_OPERATION_DENIED_WHILE_CONNECTED = "XRE41";
    final static String REPLICATION_SLAVE_SHUTDOWN_OK          = "XRE42";

    static String testUser = null;
    
    static String userDir = null;
    static String userHome = null; // Used when running distributed.
    
    static String dataEncryption = null;
      // Set to a legal encryption string to
      // create or connect to an encrypted db.
    
    static String masterServerHost = "localhost"; 
    static int masterServerPort = TestConfiguration.getCurrent().getPort(); // .. get current ports..
    static String slaveServerHost = "localhost";
    static int slaveServerPort = TestConfiguration.getCurrent().getNextAvailablePort();; // .. ..
    static String testClientHost = "localhost";
    static int slaveReplPort = TestConfiguration.getCurrent().getNextAvailablePort();;
    
    static String masterDatabasePath = null;
    static String slaveDatabasePath = null;
    static String replicatedDb = "test";
    
    static String bootLoad = ""; // The "test" to run when booting the master database.
    
    static String freezeDB = ""; // Preliminary: need to "manually" freeze db as part of initialization.
    static String unFreezeDB = ""; // Preliminary: need to "manually" unfreeze db as part of initialization.
    
    static boolean junitTest = true; // Set to false in replicationtest.properties
                                     // when running distributed using plain class w/main()
    static boolean runUnReplicated = false;
    
    static boolean simpleLoad = true;
    static int simpleLoadTuples = 1000;
    
    static int tuplesToInsertPerf = 10000;
    static int commitFreq = 0; // autocommit
    
    static String masterDbSubPath = "db_master";
    static String slaveDbSubPath = "db_slave";
    
    
    static String replicationTest = "";
    static String replicationVerify = "";
    
    static int THREADS = 0; // Number of threads and 
    static int MINUTES = 0; // minutes a StressMultiTest load should run.
                            // When ReplicationTestRunStress used as load...
    static String sqlLoadInit = "";
    
    final static String networkServerControl = "org.apache.derby.drda.NetworkServerControl";
    static String specialTestingJar = null;
    // None null if using e.g. your own modified tests.
    static String jvmVersion = null;
    static String masterJvmVersion = null;
    static String slaveJvmVersion = null;
    static String derbyVersion = null;
    static String derbyMasterVersion = null; // Needed for PoC. Remove when committed.
    static String derbySlaveVersion = null;  // Needed for PoC. Remove when committed.
    
    static String junit_jar = null; // Path for JUnit jar
    static String test_jars = null; // Path for derbyTesting.jar:junit_jar
    
    final static String FS = File.separator;
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
    final static String PS = File.pathSeparator;
    
    static boolean showSysinfo = false;
    
    static long sleepTime = 5000L; // millisecs.
    
    static final String DRIVER_CLASS_NAME = "org.apache.derby.jdbc.ClientDriver";
    static final String DB_PROTOCOL="jdbc:derby";
    
    static final String ALL_INTERFACES = "0.0.0.0";
    
    static String LF = null;
    
    static final String remoteShell = "/usr/bin/ssh -x"; // or /usr/bin/ssh ?

    Utils util = new Utils();
    
    State state = new State();

    static boolean localEnv = false; // true if all hosts have access to
                                             // same filesystem (NFS...)
    
    static String derbyProperties = null;

    String classPath = null; // Used in "localhost" testing.
    
    /** A Connection to the master database*/
    private Connection masterConn = null;
    /** A Connection to the slave database*/
    private Connection slaveConn = null;
    /** The exception thrown as a result of a startSlave connection attempt  */
    private volatile Exception startSlaveException = null;

    /**
     * List of threads that have been started by the tests and not explicitly
     * waited for. Wait for these to complete in {@link #tearDown()} so that
     * they don't interfere with subsequent test cases.
     */
    private ArrayList<Thread> helperThreads = new ArrayList<Thread>();

    private String db_uid = null;
    private String db_passwd = null;

    /**
     * Creates a new instance of ReplicationRun
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun(String testcaseName)
    {
        super(testcaseName);
        
        LF = System.getProperties().getProperty("line.separator");
    }
    
    /**
     * Creates a new instance of ReplicationRun running with authentication.
     */
    public ReplicationRun( String testcaseName, String user, String password )
    {
        this( testcaseName );

        db_uid = user;
        db_passwd = password;
    }
    
    /**
     * Parent super()
     * @throws java.lang.Exception .
     */
    protected void setUp() throws Exception
    {
        super.setUp();
    }
    
    /**
     * Parent super()
     * @throws java.lang.Exception .
     */
    protected void tearDown() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (Thread t : helperThreads) {
            t.join();
        }
        helperThreads = null;

//IC see: https://issues.apache.org/jira/browse/DERBY-5729
        close(masterConn);
        close(slaveConn);

        masterConn = null;
        slaveConn = null;
        startSlaveException = null;
        classPath = null;
        util = null;
        state = null;

        super.tearDown();
    }
    
    /** Close a connection. */
    private static void close(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    /**
     * Run the test. Extra logic in addition to BaseTestCase's similar logic,
     * to save derby.log and database files for replication directories if a
     * failure happens.
     */
    public void runBare() throws Throwable {

        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-4270

            super.runBare();

        } catch (Throwable running) {

            // Copy the master and slave's derby.log file and databases
            //
            PrintWriter stackOut = null;

            try {
                String failPath = PrivilegedFileOpsForTests.
                    getAbsolutePath(getFailureFolder());

                stackOut = new PrintWriter(
                        PrivilegedFileOpsForTests.getFileOutputStream(
                            new File(failPath, ERRORSTACKTRACEFILE), true));

                String[] replPaths = new String[]{masterDbSubPath,
                                                  slaveDbSubPath};

                for (int i=0; i < 2; i++) {
                    // Copy the derby.log file.
                    //
                    File origLog = new File(replPaths[i], DERBY_LOG);
                    File newLog = new File(failPath,
                                           replPaths[i] + "-" + DERBY_LOG);
                    PrivilegedFileOpsForTests.copy(origLog, newLog);

                    // Copy the database.
                    //
                    String dbName = TestConfiguration.getCurrent().
                        getDefaultDatabaseName();
                    File dbDir = new File(replPaths[i], dbName );
                    File newDbDir = new File(failPath,
                                             replPaths[i] + "-" + dbName);
                    PrivilegedFileOpsForTests.copy(dbDir,newDbDir);
                }
            } catch (IOException ioe) {
                // We need to throw the original exception so if there
                // is an exception saving the db or derby.log we will print it
                // and additionally try to log it to file.
                BaseTestCase.printStackTrace(ioe);
                if (stackOut != null) {
                    stackOut.println("Copying db_slave/db_master's " +
                                     DERBY_LOG + " or database failed:");
                    ioe.printStackTrace(stackOut);
                    stackOut.println();
                }
            } finally {
                if (stackOut != null) {
                    stackOut.close();
                }

                // Let JUnit take over
                throw running;
            }
        }
    }


//IC see: https://issues.apache.org/jira/browse/DERBY-3921
    String useEncryption(boolean create)
    {
        String encryptionString = "";
        if ( dataEncryption != null)
        {
            if ( create ) encryptionString = ";dataEncryption=true";
            encryptionString = encryptionString+";"+dataEncryption;
        }
        return encryptionString;
    }
    
    //////////////////////////////////////////////////////////////
    ////
    //// The replication test framework (testReplication()):
    //// a) "clean" replication run starting master and slave servers,
    ////     preparing master and slave databases,
    ////     starting and stopping replication and doing
    ////     failover for a "normal"/"failure free" replication
    ////     test run.
    //// b)  Running (positive and negative) tests at the various states 
    ////     of replication to test what is and is not accepted compared to
    ////     the functional specification.
    //// c)  Adding additional load on master and slave servers in 
    ////     different states of replication.
    ////
    //////////////////////////////////////////////////////////////
    
    /* Template
    public void testReplication()
    throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG("WARNING: Define in subclass of ReplicationRun. "
                + "See ReplicationRun_Local for an example.");
    }
     */

    void connectPing(String fullDbPath, 
            String serverHost, int serverPort, 
            String testClientHost)
        throws Exception
    {
        
        String dbURL = serverURL( fullDbPath, serverHost, serverPort );
        Connection conn = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-3738
        String lastmsg = null;
        long sleeptime = 200L;
        boolean done = false;
        int count = 0;
        while ( !done )
        {
            try
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
                Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
                conn = DriverManager.getConnection(dbURL);
                done = true;
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
                util.DEBUG("Ping Got connection after " 
//IC see: https://issues.apache.org/jira/browse/DERBY-3738
                        + count +" * "+ sleeptime + " ms.");
                conn.close();
            }
            catch ( SQLException se )
            {
                int errCode = se.getErrorCode();
                lastmsg = se.getMessage();
                String sState = se.getSQLState();
                String expectedState = "08004";
                lastmsg = errCode + " " + sState + " " + lastmsg 
                        + ". Expected: "+ expectedState;
                util.DEBUG("Got SQLException: " + lastmsg);
//IC see: https://issues.apache.org/jira/browse/DERBY-2601
                if ( (errCode == 40000)
                && (sState.equalsIgnoreCase(expectedState) ) )
                {
                    if (count++ >= 600) {
                        // Have tried 600 * 200 ms == 2 minutes without
                        // success, so give up now.
                        fail("Failover did not succeed", se);
                    }
                    util.DEBUG("Failover not complete.");
                    Thread.sleep(sleeptime); // ms.
                }
                else
                {
                    fail("Connect failed", se);
                }
            }
        }
    }
    
//IC see: https://issues.apache.org/jira/browse/DERBY-3738
    String showCurrentState(String ID, long waitTime,
            String fullDbPath, 
            String serverHost, int serverPort)
        throws Exception
    {
        int errCode = 0;
        String sState = "CONNECTED";
        String msg = null;
        Thread.sleep(waitTime); // .... until stable...
        try
        {
            ClientDataSourceInterface ds = configureDataSource(
                    fullDbPath,
                    serverHost,
                    serverPort,
                    useEncryption(false) );
            Connection conn = ds.getConnection();
            conn.close();
        }
        catch ( SQLException se )
        {
            errCode = se.getErrorCode();
            msg = se.getMessage();
            sState = se.getSQLState();
        }
        util.DEBUG(ID+": ["+serverHost+":"+serverPort+"/"+fullDbPath+"] "
                + errCode + " " + sState + " " + msg);
        return sState;
    }
    void waitForConnect(long sleepTime, int tries,
            String fullDbPath, 
            String serverHost, int serverPort)
        throws Exception
    {
        int count = 0;
        String msg = null;
        while (true)
        {
            try
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-5955
                ClientDataSourceInterface ds = configureDataSource(
                    fullDbPath, serverHost, serverPort, useEncryption(false) );
                Connection conn = ds.getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
                util.DEBUG("Wait Got connection after " 
                        + (count-1) +" * "+ sleepTime + " ms.");
                conn.close();
                return;
            }
            catch ( SQLException se )
            {
                if (count++ > tries) {
                    fail("Could not connect in " + (tries * sleepTime) + " ms",
                          se);
                }
                msg = se.getErrorCode() + "' '" + se.getSQLState()
                        + "' '" + se.getMessage();
                util.DEBUG(count  + " got '" + msg +"'.");
                Thread.sleep(sleepTime); // ms. Sleep and try again...
            }
        }        
    }
    void waitForSQLState(String expectedState, 
            long sleepTime, int tries,
            String fullDbPath, 
            String serverHost, int serverPort)
        throws Exception
    {
        int count = 0;
        String msg = null;
        while (true)
        {
            try
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-5955
//IC see: https://issues.apache.org/jira/browse/DERBY-5955
                ClientDataSourceInterface ds = configureDataSource(
                        fullDbPath,
                        serverHost,
                        serverPort,
                        useEncryption(false) );
                Connection conn = ds.getConnection();
                // Should never get here!
                conn.close();
                assertTrue("Expected SQLState'"+expectedState
                            + "', but got connection!",
                        false);
            }
            catch ( SQLException se )
            {
                int errCode = se.getErrorCode();
                msg = se.getMessage();
                String sState = se.getSQLState();
                msg = "'" + errCode + "' '" + sState + "' '" + msg +"'";
                util.DEBUG(count 
                        + ": SQLState expected '"+expectedState+"'," +
                        " got " + msg);
                if ( sState.equals(expectedState) )
                {
                    util.DEBUG("Reached SQLState '" + expectedState +"' in "
                            + (count-1)+"*"+sleepTime + "ms.");
                    return; // Got desired SQLState.
                }
                else if (count++ > tries)
                {
                    fail("SQLState '" + expectedState + "' was not reached in "
                            + (tries * sleepTime) + " ms", se);
                }
                else
                {
                    Thread.sleep(sleepTime); // ms. Sleep and try again...
                }
            }
        }
    }
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
    void shutdownDb(String jvmVersion, // Not yet used
            String serverHost, int serverPort, 
            String dbPath, String replicatedDb,
            String clientHost) // Not yet used
        throws Exception
    {
        String  dbURL = serverURL( dbPath+FS+replicatedDb, serverHost, serverPort );
        util.DEBUG("**** DriverManager.getConnection(\"" + dbURL+";shutdown=true\");");

        try{
            Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
            DriverManager.getConnection(dbURL+";shutdown=true");
            fail("Database shutdown should throw exception");
        }
        catch (SQLException se)
        {
            BaseJDBCTestCase.assertSQLState("08006", se);
        }
        
    }
    
    ////////////////////////////////////////////////////////////////
    /* Utilities.... */
    void startServerMonitor(String slaveHost)
    {
        util.DEBUG("startServerMonitor(" + slaveHost + ") NOT YET IMPLEMENTED.");
    }
    
    void runTest(String replicationTest,
            String clientVM,
            String testClientHost,
            String serverHost, int serverPort,
            String dbName)
            throws Exception
    {
        util.DEBUG("runTest(" + replicationTest
                + ", " + clientVM
                + ", " + testClientHost
                + ", " + serverHost
                + ", " + serverPort
                + ", " + dbName
                + ") "
                );
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        if ( replicationTest == null ) 
        {
            util.DEBUG("No replicationTest specified. Exitting.");
            return;
        } 
        
        if ( simpleLoad )
        {
            _testInsertUpdateDeleteOnMaster(serverHost, serverPort,
                dbName, simpleLoadTuples);
            return;
        }
        
        String URL = masterURL(dbName);
        String ijClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbyTesting.jar"
                + PS + derbyVersion +FS+ "derbytools.jar";
        String testingClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbynet.jar" // WHY IS THIS NEEDED?
                // See TestConfiguration: startNetworkServer and stopNetworkServer
                + PS + test_jars;
        
        String clientJvm = ReplicationRun.getClientJavaExecutableName();

//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        final boolean isRemote = !testClientHost.equals("localhost");
        final boolean isIjTest = (replicationTest.indexOf(".sql") >= 0);

        ArrayList<String> cmd = new ArrayList<String>();

        // For remote tests, we need to specify the Java VM to use and the
        // classpath. For local tests, we'll just use the JVM and the classpath
        // BaseTestCase.execJavaCmd() gives us. Note that this means we cannot
        // vary versions when running locally.
        if (isRemote) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5504
            cmd.add(clientJvm);
            cmd.add("-classpath");
            cmd.add(isIjTest ? ijClassPath : testingClassPath);
        }
        
        util.DEBUG("replicationTest: " + replicationTest);
        if ( isIjTest )
        {
            cmd.add("-Dij.driver=" + DRIVER_CLASS_NAME);
            cmd.add("-Dij.connection.startTestClient=" + URL);
            cmd.add("org.apache.derby.tools.ij");
            cmd.add(replicationTest);
        }
        else
        { // JUnit or plain class w/main().
            cmd.add("-Dderby.tests.trace=true");
            cmd.add("-Dtest.serverHost=" + serverHost);  // Tell the test what server
            cmd.add("-Dtest.serverPort=" + serverPort);  // and port to connect to.
            cmd.add("-Dtest.inserts=" + tuplesToInsertPerf); // for SimplePerfTest
            cmd.add("-Dtest.commitFreq=" +  commitFreq); // for SimplePerfTest
            if (THREADS != 0 && MINUTES != 0) {
                // For StressMultiTestForReplLoad as load.
                cmd.add("-Dderby.tests.ThreadsMinutes="+THREADS+"x"+MINUTES);
            }
            cmd.add("-Dtest.dbPath=" + masterDbPath(dbName)); // OK?
            if (junitTest) {
                cmd.add("junit.textui.TestRunner");
            }
            cmd.add(replicationTest);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        String[] command = util.toStringArray(cmd);
        
        long startTime = System.currentTimeMillis();
        String results = null;
        String workingDir = userHome; // Remember this is run on client against master..
        if ( !isRemote )
        {
            runUserCommandLocally(command, "runTest ", null);
        }
        else
        {
            // This doesn't work if path names contain spaces or other
            // characters with special meaning to the shell.
            // NOT Correct: ...Must be positioned where the properties file
            // is located.
            results = runUserCommandRemotely(
                    "cd " + workingDir + ";" + util.splice(command, ' '),
                    testClientHost, testUser, "runTest ");
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG("Time: " + (System.currentTimeMillis() - startTime) / 1000.0);
        
    }
    void runTestOnSlave(String replicationTest,
            String clientVM,
            String testClientHost,
            String serverHost, int serverPort,
            String dbName)
            throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG("runTestOnSlave(" + replicationTest
                + ", " + clientVM
                + ", " + testClientHost
                + ", " + serverHost
                + ", " + serverPort
                + ", " + dbName
                + ") "
                );
        
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        String URL = slaveURL(dbName);
        String ijClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbyTesting.jar"
                + PS + derbyVersion +FS+ "derbytools.jar";
        String testingClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbynet.jar" // WHY IS THIS NEEDED?
                // See TestConfiguration: startNetworkServer and stopNetworkServer
                + PS + test_jars;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        String clientJvm = ReplicationRun.getSlaveJavaExecutableName();
        
        if ( replicationTest == null ) 
        {
            util.DEBUG("No replicationTest specified. Exitting.");
            return;
        } 
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        final boolean isRemote = !serverHost.equals("localhost");
        final boolean isIjTest = (replicationTest.indexOf(".sql") >= 0);

        ArrayList<String> cmd = new ArrayList<String>();

        // For remote tests, we need to specify the Java VM to use and the
        // classpath. For local tests, we'll just use the JVM and the classpath
        // BaseTestCase.execJavaCmd() gives us. Note that this means we cannot
        // vary versions when running locally.
        if (isRemote) {
            cmd.add(clientJvm);
            cmd.add("-classpath");
            cmd.add(isIjTest ? ijClassPath : testingClassPath);
        }

        util.DEBUG("replicationTest: " + replicationTest);
        if ( isIjTest )
        {
            cmd.add("-Dij.driver=" + DRIVER_CLASS_NAME);
            cmd.add("-Dij.connection.startTestClient=" + URL);
            cmd.add("org.apache.derby.tools.ij");
            cmd.add(replicationTest);
        }
        else
        { // JUnit
            cmd.add("-Dderby.tests.trace=true");
            cmd.add("-Dtest.serverHost=" + serverHost);  // Tell the test what server
            cmd.add("-Dtest.serverPort=" + serverPort);  // and port to connect to.
            cmd.add("-Dtest.inserts=" + tuplesToInsertPerf); // for SimplePerfTest
            cmd.add("-Dtest.commitFreq=" +  commitFreq); // for SimplePerfTest
            cmd.add("-Dtest.dbPath=" + slaveDbPath(dbName)); // OK?
            cmd.add("junit.textui.TestRunner");
            cmd.add(replicationTest);
        }

        String[] command = util.toStringArray(cmd);
        
        long startTime = System.currentTimeMillis();
        String results = null;
        if ( !isRemote )
        {
            runUserCommandLocally(command, "runTestOnSlave ", null);
        }
        else
        {
            // This doesn't work if path names contain spaces or other
            // characters with special meaning to the shell.
            // Must be positioned where the properties file is located.
            results = runUserCommandRemotely(
                "cd " + userDir + ";" + util.splice(command, ' '),
                testClientHost, testUser, "runTestOnSlave ");
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG("Time: " + (System.currentTimeMillis() - startTime) / 1000.0);
        
    }
    
    /*
     *
     * Should allow:
     * - Run load in separate thread.
     *
     */
    private void runLoad(String load,
            String clientVM,
            String testClientHost,
            String masterHost, int masterPort,
            String dbSubPath) // FIXME? Should we allow extra URL options?
            throws Exception
    {
        util.DEBUG("runLoad(" + load
                + ", " + clientVM
                + ", " + testClientHost
                + ", " + masterHost
                + ", " + masterPort
                + ", " + dbSubPath
                + ") "
                );
        
        
        String URL = masterLoadURL(dbSubPath);
        String ijClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbyTesting.jar"
                // Needed for 'run resource 'createTestProcedures.subsql';' cases?
                // Nope? what is 'resource'?
                + PS + derbyVersion +FS+ "derbytools.jar";
        String testingClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbynet.jar" // WHY IS THIS NEEDED?
                // See TestConfiguration: startNetworkServer and stopNetworkServer
                + PS + test_jars;
        
        String clientJvm = ReplicationRun.getClientJavaExecutableName();

//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        final boolean isRemote = !masterHost.equals("localhost");
        final boolean isIjTest = (load.indexOf(".sql") >= 0);
        
        util.DEBUG("load: " + load);

        ArrayList<String> cmd = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

        // For remote tests, we need to specify the Java VM to use and the
        // classpath. For local tests, we'll just use the JVM and the classpath
        // BaseTestCase.execJavaCmd() gives us. Note that this means we cannot
        // vary versions when running locally.
        if (isRemote) {
            cmd.add(clientJvm);
            cmd.add("-classpath");
            cmd.add(isIjTest ? ijClassPath : testingClassPath);
        }

        if ( isIjTest )
        {
            cmd.add("-Dij.driver=" + DRIVER_CLASS_NAME);
            cmd.add("-Dij.connection.startTestClient=" + URL);
            cmd.add("org.apache.derby.tools.ij");
            cmd.add(load);
        }
        else
        {
            /* BEGIN For junit: */
            cmd.add("-Dderby.tests.trace=true");
            cmd.add("junit.textui.TestRunner");
            cmd.add(load);
            /* END */
        }

        String[] command = util.toStringArray(cmd);

        if ( !isRemote )
        {
            runUserCommandInThreadLocally(command, dbSubPath,
                    "runLoad["+dbSubPath+"] ");
        }
        else
        {
            runUserCommandInThreadRemotely(util.splice(command, ' '),
                    testClientHost, testUser, "runLoad["+dbSubPath+"] ");
        }
        
    }
    
    private void runStateTest(String stateTest,
            String clientVM,
            String testClientHost,
            String masterHost, int masterPort, // serverHost?, serverPort?
            String dbSubPath) // FIXME? Should we allow extra URL options?
            throws Exception
    {
        util.DEBUG("runStateTest(" + stateTest
                + ", " + clientVM
                + ", " + testClientHost
                + ", " + masterHost
                + ", " + masterPort
                + ", " + dbSubPath
                + ") "
                );
        
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        String URL = masterLoadURL(dbSubPath);
        String ijClassPath = derbyVersion +FS+ "derbyclient.jar"
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
                + PS + derbyVersion +FS+ "derbyTesting.jar"
                // Needed for 'run resource 'createTestProcedures.subsql';' cases?
                // Nope? what is 'resource'?
                + PS + derbyVersion +FS+ "derbytools.jar";
        String testingClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbynet.jar" // WHY IS THIS NEEDED?
                // See TestConfiguration: startNetworkServer and stopNetworkServer
                + PS + test_jars;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        String clientJvm = ReplicationRun.getClientJavaExecutableName();
        
        String command = null;
        
        if ( masterHost.equals("localhost") )
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        { // Use full classpath when running locally. Can not vary server versions!
            ijClassPath = classPath;
            testingClassPath = classPath;
        }
        util.DEBUG("stateTest: " + stateTest);
        if ( stateTest.indexOf(".sql") >= 0 )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
            command = clientJvm 
                    + " -Dij.driver=" + DRIVER_CLASS_NAME
                    + " -Dij.connection.startTestClient=" + URL
                    + " -classpath " + ijClassPath + " org.apache.derby.tools.ij"
                    + " " + stateTest
                    ;
        }
        else
        {
            /* BEGIN For junit: */
            command = "cd "+ userDir +";" // Must be positioned where the properties file is located.
                    + clientJvm 
                    + " -Dderby.tests.trace=true"
                    // + " -Djava.security.policy=\"<NONE>\""  // Now using noSecurityManager decorator
                    + " -classpath " + testingClassPath
                    + " junit.textui.TestRunner"
                    + " " + stateTest
                    ;
            /* END */
        }
        
        /* String results = */
        runUserCommandRemotely(command,
                testClientHost, // masterHost,
                testUser,
                // dbSubPath,
                "runStateTest "); // ["+dbSubPath+"]
        
    }
    
    void bootMasterDatabase(String clientVM, 
            String dbSubPath,
            String dbName,
            String masterHost,  // Where the command is to be executed.
            int masterServerPort, // master server interface accepting client requests
            String load)
        throws Exception
    {
        // Should just do a "connect....;create=true" here, instead of copying in initMaster.
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        String URL = masterURL(dbName)
                +";create=true"
                +useEncryption(true);

        {
            util.DEBUG("bootMasterDatabase getConnection("+URL+")");
            // Needed when running from classes and also when running
            // from the module path after the upgrade tests have unloaded
            // the drivers.
            DriverManager.registerDriver(new ClientDriver());
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

            Connection conn = DriverManager.getConnection(URL);
            conn.close();
        }
        
        // NB! should be done by startMaster. Preliminary needs to freeze db before copying to slave and setting replication mode.
        util.DEBUG("************************** DERBY-???? Preliminary needs to freeze db before copying to slave and setting replication mode.");
        
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
             URL = masterURL(dbName);
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
            Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
            util.DEBUG("bootMasterDatabase getConnection("+URL+")");
            Connection conn = DriverManager.getConnection(URL);
            Statement s = conn.createStatement();
            s.execute("call syscs_util.syscs_freeze_database()");
            conn.close();
        }
        
        if ( load != null )
        {
            runLoad(load,
                    clientVM, // jvmVersion,
                    testClientHost,
                    masterServerHost, masterServerPort,
                    dbSubPath+FS+dbName);
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        util.DEBUG("bootMasterDatabase done.");
    }

    /**
     * Set master db in replication master mode.
     */
    void startMaster(String clientVM,
            String dbName,
            String masterHost,  // Where the command is to be executed.
            int masterServerPort, // master server interface accepting client requests
            String slaveClientInterface, // Will be = slaveReplInterface = slaveHost if only one interface card used.
            int slaveServerPort, // masterPort, // Not used since slave don't accept client requests
            String slaveReplInterface, // slaveHost,
            int slaveReplPort) // slavePort)
            throws Exception
    {
        if ( masterHost.equalsIgnoreCase("localhost") )
        {
            startMaster_direct(dbName,
                    masterHost,  masterServerPort,
                    slaveReplInterface, slaveReplPort);
        }
        else
        {
            startMaster_ij(dbName,
//IC see: https://issues.apache.org/jira/browse/DERBY-5318
                    masterHost, 
                    slaveReplInterface, slaveReplPort);
        }
    }

    private void startMaster_ij(String dbName,
            String masterHost, // Where the master db is run.
            String slaveReplInterface, // master server interface accepting client requests
            int slaveReplPort)
            throws Exception
    {
        
        String URL = masterURL(dbName)
                +";startMaster=true;slaveHost="+slaveReplInterface
                +";slavePort="+slaveReplPort;
        String ijClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbytools.jar";
        if ( masterHost.equals("localhost") )
        { // Use full classpath when running locally. Can not vary server versions!
            ijClassPath = classPath;
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        String clientJvm = ReplicationRun.getMasterJavaExecutableName();
        
        String command = clientJvm
                + " -Dij.driver=" + DRIVER_CLASS_NAME
                + " -Dij.connection.startMaster=\"" + URL + "\""
                + " -classpath " + ijClassPath + " org.apache.derby.tools.ij"
                + " " + userHome + FS + "ij_dummy_script.sql"
                ;
        
        String results =
                runUserCommandRemotely(command,
                masterHost, // Must be run on the master!
                testUser,
                "startMaster_ij ");
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG(results);
    }
    private void startMaster_direct(String dbName,
            String masterHost,  // Where the master db is run.
            int masterServerPort, // master server interface accepting client requests
            
            String slaveReplInterface, // slaveHost,
            int slaveReplPort)
            throws Exception
    {
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        String URL = masterURL(dbName)
                +";startMaster=true;slaveHost="+slaveReplInterface
                +";slavePort="+slaveReplPort;
                
            util.DEBUG("startMaster_direct getConnection("+URL+")");
            Connection conn = null;
            boolean done = false;
            int count = 0;
            while ( !done )
            {
                try
                {
                    /* On 1.5 locking of Drivermanager.class prevents
                     * using DriverManager.getConnection() concurrently
                     * in startMaster and startSlave!
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
                    Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
                    conn = DriverManager.getConnection(URL);
                     */
                    String  connectionAttributes = "startMaster=true"
                        +";slaveHost="+slaveReplInterface
                        +";slavePort="+slaveReplPort
                        +useEncryption(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-5955
                    ClientDataSourceInterface ds = configureDataSource
                        ( masterDbPath( dbName ), masterHost, masterServerPort, connectionAttributes );
                    conn = ds.getConnection();
                    
                    done = true;
                    conn.close();
//IC see: https://issues.apache.org/jira/browse/DERBY-3738
                    util.DEBUG("startMaster_direct connected in " + count + " * 100ms.");
                }
                catch ( SQLException se )
                {
                    int errCode = se.getErrorCode();
                    String msg = se.getMessage();
                    String sState = se.getSQLState();
//IC see: https://issues.apache.org/jira/browse/DERBY-3126
                    String expectedState = "XRE04";
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
                    util.DEBUG("startMaster Got SQLException: " 
                            + errCode + " " + sState + " " + msg + ". Expected " + expectedState);
//IC see: https://issues.apache.org/jira/browse/DERBY-2601
                    if ( (errCode == 40000)
                    && (sState.equalsIgnoreCase(expectedState) ) )
                    {
                        if (count++ > 1200) {
                            // Have tried for 1200 * 100 ms == 2 minutes
                            // without success. Give up.
                            fail("startMaster did not succeed", se);
                        }
                        util.DEBUG("Not ready to startMaster. "
                                +"Beware: Will also report "
                                + "'... got a fatal error for database '...../<dbname>'"
                                + " in master derby.log.");
                        Thread.sleep(100L); // ms.
                    }
                    else
                    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3417
                        if (REPLICATION_MASTER_TIMED_OUT.equals(sState)) // FIXME! CANNOT_START_MASTER_ALREADY_BOOTED
                        {
                            util.DEBUG("Master already started?");
                        }
                        util.DEBUG("startMaster_direct Got: "
                                +state+" Expected "+expectedState);
                        throw se;
                    }
                }
            }
            util.DEBUG("startMaster_direct exit.");
    }
    
    /**
     * Get a connection to the master database.
     * @return A connection to the master database
     */
    protected Connection getMasterConnection() throws SQLException {
        if (masterConn == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
            String url = masterURL(replicatedDb);
            masterConn = DriverManager.getConnection(url);
        }
        return masterConn;
    }
    
    /**
     * Get a connection to the slave database.
     * @return A connection to the slave database
     */
    protected Connection getSlaveConnection() throws SQLException {
        if (slaveConn == null) {
            String url = slaveURL(replicatedDb);
            slaveConn = DriverManager.getConnection(url);
        }
        return slaveConn;
    }


    /**
     * Execute SQL on the master database through a Statement
     * @param sql The sql that should be executed on the master database
     * @throws java.sql.SQLException thrown if an error occured while
     * executing the sql
     */
    protected void executeOnMaster(String sql) throws SQLException {
         Statement s = getMasterConnection().createStatement();
         s.execute(sql);
         s.close();
    }

    /**
     * Execute SQL on the slave database through a Statement
     * @param sql The sql that should be executed on the slave database
     * @throws java.sql.SQLException thrown if an error occured while
     * executing the sql
     */
    protected void executeOnSlave(String sql) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
         Statement s = getSlaveConnection().createStatement();
         s.execute(sql);
         s.close();
    }

    /**
     * Set slave db in replication slave mode
     */
    void startSlave(String clientVM,
            String dbName,
            String slaveClientInterface, // slaveHost, // Where the command is to be executed.
            int slaveServerPort,
            String slaveReplInterface,
            int slaveReplPort,
            String testClientHost)
    throws Exception
    {
        if ( testClientHost.equalsIgnoreCase("localhost") )
        {
            startSlave_direct(dbName,
                    slaveClientInterface, slaveServerPort,
                    slaveReplInterface,slaveReplPort);
        }
        else
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-5318
            startSlave_ij(
                    dbName,
                    slaveClientInterface,
                    slaveReplInterface, slaveReplPort);
        }
    }

    private void startSlave_ij(
            String dbName,
            String slaveHost,  // Where the slave db is run.
            String slaveReplInterface, // slaveHost,
            int slaveReplPort)
            throws Exception
    {
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        String URL = slaveURL(dbName)
                +";startSlave=true;slaveHost="+slaveReplInterface
                +";slavePort="+slaveReplPort;
        String ijClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbytools.jar";
        if ( slaveHost.equals("localhost") )
        { // Use full classpath when running locally. Can not vary server versions!
            ijClassPath = classPath;
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        String clientJvm = ReplicationRun.getSlaveJavaExecutableName();
        
        String command = clientJvm
                + " -Dij.driver=" + DRIVER_CLASS_NAME
                + " -Dij.connection.startSlave=\"" + URL + "\""
                + " -classpath " + ijClassPath + " org.apache.derby.tools.ij"
                + " " + userHome + FS + "ij_dummy_script.sql"
                ;
        
        runUserCommandInThreadRemotely(command,
                slaveHost, // Run on the slave.
                testUser,
                "startSlave_ij ");
        
    }
    private void startSlave_direct(String dbName,
            String slaveHost,  // Where the slave db is run.
            int slaveServerPort, // slave server interface accepting client requests
            String slaveReplInterface,
            int slaveReplPort)
            throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        final String URL = slaveURL(dbName)
                +";startSlave=true;slaveHost="+slaveReplInterface
                +";slavePort="+slaveReplPort;
        
            util.DEBUG("startSlave_direct getConnection("+URL+")");
            
            final String fDbPath = slaveDbPath( dbName );
            final String fSlaveHost = slaveHost;
            final int fSlaveServerPort = slaveServerPort;
            final String fConnAttrs = "startSlave=true"
                                +";slaveHost="+slaveReplInterface
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
                                +";slavePort="+slaveReplPort
                                +useEncryption(false);
            Thread connThread = new Thread(
                    new Runnable()
            {
                public void run()
                {
                    startSlaveException = null;
                    Connection conn = null;
                    try {
                        // NB! WIll hang here until startMaster is executed!
                        /*On 1.5 locking of Drivermanager.class prevents
                         * using DriverManager.getConnection() concurrently
                         * in startMaster and startSlave!
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
                        Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
                        conn = DriverManager.getConnection(URL);
                         */
//IC see: https://issues.apache.org/jira/browse/DERBY-5955
                        ClientDataSourceInterface ds = configureDataSource(
                            fDbPath, fSlaveHost, fSlaveServerPort, fConnAttrs );
                        conn = ds.getConnection();
                        conn.close();
                    }
                    catch (SQLException se)
                    {
                        startSlaveException = se;
                    }
                    catch (Exception ex)
                    {
                        startSlaveException = ex;
                    }
                }
            }
            );
            connThread.start();
//IC see: https://issues.apache.org/jira/browse/DERBY-5517
            registerThread(connThread);
            util.DEBUG("startSlave_direct exit.");
    }
    

    void failOver(String jvmVersion,
            String dbPath, String dbSubPath, String dbName,
            String host,  // Where the db is run.
            int serverPort,
            
            String testClientHost)
            throws Exception
    {
        if ( host.equalsIgnoreCase("localhost") )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-5318
            failOver_direct(dbName);
        }
        else
        {
            failOver_ij(dbName, host, testClientHost);
        }

    }
    private void failOver_ij(
            String dbName,
            String host,  // Where the db is run.
            String testClientHost)
            throws Exception
    {
        
        String URL = masterURL(dbName)
                +";failover=true";
        String ijClassPath = derbyVersion +FS+ "derbyclient.jar"
                + PS + derbyVersion +FS+ "derbytools.jar";
        if ( host.equals("localhost") )
        { // Use full classpath when running locally. Can not vary server versions!
            ijClassPath = classPath;
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        String clientJvm = ReplicationRun.getClientJavaExecutableName();
        
        String command = clientJvm
                + " -Dij.driver=" + DRIVER_CLASS_NAME
                + " -Dij.connection.failover=\"" + URL + "\""
                + " -classpath " + ijClassPath + " org.apache.derby.tools.ij"
                + " " + userHome + FS + "ij_dummy_script.sql"
                ;
        
        // Execute the ij command on the testClientHost as testUser
        String results =
                runUserCommandRemotely(command,
                testClientHost,
                testUser,
                "failOver_ij ");
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG(results);
    }
    private void failOver_direct(String dbName)
            throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        String URL = masterURL(dbName)
                +";failover=true";
               
            util.DEBUG("failOver_direct getConnection("+URL+")");

            try
            {
                Class.forName(DRIVER_CLASS_NAME); // Needed when running from classes!
//IC see: https://issues.apache.org/jira/browse/DERBY-5318
                DriverManager.getConnection(URL);
            }
            catch (SQLException se)
            {
                int errCode = se.getErrorCode();
                String msg = se.getMessage();
//IC see: https://issues.apache.org/jira/browse/DERBY-3738
                String sState = se.getSQLState();
                String expectedState = "XRE20";
                msg = "failOver_direct Got SQLException: " 
                        + errCode + " " + sState + " " + msg 
                        + ". Expected: " + expectedState;
                util.DEBUG(msg);
//IC see: https://issues.apache.org/jira/browse/DERBY-3709
                BaseJDBCTestCase.assertSQLState(expectedState, se);
            }
   }
    
    int xFindServerPID(String serverHost, int serverPort)
    throws InterruptedException
    {
        if ( serverHost.equalsIgnoreCase("localhost") ) 
        { // Assuming we do not need the PID.
            return 0;
        }
        int pid = -1;

        String p1 = "ps auxwww"; // "/bin/ps auxwww";
        String p2 = " | grep " + serverPort; // /bin/grep
        String p3 = " | grep '.NetworkServerControl start -h '"; // /bin/grep
        String p4 = ""; // | /bin/grep '/trunk_slave/jars/'"; // Also used for master...
        String p5 = " | grep -v grep"; // /bin/grep
        String p6 = " | grep -v ssh"; // /bin/grep
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        String p7 = " | grep -v bash";  // /bin/grep// Assuming always doing remote command (ssh)
        String p8 = " | gawk '{ print $2 }'"; // /bin/gawk
        String p9 = " | head -1"; // For cases where we also get some error...
        
        String command = p1 + p2 + p3 + p4 + p5 + p6 + p7 /* + p8 */ + ";";
//IC see: https://issues.apache.org/jira/browse/DERBY-5783
        String result = runUserCommandRemotely(
                command, serverHost, testUser, "ps");
        util.DEBUG("xFindServerPID: '" + result + "'");
        // result = result.split(" ")[1]; // Without ' + p8 ' should show full line. But fails with PID less than 10000!
        command = p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + p9 + ";";
        result = runUserCommandRemotely(command, serverHost, testUser, "ps");
        if ( result == null )
        {util.DEBUG("xFindServerPID: Server process not found");return -1;} // Avoid error on parseInt below
        util.DEBUG("xFindServerPID: '" + result + "'");
        pid = Integer.parseInt(result.trim());
        util.DEBUG("xFindServerPID: " + pid);
        return pid;
    }
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
    void xStopServer(String serverHost, int serverPID)
    throws InterruptedException
    {
        if ( serverPID == -1 || serverPID == 0 )
        {util.DEBUG("Illegal PID");return;}
        String command = "kill " + serverPID;
        runUserCommandRemotely(command,
                serverHost,
                testUser,
                "xStopServer");
    }
    
    void verifySlave()
    throws Exception
    {
        util.DEBUG("BEGIN verifySlave "+slaveServerHost+":"
                   +slaveServerPort+"/"+slaveDbPath( replicatedDb ) );
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        if ( (replicationTest != null) // If 'replicationTest==null' no table was created/filled
                && simpleLoad )
        {
            _verifyDatabase(slaveServerHost, slaveServerPort, 
                    slaveDbPath( replicatedDb ),
                    simpleLoadTuples);
            // return;
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5955
        ClientDataSourceInterface ds = configureDataSource
            ( slaveDbPath( replicatedDb ), slaveServerHost, slaveServerPort, useEncryption(false) );
        Connection conn = ds.getConnection();
            
        simpleVerify(conn);
        conn.close();
        /* BEGIN Distributed repl. tests only */
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        if ( !slaveServerHost.equalsIgnoreCase("localhost") ){
        runSlaveVerificationCLient(jvmVersion,
                testClientHost,
                replicatedDb,
                slaveServerHost, slaveServerPort);}
        /* END Distributed repl. tests only */
        util.DEBUG("END   verifySlave");
    }
    void verifyMaster()
    throws Exception
    {
        util.DEBUG("BEGIN verifyMaster " + masterServerHost + ":"
                   +masterServerPort+"/"+masterDbPath( replicatedDb ) );
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        if ( (replicationTest != null)  // If 'replicationTest==null' no table was created/filled
                && simpleLoad )
        {
            _verifyDatabase(masterServerHost, masterServerPort, 
                    masterDbPath( replicatedDb ),
                    simpleLoadTuples);
            // return;
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5955
        ClientDataSourceInterface ds = configureDataSource
            ( masterDbPath( replicatedDb ), masterServerHost, masterServerPort, useEncryption(false) );
        Connection conn = ds.getConnection();
            
        simpleVerify(conn);
        conn.close();
        /* BEGIN Distributed repl. tests only */
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        if ( !masterServerHost.equalsIgnoreCase("localhost") ){
        runMasterVerificationCLient(jvmVersion,
                testClientHost,
                replicatedDb,
                masterServerHost, masterServerPort);}
        /* END Distributed repl. tests only */
        util.DEBUG("END   verifyMaster");
    }
    private void simpleVerify(Connection conn) // Verification code..
    throws SQLException
    {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select SCHEMAID, TABLENAME from sys.systables");
        while (rs.next())
        {
            util.DEBUG(rs.getString(1) + " " + rs.getString(2));
        }
    }
    
    private void runSlaveVerificationCLient(String jvmVersion,
            String testClientHost,
            String dbName,
            String serverHost,
            int serverPort)
            throws Exception
    {
        util.DEBUG("runSlaveVerificationCLient");
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        if ( replicationVerify != null){
        runTestOnSlave(replicationVerify,
                jvmVersion,
                testClientHost,
                serverHost,serverPort,
                dbName);
        }
    }
    
    private void runMasterVerificationCLient(String jvmVersion,
            String testClientHost,
            String dbName,
            String serverHost,
            int serverPort)
            throws Exception
    {
        util.DEBUG("runMasterVerificationCLient");
        if ( replicationVerify != null ){
        runTest(replicationVerify,
                jvmVersion,
                testClientHost,
                serverHost,serverPort,
                dbName);
        }
    }

    /**
     * Run a Java command locally on the test host. The spawned process
     * inherits the class path from the main test process.
     *
     * @param command the arguments to pass to the Java executable
     * @param ID an identifier used to prefix debug output
     * @param workingDir the directory in which the sub-process should run, or
     *    {@code null} to run in the same directory as the parent process
     */
    private void runUserCommandLocally(
//IC see: https://issues.apache.org/jira/browse/DERBY-5791
            String[] command, String ID, File workingDir) {
        util.DEBUG("");
        final String debugId = "runUserCommandLocally " + ID + " ";
        util.DEBUG(debugId+command);
        
        {
            util.DEBUG(debugId + "localCommand: " + Arrays.asList(command));
            
            try
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-5808
                Process proc = execJavaCmd(null, null, command, workingDir);
                processDEBUGOutput(debugId+"pDo ", proc);
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }
        
        util.DEBUG(debugId+"--- runUserCommandLocally ");
        util.DEBUG("");
        
    }

    private String runUserCommandRemotely(String command,
            String host,
            String testUser,
            String id)
    {
        final String ID= "runUserCommandRemotely "+id+" ";
        util.DEBUG(ID+"Execute '"+ command +"' on '"+ host +"'" + " as " + testUser);
        
        String localCommand = remoteShell + " "
                + "-l " + testUser + " " + host + " "
                + command
                ;

//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        String output = "";
        try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(localCommand);
            output = processOutput(ID, proc);
            int exitVal = proc.waitFor();
            util.DEBUG("ExitValue: " + exitVal);
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return output;
    }

    /**
     * Run a Java command locally on the test host in a separate thread. The
     * spawned process inherits the class path from the main test process.
     *
     * @param command the arguments to pass to the Java executable
     * @param dbDir the name of the sub-directory in which the sub-process
     *   should run, or {@code null} to run in the same directory as the
     *   parent process
     * @param id an identifier used to prefix debug output
     */
    private void runUserCommandInThreadLocally(final String[] command,
            String dbDir,
            String id)
    {
        util.DEBUG("");
        final String ID = "runUserCommandInThread "+id+" ";
        util.DEBUG(ID + "Execute '"+ command +"'");
        util.DEBUG("+++ "+ID);
     
        util.DEBUG("runUserCommand: " + command );

        String workingDirName = System.getProperty("user.dir");
        util.DEBUG("user.dir: " + workingDirName);

        // If dbDir is specified, start the process in that directory;
        // otherwise, inherit working dir from the main test process.
//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        final File workingDir = dbDir == null ?
                null : new File(workingDirName, dbDir);
        util.DEBUG(ID + "workingDir: " + workingDir);

        {
            Thread cmdThread = new Thread(
                    new Runnable()
            {
                public void run()
                {
                    util.DEBUG(ID+"************** In run().");
                    runUserCommandLocally(command, ID, workingDir);
                    util.DEBUG(ID+"************** Done run().");
                }
            }
            );
            util.DEBUG(ID+"************** Do .start().");
            cmdThread.start();
//IC see: https://issues.apache.org/jira/browse/DERBY-5643
            registerThread(cmdThread);
        }
     
        util.DEBUG(ID+"--- ");
        util.DEBUG("");
    }
    
    // FIXME: NB NB Currently only invoked from startSlave_ij (others unused!)
    private void runUserCommandInThreadRemotely(String command,
//IC see: https://issues.apache.org/jira/browse/DERBY-5791
            final String host,
            final String testUser,
            String id)
    {
        util.DEBUG("");
        final String ID=id+" runUserCommandInThreadRemotely ";
        util.DEBUG(ID+"+++ ");
        util.DEBUG(ID+"Execute '"+ command +"' on '"+ host +"'");
        
        util.DEBUG(ID + command
                + " @ " + host
                + " as " + testUser);
        
        final String[] envElements = {"CLASS_PATH="+""
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
                , "PATH="+FS+"home"+FS+testUser+FS+"bin:$PATH" // "/../bin" FIXME!!! All such!
                };
        
        String workingDirName = System.getProperty("user.home");
        util.DEBUG(ID+"user.home: " + workingDirName);
//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        util.DEBUG(ID+"envElements: " + util.splice(envElements, ' '));
        util.DEBUG(ID+"workingDir: " + workingDirName);
        
        {
            util.DEBUG(ID+"Running command on non-local host "+ host);
            
            String[] shEnvElements = {"CLASS_PATH="+""
                    , "PATH="+FS+"home"+FS+testUser+FS+"bin:${PATH}"
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
            };
//IC see: https://issues.apache.org/jira/browse/DERBY-5791
            String shellEnv = util.splice(shEnvElements, ';');
            util.DEBUG(ID+"shellEnv: " + shellEnv);

            // user.home aka workingDirName must be accessible from master,
            // slave and client.
            final String shellCmd = "cd " + workingDirName + ";pwd;"
                    + shellEnv + ";"
                    + command;
            
            util.DEBUG(ID+"shellCmd: " + shellCmd);
            
            Thread serverThread = new Thread(
                    new Runnable()
            {
                public void run()
                {
                    util.DEBUG(ID+"************** In run().");
                    runUserCommandRemotely(shellCmd, host, testUser, ID);
                    util.DEBUG(ID+"************** Done exec().");
                }
            }
            );
            util.DEBUG(ID+"************** Do .start(). ");
            serverThread.start();
//IC see: https://issues.apache.org/jira/browse/DERBY-5517
//IC see: https://issues.apache.org/jira/browse/DERBY-5517
            registerThread(serverThread);
            
        }
        
        util.DEBUG(ID+"--- ");
        util.DEBUG("");
    }
        
    void initEnvironment()
    throws IOException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.printDebug = System.getProperty("derby.tests.repltrace", "false")
                                                     .equalsIgnoreCase("true");
        util.DEBUG("printDebug: " + util.printDebug);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG("*** ReplicationRun.initEnvironment -----------------------------------------");
        util.DEBUG("*** Properties -----------------------------------------");
        userDir = System.getProperty("user.dir");
        util.DEBUG("user.dir:          " + userDir);
        
        util.DEBUG("derby.system.home: " + System.getProperty("derby.system.home"));
        
        showSysinfo = true;
        util.DEBUG("showSysinfo: " + showSysinfo);
        
        testUser = null;
        util.DEBUG("testUser: " + testUser);
        
        masterServerHost = "localhost";
        util.DEBUG("masterServerHost: " + masterServerHost);
        
        util.DEBUG("masterServerPort: " + masterServerPort);
        
        slaveServerHost = "localhost";
        util.DEBUG("slaveServerHost: " + slaveServerHost);
        
        util.DEBUG("slaveServerPort: " + slaveServerPort);
        
        util.DEBUG("slaveReplPort: " + slaveReplPort);
        
        testClientHost = "localhost";
        util.DEBUG("testClientHost: " + testClientHost);
        
        masterDatabasePath = userDir;
        util.DEBUG("masterDatabasePath: " + masterDatabasePath);
        
        slaveDatabasePath = userDir;
        util.DEBUG("slaveDatabasePath: " + slaveDatabasePath);
        
        replicatedDb = "wombat";
        util.DEBUG("replicatedDb: " + replicatedDb);
        
        bootLoad = null;
        util.DEBUG("bootLoad: " + bootLoad);
        
        freezeDB = null;
        util.DEBUG("freezeDB: " + freezeDB);
        
        unFreezeDB = null;
        util.DEBUG("unFreezeDB: " + unFreezeDB);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
        simpleLoad = System.getProperty("derby.tests.replSimpleLoad", "true")
                                                     .equalsIgnoreCase("true");
        util.DEBUG("simpleLoad: " + simpleLoad);
        
        /* Done in subclasses
        replicationTest = "org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationTestRun";
        util.DEBUG("replicationTest: " + replicationTest);
        replicationVerify = "org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationTestRunVerify";
        util.DEBUG("replicationVerify: " + replicationVerify);
         */
        
        sqlLoadInit = null;
        util.DEBUG("sqlLoadInit: " + sqlLoadInit);

        
        specialTestingJar = null;
        util.DEBUG("specialTestingJar: " + specialTestingJar);
        
        jvmVersion = System.getProperty("java.home") +FS+"lib";
        util.DEBUG("jvmVersion: " + jvmVersion);
        
        masterJvmVersion = null;
        if ( masterJvmVersion == null )
        {masterJvmVersion = jvmVersion;}
        util.DEBUG("masterJvmVersion: " + masterJvmVersion);
        
        slaveJvmVersion = null;
        if ( slaveJvmVersion == null )
        {slaveJvmVersion = jvmVersion;}
        util.DEBUG("slaveJvmVersion: " + slaveJvmVersion);
        
        classPath = System.getProperty("java.class.path"); util.DEBUG("classPath: " + classPath);
        
        util.DEBUG("derbyVersion: " + derbyVersion);
        
        derbyMasterVersion = null;
        if ( derbyMasterVersion == null )
        {derbyMasterVersion = derbyVersion;}
        util.DEBUG("derbyMasterVersion: " + derbyMasterVersion);
        
        derbySlaveVersion = null;
        if ( derbySlaveVersion == null )
        {derbySlaveVersion = derbyVersion;}
        util.DEBUG("derbySlaveVersion: " + derbySlaveVersion);
        
        String derbyTestingJar = derbyVersion + FS+"derbyTesting.jar";
        if ( specialTestingJar != null )  derbyTestingJar = specialTestingJar;
        util.DEBUG("derbyTestingJar: " + derbyTestingJar);
        
        junit_jar = derbyVersion + FS+"junit.jar";
        util.DEBUG("junit_jar: " + junit_jar);
        
        test_jars = derbyTestingJar
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
                + PS + junit_jar;
        util.DEBUG("test_jars: " + test_jars);
        
        sleepTime = 15000;
        util.DEBUG("sleepTime: " + sleepTime);
        
        runUnReplicated = false;
        util.DEBUG("runUnReplicated: " + runUnReplicated);
        
        localEnv = false;
        util.DEBUG("localEnv: " + localEnv);
        
        derbyProperties = 
                 "derby.infolog.append=true"+LF
                +"derby.drda.logConnections=true"+LF
                +"derby.drda.traceAll=true"+LF;

        
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG("--------------------------------------------------------");
        
        masterPreRepl = null; // FIXME!
        masterPostRepl = null; // FIXME!
        slavePreSlave = null; // FIXME!
        masterPostSlave = null; // FIXME!
        slavePostSlave = null; // FIXME!
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG("--------------------------------------------------------");
        // for SimplePerfTest
//IC see: https://issues.apache.org/jira/browse/DERBY-3738
        tuplesToInsertPerf = 10000;
        commitFreq = 1000; // "0" is autocommit
        
        util.DEBUG("--------------------------------------------------------");
        
            // FIXME! state.initEnvironment(cp);
        
        util.DEBUG("--------------------------------------------------------");

    }
    
    void initMaster(String host, String dbName)
    throws Exception
    {
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5318
        File masterHome = new File(masterDatabasePath, masterDbSubPath);
        File slaveHome = new File(slaveDatabasePath, slaveDbSubPath);
        util.DEBUG("initMaster");
        
        /* bootMasterDataBase now does "connect ...;create=true" */
        
        String results = null;
        if ( host.equalsIgnoreCase("localhost") || localEnv )
        {
            if (PrivilegedFileOpsForTests.exists(masterHome)) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5836
                BaseTestCase.assertDirectoryDeleted(masterHome);
            }
            util.mkDirs(masterHome.getPath()); // Create the directory
            
            // Ditto for slave:
            if (PrivilegedFileOpsForTests.exists(slaveHome)) {
                BaseTestCase.assertDirectoryDeleted(slaveHome);
            }
            util.mkDirs(slaveHome.getPath()); // Create the directory
            
            // util.writeToFile(derbyProperties, dir+FS+"derby.properties");
        }
        else
        {            
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
            String command = "mkdir -p "+masterDatabasePath+FS+masterDbSubPath+";"
                + " cd "+masterDatabasePath+FS+masterDbSubPath+";"
                + " rm -rf " + dbName + " derby.log;"
                + " rm -f Server*.trace;"
                + " ls -al;"
                ;
            results = 
                runUserCommandRemotely(command,
                host,
                testUser,
                "initMaster ");
        }
        util.DEBUG(results);
        
        
    }
    private void removeSlaveDBfiles(String host, String dbName)
    throws InterruptedException
    {
        /*
         * PoC:
         * cd /home/user/Replication/testing/db_slave
         * rm -f test/seg0/*
         */
        
        String command = "cd " + slaveDatabasePath+FS+slaveDbSubPath+";"
                + " rm -f " + dbName + FS + "seg0" + FS + "* ;"
                + " ls -al test test/seg0" // DEBUG
                ;
        
        String results =
                runUserCommandRemotely(command,
                host,
                testUser,
                // dbName, // unneccessary?
                "removeSlaveDBfiles ");
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG(results);
        
    }
    void initSlave(String host, String clientVM, String dbName)
    throws Exception
    {
        
        util.DEBUG("initSlave");
//IC see: https://issues.apache.org/jira/browse/DERBY-5318
        File slaveHome = new File(slaveDatabasePath, slaveDbSubPath);
        File masterHome = new File(masterDatabasePath, masterDbSubPath);
        File masterDb = new File(masterHome, dbName);
        
        String results = null;
        if ( host.equalsIgnoreCase("localhost") || localEnv )
        {
            // The slaveDb dir is cleaned by initMaster! NB NB SHOULD THIS BE SO?
            // util.cleanDir(slaveDb, true); // true: do delete the db directory itself.
                                          // derby.log etc will be kept.
            // Copy (.../master/test) into (.../slave/).
            File slaveDb = new File(slaveHome, dbName);
            PrivilegedFileOpsForTests.copy(masterDb, slaveDb);

            // util.writeToFile(derbyProperties, slaveDir+FS+"derby.properties");
        }
        else
        {
            String command = "mkdir -p " + slaveHome.getPath() + ";"
                + " cd " + slaveHome.getPath() +";"
                + " rm -rf " + dbName + " derby.log;"
                + " rm -f Server*.trace;"
                + " scp -r " + masterServerHost + ":" + masterDb.getPath() +"/ .;" // Copying the master DB.
                + " ls -al" // DEBUG
                ;
        
            results =
                runUserCommandRemotely(command,
                host,
                testUser,
                "initSlave ");
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG(results);
        
    }
    // ?? The following should be moved to a separate class, subclass this and
    // ?? CompatibilityCombinations
    void restartServer(String serverVM, String serverVersion,
            String serverHost,
            String interfacesToListenOn,
            int serverPort,
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
            String dbSubDirPath)
            throws Exception
    {
            stopServer(serverVM, serverVersion,
                    serverHost, serverPort);
            
            startServer(serverVM, serverVersion,
                    serverHost,
                    interfacesToListenOn, 
                    serverPort,
                    dbSubDirPath); // Distinguishing master/slave
    }
//IC see: https://issues.apache.org/jira/browse/DERBY-5729
    void startServer(String serverVM, String serverVersion,
            String serverHost,
            String interfacesToListenOn,
            int serverPort,
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
            String dbSubDirPath)
            throws Exception
    {
        util.DEBUG("");
         
        final String debugId = "startServer@" + serverHost + ":" + serverPort + " ";
        util.DEBUG(debugId+"+++ StartServer " + serverVM + " / " + serverVersion);
                
        String serverClassPath = serverVersion + FS+"derby.jar"
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
                + PS + serverVersion + FS+"derbynet.jar"
                + PS + test_jars; // Required if the test (run on the client) 
                                  // defines and uses functions on test classes.
                                  // Example: PADSTRING in StressMultiTest.

//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        final boolean isRemote = !serverHost.equals("localhost");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4980
        String workingDirName = masterDatabasePath +FS+ dbSubDirPath;
        
        ArrayList<String> ceArray = new ArrayList<String>();

        // For remote tests, we need to specify the Java VM to use and the
        // classpath. For local tests, we'll just use the JVM and the classpath
        // BaseTestCase.execJavaCmd() gives us. Note that this means we cannot
        // vary server versions when running locally.
        if (isRemote) {
            ceArray.add( ReplicationRun.getMasterJavaExecutableName() );
            ceArray.add( "-cp" );
            ceArray.add( serverClassPath );
        }
        ceArray.add( "-Dderby.system.home=" + workingDirName );
        ceArray.add( "-Dderby.infolog.append=true" );
        //ceArray.add( " -Dderby.language.logStatementText=true" ); // Goes into derby.log: Gets HUGE );
        if ( db_uid != null )
        {
            ceArray.add( "-Dderby.authentication.provider=NATIVE:" + replicatedDb + ":LOCAL" );
        }
        ceArray.add( networkServerControl );
        ceArray.add( "start" );
        ceArray.add( "-h" );
        ceArray.add( interfacesToListenOn ); // allowedClient
        ceArray.add( "-p" );
        ceArray.add( String.valueOf( serverPort ) );
        ceArray.add( "-noSecurityManager" );

        final String[] commandElements = util.toStringArray(ceArray);
//IC see: https://issues.apache.org/jira/browse/DERBY-5791

        if (!isRemote)
        {
            util.DEBUG(debugId+"Starting server on localhost "+ serverHost);
            runUserCommandInThreadLocally(commandElements, null, debugId);
        }
        else
        {
            util.DEBUG(debugId+"Starting server on non-local host "+ serverHost);
            String fullCmd = util.splice(commandElements, ' ');
            runUserCommandInThreadRemotely(
                    fullCmd, serverHost, testUser, debugId);
        }

        // Wait for the server to come up in a reasonable time.
        pingServer(serverHost, serverPort);
//IC see: https://issues.apache.org/jira/browse/DERBY-5643

        util.DEBUG(debugId+"--- StartServer ");
        util.DEBUG("");
    }
    /* 
    private NetworkServerControl startServer_direct(String serverHost, 
            String interfacesToListenOn, 
            int serverPort, 
            String fullDbDirPath,
            String securityOption) // FIXME? true/false?
    throws Exception
//IC see: https://issues.apache.org/jira/browse/DERBY-3126
    { // Wotk in progress. Not currently used! Only partly tested!
        util.DEBUG("startServer_direct " + serverHost 
                + " " + interfacesToListenOn +  " " + serverPort
                + " " + fullDbDirPath);
        assertTrue("Attempt to start server on non-localhost: " + serverHost, 
                serverHost.equalsIgnoreCase("localhost"));
        
        System.setProperty("derby.system.home", fullDbDirPath);
        System.setProperty("user.dir", fullDbDirPath);
        
        NetworkServerControl server = new NetworkServerControl(
                InetAddress.getByName(interfacesToListenOn), serverPort);
        
        server.start(null); 
//IC see: https://issues.apache.org/jira/browse/DERBY-3738
        pingServer(serverHost, serverPort, 150);
        
        Properties sp = server.getCurrentProperties();
        sp.setProperty("noSecurityManager", 
                securityOption.equalsIgnoreCase("-noSecurityManager")?"true":"false");
        // derby.log for both master and slave ends up in masters system!
        // Both are run in the same VM! Not a good idea?
        return server;
    }
     */


    void killMaster(String masterServerHost, int masterServerPort)
    throws InterruptedException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG("killMaster: " + masterServerHost +":" + masterServerPort);
        if ( masterServerHost.equals("localhost") )
        {
            stopServer(masterJvmVersion, derbyMasterVersion,
                    masterServerHost, masterServerPort);
        }
        else
        {
            int pid = xFindServerPID(masterServerHost,masterServerPort);
            xStopServer(masterServerHost, pid);
        }
    }
    void killSlave(String slaveServerHost, int slaveServerPort)
    throws InterruptedException
    {
        util.DEBUG("killSlave: " + slaveServerHost +":" + slaveServerPort);
        if ( slaveServerHost.equals("localhost") )
        {
            stopServer(slaveJvmVersion, derbySlaveVersion,
                    slaveServerHost, slaveServerPort);
        }
        else
        {
            int pid = xFindServerPID(slaveServerHost, slaveServerPort);
            xStopServer(slaveServerHost, pid);
        }
    }
    void destroySlaveDB(String slaveServerHost)
    throws InterruptedException
    {
        removeSlaveDBfiles(slaveServerHost, replicatedDb);
    }
    void stopServer(String serverVM, String serverVersion,
            String serverHost, int serverPort)
    {
        util.DEBUG("");
        final String debugId = "stopServer@" + serverHost + ":" + serverPort + " ";
        util.DEBUG("+++ stopServer " + serverVM + " / " + serverVersion
                + " " + debugId);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        String serverJvm = ReplicationRun.getServerJavaExecutableName(serverHost,serverVM);
        String serverClassPath = serverVersion + FS+"derby.jar"
                + PS + serverVersion + FS+"derbynet.jar";
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
//IC see: https://issues.apache.org/jira/browse/DERBY-3162

        final boolean isRemote = !serverHost.equals("localhost");
//IC see: https://issues.apache.org/jira/browse/DERBY-5791

        ArrayList<String> ceArray = new ArrayList<String>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

        // For remote tests, we need to specify the Java VM to use and the
        // classpath. For local tests, we'll just use the JVM and the classpath
        // BaseTestCase.execJavaCmd() gives us. Note that this means we cannot
        // vary server versions when running locally.
        if (isRemote) {
            ceArray.add( serverJvm );
            ceArray.add( "-cp" );
            ceArray.add( serverClassPath );
        }
        ceArray.add( "-Dderby.infolog.append=true" );

//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        if (JVMInfo.isModuleAware())
        {
            ceArray.add("-m");
            ceArray.add(ModuleUtil.SERVER_MODULE_NAME + "/" + networkServerControl);
        }
        else
        {
            ceArray.add( networkServerControl );
        }
        ceArray.add( "shutdown" );
        ceArray.add( "-h" );
        ceArray.add( serverHost ); // FIXME! interfacesToListenOn
        ceArray.add( "-p" );
        ceArray.add( String.valueOf(serverPort ) );
        if ( db_uid != null )
        {
            ceArray.add( "-user" );
            ceArray.add( db_uid );
            ceArray.add( "-password" );
            ceArray.add( db_passwd );
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        final String[] commandElements = util.toStringArray(ceArray);
        
        final String fullCmd = util.splice(commandElements, ' ');
        util.DEBUG(debugId+"commandElements: " + fullCmd);

//IC see: https://issues.apache.org/jira/browse/DERBY-5504
//IC see: https://issues.apache.org/jira/browse/DERBY-5504
        final boolean serverOnLocalhost =
                serverHost.equalsIgnoreCase("localhost");
        if (serverOnLocalhost)
        {
            util.DEBUG(debugId+"Stopping server on localhost "+ serverHost);
            runUserCommandLocally(commandElements, debugId, null);
        }
        else
        {
            util.DEBUG(debugId+"Stopping server on non-local host "+ serverHost);
            runUserCommandRemotely(fullCmd, serverHost, testUser, debugId);
        }
        
        util.DEBUG(debugId+"--- stopServer ");
        util.DEBUG("");
        
    }

    private String processOutput(String id, Process proc)
    throws Exception
    {
        InputStream serveInputStream = proc.getInputStream();
        InputStream serveErrorStream = proc.getErrorStream();
        
        InputStreamReader isr = new InputStreamReader(serveInputStream);
        InputStreamReader esr = new InputStreamReader(serveErrorStream);
        BufferedReader bir = new BufferedReader(isr);
        BufferedReader ber = new BufferedReader(esr);
        String line=null;
        String result = null;
        util.DEBUG(id+"---- out:");
        // if ( bir.readLine() != null ) {result = id+" ---- out:";}
        while ( (line = bir.readLine()) != null)
        {
            util.DEBUG(id+line);
            result = result + LF + line;
        }
        util.DEBUG(id+"---- err:");
        // if ( ber.readLine() != null ) {result = result + LF + id+" ---- err:";}
        while ( (line = ber.readLine()) != null)
        {
            util.DEBUG(id+line);
            result = result + LF + line;
        }
        util.DEBUG(id+"----     ");
        // result = result + LF + id+" ----";
        
        return result;
    }
    
    private void processDEBUGOutput(String id, Process proc)
    throws Exception
    {
        InputStream serveInputStream = proc.getInputStream();
        InputStream serveErrorStream = proc.getErrorStream();
        
        InputStreamReader isr = new InputStreamReader(serveInputStream);
        InputStreamReader esr = new InputStreamReader(serveErrorStream);
        BufferedReader bir = new BufferedReader(isr);
        BufferedReader ber = new BufferedReader(esr);
        String line=null;
        util.DEBUG(id+"---- out:");
        while ( (line = bir.readLine()) != null)
        {
            util.DEBUG(id+line);
        }
        util.DEBUG(id+"---- err:");
        while ( (line = ber.readLine()) != null)
        {
            util.DEBUG(id+line);
        }
        util.DEBUG(id+"----     ");

        int exitCode = proc.waitFor();
//IC see: https://issues.apache.org/jira/browse/DERBY-5643
        util.DEBUG(id + "process exit status: " + exitCode);
    }

    /**
     * Register that a thread has been started so that we can wait for it to
     * complete in {@link #tearDown()}.
     *
     * @param thread a thread that has been started
     */
    private void registerThread(Thread thread) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5517
        helperThreads.add(thread);
    }

    private void pingServer( String hostName, int port)
    throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        util.DEBUG("+++ pingServer: " + hostName +":" + port);
        NetworkServerControl controller =
            new NetworkServerControl(InetAddress.getByName(hostName), port);
//IC see: https://issues.apache.org/jira/browse/DERBY-5643
        assertTrue("Server did not start in time",
                   NetworkServerTestSetup.pingForServerStart(controller));
        util.DEBUG("--- pingServer: " + hostName +":" + port);
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-3126
    void startOptionalLoad(Load load,
            String dbSubPath,
            String serverHost,
            int serverPort)
            throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        String loadString = load.load;
        String database = load.database;
        boolean existingDB = load.existingDB;
        String testClientHost = load.clientHost;
        util.DEBUG("run load " + loadString
                + " on client " + testClientHost
                + " against server " + serverHost + ":" + serverPort
                + " using DB  " + database + "["+existingDB+"]"
                );
        if ( loadString == null )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
            util.DEBUG("No load supplied!");
            return;
        }
        if ( !existingDB )
        {
            // Create it!
//IC see: https://issues.apache.org/jira/browse/DERBY-3921
            String URL = masterURL(database)
                    +";create=true"; // Creating! No need for encryption here?
            String ijClassPath = derbyVersion +FS+ "derbyclient.jar"
                    + PS + derbyVersion +FS+ "derbyTesting.jar"
                    + PS + derbyVersion +FS+ "derbytools.jar";
        if ( serverHost.equals("localhost") )
        { // Use full classpath when running locally. Can not vary server versions!
            ijClassPath = classPath;
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
            String clientJvm = ReplicationRun.getClientJavaExecutableName();
            String command = "rm -rf /"+masterDbPath( database )+";" // FIXME! for slave load!
                    + clientJvm // "java"
                    + " -Dij.driver=" + DRIVER_CLASS_NAME
                    + " -Dij.connection.create"+database+"=\"" + URL + "\""
                    + " -classpath " + ijClassPath + " org.apache.derby.tools.ij"
                    + " " + sqlLoadInit // FIXME! Should be load specific!
                    ;
            String results =
                    runUserCommandRemotely(command,
                    testClientHost,
                    testUser,
                    "Create_"+database);
            
        }
        
        // Must run in separate thread!:
//IC see: https://issues.apache.org/jira/browse/DERBY-5791
        runLoad(loadString,
                jvmVersion,
                testClientHost,
                serverHost, serverPort,
                dbSubPath+FS+database);
        
        // FIXME! How to join and cleanup....
        
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-3921
    void makeReadyForReplication()
        throws Exception
    {   // Replace the following code in all tests with a call to makeReadyForReplication()!
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5729
        startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES,
                masterServerPort,
                masterDbSubPath);
        
        startServer(slaveJvmVersion, derbySlaveVersion,
                slaveServerHost,
                ALL_INTERFACES,
                slaveServerPort,
                slaveDbSubPath);
        
        startServerMonitor(slaveServerHost);
        
        bootMasterDatabase(jvmVersion,
                masterDatabasePath +FS+ masterDbSubPath,
                replicatedDb,
                masterServerHost,
                masterServerPort,
                null // bootLoad, // The "test" to start when booting db.
                );
        
        initSlave(slaveServerHost,
                jvmVersion,
                replicatedDb);
        
        startSlave(jvmVersion, replicatedDb,
                slaveServerHost,
                slaveServerPort,
                slaveServerHost,
                slaveReplPort,
                testClientHost);
        
        startMaster(jvmVersion, replicatedDb,
                masterServerHost,
                masterServerPort,
                masterServerHost,
                slaveServerPort,
                slaveServerHost,
                slaveReplPort);
        
    }
    
    ///////////////////////////////////////////////////////////////////////////
    /* Remove any servers or tests still running
     */
    void cleanAllTestHosts()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        util.DEBUG("************************** cleanAllTestHosts() Not yet implemented");
    }

    ///////////////////////////////////////////////////////////////////////////
    /* The following is used to run tests in the various states of replication
     */
    class State
    {
        String testPreStartedMasterServer = null;
        boolean testPreStartedMasterServerReturn = false;
        String testPreStartedSlaveServer = null;
        boolean testPreStartedSlaveServerReturn = false;
        String testPreStartedMaster = null;
        boolean testPreStartedMasterReturn = false;
        String testPreInitSlave = null;
        boolean testPreInitSlaveReturn = false;
        String testPreStartedSlave = null;
        boolean testPreStartedSlaveReturn = false;
        String testPostStartedMasterAndSlave = null;
        boolean testPostStartedMasterAndSlaveReturn = false;
        String testPreStoppedMaster = null;
        boolean testPreStoppedMasterReturn = false;
        String testPreStoppedMasterServer = null;
        boolean testPreStoppedMasterServerReturn = false;
        String testPreStoppedSlave = null;
        boolean testPreStoppedSlaveReturn = false;
        String testPreStoppedSlaveServer = null;
        boolean testPreStoppedSlaveServerReturn = false;
        String testPostStoppedSlave = null;
        boolean testPostStoppedSlaveReturn = false;
        String testPostStoppedSlaveServer = null;
        boolean testPostStoppedSlaveServerReturn = false;
                
        void initEnvironment(Properties cp)
        {
            testPreStartedMasterServer = cp.getProperty("test.PreStartedMasterServer", null);
            testPreStartedMasterServerReturn = cp.getProperty("test.PreStartedMasterServer.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPreStartedMasterServer:" 
                    + testPreStartedMasterServer + FS 
                    + testPreStartedMasterServerReturn);
            
            testPreStartedSlaveServer = cp.getProperty("test.PreStartedSlaveServer", null);
            testPreStartedSlaveServerReturn = cp.getProperty("test.PreStartedSlaveServer.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPreStartedSlaveServer:" 
                    + testPreStartedSlaveServer + FS 
                    + testPreStartedSlaveServerReturn);
            
            testPreInitSlave = cp.getProperty("test.PreInitSlave", null);
            testPreInitSlaveReturn = cp.getProperty("test.PreInitSlave.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPreInitSlave:" 
                    + testPreInitSlave + FS 
                    + testPreInitSlaveReturn);
            
            testPreStartedMaster = cp.getProperty("test.PreStartedMaster", null);
            testPreStartedMasterReturn = cp.getProperty("test.PreStartedMaster.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPreStartedMaster:" 
                    + testPreStartedMaster + FS 
                    + testPreStartedMasterReturn);
            
            testPreStartedSlave = cp.getProperty("test.PreStartedSlave", null);
            testPreStartedSlaveReturn = cp.getProperty("test.PreStartedSlave.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPreStartedSlave:" 
                    + testPreStartedSlave + FS 
                    + testPreStartedSlaveReturn);
            
            testPostStartedMasterAndSlave = cp.getProperty("test.PostStartedMasterAndSlave", null);
            testPostStartedMasterAndSlaveReturn = cp.getProperty("test.PostStartedMasterAndSlave.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPostStartedMasterAndSlave:" 
                    + testPostStartedMasterAndSlave + FS 
                    + testPostStartedMasterAndSlaveReturn);
            
            testPreStoppedMaster = cp.getProperty("test.PreStoppedMaster", null);
            testPreStoppedMasterReturn = cp.getProperty("test.PreStoppedMaster.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPreStoppedMaster:" 
                    + testPreStoppedMaster + FS 
                    + testPreStoppedMasterReturn);
            
            testPreStoppedMasterServer = cp.getProperty("test.PreStoppedMasterServer", null);
            testPreStoppedMasterServerReturn = cp.getProperty("test.PreStoppedMasterServer.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPreStoppedMasterServer:" 
                    + testPreStoppedMasterServer + FS 
                    + testPreStoppedMasterServerReturn);
            
            testPreStoppedSlave = cp.getProperty("test.PreStoppedSlave", null);
            testPreStoppedSlaveReturn = cp.getProperty("test.PreStoppedSlave.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPreStoppedSlave:" 
                    + testPreStoppedSlave + FS 
                    + testPreStoppedSlaveReturn);
            
            testPostStoppedSlave = cp.getProperty("test.PostStoppedSlave", null);
            testPostStoppedSlaveReturn = cp.getProperty("test.PostStoppedSlave.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPostStoppedSlave:" 
                    + testPostStoppedSlave + FS 
                    + testPostStoppedSlaveReturn);
            
            testPostStoppedSlaveServer = cp.getProperty("test.PostStoppedSlaveServer", null);
            testPostStoppedSlaveServerReturn = cp.getProperty("test.PostStoppedSlaveServer.return", "false")
                                                             .equalsIgnoreCase("true");
            util.DEBUG("testPostStoppedSlaveServer:" 
                    + testPostStoppedSlaveServer + FS 
                    + testPostStoppedSlaveServerReturn);
        }
        
    boolean testPreStartedMasterServer()
        throws Exception
    {
        /*
# Superflueous? Set .test=null for false? test.preStartedMasterServer=true
# Test to run:
test.preStartedMasterServer.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StartMasterCmdTooEarly
# Return from test framework immediatly:
test.preStartedMasterServer.return=true
         */
        util.DEBUG("****** BEGIN testPreStartedMasterServer");
        if ( testPreStartedMasterServer != null )
        {
            runStateTest(testPreStartedMasterServer, // E.g. org.apache.derbyTesting.functionTests.tests.replicationTests.TestPreStartedMasterServer
                    jvmVersion,
                    testClientHost, // using connect. On masterServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPreStartedMasterServerReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPreStartedMasterServer");
        return testPreStartedMasterServerReturn;
    }

    boolean testPreStartedSlaveServer()
        throws Exception
    {
        /*
# test.preStartedSlaveServer=true
test.preStartedSlaveServer.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StartSlaveCmdTooEarly
test.preStartedSlaveServer.return=true
         */
        util.DEBUG("****** BEGIN testPreStartedSlaveServer");
        if ( testPreStartedSlaveServer != null )
        {
            runStateTest(testPreStartedSlaveServer, // E.g. org.apache.derbyTesting.functionTests.tests.replicationTests.TestPreStartedSlaveServer
                    jvmVersion,
                    testClientHost, // using connect. On slaveServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPreStartedSlaveServerReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPreStartedSlaveServer");
        return testPreStartedSlaveServerReturn;
    }

    boolean testPreStartedMaster()
        throws Exception
    {
        /*
# test.preStartedMaster=true
test.preStartedMaster.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StartMasterCmd_OK
# test.preStartedMaster.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StartMasterCmd_ERR
test.preStartedMaster.return=true
         */
        util.DEBUG("****** BEGIN testPreStartedMaster");
        if ( testPreStartedMaster != null )
        {
            runStateTest(testPreStartedMaster, // E.g. org.apache.derbyTesting.functionTests.tests.replicationTests.TestPreStartedMaster
                    jvmVersion,
                    testClientHost, // using connect. On masterServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPreStartedMasterReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPreStartedMaster");
        return testPreStartedMasterReturn;
    }

    boolean testPreInitSlave()
        throws Exception
    {
        /*
# test.preInitSlave=true
test.preInitSlave.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StartSlaveCmd_OK
# test.preInitSlave.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StartSlaveCmd_ERR
test.preInitSlave.return=true
         */
        util.DEBUG("****** BEGIN testPreInitSlave");
        if ( testPreInitSlave != null )
        {
            runStateTest(testPreInitSlave,
                    jvmVersion,
                    testClientHost, // using connect. On slaveServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPreInitSlaveReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPreInitSlave");
        return testPreInitSlaveReturn;
    }

    boolean testPreStartedSlave()
        throws Exception
    {
        /*
# test.preStartedSlave=true
test.preStartedSlave.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StartSlaveCmd_OK
# test.preStartedMaster.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StartSlaveCmd_ERR
test.preStartedSlave.return=true
         */
        util.DEBUG("****** BEGIN testPreStartedSlave");
        if ( testPreStartedSlave != null )
        {
            runStateTest(testPreStartedSlave,
                    jvmVersion,
                    testClientHost, // using connect. On slaveServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPreStartedSlaveReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPreStartedSlave");
        return testPreStartedSlaveReturn;
    }

    boolean testPostStartedMasterAndSlave()
        throws Exception
    {
        /*
# test.postStartedMasterAndSlave=true
test.postStartedMasterAndSlave.test=org.apache.derbyTesting.functionTests.tests.replicationTests.XXXX_OK
# test.postStartedMasterAndSlave.test=org.apache.derbyTesting.functionTests.tests.replicationTests.XXXX_ERR
test.postStartedMasterAndSlave.return=true
         */
        util.DEBUG("****** BEGIN testPostStartedMasterAndSlave");
        if ( testPostStartedMasterAndSlave != null )
        {
            // run testPostStartedMasterAndSlave test
            runStateTest(testPostStartedMasterAndSlave,
                    jvmVersion,
                    testClientHost, // using connect. On slaveServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPostStartedMasterAndSlaveReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPostStartedMasterAndSlave");
        return testPostStartedMasterAndSlaveReturn;
    }

    boolean testPreStoppedMaster()
        throws Exception
    {
        /*
# test.preStoppedMaster=true
test.preStoppedMaster.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StopMasterCmd_OK
# test.preStoppedMaster.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StopMasterCmd_ERR
test.preStoppedMaster.return=true
         */
        util.DEBUG("****** BEGIN testPreStoppedMaster");
        if ( testPreStoppedMaster != null )
        {
            runStateTest(testPreStoppedMaster,
                    jvmVersion,
                    testClientHost, // using connect. On slaveServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPreStoppedMasterReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPreStoppedMaster");
        return testPreStoppedMasterReturn;
    }

    boolean testPreStoppedMasterServer()
        throws Exception
    {
        /*
# test.preStoppedMasterServer=true
test.preStoppedMasterServer.test=org.apache.derbyTesting.functionTests.tests.replicationTests.YYYYY_OK
# test.preStoppedMasterServer.test=org.apache.derbyTesting.functionTests.tests.replicationTests.YYYYY_ERR
test.preStoppedMasterServer.return=true
         */
        util.DEBUG("****** BEGIN testPreStoppedMasterServer");
        if ( testPreStoppedMasterServer != null )
        {
            runStateTest(testPreStoppedMasterServer,
                    jvmVersion,
                    testClientHost, // using connect. On slaveServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPreStoppedMasterServerReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPreStoppedMasterServer");
        return testPreStoppedMasterServerReturn;
    }

    boolean testPreStoppedSlave()
        throws Exception
    {
        /*
# test.preStoppedSlave=true
test.preStoppedSlave.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StopSlaveCmd_OK
# test.preStoppedSlave.test=org.apache.derbyTesting.functionTests.tests.replicationTests.StopSlaveCmd_ERR
test.preStoppedSlave.return=true
         */
        util.DEBUG("****** BEGIN testPreStoppedSlave");
        if ( testPreStoppedSlave != null )
        {
            runStateTest(testPreStoppedSlave,
                    jvmVersion,
                    testClientHost, // using connect. On slaveServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPreStoppedSlaveReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPreStoppedSlave");
        return testPreStoppedSlaveReturn;
    }

    boolean testPreStoppedSlaveServer()
        throws Exception
    {
        /*
# test.preStoppedSlaveServer=true
test.preStoppedSlaveServer.test=org.apache.derbyTesting.functionTests.tests.replicationTests.ZZZZZZ_OK
# test.preStoppedSlaveServer.test=org.apache.derbyTesting.functionTests.tests.replicationTests.ZZZZZZ_ERR
test.preStoppedSlaveServer.return=true
         */
        util.DEBUG("****** BEGIN testPreStoppedSlaveServer");
        if ( testPreStoppedSlaveServer != null )
        {
            runStateTest(testPreStoppedSlaveServer,
                    jvmVersion,
                    testClientHost, // using connect. On slaveServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPreStoppedSlaveServerReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPreStoppedSlaveServer");
        return testPreStoppedSlaveServerReturn;
    }

    boolean testPostStoppedSlaveServer()
        throws Exception
    {
        /*
# test.postStoppedSlaveServer=true
test.postStoppedSlaveServer.test=org.apache.derbyTesting.functionTests.tests.replicationTests.ZZZXXX_OK
# test.postStoppedSlaveServer.test=org.apache.derbyTesting.functionTests.tests.replicationTests.ZZZXXX_ERR
test.postStoppedSlaveServer.return=true
         */
        util.DEBUG("****** BEGIN testPostStoppedSlaveServer");
        if ( testPostStoppedSlaveServer != null )
        {
            runStateTest(testPostStoppedSlaveServer,
                    jvmVersion,
                    testClientHost, // using connect. On slaveServerHost using CLI
                    masterServerHost, masterServerPort,
                    replicatedDb);
        }
        if ( testPostStoppedSlaveServerReturn ) cleanupAndShutdown();
        util.DEBUG("****** END   testPostStoppedSlaveServer");
        return testPostStoppedSlaveServerReturn;
    }

        private void cleanupAndShutdown()
        {
            stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);

            stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        }
    }
    ///////////////////////////////////////////////////////////////////////////
    
    ///////////////////////////////////////////////////////////////////////////
    /* Load started in different states of replication. */
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
    class Load
    {
        Load(String id, Properties testRunProperties)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
            util.DEBUG("Load(): " + id);
            
            String pid = "test." + id;
            if ( testRunProperties.getProperty(pid,"false").equalsIgnoreCase("false") )
            {
                util.DEBUG(pid + " Not defined or set to false!");
            }
            else
            {
                
                pid = "test." + id + ".load";
                load = testRunProperties.getProperty(pid,
                        "org.apache.derbyTesting.functionTests.tests.replicationTests.DefaultLoad");
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
                util.DEBUG(pid+": " + load);
                
                pid = "test." + id + ".database";
                database = testRunProperties.getProperty(pid, id);
                util.DEBUG(pid+": " + database);
                
                pid = "test." + id + ".existingDB";
                existingDB = testRunProperties.getProperty(pid,"false").equalsIgnoreCase("true");
                util.DEBUG(pid+": " + existingDB);
                
                pid = "test." + id + ".clientHost";
                clientHost = testRunProperties.getProperty(pid, testClientHost);
                util.DEBUG(pid+": " + clientHost);
                
            }
            
        }
        
        String load = null; // .sql file or junit class
        String database = null; // Database name used by load
        boolean existingDB = false; // Database already exists.
        String clientHost = null; // Host running load client.
    }
    static Load masterPreRepl;
    static Load masterPostRepl;
    static Load slavePreSlave;
    static Load masterPostSlave;
    static Load slavePostSlave;
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Assert that the latest startSlave connection attempt got the expected
     * SQLState. The method will wait for upto 5 seconds for the startSlave
     * connection attemt to complete. If the connection attempt has not
     * completed after 5 seconds it is assumed to have failed.
     * @param expected the expected SQLState
     * @throws java.lang.Exception the Exception to check the SQLState of
     */
    protected void assertSqlStateSlaveConn(String expected) throws Exception {
        boolean verified = false;
        for (int i = 0; i < 10; i++) {
            if (startSlaveException != null) {
                if (startSlaveException instanceof SQLException) {
                    BaseJDBCTestCase.
                        assertSQLState("Unexpexted SQL State",
                                       expected,
                                       (SQLException)startSlaveException);
                    verified = true;
                    break;
                } else {
                    throw startSlaveException;
                }
            } else {
                Thread.sleep(500);
            }
        }
        if (!verified) {
            fail("Attempt to start slave hangs. Expected SQL state " +
                 expected);
        }
    }
    
//IC see: https://issues.apache.org/jira/browse/DERBY-3738
    void assertException(SQLException se, String expectedSqlState)
    {
        if (se == null ) // Did not get an exception
        {
            util.DEBUG("Got 'null' exception, expected '" + expectedSqlState + "'");
            assertTrue("Expected exception: " + expectedSqlState + " got: 'null' exception", 
                    expectedSqlState == null);
            return;
        }
        int ec = se.getErrorCode();
        String ss = se.getSQLState();
        String msg = "Got " + ec + " " + ss + " " + se.getMessage()
        + ". Expected " + expectedSqlState;
        util.DEBUG(msg);
        
        if ( expectedSqlState != null ) // We expect an exception
        {
            assertTrue(msg, ss.equals(expectedSqlState));
        }
        else // We do not expect an exception, but got one.
        {
            assertTrue(msg, false);
        }
    }
    
    
    void _testInsertUpdateDeleteOnMaster(String serverHost, 
            int serverPort,
            String dbPath,
            int _noTuplesToInsert)
        throws SQLException, ClassNotFoundException, IllegalAccessException,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
               InstantiationException, NoSuchMethodException, InvocationTargetException
    {
        util.DEBUG("_testInsertUpdateDeleteOnMaster: " + serverHost + ":" +
                   serverPort + "/" + dbPath + " " + _noTuplesToInsert);
        ClientDataSourceInterface ds = configureDataSource(
                dbPath, serverHost, serverPort, useEncryption(false) );
        Connection conn = ds.getConnection();
        
        PreparedStatement ps = conn.prepareStatement("create table t(i integer primary key, s varchar(64))");
        
        ps.execute();
        
        ps = conn.prepareStatement("insert into t values (?,?)");
        for (int i = 0; i< _noTuplesToInsert; i++)
        {
            ps.setInt(1,i);
            ps.setString(2,"dilldall"+i);
            ps.execute();
            if ( (i % 10000) == 0 ) conn.commit();
        }
        
        _verify(conn, _noTuplesToInsert);
        
        conn.close();
    }
    void _verifyDatabase(String serverHost, 
            int serverPort,
            String dbPath,
            int _noTuplesInserted)
        throws SQLException, ClassNotFoundException, IllegalAccessException,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
               InstantiationException, NoSuchMethodException, InvocationTargetException
    {
        util.DEBUG("_verifyDatabase: "+serverHost+":"+serverPort+"/"+dbPath);
        ClientDataSourceInterface ds = configureDataSource(
                dbPath, serverHost, serverPort, useEncryption(false) );
        Connection conn = ds.getConnection();
        
        _verify(conn,_noTuplesInserted);
        
        conn.close();
    }
    void _verify(Connection conn, int _noTuplesInserted)
        throws SQLException
    {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from t");
        rs.next();
        int count = rs.getInt(1);
        rs = s.executeQuery("select max(i) from t");
        rs.next();
        int max = rs.getInt(1);
        util.DEBUG("_verify: " + count + "/" + _noTuplesInserted + " " + max +
                   "/" + (_noTuplesInserted - 1));
        assertEquals("Expected "+ _noTuplesInserted +" tuples, got "+ count +".",
                     _noTuplesInserted, count);
        assertEquals("Expected " +(_noTuplesInserted-1) +" max, got " + max +".",
                     _noTuplesInserted - 1, max);
    }
    
    Connection getConnection(String serverHost, int serverPort,
            String databasePath, String dbSubPath, String replicatedDb)
        throws SQLException
    {
        String db = databasePath +FS+dbSubPath +FS+ replicatedDb;
        String connectionURL = serverURL( db, serverHost, serverPort );
        //String connectionURL = "jdbc:derby:"  
        //       + "//" + serverHost + ":" + serverPort + "/"
        //        + db;
        util.DEBUG(connectionURL);
        return DriverManager.getConnection(connectionURL);
    }
    
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
    String masterDbPath(String dbName)
    {
        return masterDatabasePath+FS+masterDbSubPath+FS+dbName;
    }
    String slaveDbPath(String dbName)
    {
        return slaveDatabasePath+FS+slaveDbSubPath+FS+dbName;
    }
    String masterURL(String dbName)
    {
        return serverURL( masterDbPath( dbName ), masterServerHost, masterServerPort );
    }
    String masterLoadURL(String dbSubPath)
    {
        return serverURL( masterDatabasePath+FS+dbSubPath, masterServerHost, masterServerPort );
    }
    String slaveURL(String dbName)
    {
        return serverURL( slaveDbPath( dbName ), slaveServerHost, slaveServerPort );
    }
    String  serverURL( String fullDbPath, String serverHost, int serverPort )
    {
        return DB_PROTOCOL
                +"://"+serverHost
                +":"+serverPort+"/"
                +fullDbPath
                +useEncryption(false)
                +credentials();
    }
    String  credentials()
    {
        if ( db_uid == null ) { return ""; }
        else { return ";user=" + db_uid + ";password=" + db_passwd; }
    }


//IC see: https://issues.apache.org/jira/browse/DERBY-4246
    SQLException stopSlave(
        String slaveServerHost,
        int slaveServerPort,
        String slaveDatabasePath,
        String replicatedDb,
        boolean masterServerAlive)
        throws Exception
    {
        return stopSlave(slaveServerHost,
                         slaveServerPort,
                         slaveDatabasePath,
                         ReplicationRun.slaveDbSubPath,
                         replicatedDb,
                         masterServerAlive);
    }


    SQLException stopSlave(
        String slaveServerHost,
        int slaveServerPort,
        String slaveDatabasePath,
        String subPath,
        String replicatedDb,
        boolean masterServerAlive)
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
        throws Exception
    {
        util.DEBUG("stopSlave");
        String dbPath = slaveDatabasePath + FS + subPath + FS + replicatedDb;

        String connectionURL = serverURL( dbPath, slaveServerHost, slaveServerPort ) + ";stopSlave=true";
        //String connectionURL = "jdbc:derby:"
        //    + "//" + slaveServerHost + ":" + slaveServerPort + "/"
        //    + dbPath
        //    + ";stopSlave=true"
        //    + useEncryption(false);

        if (masterServerAlive) {
            try {
                Connection conn = DriverManager.getConnection(connectionURL);
                conn.close();
                return null; // If successful.
            } catch (SQLException se) {
                return se;
            }
        } else {
            // We use a loop below, to allow for intermediate states before the
            // expected final state REPLICATION_DB_NOT_BOOTED.
            //
            // If we get here quick enough we see these error states (in order):
            //     a) SLAVE_OPERATION_DENIED_WHILE_CONNECTED
            //     b) REPLICATION_SLAVE_SHUTDOWN_OK
            //
            SQLException gotEx = null;
            int tries = 20;

            while (tries-- > 0) {
                gotEx = null;

                try {
                    DriverManager.getConnection(connectionURL);
                    fail("Unexpectedly connected");
                } catch (SQLException se) {
                    if (se.getSQLState().
                            equals(SLAVE_OPERATION_DENIED_WHILE_CONNECTED)) {
                        // Try again, shutdown did not complete yet..
                        gotEx = se;
                        util.DEBUG
                            ("got SLAVE_OPERATION_DENIED_WHILE_CONNECTED, " +
                             "sleep");
                        Thread.sleep(1000L);
                        continue;

                    } else if (se.getSQLState().
                                   equals(REPLICATION_SLAVE_SHUTDOWN_OK)) {
                        // Try again, shutdown started but did not complete yet.
                        gotEx = se;
                        util.DEBUG("got REPLICATION_SLAVE_SHUTDOWN_OK, " +
                                   "sleep..");
                        Thread.sleep(1000L);
                        continue;

                    } else if (se.getSQLState().
                                   equals(REPLICATION_DB_NOT_BOOTED)) {
                        // All is fine, so proceed
                        util.DEBUG("Got REPLICATION_DB_NOT_BOOTED as expected");
                        break;

                    } else {
                        // Something else, so report.
                        gotEx = se;
                        break;
                    }
                }
            }

            if (gotEx != null) {
                // We did not get what we expected as the final state
                // (REPLICATION_DB_NOT_BOOTED) in reasonable time, or we saw
                // something that is not a legal intermediate state, so we fail
                // now:
                throw gotEx;
            }

            return null;
        }
    }
    private static String getMasterJavaExecutableName()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        if ( masterServerHost.matches("localhost") )
        {
            return BaseTestCase.getJavaExecutableName();
        }
        return masterJvmVersion+FS+".."+FS+"bin"+FS+"java";
    }
    private static String getSlaveJavaExecutableName()
    {
        if ( slaveServerHost.matches("localhost") )
        {
            return BaseTestCase.getJavaExecutableName();
        }
        return slaveJvmVersion+FS+".."+FS+"bin"+FS+"java";
    }
    private static String getClientJavaExecutableName()
    {
        if ( testClientHost.matches("localhost") )
        {
            return BaseTestCase.getJavaExecutableName();
        }
        return jvmVersion+FS+".."+FS+"bin"+FS+"java";
    }
    private static String getServerJavaExecutableName(String serverHost,String serverVM)
    {
        if ( serverHost.matches("localhost") )
        {
            return BaseTestCase.getJavaExecutableName();
        }
        return serverVM+FS+".."+FS+"bin"+FS+"java";        
    }

    /**
     * <p>
     * Set up a data source.
     * </p>
     */
    ClientDataSourceInterface    configureDataSource
        (
         String dbName,
         String serverHost,
         int        serverPort,
         String     connectionAttributes
         ) throws ClassNotFoundException, IllegalAccessException,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                  InstantiationException, NoSuchMethodException, InvocationTargetException
    {
        ClientDataSourceInterface ds;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientDataSource");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientDataSource40");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        }

        ds.setDatabaseName( dbName );
        ds.setServerName( serverHost );
        ds.setPortNumber(serverPort);
        ds.setConnectionAttributes( connectionAttributes );

        if ( db_uid != null )
        {
            ds.setUser( db_uid );
            ds.setPassword( db_passwd );
        }
        
        return ds;
    }
    
}
