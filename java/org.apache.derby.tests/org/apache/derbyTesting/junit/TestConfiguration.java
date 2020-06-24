/*
 *
 * Derby - Class org.apache.derbyTesting.junit.TestConfiguration
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import junit.extensions.TestSetup;
import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;

import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;

/**
 * Class which holds information about the configuration of a Test.
 * 
 * A configuration manages the pool of databases in use
 * in <code>usedDbNames</code> property. One of those databases
 * is supposed to be the default database. A new default database
 * is added to the pool by <code>singleUseDatabaseDecorator</code> function.
 * <br>
 * Additional databases may be added by <code>additionalDatabaseDecorator</code>
 * function. Each of the additional databases has its logical and physical name.
 * Physical database name is automatically generated as 'singleUse/oneuseXX'
 * where 'XX' is unique number. The logical database name is used to establish
 * a connection to the database using
 * a <code>TestConfiguration::openConnection(String logicalDatabaseName)</code>
 * function.
 * <br>
 * The database files are supposed to be local and they will be
 * removed by <code>DropDatabaseSetup</code>.
 *
 */
public final class TestConfiguration {
    /**
     * Default values for configurations
     */
    private final static String DEFAULT_DBNAME = "wombat";
    private final static String DEFAULT_DBNAME_SQL = "dbsqlauth";
    
    private final static String DEFAULT_USER_NAME = "APP";
    private final static String DEFAULT_USER_PASSWORD = "APP";
    private final static int    DEFAULT_PORT = 1527;
    private final static String DEFAULT_FRAMEWORK = "embedded";
    private final static String DEFAULT_HOSTNAME = "localhost";

    private static final int LOCKFILETIMEOUT = 300000; // 5 mins

    /**
     * Maximum number of ports used by Suites.All 
     * If this changes, this constant and the Wiki
     * page at  http://wiki.apache.org/db-derby/DerbyJUnitTesting
     * need to be updated. 
     */
    private final static int MAX_PORTS_USED = 22;
    
    /** This is the base port. This does NOT change EVER during the run of a suite.
     *	It is set using the property derby.tests.basePort and it is set to default when
     *	a property isn't used. */
    private final static int basePort;
    private static int lastAssignedPort;
    private static final int bogusPort;
    static {
    	String port = BaseTestCase.getSystemProperty("derby.tests.basePort");
        if (port == null) {
        	lastAssignedPort = DEFAULT_PORT;
        } else {
        	lastAssignedPort = Integer.parseInt(port);
        }
        basePort = lastAssignedPort;
//IC see: https://issues.apache.org/jira/browse/DERBY-4700
        bogusPort = ++lastAssignedPort;
    }
    private static int assignedPortCount = 2;

    private FileOutputStream serverOutput;
		
    /** Sleep for 1000 ms before pinging the network server (again) */
    private static final int SLEEP_TIME = 1000;
            
    /**
     * Keys to use to look up values in properties files.
     */
    private final static String KEY_DBNAME = "databaseName";
    private final static String KEY_FRAMEWORK = "framework";
    private final static String KEY_USER_PASSWORD = "password";
    private final static String KEY_USER_NAME = "user";
    private final static String KEY_HOSTNAME = "hostName";
    private final static String KEY_PORT = "port";
    private final static String KEY_VERBOSE = "derby.tests.debug";    
    private final static String KEY_LOGIN_TIMEOUT = "derby.tests.login.timeout";    
    private final static String KEY_TRACE = "derby.tests.trace";
    public  final static String KEY_OMIT_LUCENE = "derby.tests.omitLucene";
    public  final static String KEY_OMIT_JSON = "derby.tests.omitJson";

    /**
     * derby.tests.stopAfterFirstFail - debugging property to exit after 
     * first failure. Can be useful for debugging cascading failures or errors
     * that lead to hang scenario.
     */
    private final static String KEY_STOP_AFTER_FIRST_FAIL = "derby.tests.stopAfterFirstFail";
    private final static String KEY_SSL = "ssl";
    private final static String KEY_JMX_PORT = "jmxPort";
    
    /**
     * Simple count to provide a unique number for database
     * names.
     */
    private static int uniqueDB;

    /** Repository of old/previous Derby releases available on the local system. */
    private static ReleaseRepository releaseRepository;

    /**
     * Default configuration for standalone JUnit tests,
     * an embedded configuration.
     */
    private static final TestConfiguration JUNIT_CONFIG
        = new TestConfiguration();
    
    /**
     * The default configuration.
     */
    private static final TestConfiguration DEFAULT_CONFIG;
        
    static {
//IC see: https://issues.apache.org/jira/browse/DERBY-2446
        DEFAULT_CONFIG = JUNIT_CONFIG;
        
        final   File dsh = new File("system");

        String absolutePath = PrivilegedFileOpsForTests.getAbsolutePath(dsh);
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

        BaseTestCase.setSystemProperty(
                "derby.system.home", absolutePath);
     }
    
    /**
     * Current configuration is stored in a ThreadLocal to
     * allow the potential for multiple tests to be running
     * concurrently with different configurations.
     */
    private static final ThreadLocal<TestConfiguration>
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            CURRENT_CONFIG = new ThreadLocal<TestConfiguration>() {
        protected TestConfiguration initialValue() {
            return DEFAULT_CONFIG;
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
    };
   
    /**
     * Get this Thread's current configuraiton for running
     * the tests.
     * Note this call must only be used while a test is
     * running, they make no sense when setting up a suite.
     * A suite itself sets up which test configurations
     * the fixtures will run in.

     * @return TestConfiguration to use.
     */
    public static TestConfiguration getCurrent() {
        return (TestConfiguration) CURRENT_CONFIG.get();
    }

    /**
     * Returns the release repository containing old Derby releases available
     * on the local system.
     * <p>
     * <strong>NOTE</strong>: It is your responsibility to keep the repository
     * up to date. This usually involves syncing the local Subversion repository
     * of previous Derby releases with the master repository at Apache.
     *
     * @see ReleaseRepository
     */
    public static synchronized ReleaseRepository getReleaseRepository() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5475
        if (releaseRepository == null) {
            try {
                releaseRepository = ReleaseRepository.getInstance();
            } catch (IOException ioe) {
                BaseTestCase.printStackTrace(ioe);
                Assert.fail("failed to initialize the release repository: " +
                        ioe.getMessage());
            }
        }
        return releaseRepository;
    }

    /**
     * WORK IN PROGRESS
     * Set this Thread's current configuration for running tests.
     * @param config Configuration to set it to.
     */
    static void setCurrent(TestConfiguration config)
    {
        CURRENT_CONFIG.set(config);
    }
    
    
    /**
     * Return a Test suite that contains all the test fixtures
     * for the passed in class running in embedded and the
     * default client server configuration.
     * <BR>
     * Each set of embedded and set of client server tests
     * is decorated with a CleanDatabaseTestSetup.
     * <BR>
     * The client server configuration is setup using clientServerSuite
     */
    public static Test defaultSuite(Class testClass)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1758
        return defaultSuite(testClass, true);
    }

    /**
     * Does the work of "defaultSuite" as defined above.  Takes
     * a boolean argument to determine whether or not to "clean"
     * the test database before each suite.  If the resultant
     * suite is going to be wrapped inside a TestSetup that creates
     * database objects to be used throughout the tests, then the
     * cleanDB parameter should be "false" to prevent cleanup of the
     * database objects that TestSetup created.  For example, see
     * XMLBindingTest.suite().
     */
    public static Test defaultSuite(Class testClass, boolean cleanDB)
    {
         final BaseTestSuite suite = new BaseTestSuite(suiteName(testClass));
         
        if (cleanDB)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-2021
            suite.addTest(new CleanDatabaseTestSetup(embeddedSuite(testClass)));
            suite.addTest(new CleanDatabaseTestSetup(clientServerSuite(testClass)));
        }
        else
        {
            suite.addTest(embeddedSuite(testClass));
            suite.addTest(clientServerSuite(testClass));
        }

        return (suite);
    }
    /**
     * Equivalent to "defaultSuite" as defined above, but assumes a server
     * has already been started. 
     * <BR>
     * Does NOT decorate for running in embedded mode, only for running on
     * the already started server.
     * <BR>
     * Return a Test suite that contains all the test fixtures
     * for the passed in class running in client server configuration
     * on an already started server.
     * <BR>
     * The set of client server tests
     * is decorated with a CleanDatabaseTestSetup.
     * <BR>
     * The client server configuration is setup using clientExistingServerSuite
     */
    public static Test defaultExistingServerSuite(Class testClass)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2807
        return defaultExistingServerSuite(testClass, true);
    }
    
    /**
     * Does the work of "defaultExistingServerSuite" as defined above.  Takes
     * a boolean argument to determine whether or not to "clean"
     * the test database before each suite.  If the resultant
     * suite is going to be wrapped inside a TestSetup that creates
     * database objects to be used throughout the tests, then the
     * cleanDB parameter should be "false" to prevent cleanup of the
     * database objects that TestSetup created.
     * <BR>
     * Does NOT decorate for running in embedded mode, only for running on
     * an already started server.
     */
    public static Test defaultExistingServerSuite(Class testClass, boolean cleanDB)
    {
         final BaseTestSuite suite = new BaseTestSuite(suiteName(testClass));
         
        if (cleanDB)
        {
            suite.addTest(new CleanDatabaseTestSetup(clientExistingServerSuite(testClass)));
        }
        else
        {
            suite.addTest(clientExistingServerSuite(testClass));
        }

        return (suite);
    }

    /**
     * Return a Test suite that contains all the test fixtures
     * for the passed in class running in client server configuration
     * on an already started server on a given host and port number.
     * <BR>
     * Takes a boolean argument to determine whether or not to "clean"
     * the test database before each suite.  If the resultant
     * suite is going to be wrapped inside a TestSetup that creates
     * database objects to be used throughout the tests, then the
     * cleanDB parameter should be "false" to prevent cleanup of the
     * database objects that TestSetup created.
     * <BR>
     * Takes a String argument to specify which host the server runs on, and
     * takes an int argument to specify the port number to use.
     * <BR>
     * Does NOT decorate for running in embedded mode, only for running on
     * an already started server.
     * <BR>
     * The set of client server tests
     * is decorated with a CleanDatabaseTestSetup.
     * <BR>
     * The client server configuration is setup using clientExistingServerSuite
     */
    public static Test existingServerSuite(Class testClass, 
            boolean cleanDB,
            String hostName,
            int portNumber)
    {
         final BaseTestSuite suite = new BaseTestSuite(suiteName(testClass));
         
        if (cleanDB)
        {
            suite.addTest(new CleanDatabaseTestSetup(
//IC see: https://issues.apache.org/jira/browse/DERBY-4427
                    clientExistingServerSuite(testClass, hostName, portNumber)));
        }
        else
        {
            suite.addTest(clientExistingServerSuite(testClass, hostName, portNumber));
        }

        return (suite);
    }
    public static Test existingServerSuite(Class testClass, 
            boolean cleanDB,
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
            String hostName,
            int portNumber,
            String dbPath)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
         final BaseTestSuite suite = new BaseTestSuite(suiteName(testClass));
         
        if (cleanDB)
        {
            suite.addTest(new CleanDatabaseTestSetup(
                    clientExistingServerSuite(testClass, hostName, portNumber, dbPath)));
        }
        else
        {
            suite.addTest(clientExistingServerSuite(testClass, hostName, portNumber, dbPath));
        }

        return (suite);
    }

    /**
     * Return a Test suite that contains all the test fixtures
     * for the passed in class running in embedded and client-
     * server *JDBC3* configurations.
     * <BR>
     * Each set of embedded and set of client server tests is
     * decorated with a CleanDatabaseTestSetup.
     * <BR>
     */
    public static Test forceJDBC3Suite(Class testClass)
    {
        final BaseTestSuite suite = new BaseTestSuite(suiteName(testClass));
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(
            new CleanDatabaseTestSetup(
                forceJDBC3Embedded(embeddedSuite(testClass))));

        suite.addTest(
            new CleanDatabaseTestSetup(
                forceJDBC3NetClient(clientServerSuite(testClass))));

        return (suite);
    }

    /**
     * Generate a suite name from a class name, taking
     * only the last element of the fully qualified class name.
     */
    static String suiteName(Class testClass)
    {
        int lastDot = testClass.getName().lastIndexOf('.');
//IC see: https://issues.apache.org/jira/browse/DERBY-1952
//IC see: https://issues.apache.org/jira/browse/DERBY-1971
        String suiteName = testClass.getName();
        if (lastDot != -1)
            suiteName = suiteName.substring(lastDot + 1, suiteName.length());
        
        return suiteName;
    }

    /**
     * Create a suite for the passed test class that includes
     * all the default fixtures from the class.
      */
    public static Test embeddedSuite(Class testClass)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        return new BaseTestSuite(testClass,
                suiteName(testClass)+":embedded");
    }
    
    /**
     * Create a suite for the passed test class that includes
     * all the default fixtures from the class, wrapped in
     * a derbyClientServerDecorator.
     * 
     */
    public static Test clientServerSuite(Class testClass)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5917
        return clientServerDecorator(bareClientServerSuite(testClass));
    }
    /**
     * Create a suite for the passed test class that includes
     * all the default fixtures from the class, wrapped in
     * a derbyClientServerDecorator with alternative port.
     * 
     */

    public static Test clientServerSuiteWithAlternativePort(Class testClass) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5917
        return clientServerDecoratorWithAlternativePort(
                bareClientServerSuite(testClass));
    }

    /**
     * Equivalent to 'clientServerSuite' above, but assumes server is
     * already running.
     *
     */
    public static Test clientExistingServerSuite(Class testClass)
    {
        // Will not start server and does not stop it when done.
//IC see: https://issues.apache.org/jira/browse/DERBY-5917
        return defaultExistingServerDecorator(bareClientServerSuite(testClass));
    }
    
    /**
     * Create a suite for the passed test class that includes
     * all the default fixtures from the class, wrapped in
     * a existingServerDecorator.
     * <BR>
     * Equivalent to 'clientServerSuite' above, but assumes server is
     * already running. Will also NOT shut down the server.
     *
     */
    public static Test clientExistingServerSuite(Class testClass, String hostName, int portNumber)
    {
               // Will not start server and does not stop it when done!.
//IC see: https://issues.apache.org/jira/browse/DERBY-5917
        return existingServerDecorator(bareClientServerSuite(testClass),
                hostName, portNumber);
    }
    public static Test clientExistingServerSuite(Class testClass, 
            String hostName, int portNumber, String dbPath)
    {
               // Will not start server and does not stop it when done!.
        return existingServerDecorator(bareClientServerSuite(testClass),
                hostName, portNumber, dbPath);
    }

    /**
     * Return a decorator for the passed in tests that sets the
     * configuration for the client to be Derby's JDBC client
     * and to start the network server at setUp.
     * <BR>
     * The database configuration (name etc.) is based upon
     * the previous configuration.
     * <BR>
     * The previous TestConfiguration is restored at tearDown and
     * the network server is shutdown.
     * @param suite the suite to decorate
     */
    public static Test clientServerDecorator(Test suite)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2419
        Test test = new NetworkServerTestSetup(suite, false);
            
        return defaultServerDecorator(test);
    }
  
    /**
     * Return a decorator for the passed in tests that sets the
     * configuration for the client to be Derby's JDBC client
     * and to start the network server at setUp.
     * <BR>
     * The database configuration (name etc.) is based upon
     * the previous configuration.
     * <BR>
     * The previous TestConfiguration is restored at tearDown and
     * the network server is shutdown.
     */
    public static Test clientServerDecorator
      (
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
       Test suite,
       String[] systemProperties,
       String[] startupArgs,
       boolean serverShouldComeUp,
       String moduleOrClassPath,
       boolean useModules,
       boolean suppressServerDiagnostics
       )
    {
        Test test = new NetworkServerTestSetup
          (
           suite, systemProperties, startupArgs, serverShouldComeUp,
           moduleOrClassPath, useModules, suppressServerDiagnostics
           );
            
        return defaultServerDecorator(test);
    }

    /**
     * Return a decorator for the passed in tests that sets the
     * configuration for the client to be Derby's JDBC client
     * and to start the network server at setUp.
     * <BR>
     * The database configuration (name etc.) is based upon
     * the previous configuration.
     * <BR>
     * The previous TestConfiguration is restored at tearDown and
     * the network server is shutdown.
     * @param suite the suite to decorate
     */
    public static Test clientServerDecoratorWithPort(Test suite, int port)
    {
        Test test = new NetworkServerTestSetup(suite, false);
//IC see: https://issues.apache.org/jira/browse/DERBY-4223
//IC see: https://issues.apache.org/jira/browse/DERBY-4217

        return existingServerDecorator(test,"localhost",port);
    }

    /**
     * Wrapper to use the alternative port number.
     */
    public static Test clientServerDecoratorWithAlternativePort(Test suite) {
        Test test = new NetworkServerTestSetup(suite, false);
//IC see: https://issues.apache.org/jira/browse/DERBY-1952
//IC see: https://issues.apache.org/jira/browse/DERBY-2047

//IC see: https://issues.apache.org/jira/browse/DERBY-3211
        return defaultServerDecoratorWithAlternativePort(test);
    }
    /**
     * Decorate a test to use suite's default host and port, 
     * but assuming the server is already running.
     */
    public static Test defaultExistingServerDecorator(Test test)
    {
        // As defaultServerDecorator but assuming 
        // server is already started.
        // Need to have client 
        // and not running in J2ME (JSR169).
//IC see: https://issues.apache.org/jira/browse/DERBY-2807
        if (!(Derby.hasClient())
                || JDBC.vmSupportsJSR169())
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite(
                "empty: no network server support in JSR169 " +
                "(or derbyclient.jar missing).");
        }
        
        Test r =
                new ServerSetup(test, DEFAULT_HOSTNAME, TestConfiguration.getCurrent().getPort());
        ((ServerSetup)r).setJDBCClient(JDBCClient.DERBYNETCLIENT); 
        
        return r;
    }
   
    /**
     * Decorate a test to use suite's default host and port.
     */
    public static Test defaultServerDecorator(Test test)
    {
        // Need to have network server and client and not
        // running in J2ME (JSR169).
        if (!supportsClientServer()) {
            return new BaseTestSuite("empty: no network server support");
        }

        //
        // This looks bogus to me. Shouldn't this get the hostname and port
        // which are specific to this test run (perhaps overridden on the
        // command line)?
        //
        return new ServerSetup(test, DEFAULT_HOSTNAME, TestConfiguration.getCurrent().getPort());
    }
   /**
    * A variant of defaultServerDecorator allowing 
    * non-default hostname and portnumber.
    */
    public static Test existingServerDecorator(Test test, 
//IC see: https://issues.apache.org/jira/browse/DERBY-3163
            String hostName, int PortNumber)
    {
    	// Need to have network server and client and not
        // running in J2ME (JSR169).
        if (!supportsClientServer()) {
            return new BaseTestSuite("empty: no network server support");
        }

        Test r =
                new ServerSetup(test, hostName, PortNumber);
        ((ServerSetup)r).setJDBCClient(JDBCClient.DERBYNETCLIENT);
        return r;
    }
    /**
    * A variant of defaultServerDecorator allowing 
    * non-default hostname, portnumber and database name.
    */
    public static Test existingServerDecorator(Test test, 
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
            String hostName, int PortNumber, String dbPath)
    {
    	// Need to have network server and client and not
        // running in J2ME (JSR169).
//IC see: https://issues.apache.org/jira/browse/DERBY-5917
//IC see: https://issues.apache.org/jira/browse/DERBY-5917
        if (!supportsClientServer()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite("empty: no network server support");
        }

        Test r =
                new ServerSetup(test, hostName, PortNumber);
        ((ServerSetup)r).setJDBCClient(JDBCClient.DERBYNETCLIENT);
        ((ServerSetup)r).setDbPath(dbPath);
        return r;
    }
   
    /**
     * Decorate a test to use suite's default host and Alternative port.
     */
    public static Test defaultServerDecoratorWithAlternativePort(Test test) {
        // Need to have network server and client and not
        // running in J2ME (JSR169).
//IC see: https://issues.apache.org/jira/browse/DERBY-5917
//IC see: https://issues.apache.org/jira/browse/DERBY-5917
        if (!supportsClientServer()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite("empty: no network server support");
        }

        int port = getCurrent().getNextAvailablePort();

        //
        // This looks bogus to me. Shouldn't this get the hostname and port
        // which are specific to this test run (perhaps overridden on the
        // command line)?
        //
        return new ServerSetup(test, DEFAULT_HOSTNAME, port);
    }

    /**
     * Check if client and server testing is supported in the test environment.
     */
    private static boolean supportsClientServer() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5917
        return JDBC.vmSupportsJDBC3() && Derby.hasClient() && Derby.hasServer();
    }

    /**
     * Create a suite of test cases to run in a client/server environment. The
     * returned test suite is not decorated with a ServerSetup.
     *
     * @param testClass the class from which to extract the test cases
     * @return a test suite with all the test cases in {@code testClass}, or
     * an empty test suite if client/server is not supported in the test
     * environment
     */
    private static Test bareClientServerSuite(Class testClass) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite(suiteName(testClass) + ":client");

        if (supportsClientServer()) {
            suite.addTestSuite(testClass);
        }
        return suite;
    }

    /**
     * Generate the unique database name for single use.
     */
    public static synchronized String generateUniqueDatabaseName()
    {
        // Forward slash is ok, Derby treats database names
        // as URLs and translates forward slash to the local
        // separator.
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        String dbName = "singleUse/oneuse";
        dbName = dbName.concat(Integer.toHexString(uniqueDB++));
        return dbName;
    }

 
    /**
     * Decorate a test to use a new database that is created upon the
     * first connection request to the database and shutdown and deleted at
     * tearDown. The configuration differs only from the current configuration
     * by the list of used databases. The new database name
     * is generated automatically as 'singleUse/oneuseXX' where 'XX' is
     * the unique number. The generated database name is added at the end
     * of <code>usedDbNames</code> and assigned as a default database name.
     * This decorator expects the database file to be local so it
     * can be removed.
     * @param test Test to be decorated
     * @return decorated test.
     */
    public static TestSetup singleUseDatabaseDecorator(Test test)
    {
        String dbName = generateUniqueDatabaseName();

//IC see: https://issues.apache.org/jira/browse/DERBY-2295
        return new DatabaseChangeSetup(new DropDatabaseSetup(test, dbName), dbName, dbName, true);
    }


    /**
     * Decorate a test to use a new database that is created upon the first
     * connection request to the database and shutdown and deleted at
     * tearDown. The configuration differs only from the current configuration
     * by the list of used databases. The generated database name is added at
     * the end of <code>usedDbNames</code> and assigned as a default database
     * name.  This decorator expects the database file to be local so it can be
     * removed.
     * @param test Test to be decorated
     * @param dbName We sometimes need to know outside to be able to pass it on
     *               to other VMs/processes.
     * @return decorated test.
     */
    public static TestSetup singleUseDatabaseDecorator(Test test, String dbName)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4179
//IC see: https://issues.apache.org/jira/browse/DERBY-4646
//IC see: https://issues.apache.org/jira/browse/DERBY-4647
        return new DatabaseChangeSetup(
            new DropDatabaseSetup(test, dbName), dbName, dbName, true);
    }

    /**
     * Decorate a test to use a new database that is created upon the
     * first connection request to the database and deleted at
     * tearDown. In contrast to plain singleUseDatabaseDecorator, the
     * database is expected to be shutdown by the test.  The
     * configuration differs only from the current configuration by
     * the list of used databases. The new database name is generated
     * automatically as 'singleUse/oneuseXX' where 'XX' is the unique
     * number. The generated database name is added at the end of
     * <code>usedDbNames</code> and assigned as a default database
     * name.  This decorator expects the database file to be local so
     * it can be removed.
     * @param test Test to be decorated
     * @return decorated test.
     */
    public static TestSetup singleUseDatabaseDecoratorNoShutdown(Test test)
    {
        String dbName = generateUniqueDatabaseName();

        return new DatabaseChangeSetup(
//IC see: https://issues.apache.org/jira/browse/DERBY-6546
            new DropDatabaseSetup(test, dbName, false),
            dbName, dbName, true);
    }

    /**
     * Decorate a test to use a new database that is created upon the
     * first connection request to the database and shutdown and deleted at
     * tearDown. The configuration differs only from the current configuration
     * by the list of used databases. 
     * The passed database name is mapped to the generated database
     * name 'singleUse/oneuseXX' where 'XX' is the unique number.
     * (by generateUniqueDatabaseName). The generated database name is added
     * at the end of <code>usedDbNames</code>.
     * This decorator expects the database file to be local so it
     * can be removed.
     * @param test Test to be decorated
     * @param logicalDbName The logical database name. This name is used to identify
     * the database in openConnection(String logicalDatabaseName) method calls.
     * @return decorated test.
     */
    public static DatabaseChangeSetup additionalDatabaseDecorator(Test test, String logicalDbName)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2295
        return new DatabaseChangeSetup(new DropDatabaseSetup(test, logicalDbName),
                                       logicalDbName,
                                       generateUniqueDatabaseName(),
                                       false);
    }
    
    /**
     * Similar to additionalDatabaseDecorator except the database will
     * not be shutdown, only deleted. It is the responsibility of the
     * test to shut it down.
     *
     * @param test Test to be decorated
     * @param logicalDbName The logical database name. This name is
     *                      used to identify the database in
     *                      openConnection(String logicalDatabaseName)
     *                      method calls.
     * @return decorated test.
     */
    public static DatabaseChangeSetup additionalDatabaseDecoratorNoShutdown(
        Test test,
        String logicalDbName)
    {
        return additionalDatabaseDecoratorNoShutdown( test, logicalDbName, false );
    }

    /**
     * Similar to additionalDatabaseDecorator except the database will
     * not be shutdown, only deleted. It is the responsibility of the
     * test to shut it down.
     *
     * @param test Test to be decorated
     * @param logicalDbName The logical database name. This name is
     *                      used to identify the database in
     *                      openConnection(String logicalDatabaseName)
     *                      method calls.
     * @param defaultDB True if the database should store its own name in its TestConfiguration.
     * @return decorated test.
     */
    public static DatabaseChangeSetup additionalDatabaseDecoratorNoShutdown
        (
         Test test,
         String logicalDbName,
         boolean defaultDB
        )
    {
        return new DatabaseChangeSetup(
//IC see: https://issues.apache.org/jira/browse/DERBY-6546
            new DropDatabaseSetup(test, logicalDbName, false),
            logicalDbName,
            generateUniqueDatabaseName(),
            defaultDB);
    }

    /**
     * Similar to additionalDatabaseDecorator except the database will
     * not be shutdown, only deleted. It is the responsibility of the
     * test to shut it down.
     *
     * @param test Test to be decorated
     * @param logicalDbName The logical database name. This name is
     *                      used to identify the database in
     *                      openConnection(String logicalDatabaseName)
     *                      method calls.
     * @param physicalDbName - Real database name on disk.
     * @return decorated test.
     */
    public static DatabaseChangeSetup additionalDatabaseDecoratorNoShutdown(
        Test test,
        String logicalDbName, String physicalDbName )
    {
        return new DatabaseChangeSetup(
//IC see: https://issues.apache.org/jira/browse/DERBY-6546
            new DropDatabaseSetup(test, logicalDbName, false),
            logicalDbName,
            physicalDbName,
            false);
    }

    /**
     * Decorate a test changing the default user name and password.
     * Typically used along with DatabasePropertyTestSetup.builtinAuthentication.
     * The tearDown method resets the default user and password value to
     * their previous settings.
     * 
     * @param test Test to decorate
     * @param user New default user
     * @param password New password
     * @return decorated test
     * 
     * @see DatabasePropertyTestSetup#builtinAuthentication(Test, String[], String)
     */
    public static Test changeUserDecorator(Test test, String user, String password)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        return new ChangeUserSetup(test, user, password);
    }   
    
    /**
     * Decorate a test to use the default database that has
     * was created in SQL authorization mode.
     * The tearDown reverts the configuration to the previous
     * configuration.
     * 
     * The database owner of this default SQL authorization mode
     * database is TEST_DBO. This decorator sets the default user
     * to be TEST_DBO.
     * 
     * Tests can use this in conjunction with
     * DatabasePropertyTestSetup.builtinAuthentication
     * to set up BUILTIN authentication and changeUserDecorator
     * to switch users. The database owner TEST_DBO must be included
     * in the list of users provided to builtinAuthentication.
     * This decorator must be the outer one in this mode.
     * <code>
     * test = DatabasePropertyTestSetup.builtinAuthentication(test,
//IC see: https://issues.apache.org/jira/browse/DERBY-2466
                new String[] {TEST_DBO,"U1","U2",},
                "nh32ew");
       test = TestConfiguration.sqlAuthorizationDecorator(test);
     * </code>
     * A utility version of sqlAuthorizationDecorator is provided
     * that combines the two setups.
     * 
     * @param test Test to be decorated
     * @return decorated test.
     * 
     * @see DatabasePropertyTestSetup#builtinAuthentication(Test, String[], String)
     */
    public static Test sqlAuthorizationDecorator(Test test)
    {       
        // Set the SQL authorization mode as a database property
        // with a modified DatabasePropertyTestSetup that does not
        // reset it.
        final Properties sqlAuth = new Properties();
        sqlAuth.setProperty("derby.database.sqlAuthorization", "true");
//IC see: https://issues.apache.org/jira/browse/DERBY-5705
        Test setSQLAuthMode = DatabasePropertyTestSetup.getNoTeardownInstance(
                test, sqlAuth, true);
        
        return changeUserDecorator(
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
            new DatabaseChangeSetup(setSQLAuthMode, DEFAULT_DBNAME_SQL, DEFAULT_DBNAME_SQL, true),
//IC see: https://issues.apache.org/jira/browse/DERBY-2419
            DerbyConstants.TEST_DBO, "dummy"); // DRDA doesn't like empty pw
    }


    /**
     * Same as sqlAuthorizationDecorator, except that the database is dropped
     * at teardown and the test is responsible for shutting down the database.
     *
     * @param test Test to be decorated
     * @return decorated test.
     *
     * @see TestConfiguration#sqlAuthorizationDecorator(Test test)
     */
    public static Test sqlAuthorizationDecoratorSingleUse(Test test)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6546
        return sqlAuthorizationDecoratorSingleUse(
                test, DEFAULT_DBNAME_SQL, false);
    }
    
    /**
     * Same as sqlAuthorizationDecoratorSingleUse, except that you can name
     * the database yourself, and you can choose whether or not the decorator
     * should shut down the database before it attempts to drop it.
     *
     * @param test Test to be decorated
     * @param dbName The name of the database to use in the test
     * @param shutdownDatabase Whether or not to shut down the database
     *   before it is dropped
     * @return decorated test.
     *
     * @see TestConfiguration#sqlAuthorizationDecorator(Test test)
     */
    public static Test sqlAuthorizationDecoratorSingleUse(
            Test test, String dbName, boolean shutdownDatabase)
    {
        // Set the SQL authorization mode as a database property
        // with a modified DatabasePropertyTestSetup that does not
        // reset it.
        final Properties sqlAuth = new Properties();
        sqlAuth.setProperty("derby.database.sqlAuthorization", "true");
//IC see: https://issues.apache.org/jira/browse/DERBY-5705
        Test setSQLAuthMode = DatabasePropertyTestSetup.getNoTeardownInstance(
                test, sqlAuth, true);

//IC see: https://issues.apache.org/jira/browse/DERBY-6546
        setSQLAuthMode = new DropDatabaseSetup(
                setSQLAuthMode, dbName, shutdownDatabase);
        
        setSQLAuthMode = new DatabaseChangeSetup
            ( setSQLAuthMode, dbName, dbName, true );

        return changeUserDecorator(setSQLAuthMode,
//IC see: https://issues.apache.org/jira/browse/DERBY-2419
                                   DerbyConstants.TEST_DBO,
                                   "dummy"); // DRDA doesn't like empty pw
    }
    

    /**
     * Utility version of sqlAuthorizationDecorator that also sets
     * up authentication. A combination of
     * DatabasePropertyTestSetup.builtinAuthentication wrapped in
     * sqlAuthorizationDecorator.
     * <BR>
     * The database owner of this default SQL authorization mode
     * database is TEST_DBO. This decorator sets the default user
     * to be TEST_DBO.
     * <BR>
     * Assumption is that no authentication is enabled on the default
     * SQL authorization database on entry.
     * 
     * @param users Set of users excluding the database owner, that will
     * be added by this decorator.
     */
    public static Test sqlAuthorizationDecorator(Test test,
            String[] users, String passwordToken)
    {
        String[] usersWithDBO = new String[users.length + 1];
//IC see: https://issues.apache.org/jira/browse/DERBY-2419
        usersWithDBO[0] = DerbyConstants.TEST_DBO;
        System.arraycopy(users, 0, usersWithDBO, 1, users.length);
        return sqlAuthorizationDecorator(
            DatabasePropertyTestSetup.builtinAuthentication(test, 
                    usersWithDBO, passwordToken));
    }
    
    /**
     * Return a decorator that changes the configuration to obtain
     * connections from a ConnectionPoolDataSource using
     * <code>getPooledConnection().getConnection()</code>
     * <p>
     * Note that statement pooling is enabled in the data source and in all the
     * connections created from it.
     * <p>
     * The tearDown reverts the configuration to the previous
     * configuration.
     * 
     * @param test the test/suite to decorate
     * @return A test setup with the requested decorator.
     */
    public static Test connectionCPDecorator(Test test)
    {
        if (JDBC.vmSupportsJDBC3()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3326
            return new ConnectorSetup(test,
             "org.apache.derbyTesting.junit.ConnectionPoolDataSourceConnector");
        } else {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite("ConnectionPoolDataSource not supported");
        }

    }

    /**
     * Return a decorator that changes the configuration to obtain
     * connections from an XADataSource using
     * <code>
     * getXAConnection().getConnection()
     * </code>
     * The connection is not connected to any global transaction,
     * thus it is in local connection mode.
     * The tearDown reverts the configuration to the previous
     * configuration.
     */
    public static Test connectionXADecorator(Test test)
    {
        if (JDBC.vmSupportsJDBC3()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2215
            return new ConnectorSetup(test,
                "org.apache.derbyTesting.junit.XADataSourceConnector");
        } else {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite("XADataSource not supported");
        }
    }
    /**
     * Return a decorator that changes the configuration to obtain
     * connections from a standard DataSource using
     * <code>
     * getConnection()
     * </code>
     * The tearDown reverts the configuration to the previous
     * configuration.
     */
    public static TestSetup connectionDSDecorator(Test test)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        return new ConnectorSetup(test,
            "org.apache.derbyTesting.junit.DataSourceConnector");
    }
    
    /**
     * Returns a decorator that forces the JDBC 3 embedded client  in
     * a Java SE 6/JDBC 4 environment. The only difference is that
     * the DataSource class names will be the "old" JDBC 3 versions
     * and not the JDBC 4 specific ones.
     * that
     * @param test
     */
    public static Test forceJDBC3Embedded(Test test)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        if (JDBC.vmSupportsJDBC4()) {
            test = new JDBCClientSetup(test, JDBCClient.EMBEDDED_30);
        }
        return test;
    }
    
    /**
     * Returns a decorator that forces the JDBC 3 network client in
     * a Java SE 6/JDBC 4 environment. The only difference is that
     * the DataSource class names will be the "old" JDBC 3 versions
     * and not the JDBC 4 specific ones.
     *
     * Assumption is that the received Test is an instance of ServerSetup,
     * which is the decorator for client server tests.  If that is not
     * the case then this method is a no-op.
     *
     * @param test Test around which to wrap the JDBC 3 network client
     *  configuration.
     */
    public static Test forceJDBC3NetClient(Test test)
    {
        if (JDBC.vmSupportsJDBC4() && (test instanceof ServerSetup))
            ((ServerSetup)test).setJDBCClient(JDBCClient.DERBYNETCLIENT_30);
        return test;
    }
    
    /**
     * Decorate a test changing the default ssl mode.
     * The tearDown method resets the default user and password value to
     * their previous settings.
     * 
     * @param test Test to decorate
     * @param ssl New ssl mode
     * @return decorated test
     */
    public static Test changeSSLDecorator(Test test, String ssl)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2274
        return new ChangeSSLSetup(test, ssl);
    }   
    
    /**
     * Default embedded configuration
     *
     */
    private TestConfiguration() {
        // Check for possibly passed in DatabaseName
        // this is used in OCRecoveryTest
//IC see: https://issues.apache.org/jira/browse/DERBY-5382
        String propDefDbName = getSystemProperties().getProperty(
                "derby.tests.defaultDatabaseName");
        if (propDefDbName != null)
            this.defaultDbName = propDefDbName;
        else
            this.defaultDbName=DEFAULT_DBNAME;
        usedDbNames.add(DEFAULT_DBNAME);
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        logicalDbMapping.put(DEFAULT_DBNAME, DEFAULT_DBNAME);
        this.userName = DEFAULT_USER_NAME;
        this.userPassword = DEFAULT_USER_PASSWORD;
//IC see: https://issues.apache.org/jira/browse/DERBY-4898
        this.connectionAttributes = new Properties();
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
        this.hostName = DEFAULT_HOSTNAME;
        this.port = basePort;
        this.isVerbose = Boolean.valueOf(
            getSystemProperties().getProperty(KEY_VERBOSE)).
            booleanValue();
//IC see: https://issues.apache.org/jira/browse/DERBY-2667
        this.doTrace = Boolean.valueOf(
            getSystemProperties().getProperty(KEY_TRACE)).
            booleanValue();
//IC see: https://issues.apache.org/jira/browse/DERBY-6401
        this.stopAfterFirstFail = Boolean.valueOf(
                getSystemProperties().getProperty(KEY_STOP_AFTER_FIRST_FAIL)).
                booleanValue();
        this.jdbcClient = JDBCClient.getDefaultEmbedded();
//IC see: https://issues.apache.org/jira/browse/DERBY-2274
        this.ssl = null;
        this.jmxPort = getNextAvailablePort();
        println("basePort=" + basePort + ", bogusPort=" + bogusPort +
                ", jmxPort=" + jmxPort);
        url = createJDBCUrlWithDatabaseName(defaultDbName);
        initConnector(null);
 
    }

    /**
     * Obtain a new configuration identical to the passed one.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
    TestConfiguration(TestConfiguration copy)
    {
        this.defaultDbName = copy.defaultDbName;
        this.usedDbNames.addAll(copy.usedDbNames);
        logicalDbMapping.putAll(copy.logicalDbMapping);
        this.userName = copy.userName;
        this.userPassword = copy.userPassword;
        this.connectionAttributes = new Properties(copy.connectionAttributes);

        this.isVerbose = copy.isVerbose;
        this.doTrace = copy.doTrace;
        this.port = copy.port;
        this.jmxPort = copy.jmxPort;
        
        this.jdbcClient = copy.jdbcClient;
        this.hostName = copy.hostName;
        
        this.ssl = copy.ssl;

        this.url = copy.url;
        initConnector(copy.connector);
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-2215
    TestConfiguration(TestConfiguration copy, JDBCClient client,
            String hostName, int port)
    {
        this.defaultDbName = copy.defaultDbName;
        this.usedDbNames.addAll(copy.usedDbNames);        
        logicalDbMapping.putAll(copy.logicalDbMapping);
        this.userName = copy.userName;
        this.userPassword = copy.userPassword;
        this.connectionAttributes = new Properties(copy.connectionAttributes);

        this.isVerbose = copy.isVerbose;
        this.doTrace = copy.doTrace;
        this.port = port;
        this.jmxPort = copy.jmxPort;
        if (bogusPort == port) {
            throw new IllegalStateException(
                    "port cannot equal bogusPort: " + bogusPort);
        }
        
        this.jdbcClient = client;
        this.hostName = hostName;

        this.ssl = copy.ssl;
        
        this.url = createJDBCUrlWithDatabaseName(defaultDbName);
        initConnector(copy.connector);
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-4417
    TestConfiguration(TestConfiguration copy, JDBCClient client,
            String hostName, int port, String dataBasePath)
    {
        this.defaultDbName = dataBasePath;
        this.usedDbNames.addAll(copy.usedDbNames);        
        logicalDbMapping.putAll(copy.logicalDbMapping);
        this.userName = copy.userName;
        this.userPassword = copy.userPassword;
        this.connectionAttributes = new Properties(copy.connectionAttributes);

        this.isVerbose = copy.isVerbose;
//IC see: https://issues.apache.org/jira/browse/DERBY-2667
        this.doTrace = copy.doTrace;
        this.port = port;
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        this.jmxPort = copy.jmxPort;
//IC see: https://issues.apache.org/jira/browse/DERBY-4700
//IC see: https://issues.apache.org/jira/browse/DERBY-4700
        if (bogusPort == port) {
            throw new IllegalStateException(
                    "port cannot equal bogusPort: " + bogusPort);
        }
        
        this.jdbcClient = client;
        this.hostName = hostName;

//IC see: https://issues.apache.org/jira/browse/DERBY-2274
        this.ssl = copy.ssl;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        this.url = createJDBCUrlWithDatabaseName(defaultDbName);
        initConnector(copy.connector);
    }

    /**
     * Obtain a new configuration identical to the passed in
     * one except for the default user and password.
     * @param copy Configuration to copy.
     * @param user New default user
     * @param password New default password.
     */
    TestConfiguration(TestConfiguration copy, String user,
            String password, String passwordToken)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        this.defaultDbName = copy.defaultDbName;
        this.usedDbNames.addAll(copy.usedDbNames);
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        logicalDbMapping.putAll(copy.logicalDbMapping);
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        this.userName = user;
        this.userPassword = password;
        this.passwordToken = passwordToken == null ?
                copy.passwordToken : passwordToken;
        this.connectionAttributes = new Properties(copy.connectionAttributes);

        this.isVerbose = copy.isVerbose;
        this.doTrace = copy.doTrace;
        this.port = copy.port;
        this.jmxPort = copy.jmxPort;
        
        this.jdbcClient = copy.jdbcClient;
        this.hostName = copy.hostName;

//IC see: https://issues.apache.org/jira/browse/DERBY-2274
//IC see: https://issues.apache.org/jira/browse/DERBY-2274
        this.ssl = copy.ssl;
        
        this.url = copy.url;
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        initConnector(copy.connector);
    }

    /**
     * Obtains a new configuration identical to the passed in one, except for
     * the default SSL mode.
     * <p>
     * The modes supported at the moment are <tt>basic</tt> and <tt>off</tt>.
     * The mode <tt>peerAuthentication</tt> is not yet supported.
     *
     * @param copy configuration to copy
     * @param ssl default SSL mode
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-2274
    TestConfiguration(TestConfiguration copy, String ssl)
    {
        this(copy);
        this.ssl = ssl;
    }

    /**
     * Obtain a new configuration identical to the passed in
     * one except for the database name. The passed database name
     * is added at the end of the list of used databases.
     * If the <code>defaulDb</code> parameter is <code>true</code>
     * the new database name is used as a default database.
     * @param copy Configuration to copy.
     * @param dbName New database name
     * @param defaultDb Indicates that the passed <code>dbName</code> is supposed
     * to be used as the default database name.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
    TestConfiguration(TestConfiguration copy, String logicalDbName,
                      String dbName, boolean defaultDb)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        this.usedDbNames.addAll(copy.usedDbNames);
        this.usedDbNames.add(dbName);
        logicalDbMapping.putAll(copy.logicalDbMapping);

        // Can not use the same logical name for different database.
        // If this assert will make failures it might be safely removed
        // since having more physical databases accessible throught the same
        // logical database name will access only the last physical database
        Assert.assertNull(logicalDbMapping.put(logicalDbName, dbName));
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

        if (defaultDb) {
            this.defaultDbName = dbName;
        } else {
            this.defaultDbName = copy.defaultDbName;
        }
        
        this.userName = copy.userName;
        this.userPassword = copy.userPassword;
        this.connectionAttributes = new Properties(copy.connectionAttributes);
//IC see: https://issues.apache.org/jira/browse/DERBY-4884
//IC see: https://issues.apache.org/jira/browse/DERBY-4884
//IC see: https://issues.apache.org/jira/browse/DERBY-4884
//IC see: https://issues.apache.org/jira/browse/DERBY-4884
//IC see: https://issues.apache.org/jira/browse/DERBY-4884

        this.isVerbose = copy.isVerbose;
//IC see: https://issues.apache.org/jira/browse/DERBY-2667
//IC see: https://issues.apache.org/jira/browse/DERBY-2667
//IC see: https://issues.apache.org/jira/browse/DERBY-2667
        this.doTrace = copy.doTrace;
        this.port = copy.port;
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        this.jmxPort = copy.jmxPort;
        
        this.jdbcClient = copy.jdbcClient;
        this.hostName = copy.hostName;

//IC see: https://issues.apache.org/jira/browse/DERBY-2274
        this.ssl = copy.ssl;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        this.url = createJDBCUrlWithDatabaseName(this.defaultDbName);
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        initConnector(copy.connector);
    }
  
    /**
     * This constructor creates a TestConfiguration from a Properties object.
     *
     * @throws NumberFormatException if the port specification is not an integer.
     */
    private TestConfiguration(Properties props) 
        throws NumberFormatException {

//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        defaultDbName = props.getProperty(KEY_DBNAME, DEFAULT_DBNAME);
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        usedDbNames.add(defaultDbName);
        logicalDbMapping.put(defaultDbName, defaultDbName);
        userName = props.getProperty(KEY_USER_NAME, DEFAULT_USER_NAME);
        userPassword = props.getProperty(KEY_USER_PASSWORD, 
                                         DEFAULT_USER_PASSWORD);
//IC see: https://issues.apache.org/jira/browse/DERBY-4898
        connectionAttributes = new Properties();
        hostName = props.getProperty(KEY_HOSTNAME, DEFAULT_HOSTNAME);
        isVerbose = Boolean.valueOf(props.getProperty(KEY_VERBOSE)).booleanValue();
//IC see: https://issues.apache.org/jira/browse/DERBY-2667
        doTrace =  Boolean.valueOf(props.getProperty(KEY_TRACE)).booleanValue();
        port = basePort;
        jmxPort = getNextAvailablePort();
//IC see: https://issues.apache.org/jira/browse/DERBY-4700
//IC see: https://issues.apache.org/jira/browse/DERBY-4700
        println("basePort=" + basePort + ", bogusPort=" + bogusPort +
                ", jmxPort=" + jmxPort);

//IC see: https://issues.apache.org/jira/browse/DERBY-2274
        ssl = props.getProperty(KEY_SSL);
        
        String framework = props.getProperty(KEY_FRAMEWORK, DEFAULT_FRAMEWORK);
        
        if ("DerbyNetClient".equals(framework)) {
            jdbcClient = JDBCClient.DERBYNETCLIENT;
        } else if ("DerbyNet".equals(framework)) {
            jdbcClient = JDBCClient.DB2CLIENT;
        } else {
            jdbcClient = JDBCClient.getDefaultEmbedded();
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
        url = createJDBCUrlWithDatabaseName(defaultDbName);
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        initConnector(null);
    }

    /**
     * Create a copy of this configuration with some additional connection
     * attributes.
     *
     * @param attrs the extra connection attributes
     * @return a copy of the configuration with extra attributes
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-4884
    TestConfiguration addConnectionAttributes(Properties attrs) {
        TestConfiguration copy = new TestConfiguration(this);
        Enumeration e = attrs.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            String val = attrs.getProperty(key);
            copy.connectionAttributes.setProperty(key, val);
        }
        copy.initConnector(connector);
        return copy;
    }

    /**
     * Get the system properties in a privileged block.
     *
     * @return the system properties.
     */
    public  static final Properties getSystemProperties() {
        // Fetch system properties in a privileged block.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return AccessController.doPrivileged(
                new PrivilegedAction<Properties>() {
            public Properties run() {
                return System.getProperties();
            }
        });
    }

    /**
     * Create JDBC connection url, including the name of the database.
     *
     * @return JDBC connection url, without attributes.
     */
    private String createJDBCUrlWithDatabaseName(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2024
        if (JDBC.vmSupportsJDBC3())
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-1952
//IC see: https://issues.apache.org/jira/browse/DERBY-2047
            String url;
           if (jdbcClient.isEmbedded()) {
               url = jdbcClient.getUrlBase();
           } else {
               url = jdbcClient.getUrlBase() + hostName + ":" + port + "/";
           }
           return url.concat(name);
        }
        // No DriverManager support so no URL support.
        return null;
    }
    
    /**
     * Initialize the connection factory.
     * Defaults to the DriverManager implementation
     * if running JDBC 2.0 or higher, otherwise a
     * DataSource implementation for JSR 169.
     *
     */
    private void initConnector(Connector oldConnector)
    {
        if (oldConnector != null)
        {
            // Use the same type of connector as the
            // configuration we are copying from.
            
            try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                Class<?> clazz = Class.forName(oldConnector.getClass().getName());
                connector = (Connector) clazz.getConstructor().newInstance();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }            
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-2024
        else if (JDBC.vmSupportsJDBC3())
        {
            try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                Class<?> clazz = Class.forName("org.apache.derbyTesting.junit.DriverManagerConnector");
                connector = (Connector) clazz.getConstructor().newInstance();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            
        } else {
            connector = new DataSourceConnector();
        }
        connector.setConfiguration(this);

//IC see: https://issues.apache.org/jira/browse/DERBY-6094
        try {
            String  loginTimeoutString = BaseTestCase.getSystemProperty( KEY_LOGIN_TIMEOUT );
            
            if ( loginTimeoutString != null )
            {
                int loginTimeout = Integer.parseInt( loginTimeoutString );

                connector.setLoginTimeout( loginTimeout );
            }
        }
        catch (Exception e) { Assert.fail(e.getMessage()); }
    }

    /**
     * Get configured JDBCClient object.
     *
     * @return JDBCClient
     */
    public JDBCClient getJDBCClient() {
        return jdbcClient;
    }

    /**
     * <p>
     * Return the jdbc url for connecting to the default database.
     * </p>
     *
     * <p>
     * The returned URL does not include the connection attributes. These must
     * either be appended to the URL when connecting, or they must be passed
     * as a {@code Properties} object to {@code DriverManager.getConnection()}.
     * </p>
     *
     * @return JDBC url.
     */
    public String getJDBCUrl() {
        return url;
    }

    /**
     * Return the jdbc url for a connecting to the database.
     * 
     * @param databaseName name of database.
     * @return JDBC connection url, including database name.
     */
    public String getJDBCUrl(String databaseName) {
        return createJDBCUrlWithDatabaseName(databaseName);
    }
    
    /**
     * Return the default database name.
     * 
     * @return default database name.
     */
    public String getDefaultDatabaseName() {
        return defaultDbName;
    }
    
    /**
     * Return the physical name for a database
     * given its logical name.
     * 
     * @return Physical name of the database.
     */
    public String getPhysicalDatabaseName(String logicalName) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2295
        return (String) logicalDbMapping.get(logicalName);
    }

    /**
     * Return the user name.
     * 
     * @return user name.
     */
    public String getUserName() {
        return userName;
    }
    
    /**
     * Return the user password.
     * 
     * @return user password.
     */
    public String getUserPassword() {
        return userPassword;
    }

    /**
     * Return the connection attributes to use in this configuration. The
     * attributes won't contain user name or password. Use
     * {@link #getUserName()} or {@link #getUserPassword()} instead to
     * retrieve those attributes.
     *
     * @return connection attributes (never {@code null})
     */
    public Properties getConnectionAttributes() {
//IC see: https://issues.apache.org/jira/browse/DERBY-4884
        return connectionAttributes;
    }

    /**
     * Get a flat string representation of the connection attributes. To
     * be used in the connectionAttributes property of a data source.
     *
     * @return all connection attributes concatenated ({@code null} if there
     * are no attributes)
     */
    String getConnectionAttributesString() {
        StringBuffer sb = new StringBuffer();
        Enumeration e = connectionAttributes.propertyNames();
        boolean first = true;
        while (e.hasMoreElements()) {
            if (!first) {
                sb.append(';');
            }
            String key = (String) e.nextElement();
            sb.append(key);
            sb.append('=');
            sb.append(connectionAttributes.getProperty(key));
            first = false;
        }

        if (first) {
            // No connection attributes.
            return null;
        }

        return sb.toString();
    }

    /**
     * Return the host name for the network server.
     *
     * @return host name.
     */
    public String getHostName() {
//IC see: https://issues.apache.org/jira/browse/DERBY-1157
        return hostName;
    }

    /**
     * Return if the base port is default or not.
     *
     * @return true if base port is default.
     */
    public static boolean isDefaultBasePort() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6178
        return (basePort == DEFAULT_PORT);
    }

    public static int getBasePort() {
//IC see: https://issues.apache.org/jira/browse/DERBY-2076
        return basePort;
    }

    /**
     * Get port number for network server.
     * 
     * @return port number.
     */
    public int getPort() {
        return port;
    }
    
    /**
     * Get the next available port. This method is multi-purposed.
     * It can be used for alternative servers and also for JMX and replication.
     * 
     * @return port number.
     */
    public int getNextAvailablePort() {
    	/* We want to crash. If you are reading this, you have to increment
    	 * the MAX_PORTS_USED constant and to edit the wiki page relative to
    	 * concurrent test running */
    	if (assignedPortCount+1 > MAX_PORTS_USED) {
    		Assert.fail("Port "+(lastAssignedPort+1)+" exceeeds expected maximum. " +
    					"You may need to update TestConfiguration.MAX_PORTS_USED and "+
    					"the Wiki page at http://wiki.apache.org/db-derby/DerbyJUnitTesting "+
    					"if test runs now require more available ports");
    	}
    	
    	int possiblePort = lastAssignedPort + 1;
		
		assignedPortCount++;
		
		lastAssignedPort = possiblePort;
		return possiblePort;
    }
    
    /**
     * Gets the value of the port number that may be used for "remote"
     * JMX monitoring and management.
     * @return the port number on which the JMX MBean server is listening for 
     *         connections
     */
    public int getJmxPort() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
        return jmxPort;
    }

    /**
     * Returns a port number where no Derby network servers are supposed to
     * be running.
     *
     * @return A port number where no Derby network servers are started.
     */
    public int getBogusPort() {
//IC see: https://issues.apache.org/jira/browse/DERBY-4700
        return bogusPort;
    }

    /**
     * Get ssl mode for network server
     * 
     * @return ssl mode
     */
    public String getSsl() {
//IC see: https://issues.apache.org/jira/browse/DERBY-2274
        return ssl;
    }

    
    /**
     * Open connection to the default database.
     * If the database does not exist, it will be created.
     * A default username and password will be used for the connection.
     *
     * @return connection to default database.
     */
    public Connection openDefaultConnection()
        throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2036
//IC see: https://issues.apache.org/jira/browse/DERBY-2043
//IC see: https://issues.apache.org/jira/browse/DERBY-2047
        return connector.openConnection();
    }
    
    /**
     * Open connection to the default database.
     * If the database does not exist, it will be created.
     *
     * @return connection to default database.
     */
    Connection openDefaultConnection(String user, String password)
        throws SQLException {
        return connector.openConnection(user, password);
    }

    /**
     * Open connection to the specified database.
     * If the database does not exist, it will be created.
     * A default username and password will be used for the connection.
     * Requires that the test has been decorated with
     * additionalDatabaseDecorator with the matching name.
     * The physical database name may differ.
     * @param logicalDatabaseName A logical database name as passed
     * to <code>additionalDatabaseDecorator</code> function.
     * @return connection to specified database.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-2087
    Connection openConnection(String logicalDatabaseName)
        throws SQLException
    {
        return connector.openConnection( getAndVetPhysicalDatabaseName( logicalDatabaseName ) );
    }
    private String  getAndVetPhysicalDatabaseName( String logicalDatabaseName )
        throws SQLException
    {
        String databaseName = getPhysicalDatabaseName( logicalDatabaseName );
        
        if ( usedDbNames.contains(databaseName) ) { return databaseName; }
        else
        {
            throw new SQLException("Database name \"" + logicalDatabaseName
                      + "\" is not in a list of used databases."
                      + "Use method TestConfiguration.additionalDatabaseDecorator first.");
        }
    }

    /**
     * Open connection to the specified database using the supplied username and password.
     * If the database does not exist, it will be created.
     * Requires that the test has been decorated with
     * additionalDatabaseDecorator with the matching name.
     * The physical database name may differ.
     * @param logicalDatabaseName A logical database name as passed
     * to <code>additionalDatabaseDecorator</code> function.
     * @return connection to specified database.
     */
    public  Connection openConnection( String logicalDatabaseName, String user, String password )
        throws SQLException
    {
        return connector.openConnection
            (
             getAndVetPhysicalDatabaseName( logicalDatabaseName ),
             user,
             password
             );
    }

    /**
     * Open connection to the specified database using the supplied username and password.
     * Treat the database name as a physical database name rather than as a logical name
     * which needs to be mapped.
     * If the database does not exist, it will be created.
     * Requires that the test has been decorated with
     * additionalDatabaseDecorator with the matching name.
     * @param physicalDatabaseName The real database name to use.
     * @param user name of user
     * @param password password of user
     * @param props extra properties to pass to the connection
     * @return connection to specified database.
     */
    public  Connection openPhysicalConnection( String physicalDatabaseName, String user, String password, Properties props )
        throws SQLException
    {
        return connector.openConnection
            (
             physicalDatabaseName,
             user,
             password,
             props
             );
    }

    /**
     * Shutdown the database for this configuration
     * assuming it is booted.
     *
     */
    public void shutdownDatabase()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2043
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-2036
//IC see: https://issues.apache.org/jira/browse/DERBY-2043
//IC see: https://issues.apache.org/jira/browse/DERBY-2047
            connector.shutDatabase();
            Assert.fail("Database failed to shut down");
        } catch (SQLException e) {
             BaseJDBCTestCase.assertSQLState("Database shutdown", "08006", e);
        }
    }
    
    /**
     * Shutdown the engine for this configuration
     * assuming it is booted.
     * This method can only be called when the engine
     * is running embedded in this JVM.
     *
     */
    public void shutdownEngine()
    {
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            connector.shutEngine(true);
            Assert.fail("Engine failed to shut down");
        } catch (SQLException e) {
             BaseJDBCTestCase.assertSQLState("Engine shutdown", "XJ015", e);
        }
    }

    /**
     * Shutdown the engine for this configuration
     * assuming it is booted.
     * This method can only be called when the engine
     * is running embedded in this JVM.
     *
     * @param deregisterDeriver if true, deregister the driver
     */
    public void shutdownEngine(boolean deregisterDeriver)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2043
//IC see: https://issues.apache.org/jira/browse/DERBY-2047
        try {
            connector.shutEngine(deregisterDeriver);
            Assert.fail("Engine failed to shut down");
        } catch (SQLException e) {
             BaseJDBCTestCase.assertSQLState("Engine shutdown", "XJ015", e);
        }
    }

    /** Get the login timeout from the connector */
    public  int getLoginTimeout() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6094
        return connector.getLoginTimeout();
    }

    /**
     * Set the login timeout for the connector.
     * @param seconds the login timeout in seconds
     * @throws SQLException if the timeout cannot be set
     */
    public void setLoginTimeout(int seconds) throws SQLException {
        connector.setLoginTimeout(seconds);
    }

    public void waitForShutdownComplete(String physicalDatabaseName) {
        String path = getDatabasePath(physicalDatabaseName);
        boolean lockfilepresent = true;
        int timeout = LOCKFILETIMEOUT; // 5 mins
        int totalsleep = 0;
        File lockfile = new File (path + File.separatorChar + "db.lck");
        File exlockfile = new File (path + File.separatorChar + "dbex.lck");
        while (lockfilepresent) {
            if (totalsleep >= timeout)
            {
                System.out.println("TestConfigruation.waitForShutdownComplete: " +
                        "been looping waiting for lock files to be deleted for at least 5 minutes, giving up");
                break;
            }
            if (lockfile.exists() || exlockfile.exists())
            {
                // TODO: is it interesting to know whether db.lck or dbex.lck or both is still present?
                try {
                    System.out.println("TestConfiguration.waitForShutdownComplete: " +
                            "db*.lck files not deleted after " + totalsleep + " ms.");
                    Thread.sleep(1000);
                    totalsleep=totalsleep+1000;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else
                lockfilepresent=false;
        }
    }
    
   /**
     * stops the Network server for this configuration.
     *
     */
    public void stopNetworkServer() {
//IC see: https://issues.apache.org/jira/browse/DERBY-2855
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-3151
            NetworkServerControlWrapper networkServer =
                    new NetworkServerControlWrapper();

            networkServer.shutdown();
            if (serverOutput != null) {
                serverOutput.close();
            }
        } catch(Exception e) {
            SQLException se = new SQLException("Error shutting down server");
            se.initCause(e);
        }
    }

   /**
     * starts the Networs server for this configuration.
     *
     */
    public void startNetworkServer() throws SQLException
    {
        Exception failException = null;
        try {
            
//IC see: https://issues.apache.org/jira/browse/DERBY-3151
            NetworkServerControlWrapper networkServer =
                    new NetworkServerControlWrapper();

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
 	    serverOutput = AccessController.doPrivileged(
                            new PrivilegedAction<FileOutputStream>() {
                public FileOutputStream run() {
                    File logs = new File("logs");
                    logs.mkdir();
                    File console = new File(logs, "serverConsoleOutput.log");
                    FileOutputStream fos = null;
                    try {
                        fos = new FileOutputStream(console.getPath(), true);
                    } catch (FileNotFoundException ex) {
                        ex.printStackTrace();
                    }
                    return fos;
                }
            });

            networkServer.start(new PrintWriter(serverOutput));

            // Wait for the network server to start
            boolean started = false;
            int retries = 10;         // Max retries = max seconds to wait

            while (!started && retries > 0) {
                try {
                    // Sleep 1 second and then ping the network server
                    Thread.sleep(SLEEP_TIME);
                    networkServer.ping();

                    // If ping does not throw an exception the server has started
                    started = true;
                } catch(Exception e) {		   
                    retries--;
                    failException = e;
                 }
                
             }

            // Check if we got a reply on ping
            if (!started) {
                 throw failException;
            }
        } catch (Exception e) {
            SQLException se = new SQLException("Error starting network  server");
            se.initCause(failException);
            throw se;
      }
    }
    /**
     * Set the verbosity, i.e., whether debug statements print.
     */
    public void	setVerbosity( boolean isChatty )	{ isVerbose = isChatty; }
    /**
     * Set JUnit test method tracing.
     */
    public void setTrace( boolean isChatty )    { doTrace = isChatty; }
    
    /**
     * Return verbose flag.
     *
     * @return verbose flag.
     */
    public boolean isVerbose() {
        return isVerbose;
    }

    /**
     * Private method printing debug information to standard out if debugging
     * is enabled.
     * <p>
     * <em>Note:</em> This method may direct output to a different location
     * than the println method in <tt>BaseJDBCTestCase</tt>.
     */
    private void println(CharSequence msg) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4700
        if (isVerbose) {
            System.out.println("DEBUG: {TC@" + hashCode() + "} " + msg);
        }
    }

    /**
     * Return JUnit test method trace flag.
     *
     * @return JUnit test method trace flag.
     */
    public boolean doTrace() {
//IC see: https://issues.apache.org/jira/browse/DERBY-2667
        return doTrace;
    }

    public boolean stopAfterFirstFail() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6401
        return stopAfterFirstFail;
    }
	/**
	 * <p>
	 * Return true if we classes are being loaded from jar files. For the time
	 * being, this simply tests that the JVMInfo class (common to the client and
	 * the server) comes out of a jar file.
	 * </p>
	 */
	public static boolean loadingFromJars()
	{
        return SecurityManagerSetup.isJars;
	}
    
    /**
     * Returns true if this JUnit test being run by the old harness.
     * Temp method to ease the switch over by allowing
     * suites to alter their behaviour based upon the
     * need to still run under the old harness.
     */
    //public static boolean runningInDerbyHarness()
    //{
    //    return runningInDerbyHarness;
    //}
    
    /**
     * Get a folder already created where a test can
     * write its failure information. The name of the folder,
     * relative to ${user.dir} is:
     * <BR>
     * <code>
     * fail/client/testclass/testname
     * <code>
     * <UL>
     * <LI> client - value of JDBCClient.getName() for the test's configuration
     * <LI> testclass - last element of the class name
     * <LI> testname - value of test.getName()
     *  </UL>
     */
    File getFailureFolder(TestCase test){
        
        StringBuffer sb = new StringBuffer();
      
        sb.append("fail");
        sb.append(File.separatorChar);
        sb.append(getJDBCClient().getName());
        sb.append(File.separatorChar);
        
        String className = test.getClass().getName();
        int lastDot = className.lastIndexOf('.');
        if (lastDot != -1)
            className = className.substring(lastDot+1, className.length());
        
        sb.append(className);
        sb.append(File.separatorChar);
        // DERBY-5620: Ensure valid file name.
//IC see: https://issues.apache.org/jira/browse/DERBY-5620
        char[] tmpName = test.getName().toCharArray();
        for (int i=0; i < tmpName.length; i++) {
            switch (tmpName[i]) {
                case '-':
                case '_':
                    continue;
                default:
                    if (!Character.isLetterOrDigit(tmpName[i])) {
                        tmpName[i] = '_';
                    }
            }
        }
        sb.append(tmpName);
        
        String base = sb.toString().intern();
        final File folder = new File(base);
        
        // Create the folder
        // TODO: Dump this configuration in some human readable format
        synchronized (base) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
                public Boolean run() {
                    if (folder.exists()) {
                        // do something
                    }
                    return folder.mkdirs();
                }
            });
        }

        return folder;
        
    }
    
    /*
     * Immutable data members in test configuration
     */
    
    /** The default database name for tests. */
    private final String defaultDbName;
    /** Holds the names of all other databases used in a test to perform a proper cleanup.
     * The <code>defaultDbName</code> is also contained here.  */
    private final ArrayList<String> usedDbNames = new ArrayList<String>();
    /** Contains the mapping of logical database names to physical database names. */
    private final HashMap<String, String> logicalDbMapping = new HashMap<String, String>();
    private final String url;
    private final String userName; 
    private final String userPassword; 
    private final int port;
    private final String hostName;
    private final JDBCClient jdbcClient;
    private final int jmxPort;
    private boolean isVerbose;
    private boolean doTrace;
    private boolean stopAfterFirstFail;
    private String ssl;

    /**
     * Extra connection attributes. Not for user name and password, use the
     * fields {@link #userName} and {@link #userPassword} for those attributes.
     */
    private Properties connectionAttributes;

    /**
     * Password token used by the builtin authentication decorators.
     * Default simple scheme is the password is a function
     * of the user and a password token. password token
     * is set by DatabasePropertyTestSetup.builtinAuthentication
     */
    private String passwordToken = "";
    
    /**
     * Indirection for obtaining connections based upon
     * this configuration.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-2215
    Connector connector;
    
    /*
     * SecurityManager related configuration.
     */
    
    /**
     * Install the default security manager setup,
     * for the current configuration.
     * @throws PrivilegedActionException 
     */
    boolean defaultSecurityManagerSetup() {
    	
    	// Testing with the DB2 client has not been performed
    	// under the security manager since it's not part
    	// of Derby so no real interest in tracking down issues.
    	if (jdbcClient.isDB2Client()) {
    		SecurityManagerSetup.noSecurityManager();
    		return false;
    	} else {
//IC see: https://issues.apache.org/jira/browse/DERBY-5631
            if (SecurityManagerSetup.NO_POLICY.equals(
//IC see: https://issues.apache.org/jira/browse/DERBY-2033
                    BaseTestCase.getSystemProperty("java.security.policy")))
            {
                // Explict setting of no security manager
                return false;
            }
    		SecurityManagerSetup.installSecurityManager();
    		return true;
    	}
    }
    
    
    /*
    ** BUILTIN password handling.
    */
    
    /**
     * Get the password that is a function of the user
     * name and the passed in token.
     */
    static final String getPassword(String user, String token)
    {
        return user.concat(token);
    }
    
    /**
     * Get the password that is a function of the user
     * name and the token for the current configuration.
     */
    public final String getPassword(String user)
    {
        return getPassword(user, passwordToken);
    }
    
    public final String getDatabasePath(String physicalDatabaseName) 
    {
        String dbName = physicalDatabaseName.replace('/', File.separatorChar);
        String dsh = BaseTestCase.getSystemProperty("derby.system.home");
        if (dsh == null) {
            Assert.fail("not implemented");
        } else {
            dbName = dsh + File.separator + dbName;
        }
        return dbName;
    }
}
