/*
 *
 * Derby - Class TestConfiguration
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
import java.security.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Hashtable;

import junit.extensions.TestSetup;
import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
public class TestConfiguration {
    /**
     * Default values for configurations
     */
    private final static String DEFAULT_DBNAME = "wombat";
    private final static String DEFAULT_DBNAME_SQL = "dbsqlauth";
    
    private final static String DEFAULT_USER_NAME = "APP";
    private final static String DEFAULT_USER_PASSWORD = "APP";
    public final static int    DEFAULT_PORT = 1527;
    private final static String DEFAULT_FRAMEWORK = "embedded";
    public final static String DEFAULT_HOSTNAME = "localhost";
    public final static String DEFAULT_SSL = "off";

    public  final   static  String  TEST_DBO = "TEST_DBO";
            
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
    private final static String KEY_TRACE = "derby.tests.trace";
    private final static String KEY_SSL = "ssl";
    
    /**
     * Simple count to provide a unique number for database
     * names.
     */
    private static int uniqueDB;


    /**
     * Default Derby test configuration object based
     * upon system properties set by the old harness.
     */
    private static final TestConfiguration DERBY_HARNESS_CONFIG = 
        new TestConfiguration(getSystemProperties());
    
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
    
    /**
     * Are we running in the harness, assume so if framework
     * was set so the 
     */
    private static final boolean runningInDerbyHarness;
    
    static {
        boolean assumeHarness = false;
        
        // In the harness if the default configuration according
        // to system properties is not embedded.
        if (!DERBY_HARNESS_CONFIG.getJDBCClient().isEmbedded())
            assumeHarness = true;
        
        // Assume harness if database name is not default
        if (!DERBY_HARNESS_CONFIG.getDefaultDatabaseName().equals(DEFAULT_DBNAME))
            assumeHarness = true;
        
        // Assume harness if user name is not default
        if (!DERBY_HARNESS_CONFIG.getUserName().equals(DEFAULT_USER_NAME))
            assumeHarness = true;
        
        // If derby.system.home set externally at startup assume
        // running in harness
        if (BaseTestCase.getSystemProperty("derby.system.home") != null)
            assumeHarness = true;

        DEFAULT_CONFIG = assumeHarness ? DERBY_HARNESS_CONFIG : JUNIT_CONFIG;
        runningInDerbyHarness = assumeHarness;
        
        if (!assumeHarness) {
            final   File dsh = new File("system");

            AccessController.doPrivileged
            (new java.security.PrivilegedAction(){
                public Object run(){
                    BaseTestCase.setSystemProperty("derby.system.home",
                                                   dsh.getAbsolutePath());
                    return null;
                }
            }
             );            
        }
     }
    
    /**
     * Current configuration is stored in a ThreadLocal to
     * allow the potential for multiple tests to be running
     * concurrently with different configurations.
     */
    private static final ThreadLocal CURRENT_CONFIG = new ThreadLocal() {
        protected Object initialValue() {
            return DEFAULT_CONFIG;
        }
    };
   
    /**
     * Get this Thread's current configuraiton for running
     * the tests.
     * @return TestConfiguration to use.
     */
    public static TestConfiguration getCurrent() {
        return (TestConfiguration) CURRENT_CONFIG.get();
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
         final TestSuite suite = new TestSuite(suiteName(testClass));
         
        if (cleanDB)
        {
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
        final TestSuite suite = new TestSuite(suiteName(testClass));

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
    private static String suiteName(Class testClass)
    {
        int lastDot = testClass.getName().lastIndexOf('.');
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
        return new TestSuite(testClass,
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
        TestSuite suite = new TestSuite(testClass,
                suiteName(testClass)+":client");
        return clientServerDecorator(suite);
    }
    /**
     * Create a suite for the passed test class that includes
     * all the default fixtures from the class, wrapped in
     * a derbyClientServerDecorator with a given port.
     * 
     */
    public static Test clientServerSuite(Class testClass, int port)
    {           
        TestSuite suite = new TestSuite(testClass,
                suiteName(testClass)+":client");
        return clientServerDecorator(suite,port);
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
        Test test = new NetworkServerTestSetup(suite, false);
            
        return defaultServerDecorator(test);
    }
    /**
     * Wrapper ot use the given port number.
     */
    public static Test clientServerDecorator(Test suite,int port)
    {
        Test test = new NetworkServerTestSetup(suite, false);
            
        return defaultServerDecorator(test,port);
    }
   
    /**
     * Decorate a test to use suite's default host and port.
     */
    public static Test defaultServerDecorator(Test test)
    {
        // Need to have network server and client and not
        // running in J2ME (JSR169).
        if (!(Derby.hasClient() && Derby.hasServer())
                || JDBC.vmSupportsJSR169())
            return new TestSuite("empty: no network server support");

        //
        // This looks bogus to me. Shouldn't this get the hostname and port
        // which are specific to this test run (perhaps overridden on the
        // command line)?
        //
        return new ServerSetup(test, DEFAULT_HOSTNAME, DEFAULT_PORT);
    }
    /**
     * Decorate a test to use suite's default host and given port.
     */
    public static Test defaultServerDecorator(Test test, int port)
    {
        // Need to have network server and client and not
        // running in J2ME (JSR169).
        if (!(Derby.hasClient() && Derby.hasServer())
                || JDBC.vmSupportsJSR169())
            return new TestSuite("empty: no network server support");

        //
        // This looks bogus to me. Shouldn't this get the hostname and port
        // which are specific to this test run (perhaps overridden on the
        // command line)?
        //
        return new ServerSetup(test, DEFAULT_HOSTNAME, port);
    }

    /**
     * Generate the unique database name for single use.
     */
    private static synchronized String generateUniqueDatabaseName()
    {
        // Forward slash is ok, Derby treats database names
        // as URLs and translates forward slash to the local
        // separator.
        String dbName = "singleUse/oneuse";
        dbName = dbName.concat(Integer.toHexString(uniqueDB++));
        return dbName;
    }

 
    /**
     * Decorate a test to use a new database that is created upon the
     * first connection request to the database and shutdown & deleted at
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

        return new DatabaseChangeSetup(new DropDatabaseSetup(test, dbName), dbName, dbName, true);
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
            new DropDatabaseSetup(test, dbName)
            {
                protected void tearDown() throws Exception {
                    // test responsible for shutdown
                    removeDatabase();
                }
            },
            dbName, dbName, true);
    }

    /**
     * Decorate a test to use a new database that is created upon the
     * first connection request to the database and shutdown & deleted at
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
    public static TestSetup additionalDatabaseDecorator(Test test, String logicalDbName)
    {
        return new DatabaseChangeSetup(new DropDatabaseSetup(test, logicalDbName),
                                       logicalDbName,
                                       generateUniqueDatabaseName(),
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
        Test setSQLAuthMode = new DatabasePropertyTestSetup(test,
                sqlAuth, true) {
            protected void tearDown() {
            }
        };
        
        return changeUserDecorator(
            new DatabaseChangeSetup(setSQLAuthMode, DEFAULT_DBNAME_SQL, DEFAULT_DBNAME_SQL, true),
            TEST_DBO, "dummy"); // DRDA doesn't like empty pw
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
        // Set the SQL authorization mode as a database property
        // with a modified DatabasePropertyTestSetup that does not
        // reset it.
        final Properties sqlAuth = new Properties();
        sqlAuth.setProperty("derby.database.sqlAuthorization", "true");
        Test setSQLAuthMode = new DatabasePropertyTestSetup(test,
                                                            sqlAuth, true) {
                protected void tearDown() { }
            };


        setSQLAuthMode = new DatabaseChangeSetup(
            new DropDatabaseSetup(setSQLAuthMode, DEFAULT_DBNAME_SQL) {
                protected void tearDown() throws Exception {
                    // test responsible for shutdown
                    removeDatabase();
                }
            },
            DEFAULT_DBNAME_SQL, DEFAULT_DBNAME_SQL, true);

        return changeUserDecorator(setSQLAuthMode,
                                   TEST_DBO,
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
        usersWithDBO[0] = TEST_DBO;
        System.arraycopy(users, 0, usersWithDBO, 1, users.length);
        return sqlAuthorizationDecorator(
            DatabasePropertyTestSetup.builtinAuthentication(test, 
                    usersWithDBO, passwordToken));
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
    public static TestSetup connectionXADecorator(Test test)
    {
        return new ConnectorSetup(test,
                "org.apache.derbyTesting.junit.XADataSourceConnector");
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
        return new ChangeSSLSetup(test, ssl);
    }   
    
    /**
     * Default embedded configuration
     *
     */
    private TestConfiguration() {
        this.defaultDbName = DEFAULT_DBNAME;
        usedDbNames.add(DEFAULT_DBNAME);
        logicalDbMapping.put(DEFAULT_DBNAME, DEFAULT_DBNAME);
        this.userName = DEFAULT_USER_NAME;
        this.userPassword = DEFAULT_USER_PASSWORD;
        this.hostName = null;
        this.port = -1;
        this.isVerbose = Boolean.valueOf(
            getSystemProperties().getProperty(KEY_VERBOSE)).
            booleanValue();
        this.doTrace = Boolean.valueOf(
            getSystemProperties().getProperty(KEY_TRACE)).
            booleanValue();
        
        this.jdbcClient = JDBCClient.getDefaultEmbedded();
        this.ssl = null;
        url = createJDBCUrlWithDatabaseName(defaultDbName);
        initConnector(null);
 
    }

    /**
     * Obtain a new configuration identical to the passed one.
     */
    TestConfiguration(TestConfiguration copy)
    {
        this.defaultDbName = copy.defaultDbName;
        this.usedDbNames.addAll(copy.usedDbNames);
        logicalDbMapping.putAll(copy.logicalDbMapping);
        this.userName = copy.userName;
        this.userPassword = copy.userPassword;

        this.isVerbose = copy.isVerbose;
        this.doTrace = copy.doTrace;
        this.port = copy.port;
        
        this.jdbcClient = copy.jdbcClient;
        this.hostName = copy.hostName;
        
        this.ssl = copy.ssl;

        this.url = copy.url;
        initConnector(copy.connector);
    }

    TestConfiguration(TestConfiguration copy, JDBCClient client,
            String hostName, int port)
    {
        this.defaultDbName = copy.defaultDbName;
        this.usedDbNames.addAll(copy.usedDbNames);        
        logicalDbMapping.putAll(copy.logicalDbMapping);
        this.userName = copy.userName;
        this.userPassword = copy.userPassword;

        this.isVerbose = copy.isVerbose;
        this.doTrace = copy.doTrace;
        this.port = port;
        
        this.jdbcClient = client;
        this.hostName = hostName;

        this.ssl = copy.ssl;
        
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
        this.defaultDbName = copy.defaultDbName;
        this.usedDbNames.addAll(copy.usedDbNames);
        logicalDbMapping.putAll(copy.logicalDbMapping);
        this.userName = user;
        this.userPassword = password;
        this.passwordToken = passwordToken == null ?
                copy.passwordToken : passwordToken;

        this.isVerbose = copy.isVerbose;
        this.doTrace = copy.doTrace;
        this.port = copy.port;
        
        this.jdbcClient = copy.jdbcClient;
        this.hostName = copy.hostName;

        this.ssl = copy.ssl;
        
        this.url = copy.url;
        initConnector(copy.connector);
    }

    /**
     * Obtain a new configuration identical to the passed in
     * one except for the default user and password.
     * @param copy Configuration to copy.
     * @param ssl New default ssl mode
     */
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
    TestConfiguration(TestConfiguration copy, String logicalDbName,
                      String dbName, boolean defaultDb)
    {
        this.usedDbNames.addAll(copy.usedDbNames);
        this.usedDbNames.add(dbName);
        logicalDbMapping.putAll(copy.logicalDbMapping);

        // Can not use the same logical name for different database.
        // If this assert will make failures it might be safely removed
        // since having more physical databases accessible throught the same
        // logical database name will access only the last physical database
        Assert.assertTrue(logicalDbMapping.put(logicalDbName, dbName) == null);

        if (defaultDb) {
            this.defaultDbName = dbName;
        } else {
            this.defaultDbName = copy.defaultDbName;
        }
        
        this.userName = copy.userName;
        this.userPassword = copy.userPassword;

        this.isVerbose = copy.isVerbose;
        this.doTrace = copy.doTrace;
        this.port = copy.port;
        
        this.jdbcClient = copy.jdbcClient;
        this.hostName = copy.hostName;

        this.ssl = copy.ssl;
        
        this.url = createJDBCUrlWithDatabaseName(this.defaultDbName);
        initConnector(copy.connector);
    }
  
    /**
     * This constructor creates a TestConfiguration from a Properties object.
     *
     * @throws NumberFormatException if the port specification is not an integer.
     */
    private TestConfiguration(Properties props) 
        throws NumberFormatException {

        defaultDbName = props.getProperty(KEY_DBNAME, DEFAULT_DBNAME);
        usedDbNames.add(defaultDbName);
        logicalDbMapping.put(defaultDbName, defaultDbName);
        userName = props.getProperty(KEY_USER_NAME, DEFAULT_USER_NAME);
        userPassword = props.getProperty(KEY_USER_PASSWORD, 
                                         DEFAULT_USER_PASSWORD);
        hostName = props.getProperty(KEY_HOSTNAME, DEFAULT_HOSTNAME);
        isVerbose = Boolean.valueOf(props.getProperty(KEY_VERBOSE)).booleanValue();
        doTrace =  Boolean.valueOf(props.getProperty(KEY_TRACE)).booleanValue();
        String portStr = props.getProperty(KEY_PORT);
        if (portStr != null) {
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException nfe) {
                // We lose stacktrace here, but it is not important. 
                throw new NumberFormatException(
                        "Port number must be an integer. Value: " + portStr); 
            }
        } else {
            port = DEFAULT_PORT;
        }

        ssl = props.getProperty(KEY_SSL);
        
        String framework = props.getProperty(KEY_FRAMEWORK, DEFAULT_FRAMEWORK);
        
        if ("DerbyNetClient".equals(framework)) {
            jdbcClient = JDBCClient.DERBYNETCLIENT;
        } else if ("DerbyNet".equals(framework)) {
            jdbcClient = JDBCClient.DB2CLIENT;
        } else {
            jdbcClient = JDBCClient.getDefaultEmbedded();
        }
        url = createJDBCUrlWithDatabaseName(defaultDbName);
        initConnector(null);
    }

    /**
     * Get the system properties in a privileged block.
     *
     * @return the system properties.
     */
    private static final Properties getSystemProperties() {
        // Fetch system properties in a privileged block.
        Properties sysProps = (Properties)AccessController.doPrivileged(
                new PrivilegedAction() {
                    public Object run() {
                        return System.getProperties();
                    }
                });
        return sysProps;
    }

    /**
     * Create JDBC connection url, including the name of the database.
     *
     * @return JDBC connection url, without attributes.
     */
    private String createJDBCUrlWithDatabaseName(String name) {
        if (JDBC.vmSupportsJDBC2())
        {
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
                connector = (Connector) Class.forName(
                  oldConnector.getClass().getName()).newInstance();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }            
        }
        else if (JDBC.vmSupportsJDBC2())
        {
            try {
                connector = (Connector) Class.forName(
                  "org.apache.derbyTesting.junit.DriverManagerConnector").newInstance();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            
        } else {
            connector = new DataSourceConnector();
        }
        connector.setConfiguration(this);
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
     * Return the jdbc url for connecting to the default database.
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
    String getPhysicalDatabaseName(String logicalName) {
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
     * Return the host name for the network server.
     *
     * @return host name.
     */
    public String getHostName() {
        return hostName;
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
     * Get ssl mode for network server
     * 
     * @return ssl mode
     */
    public String getSsl() {
        return ssl;
    }

    
    /**
     * Open connection to the default database.
     * If the database does not exist, it will be created.
     * A default username and password will be used for the connection.
     *
     * @return connection to default database.
     */
    Connection openDefaultConnection()
        throws SQLException {
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
    Connection openConnection(String logicalDatabaseName)
        throws SQLException {
        String databaseName = getPhysicalDatabaseName(logicalDatabaseName);
        if (usedDbNames.contains(databaseName))
            return connector.openConnection(databaseName);
        else
            throw new SQLException("Database name \"" + logicalDatabaseName
                      + "\" is not in a list of used databases."
                      + "Use method TestConfiguration.additionalDatabaseDecorator first.");
    }

    /**
     * Shutdown the database for this configuration
     * assuming it is booted.
     *
     */
    public void shutdownDatabase()
    {
        try {
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
            connector.shutEngine();
            Assert.fail("Engine failed to shut down");
        } catch (SQLException e) {
             BaseJDBCTestCase.assertSQLState("Engine shutdown", "XJ015", e);
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
     * Return JUnit test method trace flag.
     *
     * @return JUnit test method trace flag.
     */
    public boolean doTrace() {
        return doTrace;
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
    public static boolean runningInDerbyHarness()
    {
        return runningInDerbyHarness;
    }
    
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
        sb.append(test.getName());
        
        String base = sb.toString().intern();
        final File folder = new File(base);
        
        // Create the folder
        // TODO: Dump this configuration in some human readable format
        synchronized (base) {
            
            AccessController.doPrivileged
            (new java.security.PrivilegedAction(){
                public Object run(){
                    if (folder.exists()) {
                        // do something
                    }            
                    return new Boolean(folder.mkdirs());
                }
            }
             );            
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
    private final ArrayList usedDbNames = new ArrayList();
    /** Contains the mapping of logical database names to physical database names. */
    private final Hashtable logicalDbMapping = new Hashtable();
    private final String url;
    private final String userName; 
    private final String userPassword; 
    private final int port;
    private final String hostName;
    private final JDBCClient jdbcClient;
    private boolean isVerbose;
    private boolean doTrace;
    private String ssl;
    
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
            if ("<NONE>".equals(
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
    final String getPassword(String user)
    {
        return getPassword(user, passwordToken);
    }
}
