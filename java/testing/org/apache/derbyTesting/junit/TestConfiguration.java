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
import java.lang.reflect.Method;
import java.security.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.TestDataSourceFactory;

/**
 * Class which holds information about the configuration of a Test.
 */
public class TestConfiguration {
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
        if (!DERBY_HARNESS_CONFIG.getDatabaseName().equals(DEFAULT_DBNAME))
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
            File dsh = new File("system");

            BaseTestCase.setSystemProperty("derby.system.home",
                    dsh.getAbsolutePath());
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
     * Return a Test suite that contains all the text fixtures
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
        final TestSuite suite = new TestSuite(suiteName(testClass));
        
        suite.addTest(new CleanDatabaseTestSetup(embeddedSuite(testClass)));            
        suite.addTest(new CleanDatabaseTestSetup(clientServerSuite(testClass)));
 
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
     * all the default fixtures from the class, wrapped
     * in a single CleanDatabaseTestSetup.
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
     * @param withCleanDB True if the 
      */
    public static Test clientServerSuite(Class testClass)
    {           
        TestSuite suite = new TestSuite(testClass,
                suiteName(testClass)+":client");
        return clientServerDecorator(suite);
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
     * @param tests
     * @return
     */
    public static Test clientServerDecorator(Test suite)
    {
        if (JDBC.vmSupportsJSR169())
            return new TestSuite();
            
        TestConfiguration config = TestConfiguration.getCurrent();
        
        TestConfiguration derbyClientConfig =
            new TestConfiguration(config, JDBCClient.DERBYNETCLIENT,
                    DEFAULT_HOSTNAME, DEFAULT_PORT);
                   
        Test test = new NetworkServerTestSetup(suite, false);
            
        return new ChangeConfigurationSetup(derbyClientConfig, test);

    }
    
    /**
     * Decorate a test to use a new database that is created upon the
     * first connection request to the database and shutdown & deleted at
     * tearDown. The configuration differs only from the current configuration
     * by the database name.
     * This decorator expects the database file to be local so it
     * can be removed.
     * @param test Test to be decorated
     * @return decorated test.
     */
    public static Test singleUseDatabaseDecorator(Test test)
    {
        TestConfiguration config = TestConfiguration.getCurrent();

        // Forward slash is ok, Derby treats database names
        // as URLs and translates forward slash to the local
        // separator.
        String dbName = "singleUse/oneuse";
        // Synchronize on the literal name which will be invariant
        // since it is interned.
        synchronized (dbName) {
            dbName = dbName.concat(Integer.toHexString(uniqueDB++));
        }
        TestConfiguration newDBconfig = 
            new TestConfiguration(config, dbName);
        return new ChangeConfigurationSetup(newDBconfig,
                new DropDatabaseSetup(test));
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
     * Tests can use this in conjunction with
     * DatabasePropertyTestSetup.builtinAuthentication
     * to set up BUILTIN authentication and changeUserDecorator
     * to switch users.
     * 
     * @param test Test to be decorated
     * @return decorated test.
     * 
     * @see DatabasePropertyTestSetup#builtinAuthentication(Test, String[], String)
     */
    public static Test sqlAuthorizationDecorator(Test test)
    {
        TestConfiguration config = TestConfiguration.getCurrent();
        TestConfiguration newDBconfig = 
            new TestConfiguration(config, DEFAULT_DBNAME_SQL);
        
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

        return new ChangeConfigurationSetup(newDBconfig, setSQLAuthMode);
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
        // Copy the current configuration by creating one
        // with the same database name
        TestConfiguration config = TestConfiguration.getCurrent();
        TestConfiguration newConfig = 
            new TestConfiguration(config, config.getDatabaseName());
        
        try {
            newConfig.connector = (Connector) Class.forName(
              "org.apache.derbyTesting.junit.XADataSourceConnector").newInstance();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        
        newConfig.connector.setConfiguration(newConfig);
       
        return new ChangeConfigurationSetup(newConfig, test);
    }
    
    /**
     * Default embedded configuration
     *
     */
    private TestConfiguration() {
        this.dbName = DEFAULT_DBNAME;
        this.userName = DEFAULT_USER_NAME;
        this.userPassword = DEFAULT_USER_PASSWORD;
        this.hostName = null;
        this.port = -1;
        
        this.jdbcClient = JDBCClient.getDefaultEmbedded();
        url = createJDBCUrlWithDatabaseName(dbName);
        initConnector();
 
    }

    private TestConfiguration(TestConfiguration copy, JDBCClient client,
            String hostName, int port)
    {
        this.dbName = copy.dbName;
        this.userName = copy.userName;
        this.userPassword = copy.userPassword;

        this.isVerbose = copy.isVerbose;
        this.port = port;
        
        this.jdbcClient = client;
        this.hostName = hostName;
        
        this.url = createJDBCUrlWithDatabaseName(dbName);
        initConnector();
    }

    
    /**
     * Obtain a new configuration identical to the passed in
     * one except for the default user and password.
     * @param copy Configuration to copy.
     * @param user New default user
     * @param password New default password.
     */
    TestConfiguration(TestConfiguration copy, String user, String password)
    {
        this.dbName = copy.dbName;
        this.userName = user;
        this.userPassword = password;

        this.isVerbose = copy.isVerbose;
        this.port = copy.port;
        
        this.jdbcClient = copy.jdbcClient;
        this.hostName = copy.hostName;
        
        this.url = copy.url;
        initConnector();
    }
    /**
     * Obtain a new configuration identical to the passed in
     * one except for the database name.
     * @param copy Configuration to copy.
     * @param dbName New database name
      */
    TestConfiguration(TestConfiguration copy, String dbName)
    {
        this.dbName = dbName;
        this.userName = copy.userName;
        this.userPassword = copy.userPassword;

        this.isVerbose = copy.isVerbose;
        this.port = copy.port;
        
        this.jdbcClient = copy.jdbcClient;
        this.hostName = copy.hostName;
        
        this.url = createJDBCUrlWithDatabaseName(dbName);
        initConnector();
    }
  
    /**
     * This constructor creates a TestConfiguration from a Properties object.
     *
     * @throws NumberFormatException if the port specification is not an integer.
     */
    private TestConfiguration(Properties props) 
        throws NumberFormatException {

        dbName = props.getProperty(KEY_DBNAME, DEFAULT_DBNAME);
        userName = props.getProperty(KEY_USER_NAME, DEFAULT_USER_NAME);
        userPassword = props.getProperty(KEY_USER_PASSWORD, 
                                         DEFAULT_USER_PASSWORD);
        hostName = props.getProperty(KEY_HOSTNAME, DEFAULT_HOSTNAME);
        isVerbose = Boolean.valueOf(props.getProperty(KEY_VERBOSE)).booleanValue();
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
        
        String framework = props.getProperty(KEY_FRAMEWORK, DEFAULT_FRAMEWORK);
        
        if ("DerbyNetClient".equals(framework)) {
            jdbcClient = JDBCClient.DERBYNETCLIENT;
        } else if ("DerbyNet".equals(framework)) {
            jdbcClient = JDBCClient.DB2CLIENT;
        } else {
            jdbcClient = JDBCClient.getDefaultEmbedded();
        }
        url = createJDBCUrlWithDatabaseName(dbName);
        initConnector();
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
    private void initConnector()
    {
        if (JDBC.vmSupportsJDBC2())
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
    public String getDatabaseName() {
        return dbName;
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
     * Open a connection to a database.
     * If the database does not exist, it will be created.
     * A default username and password will be used for the connection.
     *
     * @param databaseName database to connect to
     *
     * @return connection to database.
     */
    Connection openConnection (String databaseName) throws SQLException {
        return getConnection(databaseName, "create=true");
    }
    
    /**
     * Get a connection to a database using the specified connection 
     * attributes.
     * 
     * @param databaseName database to connect to
     * @param connAttrs connection attributes
     * @return connection to database.
     * @throws SQLException
     */
    private Connection getConnection (String databaseName, String connAttrs) 
    	throws SQLException {
        Connection con = null;
        JDBCClient client =getJDBCClient();
        if (JDBC.vmSupportsJDBC2()) {            
            loadJDBCDriver(client.getJDBCDriverName());
            {
                con = DriverManager.getConnection(
                        getJDBCUrl(databaseName) + ";" + connAttrs,
                        getUserName(),
                        getUserPassword());
            }
        } else {
            //Use DataSource for JSR169
            Properties attrs = getDataSourcePropertiesForDatabase(databaseName, connAttrs);
            con = TestDataSourceFactory.getDataSource(attrs).getConnection();
        }
        return con;
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
     * Return verbose flag.
     *
     * @return verbose flag.
     */
    public boolean isVerbose() {
        return isVerbose;
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
     * Is this JUnit test being run by the old harness.
     * Temp method to ease the switch over by allowing
     * suites to alter their behaviour based upon the
     * need to still run under the old harness.
     * @return
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
    
    /**
     * Immutable data members in test configuration
     */
    private final String dbName;
    private final String url;
    private final String userName; 
    private final String userPassword; 
    private final int port;
    private final String hostName;
    private final JDBCClient jdbcClient;
    private boolean isVerbose;
    
    /**
     * Indirection for obtaining connections based upon
     * this configuration.
     */
    private Connector connector;
    

    /**
     * Generate properties which can be set on a
     * <code>DataSource</code> in order to connect to the default
     * database. If the database does not exist, it will be created.
     *
     * @return a <code>Properties</code> object containing server
     * name, port number, database name and other attributes needed to
     * connect to the default database
     */
    public static Properties getDefaultDataSourceProperties() {
        return getDataSourcePropertiesForDatabase(
                getCurrent().getDatabaseName(), "create=true");
    }
    
    /**
     * Generate properties which can be set on a <code>DataSource</code> 
     * in order to connect to a database using the specified connection 
     * attributes.
     * 
     * @param databaseName database to connect to
     * @param connAttrs connection attributes
     * @return
     */
    private static Properties getDataSourcePropertiesForDatabase
    	(String databaseName, String connAttrs) 
    {
        Properties attrs = new Properties();
        if (!getCurrent().getJDBCClient().isEmbedded()) {
            attrs.setProperty("serverName", getCurrent().getHostName());
            attrs.setProperty("portNumber", Integer.toString(getCurrent().getPort()));
        }
        attrs.setProperty("databaseName", databaseName);
        attrs.setProperty("connectionAttributes", connAttrs);
        return attrs;
    }

    /**
     * Load the specified JDBC driver
     *
     * @param driverClass name of the JDBC driver class.
     * @throws SQLException if loading the driver fails.
     */
    private static void loadJDBCDriver(String driverClass) 
        throws SQLException {
        try {
            Class.forName(driverClass).newInstance();
        } catch (ClassNotFoundException cnfe) {
            throw new SQLException("Failed to load JDBC driver '" + 
                                    driverClass + "': " + cnfe.getMessage());
        } catch (IllegalAccessException iae) {
            throw new SQLException("Failed to load JDBC driver '" +
                                    driverClass + "': " + iae.getMessage());
        } catch (InstantiationException ie) {
            throw new SQLException("Failed to load JDBC driver '" +
                                    driverClass + "': " + ie.getMessage());
        }
    }
    
    /*
     * SecurityManager related configuration.
     */
    
    /**
     * Install the default security manager setup,
     * for the current configuration.
     * @throws PrivilegedActionException 
     */
    boolean defaultSecurityManagerSetup() throws PrivilegedActionException {
    	
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
    
        
}
