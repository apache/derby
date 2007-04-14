/*

Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DriverTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.File;
import java.security.AccessController;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
*
* This test tests java.sql.Driver methods.
* Right now it just tests acceptsURL and some attributes  
* Not tested in this test: 
*   - Tests for getPropertyInfo
*   - tests for connection attributes
*/

public class DriverTest extends BaseJDBCTestCase {

    // DERBY-618 - Database name with spaces
    private static String DB_NAME_WITH_SPACES = "db name with spaces";
    
    /**
     * Set of additional databases for tests that
     * require a one-off database. The additional
     * database decorator wraps all the tests and phases.
     * They are only created if a test opens a
     * connection against them. In hard upgrade the test
     * must explictly upgrade the database.
     * The databases are shutdown at the end of each phase.
     */
    static final String[] ADDITIONAL_DBS = {
        DB_NAME_WITH_SPACES,
        "testcreatedb1", 
        "testcreatedb2",
        "'wombat'"
    };
    
    public DriverTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        // java.sql.Driver is not supported with JSR169, so return empty suite
        if (JDBC.vmSupportsJSR169())
        {
            return new TestSuite(
                "DriverTest tests java.sql.Driver, not supported with JSR169");
        }
        TestSuite suite = new TestSuite("DriverTest");
        suite.addTest(baseSuite("DriverTest:embedded"));
            suite.addTest(TestConfiguration.clientServerDecorator(
                baseSuite("DriverTest:client")));
        return suite;
    }
    
    public static Test baseSuite(String name) {
        
        TestSuite suite = new TestSuite("DriverTest");
        setBaseProps(suite, new DriverTest("testDriverCompliantVersion"));
        setBaseProps(suite, new DriverTest("testAcceptsURL"));
        setBaseProps(suite, new DriverTest("testEmbeddedAttributes"));
        setBaseProps(suite, new DriverTest("testClientAttributes"));
        setBaseProps(suite, new DriverTest("testClientURL"));
        setBaseProps(suite, new DriverTest("testDbNameWithSpaces"));
        
        return suite;
    }
    
    private static void setBaseProps(TestSuite suite, Test test) 
    {
        Properties dbprops = new Properties();

        // Use DatabasePropertyTestSetup to add some settings.
        // DatabasePropertyTestSetup uses SYSCS_SET_DATABASE_PROPERTY
        // so users are added at database level.
        // Note, that authentication is not switched on.
        dbprops.setProperty("derby.infolog.append", "true");
        dbprops.setProperty("derby.debug.true", "AuthenticationTrace");
        dbprops.setProperty("derby.user.APP", "xxxx");
        dbprops.setProperty("derby.user.testuser", "testpass");
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        dbprops.setProperty("derby.database.users." + dbName, "testuser,APP");
        test = new DatabasePropertyTestSetup (test, dbprops, true);
        suite.addTest(test);
    }

    public void tearDown() throws Exception {
        // attempt to get rid of any left-over trace files
        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                for (int i=0 ; i < 2 ; i++)
                {   
                    String traceFileName = "trace" + (i+1) + ".out";
                    File traceFile = new File(traceFileName);
                    if (traceFile.exists())
                    {
                        // if it exists, attempt to get rid of it
                        traceFile.delete();
                    }
                } 
                removeDatabases();
                return null;
            }
            
            // attempt to get rid of any databases. 
            void removeDatabases()
            {
                for ( int i=0 ; i<ADDITIONAL_DBS.length ; i++)
                {
                    String dbName = ADDITIONAL_DBS[i];
                    //TestConfiguration config = TestConfiguration.getCurrent();
                    dbName = dbName.replace('/', File.separatorChar);
                    String dsh = BaseTestCase.getSystemProperty("derby.system.home");
                    if (dsh == null) {
                        fail("not implemented");
                    } else {
                        dbName = dsh + File.separator + dbName;
                    }
                    removeDirectory(dbName);
                }
            }

            void removeDirectory(String path)
            {
                final File dir = new File(path);
                removeDir(dir);
            }

            private void removeDir(File dir) {

                // Check if anything to do!
                // Database may not have been created.
                if (!dir.exists())
                    return;

                String[] list = dir.list();

                // Some JVMs return null for File.list() when the
                // directory is empty.
                if (list != null) {
                    for (int i = 0; i < list.length; i++) {
                        File entry = new File(dir, list[i]);

                        if (entry.isDirectory()) {
                            removeDir(entry);
                        } else {
                            entry.delete();
                            //assertTrue(entry.getPath(), entry.delete());
                        }
                    }
                }
                dir.delete();
                //assertTrue(dir.getPath(), dir.delete());
            }
        });
        super.tearDown();
    }
    
    /**
     * Load the driver and check java.sql.Driver.jdbcCompliant() and
     * driver.get*Version
     * @throws Exception
     */
    public static void testDriverCompliantVersion() throws Exception 
    {   
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);

        loadDriver();
        String defaultdburl = url + ";create=true";
        
        // Test that we loaded the right driver by making a connection
        Driver driver = DriverManager.getDriver(defaultdburl);
        Properties props = new Properties();
        props.put("user", "testuser");
        props.put("password", "testpass");
        Connection conn = DriverManager.getConnection(defaultdburl, props);
        // Driver should be jdbc compliant.
        assertTrue(driver.jdbcCompliant());

        // compare driver.get*Version() with DatabaseMetadata.getDriver*Version.
        DatabaseMetaData dbmd = conn.getMetaData();

        assertEquals(dbmd.getDriverMajorVersion(), driver.getMajorVersion());
        assertEquals(dbmd.getDriverMinorVersion(), driver.getMinorVersion());
        
        conn.close();
    }
    
    /**
     * Check that drivers accept the correct urls and reject those for other supported drivers.
     * 
     * @throws SQLException, Exception
     */
    public static void testAcceptsURL() throws SQLException, Exception {
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        String orgurl = TestConfiguration.getCurrent().getJDBCUrl(dbName);

        loadDriver();
        String defaultdburl = orgurl + ";create=true";
        
        // Test that we loaded the right driver by making a connection
        Driver driver = DriverManager.getDriver(defaultdburl);

        int  frameworkOffset;
        int EMBEDDED_OFFSET = 0;
        int DERBYNETCLIENT_OFFSET = 1;
        if (usingDerbyNetClient())
            frameworkOffset = DERBYNETCLIENT_OFFSET;
        else // assume (usingEmbedded())
            frameworkOffset = EMBEDDED_OFFSET;
        
        // URLS to check.  New urls need to also be added to the acceptsUrl table
        String EMBEDDED_URL = "jdbc:derby:";
        String INVALID_URL = "jdbc:db2j:";
        String hostName = TestConfiguration.getCurrent().getHostName();
        int port = TestConfiguration.getCurrent().getPort();
        String CLIENT_URL = 
            "jdbc:derby://"+hostName+":"+port+"/"+dbName+";create=true";
        
        String[] urls = new String[]
        {
            EMBEDDED_URL,
            CLIENT_URL,
            INVALID_URL,
        };

        // Table that shows whether tested urls should return true for 
        // acceptsURL under the given framework
        // The acceptsURLTable uses  the frameworkOffset column int he table 
        // to check for valid results for each framework
        boolean[][] acceptsURLTable = new boolean[][]
        {
        // Framework/url      EMBEDDED     DERBYNETCLIENT 
        /* EMBEDDED_URL*/  {   true      ,  false        },
        /* CLIENT_URL  */  {   false     ,  true         },     
        /* INVALID_URL */  {   false     ,  false        } 
        };

        for (int u = 0; u < urls.length;u++)
        {
            String url = urls[u];
            boolean expectedAcceptance = acceptsURLTable[u][frameworkOffset];
            boolean actualAcceptance = driver.acceptsURL(url);
            assertEquals(expectedAcceptance, actualAcceptance);
        }
    }
    
    /**
     * Tests that embedded attributes can be specified in either url or info 
     * argument to connect
     * DERBY-530. Only valid for embedded driver and client. 
     */
    public static void testEmbeddedAttributes() throws SQLException
    {
        // JCC can't take embedded attributes in info or as normal url 
        // attributes, so not tested here.
        if (usingDerbyNet())
            return;
        
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        String protocol = 
            TestConfiguration.getCurrent().getJDBCClient().getUrlBase();
        if (usingDerbyNetClient())
            protocol = protocol + TestConfiguration.getCurrent().getHostName()
            + ":" + TestConfiguration.getCurrent().getPort() + "/";
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        
        Properties info = new Properties();
        // create attribute as property
        info.setProperty("create","true");
        // networkserver / DerbyNetClient tags on create to url
        if (usingEmbedded())
            assertConnect(true, protocol + "testcreatedb1", info);
        else
            assertConnect(false, protocol + "testcreatedb1", info);
        
        // create attribute in url
        if (usingEmbedded())
            assertConnect(false, protocol + "testcreatedb2;create=true", null);
        else 
            assertConnect(true, protocol + "testcreatedb2;create=true", null);
        
        // user/password in properties
        info.clear();
        info.setProperty("user","APP");
        info.setProperty("password", "xxxx");
        if (usingEmbedded())
            assertConnect(true, url, info);
        else 
            assertConnect(false, url, info);
        
        // user/password  in url
        assertConnect(false, 
            url + ";user=testuser;password=testpass", null);
        
        // user in url, password in property
        info.clear();
        info.setProperty("password","testpass");
        assertConnect(false, url + ";user=testusr",info);

        // different users in url and in properties. URL is the winner
        info.clear();
        info.setProperty("user","APP");
        info.setProperty("password","xxxx");
        assertConnect(false, 
            url + ";user=testuser;password=testpass", null);
        
        // shutdown with properties
        info.clear();
        info.setProperty("shutdown","true");                
        try {
            assertConnect(false, protocol + "testcreatedb1", info);
        } catch (SQLException se)
        {
            assertSQLState("08006", se);
        }
        
        // shutdown using url
        try {
            assertConnect(
                false, protocol + "testcreatedb2;shutdown=true", null);
        } catch (SQLException se)
        {
            assertSQLState("08006", se);
        }
    }
        
    /**
     * Tests that client side attributes cann be specified in either url or
     * as info argument to connect.
     * DERBY-530. 
     */
    public static void testClientAttributes() throws SQLException
    {
        if (!usingDerbyNetClient())
            return;
        
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);

        Properties info = new Properties();
        String traceFile = "trace1.out";
        
        // traceFile attribute in url
        assertConnect(true, url + ";traceFile=" + traceFile, info);
        
        traceFile = "trace2.out";
        // traceFile attribute in property
        info.setProperty("traceFile",traceFile);
        assertConnect(false, url, info);
        assertTraceFilesExist();
        shutdownDB(url + ";shutdown=true", null);

    }

    /**
     * Check that trace files exist
     */
    private static void assertTraceFilesExist() 
    {
        
        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                for (int i=0 ; i < 2 ; i++)
                {   
                    String traceFileName = "trace" + (i+1) + ".out";
                    File traceFile = new File(traceFileName);
                    //System.out.println("user.dir=" + System.getProperty("user.dir"));
                    //System.out.println("fullpath = " + traceFile.getAbsolutePath());
                        assertTrue(traceFile.exists());
                } 
                return null;
            }
        });
    }

    /**
     * Tests client URLs to see connection is successful or the correct exception is thrown.
     */
    public static void testClientURL() throws SQLException {
        if (!usingDerbyNetClient())
            return;
        
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        String protocol = 
            TestConfiguration.getCurrent().getJDBCClient().getUrlBase();
        if (usingDerbyNetClient())
            protocol = protocol + TestConfiguration.getCurrent().getHostName()
            + ":" + TestConfiguration.getCurrent().getPort() + "/";
        
        Properties info = null;     //test with null Properties object

        String CLIENT_CREATE_URL_WITH_COLON1 = 
            protocol + dbName + ":create=true";
        //String CLIENT_CREATE_URL_WITH_COLON2 = protocol + DERBY_SYSTEM_HOME + 
        //   File.separator + dbName + ":create=true";
        // String CLIENT_CREATE_URL_WITH_DOUBLE_QUOTES1 = 
        //     protocol + "\"" + dbName + "\";create=true";  
        // String CLIENT_CREATE_URL_WITH_DOUBLE_QUOTES2 = protocol + "\"" + 
        //     DERBY_SYSTEM_HOME + File.separator + dbName + "\";create=true"; 
        // String CLIENT_CREATE_URL_WITH_SINGLE_QUOTES1 = protocol + "'" + 
        //     DERBY_SYSTEM_HOME + File.separator + dbName + "';create=true"; 
        String CLIENT_CREATE_URL_WITH_SINGLE_QUOTES2 = 
            protocol + "'" + dbName + "';create=true";
        
        String CLIENT_SHUT_URL_WITH_SINGLE_QUOTES2 = 
            protocol + "'" + dbName + "';shutdown=true";
        
        //Client URLS
        String[] clientCreateUrls = new String[]
        {
            CLIENT_CREATE_URL_WITH_COLON1,
            //CLIENT_URL_WITH_COLON2,
            //CLIENT_URL_WITH_DOUBLE_QUOTES1,
            //CLIENT_URL_WITH_DOUBLE_QUOTES2,
            //CLIENT_URL_WITH_SINGLE_QUOTES1,
            CLIENT_CREATE_URL_WITH_SINGLE_QUOTES2
        };
        
        for (int i = 0; i < clientCreateUrls.length;i++)
        {
            String url = clientCreateUrls[i];
            try{
                if (url.equals(CLIENT_CREATE_URL_WITH_COLON1))
                {
                    Driver driver = DriverManager.getDriver(url);
                    assertNull(driver.connect(url,info));
                }
                else
                    assertConnect(true, url, info);
            }
            catch(SQLException se){
                fail ("did not expect an exception");
            }
        }
        // shutdown the databases, which should get rid of all open connections
        // currently, there's only the one; otherwise, this could be done in
        // a loop.
        shutdownDB(
            CLIENT_SHUT_URL_WITH_SINGLE_QUOTES2 + ";shutdown=true", null);
    }   
    
    /**
     * Tests URL with spaces in database name to check create and connect works. 
     * (DERBY-618). Make sure that the specified database gets created. We need 
     * to check this because even without the patch for DERBY-618, no exception
     * gets thrown when we try to connect to a database name with spaces. 
     * Instead, client driver extracts the database name as the string before 
     * the first occurence of space separator. Hence the database which gets 
     * created is wrong. e.g, if we specified database name as 
     * "db name with spaces", the database that got created by client driver 
     * was "db", which was wrong. We can check this by checking the correct URL
     * is returned by call to conn.getMetaData().getURL(). This is currently 
     * checked inside the testConnect method. We do not explicilty check the 
     * database directory creation since this check fails in remote server 
     * testing.       
     * 
     * @throws SQLException
     */
    public static void testDbNameWithSpaces() throws SQLException {
        
        Properties info = null;
        String url = null;
        
        String protocol = 
            TestConfiguration.getCurrent().getJDBCClient().getUrlBase();
        if (usingDerbyNetClient())
            protocol = protocol + TestConfiguration.getCurrent().getHostName()
            + ":" + TestConfiguration.getCurrent().getPort() + "/";
        else if (usingDerbyNet())
            protocol = protocol + "net:" + 
            TestConfiguration.getCurrent().getHostName() + ":" + 
            TestConfiguration.getCurrent().getPort() + "/";
        
        url = protocol + DB_NAME_WITH_SPACES + ";create=true";
        String shuturl = protocol + DB_NAME_WITH_SPACES + ";shutdown=true";
        
        assertConnect(false, url, null);
        shutdownDB(shuturl, null);
    }
    
    /**
     * Do java.sql.Driver.connect(String url, Properties info call)
     * 
     * @param expectUrlEqualsGetUrl boolean indicating embedded would
     *                  expect the url passed in to equal metadata.getURL()
     * @param url       url to pass to Driver.connect()
     * @param info      properties to pass to Driver.Connect()
     * 
     * @throws SQLException on error.
     */
    private static void assertConnect(
        boolean expectUrlEqualsGetUrl, String url, Properties info) 
    throws SQLException
    {
        Driver driver = DriverManager.getDriver(url);

        Connection conn = driver.connect(url, info);
        assertNotNull(conn);
   
        if (expectUrlEqualsGetUrl)
            assertEquals(url, conn.getMetaData().getURL());
        else
            assertNotSame(url, conn.getMetaData().getURL());
        ResultSet rs = 
            conn.createStatement().executeQuery("VALUES(CURRENT SCHEMA)");
        rs.next();
        assertEquals(
            rs.getString(1), conn.getMetaData().getUserName().toUpperCase());
        rs.close();
        conn.close();
        return;
    }
    
    /**
     * use this method to shutdown databases in an effort to release
     * any locks they may be holding
     */
    private static void shutdownDB(String url, Properties info) throws SQLException {
        
        Driver driver = DriverManager.getDriver(url);
        try {
            driver.connect(url, info);
        } catch (SQLException se) {
            assertSQLState("08006", se);
        }
    }

    /**
       Load the appropriate driver for the current framework
     */
    private static void loadDriver()
    {
        String driverClass =
            TestConfiguration.getCurrent().getJDBCClient().getJDBCDriverName();
        try {
            Class.forName(driverClass).newInstance();
        } catch (Exception e) {
            fail ("could not instantiate driver");
        }
    }
}
