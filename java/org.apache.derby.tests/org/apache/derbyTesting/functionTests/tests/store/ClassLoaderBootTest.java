/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.ClassLoaderBootTest

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import junit.extensions.TestSetup;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;


/*
 * This class tests a database boots using  class loaders. Test cases in this
 * class checks only one instance of a database can exist evenif database is 
 * booted using different class loader instances.    
 */
public class ClassLoaderBootTest extends BaseJDBCTestCase {

    private static URL derbyClassLocation; 
    private static URL embeddedDataSourceClassLocation; 
	static {
        // find the location of derby jar file and derbytools jar file
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        derbyClassLocation = getClassLocation("org.apache.derby.database.Database");
        embeddedDataSourceClassLocation = getClassLocation("org.apache.derby.jdbc.EmbeddedDataSource");
	}
    private static URL getClassLocation(String className)
    {
        CodeSource cs;
        try {
            Class cls = Class.forName(className);
            cs = cls.getProtectionDomain().getCodeSource();
        } catch (ClassNotFoundException e) {
            cs = null;
        }

        if(cs == null ) { return null; }
        else { return cs.getLocation(); }
    }
        

    private ClassLoader loader_1;
    private ClassLoader loader_2;
    private ClassLoader mainLoader;


    public ClassLoaderBootTest(String name ) {
        super(name);
    }

    /**
     * Runs the tests in the default embedded configuration and then
     * the client server configuration.
     */
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(ClassLoaderBootTest.class);
        Test test = suite;
        TestSetup setup = 
            new CleanDatabaseTestSetup(test) {
                 protected void setUp() throws Exception {
                     super.setUp();
                     //shutdown the database. 
                     DataSource ds = JDBCDataSource.getDataSource();
                     JDBCDataSource.shutdownDatabase(ds);
                 }
            };
        Properties p = new Properties();
        p.setProperty("derby.infolog.append", "true");
                                   
        setup = new SystemPropertyTestSetup(setup,p);
        // DERBY-2813 prevents test from running with security manager
        // on. Have to run without security manager for now.
        return SecurityManagerSetup.noSecurityManager(setup);
        //return setup;
    }


    /**
     * Simple set up, just setup the loaders.
     * @throws SQLException 
     */
    protected void setUp() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        URL[] urls = new URL[]{derbyClassLocation, embeddedDataSourceClassLocation};
        mainLoader = getThreadLoader();

        loader_1 = createDerbyClassLoader(urls);
        loader_2 = createDerbyClassLoader(urls);
    }

    protected void    tearDown()
        throws Exception
    {
        if ( mainLoader != null ) { setThreadLoader(mainLoader); }

        loader_1 = null;
        loader_2 = null;
        mainLoader = null;
    }


    /**
     * Create a new DerbyURLClassLoader inside a priv block.
     */
    private DerbyURLClassLoader createDerbyClassLoader(final URL[] urls) 
        throws Exception 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return AccessController.doPrivileged(
            new PrivilegedAction<DerbyURLClassLoader>(){
                 public DerbyURLClassLoader run()
                 {
                     return new DerbyURLClassLoader(urls);
                 }
             });
    }


    
    /* 
     * Test booting a database, that was alreadt booted by another class loader.
     */
	public void testBootingAnAlreadyBootedDatabase() throws SQLException 
    {
        //
        // This test relies on a bug fix in Java 6. Java 5 does not have this
        // bug fix and will fail this test. See DERBY-700.
        //
        if (!JDBC.vmSupportsJDBC4())
        {
            println( "The dual boot test only runs on Java 6 and higher." );
            return;
        }

        println( "The dual boot test is running." );
        
        // first boot the database using one loader and attempt 
        // to boot it using another loader, it should fail to boot.

        setThreadLoader(loader_1);
        DataSource ds_1 = JDBCDataSource.getDataSource();
        assertEquals(loader_1, getThreadLoader());
        assertEquals(loader_1, ds_1.getClass().getClassLoader());
        Connection conn1 = ds_1.getConnection();
        // now attemp to boot using another class loader.
        setThreadLoader(loader_2);
//IC see: https://issues.apache.org/jira/browse/DERBY-4361
        DataSource ds_2 = JDBCDataSource.getDataSource();
        assertEquals(loader_2, getThreadLoader());
        assertEquals(loader_2, ds_2.getClass().getClassLoader());
        try {
            ds_2.getConnection();
            fail("booted database that was already booted by another CLR");
        } catch (SQLException e) {
            SQLException ne = e.getNextException();
            assertPreventDualBoot(ne);
            JDBCDataSource.shutEngine(ds_2);
        }
        
        // shutdown the engine.
        setThreadLoader(loader_1);
        JDBCDataSource.shutEngine(ds_1);
    }

    
    /* 
     * Test booting a database, that was  booted and shutdown 
     * by another class loader.
     */
	public void testBootingDatabaseShutdownByAnotherCLR() throws SQLException 
    {
        // first boot the database using one loader and shutdown and then 
        // attempt to boot it using another loader, it should boot.

        setThreadLoader(loader_1);
        DataSource ds_1 = JDBCDataSource.getDataSource();
        assertEquals(loader_1, ds_1.getClass().getClassLoader());
        Connection conn1 = ds_1.getConnection();
        //shutdown the database.
        JDBCDataSource.shutdownDatabase(ds_1);
        // now attempt to boot using another class loader.
        setThreadLoader(loader_2);
        DataSource ds_2 = JDBCDataSource.getDataSource();
        assertEquals(loader_2, ds_2.getClass().getClassLoader());
        ds_2.getConnection();
        // shutdown the engine for both the class loaders.
//IC see: https://issues.apache.org/jira/browse/DERBY-4361
        JDBCDataSource.shutEngine(ds_2);
        JDBCDataSource.shutEngine(ds_1);
}

    private void setThreadLoader(final ClassLoader which) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        AccessController.doPrivileged(new PrivilegedAction<Void>(){
            public Void run()  {
                java.lang.Thread.currentThread().setContextClassLoader(which);
              return null;
            }
        });
    }

    private ClassLoader getThreadLoader() {
        return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>(){
            public ClassLoader run()  {
                return java.lang.Thread.currentThread().getContextClassLoader();
            }
        });
    }

	private static void assertPreventDualBoot(SQLException ne) {
		assertNotNull(ne);
		String state = ne.getSQLState();
		assertTrue("Unexpected SQLState:" + state, state.equals("XSDB6"));
	}



    /*
     * Simple specialized URLClassLoader for Derby.  
     * Filters all derby classes out of parent ClassLoader to ensure
     * that Derby classes are loaded from the URL specified
     */
    public class DerbyURLClassLoader extends URLClassLoader {
	
        /**
         * @see java.net.URLClassLoader#URLClassLoader(URL[] urls)
         */
        public DerbyURLClassLoader(URL[] urls) {
            super(urls);
        }


        /**
         * @see java.net.URLClassLoader#URLClassLoader(URL[] urls, 
         *      ClassLoader parent)
         */
        public DerbyURLClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
	
        }
	
        /**
         *@see java.net.URLClassLoader#URLClassLoader(java.net.URL[], 
         *      java.lang.ClassLoader, java.net.URLStreamHandlerFactory)
         */
        public DerbyURLClassLoader(URL[] urls, ClassLoader parent,
                                   URLStreamHandlerFactory factory) {
            super(urls, parent, factory);
		
        }
	
        /* Override the parent class loader to filter out any derby
         * jars in the classpath.  Any classes that start with 
         * "org.apache.derby" will load  from the URLClassLoader
         * 
         * @see java.lang.ClassLoader#loadClass(java.lang.String, boolean)
         */
        protected synchronized Class loadClass(String name, boolean resolve)
            throws ClassNotFoundException
        {

            Class cl = findLoadedClass(name);
            if (cl == null) {
                // cut off delegation to parent for certain classes
                // to ensure loading from the desired source
                if (!name.startsWith("org.apache.derby")) {
                    cl = getParent().loadClass(name);
		    	}
		    }
            if (cl == null) cl = findClass(name);
            if (cl == null) throw new ClassNotFoundException();
            if (resolve) resolveClass(cl);
            return cl;
        }

        /* 
         * @see java.lang.ClassLoader#loadClass(java.lang.String)
         */
        public Class loadClass(String name) throws ClassNotFoundException {
                return loadClass(name, false);
        }

    }
}

