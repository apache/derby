/*

   Derby - Class org.apache.derbyTesting.functionTests.store.ClassLoaderBootTest

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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedActionException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;


/*
 * This class tests a database boots using  class loaders. Test cases in this
 * class checks only one instance of a database can exist evenif database is 
 * booted using different class loader instances.    
 */
public class ClassLoaderBootTest extends BaseJDBCTestCase {

    private static final String POLICY_RESOURCE = "org" + File.separator + "apache" +File.separator + "derbyTesting" +File.separator +
    		"functionTests" + File.separator + "tests" + File.separator + "store" + File.separator + "ClassLoaderBootTest.policy";
    				
    
    		
	private static URL derbyClassLocation; 
	static {
        // find the location of derby jar file or location 
        // of classes. 
        CodeSource cs;
        try {
            Class cls = Class.forName("org.apache.derby.database.Database");
            cs = cls.getProtectionDomain().getCodeSource();
        } catch (ClassNotFoundException e) {
            cs = null;
        }

        if(cs == null )
            derbyClassLocation = null;        
        else 
            derbyClassLocation = cs.getLocation();
	}
        

    private ClassLoader loader_1;
    private ClassLoader loader_2;
    private ClassLoader mainLoader;


    public ClassLoaderBootTest(String name ) {
        super(name);
    }

    private static String makeServerPolicyName()
    {
        try {
            String  userDir = getSystemProperty( "user.dir" );
            
            String  fileName = userDir + File.separator + SupportFilesSetup.EXTINOUT + File.separator + POLICY_RESOURCE;
            File      file = new File( fileName );
            String  urlString = file.toURL().toExternalForm();

            return urlString;
        }
        catch (Exception e)
        {
            System.out.println( "Unexpected exception caught by makeServerPolicyName(): " + e );

            return null;
        }
    }

    /**
     * Runs the tests in the default embedded configuration and then
     * the client server configuration.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite(ClassLoaderBootTest.class);
        Test test = suite;
        // Test does not currently run wunder security manager
        // Requires AllPermissions for derbyTesting.jar to run.
        //  I therefore think it is a problem with the test not the
        // fix itself.   
        	test = SecurityManagerSetup.noSecurityManager(test);
        TestSetup setup = 
            new CleanDatabaseTestSetup(test) {
                protected void decorateSQL(Statement s) throws SQLException {
                    // table used to test  export.
                    s.execute("CREATE TABLE BOOKS(id int," +
                              "name varchar(30)," + 
                              "content clob, " + 
                              "pic blob )");
                }
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
                        
            return setup;
    }


    /**
     * Simple set up, just setup the loaders.
     * @throws SQLException 
     */
    protected void setUp() throws Exception
    {
        final URL[] urls = new URL[]{derbyClassLocation};
        mainLoader  = (ClassLoader )AccessController.doPrivileged
        (new java.security.PrivilegedAction(){

            public Object run(){
            	return java.lang.Thread.currentThread().getContextClassLoader();

            } });
       
        loader_1 = privURLClassLoader(urls);
       
        loader_2  = privURLClassLoader(urls);   
    }

	private ClassLoader privURLClassLoader(final URL[] urls) {
		return (ClassLoader )AccessController.doPrivileged
        (new java.security.PrivilegedAction(){

            public Object run(){
            	 return new URLClassLoader(urls,null);

            } });
	}



    /**
     * Given a loaded class, this
     * routine asks the class's class loader for information about where the
     * class was loaded from. Typically, this is a file, which might be
     * either a class file or a jar file. The routine figures that out, and
     * returns the name of the file. If it can't figure it out, it returns null
     */
    private static URL getFileWhichLoadedClass(final Class cls) throws Exception 
    {
        try {
         return (URL)AccessController.doPrivileged(
         new java.security.PrivilegedExceptionAction(){   
             public Object run()
             {
                 CodeSource cs = null;
                 cs = cls.getProtectionDomain().getCodeSource ();
                 if ( cs == null )
                     return null;        
                 return cs.getLocation ();
                 }
         });
        }catch(PrivilegedActionException pae) {
            throw pae.getException();
        }
    }
    
    private URL getURL(final File file) throws MalformedURLException
    {
        try {
            return (URL) AccessController.doPrivileged
            (new java.security.PrivilegedExceptionAction(){

                public Object run() throws MalformedURLException{
                return file.toURL();

                }
            }
             );
        } catch (PrivilegedActionException e) {
            throw (MalformedURLException) e.getException();
        } 
    }

    /* 
     * Test booting a database, that was alreadt booted by another class loader.
     */
	public void testBootingAnAlreadyBootedDatabase() throws SQLException 
    {
        // first boot the database using one loader and attempt 
        // to boot it using another loader, it should fail to boot.
        try {

            setThreadLoader(loader_1);
            DataSource ds_1 = JDBCDataSource.getDataSource();
            Connection conn1 = ds_1.getConnection();
            // now attemp to boot using another class loader.
            setThreadLoader(loader_2);
            try {
                DataSource ds_2 = JDBCDataSource.getDataSource();
                ds_2.getConnection();
                fail("booted database that was already booted by another CLR");
            } catch (SQLException e) {
                SQLException ne = e.getNextException();
                ClassLoaderBootTest.assertPreventDualBoot(ne);
            }
            
            // shutdown the database.
            setThreadLoader(loader_1);
            JDBCDataSource.shutdownDatabase(ds_1);
            
        } catch (SQLException se) {
            dumpSQLException(se);
        }finally {
            // set the thread context loader back to the generic one. 
            setThreadLoader(mainLoader);
        }
    }

    
    /* 
     * Test booting a database, that was  booted and shutdown 
     * by another class loader.
     */
	public void testBootingDatabaseShutdownByAnotherCLR() throws SQLException 
    {
        // first boot the database using one loader and shutdown and then 
        // attempt to boot it using another loader, it should boot.
        try {

            setThreadLoader(loader_1);
            DataSource ds_1 = JDBCDataSource.getDataSource();
            Connection conn1 = ds_1.getConnection();
            //shutdown the database.
            JDBCDataSource.shutdownDatabase(ds_1);
            // now attemp to boot using another class loader.
            setThreadLoader(loader_2);
            DataSource ds_2 = JDBCDataSource.getDataSource();
            ds_2.getConnection();
            // shutdown the database.
            JDBCDataSource.shutdownDatabase(ds_2);
            
        } catch (SQLException se) {
            dumpSQLException(se);
        }finally {
            // set the thread context loader back to the generic one. 
            setThreadLoader(mainLoader);
        }
    }



    /* 
     * Test booting the same database by multiple thereads in it's own 
     * class loader.
     */
	public void testBootingDatabaseInMultipleThread() throws Exception 
    {
        ParallelDatabaseBoots pdb = new ParallelDatabaseBoots();
        pdb.startConcurrentDatabaseBoots();
    }


    private void setThreadLoader(final ClassLoader which) {

        AccessController.doPrivileged
        (new java.security.PrivilegedAction(){
            
            public Object run()  { 
                java.lang.Thread.currentThread().setContextClassLoader(which);
              return null;
            }
        });
    }


    private static void dumpSQLException(SQLException se)
    {
		while (se != null)
		{
			se.printStackTrace();
			se = se.getNextException();
		}		
	}	

	private static void assertPreventDualBoot(SQLException ne) {
		assertNotNull(ne);
		String state = ne.getSQLState();
		assertTrue("Unexpected SQLState:" + state, state.equals("XSDB6") || state.equals("XSDBB"));
	}


    /*
     *  This class is used to test concurrent database boots. Each 
     *  thread has it's own class loader.Only one instance of the 
     *  database can exist at any time, only one thread shoud be 
     *  able to successfuly boot the database, 
     */
    private class ParallelDatabaseBoots implements Runnable{

        private volatile int noBoots = 0 ; 
        private volatile int noBootAttempts = 0;
        private Exception unExpectedException;

        /*
         * Attempts to boot the database in a separate loader. Increases
         * the counter if the database is succefully booted. 
         */
        private void bootDatabase() throws Exception {
            // boot the database , if another thread has not booted 
            // it already using a different class loader. 
            ClassLoader myLoader;
            URL[] urls = new URL[]{derbyClassLocation};
            myLoader = privURLClassLoader(urls);
        
            try {
                setThreadLoader(myLoader);
                boolean booted = false;
                DataSource ds = null;
                try {
                    ds = JDBCDataSource.getDataSource();
                    ds.getConnection();
                    // successfuly booted the database, increment 
                    // the no of boots counter. 
                    noBoots++;
                    booted = true;
                } catch (SQLException e) {
                    // failed to boot the database, 
                    // ensure it failed with correct error.
                    SQLException ne = e.getNextException();
                    assertPreventDualBoot(ne);                    
                    
                }finally {
                    synchronized(this) {
                        // attemped to boot the database.
                        noBootAttempts--;
                        notifyAll();
                    }

                }
            
                // shutdown the database, if it was booted 
                // by this thread.
                if (booted) {
                    // wait for all threads complete 
                    // their attempt to boot the database. 
                    synchronized(this) {
                    
                        while (noBootAttempts > 0) {
                            wait();
                        }
                    }
                    JDBCDataSource.shutdownDatabase(ds);
                }
            }finally {
                // set the thread context loader back to the main loader.
                setThreadLoader(mainLoader);
            }
        }

        private void startConcurrentDatabaseBoots() throws Exception {
            // first boot the database using one loader and attempt 
            // to boot it using another loader, it should fail to boot.           
            int noThreads = 10;
            noBootAttempts = noThreads;
            Thread threads[] = new Thread[noThreads];
            // create all the threads
            for(int i=0 ; i < noThreads ; i++) {
                threads[i] = new Thread(this, "bootThread=" +i);
            }

            // start  all the threads 
            for(int i=0 ; i < noThreads ; i++) {
                threads[i].start();
            }
            
            // wait for all threds to complete
            for(int i=0 ; i < noThreads ; i++)
            {
                try{
                    threads[i].join();
                }catch (java.lang.InterruptedException ie) {
                    //ignore ..
                }

            }
            
            // if there is any exception by any of the threads, test failed.
            if (unExpectedException != null)
                throw unExpectedException;
            assertEquals("More than one thread booted the database concurrently",1,noBoots);
     
        }

        
        /*
         * Attempts to boot the database on seperate thread. 
         * Impementation of run() method of Runnable interface.
         */
        public void run() {
            try {
                bootDatabase();
            } catch(Exception e) {
                // save the exception, the method that 
                // invoked the thread needs to know 
                // about it.
                unExpectedException = e;
            }
        }
    }

}

