/*
 *
 * Derby - Class BaseTestCase
 *
 * Copyright 2006 The Apache Software Foundation or its 
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.util;

import junit.framework.TestCase;
import java.io.PrintStream;
import java.net.URL;
import java.sql.SQLException;
import java.security.AccessController;

import java.security.PrivilegedActionException;

/**
 * Base class for JUnit tests.
 */
public abstract class BaseTestCase
    extends TestCase {

    /**
     * Configuration for the test case.
     * The configuration is created based on system properties.
     *
     * @see TestConfiguration
     */
    public static final TestConfiguration CONFIG = 
        TestConfiguration.DERBY_TEST_CONFIG;
    
    /**
     * No argument constructor made private to enforce naming of test cases.
     * According to JUnit documentation, this constructor is provided for
     * serialization, which we don't currently use.
     *
     * @see #BaseTestCase(String)
     */
    private BaseTestCase() {}

    /**
     * Create a test case with the given name.
     *
     * @param name name of the test case.
     */
    public BaseTestCase(String name) {
        super(name);
    }
    
    /**
     * Run the test and force installation of a security
     * manager with the default test policy file.
     * Individual tests can run without a security
     * manager or with a different policy file using
     * the decorators obtained from SecurityManagerSetup.
     * <BR>
     * Method is final to ensure security manager is
     * enabled by default. Tests should not need to
     * ovveride runTest, instead use test methods
     * setUp, tearDown methods and decorators.
     */
    public final void runBare() throws Throwable {
    	// Not ready for prime time!
    	// SecurityManagerSetup.installSecurityManager();
    	super.runBare();
    }
    
    /**
     * Print alarm string
     * @param text String to print
     */
    public static void alarm(final String text) {
        out.println("ALARM: " + text);
    }

    /**
     * Print debug string.
     * @param text String to print
     */
    public static void println(final String text) {
        if (CONFIG.isVerbose()) {
            out.println("DEBUG: " + text);
        }
    }

    /**
     * Print debug string.
     * @param t Throwable object to print stack trace from
     */
    public static void printStackTrace(Throwable t) 
    {
        while ( t!= null) {
            t.printStackTrace(out);
            
            if (t instanceof SQLException)  {
                t = ((SQLException) t).getNextException();
            } else {
                break;
            }
        }
    }

    private final static PrintStream out = System.out;
    
    /**
     * Set system property
     *
     * @param name name of the property
     * @param value value of the property
     */
    protected static void setSystemProperty(final String name, 
					    final String value)
	throws PrivilegedActionException {
	
	AccessController.doPrivileged
	    (new java.security.PrivilegedAction(){
		    
		    public Object run(){
			System.setProperty( name, value);
			return null;
			
		    }
		    
		}
	     );
	
    }
    /**
     * Remove system property
     *
     * @param name name of the property
     */
    protected static void removeSystemProperty(final String name)
	throws PrivilegedActionException {
	
	AccessController.doPrivileged
	    (new java.security.PrivilegedAction(){
		    
		    public Object run(){
			System.getProperties().remove(name);
			return null;
			
		    }
		    
		}
	     );
	
    }    
    /**
     * Get system property.
     *
     * @param name name of the property
     */
    protected static String getSystemProperty(final String name)
	throws PrivilegedActionException {

	return (String )AccessController.doPrivileged
	    (new java.security.PrivilegedAction(){

		    public Object run(){
			return System.getProperty(name);

		    }

		}
	     );
    }
    
    /**
     * Obtain the URL for a test resource, e.g. a policy
     * file or a SQL script.
     * @param name Resource name, typically - org.apache.derbyTesing.something
     * @return URL to the resource, null if it does not exist.
     * @throws PrivilegedActionException
     */
    protected static URL getTestResource(final String name)
	throws PrivilegedActionException {

	return (URL)AccessController.doPrivileged
	    (new java.security.PrivilegedAction(){

		    public Object run(){
			return BaseTestCase.class.getClassLoader().
			    getResource(name);

		    }

		}
	     );
    }  
    
    /**
     * Assert a security manager is installed.
     *
     */
    public static void assertSecurityManager()
    {
    	assertNotNull("No SecurityManager installed",
    			System.getSecurityManager());
    }
    
} // End class BaseTestCase
