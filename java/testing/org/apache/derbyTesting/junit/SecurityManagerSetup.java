/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.SecurityManagerSetup
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
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.util.Enumeration;
import java.util.Properties;


import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Setup for running Derby JUnit tests with the SecurityManager
 * which is the default for tests.
 *
 */
public final class SecurityManagerSetup extends TestSetup {
	
	private static final Properties classPathSet = new Properties();
    
    /**
     * True if the classes are loaded from jars.
     */
    static boolean isJars;
	
	/**
	 * True if a security manager was installed outside of the
	 * control of this class and BaseTestCase.
	 */
	private static final boolean externalSecurityManagerInstalled;
	
	static {
		// Determine what the set of properties
		// describing the environment is.
		externalSecurityManagerInstalled = determineClasspath();
        
	}
	
	private final String decoratorPolicyResource;
	public SecurityManagerSetup(Test test, String policyResource)
	{
		super(test);
		this.decoratorPolicyResource = policyResource;
	}
	
	/**
	 * Get a decorator that will ensure no security manger
	 * is installed to run a test. Not supported for suites.
	 * <BR>
	 * An empty suite is returned if a security manager was installed
	 * externally, i.e. not under the control of the BaseTestCase
	 * and this code. In this case the code can not support the
	 * mode of no security manager as it may not have enough information
	 * to re-install the security manager. So the passed in test
	 * will be skipped.
	 */
	public static Test noSecurityManager(Test test)
	{
		if (externalSecurityManagerInstalled)
			return new TestSuite("skipped due to external security manager "
                    + test.toString());
		return new SecurityManagerSetup(test, "<NONE>");
	}
	
    /**
     * Same as noSecurityManager() above but takes a TestSetup
     * instead of a BaseTestCase.
     */
    public static Test noSecurityManager(TestSetup tSetup)
    {
		if (externalSecurityManagerInstalled)
			return new TestSuite("skipped due to external security manager "
                    + tSetup.toString());
		return new SecurityManagerSetup(tSetup, "<NONE>");
    }

	/**
	 * "Install" no security manager.
	 * 
	 */
	static void noSecurityManager()
	{
		installSecurityManager("<NONE>");
	}
	
	/**
	 * Install specific polciy file with the security manager
	 * including the special case of no security manager.
	 */
	protected void setUp() {
		installSecurityManager(decoratorPolicyResource);
	}
    
    protected void tearDown() throws Exception
    {
        if ("<NONE>".equals(decoratorPolicyResource))
            BaseTestCase.setSystemProperty("java.security.policy", "");
        else if ( !externalSecurityManagerInstalled )
        {
            uninstallSecurityManager();
        }
    }
	
    /**
     * Return the name of the default policy.
     */
    public static String getDefaultPolicy()
    {
        return "org/apache/derbyTesting/functionTests/util/derby_tests.policy";
    }

	/**
	 * Install a SecurityManager with the default test policy
	 * file:
	 * org/apache/derbyTesting/functionTests/util/derby_tests.policy
	 * 
	 */
	static void installSecurityManager()
	{
		installSecurityManager( getDefaultPolicy() );
	}
	
	private static void installSecurityManager(String policyFile)
			 {

		if (externalSecurityManagerInstalled)
			return;
		
		Properties set = new Properties(classPathSet);
		setSecurityPolicy(set, policyFile);

		SecurityManager sm = System.getSecurityManager();
		if (sm != null) {
			// SecurityManager installed, see if it has the same settings.

			String  newPolicyProperty = set.getProperty("java.security.policy" );
			if ( newPolicyProperty == null ) { newPolicyProperty = ""; } 
                                                   
			String  oldPolicyProperty = BaseTestCase.getSystemProperty("java.security.policy");

			if ( oldPolicyProperty == null ) { oldPolicyProperty = ""; }

			if ( newPolicyProperty.equals( oldPolicyProperty ) ) { return; }
			
			// Uninstall the current manager.
			uninstallSecurityManager();
		}
		
		// Set the system properties from the desired set.
		for (Enumeration e = set.propertyNames(); e.hasMoreElements();) {
			String key = (String) e.nextElement();
			BaseTestCase.setSystemProperty(key, set.getProperty(key));
		}
		
		// Check indicator for no security manager
		if ("<NONE>".equals(set.getProperty("java.security.policy")))
			return;
		
		// and install
		AccessController.doPrivileged(new java.security.PrivilegedAction() {


                public Object run() {
				System.setSecurityManager(new SecurityManager());
				return null;
			}
		});

	}
	
	private static void setSecurityPolicy(Properties set,
			String policyResource)
	{
		if ("<NONE>".equals(policyResource)) {
			set.setProperty("java.security.policy", policyResource);
			return;
		}
		URL policyURL = BaseTestCase.getTestResource(policyResource);

		// maybe the passed in resource was an URL to begin with
		if ( policyURL == null )
		{
			try { policyURL = new URL( policyResource ); }
			catch (Exception e) { System.out.println( "Unreadable url: " + policyResource ); }
		}

		if (policyURL != null)
			set.setProperty("java.security.policy",
					policyURL.toExternalForm());
	}

	
	/**
	 * Determine the settings of the classpath in order to configure
	 * the variables used in the testing policy files.
	 * Looks for three items:
	 * 
	 * Location of derbyTesting.jar via this class
	 * Location of derby.jar via org.apache.derby.jdbc.EmbeddedSimpleDataSource
	 * Location of derbyclient.jar via org.apache.derby.jdbc.ClientDataSource
	 * 
	 * Two options are supported, either all are in jar files or
	 * all are on the classpath. Properties are set as follows:
	 * 
	 * <P>
	 * Classpath:
	 * <BR>
	 * derbyTesting.codeclasses set to URL of classes folder
	 * <P>
	 * Jar files:
	 * <BR>
	 * derbyTesting.codejar - URL of derby.jar,
	 * derbynet.jar and derbytools.jar, all assumed to be in the
	 * same location.
	 * <BR>
	 * derbyTesting.clientjar - URL of derbyclient.jar
	 * <BR>
	 * derbyTesting.testjar - URL of derbyTesting.jar
     * <BR>
     * derbyTesting.testjarpath - File system path to derbyTesting.jar
     * if the jar has a URL with a file protocol.
	 * 
	 */
	private static boolean determineClasspath()
	{
		// Security manager already installed, assume that
		// it is set up correctly.
		if (System.getSecurityManager() != null) {		
			return true;
		}

		//We need the junit classes to instantiate this class, so the
		//following should not cause runtime errors.
        URL junit = getURL(junit.framework.Test.class);
        if (junit != null)
            classPathSet.setProperty("derbyTesting.junit", junit.toExternalForm());
	
        // Load indirectly so we don't need ant-junit.jar at compile time.
        URL antjunit = getURL("org.apache.tools.ant.taskdefs.optional.junit.JUnitTestRunner");
        if (antjunit != null)
            classPathSet.setProperty("derbyTesting.antjunit", antjunit.toExternalForm());

		
        /* When inserting XML values that use external DTD's, the JAXP
         * parser needs permission to read the DTD files.  So here we set
         * a property to hold the location of the JAXP implementation
         * jar file.  We can then grant the JAXP impl the permissions
         * needed for reading the DTD files.
         */
        String jaxp = XML.getJAXPParserLocation();
        if (jaxp != null)
            classPathSet.setProperty("derbyTesting.jaxpjar", jaxp);

		URL testing = getURL(SecurityManagerSetup.class);
		
		boolean isClasspath = testing.toExternalForm().endsWith("/");
		if (isClasspath) {
			classPathSet.setProperty("derbyTesting.codeclasses",
					testing.toExternalForm());
            isJars = false;
			return false;
		}
		classPathSet.setProperty("derbyTesting.testjar", stripJar(testing));
        if (testing.getProtocol().equals("file")) {
           File f = new File(testing.getPath());
           classPathSet.setProperty("derbyTesting.testjarpath",
                                               f.getAbsolutePath());
        }
        isJars = true;
		
		URL derby = getURL("org.apache.derby.jdbc.EmbeddedSimpleDataSource");
        if (derby != null)
		    classPathSet.setProperty("derbyTesting.codejar", stripJar(derby));

		// if we attempt to check on availability of the ClientDataSource with 
		// JSR169, attempts will be made to load classes not supported in
		// that environment, such as javax.naming.Referenceable. See DERBY-2269.
		if (!JDBC.vmSupportsJSR169()) {
		    URL client = getURL("org.apache.derby.jdbc.ClientDataSource");
		    if(client != null)
		        classPathSet.setProperty("derbyTesting.clientjar", stripJar(client));
		}
	
		return false;
	}
    
    /**
     * Return the policy file system properties for use
     * by the old test harness. This ensures a consistent
     * approach to setting the properties. There are the
     * properties used to define the jar file location in
     * any policy files.
     */
    public static Properties getPolicyFilePropertiesForOldHarness()
    {
        return classPathSet;
    }
	
	/**
	 * Strip off the last token which will be the jar name.
	 * The returned string includes the trailing slash.
	 * @param url
	 * @return the jar name from the URL as a String
	 */
	private static String stripJar(URL url)
	{
		String ef = url.toExternalForm();
		return ef.substring(0, ef.lastIndexOf('/') + 1);
	}
    
    /**
     * Get the URL of the code base from a class name.
     * If the class cannot be loaded, null is returned.
     */
    static URL getURL(String className) {
        try {
            return getURL(Class.forName(className));
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
	
	/**
	 * Get the URL of the code base from a class.
	 */
	static URL getURL(final Class cl)
	{
		return (URL)
		   AccessController.doPrivileged(new java.security.PrivilegedAction() {

			public Object run() {

                /* It's possible that the class does not have a "codeSource"
                 * associated with it (ex. if it is embedded within the JVM,
                 * as can happen with Xalan and/or a JAXP parser), so in that
                 * case we just return null.
                 */
                if (cl.getProtectionDomain().getCodeSource() == null)
                    return null;

				return cl.getProtectionDomain().getCodeSource().getLocation();
			}
		});
	}

    /**
     * Remove the security manager.
     */
    private static void uninstallSecurityManager()
    {

            AccessController.doPrivileged
            (
             new java.security.PrivilegedAction()
             {
                 public Object run() {
                     System.setSecurityManager(null);
                     return null;
                 }
             }
             );

    }

}
