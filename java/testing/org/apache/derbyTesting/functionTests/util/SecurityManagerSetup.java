/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.SecurityManagerSetup
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
	private SecurityManagerSetup(Test test, String policyResource)
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
	public static Test noSecurityManager(BaseTestCase test)
	{
		if (externalSecurityManagerInstalled)
			return new TestSuite();
		return new SecurityManagerSetup(test, "<NONE>");
	}
	
	/**
	 * Install specific polciy file with the security manager
	 * including the special case of no security manager.
	 */
	protected void setUp() throws PrivilegedActionException {
		installSecurityManager(decoratorPolicyResource);
	}
	
	/**
	 * Install a SecurityManager with the default test policy
	 * file:
	 * org/apache/derbyTesting/functionTests/util/derby_tests.policy
	 * 
	 */
	static void installSecurityManager() throws PrivilegedActionException
	{
		installSecurityManager(
				"org/apache/derbyTesting/functionTests/util/derby_tests.policy");
				
	}
	
	private static void installSecurityManager(String policyFile)
			throws PrivilegedActionException {
		
		if (externalSecurityManagerInstalled)
			return;
		
		Properties set = new Properties(classPathSet);
		setSecurityPolicy(set, policyFile);

		SecurityManager sm = System.getSecurityManager();
		if (sm != null) {
			// SecurityManager installed, see if it has the same settings.

			if (set.getProperty("java.security.policy").equals(
					BaseTestCase.getSystemProperty("java.security.policy")))
					return;
			
			// Uninstall the current manager.
			AccessController.doPrivileged(new java.security.PrivilegedAction() {

				public Object run() {
					System.setSecurityManager(null);
					return null;
				}
			});
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
			String policyResource) throws PrivilegedActionException
	{
		URL policyURL = BaseTestCase.getTestResource(policyResource);

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
	 * Location of derby.jar via org.apache.derby.jdbc.EmbeddedDataSource
	 * Location of derbyclient.jar via org.apache.derby.jdbc.ClientDataSource
	 * 
	 * Two options are supported, either all are in jar files or
	 * all are on the classpath. Properties are set as follows:
	 * 
	 * <P>
	 * Classpath:
	 * <BR>
	 * derbyTesting.codeclasses set to location of classes folder
	 * <P>
	 * Jar files:
	 * <BR>
	 * derbyTesting.codejar - location of derby.jar,
	 * derbynet.jar and derbytools.jar, all assumed to be in the
	 * same location.
	 * <BR>
	 * derbyTesting.clientjar - location of derbyclient.jar (FUTURE)
	 * <BR>
	 * derbyTesting.testjar - location of derbyTesting.jar (FUTURE)
	 * 
	 */
	private static boolean determineClasspath()
	{
		// Security manager already installed, assume that
		// it is set up correctly.
		if (System.getSecurityManager() != null) {		
			return true;
		}
		
		URL testing = getURL(SecurityManagerSetup.class);
		
		boolean isClasspath = testing.toExternalForm().endsWith("/");
		if (isClasspath) {
			classPathSet.setProperty("derbyTesting.codeclasses",
					testing.toExternalForm());
			return false;
		}
		classPathSet.setProperty("derbyTesting.testjar", stripJar(testing));
		
		URL derby = null;
		try {
			derby = getURL(org.apache.derby.jdbc.EmbeddedDataSource.class);
		} catch (java.lang.NoClassDefFoundError e) {
			derby = testing;
		}		
		classPathSet.setProperty("derbyTesting.codejar", stripJar(derby));

		URL client = null;
		try {
			client = getURL(org.apache.derby.jdbc.ClientDataSource.class);
		} catch (java.lang.NoClassDefFoundError e) {
			client = derby;
		}
		
		classPathSet.setProperty("derbyTesting.clientjar", stripJar(client));
		
		return false;
	}
	
	/**
	 * Strip of the last token which will be the jar name.
	 * The returned string includes the trailing slash.
	 * @param url
	 * @return
	 */
	private static String stripJar(URL url)
	{
		String ef = url.toExternalForm();
		return ef.substring(0, ef.lastIndexOf('/') + 1);
	}
	
	/**
	 * Get the URL of the code base from a class.
	 */
	private static URL getURL(final Class cl)
	{
		return (URL)
		   AccessController.doPrivileged(new java.security.PrivilegedAction() {

			public Object run() {
				return cl.getProtectionDomain().getCodeSource().getLocation();
			}
		});
	}
}
