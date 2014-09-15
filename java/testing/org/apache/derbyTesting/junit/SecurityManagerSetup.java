/*
 *
 * Derby - Class org.apache.derbyTesting.junit.SecurityManagerSetup
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;
import java.util.Properties;
import junit.extensions.TestSetup;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;

/**
 * Configures the wrapped test to be run with the specified security policy.
 * <p>
 * This setup class normally installs the default policy file. This can be
 * overridden by specifying {@literal java.security.policy=<NONE>} (see
 * {@linkplain #NO_POLICY}), and can also be overridden by installing a
 * security manager explicitly before the default security manager is installed.
 * <p>
 * Individual tests/suites can be configured to be run without a security
 * manager, with a specific policy file, or with a specific policy file merged
 * with the default policy file. The last option is useful when you only need
 * to extend the default policy with a few extra permissions to run a test.
 */
public final class SecurityManagerSetup extends TestSetup {

    /** Constant used to indicate that no security policy is to be installed. */
    static final String NO_POLICY = "<NONE>";

    /**
     * Does the JVM support Subjects for
     * authorization through Java security manager.
     * J2ME/CDC/Foundation 1.1 does not support Subjects.
     */
    public static final boolean JVM_HAS_SUBJECT_AUTHORIZATION;
    static {
        JVM_HAS_SUBJECT_AUTHORIZATION = JDBC.haveClass("javax.security.auth.Subject");
    }
    
	
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

    static {
        // Work around bug in weme6.2 (DERBY-5558 and DERBY-6079).
        if (BaseTestCase.isJ9Platform()) {
            BaseTestCase.setSystemProperty("emma.active", "");
            BaseTestCase.setSystemProperty("jacoco.active", "");
        }
    }

    static final boolean jacocoEnabled = checkIfJacocoIsRunning();
    private static boolean checkIfJacocoIsRunning() {
        return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
                public Boolean run() {
                    try {
                        // Check if some arbitrary class from jacocoagent.jar
                        // is available.
                        Class.forName("org.jacoco.agent.rt.RT");

                        // If we got here, it means the tests are running
                        // under JaCoCo. Set the jacoco.active property to
                        // the empty string in order to activate the
                        // JaCoCo-specific permissions in derby_tests.policy,
                        // and return true.
                        System.setProperty("jacoco.active", "");
                        return true;
                    } catch (ClassNotFoundException e) {
                        return false;
                    } catch (LinkageError e) {
                        return false;
                    }
                }
        });
    }

	private final String decoratorPolicyResource;
    /** An additional policy to install (may be {@code null}). */
    private final String additionalPolicyResource;
	private SecurityManager decoratorSecurityManager = null;
	
    public SecurityManagerSetup(Test test, String policyResource) {
        this(test, policyResource, false);
    }

    /**
     * Installs a new security policy.
     *
     * @param test the test to wrap
     * @param policyResource the policy to install
     * @param mergePolicies if {@code false} the specified policy will be the
     *      only policy installed, if {@code true} the specified policy will be
     *      merged with the default test policy for the test framework
     */
    public SecurityManagerSetup(Test test, String policyResource,
                                boolean mergePolicies) {
        super(test);
        if (mergePolicies) {
            // By choice, only support merging with the default test policy.
            this.decoratorPolicyResource = getDefaultPolicy();
            this.additionalPolicyResource = policyResource;
        } else {
            this.decoratorPolicyResource = policyResource != null ?
                    policyResource : getDefaultPolicy();
            this.additionalPolicyResource = null;
        }
    }

	/**
	 * Use custom policy and SecurityManager
	 * 
	 * @param test - Test to wrap
	 * @param policyResource - policy resource. If null use default testing policy
	 * @param securityManager - Custom SecurityManager if null use the system security manager
	 */
	public SecurityManagerSetup(Test test, String policyResource, SecurityManager securityManager)
	{
        this(test, policyResource, false);
		this.decoratorSecurityManager = securityManager;
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
     * 
     * @param test Test to run without a security manager. Note that
     * this must be an instance of BaseTestCase as this call depends
     * on setup code in that class. Arbitrary Test instances cannot be passed in.
	 */
	public static Test noSecurityManager(Test test)
	{
        if (externalSecurityManagerInstalled) {
            return new BaseTestSuite(
                "skipped due to external security manager "
                + test.toString());
        }

        return new SecurityManagerSetup(test, NO_POLICY);
	}

	/**
	 * "Install" no security manager.
	 * 
	 */
	static void noSecurityManager()
	{
        installSecurityManager(NO_POLICY);
	}
	
	/**
     * Install specific policy file with the security manager
	 * including the special case of no security manager.
	 */
    protected void setUp()
            throws IOException {
        String resource = getEffectivePolicyResource(
                decoratorPolicyResource, additionalPolicyResource);
        installSecurityManager(resource, decoratorSecurityManager);
	}
    
    protected void tearDown() throws Exception
    {
        if (NO_POLICY.equals(decoratorPolicyResource))
            BaseTestCase.setSystemProperty("java.security.policy", "");
        else if ( !externalSecurityManagerInstalled )
        {
            uninstallSecurityManager();
        }
    }
	
    /**
     * Return the name of the default policy.
     */
    private static String getDefaultPolicy()
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

	private static void installSecurityManager(String policyFile) {
	   installSecurityManager(policyFile, System.getSecurityManager());
	}

	private static void installSecurityManager(String policyFile, final SecurityManager sm)
			 {
	    
		if (externalSecurityManagerInstalled)
			return;
		
		Properties set = new Properties(classPathSet);
		setSecurityPolicy(set, policyFile);

		SecurityManager currentsm = System.getSecurityManager();
		if (currentsm != null) {
			// SecurityManager installed, see if it has the same settings.

			String  newPolicyProperty = set.getProperty("java.security.policy" );
			if ( newPolicyProperty == null ) { newPolicyProperty = ""; } 
                                                   
			String  oldPolicyProperty = BaseTestCase.getSystemProperty("java.security.policy");
			SecurityManager oldSecMan = System.getSecurityManager();

			if ( oldPolicyProperty == null ) { oldPolicyProperty = ""; }

			if ( newPolicyProperty.equals( oldPolicyProperty ) &&
			        oldSecMan == sm) { return; }
			
			// Uninstall the current manager.
			uninstallSecurityManager();
		}
		
		// Set the system properties from the desired set.
		for (Enumeration e = set.propertyNames(); e.hasMoreElements();) {
			String key = (String) e.nextElement();
			BaseTestCase.setSystemProperty(key, set.getProperty(key));
		}
		
		// Check indicator for no security manager
        if (NO_POLICY.equals(set.getProperty("java.security.policy")))
			return;
		
		// and install
        AccessController.doPrivileged(new PrivilegedAction<Void>() {


                public Void run() {
                    if (sm == null)
                        System.setSecurityManager(new SecurityManager());
                    else
                        System.setSecurityManager(sm);
                    Policy.getPolicy().refresh();
                    return null;
                }
		});
        println("installed policy " + policyFile);
	}
	
	private static void setSecurityPolicy(Properties set,
			String policyResource)
	{
        if (NO_POLICY.equals(policyResource)) {
			set.setProperty("java.security.policy", policyResource);
			return;
		}
        try {
            URL policyURL = getResourceURL(policyResource);
            set.setProperty("java.security.policy", policyURL.toExternalForm());
        } catch (MalformedURLException mue) {
            BaseTestCase.alarm("Unreadable policy URL: " + policyResource);
        }
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
	 * derbyTesting.codeclasses set to URL of classes folder
     * <BR>
     * derbyTesting.ppcodeclasses set to URL of the 'classes.pptesting' folder
     * if it exists on the classpath. The existence of the package private tests
     * is determined via org.apache.derby.PackagePrivateTestSuite
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

        URL ant = getURL("org.apache.tools.ant.Task");
        if (ant != null) {
            classPathSet.setProperty("derbyTesting.ant", ant.toExternalForm());
        }

        // variables for lucene jar files
        URL luceneCore = getURL( "org.apache.lucene.store.FSDirectory" );
        if ( luceneCore != null )
        {
            classPathSet.setProperty( "derbyTesting.lucene.core", luceneCore.toExternalForm() );
            classPathSet.setProperty( "derbyTesting.lucene.core.jar.file", luceneCore.getFile() );
        }

        // Load indirectly, normally no EMMA jars in the classpath.
        // This property is needed to set correct permissions in policy files.
        URL emma = getURL("com.vladium.emma.EMMAException");
        if (emma != null) {
            classPathSet.setProperty("emma.active", "");
        }

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
        URL ppTesting = null;
        // Only try to load PackagePrivateTestSuite if the running JVM is
        // Java 1.5 or newer (class version 49 = Java 1.5).
        if (BaseTestCase.getClassVersionMajor() >= 49) {
            ppTesting = getURL("org.apache.derby.PackagePrivateTestSuite");
        }
		boolean isClasspath = testing.toExternalForm().endsWith("/");
		if (isClasspath) {
			classPathSet.setProperty("derbyTesting.codeclasses",
					testing.toExternalForm());
            // ppTesting can be null, for instance if 'classes.pptesting' is
            // not on the classpath.
            if (ppTesting != null) {
                classPathSet.setProperty("derbyTesting.ppcodeclasses",
                    ppTesting.toExternalForm());
            }
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
		
		URL derby = getURL("org.apache.derby.jdbc.BasicEmbeddedDataSource40");
        if (derby != null)
		    classPathSet.setProperty("derbyTesting.codejar", stripJar(derby));

		// if we attempt to check on availability of the ClientDataSource with 
		// JSR169, attempts will be made to load classes not supported in
		// that environment, such as javax.naming.Referenceable. See DERBY-2269.
		if (!JDBC.vmSupportsJSR169()) {
           URL client = getURL(
                    JDBC.vmSupportsJNDI() ?
                    "org.apache.derby.jdbc.ClientDataSource" :
                    "org.apache.derby.jdbc.BasicClientDataSource40");

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
    public static URL getURL(String className) {
        try {
            return getURL(Class.forName(className));
        } catch (ClassNotFoundException e) {
            return null;
        } catch (NoClassDefFoundError e) {
            return null;
        }
    }
	
	/**
	 * Get the URL of the code base from a class.
	 */
	static URL getURL(final Class cl)
	{
        return AccessController.doPrivileged(new PrivilegedAction<URL>() {

			public URL run() {

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
             new PrivilegedAction<Void>()
             {
                 public Void run() {
                      System.setSecurityManager(null);
                     return null;
                 }
             }
             );

    }

    /**
     * Returns the location of the effective policy resource.
     * <p>
     * If two valid policy resources from different locations are specified,
     * they will be merged into one policy file.
     *
     * @param policy1 first policy
     * @param policy2 second policy (may be {@code null})
     * @return The location of a policy resource, or {@linkplain #NO_POLICY}.
     * @throws IOException if reading or writing a policy resource fails
     */
    private static String getEffectivePolicyResource(String policy1,
                                                     String policy2)
            throws IOException {
        String resource = policy1;
        if (!NO_POLICY.equals(resource)) {
            URL url1 = getResourceURL(policy1);
            resource = url1.toExternalForm();
            if (policy2 != null) {
                URL url2 = getResourceURL(policy2);
                // Don't use URL.equals - it blocks and goes onto the network.
                if (!url1.toExternalForm().equals(url2.toExternalForm())) {
                    resource = mergePolicies(url1, url2);
                }
            }
        }
        return resource;
    }

    /**
     * Returns a URL for the given policy resource.
     *
     * @param policy the policy resource
     * @return A {@code URL} denoting the policy resource.
     * @throws MalformedURLException if the resource string not a valid URL
     */
    private static URL getResourceURL(final String policy)
            throws MalformedURLException {
        URL url = BaseTestCase.getTestResource(policy);
        if (url == null) {
            // Assume the policy is expressed as an URL already, probably
            // as a file.
            url =  new URL(policy);
        }
        return url;       
    }

    /**
     * Merges the two specified policy resources (typically files), and writes
     * the combined policy to a new file.
     *
     * @param policy1 the first policy
     * @param policy2 the second policy
     * @return The resource location string for a policy file.
     * @throws IOException if reading or writing to one of the resources fails
     */
    private static String mergePolicies(URL policy1, URL policy2)
            throws IOException {
        // Create target directory for the merged policy files.
        String sytemHome =
                BaseTestCase.getSystemProperty("derby.system.home");
        File sysDir = new File(sytemHome == null ? "system" : sytemHome);
        File varDir = new File(sysDir, "var");
        // If running as the first test the system directory may not exist.
        // This situation looks a little bit suspicious - investigate?
        mkdir(sysDir);
        mkdir(varDir);

        // Read the contents of both policy files and write them out to
        // a new policy file. Construct a somewhat informative file name.
        final File mergedPF = new File(varDir,
                new File(policy2.getPath()).getName() +
                    "-MERGED_WITH-" +
                new File(policy1.getPath()).getName());
        OutputStream o =
                PrivilegedFileOpsForTests.getFileOutputStream(mergedPF);
        byte[] buf = new byte[1024];
        int read;
        InputStream i1 = openStream(policy1);
        while ((read = i1.read(buf)) != -1) {
            o.write(buf, 0, read);
        }
        i1.close();
        InputStream i2 = openStream(policy2);
        while ((read = i2.read(buf)) != -1) {
            o.write(buf, 0, read);
        }
        i2.close();
        o.close();
        try {
            return AccessController.doPrivileged(
                        new PrivilegedExceptionAction<String>() {
                    public String run() throws MalformedURLException {
                        return mergedPF.toURI().toURL().toExternalForm();
                    }
                });
        } catch (PrivilegedActionException pae) {
            throw (MalformedURLException)pae.getException();
        }
    }

    /** Opens the resource stream in a privileged block. */
    private static InputStream openStream(final URL resource)
            throws IOException {
        try {
            return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<InputStream>(){
                        public InputStream run() throws IOException {
                            return resource.openStream();
                        }
                    }
                );
        } catch (PrivilegedActionException pae) {
            throw (IOException)pae.getException();
        }
    }

    /** Creates the specified directory if it doesn't exist. */
    private static void mkdir(final File dir) {
        AccessController.doPrivileged(
            new PrivilegedAction<Void>() {
                public Void run() {
                    if (!dir.exists() && !dir.mkdir()) {
                        fail("failed to create directory: " + dir.getPath());
                    }
                    return null;
                }
            }
        );
    }

    /** Prints a debug message if debugging is enabled. */
    private static void println(String msg) {
        BaseTestCase.println("{SecurityManagerSetup} " + msg);
    }
}
