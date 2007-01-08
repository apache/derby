/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.UpgradeRun

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.util.Properties;

import junit.extensions.TestSetup;
import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Utility class for creating a set of tests that
 * comprise a complete upgrade run through all
 * five phases. This includes handling the
 * class loading for the old jar files
 * and setting up the configuration to
 * use a single use database and DataSource for
 * connections.
 *
 */
class UpgradeRun {
    private static final String[] jarFiles = {
            "derby.jar", 
            //"derbynet.jar",
            //"derbyclient.jar",
            //"derbytools.jar"
            };
    
    private static String getTextVersion(int[] iv)
    {
        String version = iv[0] + "." + iv[1] +
        "." + iv[2] + "." + iv[3];
        return version;
    }

    public final static Test suite(final int[] version) {
        
        
        ClassLoader oldLoader = (ClassLoader )AccessController.doPrivileged
        (new java.security.PrivilegedAction(){

            public Object run(){
            return createClassLoader(version);

            }

        }
         );
        
        // If no jars then just skip.
        if (oldLoader == null)
        {
            TestSuite suite = new TestSuite(
                    "Empty: Skipped upgrade Tests (no jars) for " + getTextVersion(version));
            return suite;
        }
        
        
        TestSuite suite = new TestSuite(
                "Upgrade Tests from " + getTextVersion(version));

        
        for (int phase = 0;
              phase < UpgradeChange.PHASES.length; phase++)
        {
            ClassLoader loader = null;
            switch (phase)
            {
            case UpgradeChange.PH_CREATE:
            case UpgradeChange.PH_POST_SOFT_UPGRADE:
            case UpgradeChange.PH_POST_HARD_UPGRADE:
                loader = oldLoader;
                break;
            case UpgradeChange.PH_SOFT_UPGRADE:
            case UpgradeChange.PH_HARD_UPGRADE:
                break;
                
            }
            Test phaseTests = baseSuite(
                    "Upgrade Phase: " + UpgradeChange.PHASES[phase],
                    phase);
            
            suite.addTest(new PhaseChanger(phaseTests, phase, loader, version));
        }
          
        TestSetup setup = TestConfiguration.singleUseDatabaseDecorator(suite);
        
        Properties preReleaseUpgrade = new Properties();
        preReleaseUpgrade.setProperty(
                "derby.database.allowPreReleaseUpgrade", "true");
        
        setup = new SystemPropertyTestSetup(setup, preReleaseUpgrade);
   
        return SecurityManagerSetup.noSecurityManager(setup);
    }
    
    private static Test baseSuite(String name, int phase) {
        TestSuite suite = new TestSuite(name);
          
        // No connection is expected in the post hard upgrade
        // phase, so don't bother adding test fixtures.
        if (phase != UpgradeChange.PH_POST_HARD_UPGRADE)
        {
            suite.addTest(BasicSetup.suite());
            
            suite.addTest(Changes10_1.suite());
            suite.addTest(Changes10_2.suite());
            suite.addTest(Changes10_3.suite());
        }
                
        return TestConfiguration.connectionDSDecorator(suite);
    }
    
    /**
     * Get the location of jars of old release. The location is specified 
     * in the property "derbyTesting.jar.path".
     *  
     * @return location of jars of old release
     */
    private static String getOldJarLocation(int[] oldVersion) {
        String jarPath = System.getProperty("derbyTesting.jar.path");
        
        Assert.assertNotNull("derbyTesting.jar.path not set", jarPath);
      
        String version = getTextVersion(oldVersion);
        String jarLocation = jarPath + File.separator + version
            + File.separator + "lib";
                
        return jarLocation;
    }
    
    /**
     * Create a class loader using jars in the specified location. Add all jars 
     * specified in jarFiles and the testing jar.
     * 
     * @param jarLoc Location of jar files
     * @return class loader
     */
    private static ClassLoader createClassLoader(int[] version)
    {
        URL[] url = new URL[jarFiles.length];
        
        String jarLocation = getOldJarLocation(version);
        
        File lib = new File(jarLocation);
        
        // If the jars do not exist then return null
        // and the caller will set up to skip this.
        if (!lib.exists())
            return null;
        
        
        for (int i=0; i < jarFiles.length; i++) {
            try {
                url[i] = new File(lib, jarFiles[i]).toURL();
            } catch (MalformedURLException e) {
                Assert.fail(e.toString());
            }
        }
        
        // Specify null for parent class loader to avoid mixing up 
        // jars specified in the system classpath
        return new URLClassLoader(url, null);       
    }
}
