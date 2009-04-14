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
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.util.Properties;

import junit.extensions.TestSetup;
import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest;
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
class UpgradeRun extends UpgradeClassLoader
{
    
    /**
     * Set of additional databases for tests that
     * require a one-off database. The additional
     * database decorator wraps all the tests and phases.
     * They are only created if a test opens a
     * connection against them. In hard upgrade the test
     * must explictly upgrade the database.
     * The databases are shutdown at the end of each phase, unless
     * "NoShutDown" is specified. The latter is used by databases
     * which need sqlAuthorization (specified by test). This thwarts
     * normal shutdown since credentials are required so shutdown is
     * done in test, not by the tearDown methods. See
     * Changes10_4#testSQLRoles for example.
     */

    static final AdditionalDb[] ADDITIONAL_DBS = {
        new AdditionalDb("COLLATED_DB_10_3", true), // db with territory
                                                    // based collation
        new AdditionalDb("NO_ENCRYPT_10_2", true),
        new AdditionalDb("ENCRYPT_10_2",  true),
        new AdditionalDb("ROLES_10_5", false)
    };
    
    public final static Test suite(final int[] version) {
        
        ClassLoader oldLoader = makeClassLoader( version );
        
        // If no jars then just skip.
        if (oldLoader == null)
        {
            TestSuite suite = new TestSuite(
                    "Empty: Skipped upgrade Tests (no jars) for " + getTextVersion(version));
            return suite;
        }
        

        TestSuite suite = new TestSuite(
                "Upgrade Tests from " + getTextVersion(version));
        BaseTestCase.traceit("Prepare to run upgrade tests from " + getTextVersion(version));

        
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
            Test phaseTests = baseSuite(getTextVersion(version)
                    + " Upgrade Phase: " + UpgradeChange.PHASES[phase] + " ",
                    phase, version);
            
            Test phaseSet = new PhaseChanger(phaseTests, phase, loader, version);
            phaseSet = handleJavaSE6(phase, version, phaseSet);
            suite.addTest(phaseSet);
        }
          
        TestSetup setup = TestConfiguration.singleUseDatabaseDecorator(suite);
        
        for (int i = 0; i < ADDITIONAL_DBS.length; i++)
        {
            if (ADDITIONAL_DBS[i].shutDown) {
                setup = TestConfiguration.additionalDatabaseDecorator(
                    setup, ADDITIONAL_DBS[i].logicalName);
            } else {
                setup = TestConfiguration.additionalDatabaseDecoratorNoShutdown(
                    setup, ADDITIONAL_DBS[i].logicalName);
            }
        }
        
        Properties preReleaseUpgrade = new Properties();
        preReleaseUpgrade.setProperty(
                "derby.database.allowPreReleaseUpgrade", "true");
        
        setup = new SystemPropertyTestSetup(setup, preReleaseUpgrade);
   
        return SecurityManagerSetup.noSecurityManager(setup);
    }
    
    /**
     * Add the tests from the various Changes classes (sub-classes
     * of UpgradeChange) to the base suite which corresponds to
     * a single phase of a run against an old database version.
     * <BR>
     * Changes are only added if the old version is older than
     * then version the changes represent. Thus Changes10_2
     * is not added if the old database (upgrade from) is already
     * at 10.2, since Changes10_2 is intended to test upgrade
     * from an older version to 10.2.
     * <BR>
     * This is for two reasons:
     * <OL>
     * <LI> Prevents an endless increase in number of test
     * cases that do no real testing. 
     * <LI> Simplifies test fixtures by allowing them to
     * focus on cases where testing is required, and not
     * handling all future situations.
     * </OL>
     * 
     */
    private static Test baseSuite(String name, int phase, int[] version) {
        TestSuite suite = new TestSuite(name);
        
        int oldMajor = version[0];
        int oldMinor = version[1];

        // No connection is expected in the post hard upgrade
        // phase, so don't bother adding test fixtures.
        if (phase != UpgradeChange.PH_POST_HARD_UPGRADE)
        {
            suite.addTest(BasicSetup.suite());
            
            if (oldMajor == 10) {
                if (oldMinor < 1)
                    suite.addTest(Changes10_1.suite());
                if (oldMinor < 2)
                   suite.addTest(Changes10_2.suite());
                if (oldMinor < 3) {
                   //Pass the phase as a parameter to the
                   //suite method that will enable the test to add existing
                   //junit tests after checking for the phase of the current
                   //run. 
                   suite.addTest(Changes10_3.suite(phase));
                }
                if (oldMinor < 4)
                	suite.addTest(Changes10_4.suite(phase));
                if (oldMinor < 5)
                	suite.addTest(Changes10_5.suite(phase));
            }
            
            // Add DatabaseMetaData tests. Since metadata
            // queries may be changed by an upgrade it is
            // an area that is subject to bugs. Here we run
            // all or a subset of DatabaseMetaData tests
            // as required.
            switch (phase) {
            case UpgradeChange.PH_CREATE:
                // No need to test, should have been covered
                // by the original tests of the old release 
                break;
                
            case UpgradeChange.PH_POST_SOFT_UPGRADE:
                // reverted to old engine and metadata queries
                // must continue to work. Cannot run the full
                // set of tests here as the full DatabaseMetaDataTest
                // functionality may not match the old engine.
                // However we run individual fixtures that exercise
                // the code path for the metadata queries.
                //
                // Any specific change to the metadata queries
                // due to upgrade (e.g. fixing a system catalog)
                // must be tested in a ChangesM_n fixture.
                break;
            
            // Running at the new level so the full functionality
            // of DatabaseMetaData should be available.
            case UpgradeChange.PH_SOFT_UPGRADE:
            case UpgradeChange.PH_HARD_UPGRADE:
                RunDataBaseMetaDataTest(suite, oldMinor);
                break;
            }
        }
        else
        {
            suite.addTest(new BasicSetup("noConnectionAfterHardUpgrade"));
        }
                
        return TestConfiguration.connectionDSDecorator(suite);
    }
    
    
    /**
     * When running against certains old releases in Java SE 6
     * we need to setup the connections to the old
     * database to not use the specific JDBC 4
     * datasources (e.g. EmbeddedDataSource40).
     * (Since they don't exist in the old release).
     *
     */
    private static Test handleJavaSE6(int phase, int[] version, Test test)
    {
        // 
        // we need to tell the JUnit infratructure not to
        // look for the  40 datasources (e.g. EmbeddedDataSource40)
        boolean oldReleaseNeedsJDBC3 = false;
        switch (phase)
        {
        case UpgradeChange.PH_CREATE:
        case UpgradeChange.PH_POST_SOFT_UPGRADE:
            
            // Pre 10.2.2.0 need jdbc 3 drivers.
            if (version[0] == 10 && version[1] < 3)
            {
                if (version[1] < 2 || version[2] < 2)
                   oldReleaseNeedsJDBC3 = true;
            }
            break;
        default:
            break;
        }
        
        if (oldReleaseNeedsJDBC3) {
            return TestConfiguration.forceJDBC3Embedded(test);
        }
        return test;
    }
    
    // We want to run DatabaseMetaDataTest, but it includes some
    // features not supported in older versions, so we cannot just
    // add the DatabaseMetaDataTest.class as is.
    // Note also, that this does not execute fixture initialCompilationTest.
    private static void RunDataBaseMetaDataTest (TestSuite suite, int oldMinor)
    {
        Method[] methods = DatabaseMetaDataTest.class.getMethods();
        for (int i = 0; i < methods.length; i++) {
            Method m = methods[i];
            if (m.getParameterTypes().length > 0 ||
                    !m.getReturnType().equals(Void.TYPE)) {
                continue;
            }
            String name = m.getName();
            if (name.startsWith("test"))
            {
                if ((!(name.equals("testGetTablesModify") && oldMinor < 1)) &&
                   // these two tests will fail with versions before 10.2.1.6
                   // because of missing support for grant/revoke/privileges
                   (!(name.equals("testGetTablePrivileges") && oldMinor <2)) &&
                   (!(name.equals("testGetColumnPrivileges") && oldMinor <2)))
                    suite.addTest(new DatabaseMetaDataTest(name));
            }
        }
    }

}

class AdditionalDb
{
    final String logicalName;
    final boolean shutDown;
    public AdditionalDb(String logicalName, boolean shutDown)
    {
        this.logicalName = logicalName;
        this.shutDown  = shutDown;
    }
}
