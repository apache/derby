/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests._Suite

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

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Run the full upgrade suite. This is the only
 * way to run tests in this package.
 * A set of tests are run against a number of
 * previous releases, see the static OLD_VERSIONS
 * field in this class.
 * 
 * Each test against the old release consists of following phases:
   
    <OL>
    <LI> Create database with the <B>old</B> release.
    <LI> Boot the database with the <B>new</B> release in soft upgrade mode.
    Try to execute functionality that is not allowed in soft upgrade.
    <LI> Boot the database with the <B>old</B> release to ensure the
    database can be booted by the old release after soft upgrade.
    <LI> Boot the database with the <B>new</B> release in hard upgrade mode,
    specifying the upgrade=true attribute.
    <LI> Boot the database with the <B>old</B> release to ensure the
    database can not be booted by the old release after hard upgrade.
    </OL>
    The class PhaseChanger is the decorator that sets up the
    fixtures to run in a given phase.

    <P>
    The test fixtures themseleves are in JUnit test classes
    that are sub-classes of UpgradeChange. The set of fixtures
    in BasicSetup is general setup and the changes per release
    are in classes of the form Changes10_1 etc.
    
    <P>
    The class UpgradeRun hooks up the test fixtures for a set
    of runs against a single old release into a single suite.
    Each fixture is run multiple times, once per phase against
    each old release.
    
    @see UpgradeRun
    @see UpgradeChange
 */
public class _Suite extends BaseTestCase {
    
    /**
     * Property that indicates the location of the
     * old releases.
     */
    static final String OLD_RELEASE_PATH_PROPERTY =
        "derbyTesting.oldReleasePath";
    
    /**
     * The saved location in svn at apache for older releases for testing
     */
    static final String OLD_JAR_URL =
        "https://svn.apache.org/repos/asf/db/derby/jars";
    
    /**
     * List of the versions to test against.
     * The tests look for the jar files in each releasae
     * in the folder:
     * ${derbyTesting.oldReleasePath}/M.m.f.p
     * 
     * If derbyTesting.oldReleasePath is not set then it is assumed the files can
     * be accessed from the svn repository at apache. If this location is
     * not available, then the test will fail.
     * 
     * If the property is set, but ${derbyTesting.oldReleasePath}/M.m.f.p does not exist
     * for a specific release then those sets of tests will be skipped.
     * 
     * One can also set derbyTesting.oldReleasePath to a checked out
     * version of the jars from the Apache svn repo. E.g.
     * 
     * cd $HOME
     * mkdir derby_upgrade
     * cd derby_upgrade
     * svn co https://svn.apache.org/repos/asf/db/derby/jars
     * 
     * Then set derbyTesting.oldReleasePath as:
     *   -DderbyTesting.oldReleasePath=$HOME/derby_upgrade/jars
     * when running tests.
     */
    private static final int[][] OLD_VERSIONS =
    {
        {10, 0, 2, 1}, // 10.0.2.1 (incubator release)
        {10, 1, 1, 0}, // 10.1.1.0 (Aug 3, 2005 / SVN 208786)
        {10, 1, 2, 1}, // 10.1.2.1 (Nov 18, 2005 / SVN 330608)
        {10, 1, 3, 1}, // 10.1.3.1 (Jun 30, 2006 / SVN 417277)
        {10, 2, 1, 6}, // 10.2.1.6 (Oct 02, 2006 / SVN 452058)
        {10, 2, 2, 0}, // 10.2.2.0 (Dec 12, 2006 / SVN 485682)
    };

    /**
     * Use suite method instead.
     */
    private _Suite(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        TestSuite suite = new TestSuite("Upgrade Suite");       

        for (int i = 0; i < OLD_VERSIONS.length; i++) {
            // JSR169 support was only added with 10.1, so don't
            // run 10.0 to later upgrade if that's what our jvm is supporting.
            if (!(JDBC.vmSupportsJSR169() && 
                (OLD_VERSIONS[i][0]==10) && (OLD_VERSIONS[i][1]==0))); 
            suite.addTest(UpgradeRun.suite(OLD_VERSIONS[i]));
        }
        
        return suite;
    }
    

}
