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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
     * Property that gives the location of a file listing old versions 
     * to be tested.
     * To be able to override default values hard-coded for OLD_VERSIONS.
     */
     static final String OLD_VERSIONS_PATH_PROPERTY =
         "derbyTesting.oldVersionsPath";
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
        "http://svn.apache.org/repos/asf/db/derby/jars";
    
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

    private static OldVersions old;

    /**
     * Use suite method instead.
     */
    private _Suite(String name) {
        super(name);
    }
    
    public static Test suite() {
        String id = "Upgrade Suite: "
                + OLD_RELEASE_PATH_PROPERTY + "=" + UpgradeRun.jarPath
                + " / " + OLD_VERSIONS_PATH_PROPERTY + "=" + UpgradeRun.oldVersionsPath;
        TestSuite suite = new TestSuite(id);       
        
        if ( UpgradeRun.oldVersionsPath != null )
        {
            old = new OldVersions(UpgradeRun.oldVersionsPath);
        }
        OldVersions.show();

        for (int i = 0; i < old.VERSIONS.length; i++) {
            // JSR169 support was only added with 10.1, so don't
            // run 10.0 to later upgrade if that's what our jvm is supporting.
            if ((JDBC.vmSupportsJSR169() && 
                (old.VERSIONS[i][0]==10) && (old.VERSIONS[i][1]==0))) {
                traceit("Skipping 10.0 on JSR169");
                continue;
            }
            // Derby 10.3.1.4 does not boot on the phoneME advanced platform,
            // (see DERBY-3176) so don't run upgrade tests in this combination.
            if ( System.getProperty("java.vm.name").equals("CVM")
                  && System.getProperty("java.vm.version").startsWith("phoneme")
                  && old.VERSIONS[i][0]==10 && old.VERSIONS[i][1]==3 
                  && old.VERSIONS[i][2]==1 && old.VERSIONS[i][3]==4 ) {
                traceit("Skipping 10.3.1.4 on CVM/phoneme");
                continue;
            }
            suite.addTest(UpgradeRun.suite(old.VERSIONS[i]));
        }

        return suite;
    }
    

    private static class OldVersions{

        private static int[][] VERSIONS =
          {
            {10, 0, 2, 1}, // 10.0.2.1 (incubator release)
            {10, 1, 1, 0}, // 10.1.1.0 (Aug 3, 2005 / SVN 208786)
            {10, 1, 2, 1}, // 10.1.2.1 (Nov 18, 2005 / SVN 330608)
            {10, 1, 3, 1}, // 10.1.3.1 (Jun 30, 2006 / SVN 417277)
            {10, 2, 1, 6}, // 10.2.1.6 (Oct 02, 2006 / SVN 452058)
            {10, 2, 2, 0}, // 10.2.2.0 (Dec 12, 2006 / SVN 485682)
            {10, 3, 1, 4}, // 10.3.1.4 (Aug 1, 2007 / SVN 561794)
            {10, 3, 3, 0}, // 10.3.3.0 (May 12, 2008 / SVN 652961)
            {10, 4, 1, 3}, // 10.4.1.3 (April 24, 2008 / SVN 648739)
            {10, 4, 2, 0}, // 10.4.2.0 (September 05, 2008 / SVN 693552)
          };
        private OldVersions(String oldVersionsPath)
        {
            BufferedReader br = null;
            try{
                FileReader fr = new FileReader(oldVersionsPath);
                br = new BufferedReader(fr);
            }
            catch (java.io.FileNotFoundException fNFE)
            {
                alarm("File '" + oldVersionsPath 
                        + "' was not found, using default old versions for upgrade tests.");
                return;
            }
            traceit("Run upgrade tests on versions defined in '" + oldVersionsPath + "'");
            getVersions(br, oldVersionsPath);
        }

        private void getVersions(BufferedReader br, String oldVersionsPath) 
        {
            VERSIONS = new int[256][4];

            int versions = 0;

            String line = null;
            int lineNum = 0;
            try {
                while ((line = br.readLine()) != null) {
                    lineNum++;
                    /* Ignore lines not matching the regexp: "^[\\d]+\\.[\\d]+\\.[\\d]+\\.[\\d]"
                     * NB. java.util.regex.Matcher and java.util.regex.Pattern can not be
                     * used on small devices(JSR219).
                     */
                    try {
                        String[] parts = split4(line,'.');
                        // String[] parts = line.split("\\."); // JSR219 does NOT have String.split()!
                        if (parts.length >= 3) {

                            int[] vstr = new int[4];
                            for (int i = 0; i < 4; i++) // Using first 4 values
                            {
                                String str = parts[i];
                                if (i == 3) { // Clean... remove trailing non-digits
                                    str = clean(str,"0123456789");
                                }
                                vstr[i] = Integer.parseInt(str);
                            }
                            VERSIONS[versions++] = vstr;
                        } else {
                            alarm("Illegal version format on: " + line);
                        }
                    } catch (NumberFormatException nfe) {
                        alarm("NumberFormatException on line " + lineNum + ": " + line + ": " + " " + nfe.getMessage());
                    } catch (ArrayIndexOutOfBoundsException aie) {
                        alarm("ArrayIndexOutOfBoundsException on line " + lineNum + ": " + line + ": " + " " + aie.getMessage());
                    }
                }
            } catch (IOException ioe) {
                alarm("Error reading from file: " + oldVersionsPath + ioe.getMessage());
            }
            
            int[][] finalVERSIONS = new int[versions][4];
            for (int v = 0; v < versions; v++) {
                finalVERSIONS[v] = VERSIONS[v];
            }
            VERSIONS = finalVERSIONS;

        }

        private static void show() {
            traceit("Upgrade test versions listed:");
            for (int o = 0; o < VERSIONS.length; o++) {
                String ver = "";
                for (int i = 0; i < VERSIONS[o].length; i++) {
                    if (i == 0) {
                        ver = "" + VERSIONS[o][i];
                    } else {
                        ver = ver + "." + VERSIONS[o][i];
                    }
                }
                traceit(ver);
            }
        }
        private static String[] split4(String l, char c)
        {
            String[] res = new String[4];
            try{
            int p0 = l.indexOf(c);
            if (p0<0) return res;
            
            res[0] = l.substring(0, p0);
            int p1 = l.indexOf(c,p0+1);
            if (p1<0) return res;
            
            res[1] = l.substring(p0+1, p1);
            int p2 = l.indexOf(c,p1+1); 
            if (p2<0) return res;
            
            res[2] = l.substring(p1+1, p2);
            int p3 = l.indexOf(c,p2+1); 
            if (p3<0) p3=l.length();
            
            res[3] = l.substring(p2+1, p3);
            
            } catch(StringIndexOutOfBoundsException sie){
                println("split4 StringIndexOutOfBoundsException: "+sie.getMessage());
                sie.printStackTrace();
            }
            return res;
        }
        private static String clean(String l, String allowed)
        {
            for (int i=0;i<l.length();i++)
            {
                if (!matches(l.charAt(i),allowed))
                {
                    return l.substring(0,i);
                }
            }
            return l;
        }
        private static boolean matches(char c, String allowed)
        {
            for (int j=0;j<allowed.length();j++)
            {
                if (allowed.charAt(j) == c) return true;
            }
            return false;
        }
        
    }
}
