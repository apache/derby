/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.VersionCombinationConfigurator

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.compatibility;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URI;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.extensions.TestSetup;
import junit.framework.TestSuite;

import org.apache.derby.tools.sysinfo;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.DerbyDistribution;
import org.apache.derbyTesting.junit.DerbyVersion;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Generates a set of client-server combinations to run the compatibility
 * tests for.
 * <p>
 * Due to the requirement for running with a variety of Derby versions, the
 * compatibility test suite is run as multiple processes. The test is
 * controlled from the main process (the process in which the test/suite is
 * started), and this process spawns additional processes for each server
 * version and each client version. In some cases it also has to spawn
 * additional processes to accomplish other tasks.
 * <p>
 * For development purposes the default MATS suite is sufficient for ongoing
 * work. Eventually, and at least before cutting a new release, the full
 * development suite should be run, since it will test the trunk against all
 * previous releases. The other suites will test old releases against each
 * other, and as such they are of less interest since the old releases don't
 * change. Note however that these suites can be used to test releases on
 * branches where this version of the compatibility test doesn't exist (just
 * add the JARs to the release repository and configure includes or excludes
 * to suite your needs).
 * <p>
 * <strong>NOTE 1</strong>: The set of combinations computed by this class
 * depends on the number of old releases available on the local computer. If
 * there are no old releases available a warning will be emitted, but the test
 * won't fail (it will test trunk vs trunk).
 * <p>
 * <strong>NOTE 2</strong>: trunk is defined as a distribution, although it
 * hasn't been released yet. The reason is simple: we always want to test trunk
 * for incompatibilities against older versions.
 */
public class VersionCombinationConfigurator {

    /** Name of the configuration, only used for informational purposes. */
    private final String name;
    /** Decides if combinations have to involve trunk (as server or client). */
    private final boolean limitToTrunk;
    /** Decides if only the latest branch release is eligible for inclusion. */
    private final boolean newestFixpackOnly;
    private List<DerbyVersion> toInclude = Collections.emptyList();
    private List<DerbyVersion> toExclude = Collections.emptyList();

    /**
     * Returns the default configuration intended to be run as part of
     * <tt>suites.all</tt>, which is a kind of minimal acceptance test (MATS).
     * <p>
     * The default configuration is defined to be all combinations that have
     * trunk as either the server or the client.
     *
     * @return A configurator generating the default set of tests.
     */
    public static VersionCombinationConfigurator getInstanceDevMATS() {
        return new VersionCombinationConfigurator(
                "default/MATS configuration", true, true);
    }

    /**
     * Returns a configuration that will test trunk against all other available
     * releases.
     *
     * @return A configurator generating the default set of tests.
     */
    public static VersionCombinationConfigurator getInstanceDevFull() {
        return new VersionCombinationConfigurator(
                "full development configuration", true, false);
    }

    /**
     * Returns a configuration where the newest releases within each
     * major-minor version are tested against each other.
     * <p>
     * Given releases designated <tt>M.m.f.p</tt> (i.e. 10.8.1.2), this
     * configuration will include all major-minor releases with the highest
     * <ff>f</ff>.
     *
     * @return A configurator generating a reasonably sized test set.
     */
    public static VersionCombinationConfigurator getInstanceOld() {
        return new VersionCombinationConfigurator(
                "historical configuration", false, true);
    }

    /**
     * Returns a configuration where all versions found are tested against
     * each other.
     *
     * @return  A configurator generating the full set of tests.
     */
    public static VersionCombinationConfigurator getInstanceOldFull() {
        return new VersionCombinationConfigurator(
                "full historical configuration", false, false);
    }

    /**
     * Creates a version combination configurator.
     *
     * @param name name of the configurator
     * @param limitToTrunk if true, only add combinations including trunk
     * @param newestFixpackOnly whether or not to only include the newest
     *      release within each pair of major-minor version.
     */
    private VersionCombinationConfigurator(String name,
                                           boolean limitToTrunk,
                                           boolean newestFixpackOnly) {
        this.name = name;
        this.limitToTrunk = limitToTrunk;
        this.newestFixpackOnly = newestFixpackOnly;
    }

    public String getName() {
        return name;
    }

    /**
     * Adds compatibility tests to the specified suite.
     * <p>
     * The following determines how many tests are added:
     * <ul> <li>available distributions locally (release repository)</li>
     *      <li>list of includes and/or excludes (by default empty)</li>
     *      <li>the configurator's current settings</li>
     * </ul>
     *
     * @param suite the suite to add the tests to
     * @return Number of compatibility runs added.
     */
    public int addTests(TestSuite suite) {
        int runsAdded = 0;
        List<DerbyDistribution> dists = filterVersions();
        DerbyDistribution newestDist = dists.get(0);
        String newestTestingCode = newestDist.getTestingClasspath();
        // Generate a list of all the combinations.
        for (DerbyDistribution server : dists) {
            DerbyVersion serverVersion = server.getVersion();

            // Check if testing of this server version should be skipped.
            if (skipServerVersion(serverVersion)) {
                continue;
            }

            TestSuite clientSuites = new TestSuite(
                    "Client runs against server " + serverVersion.toString());
            for (DerbyDistribution client : dists) {
                if (limitToTrunk && !server.equals(newestDist) &&
                        !client.equals(newestDist)) {
                    continue;
                }
                clientSuites.addTest(
                        new ClientCompatibilityRunControl(
                                    client, newestTestingCode, serverVersion));
                runsAdded++;
            }
            TestSetup setup = new VersionedNetworkServerTestSetup(
                    clientSuites, server, newestTestingCode);
            suite.addTest(setup);
        }
        return runsAdded;
    }

    public void setIncludes(List<DerbyVersion> toInclude) {
        if (toInclude != null) {
            this.toInclude = toInclude;
        }
    }

    public void setExcludes(List<DerbyVersion> toExclude) {
        if (toExclude != null) {
            this.toExclude = toExclude;
        }
    }

    /**
     * Check if a certain server version should be skipped due to bugs that
     * prevent it from working in the current environment.
     *
     * @param version the server version to check
     * @return {@code true} if the specified version should be skipped, or
     * {@code false} otherwise
     */
    private boolean skipServerVersion(DerbyVersion version) {

        // DERBY-6098: Skip testing of server versions less than 10.10 if
        // the JVM doesn't support JNDI. Earlier versions of the server don't
        // accept connections if JNDI is not present.
        if (!JDBC.vmSupportsJNDI() && version.lessThan(DerbyVersion._10_10)) {
            println("Server version " + version + " was skipped because " +
                    "it requires JNDI to run.");
            return true;
        }

        // Default: don't skip
        return false;
    }

    /**
     * Filters Derby distributions available in the distribution repository.
     *
     * @return A list of available and accepted Derby distributions.
     */
    private List<DerbyDistribution> filterVersions() {
        DerbyDistribution[] dists =
                TestConfiguration.getReleaseRepository().getDistributions();
        List<DerbyDistribution> qualifiedDists =
                new ArrayList<DerbyDistribution>();
        for (DerbyDistribution dist: dists) {
            // Handle includes and excludes.
            DerbyVersion version = dist.getVersion();
            if (!toInclude.isEmpty() && !toInclude.contains(version)) {
                println(version.toString() + " not in include list");
                continue;
            }
            if (!toExclude.isEmpty() && toExclude.contains(version)) {
                println(version.toString() + " in exclude list");
                continue;
            }

            qualifiedDists.add(dist);
        }
        // If there are no qualified old distributions at this point, sound the
        // alarm as we're probably looking at a misconfiguration.
        if (qualifiedDists.isEmpty()) {
            alarm("No old releases found for current configuration/environment");
        }

        // Now add the version we are running off.
        DerbyDistribution runningDist = getRunningDistribution();
        if (!qualifiedDists.contains(runningDist)) {
            qualifiedDists.add(runningDist);
        }
        qualifiedDists = sortAndFilterVersions(qualifiedDists);

        println("--- " + qualifiedDists.size() + " distributions qualified");
        for (DerbyDistribution d : qualifiedDists) {
            println(d.getVersion().toString());
        }

        return qualifiedDists;
    }

    /**
     * Returns the running distribution, which is typically trunk.
     *
     * @return Information about the running distribution.
     * @throws IllegalArgumentException if parsing the version string fails, or
     *      if trunk is run off the classes directory
     */
    private DerbyDistribution getRunningDistribution() {
        File libDir = new File(getClassURI(getClass()));
        if (libDir.isDirectory()) {
            throw new IllegalStateException("only running off jars is " +
                    "supported, currently running off " + libDir);
        }
        // Get the directory the JAR file is living in.
        libDir = libDir.getParentFile();
        DerbyVersion version = DerbyVersion.parseVersionString(
                sysinfo.getVersionString());
        DerbyDistribution dist = DerbyDistribution.getInstance(libDir, version);
        if (dist == null) {
            throw new IllegalStateException(
                    "failed to get running distribution (programming error?)");
        }
        return dist;
    }

    /**
     * Sorts and filters out distributions based on the configurator settings.
     *
     * @param distributions list of distributions to filter
     * @return A filtered list of distributions.
     */
    private List<DerbyDistribution> sortAndFilterVersions(
            List<DerbyDistribution> distributions) {
        // Sort the releases based on the version number (highest first).
        Collections.sort(distributions);
        Collections.reverse(distributions);

        DerbyDistribution prev = null;
        if (newestFixpackOnly) {
            List<DerbyDistribution> filtered =
                    new ArrayList<DerbyDistribution>();
            for (DerbyDistribution d : distributions) {
                DerbyVersion ver = d.getVersion();
                if (prev == null || prev.getVersion().greaterMinorThan(ver)) {
                    filtered.add(d);
                } else {
                    println("ignored " + ver.toString() +
                            ", not the newest fixpack version for " +
                            ver.getMajor() + "." + ver.getMinor());
                }
                prev = d;
            }
            distributions = filtered;
        }
        return distributions;
    }

    /**
     * Returns the URI of the source for the specified class.
     *
     * @param cl class to find the source for
     * @return A {@code URI} pointing to the source, or {@code null} it cannot
     *      be obtained.
     */
    static URI getClassURI(final Class cl) {
        return AccessController.doPrivileged(new PrivilegedAction<URI>() {
            public URI run() {
                CodeSource cs = cl.getProtectionDomain().getCodeSource();
                if (cs != null) {
                    try {
                        return cs.getLocation().toURI();
                    } catch (URISyntaxException use) {
                        // Shouldn't happen, fall through and return null.
                        BaseTestCase.alarm("bad URI: " + use.getMessage());
                    }
                }
                return null;
            }
        });
    }

    // Forwarding convenience methods

    private static void println(String msg) {
        BaseTestCase.println(msg);
    }

    private static void alarm(String msg) {
        BaseTestCase.alarm(msg);
    }
}
