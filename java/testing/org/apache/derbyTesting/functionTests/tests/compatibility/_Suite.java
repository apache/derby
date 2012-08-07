/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility._Suite

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DerbyVersion;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.ServerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Runs a minimal set of compatibility tests aimed at discovering
 * incompatibilities introduced in the latest development version (trunk).
 * <p>
 * Only combinations where trunk is the client or the server qualify for the
 * MATS (Minimal Acceptance Test Suite), and only the latest releases from
 * older branches are tested. For more coverage see
 * {@link VersionCombinationConfigurator#getInstanceDevFull()}.
 */
public class _Suite
        extends BaseJDBCTestCase {

    /** Property for specifying versions to include. */
    static final String INCLUDE_RELEASES =
            "derby.tests.compat.includeReleases";

    /** Property for specifying versions to exclude. */
    static final String EXCLUDE_RELEASES =
            "derby.tests.compat.excludeReleases";

    /** Lazily set in {@linkplain #addVersionCombinations}, or by a subclass. */
    protected static VersionCombinationConfigurator configurator;

    /**
     * Adds compatibility tests to the specified suite according to the
     * current version combination configuration.
     *
     * @param suite the suite to add the tests to
     * @return The number of tests added.
     */
    private static int addVersionCombinations(TestSuite suite) {
        String include = getSystemProperty(INCLUDE_RELEASES);
        String exclude = getSystemProperty(EXCLUDE_RELEASES);
        List<DerbyVersion> toInclude = parseVersionList(include);
        List<DerbyVersion> toExclude = parseVersionList(exclude);

        if (configurator == null) {
            // MATS = minimal acceptance test suite
            configurator = VersionCombinationConfigurator.getInstanceDevMATS();
        }
        suite.setName("Compatibility suite, " + configurator.getName());
        configurator.setIncludes(toInclude);
        configurator.setExcludes(toExclude);
        return configurator.addTests(suite);
    }

    /**
     * Parses the list of version strings and returns a list of version objects.
     * <p>
     * <strong>NOTE</strong>: If invalid versions are found a warning is simply
     * printed to the console.
     *
     * @param versions list of Derby versions, i.e '10.8.1.2,10.7.1.1'
     * @return A list of parsed Derby versions.
     */
    private static List<DerbyVersion> parseVersionList(String versions) {
        if (versions == null || versions.length() == 0) {
            return Collections.emptyList();
        }
        String[] vlist = versions.split(",");
        List<DerbyVersion> ret = new ArrayList<DerbyVersion>(vlist.length);
        for (String v : vlist) {
            try {
                ret.add(DerbyVersion.parseVersionString(v));
            } catch (IllegalArgumentException iae) {
                alarm("badly formatted version string: " + v);
            }
        }
        return ret;
    }

    /** Don't use this.  @see #suite() */
    public _Suite(String name) {
        super(name);
        throw new IllegalStateException("invoke suite() instead");
    }

    /**
     * Returns the default set of compatibility tests, intended to be run
     * as part of suites.All.
     *
     * @return A default suite of compatibility tests.
     */
    public static Test suite() {
        // DERBY-5889: Disabling tests on Windonws while investigating.
        if (isWindowsPlatform()) {
            return new TestSuite(
                    "tests.compatibilty disabled on Windows, see DERBY-5889");
        }
        TestSuite suite = new TestSuite();
        addVersionCombinations(suite);
        TestConfiguration config = TestConfiguration.getCurrent();
        return new SecurityManagerSetup(
                new ServerSetup(suite, "localhost", config.getPort()),
                // Need permission for getProtectionDomain to determine what
                // to put on the classpath for the spawned process(es).
                VersionCombinationConfigurator.class.getName().
                    replaceAll("\\.", "/") + ".policy",
                true);
    }
}
