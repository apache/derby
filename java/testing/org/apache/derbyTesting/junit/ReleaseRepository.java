/*

   Derby - Class org.apache.derbyTesting.junit.ReleaseRepository

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
package org.apache.derbyTesting.junit;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;

/**
 * A repository for Derby releases.
 * <p>
 * The releases are used by tests, for instance by the upgrade and compatibility
 * tests, to verify characteristics and behavior across Derby releases.
 * <p>
 * This particular repository is rather dumb - it is up to the user to keep the
 * repository content updated. The repository layout is based on the layout of
 * the SVN repository for releases at
 * {@code https://svn.apache.org/repos/asf/db/derby/jars}. This means there will
 * be a directory for each release, where the directory name is the release
 * version. Inside this directory, all the distribution JARs can be found.
 * <p>
 * The repository location defaults to {@code $HOME/.derbyTestingReleases} on
 * UNIX-like systems, and to {@code %UserProfile%\.derbyTestingReleases} on
 * Windows (in Java, both of these maps to the system property 'user.home').
 * The location can be overridden by specifying the system property
 * {@code derbyTesting.oldReleasePath}.
 * <p>
 * If the default location doesn't exist, and the system property
 * {@code derbyTesting.oldReleasePath} is unspecified, it is up to the tests
 * using the release repository to decide if this condition fails the test or
 * not. If the system property is set to a non-existing directory an exception
 * will be thrown when instantiating the repository.
 * <p>
 * The repository is lazily initialized, as there's no reason to incur the
 * initialization cost when running tests that don't require the repository.
 * The disk is inspected only when the repository is instantiated, any updates
 * to the on-disk repository after the repository has been instantiated will
 * not take effect.
 * <p>
 * <em>Implementation note</em>: This code should be runnable with J2ME, which
 * means that it needs to be compatible with J2SE 1.4 for the time being.
 */
public class ReleaseRepository {

    /**
     * The property used to override the location of the repository. The name
     * is used for compatibility reasons.
     */
    private static final String OVERRIDE_HOME_PROP =
            "derbyTesting.oldReleasePath";
    private static final File DEFAULT_HOME;
    static {
        String home = BaseTestCase.getSystemProperty("user.home");
        DEFAULT_HOME = new File(home, ".derbyTestingReleases");
    }

    /** The repository instance. */
    private static ReleaseRepository repos;

    /**
     * Returns the release repository object.
     * <p>
     * The release repository will be built from a default directory, or
     * from the directory specified by the system property
     * {@code derbyTesting.oldReleasePath}.
     *
     * @return The release repository object.
     */
    public static synchronized ReleaseRepository getInstance()
            throws IOException {
        if (repos == null) {
            File location = DEFAULT_HOME;
            String overrideLoc = BaseTestCase.getSystemProperty(
                    OVERRIDE_HOME_PROP);
            if (overrideLoc != null) {
                location = new File(overrideLoc);
                if (!PrivilegedFileOpsForTests.exists(location)) {
                    throw new IOException("the specified Derby release " +
                        "repository doesn't exist: " + location.getPath());
                }
            }
            repos = new ReleaseRepository(location);
            repos.buildDistributionList();
        }
        return repos;
    }

    /** The repository location (on disk). */
    private final File reposLocation;
    /**
     * List of distributions found in the repository. If {@code null}, the
     * repository hasn't been initialized.
     */
    private List dists;

    /**
     * Creates a new, empty repository.
     *
     * @param reposLocation the location of the repository contents
     * @see #buildDistributionList()
     */
    private ReleaseRepository(File reposLocation) {
        this.reposLocation = reposLocation;
    }

    /**
     * Returns the list of distributions in the repository.
     *
     * @return A sorted list of Derby distributions, with the newest
     *      distribution at index zero, or an empty list if there are no
     *      distributions in the repository.
     */
    public DerbyDistribution[] getDistributions() {
        DerbyDistribution[] clone = new DerbyDistribution[dists.size()];
        dists.toArray(clone);
        return clone;
    }

    private void buildDistributionList() {
        if (dists != null) {
            throw new IllegalStateException("repository already initialized");
        }

        File[] tmpCandDists = reposLocation.listFiles(new FileFilter() {

            public boolean accept(File pathname) {
                if (!pathname.isDirectory()) {
                    return false;
                }
                String name = pathname.getName();
                // Stay away from regexp for now (JSR169).
                // Allow only digits and three dots ("10.8.1.2")
                int dots = 0;
                for (int i=0; i < name.length(); i++) {
                    char ch = name.charAt(i);
                    if (ch == '.') {
                        dots++;
                    } else if (!Character.isDigit(ch)) {
                        return false;
                    }
                }
                return dots == 3;
            }
        });
        if (tmpCandDists == null) {
            tmpCandDists = new File[0];
        }
        traceit("{ReleaseRepository} " + tmpCandDists.length +
                " candidate releases at " + reposLocation);

        dists = new ArrayList(tmpCandDists.length);
        for (int i=0; i < tmpCandDists.length; i++) {
            File dir = tmpCandDists[i];
            // We extract the version from the directory name.
            // We can also extract it by running sysinfo if that turns out to
            // be necessary.
            // From the check in the FileFilter we know we'll get four
            // components when splitting on dot.
            String[] comp = Utilities.split(dir.getName(), '.');
            DerbyVersion version;
            try {
                version = new DerbyVersion(
                        Integer.parseInt(comp[0]),
                        Integer.parseInt(comp[1]),
                        Integer.parseInt(comp[2]),
                        Integer.parseInt(comp[3]));
            } catch (NumberFormatException nfe) {
                traceit("skipped distribution, invalid version: " +
                        dir.getAbsolutePath());
                continue;
            }
            DerbyDistribution dist = DerbyDistribution.getInstance(
                    dir, version);
            // TODO: 10.0.1.2 is considered invalid because it doesn't have a
            //       a client JAR. Accept, ignore, or warn all the time?
            if (dist == null) {
                traceit("skipped invalid distribution: " +
                        dir.getAbsolutePath());
            } else {
                dists.add(dist);
            }
        }
        filterDistributions(dists);
        Collections.sort(dists);
        dists = Collections.unmodifiableList(dists);
    }

    /**
     * Filters out distributions that cannot be run in the current environment
     * for some reason.
     * <p>
     * The reason for getting filtered out is typically due to lacking
     * functionality or a bug in a specific Derby distribution.
     *
     * @param dists the list of distributions to filter (modified in-place)
     */
    private void filterDistributions(List dists) {
        // Specific version we want to filter out in some situations.
        DerbyVersion jsr169Support = DerbyVersion._10_1;
        DerbyVersion noPhoneMEBoot = DerbyVersion._10_3_1_4;

        for (int i=dists.size() -1; i >= 0; i--) {
            DerbyDistribution dist = (DerbyDistribution)dists.get(i);
            DerbyVersion distVersion = dist.getVersion();
            // JSR169 support was only added with 10.1, so don't
            // run 10.0 to later upgrade if that's what our jvm is supporting.
            if (JDBC.vmSupportsJSR169() &&
                    distVersion.lessThan(jsr169Support)) {
                println("skipping " + distVersion.toString() + " on JSR169");
                dists.remove(i);
                continue;
            }
            // Derby 10.3.1.4 does not boot on the phoneME advanced platform,
            // (see DERBY-3176) so don't run upgrade tests in this combination.
            if (BaseTestCase.isPhoneME() &&
                    noPhoneMEBoot.equals(distVersion)) {
                println("skipping " + noPhoneMEBoot.toString() +
                        " on CVM/phoneme");
                dists.remove(i);
                continue;
            }
        }
    }

    /** Prints a trace message if tracing is enabled. */
    private static void traceit(String msg) {
        BaseTestCase.traceit(msg);
    }

    /** Prints a debug message if debugging is enabled. */
    private static void println(String msg) {
        BaseTestCase.println(msg);
    }
}
