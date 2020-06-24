/*

   Derby - Class org.apache.derbyTesting.junit.DerbyDistribution

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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;

/**
 * Holds information required to run a Derby distribution and make choices
 * based on the version of the Derby distribution.
 * <p>
 * <em>Implementation note</em>: For simplicity distributions off the classes
 * directory have been forbidden. The main reason for this is that it is
 * sometimes a hard requirement that you must include only a single JAR from a
 * distribution on the classpath. One such example is the compatibility test,
 * where you need the testing code from one distribution and the client driver
 * from another. While it is possible to support such a configuration running
 * off the {@code classes}-directory in many scenarios, it complicates
 * the creation and handling of classpath string. Generating the JARs when
 * testing on trunk seems like an acceptable price to pay.
 */
public class DerbyDistribution implements Comparable<DerbyDistribution> {

    private static File[] EMPTY_FILE_ARRAY = new File[] {};
    public static final String JAR_RUN = "derbyrun.jar";
    public static final String JAR_CLIENT = "derbyclient.jar";
    public static final String JAR_ENGINE = "derby.jar";
    public static final String JAR_NET = "derbynet.jar";
    public static final String JAR_SHARED = "derbyshared.jar";
    public static final String JAR_TOOLS = "derbytools.jar";
    public static final String JAR_TESTING = "derbyTesting.jar";
    private static final String[] REQUIRED_JARS = {
        JAR_ENGINE, JAR_NET, JAR_CLIENT
    };

    /** The version of the Derby distribution, i.e. 10.8.1.2. */
    private final DerbyVersion version;
    /** Path to derbyrun.jar (may be {@code null}). */
    private final String derbyRunJarPath;
    /** Path to derbyclient.jar. */
    private final String derbyClientJarPath;
    /** Path to derby.jar. */
    private final String derbyEngineJarPath;
    /** Path to derbynet.jar. */
    private final String derbyNetJarPath;

    /** Shared jar (10.15 onward) */
    private final String derbySharedJarPath;

    /** Tools jar (needed for DataSources from 10.15 onward */
    private final String derbyToolsJarPath;
    
    /**
     * Production classpath, i.e. all JAR files found except for
     * derbyTesting.jar.
     */
    private final String productionClasspath;
    /** Testing classpath, i.e. path to derbyTesting.jar. */
    private final String testingClasspath;

    /**
     * Derives the information for a Derby distribution.
     *
     * @throws NullPointerException if version is {@code null}
     * @see #newInstance(DerbyVersion, File)
     */
    private DerbyDistribution(DerbyVersion version,
//IC see: https://issues.apache.org/jira/browse/DERBY-5475
                              File[] productionJars, File[] testingJars) {
        if (version == null) {
            throw new NullPointerException("version is null");
        }
        this.version = version;
        this.productionClasspath = constructJarClasspath(productionJars);
        this.testingClasspath = constructJarClasspath(testingJars);
        File root = productionJars[0].getParentFile();
        this.derbyRunJarPath = getPath(root, JAR_RUN);
        this.derbyClientJarPath = getPath(root, JAR_CLIENT);
        this.derbyEngineJarPath = getPath(root, JAR_ENGINE);
        this.derbyNetJarPath = getPath(root, JAR_NET);
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        this.derbySharedJarPath = getPath(root, JAR_SHARED);
        this.derbyToolsJarPath = getPath(root, JAR_TOOLS);
    }

    /** Returns the absolute path to the JAR if it exists, otherwise null. */
    private String getPath(File root, String jar) {
        File f = new File(root, jar);
        if (PrivilegedFileOpsForTests.exists(f)) {
            return f.getAbsolutePath();
        } else {
            return null;
        }
    }

    /** Tells if this distribution has a {@code derbyrun.jar}. */
    public boolean hasDerbyRunJar() {
        return derbyRunJarPath != null;
    }

    /**
     * Returns the path to {@code derbyrun.jar}.
     *
     * @return A path, or {@code null} if this distribution doesn't come with
     *      {@code derbyrun.jar}.
     * @see #hasDerbyRunJar()
     */
    public String getDerbyRunJarPath() {
        return derbyRunJarPath;
    }

    /** Returns the path to {@code derbyclient.jar}. */
    public String getDerbyClientJarPath() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        String retval = derbyClientJarPath;
        if (version.atLeast(DerbyVersion.FIRST_JIGSAW_VERSION))
        {
            retval =
              retval +
              File.pathSeparator + getDerbySharedJarPath() +
              File.pathSeparator + getDerbyToolsJarPath()
              ;
        }

        return retval;
   }

    /** Returns the path to {@code derby.jar}. */
    public String getDerbyEngineJarPath() {
        String retval = derbyEngineJarPath;
        if (version.atLeast(DerbyVersion.FIRST_JIGSAW_VERSION))
        {
            retval =
              retval +
              File.pathSeparator + getDerbySharedJarPath() +
              File.pathSeparator + getDerbyToolsJarPath()
              ;
        }

        return retval;
    }

    /** Returns the path to {@code derbynet.jar}. */
    public String getDerbyNetJarPath() {
        return derbyEngineJarPath;
    }

    /** Returns the path to {@code derbyshared.jar}. */
    public String getDerbySharedJarPath() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        return derbySharedJarPath;
    }

    /** Returns the path to {@code derbytools.jar}. */
    public String getDerbyToolsJarPath() {
        return derbyToolsJarPath;
    }

    /** Returns a classpath with the network server production JARs. */
    public String getServerClasspath() {
        return
            this.derbyNetJarPath + File.pathSeparator + getDerbyEngineJarPath();
    }

    /** Returns a classpath with all production JARs. */
    public String getProductionClasspath() {
        return productionClasspath;
    }

    /** Returns a classpath with all testing JARs. */
    public String getTestingClasspath() {
        return testingClasspath;
    }

    /** Returns a classpath with all production and testing JARs. */
    public String getFullClassPath() {
        return productionClasspath + File.pathSeparatorChar + testingClasspath;
    }

    /** Returns the version of this distribution. */
    public DerbyVersion getVersion() {
        return version;
    }

    /**
     * Orders this distribution and the other distribution based on the version.
     *
     * @param o the other distribution
     * @return {@code 1} if this version is newer, {@code 0} if both
     *      distributions have the same version, and {@code -1} if the other
     *      version is newer.
     */
    public int compareTo(DerbyDistribution o) {
        return version.compareTo(o.version);
    }

    private static boolean hasRequiredJars(List jars) {
        for (int i=0; i < REQUIRED_JARS.length; i++) {
            boolean hasJar = false;
            for (Iterator jarIter = jars.iterator(); jarIter.hasNext(); ) {
                File jar = (File)jarIter.next();
                if (jar.getName().equalsIgnoreCase(REQUIRED_JARS[i])) {
                    hasJar = true;
                    break;
                }
            }
            if (!hasJar) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5897
                BaseTestCase.println("missing jar: " + REQUIRED_JARS[i]);
                return false;
            }
        }
        return true;
    }

    /**
     * Helper method extracting Derby production JARs from a directory.
     *
     * @param libDir directory
     * @return A list of JARs (possibly empty).
     */
    private static File[] getProductionJars(File libDir) {
        File[] pJars = libDir.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return name.toUpperCase().endsWith(".JAR") &&
                        !isTestingJar(name);
            }
        });
        if (pJars == null) {
            return EMPTY_FILE_ARRAY;
        } else {
            return pJars;
        }
    }

    /**
     * Helper method extracting Derby testing JARs from a directory.
     *
     * @param libDir directory
     * @return A list of JARs (possibly empty).
     */
    private static File[] getTestingJars(File libDir) {
        File[] tJars = libDir.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return isTestingJar(name);
            }
        });
        if (tJars == null) {
            return EMPTY_FILE_ARRAY;
        } else {
            return tJars;
        }
    }

    public static File[] getJars(File libDir) {
        File[] jars = libDir.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return name.toUpperCase().endsWith(".JAR");
            }
        });
        return jars;
    }

    /**
     * Tells if the given file is a Derby testing JAR.
     *
     * @param name name of the file
     * @return {@code true} if a testing JAR, {@code false} otherwise
     */
    private static boolean isTestingJar(String name) {
        return name.toUpperCase().endsWith(JAR_TESTING.toUpperCase());
    }

    /**
     * Merges a list of JAR files into a classpath string.
     *
     * @param jars JAR files to merge
     * @return A classpath string.
     */
    private static String constructJarClasspath(File[] jars) {
        StringBuffer sb = new StringBuffer(512);
        for (int i=0; i < jars.length; i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5475
            try {
                sb.append(jars[i].getCanonicalPath());
            } catch (IOException ioe) {
                // Do the next best thing; use absolute path.
                String absPath = jars[i].getAbsolutePath();
                sb.append(absPath);
                BaseTestCase.println("obtaining canonical path for " +
                        absPath + " failed: " + ioe.getMessage());
            }
            sb.append(File.pathSeparatorChar);
        }
        if (jars.length > 0) {
            sb.deleteCharAt(sb.length() -1);
        }
        return sb.toString();
    }

    /**
     * <p>
     * Returns a distribution with the specified version, based on the given
     * library directory.
     * </p>
     *
     * <p>
     * It is the responsibility of the caller to ensure that the specified
     * version matches the JARs in the given directory.
     * </p>
     *
     * @param version the version of the distribution
     * @param baseDir the base dir for the distribution, holding the Derby JARs
     * @return A representation of the distribution, or {@code null} if
     *      the specified directory doesn't contain a valid distribution.
     * @throws IllegalArgumentException if {@code version} is {@code null}
     */
    public static DerbyDistribution newInstance(DerbyVersion version,
//IC see: https://issues.apache.org/jira/browse/DERBY-6126
                                                File baseDir) {
        return newInstance(version, baseDir, baseDir);
    }

    /**
     * <p>
     * Returns a distribution with the specified version, based on the given
     * library and testing directories.
     * </p>
     *
     * <p>
     * It is the responsibility of the caller to ensure that the specified
     * version matches the JARs in the given directories.
     * </p>
     *
     * @param version the version of the distribution
     * @param baseDir the directory holding the production JARs
     * @param testDir the directory holding the testing JAR
     * @return A representation of the distribution, or {@code null} if
     *      the specified directories don't make up a valid distribution.
     * @throws IllegalArgumentException if {@code version} is {@code null}
     */
    public static DerbyDistribution newInstance(DerbyVersion version,
                                                File baseDir, File testDir) {
        File[] productionJars = getProductionJars(baseDir);
        File[] testingJars = getTestingJars(testDir);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        List<File> tmpJars = new ArrayList<File>();
        tmpJars.addAll(Arrays.asList(productionJars));
        tmpJars.addAll(Arrays.asList(testingJars));
        if (hasRequiredJars(tmpJars)) {
            return new DerbyDistribution(version, productionJars, testingJars);
        }
        // Invalid distribution, ignore it.
        BaseTestCase.println("Distribution deemed invalid (note that running " +
//IC see: https://issues.apache.org/jira/browse/DERBY-6126
                "off classes isn't supported): " + baseDir.getAbsolutePath() +
                (baseDir.equals(testDir) ? ""
                                         : ", " + testDir.getAbsolutePath()));
        return null;
    }
}
