/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.UpgradeClassLoader

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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;

import junit.framework.Assert;

import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * <p>
 * This class factors out the machinery  needed to wrap a class loader around
 * the jar files for an old release.
 * </p>
 */
public class UpgradeClassLoader
{
    private static final Version FIRST_MODULE_SUPPORT_VERSION = new Version(new int[] { 10, 15, 0, 0 });
    private static final String[] _preModuleSupport_jarFiles = {
            "derby.jar", 
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            "derbynet.jar",
            "derbyTesting.jar",
            };

    private static final String[] _postModuleSupport_jarFiles = {
            "derby.jar", 
            "derbynet.jar",
            "derbyTesting.jar",
            "derbyshared.jar",
            "derbytools.jar"
            };

    static final String oldVersionsPath =
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            getSystemProperty(_Suite.OLD_VERSIONS_PATH_PROPERTY);
    static final String jarPath =
            getSystemProperty(_Suite.OLD_RELEASE_PATH_PROPERTY);

    private static String getSystemProperty(final String key) {
        return AccessController.doPrivileged(new PrivilegedAction<String>() {
            public String run() {
                return System.getProperty(key);
            }
        });
    }

    protected static String getTextVersion(int[] iv)
    {
        String version = iv[0] + "." + iv[1] +
        "." + iv[2] + "." + iv[3];
        return version;
    }

    /**
     * <p>
     * Wrap a class loader around the given version.
     * </p>
     */
    public static ClassLoader makeClassLoader( final int[] version )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        ClassLoader oldLoader = AccessController.doPrivileged(
                new PrivilegedAction<ClassLoader>() {
            public ClassLoader run() {
                return createClassLoader(version);
            }
        });

        if (oldLoader == null)
        {
            BaseTestCase.traceit("Empty: Skip upgrade Tests (no jars) for " + getTextVersion(version));
        }
        
        return oldLoader;
    }

    /**
     * Get the location of jars of old release. The location is specified 
     * in the property derbyTesting.oldReleasePath. If derbyTesting.oldReleasePath
     * is set to the empty string it is ignored.
     *  
     * @return location of jars of old release
     */
    private static String getOldJarLocation(int[] oldVersion) {
      
        if (jarPath == null || jarPath.length() == 0)
            return null;
        
        String version = getTextVersion(oldVersion);
        String jarLocation = jarPath + File.separator + version;
        
        return jarLocation;
    }

    /**
     * Get the location of jars of old release, using the url for svn at apache.
     *  
     * @return location of jars of old release
     */
    private static String getOldJarURLLocation(int[] oldVersion) {

        String oldJarUrl = _Suite.OLD_JAR_URL;
        
        String version = getTextVersion(oldVersion);
        String jarLocation = oldJarUrl + "/" + version;
        
        return jarLocation;       
    }

    /**
     * Create a class loader using jars in the specified location. Add all jars 
     * specified in jarFiles and the testing jar.
     * 
     * @param version the Derby version to create a classloader for.
     * @return class loader
     */
    private static ClassLoader createClassLoader(int[] version)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        Version oldVersion = new Version(version);
        String[] jarFiles = (oldVersion.compareTo(FIRST_MODULE_SUPPORT_VERSION) < 0) ?
            _preModuleSupport_jarFiles : _postModuleSupport_jarFiles;

        // add an extra slot at the end for junit.jar
        URL[] url = new URL[jarFiles.length + 1];
        
        String jarLocation = getOldJarLocation(version);
        
        if (jarLocation != null)
        {
            File lib = new File(jarLocation);

            // If the jars do not exist then return null
            // and the caller will set up to skip this.
            if (!lib.exists()){
                BaseTestCase.alarm("Non-existing location for jar files: '" 
                    + jarLocation + "'. Upgrade tests can NOT be run!");
                return null;
            }

            for (int i=0; i < jarFiles.length; i++) {
                try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                    url[i] = new File(lib, jarFiles[i]).toURI().toURL();
                } catch (MalformedURLException e) {
                    Assert.fail(e.toString());
                }
            }
        }
        else
        
        // if the property was not set, attempt to access the jars from 
        // the saved svn location.
        // Note, this means the test fails if there is no network connection
        // (or the server at apache is down) unless the property is set
        // to a valid location
        {
            String oldURLJarLocation = getOldJarURLLocation(version);
            for (int i=0; i < jarFiles.length; i++) {
                try {
                    url[i] = new URL(oldURLJarLocation + "/" + jarFiles[i]);
                    Object dummy = url[i].getContent(); // IOException if not available.
                } catch (MalformedURLException e) {
                    Assert.fail(e.toString());
                } catch (IOException e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5775
                    BaseTestCase.alarm("IOException connecting to location: " + oldURLJarLocation + ", msg: '" + e.getMessage() + "'." 
                        + " Upgrade tests can NOT be run!");
                        e.printStackTrace();
                    return null;
                }

            }
        }

        // add junit.jar to the classpath
        int lastSlot = jarFiles.length;
        url[lastSlot] = Assert.class.getProtectionDomain().getCodeSource().getLocation();
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

        // Create a class loader which loads Derby classes from the specified
        // URL, and JDBC classes and other system classes from the platform
        // class loader.
//IC see: https://issues.apache.org/jira/browse/DERBY-6854
        ClassLoader oldVersionLoader =
            new URLClassLoader(url, java.sql.Connection.class.getClassLoader());

        // DERBY-5316: We need to unload the JDBC driver when done with it,
        // but that can only be done if the DriverUnloader class lives in a
        // class-loader which is able to load the driver class.
        return new ClassLoader(oldVersionLoader) {
            protected Class findClass(String name)
                    throws ClassNotFoundException {
                if (name.equals(DriverUnloader.class.getName())) {
                    try {
                        byte[] b = fetchDriverUnloaderBytes();
                        return defineClass(name, b, 0, b.length);
                    } catch (IOException ioe) {
                        throw new ClassNotFoundException(name, ioe);
                    }
                }
                throw new ClassNotFoundException(name);
            }
        };
    }

    /**
     * Get a byte array with the contents of the class file for the
     * {@code DriverUnloader} class.
     */
    private static byte[] fetchDriverUnloaderBytes() throws IOException {
        InputStream in =
            DriverUnloader.class.getResourceAsStream("DriverUnloader.class");
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            return out.toByteArray();
        } finally {
            in.close();
        }
    }
}

    

