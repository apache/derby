/*

   Derby - Class org.apache.derbyTesting.functionTests.testData.serializedDataSources.SerializeDataSources

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
package org.apache.derbyTesting.functionTests.testData.serializedDataSources;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.derby.tools.sysinfo;

/**
 * Serializes and writes data sources to file, or prints information about
 * a file assumed to be written by this program.
 * <p>
 * Four entities are written to the stream:
 * <ol> <li>Derby version string - UTF</li>
 *      <li>Derby build number - UTF</li>
 *      <li>Derby data source - object</li>
 *      <li>Derby data source reference - object</li>
 * </ol>
 * <p>
 * Both embedded and client data sources are attempted serialized, and the data
 * source class names are obtained from a predefined list. If another data
 * source implementation is added to Derby, its class name must be added to the
 * list if this class is supposed to serialize it and write it to file.
 * <p>
 * Existing files are overwritten, and the file name is constructed like this:
 * <tt>&lt;ClassName&gt;-&lt;modifiedVersionString&gt;.ser</tt>
 * The version string is modified by replacing punctuation marks with
 * underscores.
 */
public class SerializeDataSources {

    /** List of known data sources in the embedded driver. */
    private static final String[] KNOWN_EMBEDDED_DATA_SOURCES ={
            "org.apache.derby.jdbc.EmbeddedDataSource",
            "org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource",
            "org.apache.derby.jdbc.EmbeddedXADataSource",
            "org.apache.derby.jdbc.EmbeddedDataSource40",
            "org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource40",
            "org.apache.derby.jdbc.EmbeddedXADataSource40",
            "org.apache.derby.jdbc.BasicEmbeddedDataSource40",
            "org.apache.derby.jdbc.BasicEmbeddedConnectionPoolDataSource40",
            "org.apache.derby.jdbc.BasicEmbeddedXADataSource40"
        };

    /** List of known data sources in the client driver. */
    private static final String[] KNOWN_CLIENT_DATA_SOURCES ={
            "org.apache.derby.jdbc.ClientDataSource",
            "org.apache.derby.jdbc.ClientConnectionPoolDataSource",
            "org.apache.derby.jdbc.ClientXADataSource",
            "org.apache.derby.jdbc.ClientDataSource40",
            "org.apache.derby.jdbc.ClientConnectionPoolDataSource40",
            "org.apache.derby.jdbc.ClientXADataSource40",
            "org.apache.derby.jdbc.BasicClientDataSource40",
            "org.apache.derby.jdbc.BasicClientConnectionPoolDataSource40",
            "org.apache.derby.jdbc.BasicClientXADataSource40"
        };

    /**
     * Serialize and write data sources to file.
     * 
     * @param versionString Derby version string (i.e. 10.3.2.1)
     * @param buildNumber Derby build number (svn)
     * @param dataSourceClasses list of data source class names
     * @return The number of data sources serialized and written to file.
     * 
     * @throws ClassNotFoundException required class is not on the classpath
     * @throws InstantiationException if instantiating data source class fails
     * @throws IllegalAccessException if instantiating data source class fails
     * @throws IOException if writing to file fails
     * @throws NamingException if creating a naming reference for the data
     *      source fails
     */
    private static int serializeDataSources(String versionString,
                                            String buildNumber,
                                            String[] dataSourceClasses)
            throws ClassNotFoundException, InstantiationException,
                   IllegalAccessException, IOException, NamingException,
                   NoSuchMethodException, InvocationTargetException {
        String modifiedVersionString = versionString.replaceAll(
                "\\.", "_");
        modifiedVersionString = modifiedVersionString.replaceAll(
                " alpha", "");
        modifiedVersionString = modifiedVersionString.replaceAll(
                "_0_0", "_1_0");
        int dsCount = 0;
        for (String dsClassName : dataSourceClasses) {
            Class dsClass;
            // Try to load the class.
            try {
                dsClass = Class.forName(dsClassName);
            } catch (ClassNotFoundException cnfe) {
                // Print error message, but keep going.
                System.out.println("\tcouldn't load " + dsClassName);
                continue;
            }
            // Create new instance.
            DataSource ds = (DataSource)dsClass.newInstance();
            // Generate file name.
            File serialized = new File(dsClass.getSimpleName() + "-" +
                    modifiedVersionString + ".ser");
            System.out.println("\twriting " + serialized.getName());
            OutputStream os = new FileOutputStream(serialized);
            ObjectOutputStream oos = new ObjectOutputStream(os);
            // Wrote version string, build number, the data source
            // object and finally a {@link javax.naming.Reference} for
            // the data source (if non a non-JNDI data source).
            oos.writeUTF(versionString);
            oos.writeUTF(buildNumber);
            oos.writeObject(ds);
            if (!(dsClassName.contains("Basic"))) {
                Method getRef = Class.forName("javax.naming.Referenceable").
                        getMethod("getReference");
                Object dsRef = getRef.invoke(ds);
                oos.writeObject(dsRef);
            }
            oos.flush();
            oos.close();
            dsCount++;
        }
        return dsCount;
    }

    /**
     * Attempts to read information from a file assumed to contain a
     * serialized data source.
     * <p>
     * All information is printed to the console.
     *
     * @param fileName the name of the file to read from
     * @return {@code true} if the file was read successfully, {@code false} if
     *      something went wrong.
     */
    private static boolean printInfoFromSerializedFile(String fileName) {
        System.out.println(">>> File: " + fileName);
        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("\tFile does not exist.");
            return false;
        }
        if (!file.canRead()) {
            System.out.println("\tCannot read file.");
            return false;
        }
        try {
            InputStream is = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(is);
            String version = ois.readUTF();
            System.out.println("\tversion: " + version);
            String buildNr = ois.readUTF();
            System.out.println("\tbuild  : " + buildNr);
            Object obj = ois.readObject();
            System.out.println("\tobject : " + obj);
            if (!(obj.getClass().getName().indexOf("Basic") > 0)) {
                obj = ois.readObject();
                System.out.println("\tobject : " + obj);
            }
        } catch (Exception e) {
            System.out.println("\t!! De-serialization failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true; 
    }

    /**
     * Serializes and writes a number of Derby data sources to disk, or
     * attempts to read information from existing files.
     * 
     * @param args arguments from the command line. If there are no arguments,
     *      the program will write data sources to file. Otherwise all
     *      arguments are assumed to be file names of files to read.
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args)
            throws Exception {
        // Obtain Derby version / information.
        int majorVersionEmbedded = sysinfo.getMajorVersion(sysinfo.DBMS);
        int minorVersionEmbedded = sysinfo.getMinorVersion(sysinfo.DBMS);
        String buildNumberEmbedded = sysinfo.getBuildNumber(sysinfo.DBMS);
        String versionEmbedded = sysinfo.getVersionString(sysinfo.DBMS);
        int majorVersionClient = sysinfo.getMajorVersion(sysinfo.CLIENT);
        int minorVersionClient = sysinfo.getMinorVersion(sysinfo.CLIENT);
        String buildNumberClient = sysinfo.getBuildNumber(sysinfo.CLIENT);
        String versionClient = sysinfo.getVersionString(sysinfo.CLIENT);

        // Check if we should try to read files.
        if (args.length > 0) {
            System.out.println("Reading files with the Derby version(s):");
            System.out.println("\tembedded: " + versionEmbedded);
            System.out.println("\tclient  : " + versionClient);
            System.out.println();
            for (int i=0; i < args.length; i++) {
                boolean status = printInfoFromSerializedFile(args[i]);
                System.out.println("File read successfully: " + status);
                System.out.println();
            }
            System.exit(0);
        }

        // We are writing data sources to file.

        // Counts to print some simple statistics at the end.
        int knownDsCount = KNOWN_EMBEDDED_DATA_SOURCES.length +
                KNOWN_CLIENT_DATA_SOURCES.length;
        int dsWritten = 0;

        // Only try to serialize data sources if we know which Derby version we
        // are dealing with.
        if (majorVersionEmbedded != -1 && minorVersionEmbedded != -1) {
            System.out.println("Serializing embedded data sources for Derby " +
                    "version " + versionEmbedded);
            dsWritten += serializeDataSources(versionEmbedded,
                    buildNumberEmbedded,
                    KNOWN_EMBEDDED_DATA_SOURCES);
        } else {
            System.err.println("No embedded data sources will be generated " +
                    "because Derby version can't be determined.");
        }

        if (majorVersionClient != -1 && minorVersionClient != -1) {
            System.out.println("Serializing client data sources for Derby " +
                    "version " + versionClient);
            dsWritten += serializeDataSources(versionClient,
                    buildNumberClient,
                    KNOWN_CLIENT_DATA_SOURCES);
        } else {
            System.err.println("No client data sources will be generated " +
                    "because Derby version can't be determined.");
        }

        // Print some simple statistics.
        System.out.println();
        System.out.println("Known data sources:   " + knownDsCount);
        System.out.println("Data sources written: " + dsWritten);
    }
}
