/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.XML
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;

import java.lang.reflect.Method;
import java.security.PrivilegedActionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.StringTokenizer;

import junit.framework.Assert;

/**
 * <p>
 * XML utility methods for the JUnit tests.
 * </p>
 *
 * <p>
 * Note that The XML tests require a more advanced version of Xalan
 * than the default version bundled with JDK 1.4. The XML tests silently
 * exit if the required environment is not found.
 * </p>
 *
 * <p>
 * To run the XML tests under JDK 1.4, you must do the following:
 * </p>
 *
 * <ul>
 * <li>Download the latest version of Xalan (2.7.0 as of this writing).</li>
 * <li>Copy all of the downloaded jars into the jre/lib/endorsed directory
 * of your JDK 1.4 installation. Those jar files are:
 * serializer.jar, xalan.jar, xercesImpl.jar, xml-apis.jar, and xsltc.jar.</li>
 * </ul>
 *
 * <p>
 *That's it! Now the XML tests should run for you under JDK 1.4.
 * </p>
 *
 * <p>
 * To run the XML tests under a higher version of the JDK, you must do the
 * following:
 * </p>
 *
 * <ul>
 * <li>Download the latest version of Xalan as described above.</li>
 * <li>Wire the downloaded jar files into your CLASSPATH.</li>
 * </ul>
 */
public class XML {
    
    /**
     * Minimum version of Xalan required to run XML tests under
     * Security Manager. In this case, we're saying that the
     * minimum version is Xalan 2.5.0 (because there's a bug
     * in earlier versions that causes problems with security
     * manager).
     */
    private static int [] MIN_XALAN_VERSION = new int [] { 2, 5, 0 };

    /**
     * Determine whether or not the classpath with which we're
     * running has the JAXP API classes required for use of
     * the Derby XML operators.
     */
    private static final boolean HAVE_JAXP =
        JDBC.haveClass("org.w3c.dom.Document");

    /**
     * Determine whether or not the classpath with which we're
     * running has a version of Xalan in it.  Xalan is required
     * for use of the Derby XML operators.  In particular we
     * check for:
     *
     *  1. Xalan classes (version doesn't matter here)
     *  2. The Xalan "EnvironmentCheck" class, which is included
     *     as part of Xalan.  This allows us to check the specific
     *     version of Xalan in use so that we can determine if
     *     if we satisfy the minimum requirement.
     */
    private static final boolean HAVE_XALAN =
            JDBC.haveClass("org.apache.xpath.XPath") &&
            JDBC.haveClass("org.apache.xalan.xslt.EnvironmentCheck");

    /**
     * Determine if we have the minimum required version of Xalan
     * for successful use of the XML operators.
     */
    private static final boolean HAVE_MIN_XALAN
            = HAVE_XALAN && checkXalanVersion();

    /**
     * The filepath for the directory that holds the XML "helper" files
     * (i.e. the files to insert and their schema documents).
     */
    private static final String HELPER_FILE_LOCATION =
        "org/apache/derbyTesting/functionTests/tests/lang/xmlTestFiles/";

    /**
     * Return true if the classpath contains JAXP and
     * Xalan classes (this method doesn't care about
     * the particular version of Xalan).
     */
    public static boolean classpathHasXalanAndJAXP()
    {
        return HAVE_JAXP && HAVE_XALAN;
    }

    /**
     * Return true if the classpath meets all of the requirements
     * for use of the SQL/XML operators.  This means that all
     * required classes exist in the classpath AND the version
     * of Xalan that we found is at least MIN_XALAN_VERSION.
     */
    public static boolean classpathMeetsXMLReqs()
    {
        return HAVE_JAXP && HAVE_MIN_XALAN;
    }

    /**
     * Insert the contents of a file into the received column of
     * the received table using "setCharacterStream".  Expectation
     * is that the file is in the directory indicated by 
     * HELPER_FILE_LOCATION.
     *
     * @param conn Connection on which to perform the insert.
     * @param tableName Table into which we want to insert.
     * @param colName Column in tableName into which we want to insert.
     * @param fName Name of the file whose content we want to insert.
     * @param numRows Number of times we should insert the received
     *  file's content.
     */
    public static void insertFile(Connection conn, String tableName,
        String colName, String fName, int numRows)
        throws IOException, SQLException, PrivilegedActionException
    {
        // First we have to figure out many chars long the file is.

        fName = HELPER_FILE_LOCATION + fName;
        java.net.URL xFile = BaseTestCase.getTestResource(fName);
        Assert.assertNotNull("XML input file missing: " + fName, xFile);
        
        int charCount = 0;
        char [] cA = new char[1024];
        InputStreamReader reader =
            new InputStreamReader(BaseTestCase.openTestResource(xFile));

        for (int len = reader.read(cA, 0, cA.length); len != -1;
            charCount += len, len = reader.read(cA, 0, cA.length));

        reader.close();

        // Now that we know the number of characters, we can insert
        // using a stream.

        PreparedStatement pSt = conn.prepareStatement(
            "insert into " + tableName + "(" + colName + ") values " +
            "(xmlparse(document cast (? as clob) preserve whitespace))");

        for (int i = 0; i < numRows; i++)
        {
            reader = new InputStreamReader(
                BaseTestCase.openTestResource(xFile));

            pSt.setCharacterStream(1, reader, charCount);
            pSt.execute();
            reader.close();
        }

        pSt.close();
    }

    /**
     * Insert an XML document into the received column of the received
     * test table using setString.  This method parallels "insertFiles"
     * above, except that it should be used for documents that require
     * a Document Type Definition (DTD).  In that case the location of
     * the DTD has to be modified _within_ the document so that it can
     * be found in the running user directory.
     *
     * Expectation is that the file to be inserted is in the directory
     * indicated by HELPER_FILE_LOCATION and that the DTD file has been
     * copied to the user's running directory (via use of the util
     * methods in SupportFilesSetup).
     *
     * @param conn Connection on which to perform the insert.
     * @param tableName Table into which we want to insert.
     * @param colName Column in tableName into which we want to insert.
     * @param fName Name of the file whose content we want to insert.
     * @param dtdName Name of the DTD file that the received file uses.
     * @param numRows Number of times we should insert the received
     *  file's content.
     */
    public static void insertDocWithDTD(Connection conn, String tableName,
        String colName, String fName, String dtdName, int numRows)
        throws IOException, SQLException, PrivilegedActionException
    {
        // Read the file into memory so we can update it.
        fName = HELPER_FILE_LOCATION + fName;
        java.net.URL xFile = BaseTestCase.getTestResource(fName);
        Assert.assertNotNull("XML input file missing: " + fName, xFile);

        int charCount = 0;
        char [] cA = new char[1024];
        StringBuffer sBuf = new StringBuffer();
        InputStreamReader reader =
            new InputStreamReader(BaseTestCase.openTestResource(xFile));

        for (int len = reader.read(cA, 0, cA.length); len != -1;
            charCount += len, len = reader.read(cA, 0, cA.length))
        {
            sBuf.append(cA, 0, len);
        }

        reader.close();

        // Now replace the DTD location.

        java.net.URL dtdURL = SupportFilesSetup.getReadOnlyURL(dtdName);
        Assert.assertNotNull("DTD file missing: " + dtdName, dtdURL);

        String docAsString = sBuf.toString();
        int pos = docAsString.indexOf(dtdName);
        if (pos != -1)
            sBuf.replace(pos, pos+dtdName.length(), dtdURL.toExternalForm());

        // Now (finally) do the insert using the in-memory document with
        // the correct DTD location.
        docAsString = sBuf.toString();
        PreparedStatement pSt = conn.prepareStatement(
            "insert into " + tableName + "(" + colName + ") values " +
            "(xmlparse(document cast (? as clob) preserve whitespace))");

        for (int i = 0; i < numRows; i++)
        {
            pSt.setString(1, docAsString);
            pSt.execute();
        }

        pSt.close();
    }

    /**
     * Determine whether or not the classpath with which we're
     * running has a version of Xalan that meets the minimum
     * Xalan version requirement.  We do that by using a Java
     * utility that ships with Xalan--namely, "EnvironmentCheck"--
     * and by parsing the info gathered by that method to find
     * the Xalan version.  We use reflection when doing this
     * so that this file will compile/execute even if XML classes
     * are missing.
     *
     * Assumption is that we only get to this method if we already
     * know that there *is* a version of Xalan in the classpath
     * and that version includes the "EnvironmentCheck" class.
     *
     * Note that this method returns false if the call to Xalan's
     * EnvironmentCheck.checkEnvironment() returns false for any
     * reason.  As a specific example, that method will always
     * return false when running with ibm131 because it cannot
     * find the required methods on the SAX 2 classes (apparently
     * the classes in ibm131 jdk don't have all of the methods
     * required by Xalan).  Thus this method will always return
     * "false" for ibm131.
     */
    private static boolean checkXalanVersion()
    {
        boolean haveMinXalanVersion = false;
        try {

            // These io objects allow us to retrieve information generated
            // by the call to EnvironmenCheck.checkEnvironment()
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            PrintWriter pW = new PrintWriter(bos);

            // Call the method using reflection.

            Class cl = Class.forName("org.apache.xalan.xslt.EnvironmentCheck");
            Method meth = cl.getMethod("checkEnvironment",
                new Class[] { PrintWriter.class });

            Boolean boolObj = (Boolean)meth.invoke(
                cl.newInstance(), new Object [] { pW });

            pW.flush();
            bos.flush();

            cl = null;
            meth = null;
            pW = null;

            /* At this point 'bos' holds a list of properties with
             * a bunch of environment information.  The specific
             * property we're looking for is "version.xalan2_2",
             * so get that property, parse the value, and see
             * if the version is at least the minimum required.
             */
            if (boolObj.booleanValue())
            {
                /* We wrote the byte array using the platform's default
                 * encodinging (that's what we get with the call to
                 * "new PrintWriter(bos)" above), so read it in using
                 * the default encoding, as well (i.e. don't pass an
                 * encoding into toString()).
                 */
                String checkEnvOutput = bos.toString();
                bos.close();

                /* The property we're looking for is on a single line
                 * of the output, and that line starts with the name
                 * of the property.  So extract that line out now. If
                 * we can't find it, just return "false" to say that
                 * we could not find the minimum version. Note: it's
                 * possible (though admittedly unlikely) that the
                 * string "version.xalan2_2" appears in the user's
                 * classpath.  Adding an equals sign ("=") at the end
                 * of our search pattern reduces the chance of the
                 * search string appearing in the classpath, but does
                 * not eliminate it...
                 */
                int pos = checkEnvOutput.indexOf("version.xalan2_2=");
                if (pos < 0)
                    return false;

                String ver = checkEnvOutput.substring(
                    pos, checkEnvOutput.indexOf("\n", pos));

                // Now pull out the one we need.
                haveMinXalanVersion = (ver != null);
                if (haveMinXalanVersion)
                {
                    /* We found the property, so parse out the necessary
                     * piece.  The value is of the form:
                     *
                     *   <productName> Major.minor.x
                     *
                     * Ex:
                     *
                     *   version.xalan2_2=Xalan Java 2.5.1 
                     *   version.xalan2_2=XSLT4J Java 2.6.6
                     */
                    int i = 0;
                    StringTokenizer tok = new StringTokenizer(ver, ". ");
                    while (tok.hasMoreTokens())
                    {
                        String str = tok.nextToken().trim();
                        if (Character.isDigit(str.charAt(0)))
                        {
                            int val = Integer.valueOf(str).intValue();
                            if (val < MIN_XALAN_VERSION[i])
                            {
                                haveMinXalanVersion = false;
                                break;
                            }
                            i++;
                        }

                        /* If we've checked all parts of the min version,
                         * then we assume we're okay. Ex. "2.5.0.2"
                         * is considered greater than "2.5.0".
                         */
                        if (i >= MIN_XALAN_VERSION.length)
                            break;
                    }

                    /* If the value had fewer parts than the
                     * mininum version, then it doesn't meet
                     * the requirement.  Ex. "2.5" is considered
                     * to be a lower version than "2.5.0".
                     */
                    if (i < MIN_XALAN_VERSION.length)
                        haveMinXalanVersion = false;
                }
            }

            /* Else the call to checkEnvironment() returned "false",
             * which means it couldn't find all of the classes/methods
             * required for Xalan to function.  So in that case we'll
             * fall through and just return false, as well.
             */

        } catch (Throwable t) {

            System.out.println("Unexpected exception while " +
                "trying to find Xalan version:");
            t.printStackTrace(System.err);

            // If something went wrong, assume we don't have the
            // necessary classes.
            haveMinXalanVersion = false;

        }

        return haveMinXalanVersion;
    }

    /**
     * Return the string form of the URL for the jar file that contains
     * whichever JAXP parser implementation is picked up from the user's
     * classpath.  If the JAXP parser is not in the user's classpath,
     * then it must be embedded within the JVM (either implicitly or else
     * through use of "endorsed standards" jars), in which case we return
     * null.
     */
    protected static String getJAXPParserLocation()
    {
        /* If the classpath does not have JAXP then we do not want to
         * instantiate the JAXPFinder class (which happens indirectly
         * if we call its static methods).  This is because JAXPFinder
         * references a JAXP class that does not exist for J2ME, so
         * if we try to call a method on JAXPFinder without a JAXP
         * parser in the classpath, the result for J2ME would be
         * be a NoClassDefFound error (DERBY-2153).
         */
        if (!classpathHasXalanAndJAXP())
            return null;

        return JAXPFinder.getJAXPParserLocation();
    }
}
