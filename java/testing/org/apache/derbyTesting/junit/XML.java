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
import java.io.InputStreamReader;

import java.lang.reflect.Method;
import java.security.PrivilegedActionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
 * serializer.jar, xalan.jar, xercesImpl.jar, and xsltc.jar.</li>
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
     * Determine whether or not the classpath with which we're
     * running has the JAXP API classes required for use of
     * the Derby XML operators.
     */
    private static final boolean HAVE_JAXP =
        JDBC.haveClass("org.w3c.dom.Document");

    /**
     * Determine whether or not the classpath with which we're
     * running has a JAXP implementation.
     */
    private static final boolean HAVE_JAXP_IMPL =
            HAVE_JAXP && checkJAXPImplementation();

    /**
     * Determine if we have support for DOM level 3 XPath, which is required
     * for successful use of the XML operators.
     */
    private static final boolean HAVE_XPATH_LEVEL_3
            = HAVE_JAXP_IMPL && checkXPathSupport();

    /**
     * The filepath for the directory that holds the XML "helper" files
     * (i.e. the files to insert and their schema documents).
     */
    private static final String HELPER_FILE_LOCATION =
        "org/apache/derbyTesting/functionTests/tests/lang/xmlTestFiles/";

    /**
     * Return true if the classpath contains JAXP and
     * an implementation of the JAXP interfaces, for example the
     * Xalan classes (this method doesn't care about
     * support for DOM level 3 XPath).
     */
    public static boolean classpathHasJAXP()
    {
        return HAVE_JAXP_IMPL;
    }

    /**
     * Return true if the classpath meets all of the requirements
     * for use of the SQL/XML operators.  This means that all
     * required classes exist in the classpath AND there is support
     * for DOM level 3 XPath.
     */
    public static boolean classpathMeetsXMLReqs()
    {
        return HAVE_XPATH_LEVEL_3;
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
     * <p>
     * Determine whether or not the classpath with which we're
     * running contains a JAXP implementation that supports
     * DOM level 3 XPath.
     * </p>
     *
     * <p>
     * Assumption is that we only get to this method if we already
     * know that there *is* an implementation of JAXP in the classpath.
     * </p>
     */
    private static boolean checkXPathSupport()
    {
        boolean supportsXPath;

        // Invoke the following using reflection to see if we have support
        // for DOM level 3 XPath:
        //
        //     DocumentBuilderFactory.newInstance().newDocumentBuilder()
        //             .getDOMImplementation().getFeature("+XPath", "3.0");
        //
        try {
            Class<?> factoryClass =
                    Class.forName("javax.xml.parsers.DocumentBuilderFactory");

            Method newFactory =
                    factoryClass.getMethod("newInstance", new Class[0]);

            Object factory = newFactory.invoke(null, new Object[0]);

            Method newBuilder = factoryClass.getMethod(
                    "newDocumentBuilder", new Class[0]);

            Object builder = newBuilder.invoke(factory, new Object[0]);

            Class<?> builderClass =
                    Class.forName("javax.xml.parsers.DocumentBuilder");

            Method getImpl = builderClass.getMethod(
                    "getDOMImplementation", new Class[0]);

            Object impl = getImpl.invoke(builder, new Object[0]);

            Class<?> domImplClass =
                    Class.forName("org.w3c.dom.DOMImplementation");

            Method getFeature = domImplClass.getMethod(
                    "getFeature", new Class[] {String.class, String.class});

            Object ret =
                    getFeature.invoke(impl, new Object[] {"+XPath", "3.0"});

            supportsXPath = (ret != null);

        } catch (Throwable t) {
            // If something went wrong, assume we don't have the
            // necessary classes.
            supportsXPath = false;
        }

        return supportsXPath;
    }

    private static boolean checkJAXPImplementation() {
        try {
            Class<?> factoryClass =
                    Class.forName("javax.xml.parsers.DocumentBuilderFactory");
            Method newFactory =
                    factoryClass.getMethod("newInstance", new Class[0]);
            Object factory = newFactory.invoke(null, new Object[0]);
            return factory != null;
        } catch (Throwable t) {
            return false;
        }
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
        if (!classpathHasJAXP())
            return null;

        try {
            Class<?> jaxpFinderClass = Class.forName("org.apache.derbyTesting.junit.JAXPFinder");
            Method locatorMethod = jaxpFinderClass.getDeclaredMethod("getJAXPParserLocation");

            return (String) locatorMethod.invoke(null);
        }
        catch (Exception e)
        {
            throw new UnsupportedOperationException( e.getClass().getName() + ": " + e.getMessage() );
        }
    }
}
