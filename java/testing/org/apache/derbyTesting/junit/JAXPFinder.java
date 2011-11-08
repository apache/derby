/*
 *
 * Derby - Class org.apache.derbyTesting.junit.JAXPFinder
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

import java.net.URL;

/* The following import is of a JAXP API class.  The JAXP API classes
 * are included within JVMs that are 1.4 and later.  However, 1.3 and
 * J2ME JVMs do not include the JAXP API classes.  As a result this
 * file can only be built with 1.4 or later (see build.xml in this
 * directory).  We have to separate this class out from junit/XML.java
 * because XML.java will be instantiated for any JUnit test, regardless
 * of whether or not the the required JAXP classes are in the user's
 * classpath.  This means that if we imported the JAXP class into
 * junit/XML.java, a user who tried to run a JUnit test without
 * the JAXP interface in his/her classpath (which typically means
 * J2ME is being used) would see a NoClassFoundError. (DERBY-2153).
 * That's not what we want; instead, all tests that do *not* rely on
 * JAXP should run as normal and any tests that require JAXP should be
 * silently skipped.
 *
 * To accomplish this goal we import/reference DocumentBuilderFactory 
 * in *this* class (JAXPFinder). Then we *only* make calls on this
 * JAXPFinder if we know for certain that all required XML classes
 * are in the user's classpath.  With this restriction in place we
 * can ensure that the JAXP class will never be instantiated for
 * environments which do not have a JAXP parser.  Thus the JUnit
 * harness will run/skip tests as expected whether or not the user's
 * classpath includes a JAXP parser.
 */
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Simple class used for determining the location of the jar 
 * file (based on the user's classpath) that contains the JAXP
 * implementation.
 */
public class JAXPFinder {

    /**
     * String form of the URL for the jar file in the user's classpath
     * that holds the JAXP implementation in use.  If the implementation
     * is embedded within, or endorsed by, the JVM, then we will set this
     * field to be an empty string.
     */
    private static String jaxpURLString = null;
    
    /**
     * Return the string form of the URL for the jar file that contains
     * whichever JAXP parser implementation is picked up from the user's
     * classpath.  If the JAXP parser is not in the user's classpath,
     * then it must be embedded within the JVM (either implicitly or else
     * through use of "endorsed standards" jars), in which case we return
     * null.
     *
     * NOTE: Assumption is that we only get here if we know there is in
     * fact a JAXP parser available to the JVM.  I.e. if a call to
     * the "classpathHasXalanAndJAXP()" method of junit/XML.java returns
     * true.
     */
    protected static String getJAXPParserLocation()
    {
        // Only get the URL if we have not already done it.
        if (jaxpURLString == null)
        {
            /* Figure out which JAXP implementation we have by
             * instantiating a DocumentBuilderFactory and then getting
             * the implementation-specific class for that object.
             * Note that we cannot just use:
             *
             *   SecurityManagerSetup.getURL(DocumentBuilderFactory.class)
             *
             * because the 1.4, 1.5, and 1.6 JVMs (at least, Sun and IBM)
             * all embed the JAXP API classes, so any attempts to look
             * up the URL for DocumentBuilderFactory.class will return
             * null for those JVMs. But in the case of, say, Sun 1.5, the
             * JAXP *implementation* classes are not embedded. So if we're
             * running with Sun 1.5 and we have an external JAXP
             * implementation (say Xerces) in the classpath, we need to
             * find the URL for that external jar file. By instantiating
             * DocumentBuilderFactory and then using the implementation-
             * specific class name we ensure that, for external (w.r.t the
             * JVM) JAXP implementations, we can find the implementation
             * jar file and thus we can assign the correct permissions.
             */
            URL jaxpURL = SecurityManagerSetup.getURL(
                DocumentBuilderFactory.newInstance().getClass());

            /* If we found a URL then the JAXP parser is in the classpath
             * in some jar external to the JVM; in that case we have the
             * the jar's location so we use/return that.  Otherwise we
             * assume that the JAXP parser is either embedded within the
             * JVM or else "endorsed" by it. In those cases we set our
             * URL string to be the empty string, which is non-null and
             * thus we will only execute this try-catch once.
             */
            jaxpURLString =
                (jaxpURL == null) ? "" : jaxpURL.toExternalForm();
        }

        // If we didn't find the JAXP parser URL, then return null.
        return ((jaxpURLString.length() == 0) ? null : jaxpURLString);
    }
}
