/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LuceneSuite

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
*/
package org.apache.derbyTesting.functionTests.tests.lang;

import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Suite holding all of the tests for the Lucene plugin.
 *
 */
public class LuceneSuite extends BaseTestCase
{

	/**
	 * Use suite method instead.
	 */
	private LuceneSuite(String name) { super(name); }

	public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("LuceneSuite");
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        Properties  properties = TestConfiguration.getSystemProperties();
        
        //
        // If we're told to omit the Lucene plugin tests, make sure
        // that the Lucene jar files aren't on the classpath.
        //
        if ( getBooleanProperty( properties, TestConfiguration.KEY_OMIT_LUCENE ) )
        {
            assertFalse( "Lucene core jar file should not be on the classpath!", JDBC.HAVE_LUCENE_CORE );
            assertFalse( "Lucene analyzer jar file should not be on the classpath!", JDBC.HAVE_LUCENE_ANALYZERS );
            assertFalse( "Lucene query parser jar file should not be on the classpath!", JDBC.HAVE_LUCENE_QUERYPARSER );
        }
        else if (JDBC.HAVE_LUCENE_CORE && suffersFromDerby6650())
        {
            alarm("Lucene tests are skipped on this platform because of "
                    + "DERBY-6650. Please upgrade to Lucene 4.8 or higher "
                    + "if you would like to run them.");
        }
        else
        {
            suite.addTest(LuceneSupportTest.suite());
            suite.addTest(LuceneSupportPermsTest.suite());
            suite.addTest(LuceneCollationTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            suite.addTest(LuceneCoarseAuthorizationTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            suite.addTest(LuceneInMemoryTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            suite.addTest(LuceneBackupTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6600
            suite.addTest(LuceneJarLoadingTest.suite());
        }

        return suite;
	}

    /** Return the boolean value of a system property */
    private static  boolean getBooleanProperty( Properties properties, String key )
    {
        return Boolean.valueOf( properties.getProperty( key ) ).booleanValue();
    }

    /**
     * With Lucene versions up to 4.7, the Lucene plugin doesn't work on
     * platforms without JMX (in particular: Java SE 8 Compact Profile 2).
     * See DERBY-6650.
     */
    private static boolean suffersFromDerby6650() {
        if (JDBC.vmSupportsJMX()) {
            // Only platforms that lack JMX support have this problem.
            return false;
        }

        Class versionClass = null;
        try {
            versionClass = Class.forName("org.apache.lucene.util.Version");
        } catch (ClassNotFoundException cnfe) {
            fail("Could not check Lucene version", cnfe);
        }

        // Check if the version is at least 4.8. Do that by looking for the
        // existence of the LUCENE_48 field in the Version class. In 4.9
        // that field was deprecated and one called LUCENE_4_8 was added.
        // If we cannot find the former, look for the latter before giving up.
        try {
            versionClass.getField("LUCENE_48");
        } catch (NoSuchFieldException nsfe1) {
            try {
                versionClass.getField("LUCENE_4_8");
            } catch (NoSuchFieldException nsfe2) {
                // Neither the LUCENE_48 field nor the LUCENE_4_8 field is
                // present, so version is lower than 4.8. We suffer from
                // DERBY-6650.
                return true;
            }
        }

        // One of the fields indicating version 4.8 or higher was found,
        // so we don't suffer from DERBY-6650.
        return false;
    }
}
