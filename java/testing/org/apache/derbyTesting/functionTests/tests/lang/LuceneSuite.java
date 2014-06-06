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

import org.apache.derbyTesting.junit.BaseTestCase;

import java.util.Properties;

import junit.framework.Test; 
import junit.framework.TestSuite;

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
		TestSuite suite = new TestSuite("LuceneSuite");
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
        else
        {
            suite.addTest(LuceneSupportTest.suite());
            suite.addTest(LuceneSupportPermsTest.suite());
            suite.addTest(LuceneCollationTest.suite());
            suite.addTest(LuceneCoarseAuthorizationTest.suite());
            suite.addTest(LuceneInMemoryTest.suite());
            suite.addTest(LuceneBackupTest.suite());
            suite.addTest(LuceneJarLoadingTest.suite());
        }

        return suite;
	}

    /** Return the boolean value of a system property */
    private static  boolean getBooleanProperty( Properties properties, String key )
    {
        return Boolean.valueOf( properties.getProperty( key ) ).booleanValue();
    }
    
}
