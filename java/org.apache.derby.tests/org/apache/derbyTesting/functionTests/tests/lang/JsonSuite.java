/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.JsonSuite

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
 * Suite holding all of the tests for the optional simple json support.
 *
 */
public class JsonSuite extends BaseTestCase
{

	/**
	 * Use suite method instead.
	 */
	private JsonSuite(String name) { super(name); }

	public static Test suite()
    {
        BaseTestSuite suite = new BaseTestSuite("JsonSuite");
        Properties  properties = TestConfiguration.getSystemProperties();
        
        //
        // If we're told to omit the json tests, make sure
        // that the json jar file isn't on the classpath.
        //
        if ( getBooleanProperty( properties, TestConfiguration.KEY_OMIT_JSON ) )
        {
            assertFalse
                ( "The json-simple jar file should not be on the classpath!", JDBC.HAVE_JSON_SIMPLE );
        }
        else
        {
            suite.addTest(SimpleJsonTest.suite());
            suite.addTest(SimpleJsonPermsTest.suite());
        }

        return suite;
	}

    /** Return the boolean value of a system property */
    private static  boolean getBooleanProperty( Properties properties, String key )
    {
        return Boolean.valueOf( properties.getProperty( key ) ).booleanValue();
    }

}
