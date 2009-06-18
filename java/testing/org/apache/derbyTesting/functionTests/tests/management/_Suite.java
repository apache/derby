/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management._Suite

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
package org.apache.derbyTesting.functionTests.tests.management;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseTestCase;


/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.management
 *
 */
public class _Suite extends BaseTestCase {

    /**
     * Use suite method instead.
     */
    private _Suite(String name) {
        super(name);
    }

    /**
     * Creates a JUnit test suite containing all the tests (subsuites) in this
     * package. The number of tests included may depend on the environment in
     * which this method is run. 
     * 
     * @return A test suite containing all tests in this package
     */
    public static Test suite() {

        TestSuite suite = new TestSuite("management");

        suite.addTest(JMXTest.suite());
        suite.addTest(ManagementMBeanTest.suite());
        suite.addTest(InactiveManagementMBeanTest.suite());
        suite.addTest(VersionMBeanTest.suite());
        suite.addTest(JDBCMBeanTest.suite());
        suite.addTest(NetworkServerMBeanTest.suite());
        suite.addTest(CustomMBeanServerBuilderTest.suite());
        return suite;
    }
}
