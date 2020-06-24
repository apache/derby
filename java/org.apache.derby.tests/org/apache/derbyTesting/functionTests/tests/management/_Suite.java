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
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;


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

        BaseTestSuite suite = new BaseTestSuite("management");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

//IC see: https://issues.apache.org/jira/browse/DERBY-6097
        if (JDBC.vmSupportsJMX()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
            suite.addTest(JMXTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1387
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
            suite.addTest(ManagementMBeanTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3424
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
            suite.addTest(InactiveManagementMBeanTest.suite());
            suite.addTest(VersionMBeanTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3385
            suite.addTest(JDBCMBeanTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
            suite.addTest(NetworkServerMBeanTest.suite());
            suite.addTest(CustomMBeanServerBuilderTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
            suite.addTest(CacheManagerMBeanTest.suite());
        }

        return suite;
    }
}
