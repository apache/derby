/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet._Suite

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
package org.apache.derbyTesting.functionTests.tests.derbynet;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;

import junit.framework.Test; 
import junit.framework.TestSuite;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.derbynet
 *
 */
public class _Suite extends BaseTestCase  {

    /**
     * Use suite method instead.
     */
    private _Suite(String name) {
        super(name);
    }
    
    public static Test suite() {

        TestSuite suite = new TestSuite("derbynet");
        suite.addTest(PrepareStatementTest.suite());
        suite.addTest(NetworkServerControlApiTest.suite());     
        suite.addTest(ShutDownDBWhenNSShutsDownTest.suite());
        suite.addTest(DRDAProtocolTest.suite());
        suite.addTest(ClientSideSystemPropertiesTest.suite());
        suite.addTest(BadConnectionTest.suite());
        suite.addTest(NetHarnessJavaTest.suite());
        suite.addTest(SecureServerTest.suite());
        suite.addTest(SSLTest.suite());
        suite.addTest(NetIjTest.suite());
        suite.addTest(NSinSameJVMTest.suite());
        
        
        // Disabled due to "java.sql.SQLSyntaxErrorException: The class
        // 'org.apache.derbyTesting.functionTests.tests.derbynet.checkSecMgr'
        //  does not exist or is inaccessible. This can happen if the class is not public."
        //  in the nightly tests with JDK 1.6 and jar files.
        //suite.addTest(CheckSecurityManager.suite());
        
        // this test refers to ConnectionPooledDataSource class
        // thus causing class not found exceptions with JSR169
        if (JDBC.vmSupportsJDBC3())
        {
            suite.addTest(NSSecurityMechanismTest.suite());
        }
        
        // These tests references a client class directly
        // thus causing class not found exceptions if the
        // client code is not in the classpath.
        if (Derby.hasClient()) {
            suite.addTest(ByteArrayCombinerStreamTest.suite());
            suite.addTest(SqlExceptionTest.suite());
        }
 
        return suite;
    }
    
}
