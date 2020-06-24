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

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;

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

    public static Test suite() throws Exception {

//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("derbynet");
//IC see: https://issues.apache.org/jira/browse/DERBY-2100
        suite.addTest(PrepareStatementTest.suite());
        suite.addTest(ShutDownDBWhenNSShutsDownTest.suite());
        suite.addTest(DRDAProtocolTest.suite());
        suite.addTest(ClientSideSystemPropertiesTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2305
        suite.addTest(BadConnectionTest.suite());
        suite.addTest(NetHarnessJavaTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2196
        suite.addTest(SecureServerTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3829
        suite.addTest(SysinfoTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2274
        suite.addTest(SSLTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3834
        suite.addTest(RuntimeInfoTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2505
        suite.addTest(NetIjTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3211
        suite.addTest(NSinSameJVMTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3273
        suite.addTest(NetworkServerControlClientCommandTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
        suite.addTest(ServerPropertiesTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3571
        suite.addTest(LOBLocatorReleaseTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3796
        suite.addTest(OutBufferedStreamTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3717
        suite.addTest(GetCurrentPropertiesTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-728
        suite.addTest(Utf8CcsidManagerTest.suite());
        suite.addTest(DerbyNetAutoStartTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3838

        // Disabled due to "java.sql.SQLSyntaxErrorException: The class
        // 'org.apache.derbyTesting.functionTests.tests.derbynet.checkSecMgr'
        //  does not exist or is inaccessible. This can happen if the class is not public."
        //  in the nightly tests with JDK 1.6 and jar files.
        //suite.addTest(CheckSecurityManager.suite());

 
        if (JDBC.vmSupportsJDBC3())
        {
            // this test refers to ConnectionPooledDataSource class
            // thus causing class not found exceptions with JSR169
//IC see: https://issues.apache.org/jira/browse/DERBY-1982
//IC see: https://issues.apache.org/jira/browse/DERBY-1496
//IC see: https://issues.apache.org/jira/browse/DERBY-1496
            suite.addTest(NSSecurityMechanismTest.suite());
            // Test does not run on J2ME    
//IC see: https://issues.apache.org/jira/browse/DERBY-4765
            suite.addTest(DerbyNetNewServerTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2031
            suite.addTest(ProtocolTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6162
            suite.addTest(NetworkServerControlApiTest.suite());
        }

        // These tests references a client class directly
        // thus causing class not found exceptions if the
        // client code is not in the classpath.
//IC see: https://issues.apache.org/jira/browse/DERBY-2213
        if (Derby.hasClient()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2054
            suite.addTest(ByteArrayCombinerStreamTest.suite());
            suite.addTest(SqlExceptionTest.suite());
            suite.addTest(Utf8CcsidManagerClientTest.suite());
        }

        return suite;
    }

}
