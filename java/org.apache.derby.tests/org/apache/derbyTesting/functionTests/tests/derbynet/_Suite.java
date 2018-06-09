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

        BaseTestSuite suite = new BaseTestSuite("derbynet");
        suite.addTest(PrepareStatementTest.suite());
        suite.addTest(ShutDownDBWhenNSShutsDownTest.suite());
        suite.addTest(DRDAProtocolTest.suite());
        suite.addTest(ClientSideSystemPropertiesTest.suite());
        suite.addTest(BadConnectionTest.suite());
        suite.addTest(NetHarnessJavaTest.suite());
        suite.addTest(SecureServerTest.suite());
        suite.addTest(SysinfoTest.suite());
        suite.addTest(SSLTest.suite());
        suite.addTest(RuntimeInfoTest.suite());
        suite.addTest(NetIjTest.suite());
        suite.addTest(NSinSameJVMTest.suite());
        suite.addTest(NetworkServerControlClientCommandTest.suite());
        suite.addTest(ServerPropertiesTest.suite());
        suite.addTest(LOBLocatorReleaseTest.suite());
        suite.addTest(OutBufferedStreamTest.suite());
        suite.addTest(GetCurrentPropertiesTest.suite());
        suite.addTest(Utf8CcsidManagerTest.suite());
        suite.addTest(DerbyNetAutoStartTest.suite());

        // Disabled due to "java.sql.SQLSyntaxErrorException: The class
        // 'org.apache.derbyTesting.functionTests.tests.derbynet.checkSecMgr'
        //  does not exist or is inaccessible. This can happen if the class is not public."
        //  in the nightly tests with JDK 1.6 and jar files.
        //suite.addTest(CheckSecurityManager.suite());

 
        if (JDBC.vmSupportsJDBC3())
        {
            // this test refers to ConnectionPooledDataSource class
            // thus causing class not found exceptions with JSR169
            suite.addTest(NSSecurityMechanismTest.suite());
            // Test does not run on J2ME    
            suite.addTest(DerbyNetNewServerTest.suite());
            suite.addTest(ProtocolTest.suite());
            suite.addTest(NetworkServerControlApiTest.suite());
        }

        // These tests references a client class directly
        // thus causing class not found exceptions if the
        // client code is not in the classpath.
        if (Derby.hasClient()) {
            suite.addTest(ByteArrayCombinerStreamTest.suite());
            suite.addTest(SqlExceptionTest.suite());
            suite.addTest(Utf8CcsidManagerClientTest.suite());
        }

        return suite;
    }

}
