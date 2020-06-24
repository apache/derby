/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools._Suite

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
package org.apache.derbyTesting.functionTests.tests.tools;

import junit.framework.Test;
import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.tools
 *
 */
public class _Suite extends BaseTestCase {

    /**
     * Use suite method instead.
     */
    private _Suite(String name) {
        super(name);
    }

    public static Test suite() {

        BaseTestSuite suite = new BaseTestSuite("tools");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(IJRunScriptTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2342
        suite.addTest(ImportExportTest.suite());
        suite.addTest(ImportExportBinaryDataTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-378
        suite.addTest(ImportExportLobTest.suite());
        suite.addTest(ImportExportProcedureTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3863
        suite.addTest(ImportExportIJTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5368
        suite.addTest(ij2Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2609
        suite.addTest(ToolScripts.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2903
        suite.addTest(SysinfoCPCheckTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3142
        suite.addTest(SysinfoLocaleTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4292
        suite.addTest(IjSecurityManagerTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5084
        suite.addTest(IjConnNameTest.suite());
        suite.addTest(RollBackWrappingWhenFailOnImportTest.suite());
        suite.addTest(ConnectWrongSubprotocolTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5084

        // running a jar file implies not using a module path
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        if (!JVMInfo.isModuleAware())
        {
            suite.addTest(derbyrunjartest.suite());
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5332
        suite.addTest(ij3Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6661
        suite.addTest(Test_6661.suite());
        
        // SysinfoAPITest currently fails when run against jars, so is
        // disabled. Only the first jar file on the classpath properly
        // returns its information through the sysinfo API.
        // See also DERBY-2343.
        //
        //suite.addTest(SysinfoAPITest.suite());

        // tests that do not run with JSR169
//IC see: https://issues.apache.org/jira/browse/DERBY-5374
        if (JDBC.vmSupportsJDBC3())  
        {
            suite.addTest(ij5Test.suite());            
        }
        
        // Tests that are compiled using 1.4 target need to
        // be added this way, otherwise creating the suite
        // will throw an invalid class version error
        if (JDBC.vmSupportsJDBC3() || JDBC.vmSupportsJSR169()) {
        }

        return suite;
    }
}
