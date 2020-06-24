/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.engine._Suite

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
package org.apache.derbyTesting.functionTests.tests.engine;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.engine
 * <P>
 * All tests are run "as-is", just as if they were run
 * individually. Thus this test is just a collection
 * of all the JUnit tests in this package (excluding itself).
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
        BaseTestSuite suite = new BaseTestSuite("engine");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(ErrorStreamTest.suite());
        suite.addTest(LockInterruptTest.suite());
        // for now disable on IBM 1.7 DERBY-5434
        if (!(isIBMJVM() && isJava7()))
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
            suite.addTest(RestrictiveFilePermissionsTest.suite());
        suite.addTest(ModuleLoadingTest.suite());
        suite.addTest(ReadMeFilesTest.suite());
        suite.addTest(ShutdownWithoutDeregisterPermissionTest.suite());
        suite.addTest(Derby6396Test.suite());

        return suite;
    }
}
