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
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseTestCase;

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
        TestSuite suite = new TestSuite("engine");

        suite.addTest(ErrorStreamTest.suite());
        suite.addTest(LockInterruptTest.suite());
        // for now disable on IBM 1.7 DERBY-5434
        if (!(isIBMJVM() && isJava7()))
            suite.addTest(RestrictiveFilePermissionsTest.suite());
        suite.addTest(ModuleLoadingTest.suite());
        suite.addTest(ReadMeFilesTest.suite());

        return suite;
    }
}
