/*

Derby - Class org.apache.derbyTesting.functionTests.tests.largedata._Suite

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/
package org.apache.derbyTesting.functionTests.tests.largedata;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;

public class _Suite extends BaseJDBCTestCase {

    public _Suite(String name) {
        super(name);
    }

    /**
     * Suite runs first the lite suite for both embedded and client with 
     * LobLimitsLiteTest.
     * Then runs the full embeddded suite with LobLimitsTest 
     * Then runs the full client suite with LobLimitsClientTest.
     * The full suite may take a very long time.
     */
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("largedata suite");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(Derby6317Test.suite());
        // DERBY-5624, currently this runs out of file descriptors on unix
        // systems with 1024 limit per user.  Setting to run only on windows
        // until solution for unix is found.
        if (isWindowsPlatform())
            suite.addTest(Derby5624Test.suite());

        suite.addTest(LobLimitsLiteTest.suite());
        suite.addTest(LobLimitsTest.suite());
        suite.addTest(LobLimitsClientTest.suite());
        suite.addTest(Derby6884Test.suite());
        return suite;
    }
}
