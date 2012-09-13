/*

Derby - Class org.apache.derbyTesting.functionTests.tests.largedata.LobLimitsLiteTest

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

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;



/**
 * LobLimitsLiteTest tests all the same functionality of 
 * LobLimitsTest but with smaller Clobs and Blobs, so that
 * the test can be run with suites.All. This 
 * helps us ensure the basic functionality does not regress.
 *   
 */

public class LobLimitsLiteTest extends LobLimitsTest {

    public LobLimitsLiteTest(String name) {
        super(name);
   
    }

    static final int _1MB = 1024*1024;
    static final int _100K = 1024 *100;
    
    public static Test suite() {
        Test test = LobLimitsTest.baseSuite(_1MB, _100K);
        TestSuite suite = new TestSuite("LobLimitsLiteTest");
        suite.addTest(test);
        suite.addTest(TestConfiguration.clientServerDecorator(test));
        return suite;
        
    }

}
