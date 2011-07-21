/*

   Derby - Class org.apache.derbyTesting.functionTests.suites.All

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.suites;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.EnvTest;

public class All extends BaseTestCase {
      
    /**
     * Use suite method instead.
     */
    private All(String name) {
        super(name);
    }

    /**
     * Generate the {@code suites.All} test suite. Sub-suites should be added
     * to {@link AllPackages#suite()} and not here.
     */
    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite("All");

        // Simple "test" that displays environment information
        // as fixture names.
        suite.addTestSuite(EnvTest.class);
        
        // All package tests
        suite.addTest(AllPackages.suite());
        
        return suite;
    }
}
