/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.ClientCompatibilitySuite

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
package org.apache.derbyTesting.functionTests.tests.compatibility;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.functionTests.tests.jdbcapi.Compat_BlobClob4BlobTest;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Returns the test suite run by each client in the compatibility test.
 * <p>
 * This is where one would add the tests from a new test class to be included
 * in the compatibility suite.
 */
public class ClientCompatibilitySuite
        extends BaseTestCase {

    public ClientCompatibilitySuite(String name) {
        super(name);
        throw new IllegalStateException(
                "use ClientCompatibilitySuite.suite() instead");
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("Client compatibility suite");
        suite.addTest(JDBCDriverTest.suite());
        // Adding the LOB suite adds around 5 minutes to each client-server
        // combination. There are also errors and/or failures when run with
        // clients older than 10.5, at least for some server versions.
        if (Boolean.parseBoolean(getSystemProperty(
                ClientCompatibilityRunControl.LOB_TESTING_PROP))) {
            suite.addTest(Compat_BlobClob4BlobTest.suite());
        }
        return suite;
    }
}
