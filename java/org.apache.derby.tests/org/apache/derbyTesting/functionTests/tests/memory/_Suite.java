/**
 * Derby - Class org.apache.derbyTesting.functionTests.tests.memory._Suite
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.memory;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;

public class _Suite extends BaseJDBCTestCase {

    public _Suite(String name) {
        super(name);
      
    }

    public static Test suite() throws Exception{
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("Memory Suite");
//IC see: https://issues.apache.org/jira/browse/DERBY-4538
        suite.addTest(TriggerTests.suite());
        suite.addTest(BlobMemTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3741
        suite.addTest(ClobMemTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3741
        suite.addTest(MultiByteClobTest.suite());
        suite.addTest(RolesDependencyTest.suite());
        suite.addTest(Derby3009Test.suite());
        suite.addTest(Derby5730Test.suite());
        suite.addTest(MemoryLeakFixesTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4137
        suite.addTest(XAMemTest.suite());
        return suite;
    }
}
