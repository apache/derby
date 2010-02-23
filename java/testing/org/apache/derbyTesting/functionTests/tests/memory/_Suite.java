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
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;

public class _Suite extends BaseJDBCTestCase {

    public _Suite(String name) {
        super(name);
      
    }

    public static Test suite() throws Exception{
        TestSuite suite = new TestSuite("Memory Suite");
        //Disable following TriggerTests until DERBY-1482 has been fixed.
        //Without that fix, the test will run into OOM errors for all
        //the test fixtures. This test is written for triggers defined
        //on table with LOB columns. No matter whether the LoB columns
        //are touched in the trigger action, it appears that Derby is
        //streaming the before and after values of LOB columns. Once
        //the streaming problem has been resolved, we should be able
        //to uncomment the following test.
        //suite.addTest(TriggerTests.suite());
        suite.addTest(BlobMemTest.suite());
        suite.addTest(ClobMemTest.suite());
        suite.addTest(MultiByteClobTest.suite());
                return suite;
    }
}
