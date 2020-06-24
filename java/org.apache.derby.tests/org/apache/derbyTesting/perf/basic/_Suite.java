/*

Derby - Class org.apache.derbyTesting.perf.basic._Suite

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
package org.apache.derbyTesting.perf.basic;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.perf.basic.jdbc.CountTest;
import org.apache.derbyTesting.perf.basic.jdbc.CoveredIdxScan;
import org.apache.derbyTesting.perf.basic.jdbc.HeapScan;
import org.apache.derbyTesting.perf.basic.jdbc.SelectDistinctTest;
import org.apache.derbyTesting.perf.basic.jdbc.SortTest;
import org.apache.derbyTesting.perf.basic.jdbc.ValuesTest;

/**
 * Basic Performance Suite
 */
public class _Suite extends BaseTestCase{
    
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("BasicPerf");
        
        suite.addTest(ValuesTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2200
        suite.addTest(CountTest.suite());
        suite.addTest(HeapScan.suite());
        suite.addTest(CoveredIdxScan.suite());
        suite.addTest(SortTest.suite());
        suite.addTest(SelectDistinctTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3981

        return suite;
    }
    
    public _Suite(String name) {
        super(name);
    }
}
