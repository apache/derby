/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests._Suite

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Run the full upgrade suite. This is the only
 * way to run tests in this package.
 *
 */
public class _Suite extends BaseTestCase {
    
    private static final int[][] OLD_VERSIONS =
    {
        // {10, 0, 2, 1}, // 10.0.2.1 (incubator release)
        {10, 1, 1, 0}, // 10.1.1.0 (Aug 3, 2005 / SVN 208786)
        {10, 1, 2, 1}, // 10.1.2.1 (Nov 18, 2005 / SVN 330608)
        {10, 1, 3, 1}, // 10.1.3.1 (Jun 30, 2006 / SVN 417277)
        // {10, 2, 1, 6}, // 10.2.1.6 (Oct 02, 2006 / SVN 452058)
        // {10, 2, 1, 6}, // 10.2.2.0 (Dec 12, 2006 / SVN 485682)
    };


    /**
     * Use suite method instead.
     */
    private _Suite(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        TestSuite suite = new TestSuite("Upgrade Suite");       

        for (int i = 0; i < OLD_VERSIONS.length; i++) {
            suite.addTest(UpgradeRun.suite(OLD_VERSIONS[i]));
        }
        
        return suite;
    }
    

}
