/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.suites.XMLSuite
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.suites;

import org.apache.derbyTesting.junit.BaseTestCase;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Run all of the XML JUnit tests as a single suite.
 * This suite is included in lang._Suite but is at
 * this level to allow easy running of just the XML tests.
 */
public final class XMLSuite extends BaseTestCase {

    /**
     * Use suite method instead.
     */
    private XMLSuite(String name)
    {
        super(name);
    }

    /**
     * Return the suite that runs the XML tests.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("XML Suite");
        
        // Add all JUnit tests for XML.
        suite.addTest(org.apache.derbyTesting.functionTests.tests.lang.XMLTypeAndOpsTest.suite());
        suite.addTest(org.apache.derbyTesting.functionTests.tests.lang.XMLBindingTest.suite());
        suite.addTest(org.apache.derbyTesting.functionTests.tests.lang.XMLMissingClassesTest.suite());
        
        return suite;
    }
}
