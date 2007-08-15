/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.ang.LangHarnessJavaTest

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
package org.apache.derbyTesting.functionTests.tests.lang;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.functionTests.util.HarnessJavaTest;

/**
 * Run a lang '.java' test from the old harness in the Junit infrastructure.
 * The test's output is compared to a master file using the facilities
 * of the super class CanonTestCase.
 * <BR>
 * This allows a faster switch to running all tests under a single
 * JUnit infrastructure. Running a test using this class does not
 * preclude it from being converted to a real JUnit assert based test.
 *
 */
public class LangHarnessJavaTest extends HarnessJavaTest {
    
    /**
     * Tests that run in both client and embedded.
     * Ideally both for a test of JDBC api functionality.
     */
    private static final String[] LANG_TESTS_BOTH =
    {
        //"AIjdbc",
    };
    
    /**
     * Tests that only run in embedded.
     */
    private static final String[] LANG_TESTS_EMEBDDED=
    {
        // old derbylang.runall suite
        //"AIjdbc",
        //"bug4356",
        //"bug5052rts",
        //"bug5054",
        //"casting", TODO: investigate/convert, uses TestUtil.isNetFramework()
        //"closed", TODO: investigate/convert
        "concateTests",
        // "currentof", being converted
        // "cursor", TODO: investigate/convert
        "dbManagerLimits",
        "deadlockMode",
        // "declareGlobalTempTableJava", TODO: investigate/convert
        // "declareGlobalTempTableJavaJDBC30", TODO: uses TestUtil.isNetFramework()
        // "errorStream", TODO: investigate/convert
        // "forbitdata", TODO: investigate/convert
        // "grantRevoke", TODO: uses TestUtil.isNetFramework()
        // "holdCursorJavaReflection", TODO don't care holdability supported.
        "JitTest",
        // "logStream", TODO: investigate/convert
        "longStringColumn",
        // "maxMemPerTab", TODO: investigate/convert
        // "outparams", TODO: investigate/convert
        // "procedure", TODO: uses TestUtil.isNetFramework()
        // "repeat", CONVERTED (StatementPlanCacheTest)
        "simpleThreadWrapper",
        // "SpillHash", TODO: calls System.exit
        // "stmtCache3", TODO: investigate/convert
        // "streams",TODO: convert
        // "timestampArith", TODO calls exit?
        // "triggerStream", TODO: investigate/convert - simple master update?
        // "unaryArithmeticDynamicParameter", TODO: investigate/convert - simple master update?
        // "updateCursor", TODO: investigate/convert 
        // "wisconsin", TODO: investigate/convert - needs ext files. 
        // "ShutdownDatabase", TODO: convert
        };
    
    private LangHarnessJavaTest(String name) {
        super(name);
     }

    protected String getArea() {
        return "lang";
    }

    /**
     * Run a set of language tests (.java files).
     *
     * @param args names of the tests to run (the .java suffix should not be
     * included in the name of a test)
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(baseSuite("main()", args));
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("jdbcapi: old harness java tests");
        suite.addTest(baseSuite("embedded", LANG_TESTS_BOTH));
        suite.addTest(baseSuite("embedded", LANG_TESTS_EMEBDDED));
        
        suite.addTest(TestConfiguration.clientServerDecorator(
                baseSuite("clientserver", LANG_TESTS_BOTH)));
        return suite;
    }
   
    private static Test baseSuite(String which, String[] set) {
        TestSuite suite = new TestSuite("lang: " + which);
        for (int i = 0; i < set.length; i++)
        {
            suite.addTest(decorate(new LangHarnessJavaTest(set[i])));
        }
        return suite;
    }
}
