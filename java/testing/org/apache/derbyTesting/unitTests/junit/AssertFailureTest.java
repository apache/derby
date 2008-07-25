/*
   Derby - Class org.apache.derbyTesting.unitTests.junit.AssertFailureTest

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

package org.apache.derbyTesting.unitTests.junit;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.shared.common.sanity.AssertFailure;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.SecurityManagerSetup;

/**
 * Testcase that test that AssertFailure's message string is correct when we
 * have permisssion to do thread dumps and when we don't. Depends on the
 * policyfiles AssertFailureTest.policy and AssertFailureTest1.policy where only
 * the former grants this permission.
 */
public class AssertFailureTest extends BaseTestCase {

    /**
     * Security policy file that allows thread dumps.
     */
    private static String POLICY_FILENAME =
        "org/apache/derbyTesting/unitTests/junit/AssertFailureTest.policy";

    /**
     * Security policy file that DOESN'T allow thread dumps.
     */
    private static String NO_DUMP_POLICY_FILENAME =
        "org/apache/derbyTesting/unitTests/junit/AssertFailureTest1.policy";

    public AssertFailureTest(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("AssertFailureTest");

        try {
            //Only add the tests if this is a sane build.
            Class.forName("org.apache.derby.shared.common.sanity." +
            		"AssertFailure");

            // Run with thread dump permissions
            suite.addTest(new SecurityManagerSetup(new AssertFailureTest(
                "testAssertFailureThreadDump"), POLICY_FILENAME));

            // Run WITHOUT thread dump permissions
            suite.addTest(new SecurityManagerSetup(new AssertFailureTest(
                "testAssertFailureNoThreadDump"), NO_DUMP_POLICY_FILENAME));

        } catch (ClassNotFoundException e) {
            //Ignore. Just return an empty suite.
        }
        return suite;
    }

    /**
     * Test that AssertFailure's message string is correct when we have
     * permisssion to do thread dumps. Must be run with correct permissions, ie.
     * with java.lang.RuntimePermission "getStackTrace" and
     * java.lang.RuntimePermission "modifyThreadGroup".
     */
    public void testAssertFailureThreadDump() {

        String s = new AssertFailure("AssertFailureTest").getThreadDump();
        //System.out.println(s);    //Debug failures

        // Assert that the string is correct, by checking that
        // it starts the right way.
        if (JVMInfo.JDK_ID >= JVMInfo.J2SE_15) {
            String expected = "---------------\n" +
            		"Stack traces for all live threads:\nThread name=";

            assertTrue("String not correct. Expected to start with:\n<"
                + expected + ">...\nWas:\n<" + s + ">.\n" ,
                s.startsWith(expected));

        } else {
            String expected = "(Skipping thread dump because it is not " +
            		"supported on JVM 1.4)\n";

            assertEquals("String not correct.", expected, s);
        }
    }

    /**
     * Test that AssertFailure's log entry is correct when we DON'T have
     * permisssion to to thread dumps. Must be run with correct permissions, ie.
     * WITHOUT java.lang.RuntimePermission "getStackTrace" and
     * java.lang.RuntimePermission "modifyThreadGroup";
     */
    public void testAssertFailureNoThreadDump() {

        String s = new AssertFailure("AssertFailureTest").getThreadDump();
        //System.out.println(s);    //Debug failures.

        // Assert that the string is correct, by checking that is starts
        // the right way.
        if (JVMInfo.JDK_ID >= JVMInfo.J2SE_15) {
            String expected = "(Skipping thread dump because of insufficient " +
            		"permissions:\njava.security.AccessControlException:";

            assertTrue("String not correct. Expected: <" + expected +
                ">\nWas:\n<" + s + ">", s.startsWith(expected));

        } else {
            String expected = "(Skipping thread dump because it is not " +
                "supported on JVM 1.4)\n";

            assertEquals("String not correct.", expected, s);
        }
    }
}
