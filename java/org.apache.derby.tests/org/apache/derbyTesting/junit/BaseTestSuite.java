/*

   Derby - Class org.apache.derbyTesting.junit.BaseTestSuite

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
   under the License.

 */
package org.apache.derbyTesting.junit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Derby replacement for {@code junit.framework.TestSuite}. This version, when
 * provided with a {@code Class} argument in a constructor or method,
 * constructs a {@code TestSuite} with a lexicographically sorted set of
 * fixtures (i.e. test cases) to avoid random fixture ordering (after Java
 * 6). Its usage is similar to the original JUnit {@code TestSuite} class.
 */
public class BaseTestSuite extends TestSuite {

    public BaseTestSuite() {
        super();
    }

    public BaseTestSuite(String name) {
        super(name);
    }

    public BaseTestSuite(Class cls, String name) {
        super(name);
        orderedSuite(cls);
    }

    public BaseTestSuite(Class cls) {
        super(TestConfiguration.suiteName(cls));
        orderedSuite(cls);
    }

    @Override
    public void addTestSuite(Class cls) {
        orderedSuite(cls);
    }

    private void orderedSuite(Class<?> cls) {
        // Extract all tests from the test class and order them.
        ArrayList<TestCase> tests = new ArrayList<TestCase>();

        Enumeration<?> e = new TestSuite(cls).tests();

        while (e.hasMoreElements()) {
            tests.add((TestCase) e.nextElement());
        }

        Collections.sort(tests, TEST_ORDERER);

        for (TestCase t : tests) {
            addTest(t);
        }
    }

    /**
     * A comparator that orders {@code TestCase}s lexicographically by
     * their names.
     */
    private static final Comparator<TestCase> TEST_ORDERER =
            new Comparator<TestCase>() {
        @Override
        public int compare(TestCase t1, TestCase t2) {
            return t1.getName().compareTo(t2.getName());
        }
    };


}
