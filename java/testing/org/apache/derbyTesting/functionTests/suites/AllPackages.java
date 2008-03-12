/*

   Derby - Class org.apache.derbyTesting.functionTests.suites.AllPackages

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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;

/**
 * All package suites for the function tests.
 * 
 * Suites added:
 * <UL>
 * <LI> tests.lang
 * <LI> tests.jdbcapi
 * <LI> tests.tools
 * <LI> tests.jdbc4 (Java SE 6  only)
 * </UL>
 */
public class AllPackages extends BaseTestCase {
    /**
     * Use suite method instead.
     */
    private AllPackages(String name) {
        super(name);
    }

    public static Test suite() throws Exception {

        TestSuite suite = new TestSuite("AllPackages");

        suite.addTest(org.apache.derbyTesting.functionTests.tests.derbynet._Suite.suite());
        suite.addTest(org.apache.derbyTesting.functionTests.tests.lang._Suite.suite());
        suite.addTest(org.apache.derbyTesting.functionTests.tests.jdbcapi._Suite.suite());
        suite.addTest(org.apache.derbyTesting.functionTests.tests.store._Suite.suite());
	 suite.addTest(org.apache.derbyTesting.functionTests.tests.tools._Suite.suite());
        suite.addTest(org.apache.derbyTesting.functionTests.tests.engine._Suite.suite());
        suite.addTest(org.apache.derbyTesting.functionTests.tests.demo._Suite.suite());

        // Suites that are compiled using Java SE 6 target need to
        // be added this way, otherwise creating the suite
        // will throw an invalid class version error
        suite.addTest(
                    addSuiteByReflection(
                            "org.apache.derbyTesting.functionTests.tests.jdbc4._Suite"));
        
        // JMX management tests are compiled and require JDK 1.5
        suite.addTest(
                addSuiteByReflection(
                        "org.apache.derbyTesting.functionTests.tests.management._Suite"));

        // Adding JUnit unit tests here to avoid creating a new JUnit
        // harness above the functionTests and unitTests
        // directories(packages)
        suite.addTest(org.apache.derbyTesting.unitTests.junit._Suite.suite());
        
        // Add the upgrade tests,See upgradeTests._Suite
        // for more information on how the old jars are
        // located. If the system property derbyTesting.oldReleasePath
        // is not set then the jars will be loaded from the Apache SVN repo.
        suite.addTest(
           org.apache.derbyTesting.functionTests.tests.upgradeTests._Suite.suite());

        return suite;
    }
    
    /**
     * Get a class's set of tests from its suite method through reflection.
     */
    private static Test addSuiteByReflection(String className) throws Exception
    {
        try {
            Class clz = Class.forName(className);
            
            Method sm = clz.getMethod("suite", null);
                  
            return (Test) sm.invoke(null, null);
        } catch (LinkageError  e) {
            return new TestSuite("SKIPPED: " + className + " - " +
                    e.getMessage());
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            if (cause instanceof LinkageError) {
               return new TestSuite("SKIPPED: " + className + " - " +
                       cause.getMessage());
            } else {
               throw ite;
            }
        }
    }

}
