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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationSuite;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;

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

        BaseTestSuite suite = new BaseTestSuite("AllPackages");

        for (Iterator it = getTestClasses().iterator(); it.hasNext(); ) {
            Object testClass = it.next();
            if (testClass instanceof String) {
                suite.addTest(addSuiteByReflection((String) testClass));
            } else {
                suite.addTest(invokeSuite((Class) testClass));
            }
        }

        return suite;
    }

    /**
     * <p>
     * Get a list of test classes to add. The classes that have been compiled
     * with target level equal to the lowest supported level are included as
     * {@code java.lang.Class} objects. Classes compiled with higher target
     * levels are included as {@code java.lang.String}s with the class names
     * so that this method does not fail with class not found errors on some
     * platforms.
     * </p>
     *
     * <p>
     * To construct a test suite from these classes, the classes' static
     * {@code suite()} methods have to be called.
     * </p>
     *
     * @return list of test classes
     */
    private static List getTestClasses() {
        ArrayList<Object> classes = new ArrayList<Object>();

        classes.add(org.apache.derbyTesting.functionTests.tests.derbynet._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.lang._Suite.class);
        
        if (shortCircuitFor_derby_7011()) { return classes; }

        // DERBY-1903
        // For the largedata test, just run the lite version of the test as
        // the full test is too big.
        classes.add(
             org.apache.derbyTesting.functionTests.tests.largedata.LobLimitsLiteTest.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.jdbcapi._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.store._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.storetests._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.tools._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.engine._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.demo._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.memory._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.memorydb._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.i18n._Suite.class);
        classes.add(org.apache.derbyTesting.functionTests.tests.multi.StressMultiTest.class);

        // Suites that are compiled using Java SE 6 target need to
        // be added this way, otherwise creating the suite
        // will throw an invalid class version error
        classes.add("org.apache.derbyTesting.functionTests.tests.jdbc4._Suite");
        
        // JMX management tests are compiled and require JDK 1.5
        classes.add("org.apache.derbyTesting.functionTests.tests.management._Suite");

        // Adding JUnit unit tests here to avoid creating a new JUnit
        // harness above the functionTests and unitTests
        // directories(packages)
        classes.add(org.apache.derbyTesting.unitTests.junit._Suite.class);
        
        // Add the upgrade tests,See upgradeTests._Suite
        // for more information on how the old jars are
        // located. If the system property derbyTesting.oldReleasePath
        // is not set then the jars will be loaded from the Apache SVN repo.
        classes.add(
           org.apache.derbyTesting.functionTests.tests.upgradeTests._Suite.class);

        // Encrypted tests
        // J2ME (JSR169) does not support encryption.
        classes.add(EncryptionSuite.class);

        // Replication tests. Implementation require DataSource.
        // Not supp. by JSR169
        classes.add(ReplicationSuite.class);

        // Compatibility tests (MATS)
        classes.add("org.apache.derbyTesting.functionTests.tests.compatibility._Suite");

        return classes;
    }
    
    /**
     * Get a class's set of tests from its suite method through reflection.
     * Ignore errors caused by the class version of the test class being
     * higher than what's supported on this platform.
     */
    private static Test addSuiteByReflection(String className) throws Exception
    {
        try {
            return invokeSuite(Class.forName(className));
        } catch (LinkageError  e) {
            return new BaseTestSuite("SKIPPED: " + className + " - " +
                    e.getMessage());
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            if (cause instanceof LinkageError) {
               return new BaseTestSuite("SKIPPED: " + className + " - " +
                       cause.getMessage());
            } else {
                System.err.println("FAILED to invoke " + className);
                ite.printStackTrace();
               throw ite;
            }
        } catch (ClassNotFoundException ce) { // Do not add a suite not built.
            return new BaseTestSuite("SKIPPED: Class not found: " + className +
                    " - " + ce.getMessage());
        }
    }

    /**
     * Invoke the static {@code suite()} method on a test class.
     *
     * @param klass the test class
     * @return the test suite returned by {@code suite()}
     * @throws Exception if the suite() method cannot be called or fails
     */
    private static Test invokeSuite(Class<?> klass) throws Exception {
        try {
            return (Test) klass.getMethod("suite").invoke(null);
        } catch (Exception e) {
            System.err.println("Failed to invoke class " + klass.getName());
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Get the class names of all the top-level JUnit test suites that are
     * included in {@code suites.All}.
     *
     * @return an array containing the class names of all the top-level
     * test suites
     */
    public static String[] getTopLevelSuiteNames() {
        List testClasses = getTestClasses();
        String[] names = new String[testClasses.size()];

        for (int i = 0; i < testClasses.size(); i++) {
            Object testClass = testClasses.get(i);
            if (testClass instanceof String) {
                names[i] = (String) testClass;
            } else {
                names[i] = ((Class) testClass).getName();
            }
        }

        return names;
    }

    /**
     * Print the class names of all the test suites included in
     * {@code suites.All}.
     *
     * @param args command line arguments (ignored)
     */
    public static void main(String[] args) {
        String[] names = getTopLevelSuiteNames();
        for (int i = 0; i < names.length; i++) {
            System.out.println(names[i]);
        }
    }
}
