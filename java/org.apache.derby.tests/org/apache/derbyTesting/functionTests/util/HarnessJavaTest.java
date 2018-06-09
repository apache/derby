/*

   Derby - Class org.apache.derbyTesting.functionTests.util.HarnessJavaTest

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
package org.apache.derbyTesting.functionTests.util;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Run a '.java' test from the old harness in the Junit infrastructure.
 * The test's output is compared to a master file using the facilities
 * of the super class CanonTestCase.
 * <BR>
 * This allows a faster switch to running all tests under a single
 * JUnit infrastructure. Running a test using this class does not
 * preclude it from being converted to a real JUnit assert based test.
 *
 */
public abstract class HarnessJavaTest extends CanonTestCase {

    private static final Object[] MAIN_ARG = new Object[] {new String[0]};
    private static final Class[] MAIN_ARG_TYPE = new Class[] {MAIN_ARG[0].getClass()};

    /**
     * Create a test, the name corresonds to the class name
     * of the test (without any package information).
     * @param name
     */
    protected HarnessJavaTest(String name) {
        super(name);
    }
    
    /**
     * Return the folder of the test, such as 'jdbcapi' or 'lang'.
     */
    protected abstract String getArea();
    
    public void runTest() throws Throwable
    {
        
        String testClassName =
            "org.apache.derbyTesting.functionTests.tests."
            + getArea() + "." + getName();

        
        String canon =
            "org/apache/derbyTesting/functionTests/master/"
            + getName() + ".out";

        
        PrintStream out = System.out;
        PrintStream testOut = new PrintStream(getOutputStream(),
                false, outputEncoding);
        setSystemOut(testOut);
                
        Class<?> test = Class.forName(testClassName);
        
        Method main = test.getDeclaredMethod("main", MAIN_ARG_TYPE);
        
        main.invoke(null, MAIN_ARG);
        
        setSystemOut(out);
          
        compareCanon(canon);
    }
    
    /**
     * Decorate a HarnessJavaTest test. Any sub-class must
     * call this decorator when adding a test to a suite.
     * This sets up the ij system properties to setup
     * the default connection to be to the default database.
     * The lock timeouts are also shortened and the test
     * will start in a clean database.
     */
    protected static Test decorate(HarnessJavaTest test)
    {
       Test dtest = new SystemPropertyTestSetup(test, new Properties())
        {
            protected void setUp() throws java.lang.Exception
            {
                TestConfiguration config = TestConfiguration.getCurrent();
                
                // With JDBC 3 connect using a JDBC URL
                if (JDBC.vmSupportsJDBC3())
                {
                   newValues.setProperty(
                        "ij.database", 
                        config.getJDBCUrl());
                }
                super.setUp();
            }
        };
        
        dtest = DatabasePropertyTestSetup.setLockTimeouts(dtest, 4, 6);
        dtest = new CleanDatabaseTestSetup(dtest);
        
        return dtest;
    }
}
