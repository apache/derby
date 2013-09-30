/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.i18n.LocalizedDisplayScriptTest
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

package org.apache.derbyTesting.functionTests.tests.i18n;

import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.TimeZoneTestSetup;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * LocalizedDisplayScriptTest runs the ij script LocalizedDisplay.sql
 * and compares the output to a canon file in the standard master package.
 * <BR>
 * Its suite() method returns a set of tests where each test is an instance of
 * this class for an individual script wrapped in a clean database decorator.
 * <BR>
 * It can also be used as a command line program
 *
 */
public final class LocalizedDisplayScriptTest extends ScriptTestCase {

    /** The character encoding used in the script. */
    private static final String ENCODING = "EUC_JP";

    /**
     * Run LocalizedDisplay.sql 
     * <code>
     * example
     * java org.apache.derbyTesting.functionTests.tests.i18n.LocalizedSuite
     * </code>
     */
    public static void main(String[] args)
    {
        junit.textui.TestRunner.run(getSuite());
    }

    /**
     * Return the suite that runs the Localized script.
     */
    public static Test suite() {
        
        TestSuite suite = new TestSuite("LocalizedDisplay");

        // This test will fail with JSR169, because lack of support for 
        // rs.getBigDecimal() will prevent the localization of numeric and
        // decimal datatypes, and this test includes a decimal datatype
        // (See DERBY-470).
        if (JDBC.vmSupportsJSR169())
            return suite;

        // DERBY-5678: This test uses EUC_JP encoding. Implementations of the
        // Java platform are not required to support that encoding. Skip the
        // test if the encoding is not supported.
        if (!Charset.isSupported(ENCODING)) {
            println("Skip LocalizedDisplayScriptTest because the encoding " +
                    ENCODING + " is not supported");
            return suite;
        }

        TestSuite localizedEmbeddedTests = new TestSuite("LocalizedDisplay:embedded");
        localizedEmbeddedTests.addTest(getSuite());
        Test embeddedrun = TestConfiguration.singleUseDatabaseDecorator(localizedEmbeddedTests);
        // add the client test
        suite.addTest(embeddedrun);

        // It's not working to have both embedded and client run in the same
        // setting as the database doesn't get deleted until after the suite is done.
        // The second run will go against the already created & encoded database,
        // resulting in localized display by default, and thus a diff with the
        // master.
        // Set up the script's run with the network client
        TestSuite localizedTests = new TestSuite("LocalizedDisplay:client");
        localizedTests.addTest(getSuite());
        Test client = TestConfiguration.clientServerDecorator(
            TestConfiguration.singleUseDatabaseDecorator(localizedTests));
        // add the client test
        suite.addTest(client);

        return suite;
    }

    /*
     * A single JUnit test that runs a single Localized script.
     */
    private LocalizedDisplayScriptTest(String localizedTest){
        super(localizedTest, ENCODING);
    }

    /**
     * Return a localized test based on the script name. 
     * The test is surrounded in a decorator that sets up the
     * desired properties which is wrapped in a decorator
     * which sets up the timezone wrapped in a decorator
     * that cleans the database.
     */
    private static Test getSuite() {
        TestSuite suite = new TestSuite("localized Display");
        Properties uiProps = new Properties();
        uiProps.put("derby.ui.locale","es_AR");
        uiProps.put("derby.ui.codeset", ENCODING);
        suite.addTest(new TimeZoneTestSetup(new SystemPropertyTestSetup(
                new LocalizedDisplayScriptTest("LocalizedDisplay"), uiProps), 
                "America/Los_Angeles"));
        return getIJConfig(suite);
    }
    
}
