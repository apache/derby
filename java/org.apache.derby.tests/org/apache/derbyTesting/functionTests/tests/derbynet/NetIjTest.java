/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.NetIjTests
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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Network client .sql tests to run via ij.
 */
/**
 * NetScripts runs ij scripts (.sql files) in the derbynet package
 * and compares the output to a canon file in the standard master package.
 * <BR>
 * Its suite() method returns a set of tests where each test is an instance of
 * this class for an individual script wrapped in a clean database decorator.
 * <BR>
 * It can also be used as a command line program to run one or more
 * ij scripts as tests.
 *
 */
public final class NetIjTest extends ScriptTestCase {

    /**
     * scripts (.sql files) - only run in client.
     */
    private static final String[] CLIENT_TESTS = {
//IC see: https://issues.apache.org/jira/browse/DERBY-3088
        "testclientij",
    };

    /**
     * Run a set of scripts (.sql files) passed in on the
     * command line. Note the .sql suffix must not be provided as
     * part of the script name.
     * <code>
     * example
     * java org.apache.derbyTesting.functionTests.tests.derbynet.NetIjTest case union
     * </code>
     */
    public static void main(String[] args)
        {
            junit.textui.TestRunner.run(getSuite(args));
        }

    /**
     * Return the suite that runs all the derbynet scripts.
     */
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("NetScripts");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        // Set up the scripts run with the network client
        BaseTestSuite clientTests = new BaseTestSuite("NetScripts:client");
        clientTests.addTest(getSuite(CLIENT_TESTS));

        int port = TestConfiguration.getCurrent().getPort();

//IC see: https://issues.apache.org/jira/browse/DERBY-4223
//IC see: https://issues.apache.org/jira/browse/DERBY-4217
        Properties prop = new Properties();
        prop.setProperty("ij.protocol",
                "jdbc:derby://localhost:"+port+"/");

        Test client = new SystemPropertyTestSetup(
                TestConfiguration.clientServerDecoratorWithPort(clientTests,port),
                prop);
                    
        // add those client tests into the top-level suite.
        suite.addTest(client);

        return suite;
    }

    /*
     * A single JUnit test that runs a single derbynet script.
     */
    private NetIjTest(String netTest){
//IC see: https://issues.apache.org/jira/browse/DERBY-4223
//IC see: https://issues.apache.org/jira/browse/DERBY-4217
        super(netTest,true);
    }

    /**
     * Return a suite of derbynet tests from the list of
     * script names. Each test is surrounded in a decorator
     * that cleans the database.
     */
    private static Test getSuite(String[] list) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("Net scripts");
        for (int i = 0; i < list.length; i++)
            suite.addTest(
                new CleanDatabaseTestSetup(
                    new NetIjTest(list[i])));

        return getIJConfig(suite);
    }
}
