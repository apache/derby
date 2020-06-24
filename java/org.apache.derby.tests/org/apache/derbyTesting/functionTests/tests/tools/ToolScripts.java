/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.tools.ToolScripts
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
package org.apache.derbyTesting.functionTests.tests.tools;

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * ToolScripts runs ij tool scripts (.sql files) in the tool package
 * and compares the output to a canon file in the
 * standard master package.
 * <BR>
 * Its suite() method returns a set of tests where each test is an instance of
 * this class for an individual script wrapped in a clean database decorator.
 * <BR>
 * It can also be used as a command line program to run one or more
 * tool based ij scripts as tests.
 *
 */
public final class ToolScripts extends ScriptTestCase {

    /**
     * Tool scripts (.sql files) that run under Derby's client
     * and emebedded configurations. Tool tests are put in this category
     * if they are likely to have some testing of the protocol,
     * typically tests related to data types.
     *
     */
    private static final String[] CLIENT_AND_EMBEDDED_TESTS = {
//IC see: https://issues.apache.org/jira/browse/DERBY-6585
        "ij4", "ij6", "ij7", "setholdij",
    };

    /**
     * Tool scripts (.sql files) that only run in embedded.
     */
    private static final String[] EMBEDDED_TESTS = {
        "showindex_embed",
    };

    /**
     * Tool scripts (.sql files) that only run in client.
     */
    private static final String[] CLIENT_TESTS = {
        "showindex_client",
    };

    /**
     * Tests that run in embedded and require JDBC3_TESTS
     * (ie. can not run on JSR169).
     */
    private static final String[] JDBC3_TESTS = {
//IC see: https://issues.apache.org/jira/browse/DERBY-5345
    	"qualifiedIdentifiers", "URLCheck",
    };


    /**
     * Tests that run with authentication and SQL authorization on.
     */
    private static final String[][][] SQLAUTHORIZATION_TESTS = {
//IC see: https://issues.apache.org/jira/browse/DERBY-3886
        {{"ij_show_roles_dbo"}, {"test_dbo", "donald"}, {"test_dbo"}},
        {{"ij_show_roles_usr"}, {"test_dbo", "donald"}, {"donald"}}
    };

    /**
     * Run a set of tool scripts (.sql files) passed in on the
     * command line. Note the .sql suffix must not be provided as
     * part of the script name.
     * <code>
     * example
     * java org.apache.derbyTesting.functionTests.tests.tool.ToolScripts case union
     * </code>
     */
    public static void main(String[] args)
        {
            junit.textui.TestRunner.run(getSuite(args));
        }

    /**
     * Return the suite that runs all the tool scripts.
     */
    public static Test suite() {

//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("ToolScripts");
        suite.addTest(getSuite(CLIENT_AND_EMBEDDED_TESTS));
        suite.addTest(getSuite(EMBEDDED_TESTS));
        if (JDBC.vmSupportsJDBC3())
            suite.addTest(getSuite(JDBC3_TESTS));
        suite.addTest(getAuthorizationSuite(SQLAUTHORIZATION_TESTS));
//IC see: https://issues.apache.org/jira/browse/DERBY-3137

        // Set up the scripts run with the network client
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite clientTests = new BaseTestSuite("ToolScripts:client");
        clientTests.addTest(getSuite(CLIENT_AND_EMBEDDED_TESTS));
        clientTests.addTest(getAuthorizationSuite(SQLAUTHORIZATION_TESTS));
        clientTests.addTest(getSuite(CLIENT_TESTS));
        Test client = TestConfiguration.clientServerDecorator(clientTests);

        // add those client tests into the top-level suite.
        suite.addTest(client);

        return suite;
    }

    /*
     * A single JUnit test that runs a single tool script.
     */
    private ToolScripts(String toolTest){
        super(toolTest);
    }

    private ToolScripts(String toolTest, String user){
//IC see: https://issues.apache.org/jira/browse/DERBY-1726
        super(toolTest,
              null /* default input encoding */,
              null /* default output encoding */,
              user);
    }

    /**
     * Return a suite of tool tests from the list of
     * script names. Each test is surrounded in a decorator
     * that cleans the database.
     */
    private static Test getSuite(String[] list) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("Tool scripts");
        for (int i = 0; i < list.length; i++)
            suite.addTest(
                new CleanDatabaseTestSetup(
                    new ToolScripts(list[i])));

        return getIJConfig(suite);
    }

    /**
     * Return a suite of tool tests from the list of script names. Each test is
     * surrounded in a decorator that cleans the database, and adds
     * authentication and authorization for each script.
     * @param list <ul><li>list[i][0][0]: script name,
     *                 <li>list[i][1]: users,
     *                 <li>list[i][2][0]: run-as-user
     *             </ul>
     */
    private static Test getAuthorizationSuite(String[][][] list) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("Tool scripts w/authorization");

        final String PWSUFFIX = "pwSuffix";

        for (int i = 0; i < list.length; i++) {
            Test clean;
//IC see: https://issues.apache.org/jira/browse/DERBY-3886

            if (list[i][0][0].startsWith("ij_show_roles")) {
                clean = new CleanDatabaseTestSetup(
                    new ToolScripts(list[i][0][0], list[i][2][0])) {
                        protected void decorateSQL(Statement s)
                                throws SQLException {
                            s.execute("create role a");
                            s.execute("create role b");
                            s.execute("create role \"\"\"eve\"\"\"");
                            s.execute("create role publicrole");
                            s.execute("grant a to b");
                            s.execute("grant publicrole to public");
                            s.execute("grant b to donald");
                        }
                    };
            } else {
                clean = new CleanDatabaseTestSetup(
                    new ToolScripts(list[i][0][0], list[i][2][0]));
            }

            suite.addTest(
                TestConfiguration.sqlAuthorizationDecorator(
                    DatabasePropertyTestSetup.builtinAuthentication(
                        clean, list[i][1], PWSUFFIX)));
        }

        return getIJConfig(suite);
    }
}
