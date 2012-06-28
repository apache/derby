/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.ClientCompatibilityRunControl

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
package org.apache.derbyTesting.functionTests.tests.compatibility;

import java.io.File;
import java.net.URL;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DerbyDistribution;
import org.apache.derbyTesting.junit.DerbyVersion;
import org.apache.derbyTesting.junit.SpawnedProcess;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Spawns the JVM process running the compatibility tests for the given client
 * version.
 */
public class ClientCompatibilityRunControl
        extends BaseJDBCTestCase {

    static final String LOB_TESTING_PROP = "derby.tests.compat.testLOBs";

    /** The descriptive name of the test case. */
    private final String realName;
    /** The Derby client to use. */
    private final DerbyDistribution clientDist;
    /** The Derby version we expect to connect to. */
    private final DerbyVersion serverVersion;
    /** Path to the testing code to use (typically from trunk). */
    private final String testingPath;

    /**
     * Creates a control object for the given client version.
     *
     * @param client the Derby client to use
     * @param testingPath path to the testing code to use (typically
     *      {@literal derbyTesting.jar} from trunk)
     * @param serverVersion the expected server version
     */
    public ClientCompatibilityRunControl(DerbyDistribution client,
                                         String testingPath,
                                         DerbyVersion serverVersion) {
        super("testClient");
        this.clientDist = client;
        this.testingPath = testingPath;
        this.serverVersion = serverVersion;
        this.realName = "combination(client " + client.getVersion().toString() +
                " <> server " + serverVersion.toString() + ")";
    }

    @Override
    public String getName() {
        return realName;
    }

    /**
     * Runs the client compatibility test suite with the client driver in a
     * separate JVM.
     * <p>
     * The server is expected to be running already.
     */
    public void testClient()
            throws Exception {
        boolean testLOBs = Boolean.parseBoolean(
                getSystemProperty(LOB_TESTING_PROP));
        // Fork the client test with a minimal classpath.
        String classpath = clientDist.getDerbyClientJarPath() +
                File.pathSeparator + testingPath +
                File.pathSeparator + getJUnitURL().getPath();
        // If we are running the LOB tests we also need derby.jar, because the
        // test code being run references classes from the iapi package.
        // This may also happen for the non-LOB tests in the future.
        if (testLOBs) {
            classpath += File.pathSeparator + clientDist.getDerbyEngineJarPath();
        }

        String[] cmd = new String[] {
            "-Dderby.tests.compat.expectedClient=" +
                clientDist.getVersion().toString(),
            "-Dderby.tests.compat.expectedServer=" +
                serverVersion.toString(),
            "-Dderby.tests.compat.testLOBs=" +
                testLOBs,
            "-Dderby.tests.basePort=" +
                TestConfiguration.getBasePort(),
            "junit.textui.TestRunner",
            "org.apache.derbyTesting.functionTests.tests." +
                "compatibility.ClientCompatibilitySuite"
        };
        Process proc = execJavaCmd(null, classpath, cmd, null);

        SpawnedProcess spawned = new SpawnedProcess(proc, realName);
        int exitCode = spawned.complete(30*60*1000); // 30 minutes
        assertTrue(spawned.getFailMessage("client VM failed: "), exitCode == 0);
        println(spawned.getFullServerOutput());
    }

	/**
	 * Returns the URL of the JUnit classes.
     *
     * @return A URL.
	 */
    private static URL getJUnitURL() {
        return VersionCombinationConfigurator.getClassURL(
                junit.framework.TestCase.class);
	}
}
