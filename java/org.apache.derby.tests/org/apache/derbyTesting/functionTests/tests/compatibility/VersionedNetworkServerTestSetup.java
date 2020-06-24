/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.VersionedNetworkServerTestSetup

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
import java.io.IOException;
import java.util.ArrayList;

import junit.framework.Test;

import org.apache.derby.drda.NetworkServerControl;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSetup;
import org.apache.derbyTesting.junit.DerbyDistribution;
import org.apache.derbyTesting.junit.DerbyVersion;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SpawnedProcess;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Starts a network server of the specified Derby version.
 */
public class VersionedNetworkServerTestSetup
        extends BaseTestSetup {

    /** The first version to support '-noSecurityManager'. */
    private static final DerbyVersion SUPPORTS_NO_SECMAN_ARG =
            new DerbyVersion(10, 3, 1, 4);
    /**
     * The first version that supports the optional arguments on shutdown.
     * <p>
     * See DERBY-4786 and related issues.
     */
    private static final DerbyVersion NO_BROKEN_SHUTDOWN =
            new DerbyVersion(10, 4, 0, 0);
    /** The Derby distribution to use. */
    private final DerbyDistribution dist;
    /** Paths for code to append to the server classpath. */
    private final String appendToClasspath;
    private SpawnedProcess spawned;
    private NetworkServerControl networkServerControl;

    public VersionedNetworkServerTestSetup(Test test, DerbyDistribution dist,
            String appendToClasspath) {
        super(test);
        this.dist = dist;
        this.appendToClasspath = appendToClasspath;
    }

    @Override
    public void setUp() {
        int port = TestConfiguration.getCurrent().getPort();
        try {
            networkServerControl =
                    NetworkServerTestSetup.getNetworkServerControl(port);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        // Make sure there is no server up already (on our port).
        println("checking for running server on port " + port);
        if (ping(false, null)) {
            fail("a server is already running at port " + port);
        }

        // java -classpath ... org.apache.derby.drda...
        // Don't use -jar derbyrun.jar because we may need additional classes
        // to be on the classpath.
        String classpath = dist.getProductionClasspath() +
                (appendToClasspath == null
                            ? ""
                            : File.pathSeparator + appendToClasspath);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        ArrayList<String> cmd = new ArrayList<String>();
        cmd.add("org.apache.derby.drda.NetworkServerControl");
        cmd.add("start");
        cmd.add("-p");
        cmd.add(Integer.toString(port));
        if (dist.getVersion().compareTo(SUPPORTS_NO_SECMAN_ARG) >= 0) {
            cmd.add("-noSecurityManager");
        }

        Process proc = null;
        try {
            proc = BaseTestCase.execJavaCmd(null, classpath,
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                cmd.toArray(new String[cmd.size()]), null);
        } catch (IOException ioe) {
            fail("failed to start server: " + ioe.getMessage());
        }
        spawned = new SpawnedProcess(proc, "NetworkServerControl");
        boolean pingOk = ping(true, proc);
        assertTrue(spawned.getFailMessage("server failed to come up"), pingOk);
        println("--- Server " + dist.getVersion() + " up");
    }

    @Override
    public void tearDown() {
        String errorMsg = null;
        boolean sawError = false;
        if (dist.getVersion().compareTo(NO_BROKEN_SHUTDOWN) < 0) {
            // We have to fork off a process to shut down the server.
            errorMsg = shutDownInSeparateProcess();
            sawError = errorMsg != null;
        } else {
            boolean pingOk = ping(true, spawned.getProcess());
            if (pingOk) {
                try {
                    networkServerControl.shutdown();
                } catch (Exception e) {
                    String msg = spawned.getFailMessage("shutdown failed");
                    errorMsg = " (failed to shut down server (" +
                            dist.getVersion().toString() + "): " +
                            e.getMessage() + " :: " + msg + ")";
                    sawError = true;
                }
            }
        }

        try {
            spawned.complete(5*1000);
        } catch (Exception e) {
            errorMsg = "process didn't die: " + e.getMessage() + (sawError ?
                    errorMsg : "");
            sawError = true;
        }
        networkServerControl = null;
        spawned = null;

        try {
            BaseTestCase.assertDirectoryDeleted(new File("wombat"));
        } catch (AssertionError ae) {
            // Catch this to generate a more complete error message.
            if (sawError) {
                errorMsg += " :: " + ae.getMessage();
            } else {
                throw ae;
            }
        }
        if (sawError) {
            fail(errorMsg);
        }
    }

    /**
     * Spawns a separate JVM process to shut down the running server using the
     * code distributed with the release.
     * <p>
     * This method was added because some versions of Derby cannot be shut down
     * using code from a newer release.
     *
     * @return An error message, or {@code null} if no errors.
     */
    private String shutDownInSeparateProcess() {
        int port = TestConfiguration.getCurrent().getPort();
        // java -classpath ... org.apache.derby.drda...
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        ArrayList<String> cmd = new ArrayList<String>();
        cmd.add("org.apache.derby.drda.NetworkServerControl");
        cmd.add("shutdown");
        cmd.add("-p");
        cmd.add(Integer.toString(port));
        if (dist.getVersion().compareTo(SUPPORTS_NO_SECMAN_ARG) >= 0) {
            cmd.add("-noSecurityManager");
        }
        Process proc;
        try {
            proc = BaseTestCase.execJavaCmd(null, dist.getProductionClasspath(),
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
                    cmd.toArray(new String[cmd.size()]), null);
        } catch (IOException ioe) {
            return "shutdown process failed to start: " + ioe.getMessage();
        }

        SpawnedProcess spawnedShutdown =
                new SpawnedProcess(proc, "shutdown process");
        int exitCode = -1;
        try {
            exitCode = spawnedShutdown.complete(10*1000L);
        }  catch (IOException ioe) {
            fail(spawnedShutdown.getFailMessage("shutdown process failed"));
        }
        if (exitCode == 0) {
            return null;
        } else {
            return spawnedShutdown.getFailMessage("abnormal process exit");
        }
    }

    /**
     * Pings the server.
     *
     * @param exepectServerUp whether the server is expected to be up or down
     * @param proc the process in which the server runs (may be {@code null})
     * @return Whether the ping is considered ok, which is determined by the
     *      response or lack of response from the server and the value of
     *      {@code expectedServerUp}.
     */
    private boolean ping(boolean exepectServerUp, Process proc) {
        boolean pingOk = false;
        try {
            pingOk = NetworkServerTestSetup.pingForServerUp(
                networkServerControl, proc, exepectServerUp);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return pingOk;
    }

    private static void println(String msg) {
        BaseTestCase.println(msg);
    }
}
