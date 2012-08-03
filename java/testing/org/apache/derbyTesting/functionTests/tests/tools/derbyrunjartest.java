/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.derbyrunjartest

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;

/**
 * Basic tests for exercising the {@code org.apache.derby.iapi.tools.run}
 * class found in {@code derbyrun.jar}.
 */
public class derbyrunjartest extends BaseTestCase {

    public derbyrunjartest(String name) {
        super(name);
    }

    public static Test suite() {
        Class cl = derbyrunjartest.class;

        TestSuite suite = new TestSuite(cl);

        // The server command can only be used on platforms that support
        // the network server. Specifically, it does not work in J2ME
        // environments.
        if (JDBC.vmSupportsJDBC3()) {
            suite.addTest(new derbyrunjartest("xtestServer"));
        }

        return new SecurityManagerSetup(
                suite,
                cl.getName().replace('.', '/') + ".policy",
                true);
    }

    /**
     * Invoke {@code org.apache.derby.iapi.tools.run} in a sub-process.
     *
     * @param toolArgs the arguments to pass to derbyrun.jar
     * @param output expected lines of output
     * @param exitCode expected exit code for the command
     */
    private void runtool(String[] toolArgs, String[] output, int exitCode)
            throws Exception {
        String runClassName = org.apache.derby.iapi.tools.run.class.getName();
        URL result = SecurityManagerSetup.getURL(runClassName);
        String derbyrunloc = null;

        if (result.toString().endsWith(".jar")) {
            derbyrunloc = result.toString().substring(5);
            if (System.getProperty("os.name").startsWith("Windows"))
              derbyrunloc = derbyrunloc.substring(1);
        }

        ArrayList cmdArgs = new ArrayList();

        // Invoke java -jar derbyrun.jar if we are running from jars, or
        // with fully qualified class name if we are running from classes.
        if (derbyrunloc == null) {
            cmdArgs.add(runClassName);
        } else {
            cmdArgs.add("-jar");
            cmdArgs.add(derbyrunloc);
        }

        cmdArgs.addAll(Arrays.asList(toolArgs));

        String[] cmd = (String[]) cmdArgs.toArray(new String[cmdArgs.size()]);
        assertExecJavaCmdAsExpected(output, cmd, exitCode);
    }

    public void testIJ() throws Exception {
        String[] cmd = { "ij", "--help" };
        String[] output = {
            "Usage: java org.apache.derby.tools.ij [-p propertyfile] [inputfile]"
        };
        runtool(cmd, output, 0);
    }

    public void testSysinfo() throws Exception {
        String[] cmd = { "sysinfo", "-cp", "help" };
        String[] output = {
            "Usage: java org.apache.derby.tools.sysinfo -cp [ [ embedded ][ server ][ client] [ tools ] [ anyClass.class ] ]"
        };
        runtool(cmd, output, 0);
    }

    public void testDblook() throws Exception {
        String[] cmd = { "dblook" };
        String[] output = {
            " Usage:",
            " java org.apache.derby.tools.dblook -d <source database url> [options]",
            " 	where the source URL is the full URL, including the connection protocol",
            " 	and any connection attributes that might apply.  For example, use",
            " 	options include:",
            " 	-z <schema name> to specify a schema to which the DDL generation",
            " 	 should be limited.  Only database objects with that schema will have",
            " 	 their DDL generated.",
            " 	-t <table one> <table two> ... to specify a list of tables for which",
            " 	 the DDL will be generated; any tables not in the list will be ignored.",
            " 	-td <value> to specify what should be appended to the end",
            " 	 of each DDL statement.",
            "		This defaults to ';'.",
            " 	-noview to prevent the generation of DDL for views.",
            " 	-append to keep from overwriting the output files.",
            " 	-verbose to have error messages printed to the console (in addition",
            " 	 to the log file).  If not specified, errors will only be printed to the",
            " 	 log file.",
            " 	-o <filename> to specify the file name to which the generated DDL",
            " 	 will be written.",
            " 		If not specified, default is the console.",
        };
        runtool(cmd, output, 0);
    }

    public void xtestServer() throws Exception {
        String[] cmd = { "server" };
        String[] output = {
            "Usage: NetworkServerControl <commands> ",
            "Commands:",
            "start [-h <host>] [-p <port number>] [-noSecurityManager] [-ssl <ssl mode>]",
            "shutdown [-h <host>][-p <port number>] [-ssl <ssl mode>] [-user <username>] [-password <password>]",
            "ping [-h <host>][-p <port number>] [-ssl <ssl mode>]",
            "sysinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
            "runtimeinfo [-h <host>][-p <port number>] [-ssl <ssl mode>]",
            "logconnections {on|off} [-h <host>][-p <port number>] [-ssl <ssl mode>]",
            "maxthreads <max>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
            "timeslice <milliseconds>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
            "trace {on|off} [-s <session id>][-h <host>][-p <port number>] [-ssl <ssl mode>]",
            "tracedirectory <trace directory>[-h <host>][-p <port number>] [-ssl <ssl mode>]",
        };
        runtool(cmd, output, 1);
    }
}
