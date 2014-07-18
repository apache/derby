/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.MissingPermissionsTest

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

package org.apache.derbyTesting.unitTests.junit;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SpawnedProcess;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test behavior when permissions are missing for:
 * <ul>
 *   <li>reading of system properties, see DERBY-6617</li>
 *   <li>read, write access to create derby.system.home, see DERBY-6617</li>
 * </ul>
 * Note: requires English locale because the test asserts on localized
 * strings.
 */
public class MissingPermissionsTest extends BaseJDBCTestCase {

    private final static String AUTH_MSG =
            "derby.connection.requireAuthentication";

    private final static String SYSTEM_HOME = "derby.system.home";

    private final static String resourcePrefix = "unitTests/junit/";
    private final static String testPrefix =
            "org/apache/derbyTesting/" + resourcePrefix;

    private final static String OK_POLICY =
            "MissingPermissionsTest.policy";
    private final static String OK_POLICY_T =
            testPrefix + OK_POLICY;

    private final static String POLICY_MINUS_PROPERTYPERMISSION =
            "MissingPermissionsTest1.policy";

    private final static String POLICY_MINUS_PROPERTYPERMISSION_T =
            testPrefix + POLICY_MINUS_PROPERTYPERMISSION;

    private final static String POLICY_MINUS_FILEPERMISSION =
            "MissingPermissionsTest2.policy";

    private final static String POLICY_MINUS_FILEPERMISSION_T =
            testPrefix + POLICY_MINUS_FILEPERMISSION;

    private final static String POLICY_MINUS_FILEPERMISSION_R =
            resourcePrefix + POLICY_MINUS_FILEPERMISSION;

    private final int KIND_EXPECT_ERROR_MSG_PRESENT = 0;
    private final int KIND_EXPECT_ERROR_MSG_ABSENT = 1;

    public MissingPermissionsTest(String name) {
        super(name);
    }


    private static Test makeTest(String fixture, String policy) {
        Test t =  new MissingPermissionsTest(fixture);
        t = new SecurityManagerSetup(t, policy);
        final Properties props = new Properties();
        props.setProperty("derby.connection.requireAuthentication", "true");
        props.setProperty("derby.database.sqlAuthorization", "true");
        props.setProperty("derby.authentication.provider", "BUILTIN");
        props.setProperty("derby.user.APP", "APPPW");

        t = new SystemPropertyTestSetup(t, props, true);
        t = TestConfiguration.changeUserDecorator(t, "APP", "APPPW");
        t = TestConfiguration.singleUseDatabaseDecorator(t);
        return t;
    }

    public static Test suite() {
        final BaseTestSuite suite =
                new BaseTestSuite("SystemPrivilegesPermissionTest");

        if (!TestConfiguration.loadingFromJars()) {
            // This test only works with jar files.
            return suite;
        }

        suite.addTest(
                new SupportFilesSetup(
                        makeTest("testMissingFilePermission",
                                POLICY_MINUS_FILEPERMISSION_T),
                        new String[] {
                            POLICY_MINUS_FILEPERMISSION_R}));

        suite.addTest(makeTest("testPresentPropertiesPermission",
                OK_POLICY_T));

        suite.addTest(makeTest("testMissingPropertiesPermission",
                POLICY_MINUS_PROPERTYPERMISSION_T));

        return suite;
    }

    /**
     * This test is run with a policy that does not lack permission to read
     * properties for derby.jar. This should leave no related error messages on
     * derby.log.
     *
     * @throws SQLException
     * @throws IOException
     * @throws PrivilegedActionException
     */
    public void testPresentPropertiesPermission()
            throws SQLException, IOException, PrivilegedActionException {

        // With credentials we are OK
        openDefaultConnection("APP", "APPPW").close();

        Connection c = null;

        // With wrong credentials we are not OK
        try {
            c = openDefaultConnection("Donald", "Duck");
            fail();
        } catch(SQLException e) {
            assertSQLState("08004", e);
        } finally {
            if (c != null) {
                c.close();
            }
        }
        verifyMessagesInDerbyLog(KIND_EXPECT_ERROR_MSG_ABSENT);
    }

    /**
     * This test is run with a policy that lacks permission to read properties
     * for derby.jar. This should lead to error messages on derby.log.
     *
     * @throws SQLException
     * @throws IOException
     * @throws PrivilegedActionException
     */
    public void testMissingPropertiesPermission()
            throws SQLException, IOException, PrivilegedActionException {
        // With credentials we are OK
        openDefaultConnection("APP", "APPPW").close();

        // But also with wrong ones, all seems OK...
        openDefaultConnection("Donald", "Duck").close();

        // Check that we see the error messages expected in derby.log
        verifyMessagesInDerbyLog(KIND_EXPECT_ERROR_MSG_PRESENT);
    }

    /**
     * This test is run with a policy that lacks permission for derby.jar to
     * create a db directory for derby.  In this scenario we expect the boot to
     * fail, and an error message to be printed to the console, so we try to
     * get it by forking a sub-process. See {@code FileMonitor#PBinitialize}
     * when it gets a {@code SecurityException} following attempt to do "{@code
     * home.mkdir(s)}".
     * <p/>
     * Note that the policy used with this text fixture also doubles as the
     * one used by the subprocess to demonstrate the lack of permission.
     *
     * @throws SQLException
     * @throws IOException
     * @throws PrivilegedActionException
     * @throws ClassNotFoundException
     * @throws java.lang.InterruptedException
     */
    public void testMissingFilePermission() throws SQLException,
            IOException,
            PrivilegedActionException,
            ClassNotFoundException,
            InterruptedException {

        // Collect the set of needed arguments to the java command
        // The command runs ij with a security manager whose policy
        // lacks the permissions to create derby.system.home.
        final List<String> args = new ArrayList<String>();
        args.add("-Djava.security.manager");
        args.add("-Djava.security.policy=extin/MissingPermissionsTest2.policy");
        args.add("-DderbyTesting.codejar="
                    + getSystemProperty("derbyTesting.codejar"));
        args.add("-DderbyTesting.testjar="
                    + getSystemProperty("derbyTesting.testjar"));
        args.add("-DderbyTesting.junit="
                    + getSystemProperty("derbyTesting.junit"));
        String antjunit = getSystemProperty("derbyTesting.antjunit");
        if (antjunit != null) {
            // This property is only available when the test is started
            // by Ant's JUnit task.
            args.add("-DderbyTesting.antjunit=" + antjunit);
        }
        args.add("-Dderby.system.home=system/nested");
        args.add("-Dij.connection.test=jdbc:derby:wombat;create=true");
        args.add("org.apache.derby.tools.ij");
        final String[] argArray = args.toArray(new String[0]);

        final Process p = execJavaCmd(argArray);
        SpawnedProcess spawned = new SpawnedProcess(p, "MPT");
        spawned.suppressOutputOnComplete(); // we want to read it ourselves

        // The started process is an interactive ij session that will wait
        // for user input. Close stdin of the process so that it stops
        // waiting and exits.
        p.getOutputStream().close();

        final int exitCode = spawned.complete(120000L); // 2 minutes

        assertTrue(
            spawned.getFailMessage("subprocess run failed: "), exitCode == 0);

        // The actual message may vary. On Java 6, the names are not quoted,
        // whereas newer versions double-quote them. On Windows, the directory
        // separator is different. Also, different JVM vendors capitalize
        // the "access denied" message differently.
        //
        // Use a regular expression that matches all known variants.
        final String expectedMessageOnConsole =
            "(?s).*The file or directory system.nested could not be created " +
            "due to a security exception: " +
            "java\\.security\\.AccessControlException: [Aa]ccess denied " +
            "\\(\"?java\\.io\\.FilePermission\"? \"?system.nested\"? " +
            "\"?write\"?\\).*";

        final String output = spawned.getFullServerOutput(); // ignore
        final String err    = spawned.getFullServerError();

        assertTrue(err, err.matches(expectedMessageOnConsole));
    }

    /**
     * Make a regex that matches a warning for missing property permission.
     * @param property the property for which read permission is missing
     * @return a pattern that matches the expected warning
     */
    private String makeMessage(String property) {
        // A variable part in the message is whether or not the names are
        // double-quoted. In Java 6 they are not. In newer versions they are.
        // Another variable part is that the captitalization of "access denied"
        // varies depending on the JVM vendor.
        final StringBuilder sb = new StringBuilder();
        sb.append("(?s).*WARNING: the property ");
        sb.append(Pattern.quote(property));
        sb.append(" could not be read due to a security exception: ");
        sb.append("java\\.security\\.AccessControlException: [Aa]ccess denied \\(");
        sb.append("\"?java\\.util\\.PropertyPermission\"? \"?");
        sb.append(Pattern.quote(property));
        sb.append("\"? \"?read\"?.*");
        return sb.toString();
    }


    private void verifyMessagesInDerbyLog(int kind) throws
            FileNotFoundException,
            IOException,
            PrivilegedActionException {

        String derbyLog = null;

        if (kind == KIND_EXPECT_ERROR_MSG_PRESENT) {
            // In this case we didn't have permission to read derby.system.home
            // so expect derby.log to be at CWD.
            derbyLog = "derby.log";
        } else if (kind == KIND_EXPECT_ERROR_MSG_ABSENT) {
            derbyLog = "system/derby.log";
        }

        final BufferedReader dl = getReader(derbyLog);
        final StringBuilder log = new StringBuilder();

        try {
            for (String line = dl.readLine(); line != null; line = dl.readLine()) {
                log.append(line);
                log.append('\n');
            }

            String logString = log.toString();
            if (kind == KIND_EXPECT_ERROR_MSG_PRESENT) {
                // We should see SecurityException when reading security
                // related properties in FileMonitor#PBgetJVMProperty
                assertTrue(logString, logString.matches(makeMessage(AUTH_MSG)));

                // We should see SecurityException when reading
                // derby.system.home in FileMonitor#PBinitialize
                assertTrue(logString,
                           logString.matches(makeMessage(SYSTEM_HOME)));
            } else if (kind == KIND_EXPECT_ERROR_MSG_ABSENT) {
                assertFalse(logString,
                            logString.matches(makeMessage(AUTH_MSG)));
                assertFalse(logString,
                            logString.matches(makeMessage(SYSTEM_HOME)));
            }
        } finally {
            dl.close();
        }
    }

    private static BufferedReader getReader(final String file)
            throws PrivilegedActionException {

        return AccessController.doPrivileged(
                new PrivilegedExceptionAction<BufferedReader>() {
            @Override
            public BufferedReader run() throws FileNotFoundException {
                return new BufferedReader(new FileReader(file));
            }});
    }
}
