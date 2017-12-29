/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.SystemPrivilegesPermissionTest

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

package org.apache.derbyTesting.unitTests.junit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.AllPermission;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Set;
import javax.security.auth.Subject;
import junit.framework.Test;
import org.apache.derby.authentication.SystemPrincipal;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.security.DatabasePermission;
import org.apache.derby.shared.common.security.SystemPermission;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;

/**
 * This class tests the basic permission classes for system privileges.
 */
public class SystemPrivilegesPermissionTest extends BaseTestCase {

    /**
     * The policy file name for the subject authorization tests.
     */
    private static final String POLICY_FILE_NAME
        = "org/apache/derbyTesting/unitTests/junit/SystemPrivilegesPermissionTest.policy";

    /**
     * The policy file name for the DatabasePermission API test.
     */
    private static final String POLICY_FILE_NAME1
        = "org/apache/derbyTesting/unitTests/junit/SystemPrivilegesPermissionTest1.policy";

    /**
     * Some directory paths for testing DatabasePermissions.
     */
    static private final String[] dirPaths = {
        "-",
        "*",
        "level0",
        "level0a",
        "level0/-",
        "level0/*",
        "level0/level1",
        "level0/level1/level2"
    };

    /**
     * Some relative directory paths for testing DatabasePermissions.
     */
    static private final String[] relDirPaths
        = new String[dirPaths.length];
    static {
        for (int i = 0; i < relDirPaths.length; i++) {
            relDirPaths[i] = "directory:" + dirPaths[i];
        }
    };

    /**
     * Some relative directory path aliases for testing DatabasePermissions.
     */
    static private final String[] relDirPathAliases
        = new String[dirPaths.length];
    static {
        for (int i = 0; i < relDirPaths.length; i++) {
            relDirPathAliases[i] = "directory:./" + dirPaths[i];
        }
    };

    /**
     * Some absolute directory paths for testing DatabasePermissions.
     */
    static private final String[] absDirPaths
        = new String[dirPaths.length];
    static {
        for (int i = 0; i < relDirPaths.length; i++) {
            absDirPaths[i] = "directory:/" + dirPaths[i];
        }
    };

    /**
     * Some absolute directory path aliases for testing DatabasePermissions.
     */
    static private final String[] absDirPathAliases
        = new String[dirPaths.length];
    static {
        for (int i = 0; i < relDirPaths.length; i++) {
            absDirPathAliases[i] = "directory:/dummy/../" + dirPaths[i];
        }
    };

    /**
     * The matrix defining which of the above directory paths imply each other.
     *
     * For instance, dirPathImpls[1][2] shows the expected value for:
     * <ul>
     * <li> DP("directory:*").implies(DP(directory:level0"))
     * <li> DP("directory:./*").implies(DP(directory:./level0"))
     * <li> DP("directory:/*").implies(DP(directory:/level0"))
     * <li> DP("directory:/dummy/..*").implies(DP(directory:/dummy/..level0"))
     * </ul>
     */
    static private final boolean[][] dirPathImpls = {
        { true, true, true, true, true, true, true, true }, 
        { false, true, true, true, false, false, false, false },
        { false, false, true, false, false, false, false, false },
        { false, false, false, true, false, false, false, false },
        { false, false, false, false, true, true, true, true },
        { false, false, false, false, false, true, true, false },
        { false, false, false, false, false, false, true, false },
        { false, false, false, false, false, false, false, true }
    };    

    /** The valid names of a SystemPermission. */
    private static final String[] VALID_SYSPERM_NAMES = {
        "server", "engine", "jmx"
    };

    /** The valid actions of a SystemPermission. */
    private static final String[] VALID_SYSPERM_ACTIONS = {
        "shutdown", "control", "monitor"
    };

    /**
     * Create a test with the given name.
     *
     * @param name name of the test
     */
    public SystemPrivilegesPermissionTest(String name) {
        super(name);
    }

    /**
     * Return a suite with all tests in this class (default suite)
     */
    public static Test suite() {
        // this suite cannot be constructed with automatic test extraction
        // (by passing a class argument); instead, the tests need to be added
        // manually since some of them require their own policy file
        BaseTestSuite suite =
            new BaseTestSuite("SystemPrivilegesPermissionTest");

        // add API tests for the basic security framework classes
        suite.addTest(
            new SystemPrivilegesPermissionTest("testSystemPrincipal"));
        suite.addTest(
            new SystemPrivilegesPermissionTest("testSystemPermission"));
        suite.addTest(
            new SystemPrivilegesPermissionTest(
                    "testSystemPermissionCollections"));

        // the DatabasePermission test attempts to canonicalize various
        // directory path names and requires an all-files-read-permission,
        // which is not granted by default derby_tests.policy
        suite.addTest(new SecurityManagerSetup(
            new SystemPrivilegesPermissionTest("testDatabasePermission"),
            POLICY_FILE_NAME1));

        // add authorization tests for security permissions; requires
        // class javax.security.auth.Subject, which is not available
        // on all JVM platforms
        if (SecurityManagerSetup.JVM_HAS_SUBJECT_AUTHORIZATION) {
            suite.addTest(new SecurityManagerSetup(
                new SystemPrivilegesPermissionTest("policyTestSystemPermissionGrants"),
                     POLICY_FILE_NAME));
            suite.addTest(new SecurityManagerSetup(
                new SystemPrivilegesPermissionTest("policyTestDatabasePermissionGrants"),
                     POLICY_FILE_NAME));
        }

        // We need to manipulate private and final fields in order to test
        // deserialization of invalid objects. Disable the security manager
        // for this test case to allow that.
        //
        // As of Java 9, you can't subvert access controls via this ruse.
        // Only run this test on Java 8.
        if (isJava8())
        {
            suite.addTest
                (
                 SecurityManagerSetup.noSecurityManager
                     (
                         new SystemPrivilegesPermissionTest("testSerialization")
                     )
                 );
        }

        return suite;
    }

    /**
     * Tests SystemPrincipal.
     */
    public void testSystemPrincipal() {
        // test a valid SystemPrincipal
        SystemPrincipal p = new SystemPrincipal("superuser");
        assertEquals("superuser", p.getName());

        // test SystemPrincipal with null name argument
        try {
            new SystemPrincipal(null);
            fail("expected NullPointerException");
        } catch (NullPointerException ex) {
            // expected exception
        }

        // test SystemPrincipal with empty name argument
        try {
            new SystemPrincipal("");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // DERBY-3476: The SystemPrincipal class should be final.
        assertTrue(Modifier.isFinal(SystemPrincipal.class.getModifiers()));
    }
    
    /**
     * Tests SystemPermission.
     */
    public void testSystemPermission() {
        // test SystemPermission with null name argument
        try {
            new SystemPermission(null, null);
            fail("expected NullPointerException");
        } catch (NullPointerException ex) {
            // expected exception
        }

        // test SystemPermission with empty name argument
        try {
            new SystemPermission("", null);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }
        
        // test SystemPermission with illegal name argument
        try {
            new SystemPermission("illegal_name", null);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // actions cannot be null
        try {
            new SystemPermission("server", null);
            fail("expected NullPointerException");
        } catch (NullPointerException ex) {
            // expected exception
        }

        // Illegal and duplicate actions are ignored.
        assertEquals("", new SystemPermission("server", "").getActions());
        assertEquals("", new SystemPermission("server", ",,").getActions());
        assertEquals("",
                     new SystemPermission("server", "illegal_action")
                             .getActions());
        assertEquals("control",
                     new SystemPermission("server", "control,").getActions());
        assertEquals("control",
                     new SystemPermission("server", "control,illegal_action")
                             .getActions());
        assertEquals("control",
                     new SystemPermission("server", "control,control")
                             .getActions());
        assertEquals("control,monitor",
                     new SystemPermission("server", "control, monitor, control")
                             .getActions());
        assertEquals("control,monitor",
                     new SystemPermission("server", "monitor, control, monitor")
                             .getActions());
        assertEquals("control",
                     new SystemPermission("server", "CoNtRoL")
                             .getActions());
        assertEquals("control",
                     new SystemPermission("server", "CoNtRoL,control")
                             .getActions());

        String[] validNames = {
            SystemPermission.ENGINE,
            SystemPermission.JMX,
            SystemPermission.SERVER
        };
        
        // In order of the canonical actions expected
        String[] validActions = {
            SystemPermission.CONTROL,
            SystemPermission.MONITOR,
            SystemPermission.SHUTDOWN,
        };
        
        // Check all valid combinations (which is all) with
        // a single action
        Permission[] all = new Permission[
                        validNames.length * validActions.length];
        
        int c = 0;
        for (int tn = 0; tn < validNames.length; tn++)
        {
            for (int a = 0; a < validActions.length; a++) {
                Permission p = new SystemPermission(
                        validNames[tn], validActions[a]);
                
                assertEquals(validNames[tn], p.getName());
                assertEquals(validActions[a], p.getActions());
                
                // test SystemPermission.equals()
                assertFalse(p.equals(null));
                assertFalse(p.equals(new Object()));
                
                this.assertEquivalentPermissions(p, p);

                all[c++] = p;
            }
        }
        // All the permissions are different.
        checkDistinctPermissions(all);
        
        // Check two actions
        for (int n = 0; n < validNames.length; n++)
        {
            for (int a = 0; a < validActions.length; a++)
            {
                Permission base = new SystemPermission(
                        validNames[n], validActions[a]);
                
                // Two actions
                for (int oa = 0; oa < validActions.length; oa++)
                {
                    Permission p = new SystemPermission(
                            validNames[n],                           
                            validActions[a] + "," + validActions[oa]);
                    
                    if (oa == a)
                    {
                        // Same action added twice
                        assertEquivalentPermissions(base, p);
                        // Canonical form should collapse into a single action
                        assertEquals(validActions[a], p.getActions());
                    }
                    else
                    {
                        // Implies logic, the one with one permission
                        // is implied by the other but not vice-versa.
                        assertTrue(p.implies(base));
                        assertFalse(base.implies(p));
                        
                        // Names in canonical form
                        int f;
                        int s;
                        if (oa < a)
                        {
                            f = oa;
                            s = a;
                        }
                        else
                        {
                            f = a;
                            s = oa;
                        }
                        assertEquals(validActions[f] + "," + validActions[s],
                                p.getActions());
                    }
                }
            }
        }

        // DERBY-3476: The SystemPermission class should be final.
        assertTrue(Modifier.isFinal(SystemPermission.class.getModifiers()));
    }

    /**
     * Test that collections of SystemPermissions behave as expected.
     * Before DERBY-6717, adding multiple single-action permissions with
     * the same name didn't work.
     */
    public void testSystemPermissionCollections() {
        Permissions allPerms = new Permissions();
        for (String name : VALID_SYSPERM_NAMES) {
            for (String action : VALID_SYSPERM_ACTIONS) {
                allPerms.add(new SystemPermission(name, action));
            }
        }

        assertEquals(VALID_SYSPERM_NAMES.length,
                     Collections.list(allPerms.elements()).size());

        // Check that the collection of all system permissions also implies
        // all system permissions.
        for (String name : VALID_SYSPERM_NAMES) {
            for (String a1 : VALID_SYSPERM_ACTIONS) {
                // allPerms should imply any valid (name, action) pair.
                assertTrue(allPerms.implies(new SystemPermission(name, a1)));

                // allPerms should also imply any valid multi-action
                // system permission.
                for (String a2 : VALID_SYSPERM_ACTIONS) {
                    assertTrue(allPerms.implies(
                            new SystemPermission(name, a1 + ',' + a2)));
                }
            }
        }

        Permissions onePerm = new Permissions();
        onePerm.add(new SystemPermission("server", "shutdown"));

        // onePerm implies server shutdown and nothing else
        assertTrue(onePerm.implies(new SystemPermission("server", "shutdown")));
        assertFalse(onePerm.implies(
                        new SystemPermission("engine", "shutdown")));
        assertFalse(onePerm.implies(
                        new SystemPermission("server", "shutdown,monitor")));

        Permissions somePerms = new Permissions();
        somePerms.add(new SystemPermission("server", "shutdown"));
        somePerms.add(new SystemPermission("jmx", "shutdown,monitor"));
        somePerms.add(new SystemPermission("engine", "shutdown,control"));
        somePerms.add(new SystemPermission("engine", "control,monitor"));

        // somePerms implies the shutdown action for server
        assertTrue(somePerms.implies(
                new SystemPermission("server", "shutdown")));
        assertFalse(somePerms.implies(
                new SystemPermission("server", "control")));
        assertFalse(somePerms.implies(
                new SystemPermission("server", "monitor")));
        assertFalse(somePerms.implies(
                new SystemPermission("server", "shutdown,monitor")));

        // somePerms implies the shutdown and monitor actions for jmx
        assertTrue(somePerms.implies(new SystemPermission("jmx", "shutdown")));
        assertTrue(somePerms.implies(new SystemPermission("jmx", "monitor")));
        assertFalse(somePerms.implies(new SystemPermission("jmx", "control")));
        assertTrue(somePerms.implies(
                new SystemPermission("jmx", "shutdown,monitor")));
        assertTrue(somePerms.implies(
                new SystemPermission("jmx", "monitor,shutdown")));
        assertFalse(somePerms.implies(
                new SystemPermission("jmx", "monitor,shutdown,control")));

        // somePerms implies shutdown, control and monitor for engine
        assertTrue(somePerms.implies(
                new SystemPermission("engine", "shutdown")));
        assertTrue(somePerms.implies(
                new SystemPermission("engine", "control")));
        assertTrue(somePerms.implies(
                new SystemPermission("engine", "monitor")));
        assertTrue(somePerms.implies(
                new SystemPermission("engine", "shutdown,monitor")));
        assertTrue(somePerms.implies(
                new SystemPermission("engine", "shutdown,monitor,control")));

        // A SystemPermission collection should not accept other permissions.
        SystemPermission sp = new SystemPermission("engine", "monitor");
        PermissionCollection collection = sp.newPermissionCollection();
        try {
            collection.add(new AllPermission());
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // Read-only collections cannot be added to.
        collection.setReadOnly();
        try {
            collection.add(sp);
            fail();
        } catch (SecurityException se) {
            // expected
        }

        // The collection does not imply other permission types.
        assertFalse(collection.implies(new AllPermission()));
    }
    
    /**
     * Tests SystemPermissions against the Policy.
     */
    public void policyTestSystemPermissionGrants() {
        final Permission shutdown
            = new SystemPermission(
                SystemPermission.SERVER,
                SystemPermission.SHUTDOWN);
        
        // test SystemPermission for authorized user
        final SystemPrincipal authorizedUser
            = new SystemPrincipal("authorizedSystemUser");
        execute(authorizedUser, new ShutdownAction(shutdown), true);
        
        // test SystemPermission for unauthorized user
        final SystemPrincipal unAuthorizedUser
            = new SystemPrincipal("unAuthorizedSystemUser");
        execute(unAuthorizedUser, new ShutdownAction(shutdown), false);
    }
    
    /**
     * Tests DatabasePermission.
     */   
    public void testDatabasePermission() throws IOException {
        // test DatabasePermission with null url
        try {
            new DatabasePermission(null, DatabasePermission.CREATE);
            fail("expected NullPointerException");
        } catch (NullPointerException ex) {
            // expected exception
        }

        // test DatabasePermission with empty url
        try {
            new DatabasePermission("", DatabasePermission.CREATE);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }
        
        // test DatabasePermission with illegal url
        try {
            new DatabasePermission("no_url", DatabasePermission.CREATE);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // test DatabasePermission with unsupported protocol
        try {
            new DatabasePermission("unknown:test", DatabasePermission.CREATE);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // this test's commented out because it's platform-dependent
        // (no reliable way to make it pass on Unix)
        // test DatabasePermission with non-canonicalizable URL
        //try {
        //    //new DatabasePermission("directory:.*/\\:///../",
        //    //                       DatabasePermission.CREATE);
        //    new DatabasePermission("directory:\n/../../../.*/\\:///../",
        //                           DatabasePermission.CREATE);
        //    fail("expected IOException");
        //} catch (IOException ex) {
        //    // expected exception
        //}

        // test DatabasePermission with null actions
        try {
            new DatabasePermission("directory:dir", null);
            fail("expected NullPointerException");
        } catch (NullPointerException ex) {
            // expected exception
        }

        // test DatabasePermission with empty actions
        try {
            new DatabasePermission("directory:dir", "");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }
        
        // test DatabasePermission with illegal action list
        try {
            new DatabasePermission("directory:dir", "illegal_action");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // test DatabasePermission with illegal action list
        try {
            new DatabasePermission("directory:dir", "illegal,action");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }
    
        // test DatabasePermission with illegal action list
        try {
            new DatabasePermission("directory:dir", "illegal,create,action");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // test DatabasePermission on illegal action list
        try {
            new DatabasePermission("directory:dir", "illegal;action");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // test DatabasePermission with illegal action list
        try {
            new DatabasePermission("directory:dir", ",");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // test DatabasePermission with illegal action list
        try {
            new DatabasePermission("directory:dir", " ");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // test DatabasePermission with illegal action list
        try {
            new DatabasePermission("directory:dir", "create,");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // test DatabasePermission on relative directory paths
        final DatabasePermission[] relDirPathPerms
            = new DatabasePermission[relDirPaths.length];
        for (int i = 0; i < relDirPaths.length; i++) {
            relDirPathPerms[i]
                = new DatabasePermission(relDirPaths[i],
                                         DatabasePermission.CREATE);
        }
        checkNameAndActions(relDirPathPerms, relDirPaths);
        checkHashCodeAndEquals(relDirPathPerms, relDirPathPerms);
        checkImplies(relDirPathPerms, relDirPathPerms, dirPathImpls);

        // test DatabasePermission on relative directory path aliases
        final DatabasePermission[] relDirPathAliasPerms
            = new DatabasePermission[relDirPathAliases.length];
        for (int i = 0; i < relDirPathAliases.length; i++) {
            relDirPathAliasPerms[i]
                = new DatabasePermission(relDirPathAliases[i],
                                         DatabasePermission.CREATE);
        }
        checkNameAndActions(relDirPathAliasPerms, relDirPathAliases);
        checkHashCodeAndEquals(relDirPathPerms, relDirPathAliasPerms);
        checkImplies(relDirPathPerms, relDirPathAliasPerms, dirPathImpls);
        checkImplies(relDirPathAliasPerms, relDirPathPerms, dirPathImpls);

        // test DatabasePermission on absolute directory paths
        final DatabasePermission[] absDirPathPerms
            = new DatabasePermission[absDirPaths.length];
        for (int i = 0; i < absDirPaths.length; i++) {
            absDirPathPerms[i]
                = new DatabasePermission(absDirPaths[i],
                                         DatabasePermission.CREATE);
        }
        checkNameAndActions(absDirPathPerms, absDirPaths);
        checkHashCodeAndEquals(absDirPathPerms, absDirPathPerms);
        checkImplies(absDirPathPerms, absDirPathPerms, dirPathImpls);

        // test DatabasePermission on absolute directory path aliases
        final DatabasePermission[] absDirPathAliasPerms
            = new DatabasePermission[absDirPathAliases.length];
        for (int i = 0; i < absDirPathAliases.length; i++) {
            absDirPathAliasPerms[i]
                = new DatabasePermission(absDirPathAliases[i],
                                         DatabasePermission.CREATE);
        }
        checkNameAndActions(absDirPathAliasPerms, absDirPathAliases);
        checkHashCodeAndEquals(absDirPathPerms, absDirPathAliasPerms);
        checkImplies(absDirPathPerms, absDirPathAliasPerms, dirPathImpls);
        checkImplies(absDirPathAliasPerms, absDirPathPerms, dirPathImpls);
        
        // test DatabasePermission for the inclusive path specification
        final String inclPermissionUrl = "directory:<<ALL FILES>>";
        final DatabasePermission[] inclPerms
            = { new DatabasePermission(inclPermissionUrl,
                                       DatabasePermission.CREATE) };
        checkNameAndActions(inclPerms,
                            new String[]{ inclPermissionUrl });
        final DatabasePermission[] inclPerms1
            = { new DatabasePermission(inclPermissionUrl,
                                       DatabasePermission.CREATE) };
        checkHashCodeAndEquals(inclPerms, inclPerms1);
        checkImplies(inclPerms, inclPerms1, new boolean[][]{ { true } });
        final boolean[][] allTrue = new boolean[1][dirPaths.length];
        for (int j = 0; j < dirPaths.length; j++) {
            allTrue[0][j] = true;
        }
        final boolean[][] allFalse = new boolean[dirPaths.length][1];
        for (int i = 0; i < dirPaths.length; i++) {
            allFalse[i][0] = false;
        }
        checkImplies(inclPerms, relDirPathPerms, allTrue);
        checkImplies(relDirPathPerms, inclPerms, allFalse);
        checkImplies(inclPerms, relDirPathAliasPerms, allTrue);
        checkImplies(relDirPathAliasPerms, inclPerms, allFalse);
        checkImplies(inclPerms, absDirPathPerms, allTrue);
        checkImplies(absDirPathPerms, inclPerms, allFalse);
        checkImplies(inclPerms, absDirPathAliasPerms, allTrue);
        checkImplies(absDirPathAliasPerms, inclPerms, allFalse);

        // Actions string is washed (lower-cased, trimmed) and duplicates
        // are removed.
        DatabasePermission perm =
                new DatabasePermission("directory:dir", "create, create");
        assertEquals("create", perm.getActions());
        perm = new DatabasePermission("directory:dir", "  CrEaTe  ");
        assertEquals("create", perm.getActions());

        // DERBY-3476: The DatabasePermission class should be final.
        assertTrue(Modifier.isFinal(DatabasePermission.class.getModifiers()));
    }

    /**
     * Tests DatabasePermissions against the Policy.
     */
    public void policyTestDatabasePermissionGrants() throws IOException {
        final DatabasePermission[] relDirPathPerms
            = new DatabasePermission[relDirPaths.length];
        for (int i = 0; i < relDirPaths.length; i++) {
            relDirPathPerms[i]
                = new DatabasePermission(relDirPaths[i],
                                         DatabasePermission.CREATE);
        }

        // test DatabasePermission for unauthorized, authorized, and
        // all-authorized users
        final int[] singleLocPaths = { 2, 3, 6, 7 };
        final SystemPrincipal authorizedUser
            = new SystemPrincipal("authorizedSystemUser");
        final SystemPrincipal unAuthorizedUser
            = new SystemPrincipal("unAuthorizedSystemUser");
        final SystemPrincipal superUser
            = new SystemPrincipal("superUser");
        for (int i = 0; i < singleLocPaths.length; i++) {
            final int j = singleLocPaths[i];
            execute(unAuthorizedUser,
                    new CreateDatabaseAction(relDirPathPerms[j]), false);
            execute(authorizedUser,
                    new CreateDatabaseAction(relDirPathPerms[j]), (j != 6));
            execute(superUser,
                    new CreateDatabaseAction(relDirPathPerms[j]), true);
        }

        // test DatabasePermission for any user
        final SystemPrincipal anyUser
            = new SystemPrincipal("anyUser");
        final DatabasePermission dbPerm
            = new DatabasePermission("directory:dir",
                                     DatabasePermission.CREATE);
        execute(anyUser,
                new CreateDatabaseAction(dbPerm), true);
    }

    /**
     * Test serialization of permissions. In particular, test that
     * deserialization of invalid objects fails.
     */
    public void testSerialization() throws IOException {
        testDatabasePermissionSerialization();
        testSystemPermissionSerialization();
        testSystemPrincipalSerialization();
    }

    /**
     * Test serialization and deserialization of DatabasePermission objects.
     */
    private void testDatabasePermissionSerialization() throws IOException {
        // Simple test of serialization/deserialization of a valid object
        DatabasePermission perm =
                new DatabasePermission("directory:dir", "create");
        assertEquals(perm, serializeDeserialize(perm, null));

        // Test of relative paths
        for (String url : relDirPaths) {
            perm = new DatabasePermission(url, "create");
            assertEquals(perm, serializeDeserialize(perm, null));
        }

        // Test of relative path aliases
        for (String url : relDirPathAliases) {
            perm = new DatabasePermission(url, "create");
            assertEquals(perm, serializeDeserialize(perm, null));
        }

        // Test of absolute paths
        for (String url : absDirPaths) {
            perm = new DatabasePermission(url, "create");
            assertEquals(perm, serializeDeserialize(perm, null));
        }

        // Test of absolute path aliases
        for (String url : absDirPathAliases) {
            perm = new DatabasePermission(url, "create");
            assertEquals(perm, serializeDeserialize(perm, null));
        }

        // Actions should be normalized when read from the stream.
        for (String actions :
                Arrays.asList("create", "CrEaTe", " create ,  create")) {
            perm = serializeDeserialize(
                    createDBPermNoCheck("directory:dir", actions),
                    null);
            assertEquals("create", perm.getActions());
        }

        // Null URL should fail on deserialization (didn't before DERBY-3476)
        perm = createDBPermNoCheck(null, "create");
        serializeDeserialize(perm, NullPointerException.class);

        // Empty URL should fail on deserialization (didn't before DERBY-3476)
        perm = createDBPermNoCheck("", "create");
        serializeDeserialize(perm, IllegalArgumentException.class);

        // Unsupported protocol should fail on deserialization (didn't before
        // DERBY-3476)
        perm = createDBPermNoCheck("unknown:test", "create");
        serializeDeserialize(perm, IllegalArgumentException.class);

        // Null actions should fail on deserialization
        serializeDeserialize(createDBPermNoCheck("directory:dir", null),
                             NullPointerException.class);

        // Empty and invalid actions should fail on deserialization
        serializeDeserialize(createDBPermNoCheck("directory:dir", ""),
                             IllegalArgumentException.class);
        serializeDeserialize(createDBPermNoCheck("directory:dir", " "),
                             IllegalArgumentException.class);
        serializeDeserialize(createDBPermNoCheck("directory:dir", ","),
                             IllegalArgumentException.class);
        serializeDeserialize(createDBPermNoCheck("directory:dir", "create,"),
                             IllegalArgumentException.class);
        serializeDeserialize(createDBPermNoCheck("directory:dir", "invalid"),
                             IllegalArgumentException.class);
        serializeDeserialize(createDBPermNoCheck("directory:dir",
                                                 "create,invalid"),
                             IllegalArgumentException.class);
    }

    /**
     * Test serialization and deserialization of SystemPermission objects.
     */
    private void testSystemPermissionSerialization() throws IOException {
        // Test all valid name/action combinations. All should succeed to
        // serialize and deserialize.
        for (String name : VALID_SYSPERM_NAMES) {
            for (String action : VALID_SYSPERM_ACTIONS) {
                // Actions are case-insensitive, so test both lower-case
                // and upper-case.
                SystemPermission pl =
                    new SystemPermission(name, action.toLowerCase(Locale.US));
                SystemPermission pu =
                    new SystemPermission(name, action.toUpperCase(Locale.US));
                assertEquals(pl, serializeDeserialize(pl, null));
                assertEquals(pu, serializeDeserialize(pu, null));
            }
        }

        // A permission can specify multiple actions ...
        SystemPermission sp = new SystemPermission(
                "server", "control,monitor,shutdown");
        assertEquals(sp, serializeDeserialize(sp, null));

        // ... but only a single name, so this should fail.
        // (Did not fail before DERBY-3476.)
        serializeDeserialize(
                createSyspermNoCheck("server,jmx", "control"),
                IllegalArgumentException.class);

        // Invalid and duplicate actions should be ignored.
        sp = serializeDeserialize(createSyspermNoCheck(
                    VALID_SYSPERM_NAMES[0],
                    "control,invalid,control,,shutdown"),
                null);
        // The next assert failed before DERBY-3476.
        assertEquals("control,shutdown", sp.getActions());

        // Empty action is allowed.
        sp = new SystemPermission(VALID_SYSPERM_NAMES[0], "");
        assertEquals(sp, serializeDeserialize(sp, null));

        // Name is case-sensitive, so this should fail.
        // (Did not fail before DERBY-3476.)
        serializeDeserialize(createSyspermNoCheck(
                VALID_SYSPERM_NAMES[0].toUpperCase(Locale.US),
                VALID_SYSPERM_ACTIONS[0]),
            IllegalArgumentException.class);

        // Empty name is not allowed.
        serializeDeserialize(createSyspermNoCheck(
                "",
                VALID_SYSPERM_ACTIONS[0]),
                IllegalArgumentException.class);

        // Null name is not allowed.
        serializeDeserialize(createSyspermNoCheck(
                null,
                VALID_SYSPERM_ACTIONS[0]),
            NullPointerException.class);

        // Null action is not allowed.
        // (Did not fail before DERBY-3476.)
        serializeDeserialize(createSyspermNoCheck(
                VALID_SYSPERM_NAMES[0],
                null),
            NullPointerException.class);

        // Test serialization of SystemPermission collections.

        // Serialization should work on empty collection.
        PermissionCollection collection = sp.newPermissionCollection();
        PermissionCollection readCollection =
                serializeDeserialize(collection, null);
        assertFalse(readCollection.elements().hasMoreElements());

        // Serialization should work on non-empty collection.
        sp = new SystemPermission(
                VALID_SYSPERM_NAMES[0], VALID_SYSPERM_ACTIONS[0]);
        collection = sp.newPermissionCollection();
        collection.add(sp);
        readCollection = serializeDeserialize(collection, null);
        assertEquals(Arrays.asList(sp),
                     Collections.list(readCollection.elements()));

        // Deserialization should fail if the collection contains a
        // permission with invalid name.
        collection.add(createSyspermNoCheck("invalid_name", "control"));
        serializeDeserialize(collection, IllegalArgumentException.class);

        // Deserialization should fail if the collection contains a
        // permission that is not a SystemPermission.
        collection = sp.newPermissionCollection();
        HashMap<String, Permission> permissions =
                new HashMap<String, Permission>();
        permissions.put("engine", new AllPermission());
        setField(collection.getClass(), "permissions", collection, permissions);
        serializeDeserialize(collection, ClassCastException.class);
    }

    /**
     * Test serialization of SystemPrincipal objects.
     */
    private void testSystemPrincipalSerialization() throws IOException {
        // Serialize and deserialize a valid object.
        SystemPrincipal p = new SystemPrincipal("superuser");
        assertEquals(p, serializeDeserialize(p, null));

        // Deserialize a SystemPrincipal whose name is null. Should fail.
        setField(SystemPrincipal.class, "name", p, null);
        serializeDeserialize(p, NullPointerException.class);

        // Deserialize a SystemPrincipal whose name is empty. Should fail.
        setField(SystemPrincipal.class, "name", p, "");
        serializeDeserialize(p, IllegalArgumentException.class);
    }

    /**
     * Create a DatabasePermission object without checking that the URL
     * and the actions are valid.
     *
     * @param url the URL of the permission
     * @param actions the actions of the permission
     * @return a DatabasePermission instance
     */
    private static DatabasePermission createDBPermNoCheck(
            String url, String actions) throws IOException {
        // First create a valid permission object, so that the checks in
        // the constructor are happy.
        DatabasePermission perm =
                new DatabasePermission("directory:dir", "create");

        // Then use reflection to override the values of the fields with
        // potentially invalid values.
        setField(Permission.class, "name", perm, url);
        setField(DatabasePermission.class, "actions", perm, actions);

        return perm;
    }

    /**
     * Create a new SystemPermission object without checking that the name
     * and actions are valid.
     *
     * @param name the name of the permission
     * @param actions the actions of the permission
     * @return a SystemPermission instance
     */
    private static SystemPermission
                        createSyspermNoCheck(String name, String actions) {
        // First create a valid permission object, so that the checks in
        // the constructor are happy.
        SystemPermission sysperm = new SystemPermission("server", "control");

        // Then use reflection to override the values of the fields with
        // potentially invalid values.
        setField(Permission.class, "name", sysperm, name);
        setField(SystemPermission.class, "actions", sysperm, actions);

        return sysperm;
    }

    /**
     * Forcefully set the value of a field, ignoring access checks such as
     * final and private.
     *
     * @param klass the class in which the field lives
     * @param name the name of the field
     * @param object the object to change
     * @param value the new value of the field
     */
    private static void setField(
            Class<?> klass, String name, Object object, Object value) {
        try {
            Field f = klass.getDeclaredField(name);
            f.setAccessible(true);
            f.set(object, value);
        } catch (NoSuchFieldException ex) {
            fail("Cannot find field " + name, ex);
        } catch (IllegalAccessException ex) {
            fail("Cannot access field " + name, ex);
        }
    }

    /**
     * Serialize an object, then deserialize it from where it's stored, and
     * finally return the deserialized object.
     *
     * @param <T> the type of the object being serialized
     * @param object the object to serialize
     * @param expectedException the type of exception being expected during
     *   deserialization, or {@code null} if deserialization is expected to
     *   be successful
     * @return the deserialized object, or {@code null} if deserialization
     *   failed as expected
     * @throws IOException if an I/O error occurred
     */
    // We actually do check the type before we cast the result to T, but the
    // compiler doesn't understand it because we're using a JUnit assert.
    // Ignore the warning.
    @SuppressWarnings("unchecked")
    private static <T> T serializeDeserialize(
                T object,
                Class<? extends Exception> expectedException)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.close();

        ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(baos.toByteArray()));
        try {
            Object deserialized = ois.readObject();
            assertNull("should have failed", expectedException);
            assertEquals(object.getClass(), deserialized.getClass());
            // This generates a compile-time warning because the compiler
            // doesn't understand that the deserialized object has to be of
            // the correct type when the above assert succeeds. Because of
            // this, "unchecked" warnings are suppressed in this method.
            return (T) deserialized;
        } catch (Exception e) {
            if (expectedException == null ||
                    !expectedException.isAssignableFrom(e.getClass())) {
                fail("unexpected exception", e);
            }
            return null;
        } finally {
            ois.close();
        }
    }

    /**
     * Runs a privileged user action for a given principal.
     */
    private <T> void execute(SystemPrincipal principal,
                         PrivilegedAction<T> action,
                         boolean isGrantExpected) {
        //println();
        //println("    testing action " + action);
        
        final RunAsPrivilegedUserAction<T> runAsPrivilegedUserAction
            = new RunAsPrivilegedUserAction<T>(principal, action);
        try {
            AccessController.doPrivileged(runAsPrivilegedUserAction);
            //println("    Congrats! access granted " + action);
            if (!isGrantExpected) {
                fail("expected AccessControlException");
            }
        } catch (AccessControlException ace) {
            //println("    Yikes! " + ace.getMessage());
            if (isGrantExpected) {
                //fail("caught AccessControlException");
                throw ace;
            }
        }
    }
    
    /**
     * Tests DatabasePermission.getName() and .getActions().
     */
    private void checkNameAndActions(DatabasePermission[] dbperm,
                                     String[] dbpath)
        throws IOException {
        //assert(dpperm.length == dbpath.length)
        for (int i = 0; i < dbperm.length; i++) {
            final DatabasePermission dbp = dbperm[i];
            assertEquals("test: " + dbp + ".getName()",
                         dbpath[i], dbp.getName());
            assertEquals("test: " + dbp + ".getActions()",
                         DatabasePermission.CREATE, dbp.getActions());
        }
    }

    /**
     * Tests DatabasePermission.hashCode() and .equals().
     */
    private void checkHashCodeAndEquals(Permission[] dbp0,
                                        Permission[] dbp1)
        throws IOException {
        //assert(dbp0.length == dbp1.length)
        for (int i = 0; i < dbp0.length; i++) {
            final Permission p0 = dbp0[i];
            for (int j = 0; j < dbp0.length; j++) {
                final Permission p1 = dbp1[j];
                if (i == j) {
                    assertTrue(p0.hashCode() == p1.hashCode());
                    assertTrue(p0.equals(p1));
                } else {
                    assertTrue(p0.hashCode() != p1.hashCode());
                    assertTrue(!p0.equals(p1));
                }
            }
        }
    }
    
    /**
     * Tests DatabasePermission.implies().
     */
    private void checkImplies(Permission[] dbp0,
                              Permission[] dbp1,
                              boolean[][] impls)
        throws IOException {
        for (int i = 0; i < dbp0.length; i++) {
            final Permission p0 = dbp0[i];
            for (int j = 0; j < dbp1.length; j++) {
                final Permission p1 = dbp1[j];
                assertEquals("test: " + p0 + ".implies" + p1,
                             impls[i][j], p0.implies(p1));
                //assertEquals("test: " + p1 + ".implies" + p0,
                //             impls[j][i], p1.implies(p0));
            }
        }
    }
    
    /**
     * Check thet a set of Permission objects are distinct,
     * do not equal or imply each other.
     */
    private void checkDistinctPermissions(Permission[] set)
    {
        for (int i = 0; i < set.length; i++)
        {
            Permission pi = set[i];
            for (int j = 0; j < set.length; j++) {
                
                Permission pj = set[j];
                
                if (i == j)
                {
                    // Permission is itself
                    assertEquivalentPermissions(pi, pj);
                    continue;
                }
                
                assertFalse(pi.equals(pj));
                assertFalse(pj.equals(pi));
                
                assertFalse(pi.implies(pj));
                assertFalse(pj.implies(pi));
            }
        }
    }
    
    private void assertEquivalentPermissions(Permission p1,
            Permission p2) {
        assertTrue(p1.equals(p2));
        assertTrue(p2.equals(p1));
        
        
        assertEquals(p1.hashCode(), p2.hashCode());
        
        assertTrue(p1.implies(p2));
        assertTrue(p2.implies(p1));
    }
    
    /**
     * Represents a Shutdown server and engine action.
     */
    public class ShutdownAction
        implements PrivilegedAction<Void> {
        protected final Permission permission;

        public ShutdownAction(Permission permission) {
            this.permission = permission;
        }
    
        public Void run() {
            //println("    checking access " + permission + "...");
            AccessController.checkPermission(permission);
            //println("    granted access " + this);
            return null;
        }

        public String toString() {
            return permission.toString();
        }
    }

    /**
     * Represents a Create Database action.
     */
    public class CreateDatabaseAction
        implements PrivilegedAction<Void> {
        protected final Permission permission;

        public CreateDatabaseAction(Permission permission) {
            this.permission = permission;
        }

        public Void run() {
            //println("    checking access " + permission + "...");
            AccessController.checkPermission(permission);
            //println("    granted access " + this);
            return null;
        }

        public String toString() {
            return permission.toString();
        }
    }

    /**
     * Returns the Authorization Identifier for a principal name.
     *
     * @param name the name of the principal
     * @return the authorization identifier for this principal
     */
    static private String getAuthorizationId(String name) {
        // RuntimeException messages not localized
        if (name == null) {
            throw new NullPointerException("name can't be null");
        }
        if (name.length() == 0) {
            throw new IllegalArgumentException("name can't be empty");
        }
        try {
            return IdUtil.getUserAuthorizationId(name);
        } catch (StandardException se) {
            throw new IllegalArgumentException(se.getMessage());
        }
    }

    /**
     * Represents a Privileged User action.
     */
    static public class RunAsPrivilegedUserAction<T>
        implements PrivilegedAction<T> {
        final private SystemPrincipal principal;
        final private PrivilegedAction<? extends T> action;

        public RunAsPrivilegedUserAction(SystemPrincipal principal,
                                         PrivilegedAction<? extends T> action) {
            this.principal = principal;
            this.action = action;
        }
        
        public T run() {
            final boolean readOnly = true;
            final Set<SystemPrincipal> principals =
                    new HashSet<SystemPrincipal>();
            final Set publicCredentials = new HashSet();
            final Set privateCredentials = new HashSet();
            // add the given principal
            principals.add(principal);
            // also add a principal with the "normalized" name for testing
            // authorization ids
            final String normalized = getAuthorizationId(principal.getName());
            principals.add(new SystemPrincipal(normalized));
            final Subject subject = new Subject(readOnly,
                                                principals,
                                                publicCredentials,
                                                privateCredentials);

            // check subject's permission with a fresh AccessControlContext,
            // not the thread's current one (Subject.doAs(subject, action))
            // println("    run doAsPrivileged() as " + principal + "...");
            // The alternative approach to use Subject.doAs(subject, action)
            // instead of Subject.doAsPrivileged(subject, action, null) has
            // issues: there are subtile differences between these methods
            // regarding the checking of the caller's protection domain.  To
            // make doAs() work, the shutdown/createDatabase permissions must
            // be granted to the codebase (class RunAsPrivilegedUserAction).
            // This, however, defeats the purpose since everyone now's granted
            // permission.  In contrast, doAsPrivileged() with a null ACC
            // seems to effectively ignore the caller's protection domain, so
            // the check now only depends on the principal's permissions.
            return Subject.doAsPrivileged(subject, action, null);
            //Subject.doAs(subject, action);
        }
    }
}
