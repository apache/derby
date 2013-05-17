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

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.SecurityManagerSetup;

import java.util.Set;
import java.util.HashSet;

import java.io.IOException;

import java.security.PrivilegedAction;
import java.security.AccessController;
import java.security.AccessControlException;
import java.security.Permission;
import javax.security.auth.Subject;

import org.apache.derby.authentication.SystemPrincipal;
import org.apache.derby.security.SystemPermission;
import org.apache.derby.security.DatabasePermission;

import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.error.StandardException;

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
     *
     * @throws Exception
     */
    public static Test suite() {
        // this suite cannot be constructed with automatic test extraction
        // (by passing a class argument); instead, the tests need to be added
        // manually since some of them require their own policy file
        TestSuite suite = new TestSuite("SystemPrivilegesPermissionTest");

        // add API tests for the basic security framework classes
        suite.addTest(
            new SystemPrivilegesPermissionTest("testSystemPrincipal"));
        suite.addTest(
            new SystemPrivilegesPermissionTest("testSystemPermission"));
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

        return suite;
    }

    /**
     * Tests SystemPrincipal.
     */
    public void testSystemPrincipal() {
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
                        if (oa < a)
                        assertEquals(validActions[f] + "," + validActions[s],
                                p.getActions());
                    }
                }
            }
        }
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
    
        // test DatabasePermission on illegal action list
        try {
            new DatabasePermission("directory:dir", "illegal;action");
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
        assertTrue(p1.implies(p2));
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
