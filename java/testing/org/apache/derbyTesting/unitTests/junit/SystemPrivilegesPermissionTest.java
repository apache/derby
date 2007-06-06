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
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import java.security.AccessController;
import java.security.AccessControlException;
import java.security.Permission;
import javax.security.auth.Subject;

import org.apache.derby.authentication.DatabasePrincipal;
import org.apache.derby.security.SystemPermission;
import org.apache.derby.security.DatabasePermission;


/**
 * This class tests the basic permission classes for system privileges.
 */
public class SystemPrivilegesPermissionTest extends BaseTestCase {

    /**
     * This test's policy file.
     */
    static private String POLICY_FILE_NAME
        = "org/apache/derbyTesting/unitTests/junit/SystemPrivilegesPermissionTest.policy";

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
     * For instance, dirPathImplications[1][2] shows the expected value for:
     * <ul>
     * <li> DP("directory:*").implies(DP(directory:level0"))
     * <li> DP("directory:./*").implies(DP(directory:./level0"))
     * <li> DP("directory:/*").implies(DP(directory:/level0"))
     * <li> DP("directory:/dummy/..*").implies(DP(directory:/dummy/..level0"))
     * </ul>
     */
    static private final boolean[][] dirPathImplications = {
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
     * Add decorators to a test run to establish a security manager
     * with this test's policy file.
     */
    static private Test decorateTest(String method) {
        final SystemPrivilegesPermissionTest undecoratedTest
            = new SystemPrivilegesPermissionTest(method);

        // install a security manager using this test's policy file
        return new SecurityManagerSetup(undecoratedTest, POLICY_FILE_NAME);
    }
    

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
        //final TestSuite ts
        //    = new TestSuite("SystemPrivilegesPermissionTest suite");
        //ts.addTest(decorateTest("testSystemPrivileges"));
        //return ts;
        return decorateTest("testSystemPrivileges");
    }

    /**
     * Test case that does a check of the XXX
     */
    public void testSystemPrivileges() throws IOException {
        //System.out.println("--> testSystemPrivileges()");
        //System.out.println("    java.security.policy = "
        //                   + System.getProperty("java.security.policy"));
        //System.out.println("    System.getSecurityManager() = "
        //                   + System.getSecurityManager());
        assertSecurityManager();
        execute();
        //System.out.println("<-- testSystemPrivileges()");
    }

    /**
     * Tests SystemPermissions.
     */
    public void execute() throws IOException {
        checkSystemPermission();
        checkDatabasePermission();
    }
    
    /**
     * Tests SystemPermission.
     */
    private void checkSystemPermission() throws IOException {
        final DatabasePrincipal authorizedUser
            = new DatabasePrincipal("authorizedSystemUser");
        final DatabasePrincipal unAuthorizedUser
            = new DatabasePrincipal("unAuthorizedSystemUser");

        // test SystemPermission with null name argument
        try {
            new SystemPermission(null);
            fail("expected NullPointerException");
        } catch (NullPointerException ex) {
            // expected exception
        }

        // test SystemPermission with empty name argument
        try {
            new SystemPermission("");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }
        
        // test SystemPermission with illegal name argument
        try {
            new SystemPermission("illegal_name");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected exception
        }

        // test SystemPermission with legal name argument
        final Permission sp0 = new SystemPermission(SystemPermission.SHUTDOWN);
        final Permission sp1 = new SystemPermission(SystemPermission.SHUTDOWN);

        // test SystemPermission.getName()
        assertEquals(sp0.getName(), SystemPermission.SHUTDOWN);

        // test SystemPermission.getActions()
        assertEquals(sp0.getActions(), "");

        // test SystemPermission.hashCode()
        assertTrue(sp0.hashCode() == sp1.hashCode());

        // test SystemPermission.equals()
        assertTrue(sp0.equals(sp1));
        assertTrue(!sp0.equals(null));
        assertTrue(!sp0.equals(new Object()));

        // test SystemPermission.implies()
        assertTrue(sp0.implies(sp1));
        assertTrue(sp1.implies(sp0));

        // test SystemPermission for authorized user against policy file
        execute(authorizedUser, new ShutdownEngineAction(sp0), true);
        
        // test SystemPermission for unauthorized user against policy file
        execute(unAuthorizedUser, new ShutdownEngineAction(sp0), false);
    }
    
    /**
     * Tests DatabasePermission.
     */
    private void checkDatabasePermission() throws IOException {
        final DatabasePrincipal authorizedUser
            = new DatabasePrincipal("authorizedSystemUser");
        final DatabasePrincipal unAuthorizedUser
            = new DatabasePrincipal("unAuthorizedSystemUser");

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

        // test DatabasePermission with non-canonicalizable URL
        try {
            new DatabasePermission("directory:.*/\\:///../",
                                   DatabasePermission.CREATE);
            fail("expected IOException");
        } catch (IOException ex) {
            // expected exception
        }

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
        final DatabasePermission[] relDirPathPermissions
            = new DatabasePermission[relDirPaths.length];
        for (int i = 0; i < relDirPaths.length; i++) {
            relDirPathPermissions[i]
                = new DatabasePermission(relDirPaths[i],
                                         DatabasePermission.CREATE);
        }
        checkNameAndActions(relDirPathPermissions,
                            relDirPaths);
        checkHashCodeAndEquals(relDirPathPermissions,
                               relDirPathPermissions);
        checkImplies(relDirPathPermissions,
                     relDirPathPermissions);

        // test DatabasePermission on relative directory path aliases
        final DatabasePermission[] relDirPathAliasPermissions
            = new DatabasePermission[relDirPathAliases.length];
        for (int i = 0; i < relDirPathAliases.length; i++) {
            relDirPathAliasPermissions[i]
                = new DatabasePermission(relDirPathAliases[i],
                                         DatabasePermission.CREATE);
        }
        checkNameAndActions(relDirPathAliasPermissions,
                            relDirPathAliases);
        checkHashCodeAndEquals(relDirPathPermissions,
                               relDirPathAliasPermissions);
        checkImplies(relDirPathPermissions,
                     relDirPathAliasPermissions);

        // test DatabasePermission on absolute directory paths
        final DatabasePermission[] absDirPathPermissions
            = new DatabasePermission[absDirPaths.length];
        for (int i = 0; i < absDirPaths.length; i++) {
            absDirPathPermissions[i]
                = new DatabasePermission(absDirPaths[i],
                                         DatabasePermission.CREATE);
        }
        checkNameAndActions(absDirPathPermissions,
                            absDirPaths);
        checkHashCodeAndEquals(absDirPathPermissions,
                               absDirPathPermissions);
        checkImplies(absDirPathPermissions,
                     absDirPathPermissions);

        // test DatabasePermission on absolute directory path aliases
        final DatabasePermission[] absDirPathAliasPermissions
            = new DatabasePermission[absDirPathAliases.length];
        for (int i = 0; i < absDirPathAliases.length; i++) {
            absDirPathAliasPermissions[i]
                = new DatabasePermission(absDirPathAliases[i],
                                         DatabasePermission.CREATE);
        }
        checkNameAndActions(absDirPathAliasPermissions,
                            absDirPathAliases);
        checkHashCodeAndEquals(absDirPathPermissions,
                               absDirPathAliasPermissions);
        checkImplies(absDirPathPermissions,
                     absDirPathAliasPermissions);
        

        // test DatabasePermission for authorized user against policy file
        execute(authorizedUser,
                new CreateDatabaseAction(relDirPathPermissions[2]), true);
        execute(authorizedUser,
                new CreateDatabaseAction(relDirPathPermissions[3]), true);
        execute(authorizedUser,
                new CreateDatabaseAction(relDirPathPermissions[6]), false);
        execute(authorizedUser,
                new CreateDatabaseAction(relDirPathPermissions[7]), true);

        // test DatabasePermission for unauthorized user against policy file
        execute(unAuthorizedUser,
                new CreateDatabaseAction(relDirPathPermissions[2]), false);
        execute(unAuthorizedUser,
                new CreateDatabaseAction(relDirPathPermissions[3]), false);
        execute(unAuthorizedUser,
                new CreateDatabaseAction(relDirPathPermissions[6]), false);
        execute(unAuthorizedUser,
                new CreateDatabaseAction(relDirPathPermissions[7]), false);
    }

    /**
     * Runs a privileges user action for a given principal.
     */
    private void execute(DatabasePrincipal principal,
                         PrivilegedExceptionAction action,
                         boolean isGrantExpected) {
        //System.out.println();
        //System.out.println("    testing action " + action);
        final RunAsPrivilegedUserAction runAsPrivilegedUserAction
            = new RunAsPrivilegedUserAction(principal, action);
        try {
            AccessController.doPrivileged(runAsPrivilegedUserAction);
            //System.out.println("    Congrats! access granted " + action);
            if (!isGrantExpected) {
                fail("expected AccessControlException");
            }
        } catch (PrivilegedActionException pae) {
            //System.out.println("    Error: " + pae.getMessage());
            throw new RuntimeException(pae);
        } catch (AccessControlException ace) {
            if (isGrantExpected) {
                fail("caught AccessControlException");
            }
            //System.out.println("    Yikes! " + ace.getMessage());
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
    private void checkHashCodeAndEquals(DatabasePermission[] dbp0,
                                        DatabasePermission[] dbp1)
        throws IOException {
        //assert(dbp0.length == dbp1.length)
        for (int i = 0; i < dbp0.length; i++) {
            final DatabasePermission p0 = dbp0[i];
            for (int j = 0; j < dbp0.length; j++) {
                final DatabasePermission p1 = dbp1[j];
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
    private void checkImplies(DatabasePermission[] dbp0,
                              DatabasePermission[] dbp1)
        throws IOException {
        //assert(dbp0.length == dbp1.length)
        for (int i = 0; i < dbp0.length; i++) {
            final DatabasePermission p0 = dbp0[i];
            for (int j = 0; j < dbp0.length; j++) {
                final DatabasePermission p1 = dbp1[j];
                assertEquals("test: " + p0 + ".implies" + p1,
                             dirPathImplications[i][j], p0.implies(p1));
                assertEquals("test: " + p1 + ".implies" + p0,
                             dirPathImplications[j][i], p1.implies(p0));
            }
        }
    }
    
    /**
     * Represents a Shutdown Engine action.
     */
    public class ShutdownEngineAction
        implements PrivilegedExceptionAction {
        protected final Permission permission;

        public ShutdownEngineAction(Permission permission) {
            this.permission = permission;
        }
    
        public Object run() throws Exception {
            //System.out.println("    checking access " + permission + "...");
            AccessController.checkPermission(permission);
            //System.out.println("    granted access " + this);
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
        implements PrivilegedExceptionAction {
        protected final Permission permission;

        public CreateDatabaseAction(Permission permission) throws IOException {
            this.permission = permission;
        }
    
        public Object run() throws Exception {
            //System.out.println("    checking access " + permission + "...");
            AccessController.checkPermission(permission);
            //System.out.println("    granted access " + this);
            return null;
        }

        public String toString() {
            return permission.toString();
        }
    }

    /**
     * Represents a Privileged User action.
     */
    public class RunAsPrivilegedUserAction
        implements PrivilegedExceptionAction {
        final private DatabasePrincipal principal;
        final private PrivilegedExceptionAction action;

        public RunAsPrivilegedUserAction(DatabasePrincipal principal,
                                         PrivilegedExceptionAction action) {
            this.principal = principal;
            this.action = action;
        }
        
        public Object run() throws PrivilegedActionException {
            final Set principalSet = new HashSet();
            final Set noPublicCredentials = new HashSet();
            final Set noPrivateCredentials = new HashSet();
            principalSet.add(principal);
            final Subject subject = new Subject(true, principalSet,
                                                noPublicCredentials,
                                                noPrivateCredentials);
        
            // Subject.doAs(subject, action) not strong enough
            //System.out.println("    run doAsPrivileged() as " + principal
            //                   + "...");
            Subject.doAsPrivileged(subject, action, null);
            return null;
        }
    }
}
