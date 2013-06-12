/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.engine.RestrictiveFilePermissionsTest

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

package org.apache.derbyTesting.functionTests.tests.engine;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import javax.sql.DataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.*;

/**
 * Tests related to use of restrictive file permissions (DERBY-5363).
 */
public class RestrictiveFilePermissionsTest extends BaseJDBCTestCase {

    final static String dbName = "RFPT_db";
    final static String dbName2 = dbName + "2";
    final static String exportFileName = "ourExport.txt";
    final static String exportFileName2 = "ourExport2.txt";
    final static String exportLobFileName = "ourExport.lob";
    final static String backupDir = "RFPT_backup";
    final static String derbyDotLog = dbName + ".log";

    static String home = null; // derby.system.home

    // Perhaps the test user is already running with umask 0077, if so we have
    // no way of discerning if Derby really does anything..
    static boolean supportsLaxTesting = false;

    public RestrictiveFilePermissionsTest(String name) {
        super(name);
    }


    public static Test suite() throws Exception {
        // We can not test file permissions before Java 1.7
        if (JVMInfo.JDK_ID < JVMInfo.J2SE_17) {
            println("warning: testing of strict permissions in " +
                    "RestrictiveFilePermissionsTest can not take place, " +
                    "need Java 7");
            return new TestSuite();
        }

        File f = new File("system/testPermissions");
        assertTrue(f.mkdirs());

        supportsLaxTesting =
            checkAccessToOwner(
                f,
                false,
                UNKNOWN);

        if (!supportsLaxTesting) {
            println("warning: testing of lax file permissions in" +
                    "RestrictiveFilePermissionsTest can not take place, " +
                    "use a more liberal runtime default (umask) for the tests");
        }

        assertDirectoryDeleted(f);

        // First collect the tests that check that, if we actually do restrict
        // permissions, the files created by Derby actually *are*
        // restricted. We test that with an embedded Derby with explicit
        // setting of the property derby.storage.useDefaultFilePermissions.
        // The extra setup file is for testJarFiles.

        TestSuite totalSuite = new TestSuite("RestrictiveFilePermissionsTest");

        Properties p = new Properties();
        p.put("derby.storage.useDefaultFilePermissions", "false");
        p.put("derby.stream.error.file", derbyDotLog);

        totalSuite.addTest(
            new SystemPropertyTestSetup(
                TestConfiguration.singleUseDatabaseDecorator(
                    new SupportFilesSetup(
                        new TestSuite(
                            RestrictiveFilePermissionsTest.class,
                            "haveWeGotAllCreatedFilesSuite"),
                        new String[] {"functionTests/tests/lang/dcl_id.jar"}),
                    dbName),
                p,
                true));

        // Next, test deployment modes, since default settings depend on them

        // For server started from command line, we should still get secure
        // permissions.
        if (Derby.hasServer()) {
            totalSuite.addTest(
                new NetworkServerTestSetup(
                    new RestrictiveFilePermissionsTest(
                        "doTestCliServerIsRestrictive"),
                    new String[]{}, // system properties
                    new String[]{}, // non-default start up arguments
                    true));
        }

        // For server started from API, we should see lax permissions.
        //
        if (supportsLaxTesting) {
            totalSuite.addTest(
                TestConfiguration.clientServerDecorator(
                    new RestrictiveFilePermissionsTest(
                        "doTestNonCliServerIsLax")));

            // For embedded, we should see lax permissions also.
            //
            p = new Properties();
            p.put("derby.stream.error.file", derbyDotLog + ".lax");

            totalSuite.addTest(
                new SystemPropertyTestSetup(
                    new RestrictiveFilePermissionsTest("dotestEmbeddedIsLax"),
                    p,
                    true));
        }

        return totalSuite;
    }


    public void setUp() throws Exception {
        getConnection();
        home = getSystemProperty("derby.system.home");
    }


    // Tests that check that check that files and directories actually have got
    // restricted permissions.
    //
    public void testDerbyDotLog() throws Exception {
        File derbydotlog = new File(home, derbyDotLog);

        checkAccessToOwner(derbydotlog, POSITIVE);
    }


    public void testDbDirectory() throws Exception {
        File derbyDbDir = new File(home, dbName);

        checkAccessToOwner(derbyDbDir, POSITIVE);
    }


    public void testServiceProperties() throws Exception {
        File servProp = new File(home, dbName + "/" + "service.properties");

        checkAccessToOwner(
            servProp, POSITIVE);
    }


    public void testTmpDirectory() throws Exception {
        File tmp = new File(home, dbName + "/" + "tmp");

        // create a temporary table so we get a file inside the tmp dir
        prepareStatement("declare global temporary table foo(i int) " +
                         "on commit preserve rows not logged").executeUpdate();

        checkAccessToOwner(
            tmp, true, POSITIVE);
    }


    public void testLockFiles() throws Exception {
        File dbLck = new File(home, dbName + "/" + "db.lck");
        File dbexLck = new File(home, dbName + "/" + "dbex.lck");

        checkAccessToOwner(dbLck, POSITIVE);

        if (PrivilegedFileOpsForTests.exists(dbexLck)) {
            checkAccessToOwner(dbexLck, POSITIVE);
        }
    }


    public void testSeg0AndConglomerates() throws Exception {
        File seg0 = new File(home, dbName + "/" + "seg0");

        checkAccessToOwner(
            seg0, true, POSITIVE);
    }


    public void testLogDirAndLogFiles() throws Exception {
        File seg0 = new File(home, dbName + "/" + "log");

        checkAccessToOwner(
            seg0, true, POSITIVE);
    }


    public void testExportedFiles() throws Exception {

        Statement s = createStatement();

        // Simple exported table file
        s.executeUpdate("call SYSCS_UTIL.SYSCS_EXPORT_TABLE(" +
                        "    'SYS'," +
                        "    'SYSTABLES'," +
                        "    '" + home + "/" + dbName + "/" +
                        exportFileName + "'," +
                        "    NULL," + // column delimiter
                        "    NULL," + // character delimiter
                        "    NULL)"); // code set
        File exp = new File(home, dbName + "/" + exportFileName);

        checkAccessToOwner(exp, POSITIVE);

        // Make a lob table and insert one row
        s.executeUpdate("create table lobtable(i int, c clob)");
        PreparedStatement ps = prepareStatement(
            "insert into lobtable values (1,?)");
        ps.setCharacterStream(
            1, new LoopingAlphabetReader(1000), 1000);
        ps.executeUpdate();

        // Export the lob table
        s.executeUpdate("call SYSCS_UTIL.SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE(" +
                        "    'SYS'," +
                        "    'SYSTABLES'," +
                        "    '" + home + "/" + dbName + "/" +
                        exportFileName2 + "'," +
                        "    NULL," + // column delimiter
                        "    NULL," + // character delimiter
                        "    NULL," + // code set
                        "    '" + home + "/" + dbName + "/" +
                        exportLobFileName + "')");

        File exp2 = new File(home, dbName + "/" + exportFileName2);
        File expLob = new File(home, dbName + "/" + exportLobFileName);

        // Check resuling exported files
        checkAccessToOwner(exp2, POSITIVE);

        checkAccessToOwner(expLob, POSITIVE);
    }


    public void testConglomsAfterCompress() throws Exception {
        Statement s = createStatement();
        s.executeUpdate("create table comptable(i int primary key, j int)");
        s.executeUpdate("create index secondary on comptable(j)");

        // insert some rows
        PreparedStatement ps = prepareStatement(
            "insert into comptable values (?,?)");
        setAutoCommit(false);
        for (int i=0; i < 10000; i++) {
            ps.setInt(1,i); ps.setInt(2,i);
            ps.executeUpdate();
        }
        commit();

        // delete helf the rows
        s.executeUpdate("delete from comptable where MOD(i, 2) = 0");
        commit();

        // compress
        setAutoCommit(true);
        s.executeUpdate("call SYSCS_UTIL.SYSCS_COMPRESS_TABLE(" +
                        "'APP', 'COMPTABLE', 0)");

        // easiest: just check all conglomerates over again..
        File seg0 = new File(home, dbName + "/" + "seg0");

        checkAccessToOwner(
            seg0, true, POSITIVE);
    }


    public void testTruncateTable() throws Exception {

        Statement s = createStatement();
        s.executeUpdate("create table trunctable(i int)");
        PreparedStatement ps = prepareStatement(
            "insert into trunctable values (?)");

        // insert some data
        setAutoCommit(false);
        for (int i=0; i < 1000; i++) {
            ps.setInt(1,i);
            ps.executeUpdate();
        }
        commit();
        setAutoCommit(true);

        // truncate the table
        s.executeUpdate("truncate table trunctable");

        // easiest: just check all conglomerates over again..
        File seg0 = new File(home, dbName + "/" + "seg0");

        checkAccessToOwner(
            seg0, true, POSITIVE);
    }


    public void testBackupRestoreFiles() throws Exception {

        // First fill the db with a jar and a temporary file, so it's
        // not trivially looking inside

        URL jar = SupportFilesSetup.getReadOnlyURL("dcl_id.jar");
        assertNotNull("dcl_id.jar", jar);

        CallableStatement cs = prepareCall("CALL SQLJ.INSTALL_JAR(?, ?, 0)");
        cs.setString(1, jar.toExternalForm());
        cs.setString(2, "testBackupFiles");
        cs.executeUpdate();

        // create a temporary table so we get a file inside the tmp dir
        prepareStatement("declare global temporary table foo(i int) " +
                         "on commit preserve rows not logged").executeUpdate();

        cs = prepareCall
            ("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        String fullBackupDir = home + "/" + backupDir;
        cs.setString(1, fullBackupDir);
        cs.execute();

        File fbd = new File(fullBackupDir);
        checkAccessToOwner(
            fbd, true, POSITIVE);

        // DERBY-6258: When taking a backup, a file called BACKUP.HISTORY
        // is created in the original database directory. Verify that its
        // permissions are restricted.
        final File db = new File(home, dbName);
        checkAccessToOwner(db, true, POSITIVE);

        // Prepare to restore
        TestConfiguration.getCurrent().shutdownDatabase();

        // Restore to same db (should replace existing)
        final DataSource ds = JDBCDataSource.getDataSource();
        final String fullRestoreDir =
            home + "/" + backupDir + "/" + dbName;

        JDBCDataSource.setBeanProperty(
            ds, "connectionAttributes", "restoreFrom=" + fullRestoreDir);
        final Connection con = ds.getConnection();

        checkAccessToOwner(
            db, true, POSITIVE);

        con.close();

        // Restore to another db than current default
        final DataSource ds2 = JDBCDataSource.getDataSource(dbName2);
        JDBCDataSource.setBeanProperty(
            ds2, "connectionAttributes", "restoreFrom=" + fullRestoreDir);
        final Connection con2 = ds2.getConnection();

        final File newDb = new File(home, dbName2);

        checkAccessToOwner(
            newDb, true, POSITIVE);
        con2.close();

        // close down both
        final DataSource[] srcs =
                new DataSource[] {JDBCDataSource.getDataSource(),
                    JDBCDataSource.getDataSource(dbName2)};

        for (int i=0; i < srcs.length; i++) {
            JDBCDataSource.setBeanProperty(
                    srcs[i], "connectionAttributes", "shutdown=true");

            try {
                srcs[i].getConnection();
                fail("shutdown failed: expected exception");
            } catch (SQLException e) {
                assertSQLState(
                    "database shutdown",
                    SQLStateConstants.CONNECTION_EXCEPTION_CONNECTION_FAILURE,
                    e);
            }
        }

        assertDirectoryDeleted(newDb);
        assertDirectoryDeleted(new File(home + "/" + backupDir));
    }


    public void testJarFiles() throws Exception {
        URL jar = SupportFilesSetup.getReadOnlyURL("dcl_id.jar");
        assertNotNull("dcl_id.jar", jar);

        CallableStatement cs = prepareCall("CALL SQLJ.INSTALL_JAR(?, ?, 0)");
        cs.setString(1, jar.toExternalForm());
        cs.setString(2, "anyName");
        cs.executeUpdate();
        File jarsDir = new File(home, dbName + "/" + "jar");

        checkAccessToOwner(
            jarsDir, true, POSITIVE);
    }


    // Derby deployment mode tests: defaults for permissions differ.
    //

    public void doTestCliServerIsRestrictive() throws Exception {
        NetworkServerControl nsctrl =
                NetworkServerTestSetup.getNetworkServerControl();
        String traceDir = home + "/" + dbName + "_tracefiles_restr";
        nsctrl.setTraceDirectory(traceDir);
        nsctrl.trace(true);
        nsctrl.ping();
        nsctrl.trace(false);

        File traceDirF = new File(traceDir);

        checkAccessToOwner(
            traceDirF, true, POSITIVE);
        nsctrl.shutdown();
        assertDirectoryDeleted(traceDirF);
    }


    public void doTestNonCliServerIsLax() throws Exception {
        NetworkServerControl nsctrl =
                NetworkServerTestSetup.getNetworkServerControl();
        String traceDir = home + "/" + dbName + "_tracefiles_lax";
        nsctrl.setTraceDirectory(traceDir);
        nsctrl.trace(true);
        nsctrl.ping();
        nsctrl.trace(false);

        File traceDirF = new File(traceDir);

        checkAccessToOwner(
            traceDirF, true, NEGATIVE);

        nsctrl.shutdown();
        assertDirectoryDeleted(traceDirF);
    }


    public void dotestEmbeddedIsLax() throws Exception {
        File derbydotlogF = new File(home, derbyDotLog + ".lax");
        checkAccessToOwner(derbydotlogF, NEGATIVE);
    }


    // Auxiliary methods
    //

    final public static int NEGATIVE = 0; // expected check outcome set
    final public static int POSITIVE = 1;
    final public static int UNKNOWN = 2;

    // Members used by limitAccessToOwner
    private static boolean initialized = false;

    // Reflection helper objects for calling into Java >= 7
    private static Class<?> filesClz;
    private static Class<?> pathClz;
    private static Class<?> pathsClz;
    private static Class<?> aclEntryClz;
    private static Class<?> aclFileAttributeViewClz;
    private static Class<?> posixFileAttributeViewClz;
    private static Class<?> posixFileAttributesClz;
    private static Class<?> posixFilePermissionClz;
    private static Class<?> userPrincipalClz;
    private static Class<?> linkOptionArrayClz;
    private static Class<?> linkOptionClz;
    private static Class<?> stringArrayClz;
    private static Class<?> fileStoreClz;

    private static Method get;
    private static Method getFileAttributeView;
    private static Method supportsFileAttributeView;
    private static Method getFileStore;
    private static Method getOwner;
    private static Method getAcl;
    private static Method principal;
    private static Method getName;
    private static Method permissionsAcl;
    private static Method permissionsPosix;
    private static Method readAttributes;

    private static Field GROUP_EXECUTE;
    private static Field GROUP_READ;
    private static Field GROUP_WRITE;
    private static Field OTHERS_EXECUTE;
    private static Field OTHERS_READ;
    private static Field OTHERS_WRITE;
    private static Set<Object> unwantedPermissions;
    /**
     * Check that the file has access only for the owner. Will throw (JUnit
     * failure) if permissions are not strict.
     * <p/>
     * We need Java 7 to ascertain whether we managed to restrict file
     * permissions: The Java 6 {@code java.io.File} API only lets us check if
     * this process has access.
     * <p/>
     * In this sense this testing code is asymmetric to the implementation in
     * Derby: Java 6 can be used to restrict accesses in Java 6 on Unix, but
     * we have no way in Java of checking the results in a portable way. So, if
     * we do not have at least Java 7, this method will be a no-op.
     *
     * @param file (or directory) for which we want to check permissions
     * @param expectedOutcome NEGATIVE or POSITIVE
     * @see #checkAccessToOwner(File, boolean, int)
     */

    public static void checkAccessToOwner(
            final File file,
            int expectedOutcome) throws Exception {

        checkAccessToOwner(file, false, expectedOutcome);
    }


    /**
     * Same as {@link #checkAccessToOwner(File, int) checkAccessToOwner}, but
     * if {@code doContents} is true, also check files directly contained in
     * this file qua directory (not recursively).
     *
     * @param file ((or directory) for which we want to check permissions
     * @param doContents if a directory, an error to call with true if not
     * @param expectedOutcome NEGATIVE, POSITIVE or UNKNOWN
     * @return true if accesses exist for others that owner (expectedOutcome ==
     *              UNKNOWN)
     */
    private static boolean checkAccessToOwner(
            final File file,
            final boolean doContents,
            final int expectedOutcome) throws Exception {
        // Needs to be called in security context since tests run with security
        // manager.
        if (doContents) {
            // visit immediately contained file in this directory also
            checkAccessToOwner(file, false, expectedOutcome);

            AccessController.doPrivileged(
                    new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    File [] files = file.listFiles();
                    for (int i = 0; i < files.length; i++){
                        checkAccessToOwner(
                            files[i], false, expectedOutcome);
                    }
                    return null;
                }});
        }

        return AccessController.
            doPrivileged(new PrivilegedExceptionAction<Boolean>() {

                public Boolean run() throws Exception {
                    // lazy initialization
                    if (!initialized) {
                        initialized = true;

                        // If found, we have >= Java 7.
                        filesClz = Class.forName(
                            "java.nio.file.Files");
                        pathClz = Class.forName(
                            "java.nio.file.Path");
                        pathsClz = Class.forName(
                            "java.nio.file.Paths");
                        aclEntryClz = Class.forName(
                            "java.nio.file.attribute.AclEntry");
                        aclFileAttributeViewClz = Class.forName(
                            "java.nio.file.attribute." +
                            "AclFileAttributeView");
                        posixFileAttributeViewClz = Class.forName(
                            "java.nio.file.attribute." +
                            "PosixFileAttributeView");
                        posixFileAttributesClz = Class.forName(
                            "java.nio.file.attribute." +
                            "PosixFileAttributes");
                        posixFilePermissionClz = Class.forName(
                            "java.nio.file.attribute." +
                            "PosixFilePermission");
                        userPrincipalClz = Class.forName(
                            "java.nio.file.attribute.UserPrincipal");
                        linkOptionArrayClz = Class.forName(
                            "[Ljava.nio.file.LinkOption;");
                        linkOptionClz = Class.forName(
                            "java.nio.file.LinkOption");
                        stringArrayClz = Class.forName(
                            "[Ljava.lang.String;");
                        fileStoreClz = Class.forName(
                            "java.nio.file.FileStore");

                        get = pathsClz.
                            getMethod("get",
                                      new Class[]{String.class,
                                                  stringArrayClz});

                        getFileAttributeView = filesClz.
                            getMethod("getFileAttributeView",
                                      new Class[]{pathClz,
                                                  Class.class,
                                                  linkOptionArrayClz});
                        supportsFileAttributeView = fileStoreClz.getMethod(
                            "supportsFileAttributeView",
                            new Class[]{Class.class});
                        getFileStore = filesClz.getMethod("getFileStore",
                                                          new Class[]{pathClz});
                        getOwner = filesClz.
                            getMethod(
                                "getOwner",
                                new Class[]{pathClz,
                                            linkOptionArrayClz});
                        getAcl = aclFileAttributeViewClz.
                            getMethod("getAcl", new Class[]{});
                        principal = aclEntryClz.
                            getMethod("principal", new Class[]{});
                        getName = userPrincipalClz.
                            getMethod("getName", new Class[]{});
                        permissionsAcl = aclEntryClz.
                            getMethod("permissions", new Class[]{});
                        permissionsPosix = posixFileAttributesClz.
                            getMethod("permissions", new Class[]{});
                        readAttributes = posixFileAttributeViewClz.
                            getMethod("readAttributes", new Class[]{});

                        GROUP_EXECUTE =
                            posixFilePermissionClz.getField("GROUP_EXECUTE");
                        GROUP_READ =
                            posixFilePermissionClz.getField("GROUP_READ");
                        GROUP_WRITE =
                            posixFilePermissionClz.getField("GROUP_WRITE");
                        OTHERS_EXECUTE =
                            posixFilePermissionClz.getField("OTHERS_EXECUTE");
                        OTHERS_READ =
                            posixFilePermissionClz.getField("OTHERS_READ");
                        OTHERS_WRITE =
                            posixFilePermissionClz.getField("OTHERS_WRITE");
                        unwantedPermissions = new HashSet<Object>();
                        unwantedPermissions.add(GROUP_EXECUTE.get(null));
                        unwantedPermissions.add(GROUP_READ.get(null));
                        unwantedPermissions.add(GROUP_WRITE.get(null));
                        unwantedPermissions.add(OTHERS_EXECUTE.get(null));
                        unwantedPermissions.add(OTHERS_READ.get(null));
                        unwantedPermissions.add(OTHERS_WRITE.get(null));
                    }

                    // Only used with expectedOutcome == UNKNOWN, otherwise
                    // we throw:
                    boolean someThingBeyondOwnerFound = false;

                    // We have Java 7. We need to call reflectively, since
                    // the source level isn't yet at Java 7.
                    try {
                        Object fileP = get.invoke(
                            null, new Object[]{file.getPath(),
                                               new String[]{}});

                        // ACLs supported on this platform? Check the current
                        // file system:
                        Object fileStore = getFileStore.invoke(
                            null,
                            new Object[]{fileP});

                        boolean aclsSupported =
                            ((Boolean)supportsFileAttributeView.invoke(
                                fileStore,
                                new Object[]{aclFileAttributeViewClz})).
                            booleanValue();

                        Object aclView = getFileAttributeView.invoke(
                            null,
                            new Object[]{
                                fileP,
                                aclFileAttributeViewClz,
                                Array.newInstance(linkOptionClz, 0)});

                        Object posixView = getFileAttributeView.invoke(
                            null,
                            new Object[]{
                                fileP,
                                posixFileAttributeViewClz,
                                Array.newInstance(linkOptionClz, 0)});

                        if (aclsSupported && aclView != null &&
                                posixView == null) {
                            // Windows
                            Object owner = getOwner.invoke(
                                null,
                                new Object[]{
                                    fileP,
                                    Array.newInstance(linkOptionClz, 0)});

                            List oldAcl =
                                (List)getAcl.invoke(aclView, (Object[])null);

                            for (Iterator i = oldAcl.iterator(); i.hasNext();) {
                                Object ace = i.next();
                                Object princ =
                                    principal.invoke(ace, (Object[])null);
                                String princName =
                                    (String)getName.invoke(
                                        princ, (Object[])null);

                                if (posixView != null) {
                                    if (princName.equals("OWNER@")) {
                                        // OK, permission for owner

                                    } else if (
                                        princName.equals("GROUP@") ||
                                        princName.equals("EVERYONE@")) {

                                        Set s = (Set)permissionsAcl.invoke(
                                            ace,
                                            (Object[])null);

                                        if (expectedOutcome == POSITIVE) {
                                            assertTrue(
                                                "Non-empty set of  " +
                                                "permissions for uid: " +
                                                princName + " for file " + file,
                                                s.isEmpty());
                                        } else {
                                            someThingBeyondOwnerFound =
                                                !s.isEmpty();
                                        }
                                    }
                                } else {
                                    // NTFS, hopefully
                                    if (princ.equals(owner)) {
                                        // OK
                                    } else {
                                        if (expectedOutcome == POSITIVE) {
                                            fail(
                                                "unexpected uid " + princName +
                                                " can access file " + file);
                                        } else {
                                            someThingBeyondOwnerFound = true;
                                        }
                                    }

                                }
                            }
                        } else if (posixView != null) {
                            // Unixen
                            Object posixFileAttributes =
                                readAttributes.invoke(posixView,
                                                      new Object[]{});
                            Set permissionsSet =
                                (Set)permissionsPosix.invoke(
                                    posixFileAttributes, new Object[]{});

                            for (Iterator i = permissionsSet.iterator();
                                 i.hasNext();) {
                                Object perm = i.next();

                                if (unwantedPermissions.contains(perm)) {
                                    if (expectedOutcome == POSITIVE) {
                                        fail("unwanted permission " + perm +
                                             " for file " + file);
                                    }
                                    someThingBeyondOwnerFound = true;
                                    break;
                                }
                            }
                        } else {
                            fail();
                        }

                        if (expectedOutcome == NEGATIVE &&
                                !someThingBeyondOwnerFound) {
                            fail(
                                "unexpected restrictive access: " + file);
                        }

                    } catch (IllegalAccessException e) {
                        // coding error
                        if (SanityManager.DEBUG) {
                            SanityManager.THROWASSERT(e);
                        }
                    } catch (IllegalArgumentException e) {
                        // coding error
                        if (SanityManager.DEBUG) {
                            SanityManager.THROWASSERT(e);
                        }
                    } catch (InvocationTargetException e) {
                        throw e;
                    }

                    if (expectedOutcome != UNKNOWN) {
                        println("checked perms on: " + file);
                    }

                    if (expectedOutcome == UNKNOWN &&
                            someThingBeyondOwnerFound) {
                        return Boolean.TRUE;
                    } else {
                        return Boolean.FALSE;
                    }
                }});
    }
}
