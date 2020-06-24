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
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

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

//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite totalSuite =
            new BaseTestSuite("RestrictiveFilePermissionsTest");

        Properties p = new Properties();
        p.put("derby.storage.useDefaultFilePermissions", "false");
        p.put("derby.stream.error.file", derbyDotLog);

        totalSuite.addTest(
            new SystemPropertyTestSetup(
                TestConfiguration.singleUseDatabaseDecorator(
                    new SupportFilesSetup(
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
                        new BaseTestSuite(
                            RestrictiveFilePermissionsTest.class,
                            "haveWeGotAllCreatedFilesSuite"),
                        new String[] {"functionTests/tests/lang/dcl_id.jar"}),
                    dbName),
                p,
                true));

        // Next, test deployment modes, since default settings depend on them

        // For server started from command line, we should still get secure
        // permissions.
//IC see: https://issues.apache.org/jira/browse/DERBY-5677
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

//IC see: https://issues.apache.org/jira/browse/DERBY-5363
        checkAccessToOwner(derbydotlog, POSITIVE);
    }


    public void testDbDirectory() throws Exception {
        File derbyDbDir = new File(home, dbName);

        checkAccessToOwner(derbyDbDir, POSITIVE);
    }


    public void testServiceProperties() throws Exception {
        File servProp = new File(home, dbName + "/" + "service.properties");

        checkAccessToOwner(
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
            servProp, POSITIVE);
    }


    public void testTmpDirectory() throws Exception {
        File tmp = new File(home, dbName + "/" + "tmp");

        // create a temporary table so we get a file inside the tmp dir
        prepareStatement("declare global temporary table foo(i int) " +
                         "on commit preserve rows not logged").executeUpdate();

        checkAccessToOwner(
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
            tmp, true, POSITIVE);
    }


    public void testLockFiles() throws Exception {
        File dbLck = new File(home, dbName + "/" + "db.lck");
        File dbexLck = new File(home, dbName + "/" + "dbex.lck");

        checkAccessToOwner(dbLck, POSITIVE);
//IC see: https://issues.apache.org/jira/browse/DERBY-5363

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
//IC see: https://issues.apache.org/jira/browse/DERBY-5363

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
//IC see: https://issues.apache.org/jira/browse/DERBY-5363

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
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5363

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
//IC see: https://issues.apache.org/jira/browse/DERBY-5363

        con.close();

        // Restore to another db than current default
        final DataSource ds2 = JDBCDataSource.getDataSource(dbName2);
        JDBCDataSource.setBeanProperty(
            ds2, "connectionAttributes", "restoreFrom=" + fullRestoreDir);
        final Connection con2 = ds2.getConnection();

        final File newDb = new File(home, dbName2);

        checkAccessToOwner(
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5363

        nsctrl.shutdown();
        assertDirectoryDeleted(traceDirF);
    }


    public void dotestEmbeddedIsLax() throws Exception {
        File derbydotlogF = new File(home, derbyDotLog + ".lax");
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
        checkAccessToOwner(derbydotlogF, NEGATIVE);
    }


    // Auxiliary methods
    //

    final public static int NEGATIVE = 0; // expected check outcome set
    final public static int POSITIVE = 1;
    final public static int UNKNOWN = 2;

    // Members used by limitAccessToOwner
    private static final Set<PosixFilePermission> UNWANTED_PERMISSIONS =
//IC see: https://issues.apache.org/jira/browse/DERBY-6865
            Collections.unmodifiableSet(EnumSet.of(
                    PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.GROUP_WRITE,
                    PosixFilePermission.OTHERS_EXECUTE,
                    PosixFilePermission.OTHERS_READ,
                    PosixFilePermission.OTHERS_WRITE));

    /**
     * Check that the file has access only for the owner. Will throw (JUnit
     * failure) if permissions are not strict.
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
            checkAccessToOwner(file, false, expectedOutcome);
//IC see: https://issues.apache.org/jira/browse/DERBY-6865
            for (File f : PrivilegedFileOpsForTests.listFiles(file)) {
                checkAccessToOwner(f, false, expectedOutcome);
            }
        }

        return AccessController.
            doPrivileged((PrivilegedExceptionAction<Boolean>) () -> {
                // Only used with expectedOutcome == UNKNOWN, otherwise
                // we throw:
                boolean someThingBeyondOwnerFound = false;

                Path fileP = Paths.get(file.getPath());

                // ACLs supported on this platform? Check the current
                // file system:
                AclFileAttributeView aclView = Files.getFileAttributeView(
                        fileP, AclFileAttributeView.class);

                PosixFileAttributeView posixView = Files.getFileAttributeView(
                        fileP, PosixFileAttributeView.class);

                if (posixView != null) {
                    // Unixen
                    for (PosixFilePermission perm :
                            posixView.readAttributes().permissions()) {
                        if (UNWANTED_PERMISSIONS.contains(perm)) {
                            if (expectedOutcome == POSITIVE) {
                                fail("unwanted permission " + perm +
                                     " for file " + file);
                            }
                            someThingBeyondOwnerFound = true;
                            break;
                        }
                    }
                } else if (aclView != null) {
                    // Windows
                    UserPrincipal owner = Files.getOwner(fileP);
                    for (AclEntry ace : aclView.getAcl()) {
                        UserPrincipal princ = ace.principal();
                        // NTFS, hopefully
                        if (!princ.equals(owner)) {
                            if (expectedOutcome == POSITIVE) {
                                fail("unexpected uid " + princ.getName() +
                                        " can access file " + file);
                            }
                            someThingBeyondOwnerFound = true;
                            break;
                        }
                    }
                } else {
                    fail();
                }

                if (expectedOutcome == NEGATIVE && !someThingBeyondOwnerFound) {
                    fail("unexpected restrictive access: " + file);
                }

                if (expectedOutcome != UNKNOWN) {
                    println("checked perms on: " + file);
                }

                return expectedOutcome == UNKNOWN && someThingBeyondOwnerFound;
        });
    }
}
