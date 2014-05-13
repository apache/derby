/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ConstraintCharacteristicsTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.derbyTesting.functionTests.tests.memorydb.MemoryDbManager;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.XATestUtil;

public class ConstraintCharacteristicsTest extends BaseJDBCTestCase
{
    private static final String LANG_DUPLICATE_KEY_CONSTRAINT = "23505";
    private static final String LANG_DEFERRED_DUP_VIOLATION_T = "23506";
    private static final String LANG_DEFERRED_DUP_VIOLATION_S = "23507";
    private static final String LANG_CHECK_CONSTRAINT_VIOLATED = "23513";
    private static final String LANG_DEFERRED_CHECK_VIOLATION_T = "23514";
    private static final String LANG_DEFERRED_CHECK_VIOLATION_S = "23515";
    private static final String LANG_DEFERRED_FK_VIOLATION_T = "23516";
    private static final String LANG_DEFERRED_FK_VIOLATION_S = "23517";
    private static final String LOCK_TIMEOUT = "40XL1";
    private static final String LANG_INCONSISTENT_C_CHARACTERISTICS = "42X97";
    private static final String LANG_DROP_OR_ALTER_NON_EXISTING_C = "42X86";
    private static final String LANG_SYNTAX_ERROR = "42X01";
    private static final String NOT_IMPLEMENTED = "0A000";
    private static final String LANG_NOT_NULL_CHARACTERISTICS = "42XAN";
    private static final String LANG_OBJECT_NOT_FOUND = "42X94";
    private static final String LANG_DB2_DUPLICATE_NAMES = "42734";
    private static final String LANG_ADD_CHECK_CONSTRAINT_FAILED = "X0Y59";

    private static String expImpDataFile;  // file used to perform
                                           // import/export
    private static String expImpDataWithNullsFile; // file used to perform
                                                   // import/export
    private static boolean exportFilesCreatedEmbedded = false;
    private static boolean exportFilesCreatedClient = false;

    // Use in memory database for speed for some tests
    private static final MemoryDbManager dbm =
            MemoryDbManager.getSharedInstance();

    private static final int WAIT_TIMEOUT_DURATION = 1;

    public ConstraintCharacteristicsTest(String name) {
        super(name);
    }


    public static Test suite() {
        final String nameRoot = ConstraintCharacteristicsTest.class.getName();
        final TestSuite suite = new TestSuite(nameRoot);

        suite.addTest(baseSuite(nameRoot + ":embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
                baseSuite(nameRoot + ":client")));

        suite.addTest(restSuite(nameRoot + ":embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
                restSuite(nameRoot + ":client")));

        return suite;
    }

    // this suite holds tests that require a more optimal 
    // locks.waitTimeout setting.
    private static Test restSuite(final String name) {

        final TestSuite suite = new TestSuite(name);

        suite.addTest(new ConstraintCharacteristicsTest(
                "testDeferredRowsInvalidation"));
        suite.addTest(new ConstraintCharacteristicsTest(
                "testLockingForUniquePK"));

        final Properties systemProperties = new Properties();
        systemProperties.setProperty(
            "derby.locks.waitTimeout", Integer.toString(WAIT_TIMEOUT_DURATION));

        return new SupportFilesSetup(
                new SystemPropertyTestSetup(suite, systemProperties, true));
    }
    
    private static Test baseSuite(final String name) {
        final TestSuite suite = new TestSuite(name);

        suite.addTest(new ConstraintCharacteristicsTest(
                      "testSyntaxAndBinding"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testDropNotNullOnUniqueColumn"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testCompressTableOKUnique"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testLockingForUniquePKWithCommit"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testLockingForUniquePKWithRollback"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testDatabaseMetaData"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testCreateConstraintDictionaryEncodings"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testAlterConstraintDictionaryEncodings"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testAlterConstraintInvalidation"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testBasicDeferral"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testRoutines"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testImport"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testDerby6374"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testXA"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testAlmostRemovedAllDups"));
        suite.addTest(new ConstraintCharacteristicsTest(
                      "testCheckConstraintsWithDeferredRows"));
        suite.addTest(new ConstraintCharacteristicsTest(
                     "testSeveralCheckConstraints"));
        suite.addTest(new ConstraintCharacteristicsTest(
                     "testManySimilarDuplicates"));

        final Properties systemProperties = new Properties();
        systemProperties.setProperty(
            "derby.locks.waitTimeout", Integer.toString(500));

        return new SupportFilesSetup(
                new SystemPropertyTestSetup(suite, systemProperties, true));
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        final Statement s = createStatement();
        s.executeUpdate("create table referenced(" +
                        "    i int primary key, j int default 0)");

        if ((usingEmbedded() && !exportFilesCreatedEmbedded) ||
            (usingDerbyNetClient() && !exportFilesCreatedClient)) {

            // We have to do this once for embedded and once for client/server
            if (usingEmbedded()) {
                exportFilesCreatedEmbedded = true;
            } else {
                exportFilesCreatedClient = true;
            }

            // Create a file for import that contains duplicate rows,
            // see testImport and testDerby6374.
            //
            expImpDataFile =
                SupportFilesSetup.getReadWrite("t.data").getPath();
            expImpDataWithNullsFile =
                SupportFilesSetup.getReadWrite("t_with_nulls.data").getPath();
            s.executeUpdate("create table t(i int)");
            s.executeUpdate("insert into t values 1,-2,-2, 3");
            s.executeUpdate("create table t_with_nulls(i int)");
            s.executeUpdate("insert into t_with_nulls values 1,null, null, 3");
            s.executeUpdate(
                "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (" +
                "    'APP' , 'T' , '" + expImpDataFile + "'," +
                "    null, null , null)");
            s.executeUpdate(
                "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (" +
                "    'APP' , 'T_WITH_NULLS' , '" + expImpDataWithNullsFile +
                "', null, null , null)");
            s.executeUpdate("drop table t");
            s.executeUpdate("drop table t_with_nulls");
        }

        s.close();
        setAutoCommit(false);
    }

    @Override
    protected void tearDown() throws Exception {
        rollback();
        setAutoCommit(true);
        getConnection().createStatement().
                executeUpdate("drop table referenced");
        dbm.cleanUp();
        super.tearDown();
    }

    public void testSyntaxAndBinding() throws SQLException {
        final Connection c = dbm.createDatabase("cct");
        c.setAutoCommit(false);

        final Statement s = c.createStatement();
        s.executeUpdate("create table referenced(i int primary key)");
        c.commit();

        //
        //   T A B L E    L E V E L    C O N S T R A I N T S
        //

        assertTableLevelDefaultBehaviorAccepted(c, s);
        assertTableLevelNonDefaultAccepted(s);

        //
        //   A L T E R    C O N S T R A I N T    C H A R A C T E R I S T I C S
        //
        s.executeUpdate(
            "create table t(i int, constraint app.c primary key(i))");

        // default, so allow
        s.executeUpdate("alter table t alter constraint c enforced");

        // not default behavior, expect error until feature implemented
        assertStatementError(
            NOT_IMPLEMENTED, s,
            "alter table t alter constraint c not enforced");

        for (String ch : illegalAlterCharacteristics) {
            // Anything beyond enforcement is illegal in ALTER context
            assertStatementError(
                LANG_SYNTAX_ERROR, s, "alter table t alter constraint c " + ch);
        }

        // Unknown constraint name
        assertStatementError(
            LANG_DROP_OR_ALTER_NON_EXISTING_C, s,
            "alter table t alter constraint cuckoo not enforced");

        //
        //   S E T   C O N S T R A I N T
        //
        s.executeUpdate("alter table t drop constraint c");
        s.executeUpdate("alter table t add constraint c " +
                        "    primary key(i) deferrable");
        s.executeUpdate("set constraints c deferred");
        s.executeUpdate("set constraints all deferred");

        // Unknown constraint name
        assertStatementError(LANG_OBJECT_NOT_FOUND, s,
                             "set constraints cuckoo deferred");
        assertStatementError(LANG_DB2_DUPLICATE_NAMES , s,
                "set constraints c,c deferred");
        c.rollback();

        //
        //   C O L U M N    L E V E L    C O N S T R A I N T S
        //

        assertColumnLevelDefaultBehaviorAccepted(c, s);
        assertColumnLevelNonDefaultAccepted(s);

        // Characteristics are not currently allowed for NOT NULL,
        // since Derby does not represent NOT NULL as a constraint,
        // but rather as an aspect of the column's data type. It is
        // possible to alter the column nullable and vice versa,
        // though.
        assertStatementError(LANG_NOT_NULL_CHARACTERISTICS, s,
                    "create table t(i int " +
                    "not null deferrable initially immediate)");
    }

    /**
     * Check that constraint characteristics are correctly encoded
     * into the STATE column in SYS.SYSCONSTRAINTS.
     * Cf. specification attached to DERBY-532.
     *
     * FIXME: Note that this test runs with property derby.constraintsTesting
     * to bypass NOT IMPLEMENTED checks.  Remove this property usage when
     * DERBY-532 is done.
     * @throws SQLException
     */
    public void testCreateConstraintDictionaryEncodings() throws SQLException {
        final Statement s = getConnection().createStatement();

        for (String[] ch : defaultCharacteristics) {
            assertDictState(s, ch[0], ch[1]);
        }

        for (String[] ch : nonDefaultCharacteristics) {
            assertDictState(s, ch[0], ch[1]);
        }

        for (String ch : illegalCharacteristics) {
            assertCreateInconsistentCharacteristics(s, ch);
        }

        rollback();
    }

    /**
     * Check that constraint characteristics are correctly encoded
     * into the STATE column in SYS.SYSCONSTRAINTS.
     * Cf. specification attached to DERBY-532.
     *
     * FIXME: Note that this test runs with property derby.constraintsTesting
     * to bypass NOT IMPLEMENTED checks.  Remove this property usage when
     * DERBY-532 is done.
     * @throws SQLException
     */
    public void testAlterConstraintDictionaryEncodings() throws SQLException {
        final Statement s = getConnection().createStatement();

        for (String[] ch : defaultCharacteristics) {
            s.executeUpdate(
                    "create table t(i int, constraint c primary key(i) " +
                    ch[0] + ")");

            assertAlterDictState(s, "enforced");
            assertAlterDictState(s, "not enforced");
            rollback();
        }

        for (String[] ch : nonDefaultCharacteristics) {
            if (ch[0].contains("not enforced")) {

                assertStatementError(NOT_IMPLEMENTED,
                        s,
                        "create table t(i int, constraint c primary key(i) " +
                                ch[0] + ")");
            } else {
                s.executeUpdate(
                        "create table t(i int, constraint c primary key(i) " +
                                ch[0] + ")");

                assertAlterDictState(s, "enforced");
                assertAlterDictState(s, "not enforced");
                rollback();
            }
        }

        for (String ch : illegalAlterCharacteristics) {
            assertAlterInconsistentCharacteristics(s, ch);
        }
    }

    /**
     * Check that altering constraint characteristics invalidates prepared
     * statements.
     * @throws SQLException
     */
    public void testAlterConstraintInvalidation() throws SQLException {
        if (usingDerbyNetClient()) {
            // Skip, since we need to see inside an embedded connection here
            return;
        }

        final Connection c = getConnection();
        final Statement s = createStatement();

        s.executeUpdate("create table t(i int, constraint c primary key(i))");
        final PreparedStatement ps =
            c.prepareStatement("insert into t values 3");
        ps.execute();

        s.executeUpdate("alter table t alter constraint c enforced ");

        final ContextManager contextManager =
                ((EmbedConnection)c).getContextManager();
        final LanguageConnectionContext lcc =
                (LanguageConnectionContext)contextManager.getContext(
                "LanguageConnectionContext");
        final GenericPreparedStatement derbyPs =
                (GenericPreparedStatement)lcc.getLastActivation().
                getPreparedStatement();

        assertFalse(derbyPs.isValid());

        rollback();
    }


    static final String[] uniqueForms = {
            "create table t(i int, j int, constraint c primary key(i)",
            "create table t(i int, j int, constraint c unique(i)",
            "create table t(i int not null, j int, constraint c unique(i)"};

    static final String[] uniqueSpec = { // corresponding to above forms
            "primary key(i)",
            "unique(i)",
            "unique(i)"};

    static final String[] checkForms = {
            "create table t(i int, j int, constraint c check (i > 0)"};

    static final String[] fkForms = {
            "create table t(i int, j int, " +
            "    constraint c foreign key(i) references referenced(i)"};

    static final String[] checkSpec = { // corresponding to above forms
            "check (i > 0)"};

    static final String[][] initialContents = new String[][] {
            {"1", "10"},
            {"2", "20"},
            {"3", "30"}};

    static final String[][] negatedInitialContents = new String[][] {
            {"-1", "10"},
            {"-2", "20"},
            {"-3", "30"}};


    static final String[] setConstraintsForms = {
            "set constraints all",
            "set constraints c"};


    public void testDatabaseMetaData() throws SQLException {

        // Test that our constraint backing index is still reported as unique
        // even if we implement it as physically non-unique when deferrable:
        // logically it is still a unique index.
        final Statement s = createStatement();
        s.executeUpdate(
            "create table t(i int not null " +
            "    constraint c primary key deferrable initially immediate)");
        final DatabaseMetaData dbmd = s.getConnection().getMetaData();
        ResultSet rs = dbmd.getIndexInfo(null, null, "T", false, false);
        rs.next();
        assertEquals("false", rs.getString("NON_UNIQUE"));

        // Test that we get the right values for DEFERRABILITY in
        // getImportedKeys, getExportedKeys and getCrossReference

        String[] cchars = new String[]{
            "deferrable initially immediate",
            "deferrable initially deferred",
            "not deferrable"
        };

        int[] dbmdState = new int[]{
            DatabaseMetaData.importedKeyInitiallyImmediate,
            DatabaseMetaData.importedKeyInitiallyDeferred,
            DatabaseMetaData.importedKeyNotDeferrable,
        };

        for (int i = 0; i < cchars.length; i++) {
            s.executeUpdate(
                    "create table child(i int, constraint c2 foreign key(i) " +
                    "    references t(i) " + cchars[i] + ")");
            rs = dbmd.getImportedKeys(null, null, "CHILD");
            rs.next();
            assertEquals(
                    Integer.toString(dbmdState[i]),
                    rs.getString("DEFERRABILITY"));
            rs.close();

            rs = dbmd.getExportedKeys(null, null, "T");
            rs.next();
            assertEquals(
                    Integer.toString(dbmdState[i]),
                    rs.getString("DEFERRABILITY"));
            rs.close();

            rs = dbmd.getCrossReference(null, null, "T", null, null, "CHILD");
            rs.next();
            assertEquals(
                    Integer.toString(dbmdState[i]),
                    rs.getString("DEFERRABILITY"));
            rs.close();
            s.executeUpdate("drop table child");
        }
    }



    public void testLockingForUniquePK() throws SQLException {
        final Statement s = createStatement();
        s.executeUpdate(
            "create table t1(i int, " +
                "constraint c1 primary key(i) not deferrable)");
        s.executeUpdate(
            "create table t2(i int, " +
                "constraint c2 primary key(i) deferrable initially deferred)");
        s.executeUpdate("insert into t1 values 1,2,3");
        s.executeUpdate("insert into t2 values 1,2,3");
        commit();

        //
        // Locks for PK insert, not deferrable
        //
        // There is an X row lock on the inserted row.
        //
        s.executeUpdate("insert into t1 values 4");
        ResultSet rs = s.executeQuery(
                LockTableTest.getSelectLocksString());

        JDBC.assertFullResultSet(rs, new String[][]{
            {"APP", "UserTransaction", "TABLE", "2",
                "IX", "T1", "Tablelock", "GRANT", "ACTIVE"},
            {"APP", "UserTransaction", "ROW", "1",
                "X", "T1", "(1,10)", "GRANT", "ACTIVE"}
        });

        Connection c2 = null;

        try {
            // Verify that another transaction has to wait
            c2 = openDefaultConnection();
            c2.setAutoCommit(false);

            final Statement s2 = c2.createStatement();
            assertStatementError(LOCK_TIMEOUT, s2, "insert into t1 values 4");
        } finally {
            if (c2 != null) {
                c2.rollback();
                c2.close();
            }
        }

        commit();

        //
        // Locks for PK insert, deferrable, not a duplicate.
        //
        s.executeUpdate("insert into t2 values 4");
        rs = s.executeQuery(
                LockTableTest.getSelectLocksString());

        JDBC.assertFullResultSet(rs, new String[][]{
            {"APP", "UserTransaction", "TABLE", "1",
                "IS", "T2", "Tablelock", "GRANT", "ACTIVE"},
            {"APP", "UserTransaction", "TABLE", "2",
                "IX", "T2", "Tablelock", "GRANT", "ACTIVE"},
            {"APP", "UserTransaction", "ROW", "1",
                "X", "T2", "(1,10)", "GRANT", "ACTIVE"}});
        commit();

        //
        // Locks for PK insert, deferrable and a duplicate
        //
        s.executeUpdate("insert into t2 values 4");
        rs = s.executeQuery(
                LockTableTest.getSelectLocksString());
        JDBC.assertFullResultSet(rs, new String[][]{
            {"APP", "UserTransaction", "TABLE", "1",
                "IS", "T2", "Tablelock", "GRANT", "ACTIVE"},
            {"APP", "UserTransaction", "TABLE", "2",
                "IX", "T2", "Tablelock", "GRANT", "ACTIVE"},
            {"APP", "UserTransaction", "ROW", "1",
                "X", "T2", "(1,11)", "GRANT", "ACTIVE"}});

        try {
            // Verify that another transaction doesn't have to wait on insert
            // It will see a timeout and assume a duplicate for checking on
            // commit, which in this case will see the timeout instead.
            c2 = openDefaultConnection();
            c2.setAutoCommit(false);

            final Statement s2 = c2.createStatement();
            s2.executeUpdate("insert into t2 values 4");
            assertCommitError(LOCK_TIMEOUT, c2);
        } finally {
            try {
                if (c2 != null) {
                    c2.rollback();
                    c2.close();
                }
            } catch (SQLException e) {
            }
        }
        rollback();

        // Thread 1: insert a row (not a duplicate)
        s.executeUpdate("insert into t2 values 5");

        // Thread 2: insert same row (duplicate)
        c2 = openDefaultConnection();
        c2.setAutoCommit(false);
        final Statement s2 = c2.createStatement();
        s2.executeUpdate("insert into t2 values 5");

        // Thread 1: try to commit: should not time out we are not doing
        // a checking scan here
        commit();
        c2.rollback();

        //
        // Let a thread 2 insert a key before and after a key inserted by t1.
        // t1 should be able to commit without waiting because the checking
        // scan should not see the rows locked before and after t1's key
        // (read committed mode).
        //
        s2.executeUpdate("insert into t2 values 10,12");

        // Insert a duplicate,
        s.executeUpdate("insert into t2 values 11,11");

        // next delete one of the duplicates,
        final Statement us = createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        rs = us.executeQuery("select * from t2 where i=11");
        rs.next();
        rs.deleteRow();
        rs.close();

        // then try to commit.
        commit();

        // clean up
        c2.rollback();
        c2.close();
        s.executeUpdate("drop table t1");
        s.executeUpdate("drop table t2");
        commit();
    }

    public void testBasicDeferral() throws SQLException {
        final Statement s = createStatement();

        for (String sCF : setConstraintsForms) {
            int idx = 0;

            //
            //   P R I M A R Y   K E Y,   U N I Q U E   C O N S T R A I N T S
            //
            for (String ct : uniqueForms) {
                try {
                    s.executeUpdate(
                        ct + " deferrable initially immediate)");
                    s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                    commit();

                    //
                    //   I N S E R T   O F   D U P L I C A T E S
                    //

                    // Normal duplicate insert should fail, still
                    // immediate mode
                    assertStatementError(LANG_DUPLICATE_KEY_CONSTRAINT,
                                         s,
                                         "insert into t values (2,30)");

                    // Now set deferred mode in one of two ways: by specifying
                    // ALL or by naming our index explicitly.
                    s.executeUpdate(sCF + " deferred");

                    // Duplicate insert should now work
                    s.executeUpdate(
                        "insert into t values (2,19),(2,21),(3,31)");

                    // Check contents
                    JDBC.assertFullResultSet(
                        s.executeQuery("select * from t"),
                        new String[][] {
                            {"1", "10"},
                            {"2", "20"},
                            {"3", "30"},
                            {"2", "19"},
                            {"2", "21"},
                            {"3", "31"}});
                    // Check contents: specify ORDER BY and force use of index
                    // use the index.
                    JDBC.assertFullResultSet(
                        s.executeQuery(
                          "select * from t --DERBY-PROPERTIES constraint=c\n" +
                          "     order by i"),
                        new String[][] {
                            {"1", "10"},
                            {"2", "20"},
                            {"2", "19"},
                            {"2", "21"},
                            {"3", "30"},
                            {"3", "31"}});

                    // Try to set immediate mode, and detect violation
                    assertStatementError(LANG_DEFERRED_DUP_VIOLATION_S,
                                         s,
                                         sCF + " immediate");
                    // Once more, error above should not roll back
                    assertStatementError(LANG_DEFERRED_DUP_VIOLATION_S,
                                         s,
                                         sCF + " immediate");

                    // Now try to commit, which should lead to rollback
                    assertCommitError(LANG_DEFERRED_DUP_VIOLATION_T,
                                      getConnection());

                    // Verify that contents are the same as before we did the
                    // duplicate inserts
                    JDBC.assertFullResultSet(
                        s.executeQuery("select * from t"), initialContents);

                    // Setting immediate now should work again:
                    s.executeUpdate(sCF + " immediate");
                    assertStatementError(LANG_DUPLICATE_KEY_CONSTRAINT,
                                         s,
                                         "insert into t values (2,30)");

                    // setting deferred again:
                    s.executeUpdate(sCF + " deferred");

                    // Duplicate insert should now work
                    s.executeUpdate(
                        "insert into t values (2,19),(2,21),(3,31)");
                    assertStatementError(LANG_DEFERRED_DUP_VIOLATION_S,
                                         s,
                                         sCF + " immediate");
                    rollback();

                    //
                    //   U P D A T E   G I V I N G   D U P L I C A T E S
                    //

                    // Now set deferred mode in one of two ways: by specifying
                    // ALL or by naming our constraint explicitly.
                    s.executeUpdate(sCF + " deferred");

                    // Now test the same, but using UPDATE instead of INSERT
                    s.executeUpdate(
                            "insert into t values (20,19),(200,21),(30,31)");
                    s.executeUpdate("update t set i=2 where i=20");
                    s.executeUpdate("update t set i=2 where i=200");
                    s.executeUpdate("update t set i=3 where i=30");

                    // Check result: specify ORDER BY and force use of index
                    // use the index
                    JDBC.assertFullResultSet(
                        s.executeQuery(
                          "select * from t --DERBY-PROPERTIES constraint=c\n" +
                          "     order by i"),
                        new String[][] {
                            {"1", "10"},
                            {"2", "20"},
                            {"2", "19"},
                            {"2", "21"},
                            {"3", "30"},
                            {"3", "31"}});

                    // Now try to commit, which should lead to rollback
                    assertCommitError(LANG_DEFERRED_DUP_VIOLATION_T,
                                      getConnection());

                    // Verify that contents are the same as before we did the
                    // duplicate updates
                    JDBC.assertFullResultSet(
                        s.executeQuery("select * from t"), initialContents);

                    // Specify ORDER BY and force use of index use the index
                    JDBC.assertFullResultSet(s.executeQuery(
                          "select * from t --DERBY-PROPERTIES constraint=c\n" +
                          "     order by i"),
                          initialContents);

                    checkConsistencyOfBaseTableAndIndex(s);

                    // Test add of a deferred constraint to an existing table
                    s.execute("alter table t drop constraint c");

                    // Insert duplicates: no constraint now
                    s.executeUpdate(
                        "insert into t values (2,19),(2,21),(3,31)");
                    commit();

                    // We can't add a constraint with immediate checking
                    // because of the existing duplicates.
                    assertStatementError(
                        LANG_DUPLICATE_KEY_CONSTRAINT,
                        s,
                        "alter table t add constraint c " + uniqueSpec[idx]);

                    // But we can add a deferred constraint:
                    s.executeUpdate(
                        "alter table t add constraint c " +
                        uniqueSpec[idx] + " deferrable initially deferred");

                    // Specify ORDER BY and force use of index use the index
                    JDBC.assertFullResultSet(
                        s.executeQuery(
                          "select * from t --DERBY-PROPERTIES constraint=c\n" +
                          "     order by i"),
                        new String[][] {
                            {"1", "10"},
                            {"2", "20"},
                            {"2", "19"},
                            {"2", "21"},
                            {"3", "30"},
                            {"3", "31"}});

                    // But since we still have duplicates, the commit will fail
                    assertCommitError(LANG_DEFERRED_DUP_VIOLATION_T,
                                      getConnection());

                    checkConsistencyOfBaseTableAndIndex(s);


                } finally {
                    idx++;
                    dropTable("t");
                    commit();
                }
            }


            //
            //   C H E C K   C O N S T R A I N T S
            //

            idx = 0;

            for (String ct : checkForms) {
                try {
                    s.executeUpdate(
                        ct + " deferrable initially immediate)");
                    s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                    commit();

                    //
                    //   I N S E R T   O F   V I O L A T I N G   R O W S
                    //

                    // Normal duplicate insert should fail, still
                    // immediate mode
                    assertStatementError(LANG_CHECK_CONSTRAINT_VIOLATED,
                                         s,
                                         "insert into t values (-2,30)");

                    // Now set deferred mode in one of two ways: by specifying
                    // ALL or by naming our index explicitly.
                    s.executeUpdate(sCF + " deferred");

                    // Rows violating CHECK constraint should now work
                    s.executeUpdate(
                        "insert into t values (-2,30),(1,31),(-3,32)");

                    // Check contents
                    JDBC.assertFullResultSet(
                        s.executeQuery("select * from t"),
                        new String[][] {
                            {"1", "10"},
                            {"2", "20"},
                            {"3", "30"},
                            {"-2", "30"},
                            {"1", "31"},
                            {"-3", "32"}});

                    // Try to set immediate mode, and detect violation
                    assertStatementError(LANG_DEFERRED_CHECK_VIOLATION_S,
                                         s,
                                         sCF + " immediate");
                    // Once more, error above should not roll back
                    assertStatementError(LANG_DEFERRED_CHECK_VIOLATION_S,
                                         s,
                                         sCF + " immediate");

                    // Now try to commit, which should lead to rollback
                    assertCommitError(LANG_DEFERRED_CHECK_VIOLATION_T,
                                      getConnection());

                    // Verify that contents are the same as before we did the
                    // duplicate inserts
                    JDBC.assertFullResultSet(
                        s.executeQuery("select * from t"), initialContents);

                    // Setting immediate now should work again:
                    s.executeUpdate(sCF + " immediate");
                    assertStatementError(LANG_CHECK_CONSTRAINT_VIOLATED,
                                         s,
                                         "insert into t values (-2,30)");

                    // setting deferred again:
                    s.executeUpdate(sCF + " deferred");

                    // Insert with check violations should now work
                    s.executeUpdate(
                        "insert into t values (-2,19),(2,21),(-3,31)");
                    assertStatementError(LANG_DEFERRED_CHECK_VIOLATION_S,
                                         s,
                                         sCF + " immediate");
                    rollback();

                    //
                    //  U P D A T E   G I V I N G   V I O L A T I N G   R O W S
                    //

                    // Now set deferred mode in one of two ways: by specifying
                    // ALL or by naming our constraint explicitly.
                    s.executeUpdate(sCF + " deferred");

                    // Now test the same, but using UPDATE instead of INSERT
                    s.executeUpdate(
                            "insert into t values (20,19),(200,21),(30,31)");
                    s.executeUpdate("update t set i=-2 where i=20");
                    s.executeUpdate("update t set i=-3 where i=200");
                    s.executeUpdate("update t set i=-4 where i=30");

                    // Check result
                    JDBC.assertFullResultSet(
                        s.executeQuery(
                          "select * from t order by j"),
                        new String[][] {
                            {"1", "10"},
                            {"-2", "19"},
                            {"2", "20"},
                            {"-3", "21"},
                            {"3", "30"},
                            {"-4", "31"}});

                    // Now try to commit, which should lead to rollback
                    assertCommitError(LANG_DEFERRED_CHECK_VIOLATION_T,
                                      getConnection());

                    // Verify that contents are the same as before we did the
                    // duplicate inserts
                    JDBC.assertFullResultSet(
                        s.executeQuery("select * from t"), initialContents);

                    JDBC.assertFullResultSet(s.executeQuery(
                          "select * from t order by i"),
                          initialContents);

                    checkConsistencyOfBaseTableAndIndex(s);

                    // Test add of a deferred constraint to an existing table
                    s.execute("alter table t drop constraint c");

                    // Insert "violating" rows: no constraint now
                    s.executeUpdate(
                        "insert into t values (-2,19),(2,21),(-3,31)");
                    commit();

                    // We can't add a constraint with immediate checking
                    // because of the existing violations..
                    assertStatementError(
                        LANG_ADD_CHECK_CONSTRAINT_FAILED,
                        s,
                        "alter table t add constraint c " + checkSpec[idx]);

                    // But we can add a deferred constraint:
                    s.executeUpdate(
                        "alter table t add constraint c " +
                        checkSpec[idx] + " deferrable initially deferred");

                    JDBC.assertFullResultSet(
                        s.executeQuery(
                          "select * from t order by i,j"),
                        new String[][] {
                            {"-3", "31"},
                            {"-2", "19"},
                            {"1", "10"},
                            {"2", "20"},
                            {"2", "21"},
                            {"3", "30"}});

                    // But since we still have violations, the commit will fail
                    assertCommitError(LANG_DEFERRED_CHECK_VIOLATION_T,
                                      getConnection());

                    checkConsistencyOfBaseTableAndIndex(s);


                } finally {
                    idx++;
                    dropTable("t");
                    commit();
                }
            }
        }
    }

    /**
     * Test that if the constraint mode is immediate and a routine has changed
     * this to introduce duplicates, we raise an error and roll back on exit
     * from the routine.
     * @throws SQLException
     */
    public void testRoutines() throws SQLException {
        final Statement s = createStatement();

        //
        //   P R I M A R Y   K E Y,   U N I Q U E   C O N S T R A I N T S
        //

        // Caller has not explicitly done any "SET CONSTRAINTS", but
        // constraint is initially immediate
        for (String ct : uniqueForms) {
            try {
                s.executeUpdate(
                        ct + " deferrable initially immediate)");
                s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                commit();

                declareCalledNested(s);
                assertStatementError(
                        LANG_DEFERRED_DUP_VIOLATION_T,
                        s,
                        "call calledNested(false)");
            } finally {
                dropTable("t");
            }
        }

        // Constraint is initially deferred, but mode then set to immediate
        // before the call
        for (String setConstraintForm : setConstraintsForms) {
            for (String ct : uniqueForms) {
                try {
                    s.executeUpdate(
                        ct + " deferrable initially deferred)");
                    s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                    commit();

                    s.executeUpdate(setConstraintForm + " immediate");
                    declareCalledNested(s);
                    assertStatementError(LANG_DEFERRED_DUP_VIOLATION_T,
                            s,
                            "call calledNested(false)");
                } finally {
                    dropTable("t");
                    commit();
                }
            }
        }

        // Check that we don't bark if we actually introduced the duplicates
        // in the caller session context
        for (String ct : uniqueForms) {
            try {
                s.executeUpdate(
                        ct + " deferrable initially deferred)");
                s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));

                declareCalledNested(s);
                s.executeUpdate("call calledNested(false)");
            } finally {
                rollback();
            }
        }


        //
        //    C H E C K   C O N S T R A I N T S
        //
        for (String ct : checkForms) {
            try {
                s.executeUpdate(
                        ct + " deferrable initially immediate)");
                s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                commit();

                declareCalledNested(s);
                assertStatementError(
                        LANG_DEFERRED_CHECK_VIOLATION_T,
                        s,
                        "call calledNested(true)");
            } finally {
                dropTable("t");
                commit();
            }
        }

        // Constraint is initially deferred, but mode then set to immediate
        // before the call
        for (String setConstraintForm : setConstraintsForms) {
            for (String ct : checkForms) {
                try {
                    s.executeUpdate(
                        ct + " deferrable initially deferred)");
                    s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                    commit();

                    s.executeUpdate(setConstraintForm + " immediate");
                    declareCalledNested(s);
                    assertStatementError(LANG_DEFERRED_CHECK_VIOLATION_T,
                            s,
                            "call calledNested(true)");
                } finally {
                    dropTable("t");
                    commit();
                }
            }
        }

        // Check that we don't bark if we actually introduced the violations
        // in the caller session context
        for (String ct : checkForms) {
            try {
                s.executeUpdate(ct + " deferrable initially deferred)");
                s.executeUpdate("insert into t values " +
                                rs2Values(negatedInitialContents));
                declareCalledNested(s);
                s.executeUpdate("call calledNested(true)");
            } finally {
                rollback();
            }
        }
        
        // Check what happens if routine set mode to immediate with
        // deferred rows inserted by caller
        for (String ct : checkForms) {
            try {
                s.executeUpdate(
                   ct + " deferrable initially deferred)");
                s.executeUpdate(
                   "insert into t values " + rs2Values(negatedInitialContents));
                declareCalledNestedSetImmediate(s);
                assertStatementError(LANG_DEFERRED_CHECK_VIOLATION_S,
                                     s, "call calledNestedSetImmediate()");
            } finally {
                rollback();
            }
        }

        //
        //    F O R E I G N   K E Y   C O N S T R A I N T S
        //
        for (String ct : fkForms) {
            try {
                s.executeUpdate(
                        ct + " deferrable initially immediate)");
                s.executeUpdate("insert into referenced values " +
                        rs2Values(initialContents));
                s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                commit();

                declareCalledNestedFk(s);
                assertStatementError(
                        LANG_DEFERRED_FK_VIOLATION_T,
                        s,
                        "call calledNestedFk()");
            } finally {
                dropTable("t");
                dontThrow(s, "delete from referenced");
                commit();
            }
        }

        // Constraint is initially deferred, but mode then set to immediate
        // before the call
        for (String setConstraintForm : setConstraintsForms) {
            for (String ct : fkForms) {
                try {
                    s.executeUpdate(
                        ct + " deferrable initially deferred)");
                    s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                    s.executeUpdate(
                        "insert into referenced(i) select i from t");
                    commit();

                    s.executeUpdate(setConstraintForm + " immediate");
                    declareCalledNestedFk(s);
                    assertStatementError(LANG_DEFERRED_FK_VIOLATION_T,
                            s,
                            "call calledNestedFk()");
                } finally {
                    dropTable("t");
                    dontThrow(s, "delete from referenced");
                    commit();
                }
            }
        }

        // Check that we don't bark if we actually introduced the violations
        // in the caller session context
        for (String ct : fkForms) {
            try {
                s.executeUpdate(
                        ct + " deferrable initially deferred)");
                s.executeUpdate(
                        "insert into t values " + rs2Values(initialContents));
                declareCalledNestedFk(s);
                s.executeUpdate("call calledNestedFk()");
                assertCommitError(LANG_DEFERRED_FK_VIOLATION_T,
                                  getConnection());
            } finally {
                rollback();
            }
        }

        // Check what happens if routine set mode to immediate with
        // deferred rows inserted by caller
        for (String ct : fkForms) {
            try {
                s.executeUpdate(
                   ct + " deferrable initially deferred)");
                s.executeUpdate(
                   "insert into t values " + rs2Values(initialContents));
                declareCalledNestedSetImmediate(s);
                assertStatementError(LANG_DEFERRED_FK_VIOLATION_S, s,
                                     "call calledNestedSetImmediate()");
            } finally {
                rollback();
            }
        }


    }

    public void testDeferredRowsInvalidation() throws SQLException {
        final Statement s = createStatement();
        //
        //   U N I Q U E,   P R I M A R Y   K E Y   C O N S T R A I N T
        //

        //   D r o p   t h e   c o n s t r a i n t
        s.executeUpdate("create table t(i int, " +
                        "  constraint c primary key (i) initially deferred)");
        s.executeUpdate("insert into t values 1,2,2,3");
        s.executeUpdate("alter table t drop constraint c");

        // Commit (below) normally forces checking of the deferred constraint
        // "c".  The dropping of the constraint should make sure we don't see
        // any issue with the recorded information lcc#deferredHashTables
        // i.e. {index conglomerate -> duplicate rows} .
        //
        // See LanguageConnectionContext#forgetDeferredConstraintsData.
        commit();
        s.executeUpdate("drop table t");
        commit();

        //   D r o p   t h e   t a b l e
        s.executeUpdate("create table t(i int, " +
                        "  constraint c primary key (i) initially deferred)");
        s.executeUpdate("insert into t values 1,2,2,3");
        assertStatementError(LANG_DEFERRED_DUP_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("drop table t");
        commit();

        //   T r u n c a t e   t h e   t a b l e
        s.executeUpdate("create table t(i int, " +
                        "  constraint c primary key (i) initially deferred)");
        s.executeUpdate("insert into t values 1,2,2,3");
        assertStatementError(LANG_DEFERRED_DUP_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("truncate table t");
        s.executeUpdate("set constraints c immediate");
        s.executeUpdate("set constraints c deferred");
        s.executeUpdate("insert into t values 1,2,2,3");
        assertStatementError(LANG_DEFERRED_DUP_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("drop table t");
        commit();

        //   C o m p r e s s   t h e   t a b l e
        s.executeUpdate("create table t(i int, " +
                "  constraint c primary key (i) initially deferred)");
        s.executeUpdate("insert into t values 1,2,2,3");
        assertStatementError(LANG_DEFERRED_DUP_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("delete from t where i=1");
        s.executeUpdate("call syscs_util.syscs_compress_table('APP', 'T', 0)");
        assertStatementError(LANG_DEFERRED_DUP_VIOLATION_S, s,
                             "set constraints c immediate");
        assertCommitError(LANG_DEFERRED_DUP_VIOLATION_T, getConnection());

        s.executeUpdate("create table t(i int, " +
                "  constraint c primary key (i) initially deferred)");
        commit();
        s.executeUpdate("insert into t values 1,2,3");
        s.executeUpdate("delete from t where i=1");

        // Inline compress times out if we add a PK (even without deferred
        // constraints)
        assertStatementError(LOCK_TIMEOUT, s,
                "call syscs_util.syscs_inplace_compress_table(" +
                "'APP', 'T', 1, 1, 1)");
        // assertStatementError(LANG_DEFERRED_DUP_VIOLATION_S, s,
        //                      "set constraints c immediate");
        // assertCommitError(LANG_DEFERRED_DUP_VIOLATION_T, getConnection());
        s.executeUpdate("drop table t");
        commit();

        //
        //   C H E C K   C O N S T R A I N T
        //

        //   D r o p   t h e   c o n s t r a i n t
        s.executeUpdate("create table t(i int, " +
                "  constraint c check (i > 0) initially deferred)");
        s.executeUpdate("insert into t values -1,-2, -2, -3");
        assertStatementError(LANG_DEFERRED_CHECK_VIOLATION_S, s,
                             "set constraints c immediate");

        s.executeUpdate("alter table t drop constraint c");
        commit();
        s.executeUpdate("drop table t");
        commit();

        //   D r o p   t h e   t a b l e
        s.executeUpdate("create table t(i int, " +
                "  constraint c check (i > 0) initially deferred)");
        s.executeUpdate("insert into t values -1, -2, -2, -3");
        assertStatementError(LANG_DEFERRED_CHECK_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("drop table t");
        commit();

        //   T r u n c a t e   t h e   t a b l e
        s.executeUpdate("create table t(i int, " +
                "  constraint c check (i > 0) initially deferred)");
        s.executeUpdate("insert into t values -1, -2, -2, -3");
        assertStatementError(LANG_DEFERRED_CHECK_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("truncate table t");
        commit();
        s.executeUpdate("drop table t");
        commit();

        //   C o m p r e s s   t h e   t a b l e
        //
        // We can no longer rely on row locations, so we do a full table scan
        // instead to detect any violations.
        s.executeUpdate("create table t(i int, " +
                "  constraint c check (i > 0) initially deferred)");

        s.executeUpdate("insert into t values -1, -2, -2, -3");
        s.executeUpdate("delete from t where i=-2");
        s.executeUpdate("call syscs_util.syscs_compress_table('APP', 'T', 0)");
        assertCommitError(LANG_DEFERRED_CHECK_VIOLATION_T, getConnection());

        s.executeUpdate("create table t(i int, " +
                "  constraint c check (i > 0) initially deferred)");
        commit();
        s.executeUpdate("insert into t values -1, -2, -2, -3");
        s.executeUpdate("delete from t where i=-2");
        s.executeUpdate("call syscs_util.syscs_inplace_compress_table(" +
                "'APP', 'T', 1, 1, 1)");
        assertCommitError(LANG_DEFERRED_CHECK_VIOLATION_T, getConnection());
        s.executeUpdate("drop table t");
        commit();


        //
        //   F O R E I G N   K E Y  C O N S T R A I N T
        //

        //   D r o p   t h e   c o n s t r a i n t
        s.executeUpdate("create table t(i int, constraint c foreign key(i) " +
                "references referenced(i) initially deferred)");
        s.executeUpdate("insert into t values 1,2,3");
        assertStatementError(LANG_DEFERRED_FK_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("alter table t drop constraint c");
        commit();
        s.executeUpdate("drop table t");
        commit();

        //   T r u n c a t e   t h e   r e f e r e n c i n g   t a b l e
        s.executeUpdate("create table t(i int, constraint c foreign key(i) " +
                "references referenced(i) initially deferred)");

        s.executeUpdate("insert into t values 1,2,3");
        assertStatementError(LANG_DEFERRED_FK_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("truncate table t");
        commit();
        s.executeUpdate("insert into t values 1,2,3");
        assertStatementError(LANG_DEFERRED_FK_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("drop table t");
        commit();

        //   T r u n c a t e   t h e   r e f e r e n c e d   t a b l e
        //
        //   This is only legal if all the referencing constraints are deferred
        //   and have ON DELETE NO ACTION.
        //
        s.executeUpdate(
            "create table t(i int, constraint c foreign key(i) " +
            "references referenced(i) on delete NO ACTION initially deferred)");
        s.executeUpdate("insert into referenced(i) values 4,5,6");
        s.executeUpdate("insert into t values 4,5,6");
        s.executeUpdate("set constraints c immediate");
        s.executeUpdate("set constraints c deferred");
        s.executeUpdate("delete from referenced where i=4");
        s.executeUpdate("truncate table referenced");
        assertStatementError(LANG_DEFERRED_FK_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("drop table t");
        s.executeUpdate("truncate table referenced");
        commit();

        //   C o m p r e s s   t h e   r e f e r e n c i n g   t a b l e

        // Compress by recreating the conglomerate
        s.executeUpdate("create table t(i int, constraint c foreign key(i) " +
                "references referenced(i) initially deferred)");
        s.executeUpdate("insert into referenced(i) values 4,5,6");
        s.executeUpdate("insert into t values 4,5,6,7");
        s.executeUpdate("delete from t where i=5");
        s.executeUpdate("call syscs_util.syscs_compress_table('APP', 'T', 0)");
        assertCommitError(LANG_DEFERRED_FK_VIOLATION_T, getConnection());

        // In-place compress
        s.executeUpdate("create table t(i int, constraint c foreign key(i) " +
                "references referenced(i) initially deferred)");
        s.executeUpdate("insert into referenced(i) values 4,5,6");
        s.executeUpdate("insert into t values 4,5,6,7");
        s.executeUpdate("delete from t where i=5");
        // s.executeUpdate("call syscs_util.syscs_inplace_compress_table(" +
        //                 "    'APP', 'T', 1,1,1)");
        assertStatementError(
            LOCK_TIMEOUT, s,
            "call syscs_util.syscs_inplace_compress_table('APP', 'T', 1,1,1)");
        // assertCommitError(LANG_DEFERRED_FK_VIOLATION_T, getConnection());


        //   C o m p r e s s   t h e   r e f e r e n c e d   t a b l e
        //
        // Compress by recreating the conglomerate
        s.executeUpdate(
            "create table t(i int, constraint c foreign key(i) " +
            "references referenced(i) ON DELETE NO ACTION initially deferred)");
        s.executeUpdate("insert into referenced(i) values 4,5,6");
        s.executeUpdate("insert into t values 4,5,6");
        s.executeUpdate("delete from referenced where i=5");
        assertStatementError(LANG_DEFERRED_FK_VIOLATION_S, s,
                             "set constraints c immediate");
        s.executeUpdate("call syscs_util.syscs_compress_table('APP', 'T', 0)");
        assertCommitError(LANG_DEFERRED_FK_VIOLATION_T, getConnection());

        // In-place compress
        s.executeUpdate(
            "create table t(i int, constraint c foreign key(i) " +
            "references referenced(i) ON DELETE NO ACTION initially deferred)");
        s.executeUpdate("insert into referenced(i) values 4,5,6");
        s.executeUpdate("insert into t values 4,5,6");
        s.executeUpdate("delete from referenced where i=5");
        assertStatementError(LANG_DEFERRED_FK_VIOLATION_S, s,
                             "set constraints c immediate");
        // s.executeUpdate("call syscs_util.syscs_inplace_compress_table(" +
        //                 "    'APP', 'T', 1,1,1)");
        assertStatementError(
           LOCK_TIMEOUT, s,
           "call syscs_util.syscs_inplace_compress_table('APP', 'T', 1, 1, 1)");
        // assertCommitError(LANG_DEFERRED_FK_VIOLATION_T, getConnection());
    }

    /**
     * Import uses other code paths than normal insert, so test it. Not very
     * useful with deferred constraints, however, since the IMPORT performs an
     * implicit commit at the end. However, the implementation goes through the
     * motions of deferring the checking, and the actual checking happens at
     * commit time. So, if the implicit commit is lifted in the future, the
     * deferred constraints should work. For now, the only net effect is to
     * delay the violation detection, so we should recommend immediate checking
     * in conjunction with import.
     *
     * @throws SQLException
     */
    public void testImport() throws SQLException {
        final Statement s = createStatement();

        s.executeUpdate("create table t(i int)");

        try {

            // Try the test cases below with both "replace" and "append"
            // semantics
            for (int addOrReplace = 0; addOrReplace < 2; addOrReplace++) {
                //
                //  P R I M A R Y   C O N S T R A I N T
                //
                s.executeUpdate("alter table t alter column i not null");
                s.executeUpdate(
                    "alter table t " +
                    "add constraint c primary key(i) " +
                    "    deferrable initially immediate");
                commit();

                s.executeUpdate("set constraints c deferred");

                // import and implicit commit leads to checking
                assertStatementError(
                    LANG_DEFERRED_DUP_VIOLATION_T, s,
                    "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                    "    'APP' , 'T' , '" + expImpDataFile + "'," +
                    "    null, null , null, " + addOrReplace + ")");

                //
                //   U N I Q U E   N O T   N U L L   C O N S T R A I N T
                //
                s.executeUpdate("alter table t alter column i not null");
                s.executeUpdate("alter table t drop constraint c");
                s.executeUpdate
                    ("alter table t " +
                     "add constraint c unique(i) " +
                     "    deferrable initially immediate");
                commit();

                s.executeUpdate("set constraints c deferred");

                // import and implicit commit leads to checking
                assertStatementError(
                    LANG_DEFERRED_DUP_VIOLATION_T, s,
                    "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                    "    'APP' , 'T' , '" + expImpDataFile + "'," +
                    "    null, null , null, " + addOrReplace + ")");

                //
                //   n u l l a b l e   U N I Q U E   C O N S T R A I N T
                //
                s.executeUpdate("alter table t alter column i null");
                s.executeUpdate("alter table t drop constraint c");
                s.executeUpdate(
                    "alter table t " +
                    "add constraint c unique(i) initially deferred");
                commit();

                // import and implicit commit leads to checking
                assertStatementError(
                    LANG_DEFERRED_DUP_VIOLATION_T, s,
                    "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                    "    'APP' , 'T' , '" + expImpDataFile + "'," +
                    "    null, null , null, " + addOrReplace + ")");

                // Import OK data with multiple NULLs should still work with
                // nullable UNIQUE deferred constraint
                s.executeUpdate(
                        "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                        "    'APP' , 'T' , '" +
                        expImpDataWithNullsFile + "'," +
                        "    null, null , null, " + addOrReplace + ")");
                s.executeUpdate("alter table t drop constraint c");
                s.executeUpdate("truncate table t");
                commit();

                //
                //   C H E C K   C O N S T R A I N T
                //
                s.executeUpdate(
                    "alter table t " +
                    "add constraint c check (i > 0) initially deferred");

                // import and implicit commit leads to checking
                assertStatementError(
                    LANG_DEFERRED_CHECK_VIOLATION_T, s,
                    "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                    "    'APP' , 'T' , '" + expImpDataFile + "'," +
                    "    null, null , null, " + addOrReplace + ")");

                s.executeUpdate("truncate table t");
                commit();
            }
        } finally {
            dropTable("t");
            commit();
        }
    }

    // Adapted from UniqueConstraintSetNullTest which exposed an error
    // when we ran all regressions with default deferrable: when a NOT NULL
    // clause was dropped, the test used to drop and recreate the index to
    // be non-unique was incomplete in the deferrable case.
    public void testDropNotNullOnUniqueColumn() throws SQLException {
        final Statement s = createStatement();

        s.executeUpdate("create table constraintest (" +
                "val1 varchar (20) not null, " +
                "val2 varchar (20))");

        s.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1) deferrable initially immediate");

        s.executeUpdate("alter table constraintest alter column val1 null");

        s.executeUpdate("insert into constraintest(val1) values 'name1'");

        assertStatementError(
            LANG_DUPLICATE_KEY_CONSTRAINT, s,
            "insert into constraintest(val1) values 'name1'");

        final PreparedStatement ps = prepareStatement(
                "insert into constraintest(val1) values (?)");
        ps.setString(1, null);
        ps.executeUpdate();
        ps.setString(1, null);
        ps.executeUpdate();
    }


    public void testDerby6374() throws SQLException {
        final Statement s = createStatement();

        s.executeUpdate("create table t(i int)");

        try {
            // Try the test cases below with both "replace" and not with
            // the import statement:
            for (int addOrReplace = 0; addOrReplace < 2; addOrReplace++) {

                // Import duplicate data into a table a nullable
                // UNIQUE constraint
                s.executeUpdate("alter table t add constraint c unique(i)");
                commit();

                assertStatementError(
                    LANG_DUPLICATE_KEY_CONSTRAINT, s,
                    "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                    "    'APP' , 'T' , '" + expImpDataFile + "'," +
                    "    null, null , null, " + addOrReplace + ")");

                s.executeUpdate("alter table t drop constraint c");
            }
        } finally {
            dropTable("t");
            commit();
        }
    }

    public void testXA() throws SQLException, XAException {
        final XADataSource xads = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(xads, "databaseName", "wombat");

        final int UNIQUE_PK = 0; // loop iteration 0
        final int CHECK = 1;     // loop iteration 1

        final String[] expectedError = {
            LANG_DEFERRED_DUP_VIOLATION_T,
            LANG_DEFERRED_CHECK_VIOLATION_T};

        for (int i = UNIQUE_PK; i <= CHECK; i++) {
            final XAConnection xaconn = xads.getXAConnection();

            try {
                final XAResource xar = xaconn.getXAResource();
                final Connection conn = xaconn.getConnection();
                conn.setAutoCommit(false);

                final Statement s = conn.createStatement();

                //
                // Do XA rollback when we have a violation; expect normal
                // operation.
                //
                Xid xid = (i == UNIQUE_PK) ?
                        doXAWorkUniquePK(s, xar) :
                        doXAWorkCheck(s, xar);

                xar.rollback(xid);
                assertXidRolledBack(xar, xid);

                //
                // Do an XA prepare when we have a violation; expect exception
                // and rollback.
                //
                xid = (i == UNIQUE_PK) ?
                        doXAWorkUniquePK(s, xar) :
                        doXAWorkCheck(s, xar);

                try {
                    xar.prepare(xid);
                    fail("Expected XA prepare to fail due to " +
                         "constraint violation");
                } catch (XAException xe) {
                    assertEquals(XAException.XA_RBINTEGRITY, xe.errorCode);

                    if (!usingDerbyNetClient()) {
                        Throwable t = xe.getCause();
                        assertTrue(t != null && t instanceof SQLException);
                        assertSQLState(expectedError[i], (SQLException)t);
                    }

                    assertXidRolledBack(xar, xid);
                }

                //
                // Do XA commit (1PC, no prepare) when we have a violation;
                // expect exception and rollback.
                //
                xid = (i == UNIQUE_PK) ?
                        doXAWorkUniquePK(s, xar) :
                        doXAWorkCheck(s, xar);

                try {
                    xar.commit(xid, true);
                    fail("Expected XA commit to fail due to " +
                         "constraint violation");
                } catch (XAException xe) {
                    assertEquals(XAException.XA_RBINTEGRITY, xe.errorCode);

                    if (!usingDerbyNetClient()) {
                        Throwable t = xe.getCause();
                        assertTrue(t != null && t instanceof SQLException);
                        assertSQLState(expectedError[i], (SQLException)t);
                    }

                    assertXidRolledBack(xar, xid);
                }

            } finally {
                if (usingDerbyNetClient()) {
                    xaconn.getConnection().rollback();
                }
                xaconn.close();
            }
        }
    }

    // Exposed a bug when running regression suites with default
    // deferrable: compress recreates the index.
    public void testCompressTableOKUnique() throws SQLException {
        final Statement stmt = createStatement();
        stmt.executeUpdate(
                "create table table1(" +
                "name1 int unique deferrable initially immediate, " +
                "name2 int unique not null, " +
                "name3 int primary key)");
        try {
            stmt.execute(
                "call syscs_util.syscs_compress_table('APP','TABLE1',1)");
            stmt.executeUpdate(
                "insert into table1 values(1,11,111)");

            // The following should run into problem because of constraint
            // on name1
            assertStatementError(
                LANG_DUPLICATE_KEY_CONSTRAINT, stmt,
                "insert into table1 values(1,22,222)");

            // The following should run into problem because of constraint
            // on name2
            assertStatementError(
                LANG_DUPLICATE_KEY_CONSTRAINT, stmt,
                "insert into table1 values(3,11,333)");

            // The following should run into problem because of constraint
            // on name3
            assertStatementError(
                LANG_DUPLICATE_KEY_CONSTRAINT, stmt,
                "insert into table1 values(4,44,111)");

        } finally {
            stmt.executeUpdate("drop table table1");
        }
    }


    final static long NO_OF_INSERTED_ROWS = (1024L * 1024L * 10) / 256;
    public void testManySimilarDuplicates() throws SQLException {
        if (usingDerbyNetClient()) {
            // skip, too heavy fixture to do twice... we use
            // in memory db in any case...
            return;
        }

        final Connection c = dbm.createDatabase("cct");
        c.setAutoCommit(false);

        final Statement s = c.createStatement();
        try {
            s.executeUpdate(
                "create table t (i varchar(256), " +
                "    constraint c primary key(i) initially deferred)");

            final PreparedStatement ps =
                c.prepareStatement("insert into t values ?");

            char[] value = new char[256];
            Arrays.fill(value, 'a');
            ps.setString(1, String.valueOf(value));

            for (long l=0; l < NO_OF_INSERTED_ROWS; l++) {
                ps.executeUpdate();
            }
            c.commit();
            fail();
        } catch (SQLException e) {
            assertSQLState(LANG_DEFERRED_DUP_VIOLATION_T, e);
            s.executeUpdate("call syscs_util.syscs_checkpoint_database()");
        }
    }

    /**
     * Remove all duplicates except the last
     * @throws java.sql.SQLException
     */
    public void testAlmostRemovedAllDups() throws SQLException {
        final Statement s = createStatement();
        s.executeUpdate(
            "create table t(i int, j int, " +
            "    constraint c primary key (i) initially deferred)");

        try {
            final PreparedStatement ps = prepareStatement(
                "insert into t values (?,?)");

            for (int i=0; i < 10; i++) {
                ps.setInt(1, 1);
                ps.setInt(2, i);
                ps.executeUpdate();
            }

            // leave one row
            s.executeUpdate("delete from t where j > 0");
            commit(); // should work

            s.executeUpdate("truncate table t");

            // make many different duplicates and delete all except the last
            // two rows, i.e. one duplicate left.
            for (int i=0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setInt(2, i);
                ps.executeUpdate();

                ps.setInt(1, i);
                ps.setInt(2, i);
                ps.executeUpdate();
            }

            s.execute("delete from t where i < 9");
            JDBC.assertFullResultSet(s.executeQuery("select * from t"),
                new String[][]{
                    {"9","9"},
                    {"9","9"}});
            commit();
        } catch (SQLException e) {
            assertSQLState(LANG_DEFERRED_DUP_VIOLATION_T, e);
        } finally {
            dropTable("t");
            commit();
        }
    }

    private static void setupTab1(final String db) throws SQLException {
        final Connection c = dbm.getConnection(db);
        final Statement stmt = c.createStatement();
        stmt.execute(
                "create table tab1 (i integer)");
        stmt.executeUpdate(
                "alter table tab1 add constraint con1 unique (i) deferrable");
        final PreparedStatement ps = c.prepareStatement("insert into tab1 " +
                "values (?)");

        for (int i = 0; i < 10; i++) {
            ps.setInt(1, i);
            ps.executeUpdate();
        }

        ps.close();
        stmt.close();
        c.commit();
    }


    private static void dropTab1(final String db) throws SQLException {
        final Connection c = dbm.getConnection(db);
        final Statement stmt = c.createStatement();

        try {
            stmt.execute("drop table tab1");
            c.commit();
        } catch (SQLException e) {
            // ignore so we get to see original exception if there is one
        }
    }

    /**
     * Test inserting a duplicate record while original is deleted in a
     * transaction and later committed.
     * <p/>
     * This test was lifted from UniqueConstraintMultiThrededTest
     * except that here we run it with a deferrable constraint. We
     * include it her e since it exposed a bug during implementation
     * of deferrable constraints: we check a deferrable constraint
     * <em>after</em> the insert (cf. {@code IndexChanger}) by using a
     * BTree scan.  Iff the constraint mode is deferred, we treat any
     * lock or deadlock timeout as if it were a duplicate, allowing us
     * to defer the check till commit time, as so possibly gain more
     * concurrency. To get speed in this case, the scan returns
     * immediately if it can't get a lock.  The error was that, if the
     * constraint mode is <em>not</em> deferred (i.e. immediate), we
     * should wait for the lock, and we didn't. This was exposed by
     * this test since the 2 seconds wait makes it work in the normal
     * case (the lock would be released), but in the no-wait scan, we
     * saw a the lock time-out error.
     * @throws java.lang.Exception
     */
    public void testLockingForUniquePKWithCommit () throws Exception {
        final String db = "cct";
        dbm.createDatabase(db).close();
        setupTab1(db);

        try {
            for (int i = 0; i < 4; i++) {
                for (int j = 0; j < 4; j++) {
                    executeThreads(
                        db,
                        (int)Math.pow(2,i),
                        (int)Math.pow(2,j), true);
                }
            }
        } finally {
            dropTab1(db);
        }
    }

    /**
     * Test inserting a duplicate record while original is deleted in
     * a transaction and later rolled back.
     * <p/>
     * See also comment for {@link #testLockingForUniquePKWithCommit() }.
     *
     * @throws java.lang.Exception
     */
    public void testLockingForUniquePKWithRollback () throws Exception {
        final String db = "cct";
        dbm.createDatabase(db).close();
        setupTab1(db);

        try {
            for (int i = 0; i < 4; i++) {
                for (int j = 0; j < 4; j++) {
                    executeThreads(
                        db,
                        (int)Math.pow(2,i),
                        (int)Math.pow(2,j), false);
                }
            }
        } finally {
            dropTab1(db);
        }
    }


    /**
     * A bit of white box testing to cover different code paths. SOmetimes, on
     * INSERT and UPDATE, the actual writing of the rows is deferred, e.g.
     * due to a "self" select, or due to the presence of triggers.
     *
     * @throws SQLException
     */
    public void testCheckConstraintsWithDeferredRows () throws
            SQLException {

        final Statement s = createStatement();

        try {
            s.executeUpdate(
                "create table tab1 (c1 int, " +
                "constraint c check (c1 > 0) deferrable initially deferred)");
            commit();

            //
            // I N S E R T,   D E F E R R E D   P R O C E S S I N G
            //

            // INSERT from self causes the violation
            s.executeUpdate("insert into tab1 values (4)");
            s.executeUpdate("insert into tab1 values (3)");
            s.executeUpdate("insert into tab1 select c1-3 from tab1");
            assertCommitError(LANG_DEFERRED_CHECK_VIOLATION_T,
                              getConnection());

            //
            // U P D A T E,   D E F E R R E D   P R O C E S S I N G
            //

            // correlated query to force deferred update processing:
            s.executeUpdate("insert into tab1 values (2)");
            s.executeUpdate("update tab1 as grr set c1=-1 where c1 = 2 and " +
                    "((select max(c1) from tab1 where grr.c1 > 0) > 0)");

            assertCommitError(LANG_DEFERRED_CHECK_VIOLATION_T,
                              getConnection());

            // Correlated query to force deferred update processing but with
            // trigger which causes another code path.
            s.executeUpdate("insert into tab1 values (2)");
            s.executeUpdate("create table trigtab(i int)");
            s.executeUpdate("create trigger mytrigger " +
                    "after update on tab1 insert into trigtab values 1");
            s.executeUpdate("update tab1 as grr set c1=-1 where c1 = 2 and " +
                    "((select max(c1) from tab1 where grr.c1 > 0) > 0)");

            assertCommitError(LANG_DEFERRED_CHECK_VIOLATION_T,
                              getConnection());
        } finally {
            // clean up
            dropTable("tab1");
            dropTable("trigtab");
            commit();
        }
    }

    final static String DID = "deferrable initially deferred";
    /**
     * We can have several constraints broken on one row write,
     * test is we register all correctly
     *
     * @throws SQLException
     */
    public void testSeveralCheckConstraints () throws SQLException {
        final Statement s = createStatement();

        try {
            s.executeUpdate(
                "create table t(" +
                "i int, constraint ci check (i > 0) " + DID + ", " +
                "j int, constraint cj check (j > 0) " + DID + ", " +
                "k int, constraint ck check (k > 0) " + DID + ")");
            commit();

            final String[] setStrings = {
                "j = -j, k = -k",
                "i = -i, k = -k",
                "i = -i, j = -j"};

            final String[] makeImmediate = {"ci", "cj", "ck"};

            // All constraints are broken, make all except one good and
            // check that the one still barfs when made immediate.
            for (int i = 0; i < 3; i++) {
                s.executeUpdate("insert into t values (-1, -2, -3)");
                s.executeUpdate("update t set " + setStrings[i]);

                try {
                    // We force checking of only the column which is still
                    // broken
                    s.executeUpdate("set constraints " + makeImmediate[i] +
                                    " immediate");
                    fail("expected violation: " + i);
                } catch (SQLException e) {
                    assertSQLState(LANG_DEFERRED_CHECK_VIOLATION_S, e);
                    assertTrue(e.getMessage().contains(
                                   makeImmediate[i].toUpperCase()));
                }
                rollback();
            }

            // Check that we accumulate the set of broken constraints
            // when many are violated on separate occasions
            s.executeUpdate("insert into t values (-1,  2,  3)");
            s.executeUpdate("insert into t values ( 1, -2,  3)");
            s.executeUpdate("insert into t values ( 1,  2, -3)");

            for (int i = 0; i < 3; i++) {
                try {
                    s.executeUpdate("set constraints " + makeImmediate[i] +
                                    " immediate");
                    fail("expected violation: " + i);
                } catch (SQLException e) {
                    assertSQLState(LANG_DEFERRED_CHECK_VIOLATION_S, e);
                    assertTrue(e.getMessage().contains(
                                   makeImmediate[i].toUpperCase()));
                }
            }

            rollback();

            // Violations on the same the same row many times
            s.executeUpdate("insert into t values (-1,  2,  3)");
            s.executeUpdate("update t set i=-1");
            s.executeUpdate("update t set i=-1");
            assertCommitError(LANG_DEFERRED_CHECK_VIOLATION_T,
                              getConnection());

        } finally {
            dropTable("t");
            commit();
        }
    }

    /**
     * Deletes a record in a transaction and tries to insert the same
     * from a different transaction. Once second transaction goes on wait
     * first transaction is committed or rolled back based on third
     * parameter (boolean commit).
     *
     * @param db string of in-memory db to use
     * @param isolation1 isolation level for 1st thread
     * @param isolation2 isolation level for 2nd thread
     * @param commit whether or not to commit
     *
     * (Lifted from UniqueConstraintMultiThrededTest to test with deferrable
     * constraint.)
     *
     * @throws java.lang.Exception
     */
    private static void executeThreads (
        final String db,
        final int isolation1,
        final int isolation2,
        final boolean commit) throws Exception {

        final Connection con1 = dbm.getConnection(db);
        con1.setTransactionIsolation(isolation1);
        final Connection con2 = dbm.getConnection(db);

        try {
            con2.setTransactionIsolation(isolation2);
            final DBOperations dbo1 = new DBOperations (con1, 5);
            final DBOperations dbo2 = new DBOperations (con2, 5);
            dbo1.delete();
            final Thread t = new Thread (dbo2);
            t.start();

            Thread.sleep((WAIT_TIMEOUT_DURATION * 1000) / 2 );

            if (commit) {
                dbo1.rollback();
                t.join();
                assertSQLState(
                    "isolation levels: " + isolation1 + " " + isolation2,
                    LANG_DUPLICATE_KEY_CONSTRAINT,
                    dbo2.getException());
            } else {
                dbo1.commit();
                t.join();
                assertNull("isolation levels: " + isolation1
                            + " " + isolation2 + ": exception " + 
                        dbo2.getException(), dbo2.getException());
            }
            assertNull("unexpected failure: " + isolation1
                        + " " + isolation2 + ": exception " + 
                        dbo2.getUnexpectedException(), 
                    dbo2.getUnexpectedException());
        }
        finally {
            con1.commit();
            con2.commit();
            con1.close();
            con2.close();
        }

    }


    private Xid doXAWorkUniquePK(final Statement s, final XAResource xar)
            throws SQLException, XAException {
        final Xid xid = XATestUtil.getXid(1,05,32);
        // Start work on a transaction branch
        xar.start(xid, XAResource.TMNOFLAGS);

        // Create the table and insert some records which violate a deferred
        // constraint into it.
        s.executeUpdate(
            "create table derby532xa(i int, " +
            "    constraint derby532xa_c primary key(i) initially deferred)");
        s.executeUpdate("insert into derby532xa values 1,1,2");

        // End work on a transaction branch
        xar.end(xid, XAResource.TMSUCCESS);
        return xid;
    }

    private Xid doXAWorkCheck(final Statement s, final XAResource xar)
            throws SQLException, XAException {
        final Xid xid = XATestUtil.getXid(1,05,32);
        // Start work on a transaction branch
        xar.start(xid, XAResource.TMNOFLAGS);

        // Create the table and insert some records which violate a deferred
        // constraint into it.
        s.executeUpdate(
            "create table derby532xa(i int, " +
            "    constraint derby532xa_c check(i > 0) initially deferred)");
        s.executeUpdate("insert into derby532xa values -1, 1,-2");

        // End work on a transaction branch
        xar.end(xid, XAResource.TMSUCCESS);
        return xid;
    }

    private void assertXidRolledBack(final XAResource xar, final Xid xid) {
        try {
            xar.rollback(xid);
            fail("expected the transaction to be unknown");
        } catch (XAException xe) {
            assertEquals(xe.errorCode, XAException.XAER_NOTA);
        }
    }

    /**
     * Format rows to single string in syntax suitable for VALUES statement:
     * "{@code (v1,v2,..,vn), (v1,v2,..,vn),....}"
     *
     * @param rs result set strings
     * @return  the formatted string
     */
    private static String rs2Values(final String[][] rs) {
        final StringBuilder sb = new StringBuilder();

        for (String[] row : rs) {
            sb.append('(');
            for (String v : row) {
                sb.append(v);
                sb.append(',');
            }
            sb.deleteCharAt(sb.length() - 1); // trailing comma
            sb.append("),");
        }
        sb.deleteCharAt(sb.length() - 1); // trailing comma
        return sb.toString();
    }

    private static void checkConsistencyOfBaseTableAndIndex(Statement s)
            throws SQLException {
        JDBC.assertFullResultSet(
            s.executeQuery("values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T')"),
            new String[][] {{"1"}});
    }

    private final static String[] tableConstraintTypes = {
            " foreign key (i) references referenced(i)",
            " primary key(i)",
            " unique(i)",
            " check(i<3)"
        };

    private final static String[] columnConstraintTypes = {
            " references referenced(i)",
            " primary key",
            " unique",
            " check(i<3)"
        };

    // Each of the three characteristics can have 3 values
    // corresponding to {default, on, off}. This translates into 3 x 3
    // x 3 = 27 syntax permutations, classified below with their
    // corresponding dictionary state.
    //
    private final static String[][] defaultCharacteristics = {
        {" not deferrable initially immediate enforced", "E"},
        {" not deferrable initially immediate", "E"},
        {" not deferrable enforced", "E"},
        {" not deferrable", "E"},
        {" initially immediate enforced", "E"},
        {" initially immediate", "E"},
        {" enforced", "E"},
        {"", "E"}};

    private final static String[][] nonDefaultCharacteristics = {
        {" deferrable", "i"},
        {" deferrable initially immediate", "i"},
        {" deferrable enforced", "i"},
        {" deferrable initially immediate enforced", "i"},
        {" deferrable initially deferred", "e"},
        {" deferrable initially deferred enforced", "e"},
        {" initially deferred enforced", "e"},
        {" initially deferred", "e"},
        {" deferrable not enforced", "j"},
        {" deferrable initially immediate not enforced", "j"},
        {" deferrable initially deferred not enforced", "d"},
        {" initially deferred not enforced", "d"},
        {" not enforced", "D"},
        {" initially immediate not enforced", "D"},
        {" not deferrable not enforced", "D"},
        {" not deferrable initially immediate not enforced", "D"}
    };

    private final static String[] illegalCharacteristics = {
        " not deferrable initially deferred",
        " not deferrable initially deferred enforced",
        " not deferrable initially deferred not enforced"
    };

    private final static String[] illegalAlterCharacteristics;

    static {
        final List<String> characteristics = new ArrayList<String>();
        characteristics.addAll(Arrays.asList(defaultCharacteristics[0]));
        characteristics.addAll(Arrays.asList(nonDefaultCharacteristics[0]));
        characteristics.addAll(Arrays.asList(illegalCharacteristics));
        characteristics.remove(" not enforced");
        characteristics.remove(" enforced");
        characteristics.remove("");
        illegalAlterCharacteristics = characteristics.toArray(new String[0]);
    }

    private final static Map<String, String[]> inverseState =
            new HashMap<String, String[]>();

    static {
        inverseState.put("E", new String[]{"E", "D"});
        inverseState.put("D", new String[]{"E", "D"});
        inverseState.put("i", new String[]{"i", "j"});
        inverseState.put("j", new String[]{"i", "j"});
        inverseState.put("i", new String[]{"i", "j"});
        inverseState.put("e", new String[]{"e", "d"});
        inverseState.put("d", new String[]{"e", "d"});
    }

    /**
     * Assert that we fail with feature not implemented
     * until feature is implemented (for characteristics that are not Derby
     * default).
     *
     * @param s statement

     * @throws SQLException
     */
    private static void assertTableLevelNonDefaultAccepted(
            final Statement s) throws SQLException {

        for (String ct : tableConstraintTypes) {
            for (String[] ch : nonDefaultCharacteristics) {
                // Only primary key and unique implemented
                if (ch[0].contains("not enforced")) {

                    assertStatementError(NOT_IMPLEMENTED,
                            s,
                            "create table t(i int, constraint c " +
                                    ct + ch[0] + ")");
                } else {
                    s.executeUpdate("create table t(i int, constraint c " +
                                    ct + ch[0] + ")");
                    s.executeUpdate("drop table t");
                }
            }
        }
    }

    /**
     * Assert that we allow non defaults
     *
     * @param s statement

     * @throws SQLException
     */
    private static void assertColumnLevelNonDefaultAccepted(
            final Statement s) throws SQLException {

        for (String ct : columnConstraintTypes) {
            for (String[] ch : nonDefaultCharacteristics) {
                // Only primary key and unique implemented
                if (ch[0].contains("not enforced")) {

                    assertStatementError(NOT_IMPLEMENTED,
                            s,
                            "create table t(i int " +
                                    ct + ch[0] + ")");
                } else {
                    s.executeUpdate("create table t(i int " +
                            ct + ch[0] + ")");
                    s.executeUpdate("drop table t");
                }
            }
        }
    }

    /**
     * Assert that we accept characteristics that merely specify the default
     * behavior anyway.
     *
     * @param c connection
     * @param s statement
     *
     * @throws SQLException
     */
    private static void assertTableLevelDefaultBehaviorAccepted (
            final Connection c,
            final Statement s) throws SQLException {

        for (String ct : tableConstraintTypes) {
            for (String[] ch : defaultCharacteristics) {
                assertUpdateCount(s, 0,
                    "create table t(i int, constraint c " + ct + ch[0] + ")");
                c.rollback();
            }
        }
    }

    /**
     * Assert that we accept characteristics that merely specify the default
     * behavior anyway.
     *
     * @param c connection
     * @param s statement
     *
     * @throws SQLException
     */
    private static void assertColumnLevelDefaultBehaviorAccepted (
            final Connection c,
            final Statement s) throws SQLException {

        for (String ct : columnConstraintTypes) {
            for (String ch[] : defaultCharacteristics) {
                assertUpdateCount(s, 0,
                    "create table t(i int " + ct + ch[0] + ")");
                c.rollback();
            }
        }
    }


    /**
     * Check that the dictionary state resulting from {@code characteristics}
     * equals {@code}.
     *
     * @param s                Statement to use
     * @param characteristics  A table level constraint characteristics string
     * @param code             Character encoding for characteristics
     *
     * @throws SQLException
     */
    private void assertDictState(
            final Statement s,
            final String characteristics,
            final String code) throws SQLException {

        for (String ct: tableConstraintTypes) {
            try {
                s.executeUpdate(
                    "create table t(i int, constraint c " + ct + " " +
                    characteristics + ")");

                if (characteristics.contains("not enforced")) {
                    fail();
                } else {
                    JDBC.assertFullResultSet(
                        s.executeQuery(
                            "select state from sys.sysconstraints " +
                            "    where constraintname = 'C'"),
                        new String[][]{{code}});
                    rollback();
                }
            } catch (SQLException e) {
                if (characteristics.contains("not enforced")) {
                    assertSQLState(NOT_IMPLEMENTED, e);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Check that the altered dictionary state resulting from new
     * {@code characteristics} equals {@code}.
     *
     * @param s                The statement object to use
     * @param enforcement  String containing ENFORCED or NOT ENFORCED
     *
     * @throws SQLException
     */
    private void assertAlterDictState(
            final Statement s,
            final String enforcement) throws SQLException {

        final String oldState = getOldState(s);
        final String newState = computeNewState(oldState, enforcement);

        if (!enforcement.contains("not enforced")) {
            s.executeUpdate("alter table t alter constraint c " +
                    enforcement);
            JDBC.assertFullResultSet(
                    s.executeQuery("select state from sys.sysconstraints " +
                            "    where constraintname = 'C'"),
                    new String[][]{{newState}});
        } else {
            assertStatementError(NOT_IMPLEMENTED, s,
                    "alter table t alter constraint c " + enforcement);
        }
    }

    private String getOldState(final Statement s) throws SQLException {
        final ResultSet rs = s.executeQuery(
                "select state from sys.sysconstraints " +
                "    where constraintname = 'C'");
        try {
            rs.next();
            return rs.getString(1);
        } finally {
            rs.close();
        }
    }


    private String computeNewState(String oldState, String enforcement) {
        return inverseState.get(oldState)[
            enforcement.equals("enforced") ? 0 : 1];
    }

    private void assertCreateInconsistentCharacteristics(
            final Statement s,
            final String characteristics) throws SQLException {

        for (String ct: tableConstraintTypes) {
            try {
                s.executeUpdate(
                    "create table t(i int, constraint c " + ct + " " +
                    characteristics + ")");
                fail("wrong characteristics unexpectedly passed muster");
                rollback();
            } catch (SQLException e) {
                assertSQLState(LANG_INCONSISTENT_C_CHARACTERISTICS, e);
            }
        }
    }

    private void assertAlterInconsistentCharacteristics(
            final Statement s,
            final String characteristics) throws SQLException {

        try {
            s.executeUpdate("alter table t alter constraint c " +
                    characteristics);
            fail("wrong characteristics unexpectedly passed muster");
            rollback();
        } catch (SQLException e) {
            assertSQLState(LANG_SYNTAX_ERROR, e);
        }
    }

    private void declareCalledNested(final Statement s) throws SQLException {
        s.executeUpdate(
            "create procedure calledNested(isCheckConstraint boolean)" +
            "  language java parameter style java" +
            "  external name '" +
            this.getClass().getName() +
            ".calledNested' modifies sql data");
    }

    private void declareCalledNestedFk(final Statement s) throws SQLException {
        s.executeUpdate(
            "create procedure calledNestedFk()" +
            "  language java parameter style java" +
            "  external name '" +
            this.getClass().getName() +
            ".calledNestedFk' modifies sql data");
    }
    private void declareCalledNestedSetImmediate(final Statement s) 
            throws SQLException {
        s.executeUpdate(
            "create procedure calledNestedSetImmediate()" +
            "  language java parameter style java" +
            "  external name '" +
            this.getClass().getName() +
            ".calledNestedSetImmediate' modifies sql data");
    }

    public static void calledNested(final boolean isCheckConstraint)
            throws SQLException
    {
        final Connection c =
            DriverManager.getConnection("jdbc:default:connection");
        final Statement cStmt = c.createStatement();

        cStmt.executeUpdate("set constraints c deferred");
        cStmt.executeUpdate("insert into t values " +
                rs2Values(isCheckConstraint ?
                        negatedInitialContents :
                        initialContents));
        c.close();
    }

    public static void calledNestedFk() throws SQLException
    {
        final Connection c =
            DriverManager.getConnection("jdbc:default:connection");
        final Statement cStmt = c.createStatement();

        cStmt.executeUpdate("set constraints c deferred");
        cStmt.executeUpdate("insert into t select i*2, j*2 from t");
        c.close();
    }

    public static void calledNestedSetImmediate() throws SQLException
    {
        final Connection c =
            DriverManager.getConnection("jdbc:default:connection");
        final Statement cStmt = c.createStatement();

        try {
            cStmt.executeUpdate("set constraints c immediate");
        } finally { 
            c.close();
        }
    }

    private void dontThrow(Statement st, String stm) {
        try {
            st.executeUpdate(stm);
        } catch (SQLException e) {
            // ignore, best effort here
            println("\"" + stm+ "\"failed: " + e);
        }
    }
}

