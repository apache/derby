/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ForeignKeysDeferrableTest

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

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import static org.apache.derbyTesting.junit.BaseJDBCTestCase.usingDerbyNetClient;
import static org.apache.derbyTesting.junit.BaseJDBCTestCase.usingEmbedded;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import static org.apache.derbyTesting.junit.TestConfiguration.clientServerSuite;
import static org.apache.derbyTesting.junit.TestConfiguration.embeddedSuite;

/**
 * Test deferrable foreign key constraints.
 *
 * NOTE: In contrast to constraint checking, the <em>referential actions</em>
 * specified by a referential constraint are never deferred.
 * (SQL 2011: section 4.18.2).
 * <p/>
 * Section 4.18.3.3:
 * An {@code <update rule>} that does not contain NO ACTION specifies a
 * referential update action. A {@code <delete rule>} that does not specify NO
 * ACTION specifies a referential delete action. Referential update
 * actions and referential delete actions are collectively called
 * referential actions. Referential actions are carried out before, and
 * are not part of, the checking of a referential constraint. Deferring a
 * referential constraint defers the checking of the {@code <search condition>}
 * of the constraint (a {@code <match predicate>}) but does not defer the
 * referential actions of the referential constraint.
 * <p/>
 * NOTE 52 - For example, if a referential update action such as ON UPDATE
 * CASCADE is specified, then any UPDATE operation on the referenced table will
 * be cascaded to the referencing table as part of the UPDATE operation, even
 * if the referential constraint is deferred. Consequently, the referential
 * constraint cannot become violated by the UPDATE statement. On the other
 * hand, ON UPDATE SET DEFAULT could result in a violation of the referential
 * constraint if there is no matching row after the referencing column is set
 * to its default value. In addition, INSERT and UPDATE operations on the
 * referencing table do not entail any automatic enforcement of the referential
 * constraint. Any such violations of the constraint will be detected when the
 * referential constraint is eventually checked, at or before a commit.
 * <p/>
 * NOTE 53 - Even if constraint checking is not deferred, ON UPDATE
 * RESTRICT is a stricter condition than ON UPDATE NO ACTION. ON UPDATE
 * RESTRICT prohibits an update to a particular row if there are any
 * matching rows; ON UPDATE NO ACTION does not perform its constraint
 * NOTE 54 - Ditto for DELETE.
 * <p/>
 * Line numbers in the comments refer to svn revision 1580845 of Derby trunk.
 */
public class ForeignKeysDeferrableTest extends BaseJDBCTestCase
{
    private static final String  LANG_DEFERRED_FK_CONSTRAINT_T = "23516";
    private static final String  LANG_DEFERRED_FK_CONSTRAINT_S = "23517";
    private static final String  LANG_ADD_FK_CONSTRAINT_VIOLATION = "X0Y45";
    private static final String  LANG_FK_VIOLATION = "23503";


    private static String expImpDataFile;  // file used to perform
                                           // import/export
    private static boolean exportFilesCreatedEmbedded = false;
    private static boolean exportFilesCreatedClient = false;


    public ForeignKeysDeferrableTest(String name) {
        super(name);
    }


    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(new SupportFilesSetup(
                embeddedSuite(ForeignKeysDeferrableTest.class)));
        suite.addTest(new SupportFilesSetup(
                clientServerSuite(ForeignKeysDeferrableTest.class)));

        return suite;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Statement s = createStatement();

        // <table_name> ::= <role>_<statementType>_<action>
        // where:
        // <role> ::=  "ref"   /* a referenced table */
        //           | "t"     /* a referencing table: carries the FK */
        //
        // <statement type> ::= "d"  /* delete */
        //                    | "u"  /* update */
        //
        // <action> ::= "r"  /* restrict */
        //            | "c"  /* cascade */
        //            | "na" /* no action */
        //            | "nu" /* set null */
        //
        // Insert statements borrow the "d" tables.  The constraint names use
        // similar semantics, e.g. "c_d_c": constraint, delete table set,
        // cascade.

        s.executeUpdate(
                "create table ref_d_r(i int primary key, j int default 2)");
        s.executeUpdate(
                "create table ref_d_c(i int primary key, j int default 2)");
        s.executeUpdate(
                "create table ref_d_na(i int primary key, j int default 2)");
        s.executeUpdate(
                "create table ref_d_nu(i int primary key, j int default 2)");

        s.executeUpdate(
                "create table ref_u_r(i int primary key, j int default 2)");
        s.executeUpdate(
                "create table ref_u_na(i int primary key, j int default 2)");

        s.executeUpdate("insert into ref_d_r values (1, default)");
        s.executeUpdate("insert into ref_d_c values (1, default)");
        s.executeUpdate("insert into ref_d_na values (1, default)");
        s.executeUpdate("insert into ref_d_nu values (1, default)");

        s.executeUpdate("insert into ref_u_r values (1, default)");
        s.executeUpdate("insert into ref_u_na values (1, default)");


        // Tables for testing delete with {CASCADE, RESTRICT, SET NULL,
        // NOACTION}
        s.executeUpdate(
            "create table t_d_r(i int, j int default 2," +
            "   constraint c_d_r foreign key (i) references ref_d_r(i) " +
            "   on delete restrict " +
            "deferrable initially immediate)");

        s.executeUpdate(
            "create table t_d_c(i int, j int default 2," +
            "   constraint c_d_c foreign key (i) references ref_d_c(i) " +
            "   on delete cascade " +
            "deferrable initially immediate)");

        s.executeUpdate(
            "create table t_d_na(i int, j int default 2," +
            "   constraint c_d_na foreign key (i) references ref_d_na(i) " +
            "   on delete no action " +
            "deferrable initially immediate)");

        s.executeUpdate(
            "create table t_d_nu(i int, j int default 2," +
            "   constraint c_d_nu foreign key (i) references ref_d_nu(i) " +
            "   on delete set null " +
            "deferrable initially immediate)");

        // Tables for testing update with {RESTRICT, NOACTION}
        s.executeUpdate(
            "create table t_u_r(i int, j int default 2," +
            "   constraint c_u_r foreign key (i) references ref_u_r(i) " +
            "   on update restrict " +
            "deferrable initially immediate)");

        s.executeUpdate(
            "create table t_u_na(i int, j int default 2," +
            "   constraint c_u_na foreign key (i) references ref_u_na(i) " +
            "   on update no action " +
            "deferrable initially immediate)");

        s.executeUpdate("insert into t_d_r   values (1, default)");
        s.executeUpdate("insert into t_d_c   values (1, default)");
        s.executeUpdate("insert into t_d_na  values (1, default)");
        s.executeUpdate("insert into t_d_nu  values (1, default)");
        s.executeUpdate("insert into t_u_r   values (1, default)");
        s.executeUpdate("insert into t_u_na  values (1, default)");

        if ((usingEmbedded() && !exportFilesCreatedEmbedded) ||
            (usingDerbyNetClient() && !exportFilesCreatedClient)) {

            // We have to do this once for embedded and once for client/server
            if (usingEmbedded()) {
                exportFilesCreatedEmbedded = true;
            } else {
                exportFilesCreatedClient = true;
            }

            // Create a file for import that contains rows for which the
            // foreign key constraint doesn't hold.
            expImpDataFile =
                SupportFilesSetup.getReadWrite("t.data").getPath();
            s.executeUpdate("create table t(i int, j int)");
            s.executeUpdate("insert into t values (1,2),(2,2)");
            s.executeUpdate(
                "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (" +
                "    'APP' , 'T' , '" + expImpDataFile + "'," +
                "    null, null , null)");
            s.executeUpdate("drop table t");
        }

        setAutoCommit(false);
    }

    @Override
    protected void tearDown() throws Exception {
        rollback();
        setAutoCommit(true);
        Statement s = createStatement();
        dontThrow(s, "drop table t_d_r   ");
        dontThrow(s, "drop table t_d_c   ");
        dontThrow(s, "drop table t_d_na  ");
        dontThrow(s, "drop table t_d_nu  ");
        dontThrow(s, "drop table t_u_r   ");
        dontThrow(s, "drop table t_u_na  ");

        dontThrow(s, "drop table ref_d_r");
        dontThrow(s, "drop table ref_d_c");
        dontThrow(s, "drop table ref_d_na");
        dontThrow(s, "drop table ref_d_nu");

        dontThrow(s, "drop table ref_u_r");
        dontThrow(s, "drop table ref_u_na");

        super.tearDown();
    }

    /**
     * Insert row in non-deferred code path. Note that this use of "deferred"
     * refers to the insert processing, not the deferrable constraint.
     * It doesn't matter what constraint action we have on a FK when inserting,
     * it is always in "NO ACTION" mode, i.e. the constraint can be
     * deferred. In this example, we use the {t,ref}_d_r table pair, but it
     * could have been any of the others.
     *
     * @throws SQLException
     */
    public void testInsertDirect() throws SQLException {
        Statement s = createStatement();
        final String DIRECT_INSERT_SQL =
                "insert into t_d_r values (2, default)";

        // ...ForeignKeyRIChecker.doCheck(ForeignKeyRIChecker.java:99)
        // ...GenericRIChecker.doCheck(GenericRIChecker.java:91)
        // ...RISetChecker.doFKCheck(RISetChecker.java:121)
        // ...InsertResultSet.normalInsertCore(InsertResultSet.java:1028)

        assertStatementError(LANG_FK_VIOLATION, s, DIRECT_INSERT_SQL);

        s.executeUpdate("set constraints c_d_r deferred");
        s.executeUpdate(DIRECT_INSERT_SQL);

        assertCommitError(LANG_DEFERRED_FK_CONSTRAINT_T, getConnection());

        // Now see deferred check succeed by actually adding referenced key
        // *after* the insert of the referencing row. Also check that setting
        // immediate constraint mode throws a statement level error.

        s.executeUpdate("set constraints c_d_r deferred");
        s.executeUpdate(DIRECT_INSERT_SQL);
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_r immediate");
        s.executeUpdate("insert into ref_d_r values (2, default)");
        commit();

        // Now see deferred check of we after inserting the referencing row
        // delete it again before commit. Also check that setting immediate
        // constraint mode throws a statement level error.

        s.executeUpdate("set constraints c_d_r deferred");
        s.executeUpdate(DIRECT_INSERT_SQL.replaceAll("2", "3"));

        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_r immediate");
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_r immediate");

        s.executeUpdate("delete from t_d_r where i=3");
        commit();
    }

    /**
     * Insert row in deferred code path. Note that this use of "deferred"
     * refers to the insert processing, not the deferrable constraint.
     * It doesn't matter what constraint action we have on a FK when inserting,
     * it is always in "NO ACTION" mode, i.e. the constraint can be
     * deferred. In this example, we use the {t,ref}_d_r table pair, but it
     * could have been any of the others.
     *
     * @throws SQLException
     */
    public void testInsertDeferred() throws SQLException {
        Statement s = createStatement();
        final String DEFERRED_INSERT_SQL =
                "insert into t_d_r select i+1,j from t_d_r";

        // ...ForeignKeyRIChecker.doCheck(ForeignKeyRIChecker.java:99)
        // ...GenericRIChecker.doCheck(GenericRIChecker.java:91)
        // ...RISetChecker.doFKCheck(RISetChecker.java:121)
        // ...InsertResultSet.normalInsertCore(InsertResultSet.java:1205)
        // ...InsertResultSet.open(InsertResultSet.java:497)

        assertStatementError(LANG_FK_VIOLATION, s, DEFERRED_INSERT_SQL);

        s.executeUpdate("set constraints c_d_r deferred");
        s.executeUpdate(DEFERRED_INSERT_SQL);

        assertCommitError(LANG_DEFERRED_FK_CONSTRAINT_T, getConnection());

        // Now see deferred check succeed by actually adding referenced key
        // *after* the insert of the referencing row. Also check that setting
        // immediate constraint mode throws a statement level error.

        s.executeUpdate("set constraints c_d_r deferred");
        s.executeUpdate(DEFERRED_INSERT_SQL);
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_r immediate");
        s.executeUpdate("insert into ref_d_r values (2, default)");
        commit();

        // Now see deferred check of we after inserting the referencing row
        // delete it again before commit. Also check that setting immediate
        // constraint mode throws a statement level error.

        s.executeUpdate("set constraints c_d_r deferred");
        s.executeUpdate(DEFERRED_INSERT_SQL);

        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_r immediate");
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_r immediate");

        s.executeUpdate("delete from t_d_r where i=3");
        commit();
    }

    /**
     * Update row in non-deferred code path. Note that this use of "deferred"
     * refers to the insert processing, not the deferrable constraint.
     *
     * @throws SQLException
     */
    public void testUpdateDirect() throws SQLException {
        Statement s = createStatement();

        // Child:

        final String RESTRICTED_UPDATE_CHILD_SQL =
            "update t_u_r set i=2 where j=2";

        final String NO_ACTION_UPDATE_CHILD_SQL =
            "update t_u_na set i=2 where j=2";

        // NO ACTION: This should be deferred in deferred mode
        // ...ForeignKeyRIChecker.doCheck(ForeignKeyRIChecker.java:99)
        // ...GenericRIChecker.doCheck(GenericRIChecker.java:91)
        // ...RISetChecker.doFKCheck(RISetChecker.java:121)
        // ...UpdateResultSet.collectAffectedRows(UpdateResultSet.java:614)
        // ...UpdateResultSet.open(UpdateResultSet.java:259)

        //   N O   A C T I O N,     R E S T R I C T
        // Both are treated as NO ACTION since we are updating the child, not
        // the parent here.
        String[] constraint = new String[]{"c_u_r", "c_u_na"};
        String[] sql = new String[]{RESTRICTED_UPDATE_CHILD_SQL,
                                    NO_ACTION_UPDATE_CHILD_SQL};

        for (int i = 0; i < 2; i++) {
            assertStatementError(LANG_FK_VIOLATION, s, sql[i]);
            final String setDeferred =
                    "set constraints " + constraint[i] + " deferred";
            final String setImmediate =
                    "set constraints " + constraint[i] + " immediate";
            s.executeUpdate(setDeferred);
            s.executeUpdate(sql[i]);
            assertStatementError(
                    LANG_DEFERRED_FK_CONSTRAINT_S, s, setImmediate);
            assertStatementError(
                    LANG_DEFERRED_FK_CONSTRAINT_S, s, setImmediate);
            assertCommitError(LANG_DEFERRED_FK_CONSTRAINT_T, getConnection());
        }

        //   C A S C A D E,    S E T   N U L L
        //   Not applicable for update (Derby restriction). But if they had been
        //   implemented, the behavior should be as for NO ACTION above.

        // Parent:

        // Always performed deferred update mode, cf. this this explanation in
        // TableDescriptor#getAllRelevantConstraints: "For update, if we are
        // updating a referenced key, then we have to do it in deferred mode
        // (in case we update multiple rows)".
    }

    /**
     * Update row in deferred code path. Note that this use of "deferred"
     * refers to the insert processing, not the deferrable constraint.
     *
     * @throws SQLException
     */
    public void testUpdateDeferred() throws SQLException {
        Statement s = createStatement();

        // Update child

        final String RESTRICTED_UPDATE_CHILD_SQL =
            "update t_u_r set i=2 where i=1";

        final String NO_ACTION_UPDATE_CHILD_SQL =
            "update t_u_na set i=2 where i=1";

        final String RESTRICTED_UPDATE_PARENT_SQL =
            "update ref_u_r set i = 2 where i = 1";

        final String NO_ACTION_UPDATE_PARENT_SQL =
            "update ref_u_na set i = 2 where i = 1";

        // RESTRICT and NO ACTION. Since we are updating the child, we
        // run implicitly in NO ACTION mode, so can be deferred
        //
        // ...ForeignKeyRIChecker.doCheck(ForeignKeyRIChecker.java:99)
        // ...RISetChecker.doRICheck(RISetChecker.java:151)
        // ...UpdateResultSet.runChecker(UpdateResultSet.java:1005)
        // ...UpdateResultSet.open(UpdateResultSet.java:274)

        //   N O   A C T I O N,     R E S T R I C T
        // Both are treated as NO ACTION since we are updating the child, not
        // the parent here.

        String[] constraint = new String[]{"c_u_r", "c_u_na"};
        String[] sql = new String[]{RESTRICTED_UPDATE_CHILD_SQL,
                                    NO_ACTION_UPDATE_CHILD_SQL};

        for (int i = 0; i < 2; i++) {
            assertStatementError(LANG_FK_VIOLATION, s, sql[i]);
            final String setDeferred =
                    "set constraints " + constraint[i] + " deferred";
            final String setImmediate =
                    "set constraints " + constraint[i] + " immediate";
            s.executeUpdate(setDeferred);
            s.executeUpdate(sql[i]);
            assertStatementError(
                    LANG_DEFERRED_FK_CONSTRAINT_S, s, setImmediate);
            assertStatementError(
                    LANG_DEFERRED_FK_CONSTRAINT_S, s, setImmediate);
            assertCommitError(LANG_DEFERRED_FK_CONSTRAINT_T, getConnection());
        }



        // Parent, RESTRICT Should *never* be deferred.
        // ...ReferencedKeyRIChecker.doCheck(ReferencedKeyRIChecker.java:108)
        // ...RISetChecker.doRICheck(RISetChecker.java:151)
        // ...UpdateResultSet.runChecker(UpdateResultSet.java:961)
        // ...UpdateResultSet.open(UpdateResultSet.java:269)
        assertStatementError(LANG_FK_VIOLATION, s,
                             RESTRICTED_UPDATE_PARENT_SQL);

        // Since the action is RESTRICT, deferred constraint doesn't help:
        s.executeUpdate("set constraints c_u_r deferred");
        assertStatementError(LANG_FK_VIOLATION, s,
                             RESTRICTED_UPDATE_PARENT_SQL);


        // Parent, NO ACTION (different code path, cf. line 269 vs 274).
        // Should be deferred if constraint mode is deferred.
        //
        // ...ReferencedKeyRIChecker.doCheck(ReferencedKeyRIChecker.java:108)
        // ...RISetChecker.doRICheck(RISetChecker.java:151)
        // ...UpdateResultSet.runChecker(UpdateResultSet.java:961)
        // ...UpdateResultSet.open(UpdateResultSet.java:274) <-- Note:difference
        assertStatementError(LANG_FK_VIOLATION, s, NO_ACTION_UPDATE_PARENT_SQL);

        // Since the action is NO ACTION, deferral should work
        s.executeUpdate("set constraints c_u_na deferred");
        s.executeUpdate(NO_ACTION_UPDATE_PARENT_SQL);
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_u_na immediate");
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_u_na immediate");
        assertCommitError(LANG_DEFERRED_FK_CONSTRAINT_T, getConnection());

        // Resolve by resetting the value in the referenced row
        s.executeUpdate("set constraints c_u_na deferred");
        s.executeUpdate(NO_ACTION_UPDATE_PARENT_SQL);
        s.executeUpdate("update ref_u_na set i=1 where i=2");
        commit();

        // Resolve by resetting the referencing row
        s.executeUpdate("set constraints c_u_na deferred");
        s.executeUpdate(NO_ACTION_UPDATE_CHILD_SQL);
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_u_na immediate");
        s.executeUpdate("update t_u_na set i=1 where i=2");
        commit();

    }

    /**
     * Delete row in non-deferred code path. Note that this use of "deferred"
     * refers to the insert processing, not the deferrable constraint.
     *
     * @throws SQLException
     */
    public void testDeleteDirect() throws SQLException {
        Statement s = createStatement();

        // Delete of child row is trivial, parent no affected.

        // Parent

        final String RESTRICTED_DELETE_SQL = "delete from ref_d_r where i = 1";
        final String NO_ACTION_DELETE_SQL = "delete from ref_d_na where i = 1";
        final String CASCADE_DELETE_SQL = "delete from ref_d_c where i = 1";
        final String SET_NULL_DELETE_SQL = "delete from ref_d_nu where i = 1";

        // RESTRICT and NO ACTION: As far as triggers, there is no difference
        // when the execution of checking happens here, since in the presence
        // of triggers, row processing is deferred. But for deferred constraints
        // we need to treat these two differently: The RESTRICT code path
        // should check even in the presence of deferred FK constraints,
        // the NO ACTION code path should wait.
        //
        // ...ReferencedKeyRIChecker.doCheck(ReferencedKeyRIChecker.java:108)
        // ...RISetChecker.doPKCheck(RISetChecker.java:97)
        // ...DeleteResultSet.collectAffectedRows(DeleteResultSet.java:392)
        // ...DeleteResultSet.open(DeleteResultSet.java:136)

        //   R E S T R I C T
        assertStatementError(LANG_FK_VIOLATION, s, RESTRICTED_DELETE_SQL);

        // Since the action is RESTRICT, deferred constraint doesn't help.
        s.executeUpdate("set constraints c_d_r deferred");
        assertStatementError(LANG_FK_VIOLATION, s, RESTRICTED_DELETE_SQL);

        //   N O   A C T I O N
        assertStatementError(LANG_FK_VIOLATION, s, NO_ACTION_DELETE_SQL);

        // Since the action is NO ACTION, deferral should work
        s.executeUpdate("set constraints c_d_na deferred");
        s.executeUpdate(NO_ACTION_DELETE_SQL);
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_na immediate");
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_na immediate");
        assertCommitError(LANG_DEFERRED_FK_CONSTRAINT_T, getConnection());

        // Resolve by removing the referencing row
        s.executeUpdate("set constraints c_d_na deferred");
        s.executeUpdate(NO_ACTION_DELETE_SQL);
        s.executeUpdate("delete from t_d_na where i=1");
        commit();

        // Resolve by re-inserting the referenced row
        s.executeUpdate("insert into ref_d_na values (1, default)");
        s.executeUpdate("insert into t_d_na values (1, default)");
        commit();
        s.executeUpdate("set constraints c_d_na deferred");
        s.executeUpdate(NO_ACTION_DELETE_SQL);
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_na immediate");
        s.executeUpdate("insert into ref_d_na values (1, default)");
        commit();

        //   C A S C A D E : not impacted by deferred
        s.executeUpdate(CASCADE_DELETE_SQL);
        JDBC.assertEmpty(s.executeQuery("select * from t_d_c"));
        rollback();

        s.executeUpdate("set constraints c_d_c deferred");
        s.executeUpdate(CASCADE_DELETE_SQL);
        JDBC.assertEmpty(s.executeQuery("select * from t_d_c"));
        commit();

        //   S E T   N U L L : not impacted by deferred
        s.executeUpdate(SET_NULL_DELETE_SQL);
        assertResults(
                s.executeQuery("select i from t_d_nu"),
                new String[][]{{null}},
                false);
        rollback();

        s.executeUpdate("set constraints c_d_nu deferred");
        s.executeUpdate(SET_NULL_DELETE_SQL);
        assertResults(
                s.executeQuery("select i from t_d_nu"),
                new String[][]{{null}},
                false);
        commit();
    }

    /**
     * Delete row in deferred code path. Note that this use of "deferred"
     * refers to the insert processing, not the deferrable constraint.
     *
     * @throws SQLException
     */
        public void testDeleteDeferred() throws SQLException {
        Statement s = createStatement();
        // Delete of child row is trivial, parent no affected.

        // Parent

        final String RESTRICTED_DELETE_SQL =
                "delete from ref_d_r where i = 1 and " +
                "    i in (select i from ref_d_r)";

        final String NO_ACTION_DELETE_SQL =
                "delete from ref_d_na where i = 1 and " +
                "    i in (select i from ref_d_na)";

        final String CASCADE_DELETE_SQL =
                "delete from ref_d_c where i = 1 and " +
                "    i in (select i from ref_d_c)";

        final String SET_NULL_DELETE_SQL =
                "delete from ref_d_nu where i = 1 and " +
                "    i in (select i from ref_d_nu)";


        // RESTRICT - This checking should *never* be deferred
        // ...ReferencedKeyRIChecker.doCheck(ReferencedKeyRIChecker.java:108)
        // ...RISetChecker.doPKCheck(RISetChecker.java:97)
        // ...DeleteResultSet.runFkChecker(DeleteResultSet.java:559)
        // ...DeleteResultSet.open(DeleteResultSet.java:151)

        //   R E S T R I C T
        assertStatementError(LANG_FK_VIOLATION, s, RESTRICTED_DELETE_SQL);

        // Since the action is RESTRICT, deferred constraint doesn't help.
        s.executeUpdate("set constraints c_d_r deferred");
        assertStatementError(LANG_FK_VIOLATION, s, RESTRICTED_DELETE_SQL);

        // NO ACTION - This checking should be deferred in deferred mode
        // ...ReferencedKeyRIChecker.doCheck(ReferencedKeyRIChecker.java:108)
        // ...RISetChecker.doPKCheck(RISetChecker.java:97)
        // ...DeleteResultSet.runFkChecker(DeleteResultSet.java:559)
        // ...DeleteResultSet.open(DeleteResultSet.java:154) <-- Note:difference

        //   N O   A C T I O N
        assertStatementError(LANG_FK_VIOLATION, s, NO_ACTION_DELETE_SQL);

        // Since the action is NO ACTION, deferral should work
        s.executeUpdate("set constraints c_d_na deferred");
        s.executeUpdate(NO_ACTION_DELETE_SQL);
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_na immediate");
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_na immediate");
        assertCommitError(LANG_DEFERRED_FK_CONSTRAINT_T, getConnection());

        // Resolve by removing the referencing row
        s.executeUpdate("set constraints c_d_na deferred");
        s.executeUpdate(NO_ACTION_DELETE_SQL);
        s.executeUpdate("delete from t_d_na where i=1");
        commit();

        // Resolve by re-inserting the referenced row
        s.executeUpdate("insert into ref_d_na values (1, default)");
        s.executeUpdate("insert into t_d_na values (1, default)");
        commit();
        s.executeUpdate("set constraints c_d_na deferred");
        s.executeUpdate(NO_ACTION_DELETE_SQL);
        assertStatementError(LANG_DEFERRED_FK_CONSTRAINT_S, s,
                             "set constraints c_d_na immediate");
        s.executeUpdate("insert into ref_d_na values (1, default)");
        commit();

        //   C A S C A D E : not impacted by deferred
        s.executeUpdate(CASCADE_DELETE_SQL);
        JDBC.assertEmpty(s.executeQuery("select * from t_d_c"));
        rollback();

        s.executeUpdate("set constraints c_d_c deferred");
        s.executeUpdate(CASCADE_DELETE_SQL);
        JDBC.assertEmpty(s.executeQuery("select * from t_d_c"));
        commit();

        //   S E T   N U L L : not impacted by deferred
        s.executeUpdate(SET_NULL_DELETE_SQL);
        assertResults(
                s.executeQuery("select i from t_d_nu"),
                new String[][]{{null}},
                false);
        rollback();

        s.executeUpdate("set constraints c_d_nu deferred");
        s.executeUpdate(SET_NULL_DELETE_SQL);
        assertResults(
                s.executeQuery("select i from t_d_nu"),
                new String[][]{{null}},
                false);
        commit();
    }

    /**
     * Insert using bulk import code path, i.e. IMPORT. Since IMPORT
     * always performs a commit at the end, we strictly do no need to do
     * extra processing for deferrable constraints, but we do so
     * anyway to prepare for possible future lifting of this restriction to
     * IMPORT. This behavior can no be observed externally, but we include
     * the test here anyway as a baseline.
     *
     * @throws SQLException
     */
    public void testBulkInsert() throws SQLException {
        Statement s = createStatement();

        // Try the test cases below with both "replace" and "append"
        // semantics. It doesn't matter what constraint action
        // we have on a FK when inserting, it is always in "NO ACTION" mode,
        // i.e. the constraint can be deferred. In this example, we
        // use the {t,ref}_d_r table pair, but it could have been any of the
        // others.
        for (int addOrReplace = 0; addOrReplace < 2; addOrReplace++) {
            // import and implicit commit leads to checking

            // ADD:
            // ...ForeignKeyRIChecker.doCheck(ForeignKeyRIChecker.java:99)
            // ...GenericRIChecker.doCheck(GenericRIChecker.java:91)
            // ...RISetChecker.doFKCheck(RISetChecker.java:121)
            // ...InsertResultSet.normalInsertCore(InsertResultSet.java:1028)
            // ...InsertResultSet.open(InsertResultSet.java:497)

            // REPLACE:
            // ...InsertResultSet.bulkValidateForeignKeysCore(
            //         InsertResultSet.java:1726)
            // ...InsertResultSet.bulkValidateForeignKeys(
            //         InsertResultSet.java:1594)
            // ...InsertResultSet.open(InsertResultSet.java:490)

            assertStatementError(
                LANG_FK_VIOLATION,
                s,
                "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                "    'APP' , 'T_D_R' , '" + expImpDataFile + "'," +
                "    null, null , null, " + addOrReplace + ")");

            s.executeUpdate("set constraints c_d_r deferred");
            assertStatementError(
                LANG_DEFERRED_FK_CONSTRAINT_T,
                s,
                "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                "    'APP' , 'T_D_R' , '" + expImpDataFile + "'," +
                "    null, null , null, " + addOrReplace + ")");
        }
    }

    public void testAddConstraint() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create table t2(i int)");

        try {
            s.executeUpdate("insert into t2 values 1,2");
            commit();

            // First, try with immediate checking
            assertStatementError(LANG_ADD_FK_CONSTRAINT_VIOLATION, s,
                    "alter table t2 add constraint " +
                    "    c2 foreign key(i) references ref_d_r(i)");
            s.executeUpdate("delete from t2 where i=2");

            // Delete the row with 2 should make it OK to add the constraint:
            s.executeUpdate("alter table t2 add constraint " +
                    "c2 foreign key(i) references ref_d_r(i)");

            rollback();
            s.executeUpdate("delete from t2");

            // Now try with deferred constraint
            s.executeUpdate("insert into t2 values 1,2");
            commit();

            s.executeUpdate("alter table t2 add constraint " +
                    "    c2 foreign key(i) references ref_d_r(i) " +
                    "    initially deferred");

            assertCommitError(LANG_DEFERRED_FK_CONSTRAINT_T, getConnection());


        } finally {
            dontThrow(s, "drop table t2");
            commit();
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
