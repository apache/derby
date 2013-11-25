/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.TriggerWhenClauseTest

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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for the WHEN clause in CREATE TRIGGER statements, added in DERBY-534.
 */
public class TriggerWhenClauseTest extends BaseJDBCTestCase {

    /**
     * List that tracks calls to {@code intProcedure()}. It is used to verify
     * that triggers have fired.
     */
    private static List<Integer> procedureCalls;

    private static final String SYNTAX_ERROR = "42X01";
    private static final String REFERENCES_SESSION_SCHEMA = "XCL51";
    private static final String NOT_BOOLEAN = "42X19";
    private static final String HAS_PARAMETER = "42Y27";
    private static final String HAS_DEPENDENTS = "X0Y25";
    private static final String TABLE_DOES_NOT_EXIST = "42X05";
    private static final String TRUNCATION = "22001";
    private static final String NOT_AUTHORIZED = "42504";
    private static final String NO_TABLE_PERMISSION = "42500";
    private static final String USER_EXCEPTION = "38000";
    private static final String JAVA_EXCEPTION = "XJ001";
    private static final String NOT_SINGLE_COLUMN = "42X39";
    private static final String NON_SCALAR_QUERY = "21000";
    private static final String TRIGGER_RECURSION = "54038";
    private static final String PROC_USED_AS_FUNC = "42Y03";

    public TriggerWhenClauseTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.sqlAuthorizationDecorator(
            new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(TriggerWhenClauseTest.class)));
    }

    @Override
    protected void initializeConnection(Connection conn) throws SQLException {
        // Run the test cases with auto-commit off so that all changes to
        // the database can be rolled back in tearDown().
        conn.setAutoCommit(false);
    }

    @Override
    protected void setUp() {
        procedureCalls = Collections.synchronizedList(new ArrayList<Integer>());
    }

    @Override
    protected void tearDown() throws Exception {
        procedureCalls = null;
        super.tearDown();
    }

    /**
     * A procedure that takes an {@code int} argument and adds it to the
     * {@link #procedureCalls} list. Can be used as a stored procedure to
     * verify that a trigger has been called. Particularly useful in BEFORE
     * triggers, as they are not allowed to modify SQL data.
     *
     * @param i an integer
     */
    public static void intProcedure(int i) {
        procedureCalls.add(i);
    }

    public void testBasicSyntax() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(y varchar(20))");

        // Create after triggers that should always be executed. Create row
        // trigger, statement trigger and implicit statement trigger.
        s.execute("create trigger tr01 after insert on t1 for each row "
                + "when (true) insert into t2 values 'Executed tr01'");
        s.execute("create trigger tr02 after insert on t1 for each statement "
                + "when (true) insert into t2 values 'Executed tr02'");
        s.execute("create trigger tr03 after insert on t1 "
                + "when (true) insert into t2 values 'Executed tr03'");

        // Create corresponding triggers that should never fire (their WHEN
        // clause is false).
        s.execute("create trigger tr04 after insert on t1 for each row "
                + "when (false) insert into t2 values 'Executed tr04'");
        s.execute("create trigger tr05 after insert on t1 for each statement "
                + "when (false) insert into t2 values 'Executed tr05'");
        s.execute("create trigger tr06 after insert on t1 "
                + "when (false) insert into t2 values 'Executed tr06'");

        // Create triggers with EXISTS subqueries in the WHEN clause. The
        // first returns TRUE and the second returns FALSE.
        s.execute("create trigger tr07 after insert on t1 "
                + "when (exists (select * from sysibm.sysdummy1)) "
                + "insert into t2 values 'Executed tr07'");
        s.execute("create trigger tr08 after insert on t1 "
                + "when (exists "
                + "(select * from sysibm.sysdummy1 where ibmreqd <> 'Y')) "
                + "insert into t2 values 'Executed tr08'");

        // WHEN clause returns NULL, trigger should not be fired.
        s.execute("create trigger tr09 after insert on t1 "
                + "when (cast(null as boolean))"
                + "insert into t2 values 'Executed tr09'");

        // WHEN clause contains reference to a transition variable.
        s.execute("create trigger tr10 after insert on t1 "
                + "referencing new as new for each row "
                + "when (new.x <> 2) insert into t2 values 'Executed tr10'");

        // WHEN clause contains reference to a transition table.
        s.execute("create trigger tr11 after insert on t1 "
                + "referencing new table as new "
                + "when (exists (select * from new where x > 5)) "
                + "insert into t2 values 'Executed tr11'");

        // Scalar subqueries are allowed in the WHEN clause, but they need an
        // extra set of parantheses.
        //
        // The first set of parantheses is required by the WHEN clause syntax
        // itself: WHEN ( <search condition> )
        //
        // The second set of parantheses is required by <search condition>.
        // Follow this path through the SQL standard's syntax rules:
        //    <search condition> -> <boolean value expression>
        //      -> <boolean term> -> <boolean factor> -> <boolean test>
        //      -> <boolean primary> -> <boolean predicand>
        //      -> <nonparenthesized value expression primary>
        //      -> <scalar subquery> -> <subquery> -> <left paren>
        assertCompileError(SYNTAX_ERROR,
                "create trigger tr12 after insert on t1 "
                + "when (values true) insert into t2 values 'Executed tr12'");
        assertCompileError(SYNTAX_ERROR,
                "create trigger tr13 after insert on t1 "
                + "when (select true from sysibm.sysdummy1) "
                + "insert into t2 values 'Executed tr13'");
        s.execute("create trigger tr12 after insert on t1 "
                + "when ((values true)) insert into t2 values 'Executed tr12'");
        s.execute("create trigger tr13 after insert on t1 "
                + "when ((select true from sysibm.sysdummy1)) "
                + "insert into t2 values 'Executed tr13'");

        // Now fire the triggers and verify the results.
        assertUpdateCount(s, 3, "insert into t1 values 1, 2, 3");
        JDBC.assertFullResultSet(
            s.executeQuery("select y, count(*) from t2 group by y order by y"),
            new String[][] {
                { "Executed tr01", "3" },
                { "Executed tr02", "1" },
                { "Executed tr03", "1" },
                { "Executed tr07", "1" },
                { "Executed tr10", "2" },
                { "Executed tr12", "1" },
                { "Executed tr13", "1" },
            });

        // Empty t2 before firing the triggers again.
        s.execute("delete from t2");

        // Insert more rows with different values and see that a slightly
        // different set of triggers get fired.
        assertUpdateCount(s, 2, "insert into t1 values 2, 6");
        JDBC.assertFullResultSet(
            s.executeQuery("select y, count(*) from t2 group by y order by y"),
            new String[][] {
                { "Executed tr01", "2" },
                { "Executed tr02", "1" },
                { "Executed tr03", "1" },
                { "Executed tr07", "1" },
                { "Executed tr10", "1" },
                { "Executed tr11", "1" },
                { "Executed tr12", "1" },
                { "Executed tr13", "1" },
            });
    }

    /**
     * A row trigger whose WHEN clause contains a subquery, used to cause a
     * NullPointerException in some situations.
     */
    public void testSubqueryInWhenClauseNPE() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(x int)");
        s.execute("create trigger tr1 after insert on t1 for each row "
                + "when ((values true)) insert into t2 values 1");

        // This statement used to result in a NullPointerException.
        s.execute("insert into t1 values 1,2,3");
    }

    /**
     * Test generated columns referenced from WHEN clauses. In particular,
     * test that references to generated columns are disallowed in the NEW
     * transition variable of BEFORE triggers. See DERBY-3948.
     *
     * @see GeneratedColumnsTest#test_024_beforeTriggers()
     */
    public void testGeneratedColumns() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int, y int, "
                + "z int generated always as (x+y))");
        s.execute("create table t2(x int)");
        s.execute("create procedure int_proc(i int) language java "
                + "parameter style java external name '"
                + getClass().getName() + ".intProcedure' no sql");

        // BEFORE INSERT trigger without generated column in WHEN clause, OK.
        s.execute("create trigger btr1 no cascade before insert on t1 "
                + "referencing new as new for each row when (new.x < new.y) "
                + "call int_proc(1)");

        // BEFORE INSERT trigger with generated column in WHEN clause, fail.
        assertCompileError(GeneratedColumnsHelper.BAD_BEFORE_TRIGGER,
                "create trigger btr2 no cascade before insert on t1 "
                + "referencing new as new for each row when (new.x < new.z) "
                + "select * from sysibm.sysdummy1");

        // BEFORE UPDATE trigger without generated column in WHEN clause, OK.
        s.execute("create trigger btr3 no cascade before update on t1 "
                + "referencing new as new old as old for each row "
                + "when (new.x < old.x) call int_proc(3)");

        // BEFORE UPDATE trigger with generated column in WHEN clause. OK,
        // since the generated column is in the OLD transition variable.
        s.execute("create trigger btr4 no cascade before update on t1 "
                + "referencing old as old for each row when (old.x < old.z) "
                + "call int_proc(4)");

        // BEFORE UPDATE trigger with generated column in NEW transition
        // variable, fail.
        assertCompileError(GeneratedColumnsHelper.BAD_BEFORE_TRIGGER,
                "create trigger btr5 no cascade before update on t1 "
                + "referencing new as new for each row when (new.x < new.z) "
                + "select * from sysibm.sysdummy1");

        // BEFORE DELETE trigger without generated column in WHEN clause, OK.
        s.execute("create trigger btr6 no cascade before delete on t1 "
                + "referencing old as old for each row when (old.x < 3) "
                + "call int_proc(6)");

        // BEFORE DELETE trigger with generated column in WHEN clause. OK,
        // since the generated column is in the OLD transition variable.
        s.execute("create trigger btr7 no cascade before delete on t1 "
                + "referencing old as old for each row when (old.x < old.z) "
                + "call int_proc(7)");

        // References to generated columns in AFTER triggers should always
        // be allowed.
        s.execute("create trigger atr1 after insert on t1 "
                + "referencing new as new for each row "
                + "when (new.x < new.z) insert into t2 values 1");
        s.execute("create trigger atr2 after update on t1 "
                + "referencing new as new old as old for each row "
                + "when (old.z < new.z) insert into t2 values 2");
        s.execute("create trigger atr3 after delete on t1 "
                + "referencing old as old for each row "
                + "when (old.x < old.z) insert into t2 values 3");

        // Finally, fire the triggers.
        s.execute("insert into t1(x, y) values (1, 2), (4, 3)");
        s.execute("update t1 set x = y");
        s.execute("delete from t1");

        // Verify that the before triggers were executed as expected.
        assertEquals(Arrays.asList(1, 3, 4, 4, 6, 7, 7), procedureCalls);

        // Verify that the after triggers were executed as expected.
        JDBC.assertFullResultSet(
                s.executeQuery("select * from t2 order by x"),
                new String[][]{{"1"}, {"1"}, {"2"}, {"3"}, {"3"}});
    }

    /**
     * Test various illegal WHEN clauses.
     */
    public void testIllegalWhenClauses() throws SQLException {
        Statement s = createStatement();
        s.execute("declare global temporary table temptable (x int) "
                + "not logged");
        s.execute("create table t1(x int)");
        s.execute("create table t2(x int)");
        s.execute("create procedure int_proc(i int) language java "
                + "parameter style java external name '"
                + getClass().getName() + ".intProcedure' no sql");

        // CREATE TRIGGER should fail if the WHEN clause references a table
        // in the SESSION schema.
        assertCompileError(REFERENCES_SESSION_SCHEMA,
                "create trigger tr1 after insert on t1 "
                + "when (exists (select * from session.temptable)) "
                + "insert into t2 values 1");

        // The WHEN clause expression must be BOOLEAN.
        assertCompileError(NOT_BOOLEAN,
                "create trigger tr after insert on t1 "
                + "when (1) insert into t2 values 1");
        assertCompileError(NOT_BOOLEAN,
                "create trigger tr after update on t1 "
                + "when ('abc') insert into t2 values 1");
        assertCompileError(NOT_BOOLEAN,
                "create trigger tr after delete on t1 "
                + "when ((values 1)) insert into t2 values 1");
        assertCompileError(NOT_BOOLEAN,
                "create trigger tr no cascade before insert on t1 "
                + "when ((select ibmreqd from sysibm.sysdummy1)) "
                + "call int_proc(1)");
        assertCompileError(NOT_BOOLEAN,
                "create trigger tr no cascade before insert on t1 "
                + "when ((select ibmreqd from sysibm.sysdummy1)) "
                + "call int_proc(1)");
        assertCompileError(NOT_BOOLEAN,
                "create trigger tr no cascade before update on t1 "
                + "referencing old as old for each row "
                + "when (old.x) call int_proc(1)");

        // Dynamic parameters (?) are not allowed in the WHEN clause.
        assertCompileError(HAS_PARAMETER,
                "create trigger tr no cascade before delete on t1 "
                + "when (?) call int_proc(1)");
        assertCompileError(HAS_PARAMETER,
                "create trigger tr after insert on t1 "
                + "when (cast(? as boolean)) call int_proc(1)");
        assertCompileError(HAS_PARAMETER,
                "create trigger tr after delete on t1 "
                + "when ((select true from sysibm.sysdummy where ibmreqd = ?)) "
                + "call int_proc(1)");

        // Subqueries in the WHEN clause must have a single column
        assertCompileError(NOT_SINGLE_COLUMN,
                "create trigger tr no cascade before insert on t1 "
                + "when ((values (true, false))) call int_proc(1)");
        assertCompileError(NOT_SINGLE_COLUMN,
                "create trigger tr after update of x on t1 "
                + "when ((select tablename, schemaid from sys.systables)) "
                + "call int_proc(1)");
    }

    /**
     * Verify that the SPS of a WHEN clause is invalidated when one of its
     * dependencies is changed in a way that requires recompilation.
     */
    public void testWhenClauseInvalidation() throws SQLException {
        // Statement that checks the validity of the WHEN clause SPS.
        PreparedStatement spsValid = prepareStatement(
                "select valid from sys.sysstatements "
                + "where stmtname like 'TRIGGERWHEN%'");

        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(x int)");
        s.execute("create table t3(x int)");
        s.execute("insert into t1 values 1");

        s.execute("create trigger tr after insert on t2 "
                + "referencing new as new for each row "
                + "when (exists (select * from t1 where x = new.x)) "
                + "insert into t3 values new.x");

        // SPS is initially valid.
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "true");

        // Compressing the table referenced in the WHEN clause should
        // invalidate the SPS.
        PreparedStatement compress = prepareStatement(
                "call syscs_util.syscs_compress_table(?, 'T1', 1)");
        compress.setString(1, TestConfiguration.getCurrent().getUserName());
        compress.execute();
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "false");

        // Invoking the trigger should recompile the SPS.
        s.execute("insert into t2 values 0,1,2");
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "true");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t3"), "1");
    }

    /**
     * Test that dropping objects referenced from the WHEN clause will
     * detect that the trigger depends on the object.
     */
    public void testDependencies() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int, y int, z int)");
        s.execute("create table t2(x int, y int, z int)");

        Savepoint sp = getConnection().setSavepoint();

        // Dropping columns referenced via the NEW transition variable in
        // a WHEN clause should fail.
        s.execute("create trigger tr after insert on t1 "
                + "referencing new as new for each row "
                + "when (new.x < new.y) values 1");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column x restrict");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column y restrict");
        s.execute("alter table t1 drop column z restrict");
        getConnection().rollback(sp);

        // Dropping columns referenced via the OLD transition variable in
        // a WHEN clause should fail.
        s.execute("create trigger tr no cascade before delete on t1 "
                + "referencing old as old for each row "
                + "when (old.x < old.y) values 1");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column x restrict");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column y restrict");
        s.execute("alter table t1 drop column z restrict");
        getConnection().rollback(sp);

        // Dropping columns referenced via either the OLD or the NEW
        // transition variable referenced in the WHEN clause should fail.
        s.execute("create trigger tr no cascade before update on t1 "
                + "referencing old as old new as new for each row "
                + "when (old.x < new.y) values 1");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column x restrict");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column y restrict");
        s.execute("alter table t1 drop column z restrict");
        getConnection().rollback(sp);

        // Dropping columns referenced either in the WHEN clause or in the
        // triggered SQL statement should fail.
        s.execute("create trigger tr no cascade before insert on t1 "
                + "referencing new as new for each row "
                + "when (new.x < 5) values new.y");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column x restrict");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column y restrict");
        s.execute("alter table t1 drop column z restrict");
        getConnection().rollback(sp);

        // Dropping any column in a statement trigger with a NEW transition
        // table fails, even if the column is not referenced in the WHEN clause
        // or in the triggered SQL text.
        s.execute("create trigger tr after update of x on t1 "
                + "referencing new table as new "
                + "when (exists (select 1 from new where x < y)) values 1");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column x restrict");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column y restrict");
        // Z is not referenced, but the transition table depends on all columns.
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column z restrict");
        getConnection().rollback(sp);

        // Dropping any column in a statement trigger with an OLD transition
        // table fails, even if the column is not referenced in the WHEN clause
        // or in the triggered SQL text.
        s.execute("create trigger tr after delete on t1 "
                + "referencing old table as old "
                + "when (exists (select 1 from old where x < y)) values 1");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column x restrict");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column y restrict");
        // Z is not referenced, but the transition table depends on all columns.
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column z restrict");
        getConnection().rollback(sp);

        // References to columns in other ways than via transition variables
        // or transition tables should also be detected.
        s.execute("create trigger tr after delete on t1 "
                + "referencing old table as old "
                + "when (exists (select 1 from t1 where x < y)) values 1");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column x restrict");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t1 drop column y restrict");
        s.execute("alter table t1 drop column z restrict");
        getConnection().rollback(sp);

        // References to columns in another table than the trigger table
        // should prevent them from being dropped.
        s.execute("create trigger tr after insert on t1 "
                + "when (exists (select * from t2 where x < y)) "
                + "values 1");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t2 drop column x restrict");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t2 drop column y restrict");
        s.execute("alter table t2 drop column z restrict");

        // Dropping a table referenced in a WHEN clause should fail and leave
        // the trigger intact. Before DERBY-2041, DROP TABLE would succeed
        // and leave the trigger in an invalid state so that subsequent
        // INSERT statements would fail when trying to fire the trigger.
        assertStatementError(HAS_DEPENDENTS, s, "drop table t2");
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select triggername from sys.systriggers"), "TR");
        s.executeUpdate("insert into t1 values (1, 2, 3)");
        getConnection().rollback(sp);

        // Test references to columns in both the WHEN clause and the
        // triggered SQL statement.
        s.execute("create trigger tr after update on t1 "
                + "when (exists (select * from t2 where x < 5)) "
                + "select y from t2");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t2 drop column x restrict");
        assertStatementError(HAS_DEPENDENTS, s,
                "alter table t2 drop column y restrict");
        s.execute("alter table t2 drop column z restrict");

        // DROP TABLE should fail because of the dependencies (didn't before
        // DERBY-2041).
        assertStatementError(HAS_DEPENDENTS, s, "drop table t2");
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select triggername from sys.systriggers"), "TR");
        getConnection().rollback(sp);
    }

    /**
     * Verify that DERBY-4874, which was fixed before support for the WHEN
     * clause was implemented, does not affect the WHEN clause.
     * The first stab at the WHEN clause implementation did suffer from it.
     */
    public void testDerby4874() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t(x varchar(3))");
        s.execute("create trigger tr after update of x on t "
                + "referencing new as new for each row "
                + "when (new.x < 'abc') values 1");
        s.execute("insert into t values 'aaa'");

        // Updating X to something longer than 3 characters should fail,
        // since it's a VARCHAR(3).
        assertStatementError(TRUNCATION, s, "update t set x = 'aaaa'");

        // Change the type of X to VARCHAR(4) and try again. This time it
        // should succeed, but it used to fail because the trigger hadn't
        // been recompiled and still thought the max length was 3.
        s.execute("alter table t alter x set data type varchar(4)");
        assertUpdateCount(s, 1, "update t set x = 'aaaa'");

        // Updating it to a longer value should still fail.
        assertStatementError(TRUNCATION, s, "update t set x = 'aaaaa'");
    }

    /**
     * Verify that Cloudscape bug 4821, which was fixed long before support
     * for the WHEN clause was implemented, does not affect the WHEN clause.
     * The first stab at the WHEN clause implementation did suffer from it.
     */
    public void testCloudscapeBug4821() throws SQLException {
        // First create a trigger, and immediately perform an ALTER TABLE
        // statement on the trigger table to make sure the trigger's SPS is
        // invalid and must be recompiled the first time it's fired.
        Statement s = createStatement();
        s.execute("create table cs4821.t(x int)");
        s.execute("create trigger cs4821.tr after insert on cs4821.t "
                + "when (true) values 1");
        s.execute("alter table cs4821.t add column y int");
        commit();

        // Fire the trigger and leave the transaction open afterwards.
        s.execute("insert into cs4821.t(x) values 1");

        // Now try to read all rows from the SYS.SYSSTATEMENTS table from
        // another transaction. Used to time out because the transaction
        // that recompiled the trigger kept the lock on the system table.
        Connection c2 = openDefaultConnection();
        Statement s2 = c2.createStatement();
        JDBC.assertDrainResults(
                s2.executeQuery("select * from sys.sysstatements"));
        s2.close();
        JDBC.cleanup(c2);

        // Remove all tables and triggers created by this test case.
        JDBC.dropSchema(getConnection().getMetaData(), "CS4821");
    }

    /**
     * When SQL authorization is enabled, the trigger action (including the
     * WHEN clause) should execute with definer's rights. Verify that it is
     * so.
     */
    public void testGrantRevoke() throws SQLException {
        Connection c1 = openDefaultConnection("u1", "dummy");
        c1.setAutoCommit(true);
        Statement s1 = c1.createStatement();

        s1.execute("create table t1(x varchar(20))");
        s1.execute("create table t2(x varchar(200))");
        s1.execute("create table t3(x int)");
        s1.execute("create function is_true(s varchar(128)) returns boolean "
                + "deterministic language java parameter style java "
                + "external name 'java.lang.Boolean.parseBoolean' no sql");

        // Trigger that fires on T1 if inserted value is 'true'.
        s1.execute("create trigger tr1 after insert on t1 "
                + "referencing new as new for each row "
                + "when (is_true(new.x)) insert into t2(x) values new.x");

        // Trigger that fires on T1 on insert if T3 has more than 1 row.
        s1.execute("create trigger tr2 after insert on t1 "
                + "when (exists (select * from t3 offset 1 row)) "
                + "insert into t2(x) values '***'");

        // Allow U2 to insert into T1, but nothing else on U1's schema.
        s1.execute("grant insert on table t1 to u2");

        Connection c2 = openDefaultConnection("u2", "dummy");
        c2.setAutoCommit(true);
        Statement s2 = c2.createStatement();

        // User U2 is not authorized to invoke the function IS_TRUE, but
        // is allowed to insert into T1.
        assertStatementError(NOT_AUTHORIZED, s2, "values u1.is_true('abc')");
        assertUpdateCount(s2, 4,
                "insert into u1.t1(x) values 'abc', 'true', 'TrUe', 'false'");

        // Verify that the trigger fired. Since the trigger runs with
        // definer's rights, it should be allowed to invoke IS_TRUE in the
        // WHEN clause even though U2 isn't allowed to invoke it directly.
        JDBC.assertFullResultSet(s1.executeQuery("select * from t2 order by x"),
                                 new String[][] {{"TrUe"}, {"true"}});
        s1.execute("delete from t2");

        // Now test that TR1 will also fire, even though U2 isn't granted
        // SELECT privileges on the table read by the WHEN clause.
        s1.execute("insert into t3 values 1, 2");
        assertUpdateCount(s2, 2, "insert into u1.t1(x) values 'x', 'y'");
        JDBC.assertSingleValueResultSet(
                s1.executeQuery("select * from t2 order by x"), "***");
        s1.execute("delete from t2");

        // Now invalidate the triggers and make sure they still work after
        // recompilation.
        s1.execute("alter table t1 alter column x set data type varchar(200)");
        assertUpdateCount(s2, 2, "insert into u1.t1(x) values 'true', 'false'");
        JDBC.assertFullResultSet(s1.executeQuery("select * from t2 order by x"),
                                 new String[][] {{"***"}, {"true"}});
        s1.execute("delete from t2");

        // Revoke U2's insert privilege on T1.
        s1.execute("revoke insert on table t1 from u2 ");

        // U2 should not be allowed to insert into T1 anymore.
        assertStatementError(NO_TABLE_PERMISSION, s2,
                             "insert into u1.t1(x) values 'abc'");

        // U1 should still be allowed to do it (since U1 owns T1), and the
        // triggers should still be working.
        assertUpdateCount(s1, 2, "insert into t1(x) values 'true', 'false'");
        JDBC.assertFullResultSet(s1.executeQuery("select * from t2 order by x"),
                                 new String[][] {{"***"}, {"true"}});
        s1.execute("delete from t2");

        // Now try to define a trigger in U2's schema that needs to invoke
        // U1.IS_TRUE. Should fail because U2 isn't allowed to invoke it.
        s2.execute("create table t(x varchar(200))");
        assertStatementError(NOT_AUTHORIZED, s2,
                             "create trigger tr after insert on t "
                             + "referencing new as new for each row "
                             + "when (u1.is_true(new.x)) values 1");

        // Try again after granting execute permission to U2.
        s1.execute("grant execute on function is_true to u2");
        s2.execute("create trigger tr after insert on t "
                + "referencing new as new for each row "
                + "when (u1.is_true(new.x)) values 1");

        // Fire trigger.
        assertUpdateCount(s2, 3, "insert into t values 'ab', 'cd', 'ef'");

        // Revoking the execute permission will fail because the trigger
        // depends on it.
        assertStatementError(HAS_DEPENDENTS, s1,
                "revoke execute on function is_true from u2 restrict");

        s1.close();
        s2.close();

        c2.setAutoCommit(false);
        JDBC.dropSchema(c2.getMetaData(), "U2");
        c1.setAutoCommit(false);
        JDBC.dropSchema(c1.getMetaData(), "U1");
    }

    /**
     * Test that the trigger fails gracefully if the WHEN clause throws
     * a RuntimeException.
     */
    public void testRuntimeException() throws SQLException {
        Statement s = createStatement();
        s.execute("create function f(x varchar(10)) returns int "
                + "deterministic language java parameter style java "
                + "external name 'java.lang.Integer.parseInt' no sql");
        s.execute("create table t1(x varchar(10))");
        s.execute("create table t2(x varchar(10))");
        s.execute("create trigger tr after insert on t1 "
                + "referencing new as new for each row "
                + "when (f(new.x) < 100) insert into t2 values new.x");

        // Insert a value that causes Integer.parseInt() to throw a
        // NumberFormatException. The NFE will be wrapped in two SQLExceptions.
        assertStatementError(new String[] {USER_EXCEPTION, JAVA_EXCEPTION}, s,
                "insert into t1 values '1', '2', 'hello', '3', '121'");

        // The statement should be rolled back, so nothing should be in
        // either of the tables.
        assertTableRowCount("T1", 0);
        assertTableRowCount("T2", 0);

        // Now try again with values that don't cause exceptions.
        assertUpdateCount(s, 4, "insert into t1 values '1', '2', '3', '121'");

        // Verify that the trigger fired this time.
        JDBC.assertFullResultSet(s.executeQuery("select * from t2 order by x"),
                                 new String[][] {{"1"}, {"2"}, {"3"}});
    }

    /**
     * Test that scalar subqueries are allowed, and that non-scalar subqueries
     * result in exceptions when the trigger fires.
     */
    public void testScalarSubquery() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(x int)");
        s.execute("create table t3(x int)");

        s.execute("insert into t3 values 0,1,2,2");

        s.execute("create trigger tr1 after insert on t1 "
                + "referencing new as new for each row "
                + "when ((select x > 0 from t3 where x = new.x)) "
                + "insert into t2 values 1");

        // Subquery returns no rows, so the trigger should not fire.
        s.execute("insert into t1 values 42");
        assertTableRowCount("T2", 0);

        // Subquery returns a single value, which is false, so the trigger
        // should not fire.
        s.execute("insert into t1 values 0");
        assertTableRowCount("T2", 0);

        // Subquery returns a single value, which is true, so the trigger
        // should fire.
        s.execute("insert into t1 values 1");
        assertTableRowCount("T2", 1);

        // Subquery returns multiple values, so an error should be raised.
        assertStatementError(NON_SCALAR_QUERY, s, "insert into t1 values 2");
        assertTableRowCount("T2", 1);
    }

    /**
     * Test that a WHEN clause can call the CURRENT_USER function.
     */
    public void testCurrentUser() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(x varchar(10))");

        // Create one trigger that should only fire when current user is U2,
        // and one that should only fire when current user is different from
        // U2.
        s.execute("create trigger tr01 after insert on t1 "
                + "when (current_user = 'U2') "
                + "insert into t2 values 'TR01'");
        s.execute("create trigger tr02 after insert on t1 "
                + "when (current_user <> 'U2') "
                + "insert into t2 values 'TR02'");
        s.execute("grant insert on t1 to u2");

        commit();

        // Used to get an assert failure or a NullPointerException here before
        // DERBY-6348. Expect it to succeed, and expect TR02 to have fired.
        s.execute("insert into t1 values 1");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t2"), "TR02");

        rollback();

        // Now try the same insert as user U2.
        Connection c2 = openUserConnection("u2");
        c2.setAutoCommit(true);
        Statement s2 = c2.createStatement();
        s2.execute("insert into "
            + JDBC.escape(TestConfiguration.getCurrent().getUserName(), "T1")
            + " values 1");
        s2.close();
        c2.close();

        // Since the insert was performed by user U2, expect TR01 to have fired.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t2"), "TR01");

        // Cleanup.
        dropTable("T1");
        dropTable("T2");
        commit();
    }

    /**
     * Test that a trigger with a WHEN clause can be recursive.
     */
    public void testRecursiveTrigger() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t(x int)");
        s.execute("create trigger tr1 after insert on t "
                + "referencing new as new for each row "
                + "when (new.x > 0) insert into t values new.x - 1");

        // Now fire the trigger. This used to cause an assert failure or a
        // NullPointerException before DERBY-6348.
        s.execute("insert into t values 15, 1, 2");

        // The row trigger will fire three times, so that the above statement
        // will insert the values { 15, 14, 13, ... , 0 }, { 1, 0 } and
        // { 2, 1, 0 }.
        String[][] expectedRows = {
            {"0"}, {"0"}, {"0"}, {"1"}, {"1"}, {"1"}, {"2"}, {"2"}, {"3"},
            {"4"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"}, {"10"}, {"11"},
            {"12"}, {"13"}, {"14"}, {"15"}
        };

        JDBC.assertFullResultSet(s.executeQuery("select * from t order by x"),
                                 expectedRows);

        // Now fire the trigger with a value so that the maximum trigger
        // recursion depth (16) is exceeded, and verify that we get the
        // expected error.
        assertStatementError(TRIGGER_RECURSION, s, "insert into t values 16");

        // The contents of the table should not have changed, since the
        // above statement failed and was rolled back.
        JDBC.assertFullResultSet(s.executeQuery("select * from t order by x"),
                                 expectedRows);
    }

    /**
     * The WHEN clause text is stored in a LONG VARCHAR column in the
     * SYS.SYSTRIGGERS table. This test case verifies that the WHEN clause
     * is not limited to the usual LONG VARCHAR maximum length (32700
     * characters).
     */
    public void testVeryLongWhenClause() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(x int)");

        // Construct a WHEN clause that is more than 32700 characters.
        StringBuilder sb = new StringBuilder("(values /* a very");
        for (int i = 0; i < 10000; i++) {
            sb.append(", very");
        }
        sb.append(" long comment */ true)");

        String when = sb.toString();
        assertTrue(when.length() > 32700);

        s.execute("create trigger very_long_trigger after insert on t1 "
                + "when (" + when + ") insert into t2 values 1");

        // Verify that the WHEN clause was stored in SYS.SYSTRIGGERS.
        JDBC.assertSingleValueResultSet(
            s.executeQuery("select whenclausetext from sys.systriggers "
                         + "where triggername = 'VERY_LONG_TRIGGER'"),
            when);

        // Verify that the trigger fires.
        s.execute("insert into t1 values 1");
        assertTableRowCount("T1", 1);
        assertTableRowCount("T2", 1);
    }

    /**
     * Test a WHEN clause that invokes a function declared with READ SQL DATA.
     */
    public void testFunctionReadsSQLData() throws SQLException {
        Statement s = createStatement();
        s.execute("create function f(x varchar(10)) returns boolean "
                + "language java parameter style java external name '"
                + getClass().getName() + ".tableIsEmpty' reads sql data");

        s.execute("create table t1(x varchar(10))");
        s.execute("create table t2(x varchar(10))");
        s.execute("create table t3(x int)");
        s.execute("create table t4(x int)");
        s.execute("insert into t3 values 1");

        s.execute("create trigger tr after insert on t1 "
                + "referencing new as new for each row "
                + "when (f(new.x)) insert into t2 values new.x");

        s.execute("insert into t1 values 'T3', 'T4', 'T3', 'T4', 'T3', 'T4'");

        JDBC.assertFullResultSet(
                s.executeQuery("select x, count(x) from t2 group by x"),
                new String[][] {{"T4", "3"}});
    }

    /**
     * Stored function used by {@link #testFunctionReadsSQLData()}. It
     * checks whether the given table is empty.
     *
     * @param table the table to check
     * @return {@code true} if the table is empty, {@code false} otherwise
     */
    public static boolean tableIsEmpty(String table) throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from " + JDBC.escape(table));
        boolean empty = !rs.next();

        rs.close();
        s.close();
        c.close();

        return empty;
    }

    /**
     * <p>
     * SQL:2011, part 2, 11.49 &lt;trigger definition&gt;, syntax rule 11
     * says that the WHEN clause shall not contain routines that possibly
     * modifies SQL data. Derby does not currently allow functions to be
     * declared as MODIFIES SQL DATA. It does allow procedures to be declared
     * as MODIFIES SQL DATA, but the current grammar does not allow procedures
     * to be invoked from a WHEN clause. So there's currently no way to
     * invoke routines that possibly modifies SQL data from a WHEN clause.
     * </p>
     *
     * <p>
     * This test case verifies that it is not possible to declare a function
     * as MODIFIES SQL DATA, and that it is not possible to call a procedure
     * from a WHEN clause. If support for any of those features is added,
     * this test case will start failing as a reminder that code must be
     * added to prevent routines that possibly modifies SQL data from being
     * invoked from a WHEN clause.
     * </p>
     */
    public void testRoutineModifiesSQLData() throws SQLException {
        // Functions cannot be declared as MODIFIES SQL DATA currently.
        // Expect a syntax error.
        assertCompileError(SYNTAX_ERROR,
            "create function f(x int) returns int language java "
            + "parameter style java external name 'java.lang.Math.abs' "
            + "modifies sql data");

        // Declare a procedure as MODIFIES SQL DATA.
        Statement s = createStatement();
        s.execute("create procedure p(i int) language java "
                + "parameter style java external name '"
                + getClass().getName() + ".intProcedure' no sql");

        // Try to call that procedure from a WHEN clause. Expect it to fail
        // because procedure invocations aren't allowed in a WHEN clause.
        s.execute("create table t(x int)");
        assertCompileError(SYNTAX_ERROR,
            "create trigger tr after insert on t when (call p(1)) values 1");
        assertCompileError(PROC_USED_AS_FUNC,
            "create trigger tr after insert on t when (p(1)) values 1");
    }

    /**
     * Verify that aggregates (both built-in and user-defined) can be used
     * in a WHEN clause.
     */
    public void testAggregates() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(y varchar(10))");
        s.execute("create derby aggregate mode_int for int "
                + "external name '" + ModeAggregate.class.getName() + "'");

        s.execute("create trigger tr1 after insert on t1 "
                + "referencing new table as new "
                + "when ((select max(x) from new) between 0 and 3) "
                + "insert into t2 values 'tr1'");

        s.execute("create trigger tr2 after insert on t1 "
                + "referencing new table as new "
                + "when ((select count(x) from new) between 0 and 3) "
                + "insert into t2 values 'tr2'");

        s.execute("create trigger tr3 after insert on t1 "
                + "referencing new table as new "
                + "when ((select mode_int(x) from new) between 0 and 3) "
                + "insert into t2 values 'tr3'");

        s.execute("insert into t1 values 2, 4, 4");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t2 order by y"),
                "tr2");

        s.execute("delete from t2");

        s.execute("insert into t1 values 2, 2, 3, 1, 0");
        JDBC.assertFullResultSet(
                s.executeQuery("select * from t2 order by y"),
                new String[][] {{"tr1"}, {"tr3"}});
    }
}
