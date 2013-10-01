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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
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

    private static final String REFERENCES_SESSION_SCHEMA = "XCL51";

    public TriggerWhenClauseTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(TriggerWhenClauseTest.class);
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
            });
    }

    /**
     * A row trigger whose WHEN clause contains a subquery, could cause a
     * NullPointerException. This test case is disabled until the bug is fixed.
     */
    public void xtestSubqueryInWhenClauseNPE() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(x int)");
        s.execute("create trigger tr1 after insert on t1 for each row "
                + "when ((values true)) insert into t2 values 1");

        // This statement results in a NullPointerException.
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
     * Test that CREATE TRIGGER fails if the WHEN clause references a table
     * in the SESSION schema.
     */
    public void testSessionSchema() throws SQLException {
        Statement s = createStatement();
        s.execute("declare global temporary table temptable (x int) "
                + "not logged");
        s.execute("create table t1(x int)");
        s.execute("create table t2(x int)");

        assertCompileError(REFERENCES_SESSION_SCHEMA,
                "create trigger tr1 after insert on t1 "
                + "when (exists (select * from session.temptable)) "
                + "insert into t2 values 1");
    }
}
