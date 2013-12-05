/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_11

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.JDBC;


/**
 * Upgrade test cases for 10.11.
 */
public class Changes10_11 extends UpgradeChange
{

    //////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    //////////////////////////////////////////////////////////////////

    private static  final   String  SYNTAX_ERROR = "42X01";
    private static  final   String  HARD_UPGRADE_REQUIRED = "XCL47";

    //////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    //////////////////////////////////////////////////////////////////

    public Changes10_11(String name) {
        super(name);
    }

    //////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    //////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests to test the changes made in 10.11.
     *
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        return new TestSuite(Changes10_11.class, "Upgrade test for 10.11");
    }

    //////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    //////////////////////////////////////////////////////////////////

    public void testTriggerWhenClause() throws SQLException {
        String createTrigger =
                "create trigger d534_tr1 after insert on d534_t1 "
                + "referencing new as new for each row mode db2sql "
                + "when (new.x <> 2) insert into d534_t2 values new.x";

        Statement s = createStatement();
        switch (getPhase()) {
            case PH_CREATE:
                s.execute("create table d534_t1(x int)");
                s.execute("create table d534_t2(y int)");
                assertCompileError(SYNTAX_ERROR, createTrigger);
                break;
            case PH_SOFT_UPGRADE:
                assertCompileError(HARD_UPGRADE_REQUIRED, createTrigger);
                break;
            case PH_POST_SOFT_UPGRADE:
                assertCompileError(SYNTAX_ERROR, createTrigger);
                break;
            case PH_HARD_UPGRADE:
                s.execute(createTrigger);
                s.execute("insert into d534_t1 values 1, 2, 3");
                JDBC.assertFullResultSet(
                        s.executeQuery("select * from d534_t2 order by y"),
                        new String[][]{{"1"}, {"3"}});
                break;
        }
    }

    /**
     * Test how dropping trigger dependencies works across upgrade and
     * downgrade. Regression test for DERBY-2041.
     */
    public void testDropTriggerDependencies() throws SQLException {
        if (!oldAtLeast(10, 2)) {
            // Support for SYNONYMS was added in 10.1. Support for CALL
            // statements in trigger actions was added in 10.2. Since this
            // test case uses both of those features, skip it on the oldest
            // versions.
            return;
        }

        setAutoCommit(false);
        Statement s = createStatement();
        switch (getPhase()) {
            case PH_CREATE:
                // Let's create some objects to use in the triggers.
                s.execute("create table d2041_t(x int)");
                s.execute("create table d2041_table(x int)");
                s.execute("create table d2041_synonym_table(x int)");
                s.execute("create synonym d2041_synonym "
                        + "for d2041_synonym_table");
                s.execute("create view d2041_view(x) as values 1");
                s.execute("create function d2041_func(i int) returns int "
                        + "language java parameter style java "
                        + "external name 'java.lang.Math.abs' no sql");
                s.execute("create procedure d2041_proc() "
                        + "language java parameter style java "
                        + "external name 'java.lang.Thread.yield' no sql");

                // Create the triggers with the old version.
                createDerby2041Triggers(s);
                commit();
                break;
            case PH_SOFT_UPGRADE:
                // Drop the trigger dependencies. Since the triggers were
                // created with the old version, the dependencies were not
                // registered, so expect the DROP operations to succeed.
                dropDerby2041TriggerDeps(s, false);

                // The triggers still exist, so it is possible to drop them.
                dropDerby2041Triggers(s);

                // We want to use the objects further, so roll back the
                // DROP operations.
                rollback();

                // Recreate the triggers with the new version.
                dropDerby2041Triggers(s);
                createDerby2041Triggers(s);
                commit();

                // Dropping the dependencies now should fail.
                dropDerby2041TriggerDeps(s, true);
                break;
            case PH_POST_SOFT_UPGRADE:
                // After downgrade, the behaviour isn't quite consistent. The
                // dependencies were registered when the triggers were created
                // with the new version, but the old versions only have code
                // to detect some of the dependencies. So some will fail and
                // others will succeed.

                // Dependencies on tables and synonyms are detected.
                assertStatementError("X0Y25", s, "drop table d2041_table");
                assertStatementError("X0Y25", s, "drop synonym d2041_synonym");

                // Dependencies on views, functions and procedures are not
                // detected.
                s.execute("drop view d2041_view");
                s.execute("drop function d2041_func");
                s.execute("drop procedure d2041_proc");

                // Restore the database state.
                rollback();
                break;
            case PH_HARD_UPGRADE:
                // In hard upgrade, we should be able to detect the
                // dependencies registered when the triggers were created
                // in the soft-upgraded database.
                dropDerby2041TriggerDeps(s, true);
        }
    }

    private void createDerby2041Triggers(Statement s) throws SQLException {
        s.execute("create trigger d2041_tr1 after insert on d2041_t "
                + "for each row mode db2sql insert into d2041_table values 1");
        s.execute("create trigger d2041_tr2 after insert on d2041_t "
                + "for each row mode db2sql "
                + "insert into d2041_synonym values 1");
        s.execute("create trigger d2041_tr3 after insert on d2041_t "
                + "for each row mode db2sql select * from d2041_view");
        s.execute("create trigger d2041_tr4 after insert on d2041_t "
                + "for each row mode db2sql values d2041_func(1)");
        s.execute("create trigger d2041_tr5 after insert on d2041_t "
                + "for each row mode db2sql call d2041_proc()");
    }

    private void dropDerby2041Triggers(Statement s) throws SQLException {
        for (int i = 1; i <= 5; i++) {
            s.execute("drop trigger d2041_tr" + i);
        }
    }

    private void dropDerby2041TriggerDeps(Statement s, boolean expectFailure)
            throws SQLException {
        String[] stmts = {
            "drop table d2041_table",
            "drop synonym d2041_synonym",
            "drop view d2041_view",
            "drop function d2041_func",
            "drop procedure d2041_proc",
        };

        for (String stmt : stmts) {
            if (expectFailure) {
                assertStatementError("X0Y25", s, stmt);
            } else {
                assertUpdateCount(s, 0, stmt);
            }
        }
    }
}
