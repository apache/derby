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
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for the WHEN clause in CREATE TRIGGER statements, added in DERBY-534.
 */
public class TriggerWhenClauseTest extends BaseJDBCTestCase {

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

}
