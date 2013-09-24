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
        s.execute("create trigger tr1 after insert on t1 for each row "
                + "when (true) insert into t2 values 'Executed tr1'");
        s.execute("create trigger tr2 after insert on t1 for each statement "
                + "when (true) insert into t2 values 'Executed tr2'");
        s.execute("create trigger tr3 after insert on t1 "
                + "when (true) insert into t2 values 'Executed tr3'");

        // Create corresponding triggers that should never fire (their WHEN
        // clause is false).
        s.execute("create trigger tr4 after insert on t1 for each row "
                + "when (false) insert into t2 values 'Executed tr4'");
        s.execute("create trigger tr5 after insert on t1 for each statement "
                + "when (false) insert into t2 values 'Executed tr5'");
        s.execute("create trigger tr6 after insert on t1 "
                + "when (false) insert into t2 values 'Executed tr6'");

        // Create triggers with EXISTS subqueries in the WHEN clause. The
        // first returns TRUE and the second returns FALSE.
        s.execute("create trigger tr7 after insert on t1 "
                + "when (exists (select * from sysibm.sysdummy1)) "
                + "insert into t2 values 'Executed tr7'");
        s.execute("create trigger tr8 after insert on t1 "
                + "when (exists "
                + "(select * from sysibm.sysdummy1 where ibmreqd <> 'Y')) "
                + "insert into t2 values 'Executed tr8'");

        // WHEN clause returns NULL, trigger should not be fired.
        s.execute("create trigger tr9 after insert on t1 "
                + "when (cast(null as boolean))"
                + "insert into t2 values 'Executed tr9'");

        // Now fire the triggers and verify the results.
        assertUpdateCount(s, 3, "insert into t1 values 1, 2, 3");
        JDBC.assertFullResultSet(
            s.executeQuery("select y, count(*) from t2 group by y order by y"),
            new String[][] {
                { "Executed tr1", "3" },
                { "Executed tr2", "1" },
                { "Executed tr3", "1" },
                { "Executed tr7", "1" },
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
