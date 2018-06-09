/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.storetests.Derby94Test

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

package org.apache.derbyTesting.functionTests.tests.storetests;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Regression test case for DERBY-94, which prevented some locks from
 * being released at the end of the transaction if lock escalation had
 * happened.
 */
public class Derby94Test extends BaseJDBCTestCase {
    public Derby94Test(String name) {
        super(name);
    }

    public static Test suite() {
        Test test = TestConfiguration.embeddedSuite(Derby94Test.class);
        // Reduce lock escalation threshold to make it possible to test
        // with fewer rows.
        test = DatabasePropertyTestSetup.singleProperty(
                test, "derby.locks.escalationThreshold", "102");
        test = new CleanDatabaseTestSetup(test);
        return test;
    }

    public void testDerby94() throws SQLException {
        setAutoCommit(false);

        PreparedStatement locktable = prepareStatement(
                "select type, lockcount, mode, tablename, lockname, state "
                        + "from syscs_diag.lock_table "
                        + "order by tablename, type desc, mode, "
                        + "lockcount, lockname");

        Statement s = createStatement();

        s.execute("create table t1(c1 int, c2 int not null primary key)");
        s.execute("create table t2(c1 int)");

        PreparedStatement ins1 = prepareStatement(
                                        "insert into t1 values (?, ?)");
        for (int i = 0; i < 160; i++) {
            ins1.setInt(1, i);
            ins1.setInt(2, 200 + i);
            ins1.execute();
        }

        s.execute("insert into t2 values 0, 1, 2, 3, 4, 5, 6, 7, 8, 9");

        commit();

        Statement s1 = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                       ResultSet.CONCUR_UPDATABLE);
        ResultSet rs1 = s1.executeQuery("select * from t1 for update of c1");

        assertTrue(rs1.next());
        assertEquals(0, rs1.getInt("c1"));
        assertEquals(200, rs1.getInt("c2"));
        rs1.updateInt("c1", 999);
        rs1.updateRow();

        assertTrue(rs1.next());
        assertEquals(1, rs1.getInt("c1"));
        assertEquals(201, rs1.getInt("c2"));

        Statement s2 = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                       ResultSet.CONCUR_UPDATABLE);
        ResultSet rs2 = s2.executeQuery("select * from t2 for update of c1");
        assertTrue(rs2.next());
        assertEquals(0, rs2.getInt("c1"));

        JDBC.assertFullResultSet(
                locktable.executeQuery(),
                new String[][] {
                    { "TABLE", "2", "IX", "T1", "Tablelock", "GRANT" },
                    { "ROW",   "1", "U",  "T1", "(1,8)",     "GRANT" },
                    { "ROW",   "1", "X",  "T1", "(1,7)",     "GRANT" },
                    { "TABLE", "1", "IX", "T2", "Tablelock", "GRANT" },
                    { "ROW",   "1", "U",  "T2", "(1,7)",     "GRANT" },
                });

        // The following insert should get X lock on t2 because of escalation,
        // but should leave U lock on t1 as it is.
        assertUpdateCount(s, 160, "insert into t2 select c1 from t1");

        JDBC.assertFullResultSet(
                locktable.executeQuery(),
                new String[][] {
                    { "TABLE", "3", "IX", "T1", "Tablelock", "GRANT" },
                    { "ROW",   "1", "U",  "T1", "(1,8)",     "GRANT" },
                    { "ROW",   "1", "X",  "T1", "(1,7)",     "GRANT" },
                    { "TABLE", "4", "IX", "T2", "Tablelock", "GRANT" },
                    { "TABLE", "1", "X",  "T2", "Tablelock", "GRANT" },
                });

        // The following update statement should escalate the locks on t1
        // to table level X lock.
        assertUpdateCount(s, 160, "update t1 set c1 = c1 + 999");

        JDBC.assertFullResultSet(
                locktable.executeQuery(),
                new String[][] {
                    { "TABLE", "8", "IX", "T1", "Tablelock", "GRANT" },
                    { "TABLE", "1", "X",  "T1", "Tablelock", "GRANT" },
                    { "TABLE", "4", "IX", "T2", "Tablelock", "GRANT" },
                    { "TABLE", "1", "X",  "T2", "Tablelock", "GRANT" },
                });

        rs1.close();
        rs2.close();
        commit();

        // The following lock table dump should not show any locks.
        // The above commit should have release them.
        JDBC.assertEmpty(locktable.executeQuery());
    }
}
