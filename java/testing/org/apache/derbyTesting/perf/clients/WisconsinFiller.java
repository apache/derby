/*

Derby - Class org.apache.derbyTesting.perf.clients.WisconsinFiller

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

package org.apache.derbyTesting.perf.clients;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import org.apache.derbyTesting.functionTests.tests.lang.wisconsin;

/**
 * Class which creates and populates the tables used by
 * {@code IndexJoinClient}. These are the same tables as the ones used by the
 * functional Wisconsin test found in the lang suite.
 */
public class WisconsinFiller implements DBFiller {

    public void fill(Connection c) throws SQLException {
        c.setAutoCommit(false);

        dropTable(c, "TENKTUP1");
        dropTable(c, "TENKTUP2");
        dropTable(c, "ONEKTUP");
        dropTable(c, "BPRIME");

        wisconsin.createTables(c, false);

        c.commit();
    }

    /**
     * Helper method which drops a table if it exists. Nothing happens if
     * the table doesn't exist.
     *
     * @param c the connection to use
     * @param table the table to drop
     * @throws SQLException if an unexpected database error occurs
     */
    static void dropTable(Connection c, String table) throws SQLException {
        // Create a savepoint that we can roll back to if drop table fails.
        // This is not needed by Derby, but some databases (e.g., PostgreSQL)
        // don't allow more operations in a transaction if a statement fails,
        // and we want to be able to run these tests against other databases
        // than Derby.
        Savepoint sp = c.setSavepoint();
        Statement stmt = c.createStatement();
        try {
            stmt.executeUpdate("DROP TABLE " + table);
        } catch (SQLException e) {
            // OK to fail if table doesn't exist, roll back to savepoint
            c.rollback(sp);
        }
        stmt.close();
        c.releaseSavepoint(sp);
    }
}
