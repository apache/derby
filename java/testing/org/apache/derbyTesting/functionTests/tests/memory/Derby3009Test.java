/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.memory.Derby3009Test
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.memory;

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Regression test for DERBY-3009 which caused OutOfMemoryError when creating
 * many foreign key constraints on a table. Run the test with 16 MB heap to
 * expose the problem.
 */
public class Derby3009Test extends BaseJDBCTestCase {
    public Derby3009Test(String name) {
        super(name);
    }

    public static Test suite() {
        // The OOME happened in the engine, so run this test in embedded mode.
        return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(Derby3009Test.class));
    }

    public void testTableWithManyForeignKeys() throws SQLException {
        Statement s = createStatement();

        // Create 50 tables with 50 columns each (plus primary key column).
        final int tables = 50;
        final int columns = 50;
        for (int i = 1; i <= tables; i++) {
            StringBuffer sql = new StringBuffer("create table d3009_t");
            sql.append(i);
            sql.append("(id int primary key");
            for (int j = 1; j <= columns; j++) {
                sql.append(", x").append(j).append(" int");
            }
            sql.append(")");
            s.execute(sql.toString());
        }

        // Now add many foreign key constraints to table 50. Used to cause an
        // OutOfMemoryError before DERBY-3009.
        for (int i = 1; i <= tables; i++) {
            s.execute("alter table d3009_t50 add constraint d3009_fk" + i +
                      " foreign key(x" + i + ") references d3009_t" + i);
        }
    }
}
