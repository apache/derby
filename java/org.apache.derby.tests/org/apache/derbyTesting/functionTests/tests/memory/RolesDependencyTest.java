/**
 * Derby - Class org.apache.derbyTesting.functionTests.tests.memory.RolesDependencyTest
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test memory requirements for the activation's dependencies on the current
 * role.
 */
public class RolesDependencyTest extends BaseJDBCTestCase {

    public RolesDependencyTest(String name) {
        super(name);
    }

    public static Test suite() {
        Test test = new BaseTestSuite(RolesDependencyTest.class);
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        // Populate database with data as DBO
        test = new CleanDatabaseTestSetup(test) {
            protected void decorateSQL(Statement s) throws SQLException {
                s.execute("create schema s1");
                s.execute("create table s1.t1(x int)");
                s.execute("insert into s1.t1 values 1");
                s.execute("create role role1");
                s.execute("grant select on s1.t1 to role1");
                s.execute("grant role1 to user1");
            }
        };

        // Enable SQL authorization
        return TestConfiguration.sqlAuthorizationDecorator(test);
    }

    /**
     * Regression test case for DERBY-4571. When executing a query that
     * needed a privilege that was granted to the current role, a reference
     * to the activation would be leaked at each execution because the
     * activation's dependency on the current role was added to the dependency
     * manager and never removed. This eventually lead to OutOfMemoryErrors.
     */
    public void testCurrentRoleDependencyMemleak() throws SQLException {
        Connection c = openUserConnection("user1");
        Statement s = c.createStatement();
        s.execute("set role role1");
        for (int i = 0; i < 40000; i++) {
            ResultSet rs = s.executeQuery("select * from s1.t1");
            JDBC.assertDrainResults(rs, 1);
        }
        s.close();
        c.close();
    }
}
