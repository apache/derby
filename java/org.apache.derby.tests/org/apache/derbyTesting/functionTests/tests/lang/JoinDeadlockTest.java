/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.JoinDeadlockTest
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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;


public final class JoinDeadlockTest extends  BaseJDBCTestCase {
    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name test name
     */
    public JoinDeadlockTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        final Properties systemProperties = new Properties();
        systemProperties.setProperty("derby.locks.waitTimeout", "7");
        systemProperties.setProperty("derby.locks.deadlockTimeout", "5");

        return new SystemPropertyTestSetup(
                TestConfiguration.defaultSuite(JoinDeadlockTest.class),
                systemProperties, 
                true);
    }

    public void testJoinDeadlock() throws Exception
    {
        Statement st = createStatement();
        
        try {
            Connection c2 = openDefaultConnection();
            setAutoCommit(false);
            c2.setAutoCommit(false);
            
            // user 1 for bug 1573
            // a deadlock when reopening a join gets an assertion
            // violation in close()
            st.executeUpdate("create table outer1(c1 int)");
            st.executeUpdate("create index o1_i1 on outer1(c1)");
            st.executeUpdate("insert into outer1 (c1) values 1, 2");
            commit();
            st.executeUpdate("create table inner1(c1 int, c2 char(254))");
            st.executeUpdate("create index i1_i1 on inner1(c1)");
            st.executeUpdate("insert into inner1 (c1) values 1, 2");
            commit();
            st.executeUpdate("create table inner2(c1 int, c2 char(254))");
            st.executeUpdate("create index i2_i1 on inner2(c1)");
            st.executeUpdate("insert into inner2 (c1) values 1, 2");
            commit();
            
            // this user will get lock timeout in subquery on 2nd next
            ResultSet c1_rs = st.executeQuery(
                    "select * from outer1 where c1 <= (select count(*) " + 
                            "from inner1, inner2 where outer1.c1 = outer1.c1)");
            c1_rs.next();
            assertEquals(c1_rs.getString(1), "1");
            
            Statement c2_st = c2.createStatement();
            c2_st.executeUpdate("update inner1 set c1 = c1 where c1 = 1");
            
            try {
                c1_rs.next();
            } catch (SQLException e) {
                assertSQLState("40XL1", e);
            }

            c2.rollback();
            c2.close();
        } finally {
            // cleanup
            dontThrow(st, "drop table outer1");
            dontThrow(st, "drop table inner1");
            dontThrow(st, "drop table inner2");
            commit();
        }
    }


    private void dontThrow(Statement st, String stm) {
        try {
            st.executeUpdate(stm);
        } catch (SQLException e) {
            // ignore, best effort here
            println("\"" + stm+ "\" failed");
        }
    }
}
