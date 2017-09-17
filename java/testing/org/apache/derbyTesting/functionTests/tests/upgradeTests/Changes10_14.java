/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_14

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
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;


/**
 * Upgrade test cases for 10.14.
 */
public class Changes10_14 extends UpgradeChange
{

    //////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    //////////////////////////////////////////////////////////////////

    private static final String UPGRADE_REQUIRED = "XCL47";
    private static final String LANG_AI_OVERFLOW = "42Z24";

    //////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    //////////////////////////////////////////////////////////////////

    public Changes10_14(String name) {
        super(name);
    }

    //////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    //////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests to test the changes made in 10.13.
     *
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        return new BaseTestSuite(Changes10_14.class, "Upgrade test for 10.14");
    }

    //////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    //////////////////////////////////////////////////////////////////

    /**
     * Test the addition of support for changing the cycling
     * behavior of identity columns. DERBY-6904.
     */
    public void testAlterTableSetCycle() throws SQLException {
        Statement s = createStatement();

        // 10.11 upgraded all identity columns to be backed by sequences
        boolean atLeast10_11 = oldAtLeast(10,11);
        
        switch (getPhase()) {
            case PH_CREATE:
                s.execute
                  (
                   "create table t_cycle_6904_1\n" +
                   "(a int generated always as identity (start with 2147483646), b int)"
                   );
                s.execute
                  (
                   "create table t_cycle_6904_2\n" +
                   "(a int generated always as identity (start with 2147483646), b int)"
                   );
                break;

            case PH_SOFT_UPGRADE:
                // We only support the SET GENERATED clause if the database
                // is at level 10.11 or higher.
                if (atLeast10_11)
                {
                    s.execute("alter table t_cycle_6904_1 alter column a set no cycle");
                    s.execute("alter table t_cycle_6904_1 alter column a set cycle");
                    s.execute("insert into t_cycle_6904_1(b) values (1)");
                    s.execute("insert into t_cycle_6904_1(b) values (2)");
                    s.execute("insert into t_cycle_6904_1(b) values (3)");
                    JDBC.assertFullResultSet(
                        s.executeQuery("select * from t_cycle_6904_1 order by b"),
                        new String[][]
                        {
                          { "2147483646", "1" },
                          { "2147483647", "2" },
                          { "-2147483648", "3" },
                        });
                }
                else
                {
                    assertCompileError
                      (
                       UPGRADE_REQUIRED,
                       "alter table t_cycle_6904_1 alter column a set no cycle"
                       );
                    assertCompileError
                      (
                       UPGRADE_REQUIRED,
                       "alter table t_cycle_6904_1 alter column a set cycle"
                       );
                }
                break;
                
            case PH_POST_SOFT_UPGRADE:

                if (atLeast10_11)
                {
                    s.execute("insert into t_cycle_6904_1(b) values (4)");
                    JDBC.assertFullResultSet(
                        s.executeQuery("select * from t_cycle_6904_1 order by b"),
                        new String[][]
                        {
                          { "2147483646", "1" },
                          { "2147483647", "2" },
                          { "-2147483648", "3" },
                          { "-2147483647", "4" },
                        });
                }
                else
                {
                    s.execute("insert into t_cycle_6904_1(b) values (1)");
                    s.execute("insert into t_cycle_6904_1(b) values (2)");
                    assertStatementError
                      (
                       LANG_AI_OVERFLOW,
                         s, "insert into t_cycle_6904_1(b) values (3)"
                       );
                }
                break;
              
            case PH_HARD_UPGRADE:
                s.execute("alter table t_cycle_6904_2 alter column a set no cycle");
                s.execute("alter table t_cycle_6904_2 alter column a set cycle");
                s.execute("insert into t_cycle_6904_2(b) values (1)");
                s.execute("insert into t_cycle_6904_2(b) values (2)");
                s.execute("insert into t_cycle_6904_2(b) values (3)");

                JDBC.assertFullResultSet
                  (
                   s.executeQuery("select * from t_cycle_6904_2 order by b"),
                   new String[][]
                   {
                       { "2147483646", "1" },
                       { "2147483647", "2" },
                       { "-2147483648", "3" },
                   });
                break;
        };
    }

}
