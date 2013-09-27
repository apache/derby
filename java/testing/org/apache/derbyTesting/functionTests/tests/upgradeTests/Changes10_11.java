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
}
