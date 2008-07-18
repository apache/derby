/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_5

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

import org.apache.derbyTesting.junit.SupportFilesSetup;

import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Upgrade test cases for 10.5.
 * If the old version is 10.5 or later then these tests
 * will not be run.
 * <BR>
    10.5 Upgrade issues

    <UL>
    <LI> testUpdateStatisticsProcdure - DERBY-269
    Make sure that SYSCS_UTIL.SYSCS_UPDATE_STATISTICS can only be run in Derby
    10.5 and higher.
    </UL>

 */
public class Changes10_5 extends UpgradeChange {

    public Changes10_5(String name) {
        super(name);
    }

    /**
     * Return the suite of tests to test the changes made in 10.5.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.5");

        suite.addTestSuite(Changes10_5.class);
        return new SupportFilesSetup((Test) suite);
    }

    /**
     * Make sure that SYSCS_UTIL.SYSCS_UPDATE_STATISTICS can only be run in 
     * Derby 10.5 and higher. 
     * DERBY-269
     * Test added for 10.5.
     * @throws SQLException
     *
     */
    public void testUpdateStatisticsProcdure() throws SQLException
    {
    	Statement s;
        switch (getPhase())
        {
        case PH_CREATE:
            s = createStatement();
            s.execute("CREATE TABLE DERBY_269(c11 int, c12 char(20))");
            s.execute("INSERT INTO DERBY_269 VALUES(1, 'DERBY-269')");
            s.execute("CREATE INDEX I1 ON DERBY_269(c12)");
            s.close();
            break;

        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
            // new update statistics procedure should not be found
            // on soft-upgrade.
            s = createStatement();
            assertStatementError("42Y03", s,
                    "call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                    "('APP', 'DERBY_269', null)");
            assertStatementError("42Y03", s,
                    "call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                    "('APP', 'DERBY_269', 'I1')");
            s.close();
            break;

        case PH_HARD_UPGRADE:
        	//We are at Derby 10.5 release and hence should find the
        	//update statistics procedure
            s = createStatement();
            s.execute("call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
            		"('APP', 'DERBY_269', null)");
            s.execute("call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
            		"('APP', 'DERBY_269', 'I1')");
            s.close();
            break;
        }
    }
}
