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

    private static  final   String  BAD_SYNTAX = "42X01";

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

    /**
     * Test that the DETERMINISTIC keyword is not allowed until you
     * hard-upgrade to 10.5.
     *
     */
    public void testDeterminismKeyword() throws SQLException
    {
        String  sqlstate = null;
        
        switch (getPhase())
        {
        case PH_SOFT_UPGRADE:
            sqlstate = SQLSTATE_NEED_UPGRADE;
            break;
            
        case PH_POST_SOFT_UPGRADE:
            sqlstate = BAD_SYNTAX;
            break;

        case PH_HARD_UPGRADE:
            sqlstate = null;
            break;

        default:
            return;
        }
        
        possibleError
            (
             sqlstate,
             "create function f_3570_12()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "deterministic\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        possibleError
            (
             sqlstate,
             "create procedure p_3570_13()\n" +
             "language java\n" +
             "not deterministic\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
    }

    /**
     * <p>
     * Run a statement. If the sqlstate is not null, then we expect that error.
     * </p>
     */
    private void    possibleError( String sqlstate, String text )
        throws SQLException
    {
        if ( sqlstate != null )
        {
            assertCompileError( sqlstate, text );
        }
        else
        {
            Statement   s = createStatement();
            s.execute( text );
            s.close();
        }
    }

}
