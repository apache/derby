/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_1

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

/**
 * Upgrade test cases for changes made in 10.1.
 * If the old version is 10.1 or later then these tests
 * will not be run.
 * <BR>
    10.1 Upgrade issues

    <UL>
    <LI> testProcedureSignature - Routines with explicit Java signatures.
    </UL>

 */
public class Changes10_1 extends UpgradeChange {
    
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("Upgrade changes for 10.1");
        
        suite.addTestSuite(Changes10_1.class);
        
        return suite;
    }

    public Changes10_1(String name) {
        super(name);
    }
    
    /**
     * A CREATE PROCEDURE with a signature (of no arguments).
     */
    private static final String PROC_SIGNATURE =
        "CREATE PROCEDURE GC() " +
        "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME" +
        " 'java.lang.System.gc()'";
    
    /**
     * Check that a procedure with a signature can not be added if the
     * on-disk database version is 10.0.
     * Test added by 10.1.
     * @throws SQLException 
     *
     */
    public void testProcedureSignature() throws SQLException
    {      
       Statement s = createStatement();
        switch (getPhase())
        {
        case PH_CREATE:
        case PH_POST_SOFT_UPGRADE:
            break;
        case PH_SOFT_UPGRADE:
            assertStatementError(SQLSTATE_NEED_UPGRADE, s,
                    PROC_SIGNATURE);
            break;

        case PH_HARD_UPGRADE:
            s.execute(PROC_SIGNATURE);
            s.execute("CALL GC()");
            break;
        }
        s.close();
    }
}
