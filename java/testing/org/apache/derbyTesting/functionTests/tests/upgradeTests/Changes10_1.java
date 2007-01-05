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
import junit.framework.TestSuite;

/**
 * Upgrade test cases for changes made in 10.1.
 */
public class Changes10_1 extends UpgradeChange {
    
    public static Test suite() {
        TestSuite suite = new TestSuite("Upgrade changes for 10.1");
        
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
       boolean signaturesAllowedInOldRelease = oldAtLeast(10, 1);
        
        switch (getPhase())
        {
        case PH_CREATE:
            break;
        case PH_SOFT_UPGRADE:
        {
            Statement s = createStatement();
            try {
                s.execute(PROC_SIGNATURE);
                if (!signaturesAllowedInOldRelease)
                    fail("created procedure with signature");

            } catch (SQLException sqle) {
                if (signaturesAllowedInOldRelease)
                    fail("failed to create valid procedure");
                
                assertSQLState("XCL47", sqle);
            }
            s.close();
            break;
        }
        case PH_POST_SOFT_UPGRADE:
        {
            Statement s = createStatement();
            try {
                s.execute("CALL GC()");
                if (!signaturesAllowedInOldRelease)
                    fail("procedure was created in soft upgrade!");
                    
            } catch (SQLException sqle)
            {
                if (signaturesAllowedInOldRelease)
                    fail("procedure was created not in soft upgrade!");
            }
            s.close();
            break;
        }
        case PH_HARD_UPGRADE:
        {
            Statement s = createStatement();
            if (!signaturesAllowedInOldRelease)
                s.execute(PROC_SIGNATURE);
            s.execute("CALL GC()");
            s.close();
            break;
        }
        }
    }
}
