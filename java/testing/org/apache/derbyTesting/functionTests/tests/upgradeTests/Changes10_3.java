/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_3

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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Upgrade test cases for changes made in 10.3.
 * <BR>
 * 10.3 Upgrade issues
 */
public class Changes10_3 extends UpgradeChange {
    
    public static Test suite() {
        TestSuite suite = new TestSuite("Upgrade changes for 10.3");
        
        suite.addTestSuite(Changes10_3.class);
        
        return suite;
    }

    public Changes10_3(String name) {
        super(name);
    }
    

    
    /**
     * Verify the compilation schema is nullable after upgrade to 10.3
     * or later. (See DERBY-630)
     * @throws SQLException
     */
    public void testCompilationSchema() throws SQLException
    {        
       switch (getPhase())
        {
            case PH_CREATE:
            case PH_POST_SOFT_UPGRADE:
            case PH_POST_HARD_UPGRADE:
                // 10.0-10.2 inclusive had the system schema incorrect.
                if (!oldAtLeast(10, 3))
                    return;
                break;
        }

        DatabaseMetaData dmd = getConnection().getMetaData();

        ResultSet rs = dmd.getColumns(null, "SYS", "SYSSTATEMENTS", "COMPILATIONSCHEMAID");
        rs.next();
        assertEquals("SYS.SYSSTATEMENTS.COMPILATIONSCHEMAID IS_NULLABLE",
                        "YES", rs.getString("IS_NULLABLE"));
        rs.close();

        rs = dmd.getColumns(null, "SYS", "SYSVIEWS", "COMPILATIONSCHEMAID");
        rs.next();
        assertEquals("SYS.SYSVIEWS.COMPILATIONSCHEMAID IS_NULLABLE",
                        "YES", rs.getString("IS_NULLABLE"));
    }
}
