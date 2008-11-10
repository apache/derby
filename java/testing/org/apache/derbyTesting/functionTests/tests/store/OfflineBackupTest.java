/*

Derby - Class org.apache.derbyTesting.functionTests.store.OfflineBackupTest

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;


public class OfflineBackupTest extends BaseJDBCTestCase {


    public OfflineBackupTest(String name) {
        super(name);
    }

    public void testCreateFromRestoreFrom() throws SQLException, IOException {
        getConnection();
        TestConfiguration.getCurrent().shutdownDatabase();
        File origdbloc = new File("system","wombat");
        File backupdbloc = new File("system","wombatbackup");
        PrivilegedFileOpsForTests.copy(origdbloc, backupdbloc);
        Connection connCreateFrom = DriverManager.getConnection(
            "jdbc:derby:wombatCreateFrom;createFrom=system/wombatbackup");
        checkAllConsistency(connCreateFrom);
        try {
            DriverManager.getConnection("jdbc:derby:wombatCreateFrom;shutdown=true");
        } catch (SQLException se) {
            assertSQLState("Database shutdown", "08006", se);
        }
        Connection connRestoreFrom = DriverManager.getConnection(
            "jdbc:derby:wombatRestoreFrom;restoreFrom=system/wombatbackup");
        checkAllConsistency(connRestoreFrom);
        try {
            DriverManager.getConnection("jdbc:derby:wombatRestoreFrom;shutdown=true");
        } catch (SQLException se) {
            assertSQLState("Database shutdown", "08006", se);
        }

        removeDirectory(backupdbloc);
        removeDirectory(new File("system","wombatCreateFrom"));
        removeDirectory(new File("system","wombatRestoreFrom"));
        
    }
    
  

    public static Test suite() {
        
        if (JDBC.vmSupportsJSR169())
            return new TestSuite("Empty OfflineBackupTest (uses DriverManager)");
        return TestConfiguration.embeddedSuite(OfflineBackupTest.class);        
    }


}
