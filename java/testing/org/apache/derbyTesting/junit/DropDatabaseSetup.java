/*
 *
 * Derby - Class org.apache.derbyTesting.junit.DropDatabaseSetup
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.io.File;
import java.security.AccessController;
import java.sql.SQLException;

import junit.extensions.TestSetup;
import junit.framework.Test;

/**
 * Shutdown and drop the current database at tearDown time.
 *
 */
class DropDatabaseSetup extends TestSetup {

    DropDatabaseSetup(Test test) {
        super(test);
     }
    
    /**
     * Drop the current database.
     */
    protected void tearDown() throws Exception {  
        super.tearDown();
        
        try {
            TestConfiguration.getCurrent().getDefaultConnection(
                    "shutdown=true");
            fail("Database failed to shut down");
        } catch (SQLException e) {
            BaseJDBCTestCase.assertSQLState("Database shutdown", "08006", e);
        }

        String dbName = TestConfiguration.getCurrent().getDatabaseName();
        dbName = dbName.replace('/', File.separatorChar);
        
        String dsh = BaseTestCase.getSystemProperty("derby.system.home");
        if (dsh == null)
            fail("not implemented");
        else
            dbName = dsh + File.separator + dbName;
        
        final File dbDir = new File(dbName);
        AccessController.doPrivileged(new java.security.PrivilegedAction() {

            public Object run() {
                removeDBDir(dbDir);
                return null;
            }
        });
    } 

    private static void removeDBDir(File dbDir) {

        String[] list = dbDir.list();

        // Some JVMs return null for File.list() when the
        // directory is empty.
        if (list != null) {
            for (int i = 0; i < list.length; i++) {
                File entry = new File(dbDir, list[i]);

                if (entry.isDirectory()) {
                    removeDBDir(entry);
                } else {
                    assertTrue(entry.delete());
                }
            }
        }

        assertTrue(dbDir.delete());
    }
}
