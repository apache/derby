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

import javax.sql.DataSource;

import junit.framework.Test;

/**
 * Shutdown and drop the database identified by the logical
 * name passed in when creating this decorator.
 *
 */
class DropDatabaseSetup extends BaseTestSetup {

    final String logicalDBName;
    DropDatabaseSetup(Test test, String logicalDBName) {
        super(test);
        this.logicalDBName = logicalDBName;
     }
    
    /**
     * Shutdown the database and then remove all of its files.
     */
    protected void tearDown() throws Exception {
        
        TestConfiguration config = TestConfiguration.getCurrent();
        
        // Ensure the database is booted
        // since that is what shutdownDatabase() requires.
        boolean shutdown;
        try {
            config.openConnection(logicalDBName).close();
            shutdown = true;
        } catch (SQLException e) {
            // If the database cannot be booted due
            // to some restrictions such as authentication
            // or encrypted (ie here we don't know the 
            // correct authentication tokens, then it's
            // ok since we just want it shutdown anyway!
            if ("XJ040".equals(e.getSQLState()))
            {
                shutdown = false;
            }
            else
            {
                throw e;
            }
        }
        if (shutdown)
        {
            DataSource ds = JDBCDataSource.getDataSourceLogical(logicalDBName);
            JDBCDataSource.shutdownDatabase(ds);
        }

        removeDatabase();
    }

    void removeDatabase()
    {
        TestConfiguration config = TestConfiguration.getCurrent();
        String dbName = config.getPhysicalDatabaseName(logicalDBName);
        dbName = dbName.replace('/', File.separatorChar);
        String dsh = BaseTestCase.getSystemProperty("derby.system.home");
        if (dsh == null) {
            fail("not implemented");
        } else {
            dbName = dsh + File.separator + dbName;
        }
        removeDirectory(dbName);
    }


    static void removeDirectory(String path)
    {
        final File dir = new File(path);
        AccessController.doPrivileged(new java.security.PrivilegedAction() {

            public Object run() {
                removeDir(dir);
                return null;
            }
        });
        
    }

    private static void removeDir(File dir) {
        
        // Check if anything to do!
        // Database may not have been created.
        if (!dir.exists())
            return;

        String[] list = dir.list();

        // Some JVMs return null for File.list() when the
        // directory is empty.
        if (list != null) {
            for (int i = 0; i < list.length; i++) {
                File entry = new File(dir, list[i]);

                if (entry.isDirectory()) {
                    removeDir(entry);
                } else {
                    assertTrue(entry.getPath(), entry.delete());
                }
            }
        }

        assertTrue(dir.getPath(), dir.delete());
    }
}
