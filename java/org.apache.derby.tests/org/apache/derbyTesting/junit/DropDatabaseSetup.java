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
import java.sql.SQLException;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;

/**
 * Shutdown and drop the database identified by the logical
 * name passed in when creating this decorator.
 *
 */
class DropDatabaseSetup extends BaseTestSetup {

    private final String logicalDBName;
    private final boolean shutdownBeforeDrop;

    DropDatabaseSetup(Test test, String logicalDBName) {
        this(test, logicalDBName, true);
    }

    DropDatabaseSetup(Test test, String logicalDBName, boolean shutdown) {
        super(test);
        this.logicalDBName = logicalDBName;
        this.shutdownBeforeDrop = shutdown;
    }
    
    /**
     * Shutdown the database and then remove all of its files.
     */
    @Override
    protected void tearDown() throws Exception {
        if (shutdownBeforeDrop) {
            shutdownDatabase();
        }

        removeDatabase();
    }

    private void shutdownDatabase() throws SQLException {

        TestConfiguration config = TestConfiguration.getCurrent();
        
        // Ensure the database is booted
        // since that is what shutdownDatabase() requires.
        boolean shutdown;
        try {
            config.openConnection(logicalDBName).close();
            shutdown = true;
        } catch (SQLException e) {
            String  sqlState = e.getSQLState();
            // If the database cannot be booted due
            // to some restrictions such as authentication
            // or encrypted (ie here we don't know the 
            // correct authentication tokens, then it's
            // ok since we just want it shutdown anyway!
            if ( "XJ040".equals( sqlState ) || "08004".equals( sqlState ) || "4251I".equals( sqlState ) )
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
        //DERBY-5995 (Add a test case to check the 3 readme files get created 
        // even when log directory has been changed with jdbc url attribute 
        // logDevice )
        String logDevice = config.getConnectionAttributes().getProperty("logDevice");
        if (logDevice != null) {
            removeDirectory(logDevice);
        }
    }


    static void removeDirectory(String path)
    {
        final File dir = new File(path);
        removeDirectory(dir);
    }
    
    static void removeDirectory(final File dir) {
        // Check if anything to do!
        // Database may not have been created.
        if (!PrivilegedFileOpsForTests.exists(dir)) {
            return;
        }

        BaseTestCase.assertDirectoryDeleted(dir);
    }

    /**
     * Remove all the files in the list
     * @param list the list of files that will be deleted
     **/
    static void removeFiles(String[] list) {
        for (int i = 0; i < list.length; i++) {
             try {
                 File dfile = new File(list[i].toString());            
                 assertTrue(list[i].toString(), dfile.delete());
             } catch (IllegalArgumentException e) {
                 fail("open file error");
             }
        }
    }
}
