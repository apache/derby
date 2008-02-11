/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.PhaseChanger

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

import java.security.AccessController;
import java.sql.SQLException;

import javax.sql.DataSource;

import junit.extensions.TestSetup;
import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseTestSetup;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Decorator that sets the phase of the upgrade process
 * for a suite of upgrade tests.
 */
final class PhaseChanger extends BaseTestSetup {

    private final int phase;
    private final int[] version;
    private ClassLoader loader;
    private ClassLoader previousLoader;
    
    public PhaseChanger(Test test, int phase,
            ClassLoader loader, int[] version) {
        super(test);
        this.phase = phase;
        this.loader = loader;
        this.version = version;
    }
    
    /**
     * Set the phase and boot the database, creating it
     * or upgrading it as required. The thread context
     * class loader is changed to point to the old
     * jar files if required for the phase.
     */
    protected void setUp() throws SQLException
    {
        UpgradeChange.phase.set(new Integer(phase));
        UpgradeChange.oldVersion.set(version);
        
        
        if (loader != null) {
            previousLoader = Thread.currentThread().getContextClassLoader();
            setThreadLoader(loader);
        }
         
        DataSource ds = JDBCDataSource.getDataSource();
        switch (phase)
        {
        case UpgradeChange.PH_POST_HARD_UPGRADE:
            // Post hard upgrade is expected to fail
            // since the database cannot be accessed
            // by the old version anymore. This will
            // be explictly tested eslewhere rather than
            // in a setup method.
            return;
            
        case UpgradeChange.PH_CREATE:
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
            break;
            
        case UpgradeChange.PH_HARD_UPGRADE:
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "false");
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                    "upgrade=true");
            break;
        default:
            break;
        }
        
        // Ensure the database exists or upgrade it.
        ds.getConnection().close();

    }
    
    /**
     * Shutdown the database(s) and reset the class loader.
     * @throws InterruptedException 
     */
    protected void tearDown() throws InterruptedException
    {
        if (phase != UpgradeChange.PH_POST_HARD_UPGRADE) {
            DataSource ds = JDBCDataSource.getDataSource();
            JDBCDataSource.shutdownDatabase(ds);

            for (int i = 0; i < UpgradeRun.ADDITIONAL_DBS.length; i++)
            {
                ds = JDBCDataSource.getDataSourceLogical(
                    UpgradeRun.ADDITIONAL_DBS[i].logicalName);

                if (UpgradeRun.ADDITIONAL_DBS[i].shutDown) {
                    boolean shutdown = true;
                    try {
                        ds.getConnection().close();
                    } catch (SQLException e) {
                        // if the database was never created
                        // don't bother shutting it down
                        String sqlState = e.getSQLState();
                        if ("XJ004".equals(sqlState) ||
                                "XJ040".equals(sqlState)) {
                            shutdown = false;
                        }
                    }

                    if (shutdown)
                        JDBCDataSource.shutdownDatabase(ds);
                } // else done by test
            }
        }
        
       
        if (loader != null)
            setThreadLoader(previousLoader);       
        loader = null;
        previousLoader = null;
        
        UpgradeChange.phase.set(null);
        UpgradeChange.oldVersion.set(null);
    }
    
    private void setThreadLoader(final ClassLoader which) {

        AccessController.doPrivileged
        (new java.security.PrivilegedAction(){
            
            public Object run()  { 
                java.lang.Thread.currentThread().setContextClassLoader(which);
              return null;
            }
        });
    }
    
    private ClassLoader getThreadLoader() {

        return (ClassLoader) AccessController.doPrivileged
        (new java.security.PrivilegedAction(){
            
            public Object run()  { 
                return Thread.currentThread().getContextClassLoader();
            }
        });
    }
}
