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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.SQLException;

import javax.sql.DataSource;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
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
    private boolean trace = false;
    private String upgradeAttributes="upgrade=true";
    
    public PhaseChanger(Test test, int phase,
            ClassLoader loader, int[] version, boolean useCreateOnUpgrade) {
        super(test);
        this.phase = phase;
        this.loader = loader;
        this.version = version;
        if (useCreateOnUpgrade) {
            upgradeAttributes += ";create=true";
        }
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
        
        TestConfiguration config = TestConfiguration.getCurrent();
        trace = config.doTrace();
        if ( trace )
        {
            String versStr = ((int[]) UpgradeChange.oldVersion.get())[0] 
                    + "." +  ((int[]) UpgradeChange.oldVersion.get())[1]
                    + "." +  ((int[]) UpgradeChange.oldVersion.get())[2]
                    + "." +  ((int[]) UpgradeChange.oldVersion.get())[3];
            BaseTestCase.traceit("Test upgrade from: " + versStr + ", phase: " 
                    + UpgradeChange.PHASES[phase]);
            if (UpgradeChange.PHASES[phase].equals("UPGRADE")) {
                BaseTestCase.traceit("Upgrade attributes = " + upgradeAttributes);
            }
        }
        
        if (loader != null) {
            previousLoader = Thread.currentThread().getContextClassLoader();
            UpgradeClassLoader.setThreadLoader(loader);
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
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                    upgradeAttributes);
            break;
        default:
            break;
        }
        
        // Ensure the database exists or upgrade it.
        ds.getConnection().close();

    }
    
    /**
     * Shutdown the database engine and reset the class loader.
     * @throws SQLException if the engine couldn't be stopped
     */
    protected void tearDown() throws Exception
    {
        if ( trace ) BaseTestCase.traceit(" Test upgrade done.");
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.shutEngine(ds);

        // When we're done with the old driver, make sure it's deregistered
        // from the DriverManager (if running on a platform that has the
        // DriverManager class). The shutEngine() should also deregister the
        // driver, but on some versions it doesn't (DERBY-2905, DERBY-5316).
        if (phase == UpgradeChange.PH_POST_HARD_UPGRADE &&
                JDBC.vmSupportsJDBC3()) {
            deregisterDriver();
        }

        // Workaround for DERBY-4895, which prevented the engine classes from
        // being garbage collected.
        if (phase == UpgradeChange.PH_POST_HARD_UPGRADE) {
            clearDerby4895ThreadLocal();
        }

        if (loader != null)
            UpgradeClassLoader.setThreadLoader(previousLoader);       
        loader = null;
        previousLoader = null;
        
        UpgradeChange.phase.set(null);
        UpgradeChange.oldVersion.set(null);
    }

    /**
     * Deregister all JDBC drivers in the class loader associated with this
     * version.
     */
    private void deregisterDriver() throws Exception {
        // DriverManager only allows deregistering of drivers from classes
        // that live in a class loader that is able to load the driver. So
        // create an instance of DriverUnloader in the old driver's class
        // loader.
        Class unloader = Class.forName(
                DriverUnloader.class.getName(), true, loader);
        Method m = unloader.getMethod("unload", (Class[]) null);
        m.invoke(null, (Object[]) null);
    }

    /**
     * Clear a static ThreadLocal field in TableDescriptor so that the engine
     * classes can be garbage collected when they are no longer used. This is
     * a workaround for DERBY-4895, which affects Derby 10.5 and 10.6.
     */
    private void clearDerby4895ThreadLocal() throws Exception {
        boolean isAffectedVersion =
            (UpgradeRun.lessThan(new int[] {10,5,0,0}, version) &&
             UpgradeRun.lessThan(version, new int[] {10,5,3,2}))
            ||
            (UpgradeRun.lessThan(new int[] {10,6,0,0}, version) &&
             UpgradeRun.lessThan(version, new int[] {10,6,2,3}));

        if (!isAffectedVersion) {
            // Nothing to work around in this version.
            return;
        }

        Class td = Class.forName(
                "org.apache.derby.iapi.sql.dictionary.TableDescriptor",
                true, loader);
        Field f = td.getDeclaredField("referencedColumnMap");
        f.setAccessible(true);
        f.set(null, null);
    }
}
