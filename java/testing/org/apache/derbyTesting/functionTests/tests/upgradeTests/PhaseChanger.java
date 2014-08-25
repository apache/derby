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
import java.util.Set;

import javax.sql.DataSource;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSetup;
import org.apache.derbyTesting.junit.ClassLoaderTestSetup;
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
        UpgradeChange.phase.set(phase);
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
            ClassLoaderTestSetup.setThreadLoader(loader);
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

        // Get a handle to the old engine's ContextService if this version is
        // affected by DERBY-23. The actual workaround for DERBY-23 must be
        // done after the engine has been shut down, but we fetch the handle
        // to the service before shutdown, while it's still easily available.
        Object contextService = getDerby23ContextService();

        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.shutEngine(ds);

        // When shutting down the old engine for good, make sure that it's
        // made eligible for garbage collection by working around bugs in
        // some of the old versions.
        if (phase == UpgradeChange.PH_POST_HARD_UPGRADE) {
            // Workaround for DERBY-2905. Some versions don't deregister the
            // JDBC driver when shutting down the engine.
            deregisterDriver();

            // Workaround for DERBY-4895, which prevented the engine classes
            // from being garbage collected.
            clearDerby4895ThreadLocal();
        }

        // Workaround for DERBY-23, continued. If this is one of the affected
        // versions, clear the fields that prevent the engine from being
        // garbage collected.
        clearDerby23ThreadLocals(contextService);

        if (loader != null)
            ClassLoaderTestSetup.setThreadLoader(previousLoader);
        loader = null;
        previousLoader = null;
        
        UpgradeChange.phase.set(null);
        UpgradeChange.oldVersion.set(null);
    }

    /**
     * Make sure the JDBC driver in the class loader associated with this
     * version is deregistered. This is a workaround for DERBY-2905, which
     * affected Derby 10.2 - 10.7, and it is needed to make the old engine
     * classes eligible for garbage collection.
     */
    private void deregisterDriver() throws Exception {
        boolean isAffectedVersion =
                UpgradeRun.lessThan(new int[] {10,2,0,0}, version) &&
                UpgradeRun.lessThan(version, new int[] {10,8,0,0});

        if (JDBC.vmSupportsJDBC3()) {
            // DriverManager only allows deregistering of drivers from classes
            // that live in a class loader that is able to load the driver. So
            // create an instance of DriverUnloader in the old driver's class
            // loader.
            Class<?> unloader = Class.forName(
                    DriverUnloader.class.getName(), true, loader);
            Method m = unloader.getMethod("unload", (Class[]) null);
            Boolean res = (Boolean) m.invoke(null, (Object[]) null);

            // Check that there weren't any drivers to unload except in the
            // versions affected by DERBY-2905.
            assertEquals("Unexpected result from driver unloading",
                         isAffectedVersion, res.booleanValue());
        }
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
        clearField(td, "referencedColumnMap", null);
    }

    /**
     * Clear a field that is possibly private or final.
     *
     * @param cls the class in which the field lives
     * @param name the name of the field to clear
     * @param instance the instance whose field should be cleared,
     *                 or null if the field is static
     */
    private static void clearField(Class cls, String name, Object instance)
            throws Exception {
        Field f = cls.getDeclaredField(name);
        f.setAccessible(true);
        f.set(instance, null);
    }

    /**
     * Get a handle to the ContextService in the old engine if the version
     * is affected by DERBY-23.
     *
     * @return the ContextService, if this version is affected by DERBY-23,
     * or null otherwise
     */
    private Object getDerby23ContextService() throws Exception {
        if (loader != null &&
                UpgradeRun.lessThan(version, new int[] {10,2,1,6})) {
            Class cls = Class.forName(
                    "org.apache.derby.iapi.services.context.ContextService",
                    true, loader);
            Field f = cls.getDeclaredField("factory");
            f.setAccessible(true);
            return f.get(null);
        }

        return null;
    }

    /**
     * Clear some fields in ContextService to allow the engine to be garbage
     * collected. This is a workaround for DERBY-23.
     *
     * @param contextService the context service for an engine that has been
     * shut down, or null if this version of the engine doesn't suffer from
     * DERBY-23
     */
    private void clearDerby23ThreadLocals(Object contextService)
            throws Exception {
        if (contextService != null) {
            Class cls = contextService.getClass();

            // DERBY-5343: Ideally, we'd just set the two fields to null
            // like this:
            //
            //     clearField(cls, "threadContextList", contextService);
            //     clearField(cls, "allContexts", contextService);
            //
            // However, the fields are final in the versions that suffer from
            // DERBY-23, and Java versions prior to Java 5 don't allow us to
            // modify final fields. So let's take a different approach to make
            // it work on Java 1.4.2 and Foundation Profile 1.1 as well.

            // The field threadContextList is a ThreadLocal. Clear it in the
            // current thread. Assuming all other threads that have accessed
            // the database engine (background threads and any helper threads
            // started by the test cases) are stopped and made eligible for
            // garbage collection, this should be a sufficient replacement for
            // setting the field to null.
            Field tclField = cls.getDeclaredField("threadContextList");
            tclField.setAccessible(true);
            ThreadLocal<?> tcl = (ThreadLocal) tclField.get(contextService);
            tcl.set(null);

            // The field allContexts is a HashSet. Calling clear() should be
            // equivalent to setting it to null in terms of making its elements
            // eligible for garbage collection.
            Field acField = cls.getDeclaredField("allContexts");
            acField.setAccessible(true);
            Set ac = (Set) acField.get(contextService);
            ac.clear();
        }
    }
}
