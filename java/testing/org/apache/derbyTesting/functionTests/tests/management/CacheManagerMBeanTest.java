/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management.CacheManagerMBeanTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.management;

import java.sql.PreparedStatement;
import java.util.Hashtable;
import java.util.Set;
import javax.management.ObjectName;
import junit.framework.Test;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test cases for {@code CacheManagerMBean}.
 */
public class CacheManagerMBeanTest extends MBeanTest {

    public CacheManagerMBeanTest(String name) {
        super(name);
    }

    public static Test suite() {
        return MBeanTest.suite(CacheManagerMBeanTest.class,
                               "CacheManagerMBeanTest");
    }

    @Override
    protected void setUp() throws Exception {
        // Set up management.
        super.setUp();

        // Shut down the database before running the test case, so that the
        // test case can assume that it starts from a clean state where the
        // cache beans have not started yet. shutdownDatabase() fails if
        // the database is not already booted, so get a connection first to
        // ensure that it is booted (otherwise, the test will fail if it
        // runs standalone or first in a suite).
        getConnection().close();
        TestConfiguration.getCurrent().shutdownDatabase();
    }

    /**
     * Create an {@code ObjectName} that identifies a {@code CacheManager}
     * management bean, or a pattern that potentially matches multiple
     * beans.
     *
     * @param cacheName the name of the cache (such as PageCache), or
     *   {@code null} to create a pattern that matches all cache names
     * @param dbName the name of the database, or {@code null} to create
     *   a pattern that matches all database names
     * @return an {@code ObjectName} suitable for looking up beans
     */
    private ObjectName createObjectName(String cacheName, String dbName)
            throws Exception {
        Hashtable<String, String> props = new Hashtable<String, String>();
        props.put("type", "CacheManager");
        props.put("name", cacheName == null ? "*" : cacheName);
        props.put("db", dbName == null ? "*" : ObjectName.quote(dbName));
        return getDerbyMBeanName(props);
    }

    /**
     * Test case that verifies that {@code CacheManagerMBean}s start when
     * a database is started, and stop when the database is shut down.
     */
    public void testAllMBeansStartedAndStopped() throws Exception {
        // This pattern matches all CacheManager management beans when used
        // in a query.
        ObjectName pattern = createObjectName(null, null);

        // There should be no CacheManager MBeans before the database
        // is booted.
        Set<ObjectName> names = queryMBeans(pattern);
        if (!names.isEmpty()) {
            fail("Should not find MBeans before boot, found: " + names);
        }

        // Boot the database, so that the MBeans are started.
        getConnection();

        // There should be two CacheManager beans. One for the page cache
        // and one for the container cache.
        names = queryMBeans(pattern);
        assertEquals("Incorrect number of MBeans found in " + names,
                     2, names.size());

        // Shut down the database.
        TestConfiguration.getCurrent().shutdownDatabase();

        // There should be no CacheManager MBeans after the database has
        // been shut down.
        names = queryMBeans(pattern);
        if (!names.isEmpty()) {
            fail("Should not find MBeans after shutdown, found: " + names);
        }
    }

    /**
     * Test the {@code CacheManagerMBean} for the page cache.
     */
    public void testPageCache() throws Exception {
        getConnection(); // boot the database
        Set<ObjectName> names =
                queryMBeans(createObjectName("PageCache", null));

        assertEquals("Should have a single page cache", 1, names.size());

        ObjectName name = names.iterator().next();

        assertBooleanAttribute(false, name, "CollectAccessCounts");
        assertLongAttribute(0, name, "HitCount");
        assertLongAttribute(0, name, "MissCount");
        assertLongAttribute(0, name, "EvictionCount");
        // Default page cache size is 1000
        assertLongAttribute(1000, name, "MaxEntries");
        // Cannot reliably tell how many entries to expect.
        // More than 0 for sure.
        Long allocated = (Long) getAttribute(name, "AllocatedEntries");
        assertTrue("Allocated entries: " + allocated, allocated > 0);
        Long used = (Long) getAttribute(name, "UsedEntries");
        assertTrue("Used entries: " + used, used > 0);

        // Execute a statement against a table, so that the cache will be
        // accessed.
        PreparedStatement ps = prepareStatement(
                                    "select * from sysibm.sysdummy1");
        JDBC.assertDrainResults(ps.executeQuery());

        // Since collection of access counts is disabled by default, don't
        // expect the counts to be updated.
        assertLongAttribute(0, name, "HitCount");
        assertLongAttribute(0, name, "MissCount");
        assertLongAttribute(0, name, "EvictionCount");

        // Now enable the access counts and re-execute the query. It
        // should result in a hit in the page cache.
        setAttribute(name, "CollectAccessCounts", Boolean.TRUE);
        assertBooleanAttribute(true, name, "CollectAccessCounts");
        JDBC.assertDrainResults(ps.executeQuery());
        assertLongAttribute(1, name, "HitCount");
        assertLongAttribute(0, name, "MissCount");

        // Disable the access counts.
        setAttribute(name, "CollectAccessCounts", Boolean.FALSE);
        assertBooleanAttribute(false, name, "CollectAccessCounts");
    }

    /**
     * Test the {@code CacheManagerMBean} for the page cache.
     */
    public void testContainerCache() throws Exception {
        getConnection(); // boot the database
        Set<ObjectName> names =
                queryMBeans(createObjectName("ContainerCache", null));

        assertEquals("Should have a single container cache", 1, names.size());

        ObjectName name = names.iterator().next();

        assertBooleanAttribute(false, name, "CollectAccessCounts");
        assertLongAttribute(0, name, "HitCount");
        assertLongAttribute(0, name, "MissCount");
        assertLongAttribute(0, name, "EvictionCount");
        // Default container cache size is 100
        assertLongAttribute(100, name, "MaxEntries");
        // Cannot reliably tell how many entries to expect.
        // More than 0 for sure.
        Long allocated = (Long) getAttribute(name, "AllocatedEntries");
        assertTrue("Allocated entries: " + allocated, allocated > 0);
        Long used = (Long) getAttribute(name, "UsedEntries");
        assertTrue("Used entries: " + used, used > 0);
    }
}
