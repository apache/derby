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

import java.security.Permission;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Set;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;
import junit.framework.Test;
import org.apache.derby.shared.common.security.SystemPermission;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test cases for {@code CacheManagerMBean}.
 */
public class CacheManagerMBeanTest extends MBeanTest {

    private final static int DEFAULT_PAGE_CACHE_SIZE = 1000;
    private final static int DEFAULT_CONTAINER_CACHE_SIZE = 100;
    private final static int DEFAULT_STATEMENT_CACHE_SIZE = 100;

    private static String[] ALL_ATTRIBUTES = {
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
        "CollectAccessCounts", "HitCount", "MissCount", "EvictionCount",
        "MaxEntries", "AllocatedEntries", "UsedEntries"
    };

    public CacheManagerMBeanTest(String name) {
        super(name);
    }

    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite();
        suite.addTest(MBeanTest.suite(CacheManagerMBeanTest.class,
                                      "CacheManagerMBeanTest"));

        // Test that the management bean can only be accessed with proper
        // permissions. The custom policy files only have entries for jar
        // files, so skip these test cases when running from classes.
        if (TestConfiguration.loadingFromJars()) {
            Test negative = new CacheManagerMBeanTest("withoutPermsTest");
            negative = JMXConnectionDecorator.platformMBeanServer(negative);
            negative = new SecurityManagerSetup(negative,
                    "org/apache/derbyTesting/functionTests/tests/management/"
                        + "CacheManagerMBeanTest.withoutPerm.policy");
            suite.addTest(negative);

            Test positive = new CacheManagerMBeanTest("withPermsTest");
            positive = JMXConnectionDecorator.platformMBeanServer(positive);
            positive = new SecurityManagerSetup(positive,
                    "org/apache/derbyTesting/functionTests/tests/management/"
                            + "CacheManagerMBeanTest.withPerm.policy");
            suite.addTest(positive);
        }

        return suite;
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

        // There should be three CacheManager beans. One for the page cache,
        // one for the container cache, and one for the statement cache.
        names = queryMBeans(pattern);
        assertEquals("Incorrect number of MBeans found in " + names,
                     3, names.size());
//IC see: https://issues.apache.org/jira/browse/DERBY-6733

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
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
        assertLongAttribute(DEFAULT_PAGE_CACHE_SIZE, name, "MaxEntries");
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
        assertLongAttribute(DEFAULT_CONTAINER_CACHE_SIZE, name, "MaxEntries");
        // Cannot reliably tell how many entries to expect.
        // More than 0 for sure.
        Long allocated = (Long) getAttribute(name, "AllocatedEntries");
        assertTrue("Allocated entries: " + allocated, allocated > 0);
        Long used = (Long) getAttribute(name, "UsedEntries");
        assertTrue("Used entries: " + used, used > 0);
    }

    /**
     * Test the {@code CacheManagerMBean} for the statement cache.
     */
    public void testStatementCache() throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
        getConnection(); // boot the database
        Set<ObjectName> names =
                queryMBeans(createObjectName("StatementCache", null));

        assertEquals("Should have a single statement cache", 1, names.size());

        ObjectName name = names.iterator().next();

        assertBooleanAttribute(false, name, "CollectAccessCounts");
        assertLongAttribute(0, name, "HitCount");
        assertLongAttribute(0, name, "MissCount");
        assertLongAttribute(0, name, "EvictionCount");
        assertLongAttribute(DEFAULT_STATEMENT_CACHE_SIZE, name, "MaxEntries");
        // The statement cache is initially empty
        assertLongAttribute(0, name, "AllocatedEntries");
        assertLongAttribute(0, name, "UsedEntries");

        // Prepare a statement. Now there should be one allocated entry, and
        // that entry is also a used entry.
        prepareStatement("values 1").close();
        assertLongAttribute(1, name, "AllocatedEntries");
        assertLongAttribute(1, name, "UsedEntries");

        // One more...
        prepareStatement("values 2").close();
        assertLongAttribute(2, name, "AllocatedEntries");
        assertLongAttribute(2, name, "UsedEntries");

        // Now clear the statement cache. One more entry is allocated (for
        // the statement that clears the cache), but no entries should be
        // used after the statement cache is cleared.
        Statement s = createStatement();
        s.execute("call syscs_util.syscs_empty_statement_cache()");
        assertLongAttribute(3, name, "AllocatedEntries");
        assertLongAttribute(0, name, "UsedEntries");

        // None of the accesses to the statement cache should have been
        // counted so far.
        assertLongAttribute(0, name, "HitCount");
        assertLongAttribute(0, name, "MissCount");
        assertLongAttribute(0, name, "EvictionCount");

        // Enable counting of cache accesses.
        setAttribute(name, "CollectAccessCounts", Boolean.TRUE);
        assertBooleanAttribute(true, name, "CollectAccessCounts");

        // Prepare a statement. Since the cache is empty, it must be a miss.
        prepareStatement("values 1").close();
        assertLongAttribute(0, name, "HitCount");
        assertLongAttribute(1, name, "MissCount");
        assertLongAttribute(0, name, "EvictionCount");

        // One more...
        prepareStatement("values 2").close();
        assertLongAttribute(0, name, "HitCount");
        assertLongAttribute(2, name, "MissCount");
        assertLongAttribute(0, name, "EvictionCount");

        // Now, this should cause a hit.
        prepareStatement("values 1").close();
        assertLongAttribute(1, name, "HitCount");
        assertLongAttribute(2, name, "MissCount");
        assertLongAttribute(0, name, "EvictionCount");

        // Prepare so many statements that the cache is filled twice.
        for (int i = 0; i < DEFAULT_STATEMENT_CACHE_SIZE * 2; i++) {
            prepareStatement("values 1, " + i).close();
        }

        // None of the above statements were already in the cache, so expect
        // all of them to cause misses.
        assertLongAttribute(1, name, "HitCount");
        assertLongAttribute(
                2 + DEFAULT_STATEMENT_CACHE_SIZE * 2, name, "MissCount");

        // We have prepared 2 + (DEFAULT_STATEMENT_CACHE_SIZE * 2) statements,
        // and the cache can only hold DEFAULT_STATEMENT_CACHE_SIZE of them,
        // so expect DEFAULT_STATEMENT_CACHE_SIZE + 2 statements to have been
        // evicted from the cache.
        assertLongAttribute(2 + DEFAULT_STATEMENT_CACHE_SIZE,
                            name, "EvictionCount");

        // Expect the cache to be full.
        assertLongAttribute(DEFAULT_STATEMENT_CACHE_SIZE, name, "MaxEntries");
        assertLongAttribute(DEFAULT_STATEMENT_CACHE_SIZE,
                            name, "AllocatedEntries");
        assertLongAttribute(DEFAULT_STATEMENT_CACHE_SIZE, name, "UsedEntries");

        // Disable the access counts.
        setAttribute(name, "CollectAccessCounts", Boolean.FALSE);
        assertBooleanAttribute(false, name, "CollectAccessCounts");
    }

    /**
     * Test that the CacheManagerMBean cannot be accessed if the code
     * base lacks SystemPermission("engine", "monitor").
     */
    public void withoutPermsTest() throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
        getConnection(); // boot the database
        Set<ObjectName> names =
                queryMBeans(createObjectName("StatementCache", null));

        assertEquals("Should have a single statement cache", 1, names.size());

        ObjectName name = names.iterator().next();

        // This is the permission required to access the MBean, but we don't
        // have it.
        SystemPermission monitorPerm =
                new SystemPermission("engine", "monitor");

        // Reading attributes should cause security exception.
        for (String attrName : ALL_ATTRIBUTES) {
            try {
                getAttribute(name, attrName);
                fail();
            } catch (RuntimeMBeanException e) {
                vetException(e, monitorPerm);
            }
        }

        // Modifying attributes should also cause security exception.
        try {
            setAttribute(name, "CollectAccessCounts", Boolean.FALSE);
            fail();
        } catch (RuntimeMBeanException e) {
            vetException(e, monitorPerm);
        }
    }

    /**
     * Check that an exception raised when accessing an MBean, is caused
     * by missing a specific permission.
     *
     * @param e the exception to check
     * @param perm the missing permission to check for
     */
    private void vetException(RuntimeMBeanException e, Permission perm) {
        Throwable cause = e.getCause();
        if (cause instanceof SecurityException) {
            String msg = cause.getMessage();
            if (msg != null && msg.contains(perm.toString())) {
                // This is the expected exception.
                return;
            }
        }

        fail("Unexpected exception", e);
    }

    /**
     * Test that the CacheManagerMBean can be accessed if the code base
     * runs with the same permissions as the {@link #withoutPermsTest} test
     * case plus SystemPermission("engine", "monitor").
     */
    public void withPermsTest() throws Exception {
        getConnection(); // boot the database
        Set<ObjectName> names =
                queryMBeans(createObjectName("StatementCache", null));

        assertEquals("Should have a single statement cache", 1, names.size());

        ObjectName name = names.iterator().next();

        // Expect no SecurityException when reading attributes ...
        for (String attrName : ALL_ATTRIBUTES) {
            getAttribute(name, attrName);
        }

        // ... or when modifying them.
        setAttribute(name, "CollectAccessCounts", Boolean.FALSE);
    }
}
