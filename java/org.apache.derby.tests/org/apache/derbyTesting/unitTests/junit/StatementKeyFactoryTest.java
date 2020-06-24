/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.StatementKeyFactoryTest

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

package org.apache.derbyTesting.unitTests.junit;

import java.sql.ResultSet;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derby.client.am.stmtcache.StatementKey;
import org.apache.derby.client.am.stmtcache.StatementKeyFactory;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that statement key equality is correct, and that the factory produces
 * correct keys.
 * <p>
 * Objects implementing {@link StatementKey} is crucial for correct
 * operation of the JDBC statement object cache.
 * 
 * @see org.apache.derby.client.am.stmtcache.JDBCStatementCache
 */
public class StatementKeyFactoryTest
        extends BaseTestCase {

    public StatementKeyFactoryTest(String name) {
        super(name);
    }

    /**
     * Creating keys with <code>null</code> for required information should
     * fail, as it can lead to NPEs in the key implementations and/or the wrong
     * statement to be fetched from the cache.
     */
    public void testCreationBasicWithNulls() {
        try {
            StatementKeyFactory.newPrepared(null, null, 0);
            fail("Creation with <null> should have failed");
        } catch (IllegalArgumentException iae) {
            // As expected
        }
        try {
            StatementKeyFactory.newPrepared(null, "app", 0);
            fail("Creation with <null> should have failed");
        } catch (IllegalArgumentException iae) {
            // As expected
        }
        try {
            StatementKeyFactory.newPrepared("values 1", null, 0);
            fail("Creation with <null> should have failed");
        } catch (IllegalArgumentException iae) {
            // As expected
        }
    }

    public void testCreationBasic() {
        StatementKey stdKey = StatementKeyFactory.newPrepared("values 1", "APP", 
                                            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        StatementKey key = StatementKeyFactory.newPrepared(
                "select * from sys.systables", "APP", 
                                            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertFalse(key.equals(stdKey));
        assertFalse(stdKey.equals(key));
    }

    public void testEqualityBasic() {
        StatementKey key1 = StatementKeyFactory.newPrepared(
                "select * from sys.systables", "APP", 
                                            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        StatementKey key2 = StatementKeyFactory.newPrepared(
                "select * from sys.systables", "APP", 
                                            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        StatementKey key3 = StatementKeyFactory.newPrepared(
                "select * from sys.systables", "APP", 
                                            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertTrue(key1.equals(key2));
        assertTrue(key2.equals(key1));
        assertTrue(key2.equals(key3));
        assertTrue(key1.equals(key3));
    }

    public void testEqualityDefaultNoAutoGenKey() {
        int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        StatementKey basicKey = StatementKeyFactory.newPrepared(
                "values 2", "APP", holdability);
        StatementKey simplifiedKey = StatementKeyFactory.newPrepared(
                "values 2", "APP", holdability, Statement.NO_GENERATED_KEYS);
        assertTrue(basicKey.equals(simplifiedKey));
        assertTrue(simplifiedKey.equals(basicKey));
    }

    public void testEqualityNoAutoVsAutoGenKey() {
        int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        StatementKey basicKey = StatementKeyFactory.newPrepared(
                "values 2", "APP", holdability);
        StatementKey autoKey = StatementKeyFactory.newPrepared(
                "values 2", "APP", holdability, Statement.RETURN_GENERATED_KEYS);
        assertFalse(basicKey.equals(autoKey));
        assertFalse(autoKey.equals(basicKey));
    }

    public void testUnequalityVarious() {
        String sql = "select * from sys.systables";
        String schema = "APP";
        int rsh = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        int rst = ResultSet.TYPE_SCROLL_INSENSITIVE;
        int rsc = ResultSet.CONCUR_UPDATABLE;
        int auto = Statement.RETURN_GENERATED_KEYS;
        // Create a one key of each type, all different from each other.
        StatementKey[] keys = new StatementKey[] {
            StatementKeyFactory.newPrepared(sql, schema, rsh),
            StatementKeyFactory.newPrepared(sql, schema, rsh, auto),
//IC see: https://issues.apache.org/jira/browse/DERBY-3324
            StatementKeyFactory.newPrepared(sql, schema, rst, rsc, rsh),
            StatementKeyFactory.newCallable(sql, schema, rsh),
            StatementKeyFactory.newCallable(sql, schema, rst, rsc, rsh)};
        for (int outer=0; outer < keys.length; outer++) {
            StatementKey current = keys[outer];
            for (int inner=0; inner < keys.length; inner++) {
                if (outer != inner) {
                    if (current.equals(keys[inner])) {
                        fail("[" + current.toString() + "] should not equal [" +
                                keys[inner].toString() + "]");
                    }
                } else {
                    // Should equal itself.
                    assertTrue(current.equals(keys[inner]));
                }
            }
        }
    }

    public void testCallableVsPrepared() {
        String sql = "select colA, colB from mytable";
        String schema = "SOMEAPP";
        int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        StatementKey callable =
                StatementKeyFactory.newCallable(sql, schema, holdability);
        StatementKey prepared =
                StatementKeyFactory.newPrepared(sql, schema, holdability);
        assertFalse(callable.equals(prepared));
        assertFalse(prepared.equals(callable));
    }

    /**
     * Returns the appropriate tests.
     * <p>
     * Run only client/server, because the code being tested does not live
     * in the embedded driver (yet).
     * 
     * @return A suite of tests (may be empty).
     */
    public static Test suite() {
        // Run only client/server, because the code being tested does not live
        // in the embedded driver (yet).
        return TestConfiguration.clientServerSuite(
                StatementKeyFactoryTest.class);
    }
}
