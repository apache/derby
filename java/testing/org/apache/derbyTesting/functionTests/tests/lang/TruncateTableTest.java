/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.TruncateTableTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for TRUNCATE TABLE. Currently, Derby only supports TRUNCATE TABLE in
 * debug builds.
 */
public class TruncateTableTest extends BaseJDBCTestCase {

    public TruncateTableTest(String name) {
        super(name);
    }

    public static Test suite() {
        if (!SanityManager.DEBUG) {
            // Since Derby doesn't support TRUNCATE TABLE in non-debug builds,
            // only test that a "not implemented" exception is thrown.
            return new TruncateTableTest("unsupportedInInsaneBuilds");
        }
        return TestConfiguration.defaultSuite(TruncateTableTest.class);
    }

    /**
     * Test that a "not implemented" exception is thrown if TRUNCATE TABLE
     * is used in insane builds.
     */
    public void unsupportedInInsaneBuilds() throws SQLException {
        assertFalse("Not to be tested in sane builds", SanityManager.DEBUG);
        assertStatementError("0A000", createStatement(),
                             "truncate table table_that_does_not_exist");
    }

    /**
     * Test that TRUNCATE TABLE works when there is an index on one of the
     * columns. (This code would throw a {@code NullPointerException} before
     * DERBY-3352 was fixed).
     */
    public void testTruncateWithIndex() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t_with_index (x varchar(128) unique, y int)");
        s.execute("insert into t_with_index values ('one', 1), ('two', 2)");
        s.execute("truncate table t_with_index");
        assertTableRowCount("T_WITH_INDEX", 0);
    }
}
