/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.LikeTest
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
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for statements with a LIKE clause.
 */
public class LikeTest extends BaseJDBCTestCase {
    public LikeTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(LikeTest.class);
    }

    /**
     * Test that LIKE expressions are optimized and use indexes to limit the
     * scan if the arguments are concatenated string literals. DERBY-4791.
     */
    public void testOptimizeConcatenatedStringLiterals() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table t (x varchar(128) primary key, y int)");
        s.execute("insert into t(x) values " +
                  "'abc', 'def', 'ghi', 'ab', 'de', 'gh'");
        s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        // Check that an optimizable LIKE predicate (one that doesn't begin
        // with a wildcard) with a string literal picks an index scan.
        String[][] expectedRows = { {"ab", null}, {"abc", null} };
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t where x like 'ab%'"),
                expectedRows);
        assertTrue(SQLUtilities.getRuntimeStatisticsParser(s).usedIndexScan());

        // Now do the same test, but concatenate two string literals instead
        // of using a single string literal. This should be optimized the
        // same way.
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t where x like 'a'||'b'||'%'"),
                expectedRows);
        assertTrue(SQLUtilities.getRuntimeStatisticsParser(s).usedIndexScan());
    }
}
