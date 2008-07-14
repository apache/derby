/*
 * 
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UngroupedAggregatesTest
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test case for ungroupedAggregatesNegative.sql. 
 * It provides negative tests for ungrouped aggregates.
 */
public class UngroupedAggregatesNegativeTest extends BaseJDBCTestCase {

    public UngroupedAggregatesNegativeTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(
                UngroupedAggregatesNegativeTest.class);
    }

    public void setUp() throws SQLException {
        String sql = "create table t1 (c1 int)";
        Statement st = createStatement();
        st.executeUpdate(sql);

        sql = "create table t2 (c1 int)";
        st.executeUpdate(sql);

        sql = "insert into t2 values 1,2,3";
        assertEquals(3, st.executeUpdate(sql));

        st.close();
    }

    public void tearDown() throws SQLException {
        dropTable("t1");
        dropTable("t2");
    }

    /**
     * Mix aggregate and non-aggregate expressions in the select list.
     */
    public void testSelect() throws SQLException {
        String sql = "select c1, max(c1) from t1";
        assertCompileError("42Y35", sql);

        sql = "select c1 * max(c1) from t1";
        assertCompileError("42Y35", sql);
    }

    /**
     * Aggregate in where clause.
     */
    public void testWhereClause() {
        String sql = "select c1 from t1 where max(c1) = 1";
        assertCompileError("42903", sql);
    }

    /**
     * Aggregate in ON clause of inner join.
     */
    public void testOnClause() {
        String sql = "select * from t1 join t1 " + "as t2 on avg(t2.c1) > 10";
        assertCompileError("42Z07", sql);
    }

    /**
     * Correlated subquery in select list, 
     * and noncorrelated subquery that returns more than 1 row.
     * @throws SQLException 
     */
    public void testSubquery() throws SQLException {
        String sql = "select max(c1), (select t2.c1 from t2 "
                + "where t1.c1 = t2.c1) from t1";
        assertCompileError("42Y29", sql);

        sql = "select max(c1), (select t2.c1 from t2) from t1";
        Statement st = createStatement();
        assertStatementError("21000", st, sql);
        st.close();
    }
}
