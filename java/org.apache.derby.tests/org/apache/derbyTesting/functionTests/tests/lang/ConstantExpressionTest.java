/**
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ConstantExpressionTest
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test case for constantExpression.sql, which provides tests
 * for constant expression evaluation.
 */
public class ConstantExpressionTest extends BaseJDBCTestCase {

    /**
     * Constructor. 
     */
    public ConstantExpressionTest(String name) throws SQLException {
        super(name);
    }

    public static Test suite(){    
        return TestConfiguration.defaultSuite(
                ConstantExpressionTest.class);
    }

    /**
     * Create the table t1 with 3 rows, and two procedures.
     * @throws SQLException 
     *      if SQLException is thrown.
     * @see junit.framework.TestCase#setUp()
     */
    public void setUp() throws SQLException{
        String sql = "create table t1(c1 int)";
        Statement st = createStatement();
        st.executeUpdate(sql);

        sql = "insert into t1 values 1, 2, 3";
        assertEquals("Fail to insert into table", 3, st.executeUpdate(sql));

        st.close();
    }

    /** 
     * Drop table t1 and close two procedures.
     * @throws Exception 
     *      if Exception is thrown.
     * @see org.apache.derbyTesting.junit.BaseJDBCTestCase#tearDown()
     */
    public void tearDown() throws Exception {
        dropTable("t1");
        super.tearDown();
    }

    /**
     * Test false constant expressions.
     * @throws SQLException
     *      if SQLException is thrown.
     */
    public void testFalseConstantExpressions() throws SQLException{
        String[] falseCases = {
                "1 <> 1", "1 = 1 and 1 = 0", "1 = (2 + 3 - 2)",
                "(case when 1 = 1 then 0 else 1 end) = 1",
                "1 in (2, 3, 4)", "1 between 2 and 3",
        };

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet rs;
        String sql;
        for(int i = 0; i < falseCases.length; i++){
            sql = "select * from t1 where " + falseCases[i];
            rs = st.executeQuery(sql);
            JDBC.assertEmpty(rs);
        }

        st.close();

        sql = "select * from t1 where ? = 1";
        PreparedStatement ps1 = prepareStatement(sql);
        ps1.setInt(1, 0);
        rs = ps1.executeQuery();
        JDBC.assertEmpty(rs);
        ps1.close();

        sql = "select * from t1 where cast(? as int) = 1";
        PreparedStatement ps2 = prepareStatement(sql);
        ps2.setInt(1, 0);
        rs = ps2.executeQuery();
        JDBC.assertEmpty(rs);
        ps2.close();
    }

    /**
     * Test true constant expressions.
     * @throws SQLException
     */
    public void testTrueConstantExpressions() throws SQLException{
        String[] trueCases = {
                "1 = 1", "1 = 0 or 1 = 1", "1 + 2 = (2 + 3 - 2)",
                "(case when 1 = 1 then 1 else 0 end) = 1",
                "1 in (2, 3, 4, 4, 3, 2, 1)", "1 + 1 between 0 and 3",
        };
        String[][] content = {
                { "1", }, { "2", }, { "3", }, 
        };

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet rs;
        String sql;
        for(int i = 0; i < trueCases.length; i++){
            sql = "select * from t1 where " + trueCases[i];
            rs = st.executeQuery(sql);
            JDBC.assertFullResultSet(rs, content);
        }

        st.close();

        sql = "select * from t1 where ? = 1";
        PreparedStatement ps1 = prepareStatement(sql);
        ps1.setInt(1, 1);
        rs = ps1.executeQuery();
        JDBC.assertFullResultSet(rs, content);
        ps1.close();

        sql = "select * from t1 where cast(? as int) = 1";
        PreparedStatement ps2 = prepareStatement(sql);
        ps2.setInt(1, 1);
        rs = ps2.executeQuery();
        JDBC.assertFullResultSet(rs, content);
        ps2.close();
    }
}
