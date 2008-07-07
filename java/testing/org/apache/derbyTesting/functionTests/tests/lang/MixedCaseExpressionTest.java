/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.MixedCaseExpressionTest
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
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 *Test case for case.sql.
 */
public class MixedCaseExpressionTest extends BaseJDBCTestCase {

    public MixedCaseExpressionTest(String name) {
        super(name);
    }

    public static Test suite(){
        return TestConfiguration.defaultSuite(MixedCaseExpressionTest.class);
    }

    /**
     *  This test is for keyword case insensitivity.
     * @throws SQLException
     */
    public void testKeywordsWithMixedCase() throws SQLException{
        Statement st = createStatement();

        String sql = "cReAtE tAbLe T (x InT)";
        st.executeUpdate(sql);

        sql = "CrEaTe TaBlE s (X iNt)";
        st.executeUpdate(sql);

        sql = "iNsErT iNtO t VaLuEs (1)";
        assertEquals("Mistake in inserting table", 1, st.executeUpdate(sql));

        sql = "InSeRt InTo S vAlUeS (2)";
        assertEquals("Mistake in inserting table", 1, st.executeUpdate(sql));

        sql = "sElEcT * fRoM t";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), "1");

        sql = "SeLeCt * FrOm s";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), "2");

        dropTable("s");

        dropTable("t");
    }
}
