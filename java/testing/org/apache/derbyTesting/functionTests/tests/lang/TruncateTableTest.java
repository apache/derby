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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for TRUNCATE TABLE.
 * 
 */
public class TruncateTableTest extends BaseJDBCTestCase {

    public TruncateTableTest(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("TruncateTableTest Test");
        suite.addTest(TestConfiguration.defaultSuite(TruncateTableTest.class));
        return TestConfiguration.sqlAuthorizationDecorator(suite);
    }

    /**
     * Test that TRUNCATE TABLE works when there is an index on one of the
     * columns. Verify that default "CONTINUE IDENTITY" semantics are enforced.
     */
    public void testTruncateWithIndex() throws SQLException {
        Statement st = createStatement();
        ResultSet rs;
        String[][] expRS;
        //creating a table with one column auto filled with a unique value
        st.executeUpdate("create table t1(a int not null generated always as identity primary key, b varchar(100))");
        //populate the table
        st.executeUpdate("insert into t1(b) values('one'),('two'),('three'),('four'),('five')");
        //varify the inserted values
        rs = st.executeQuery("select * from t1 order by a");
        expRS = new String[][]{
                        {"1","one"},
                        {"2","two"},
                        {"3","three"},
                        {"4","four"},
                        {"5","five"}
                };
        JDBC.assertFullResultSet(rs, expRS);
        //executing the truncate table
        st.executeUpdate("truncate table t1");
        //confirm whether the truncation worked
        assertTableRowCount("T1", 0);

        //testing whether the truncation work as "CONTINUE IDENTITY"
        //semantics are enforced
        st.executeUpdate("insert into t1(b) values('six'),('seven')");
        rs = st.executeQuery("select * from t1 order by a");
        expRS = new String[][]{
                        {"6","six"},
                        {"7","seven"}
                };
        JDBC.assertFullResultSet(rs, expRS);

    }
}
