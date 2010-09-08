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

import java.sql.PreparedStatement;
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

    /**
     * Test that TRUNCATE TABLE cannot be performed on a table with a
     * delete trigger.
     */
    public void testTruncateWithDeleteTrigger() throws Exception {
        Statement s = createStatement();

        // Create two tables, t1 and t2, where deletes from t1 cause inserts
        // into t2.
        s.execute("create table deltriggertest_t1(x int)");
        s.execute("create table deltriggertest_t2(y int)");
        s.execute("create trigger deltriggertest_tr after delete on "
                + "deltriggertest_t1 referencing old as old for each row "
                + "insert into deltriggertest_t2 values old.x");

        // Prepare a statement that checks the number of rows in the
        // destination table (t2).
        PreparedStatement checkDest = prepareStatement(
                "select count(*) from deltriggertest_t2");

        // Insert rows into t1, delete them, and verify that t2 has grown.
        s.execute("insert into deltriggertest_t1 values 1,2,3");
        JDBC.assertSingleValueResultSet(checkDest.executeQuery(), "0");
        assertUpdateCount(s, 3, "delete from deltriggertest_t1");
        JDBC.assertSingleValueResultSet(checkDest.executeQuery(), "3");

        // Now do the same with TRUNCATE instead of DELETE. Expect it to fail
        // because there is a delete trigger on the table.
        s.execute("insert into deltriggertest_t1 values 4,5");
        assertStatementError("XCL49", s, "truncate table deltriggertest_t1");
        JDBC.assertSingleValueResultSet(checkDest.executeQuery(), "3");
    }

    /**
     * Test that TRUNCATE TABLE isn't allowed on a table referenced by a
     * foreign key constraint on another table.
     */
    public void testTruncateWithForeignKey() throws SQLException {
        Statement s = createStatement();

        // Create two tables with a foreign key relationship.
        s.execute("create table foreignkey_t1(x int primary key)");
        s.execute("create table foreignkey_t2(y int references foreignkey_t1)");
        s.execute("insert into foreignkey_t1 values 1,2");
        s.execute("insert into foreignkey_t2 values 2");

        // Truncating the referenced table isn't allowed as that would
        // break referential integrity.
        assertStatementError("XCL48", s, "truncate table foreignkey_t1");

        // Truncating the referencing table is OK.
        s.execute("truncate table foreignkey_t2");
        assertTableRowCount("FOREIGNKEY_T2", 0);
    }

    /**
     * Test that TRUNCATE TABLE is allowed on a referenced table if it's only
     * referenced by itself.
     */
    public void testSelfReferencing() throws SQLException {
        Statement s = createStatement();
        s.execute("create table self_referencing_t1(x int primary key, "
                + "y int references self_referencing_t1)");
        s.execute("insert into self_referencing_t1 values (1, null), (2, 1)");
        s.execute("truncate table self_referencing_t1");
        assertTableRowCount("SELF_REFERENCING_T1", 0);
    }
}
