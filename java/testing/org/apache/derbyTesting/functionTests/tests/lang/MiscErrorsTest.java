/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.MiscErrorsTest

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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests miscellaneous error situations.
 */
public class MiscErrorsTest extends BaseJDBCTestCase {

    public MiscErrorsTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(MiscErrorsTest.class);
    }

    public void testLexicalError() {
        String sql = "select @#^%*&! from swearwords";
        assertCompileError("42X02", sql);
    }

    /**
     * Try to create duplicate table.
     */
    public void testDuplicateTableCreation() throws SQLException {
        String sql = "create table a (one int)";
        Statement st = createStatement();
        st.executeUpdate(sql);

        sql = "create table a (one int, two int)";
        assertStatementError("X0Y32", st, sql);

        sql = "create table a (one int)";
        assertStatementError("X0Y32", st, sql);

        dropTable("a");

        sql = "create table a (one int, two int, three int)";
        st.executeUpdate(sql);

        sql = "insert into a values (1,2,3)";
        assertEquals(1, st.executeUpdate(sql));

        sql = "select * from a";
        JDBC.assertUnorderedResultSet(st.executeQuery(sql),
                new String[][] { { "1", "2", "3", } });

        dropTable("a");
    }

    /**
     * See that statements that fail at parse or bind time
     * are not put in the statement cache.
     */
    public void testStatementCache() throws SQLException {
        String sql = "values 1";
        Statement st = createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), "1");

        //a stronger test.
        sql = "select SQL_TEXT from syscs_diag.statement_cache"
            + " where CAST(SQL_TEXT AS LONG VARCHAR)" + " LIKE '%values 1%'";
        JDBC.assertUnorderedResultSet(st.executeQuery(sql),
                new String[][] {
                    { sql }, { "values 1" }, }
        );

        sql = "select SQL_TEXT from syscs_diag.statement_cache"
                + " where CAST(SQL_TEXT AS LONG VARCHAR)" + " LIKE '%932432%'";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), sql);

        sql = "VALUES FRED932432";
        assertCompileError("42X04", sql);

        sql = "SELECT * FROM BILL932432";
        assertCompileError("42X05", sql);

        sql = "SELECT 932432";
        assertCompileError("42X01", sql);

        sql = "select SQL_TEXT from syscs_diag.statement_cache"
                + " where CAST(SQL_TEXT AS LONG VARCHAR)" + " LIKE '%932432%'";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), sql);
    }
}
