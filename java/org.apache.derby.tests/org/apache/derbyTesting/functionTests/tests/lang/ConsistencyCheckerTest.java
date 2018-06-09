/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ConsistencyCheckerTest
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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;


public final class ConsistencyCheckerTest extends  BaseJDBCTestCase {

    private Statement st;
    private ResultSet rs;
    private String [] expColNames;
    private String [][] expRS;
    private final String LANG_INDEX_ROW_COUNT_MISMATCH = "X0Y55";
    private final String LANG_INCONSISTENT_ROW_LOCATION = "X0X62";
    private final String LANG_INDEX_COLUMN_NOT_EQUAL = "X0X61";
    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name test name
     */
    public ConsistencyCheckerTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        return TestConfiguration.defaultSuite(ConsistencyCheckerTest.class);
    }

    public void testConsistencyChecker() throws Exception
    {
        CallableStatement cSt;
        st = createStatement();


        try {
            x("create table t1(i int, s smallint, c10 char(10), "
                             + "vc10 varchar(10), dc decimal(5,2))");

            x("create index t1_i on t1(i)");
            x("create index t1_s on t1(s)");
            x("create index t1_c10 on t1(c10)");
            x("create index t1_vc10 on t1(vc10)");
            x("create index t1_dc on t1(dc)");

            // populate the tables

            x("insert into t1 values (1, 11, '1 1', '1 1 1 ', 111.11)");
            x("insert into t1 values (2, 22, '2 2', '2 2 2 ', 222.22)");
            x("insert into t1 values (3, 33, '3 3', '3 3 3 ', 333.33)");
            x("insert into t1 values (4, 44, '4 4', '4 4 4 ', 444.44)");

            // verify that everything is alright

            q("values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");
            c1();
            r1();

            x("CREATE PROCEDURE RFHR(P1 VARCHAR(128), P2 VARCHAR(128))"
              + "LANGUAGE JAVA EXTERNAL NAME "
              + "'org.apache.derbyTesting.functionTests.util.T_Consis"
              + "tencyChecker.reinsertFirstHeapRow'"
              + "PARAMETER STYLE JAVA");

            x("CREATE PROCEDURE DFHR(P1 VARCHAR(128), P2 VARCHAR(128))"
              + "LANGUAGE JAVA EXTERNAL NAME "
              + "'org.apache.derbyTesting.functionTests.util.T_Consis"
              + "tencyChecker.deleteFirstHeapRow'"
              + "PARAMETER STYLE JAVA");

            x("CREATE PROCEDURE NFHR(P1 VARCHAR(128), P2 VARCHAR(128))"
              + "LANGUAGE JAVA EXTERNAL NAME "
              + "'org.apache.derbyTesting.functionTests.util.T_Consis"
              + "tencyChecker.nullFirstHeapRow'"
              + "PARAMETER STYLE JAVA");

            setAutoCommit(false);

            // differing row counts
            // RFHR: reinsertFirstHeapRow
            cSt = prepareCall("call RFHR('APP', 'T1')");
            assertUpdateCount(cSt, 0);

            e(LANG_INDEX_ROW_COUNT_MISMATCH,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            // Drop and recreate each index to see differing count move to next
            // index.

            x("drop index t1_i");
            x("create index t1_i on t1(i)");

            e(LANG_INDEX_ROW_COUNT_MISMATCH,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");
            x("drop index t1_s");
            x("create index t1_s on t1(s)");
            e(LANG_INDEX_ROW_COUNT_MISMATCH,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_c10");
            x("create index t1_c10 on t1(c10)");
            e(LANG_INDEX_ROW_COUNT_MISMATCH,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_vc10");
            x("create index t1_vc10 on t1(vc10)");
            e(LANG_INDEX_ROW_COUNT_MISMATCH,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_dc");
            x("create index t1_dc on t1(dc)");

            // Everything should be back to normal
            q("values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");
            c1();
            r1();

            //
            // Delete 1st row from heap
            //
            // DFHR: deleteFirstHeapRow
            //
            cSt = prepareCall("call DFHR('APP', 'T1')");
            assertUpdateCount(cSt, 0);

            e(LANG_INCONSISTENT_ROW_LOCATION,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            //
            // Drop and recreate each index to see differing count
            // move to next index.
            //

            x("drop index t1_i");
            x("create index t1_i on t1(i)");

            e(LANG_INCONSISTENT_ROW_LOCATION,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_s");
            x("create index t1_s on t1(s)");

            e(LANG_INCONSISTENT_ROW_LOCATION,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_c10");
            x("create index t1_c10 on t1(c10)");

            e(LANG_INCONSISTENT_ROW_LOCATION,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_vc10");
            x("create index t1_vc10 on t1(vc10)");

            e(LANG_INCONSISTENT_ROW_LOCATION,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_dc");
            x("create index t1_dc on t1(dc)");

            //
            // Everything should be back to normal
            //

            q("values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");
            c1();
            r1();

            //
            // Set 1st row from heap to all nulls
            //

            q("select * from t1");

            expColNames = new String [] {"I", "S", "C10", "VC10", "DC"};
            JDBC.assertColumnNames(rs, expColNames);

            expRS = new String [][]
                {
                    {"2", "22", "2 2", "2 2 2", "222.22"},
                    {"3", "33", "3 3", "3 3 3", "333.33"},
                    {"4", "44", "4 4", "4 4 4", "444.44"},
                    {"1", "11", "1 1", "1 1 1", "111.11"}
                };
            JDBC.assertFullResultSet(rs, expRS, true);

            // NFHR: nullFirstHeapRow
            cSt = prepareCall("call NFHR('APP', 'T1')");
            assertUpdateCount(cSt, 0);

            q("select * from t1");

            expColNames = new String [] {"I", "S", "C10", "VC10", "DC"};
            JDBC.assertColumnNames(rs, expColNames);

            expRS = new String [][]
                {
                    {null, null, null, null, null},
                    {"3", "33", "3 3", "3 3 3", "333.33"},
                    {"4", "44", "4 4", "4 4 4", "444.44"},
                    {"1", "11", "1 1", "1 1 1", "111.11"}
                };

            JDBC.assertFullResultSet(rs, expRS, true);

            e(LANG_INDEX_COLUMN_NOT_EQUAL,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            //
            // Drop and recreate each index to see differing count
            // move to next index.
            //

            x("drop index t1_i");
            x("create index t1_i on t1(i)");

            e(LANG_INDEX_COLUMN_NOT_EQUAL,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_s");
            x("create index t1_s on t1(s)");

            e(LANG_INDEX_COLUMN_NOT_EQUAL,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_c10");
            x("create index t1_c10 on t1(c10)");

            e(LANG_INDEX_COLUMN_NOT_EQUAL,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_vc10");
            x("create index t1_vc10 on t1(vc10)");

            e(LANG_INDEX_COLUMN_NOT_EQUAL,
              "values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");

            x("drop index t1_dc");
            x("create index t1_dc on t1(dc)");

            //
            // Everything should be back to normal.
            //

            q("values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1')");
            c1();
            r1();

            // RESOLVE - Next test commented out due to inconsistency
            // in store error message (sane vs. insane).  Check every
            // index once store returns consistent error.
            // insert a row with a bad row location into index call
            // org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker::
            //     insertBadRowLocation('APP', 'T1', 'T1_I');
            // values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'T1');

        } finally {
            // cleanup
            dontThrow(st, "drop table t1");
            commit();

            st = null;
            rs= null;
            expColNames = null;
            expRS = null;
        }
    }

    private void x(String stmt) throws SQLException {

        st.executeUpdate(stmt);
    }

    private void q(String query) throws SQLException {
        rs = st.executeQuery(query);
    }

    private void c1() throws SQLException {
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
    }

    private void r1() throws SQLException {
        expRS = new String [][]
        {
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
    }

    private void e(String expectedState, String stmt) {
        assertStatementError(expectedState, st, stmt);
    }

    private void dontThrow(Statement st, String stm) {
        try {
            st.executeUpdate(stm);
        } catch (SQLException e) {
            // ignore, best effort here
        }
    }
}
