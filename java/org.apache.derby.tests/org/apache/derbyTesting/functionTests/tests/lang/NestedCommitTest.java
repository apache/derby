/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.NestedCommitTest
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test nested commit
 */

public final class NestedCommitTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name test name
     */
    public NestedCommitTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        return TestConfiguration.defaultSuite(NestedCommitTest.class);
    }

    public void testNestedCommit() throws Exception
    {
        ResultSet rs;
        CallableStatement cSt;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;

        st.getConnection().setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

        try {
            // Make sure that we cannot do a commit/rollback on a nested
            // connection when we are in the middle of something that has to be
            // atomic (e.g. DML). commit/rollback on a nested connection is
            // only permitted when we are doing something simple like CALL
            // myMethod() or VALUES myMethod().

            st.executeUpdate("CREATE PROCEDURE doConnCommit() "
                    + "       DYNAMIC RESULT SETS 0 LANGUAGE JAVA "
                    + "       EXTERNAL NAME "
                    + "'org.apache.derbyTesting.functionTests.util.Triggers"
                    + ".doConnCommit' "
                    + "    CONTAINS SQL"
                    + "       PARAMETER STYLE JAVA");

            st.executeUpdate("CREATE PROCEDURE doConnRollback() "
                    + "       DYNAMIC RESULT SETS 0 LANGUAGE JAVA "
                    + "       EXTERNAL NAME "
                    + "'org.apache.derbyTesting.functionTests.util.Triggers"
                    + ".doConnRollback' "
                    + "    CONTAINS SQL"
                    + "       PARAMETER STYLE JAVA");

            st.executeUpdate("CREATE PROCEDURE doConnStmt(IN TEXT CHAR(50)) "
                    + "       DYNAMIC RESULT SETS 0 LANGUAGE JAVA "
                    + "       EXTERNAL NAME "
                    + "'org.apache.derbyTesting.functionTests.util.Triggers"
//IC see: https://issues.apache.org/jira/browse/DERBY-6516
                    + ".doConnStmt' "
                    + "    CONTAINS SQL"
                    + "       PARAMETER STYLE JAVA");

            st.executeUpdate("CREATE FUNCTION doConnCommitInt() "
                    + "       RETURNS INT EXTERNAL NAME "
                    + "'org.apache.derbyTesting.functionTests.util.Triggers"
                    + ".doConnCommitInt' "
                    + "       LANGUAGE JAVA PARAMETER STYLE JAVA");

            st.executeUpdate("CREATE FUNCTION doConnStmtInt(TEXT CHAR(50)) "
                    + "       RETURNS INT EXTERNAL NAME "
                    + "'org.apache.derbyTesting.functionTests.util.Triggers"
//IC see: https://issues.apache.org/jira/browse/DERBY-6516
                    + ".doConnStmtInt' "
                    + "       LANGUAGE JAVA PARAMETER STYLE JAVA");

            st.executeUpdate("create table x (x int)");
            st.executeUpdate("insert into x values 1,2,3,4");

            setAutoCommit(false);

            // All the following calls should succeed
            //
            cSt = prepareCall("call doConnCommit()");
            assertUpdateCount(cSt, 0);

            cSt = prepareCall("call doConnRollback()");
            assertUpdateCount(cSt, 0);

            // No longer supported as language statements.
            // call doConnStmt('commit'); call doConnStmt('rollback');

            cSt = prepareCall("call doConnStmt('call doConnCommit()')");
            assertUpdateCount(cSt, 0);

            cSt = prepareCall("call doConnStmt('call doConnRollback()')");
            assertUpdateCount(cSt, 0);

            // call doConnStmt('call doConnStmt(''call
            // doConnStmt(''''commit'''')'')');

            rs = st.executeQuery("values doConnCommitInt()");
            assertTrue(rs.next());
            try {
                rs.getString(1);
            } catch (SQLException e) {
                assertSQLState("XCL16", e);
            }

            // values doConnStmtInt('commit');
            // values doConnStmtInt('rollback');
            // values doConnStmtInt('call doConnStmt(
            //                     ''call doConnStmt(''''commit'''')'')');

            rs = st.executeQuery(
                    "values doConnStmtInt('values doConnCommitInt()')");
            JDBC.assertFullResultSet(rs, new String [][]{{"1"}}, true);

            // fail

            assertStatementError(
                    "38000", st,
                    "insert into x select x+doConnCommitInt() from x");

            assertStatementError(
                    "38000", st,
                    "delete from x where x in (select x+doConnCommitInt() from x)");

            assertStatementError(
                    "38000", st,
                    "delete from x where x = doConnCommitInt()");

            assertStatementError(
                    "38000", st,
                    "update x set x = doConnCommitInt()");

            // insert into x values doConnStmtInt(
            //          'call doConnStmt(''call doConnStmt(''''commit'''')'')');
            // select doConnStmtInt('call doConnStmt(
            //          ''call doConnStmt(''''rollback'''')'')') from x;

            assertStatementError(
                    "38000", st,
                    "select doConnStmtInt('call doConnStmt(" +
                            "''call doConnCommit()'')') from x");

            cSt = prepareCall(
                "call doConnStmt('set isolation serializable')");
            assertUpdateCount(cSt, 0);

        } finally {
            // clean up
            dontThrow(st, "drop table x");
            dontThrow(st, "drop procedure doConnCommit");
            dontThrow(st, "drop procedure doConnRollback");
            dontThrow(st, "drop function doConnCommitInt");
            dontThrow(st, "drop procedure doConnStmt");
            dontThrow(st, "drop function doConnStmtInt");
            commit();
        }
    }

    private void dontThrow(Statement st, String stm) {
        try {
            st.executeUpdate(stm);
        } catch (SQLException e) {
            // ignore, best effort here
        }
    }
}
