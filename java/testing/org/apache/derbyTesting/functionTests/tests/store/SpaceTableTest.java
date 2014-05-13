/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.store.SpaceTableTest

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
package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Properties;

import junit.framework.Assert;
import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests the printing of the WAIT state in the LOCK TABLE.
 */
public class SpaceTableTest extends BaseJDBCTestCase {

    public SpaceTableTest(String name) {
        super(name);
    }

    /**
     * Construct top level suite in this JUnit test
     * The suite is wrapped in a DatabasePropertyTestSetup to set
     * the lock wait timeout.
     *
     * @return A suite containing embedded fixtures
     */
    public static Test suite() {

        Test suite = TestConfiguration.embeddedSuite (SpaceTableTest.class);
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the schemas and table used in the test cases.
             *
             * @throws SQLException
             */
            protected void decorateSQL(Statement s) throws SQLException {
                Connection conn = getConnection();
                conn.setAutoCommit(false);
                String createWaitForPostCommit=
                    "CREATE PROCEDURE WAIT_FOR_POST_COMMIT() " +
                        "DYNAMIC RESULT SETS 0 " +
                        "LANGUAGE JAVA EXTERNAL NAME " +
                        "'org.apache.derbyTesting.functionTests.util.T_Access." +
                            "waitForPostCommitToFinish' " +
                        "PARAMETER STYLE JAVA";
                s.executeUpdate(createWaitForPostCommit);
                conn.commit();
                conn.setAutoCommit(true);
            }
        };
    }

    
    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Tear-down the fixture by removing the tables and schemas
     * @throws Exception
     */
    protected void tearDown() throws Exception {
        try {
        Statement stmt = createStatement();
        // cannot drop wait_for_post_commit or it will not exist in
        // all test methods. CleanDatabaseSetup should take care of it.
        // stmt.executeUpdate("drop procedure WAIT_FOR_POST_COMMIT");
        stmt.executeUpdate("drop table IDELETEU");
        stmt.executeUpdate("drop table platypus");
        stmt.executeUpdate("drop table 'platypus2'");
        dropFooTables(stmt);
        // force pagesize back to default
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', NULL)");
        } catch (SQLException sqe) {
            assertSQLState("42Y55", sqe);
        }
        super.tearDown();
    }

    public void testIDeleteu() throws SQLException, InterruptedException {
        Statement stmt = createStatement();
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', '4096')");
        stmt.executeUpdate("create table ideleteu " +
                "(a varchar(2000), b varchar(2000))");
        String insertString = "insert into ideleteu values ('" +
                Formatters.padString("rrrrrrrrrr",2000) + "', '" +
                Formatters.padString("ssssssssssssssss",2000) + "')";
        for (int i=0; i<5; i++)
        stmt.executeUpdate(insertString);
        
        assertSpaceTableOK("IDELETEU", 
                new String[][]{{"IDELETEU","0","6","0","4096","0"}}, false);
        
        stmt.executeUpdate("delete from ideleteu");
        // explicit commit, superfluous, as autocommit set in decorateSQL
        commit();
        stmt.execute("call WAIT_FOR_POST_COMMIT()");
        // this is one of 2 places where in the sql version of this test we
        // still saw highly intermittent diffs even with the wait for post commit
        // see DERBY-5133. So call to local check.
        assertSpaceTableOK("IDELETEU", 
                new String[][]{{"IDELETEU","0","1","5","4096","20480"}}, true);
        stmt.executeUpdate("drop table ideleteu");
        commit();
        // the default is 4096 (with short columns), so no need to set to storage page size back.
    }
    
    public void testPlatypi() throws SQLException, InterruptedException
    {
        // check results when inserting data after creating indexes
        Statement stmt = createStatement();
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', '4096')");
        JDBC.assertEmpty(doSpaceTableSelect("PLATYPUS"));
        stmt.executeUpdate("create table platypus " +
                "(a varchar(1000), b varchar(3500), " +
                "c varchar(400), d varchar(100))");
        stmt.executeUpdate("create index kookaburra on platypus (a)");
        stmt.executeUpdate("create index echidna on platypus (c)");
        stmt.executeUpdate("create index wallaby on platypus (a,c,d)");
        assertSpaceTableOK("PLATYPUS", new String[][]{
                {"ECHIDNA","1","1","0","4096","0"},
                {"KOOKABURRA","1","1","0","4096","0"},
                {"PLATYPUS","0","1","0","4096","0"},
                {"WALLABY","1","1","0","4096","0"},
                }, false);
        String insertString = "insert into platypus values ('" +
                Formatters.padString("wwwwwww",1000) + "', '" +
                Formatters.padString("xxx",3500) + "', '" +
                Formatters.padString("yy",400) + "', '" +
                Formatters.padString("zzz",100) + "')";
        for (int i=0; i<6; i++)
        stmt.executeUpdate(insertString);
        assertSpaceTableOK("PLATYPUS", new String[][]{
                {"ECHIDNA","1","1","0","4096","0"},
                {"KOOKABURRA","1","4","0","4096","0"},
                {"PLATYPUS","0","13","0","4096","0"},
                {"WALLABY","1","8","0","4096","0"},
                }, false);
        for (int i=0; i<4; i++)
        stmt.executeUpdate(insertString);
        assertSpaceTableOK("PLATYPUS", new String[][]{
                {"ECHIDNA","1","3","0","4096","0"},
                {"KOOKABURRA","1","8","0","4096","0"},
                {"PLATYPUS","0","21","0","4096","0"},
                {"WALLABY","1","16","0","4096","0"},
                }, false);
        stmt.executeUpdate("delete from platypus");
        // we've got autocommit on, so there was a commit...
        stmt.execute("call WAIT_FOR_POST_COMMIT()");
        assertSpaceTableOK("PLATYPUS", new String[][]{
                {"ECHIDNA","1","1","2","4096","8192"},
                {"KOOKABURRA","1","1","7","4096","28672"},
                {"PLATYPUS","0","1","20","4096","81920"},
                {"WALLABY","1","15","1","4096","4096"},
                }, true);
        
        // check the results when we create indexes after inserting data
        // also note this table has lower case name
        stmt.executeUpdate("create table \"platypus2\" " +
                "(a varchar(10), b varchar(1500), " +
                "c varchar(400), d varchar(100))");
        insertString = "insert into \"platypus2\" values ('" +
                Formatters.padString("wwwwwww",10) + "', '" +
                Formatters.padString("xxx",1500) + "', '" +
                Formatters.padString("yy",400) + "', '" +
                Formatters.padString("zzz",100) + "')";
        for (int i=0; i<6; i++)
        stmt.executeUpdate(insertString);
        stmt.executeUpdate("create index kookaburra2 on \"platypus2\" (a)");
        stmt.executeUpdate("create index echidna2 on \"platypus2\" (c)");
        stmt.executeUpdate("create index wallaby2 on \"platypus2\" (a,c,d)");
        assertSpaceTableOK("platypus2", new String[][]{
                {"ECHIDNA2","1","1","0","4096","0"},
                {"KOOKABURRA2","1","1","0","4096","0"},
                {"WALLABY2","1","1","0","4096","0"},
                {"platypus2","0","6","0","4096","0"},
                }, false);

        ResultSet rs = stmt.executeQuery(
                "select conglomeratename, isindex, numallocatedpages, " +
                "numfreepages, pagesize, estimspacesaving " +
                "from SYS.SYSSCHEMAS s, " +
                "SYS.SYSTABLES t, " +
                "new org.apache.derby.diag.SpaceTable(SCHEMANAME,TABLENAME) v " +
                "where s.SCHEMAID = t.SCHEMAID " +
                "and s.SCHEMANAME = 'APP' " +
                "order by conglomeratename");
        JDBC.assertFullResultSet(rs, new String [][] {
                {"ECHIDNA","1","1","2","4096","8192"},
                {"ECHIDNA2","1","1","0","4096","0"},
                {"KOOKABURRA","1","1","7","4096","28672"},
                {"KOOKABURRA2","1","1","0","4096","0"},
                {"PLATYPUS","0","1","20","4096","81920"},
                {"WALLABY","1","15","1","4096","4096"},
                {"WALLABY2","1","1","0","4096","0"},
                {"platypus2","0","6","0","4096","0"},
                });
        stmt.executeUpdate("drop table platypus");
        stmt.executeUpdate("drop table \"platypus2\"");
    }
    
    public void testReservedSpace() throws SQLException, InterruptedException
    {
        Statement stmt = createStatement();
        // first ensure we're using the default.
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', '4096')");
        setAutoCommit(false);
        
        // no reserved space set - default
        stmt.executeUpdate("create table foo_int (a int)");
        stmt.executeUpdate("create table foo_char (a char(100))");
        stmt.executeUpdate("create table foo_varchar (a varchar(32000))");
        // let the foo_longxxx get created at 32K (the default for long types)
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', NULL)");
        stmt.executeUpdate("create table foo_longvarchar (a long varchar)");
        stmt.executeUpdate("create table foo_longvarbinary " +
                "(a long varchar for bit data)");
        // Back to 4K
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', '4096')");
        stmt.executeUpdate("create table foo_bit (a char(100) for bit data)");
        stmt.executeUpdate("create table foo_varbinary " +
                "(a varchar(1000) for bit data)");
        
        JDBC.assertFullResultSet(doSpaceTableSelect2(), new String [][] {
                {"FOO_BIT","4096"},
                {"FOO_CHAR","4096"},
                {"FOO_INT","4096"},
                {"FOO_LONGVARBINARY","32768"},
                {"FOO_LONGVARCHAR","32768"},
                {"FOO_VARBINARY","4096"},
                {"FOO_VARCHAR","4096"},
                });
        
        dropFooTables(stmt);
        
        // test with 65K reserved space
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.ReservedSpace', '65')");
        stmt.executeUpdate("create table foo_int (a int)");
        stmt.executeUpdate("create table foo_char (a char(100))");
        stmt.executeUpdate("create table foo_varchar (a varchar(32000))");
        // let the foo_longxxx get created at 32K (the default for long types)
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', NULL)");
        stmt.executeUpdate("create table foo_longvarchar (a long varchar)");
        stmt.executeUpdate("create table foo_longvarbinary " +
                "(a long varchar for bit data)");
        // Back to 4K
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', '4096')");
        stmt.executeUpdate("create table foo_bit (a char(100) for bit data)");
        stmt.executeUpdate("create table foo_varbinary " +
                "(a varchar(1000) for bit data)");
        
        // reset the reserved space to default (by setting property to NULL)
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.ReservedSpace', NULL)");
        
        JDBC.assertFullResultSet(doSpaceTableSelect2(), new String [][] {
            {"FOO_BIT","4096"},
            {"FOO_CHAR","4096"},
            {"FOO_INT","4096"},
            {"FOO_LONGVARBINARY","32768"},
            {"FOO_LONGVARCHAR","32768"},
            {"FOO_VARBINARY","4096"},
            {"FOO_VARCHAR","4096"},
            });
        
        dropFooTables(stmt);
        
        // 8K pagesize
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', '8192')");
        stmt.executeUpdate("create table foo_int (a int)");
        stmt.executeUpdate("create table foo_char (a char(100))");
        stmt.executeUpdate("create table foo_varchar (a varchar(32000))");
        stmt.executeUpdate("create table foo_longvarchar (a long varchar)");
        stmt.executeUpdate("create table foo_longvarbinary " +
                "(a long varchar for bit data)");
        stmt.executeUpdate("create table foo_bit (a char(100) for bit data)");
        stmt.executeUpdate("create table foo_varbinary " +
                "(a varchar(1000) for bit data)");
        
        JDBC.assertFullResultSet(doSpaceTableSelect2(), new String [][] {
            {"FOO_BIT","8192"},
            {"FOO_CHAR","8192"},
            {"FOO_INT","8192"},
            {"FOO_LONGVARBINARY","8192"},
            {"FOO_LONGVARCHAR","8192"},
            {"FOO_VARBINARY","8192"},
            {"FOO_VARCHAR","8192"},
            });
        
        dropFooTables(stmt);
        
        // test with commit after setting pageSize at 4096
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.storage.pageSize', '4096')");
        commit();
        stmt.executeUpdate("create table foo_int (a int)");
        stmt.executeUpdate("create table foo_char (a char(100))");
        stmt.executeUpdate("create table foo_varchar (a varchar(32000))");
        stmt.executeUpdate("create table foo_longvarchar (a long varchar)");
        stmt.executeUpdate("create table foo_longvarbinary " +
                "(a long varchar for bit data)");
        stmt.executeUpdate("create table foo_bit (a char(100) for bit data)");
        stmt.executeUpdate("create table foo_varbinary " +
                "(a varchar(1000) for bit data)");
        
        JDBC.assertFullResultSet(doSpaceTableSelect2(), new String [][] {
            {"FOO_BIT","4096"},
            {"FOO_CHAR","4096"},
            {"FOO_INT","4096"},
            {"FOO_LONGVARBINARY","4096"},
            {"FOO_LONGVARCHAR","4096"},
            {"FOO_VARBINARY","4096"},
            {"FOO_VARCHAR","4096"},
            });
        
        commit();
    }
    
    private ResultSet doSpaceTableSelect(String tableName) throws SQLException
    {
        ResultSet rs = null;
        String stmt_str = 
                "select " + 
                        "conglomeratename, " +
                        "isindex, "           + 
                        "numallocatedpages, " + 
                        "numfreepages, "      + 
                        "pagesize, "          + 
                        "estimspacesaving "   + 
                        "from new org.apache.derby.diag.SpaceTable('" +
                        tableName + "') t order by conglomeratename";

        PreparedStatement space_stmt = prepareStatement(stmt_str);
        rs = space_stmt.executeQuery();
        return rs;
    }
    
    private ResultSet doSpaceTableSelect2() throws SQLException
    {
        ResultSet rs = null;
        String stmt_str = 
                "select " + 
                        "v.conglomeratename, pagesize " +
                        "from SYS.SYSSCHEMAS s, SYS.SYSTABLES t, " +
                        "new org.apache.derby.diag.SpaceTable(" +
                        "schemaname, tablename) v " +
                        "where s.schemaid=t.schemaid and " +
                        "conglomeratename in (" +
                        "'FOO_INT', 'FOO_VARCHAR', 'FOO_CHAR', 'FOO_LONGVARCHAR'," +
                        "'FOO_VARBINARY', 'FOO_LONGVARBINARY', 'FOO_BIT') " +
                        "order by 1";

        PreparedStatement space_stmt = prepareStatement(stmt_str);
        rs = space_stmt.executeQuery();
        return rs;
    }
    
    private void assertSpaceTableOK(String tableName, String[][] expRS, 
            boolean trytwice)
            throws SQLException, InterruptedException {

        ResultSet rs = doSpaceTableSelect(tableName);
            String failString = "";
            if (!trytwice)
                JDBC.assertFullResultSet(rs, expRS);
            else
                failString = checkSpaceTable(rs, expRS, false);
            // if we have a failString, we must have been
            // through the checkSpaceTable and had a failure;
            // go do it again, but this time we'll send it through the
            // JDBC class' version.
            if (failString.length() > 0)
                assertSpaceTableOK(tableName, expRS, false);
    }

    // this method is needed because in a very few occassions we've seen
    // instability in a few rows in this test. 
    // In those cases, *if* we see unexpected results, we're going to assume it is
    // a time-related issue and we sleep for a bit, then try 
    public static String checkSpaceTable(
            ResultSet rs,
            Object [][] expectedRows,
            boolean secondtime)
                    throws SQLException
    {
        String failString="";
        int rows;
        ResultSetMetaData rsmd = rs.getMetaData();

        // Assert that we have the right number of columns. If we expect an
        // empty result set, the expected column count is unknown, so don't
        // check.
        if (expectedRows.length > 0) {
            Assert.assertEquals("Unexpected column count:",
                    expectedRows[0].length, rsmd.getColumnCount());
        }

        for (rows = 0; rs.next(); rows++)
        {
            /* If we have more actual rows than expected rows, don't
             * try to assert the row.  Instead just keep iterating
             * to see exactly how many rows the actual result set has.
             */
            if (rows < expectedRows.length)
            {
                failString = assertRowInResultSet(rs, rows + 1,
                        expectedRows[rows]);
            }
        }

        rs.close();

        // And finally, assert the row count.
        Assert.assertEquals("Unexpected row count:", expectedRows.length, rows);
        
        return failString;
    }
    
    private static String assertRowInResultSet(ResultSet rs, int rowNum,
            Object [] expectedRow) throws SQLException
    {
        String assertString = "";
        int cPos = 0;
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 0; i < expectedRow.length; i++)
        {
            cPos = i+1; 
            Object obj;
            // Trim the expected value, if non-null.
            if (expectedRow[i] != null)
                expectedRow[i] = ((String)expectedRow[i]).trim();

            obj = rs.getString(cPos);

            // Trim the rs string.
            if (obj != null)
                obj = ((String)obj).trim();

            boolean ok = (rs.wasNull() && (expectedRow[i] == null))
                    || (!rs.wasNull()
                            && (expectedRow[i] != null)
                            && (expectedRow[i].equals(obj)
                                    || (obj instanceof byte[] // Assumes byte arrays
                                            && Arrays.equals((byte[] )obj,
                                                    (byte[] )expectedRow[i]))));
            if (!ok)
            {
                Object expected = expectedRow[i];
                Object found = obj;
                if (!expected.equals(found))
                {
                    assertString = "Column value mismatch @ column '" +
                            rsmd.getColumnName(cPos) + "', row " + rowNum +
                            ":\n    Expected: >" + expected +
                            "<\n    Found:    >" + found + "<";
                }
            }
        }
        return assertString;
    }
    
    public void dropFooTables(Statement stmt) throws SQLException {
        stmt.executeUpdate("drop table foo_int");
        stmt.executeUpdate("drop table foo_char");
        stmt.executeUpdate("drop table foo_varchar");
        stmt.executeUpdate("drop table foo_longvarchar");
        stmt.executeUpdate("drop table foo_longvarbinary");
        stmt.executeUpdate("drop table foo_bit");
        stmt.executeUpdate("drop table foo_varbinary");
    }
}
