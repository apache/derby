/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.store.AccessTest

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
   http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*/

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.AssertionFailedError;
import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;


public final class AccessTest extends BaseJDBCTestCase {

    /**
     * Array with names of database properties that may be modified by
     * the test cases in this class. The properties will be cleared in
     * {@link #tearDown()}.
     */
    private static final String[] MODIFIED_DB_PROPS = {
        "derby.storage.pageSize",
        "derby.storage.minimumRecordSize",
        "derby.storage.pageReservedSpace",
    };

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public AccessTest(String name)
    {
        super(name);
    }
    
    public static Test suite() {
        Properties sysProps = new Properties();
        sysProps.put("derby.optimizer.optimizeJoinOrder", "false");
        sysProps.put("derby.optimizer.ruleBasedOptimization", "true");
        sysProps.put("derby.optimizer.noTimeout", "true");

        Test suite = TestConfiguration.embeddedSuite(AccessTest.class);
        return new CleanDatabaseTestSetup(new SystemPropertyTestSetup(suite, sysProps, true)) {
            /**
             * Creates the table used in the test cases.
             *
             */
            protected void decorateSQL(Statement s) throws SQLException {
                Connection conn = s.getConnection();
                conn.setAutoCommit(false);

                s.execute("CREATE FUNCTION  PADSTRING (DATA VARCHAR(32000), "
                        + "LENGTH INTEGER) RETURNS VARCHAR(32000) EXTERNAL NAME " +
                        "'org.apache.derbyTesting.functionTests.util.Formatters" +
                ".padString' LANGUAGE JAVA PARAMETER STYLE JAVA");
            }
        };
    }    

    /**
     * Tear down the test environment.
     */
    protected void tearDown() throws Exception {
        rollback();
        Statement s = createStatement();
        //DERBY-5119 Table foo is used in lots of fixtures.
        // make sure it gets cleaned up.
        try {
            s.executeUpdate("DROP TABLE FOO");
        } catch (SQLException se) {
            // if the table couldn't drop make sure it is because it doesn't
            // exist
            assertSQLState("42Y55",se);
        }
        // Clear the database properties set by this test so that they
        // don't affect other tests.
        PreparedStatement clearProp = prepareStatement(
                "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, NULL)");
        for (int i = 0; i < MODIFIED_DB_PROPS.length; i++) {
            clearProp.setString(1, MODIFIED_DB_PROPS[i]);
            clearProp.executeUpdate();
        }
        commit();

        super.tearDown();
    }

    //---------------------------------------------------------
    //    test qualifier skip code on fields with length  
    //    having the 8th bit set in low order length byte. 
    // --------------------------------------------------------
    public void testQualifierSkipLOLB() throws Exception
    {

        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;

        setAutoCommit(false);

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '32768')");
        cSt.execute();
        st.executeUpdate("create table a ( " +
                "i1 int, col00 varchar(384), col01 varchar(390), i2 int )");
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        // insert rows
        st.executeUpdate(
                "insert into a values (1, PADSTRING('10',384), "
                + "PADSTRING('100',390), 1000)");
        st.executeUpdate(
                "insert into a values (2, PADSTRING('20',384), "
                + "PADSTRING('200',390), 2000)");
        st.executeUpdate(
                "insert into a values (3, PADSTRING('30',384), "
                + "PADSTRING('300',390), 3000)");

        rs = st.executeQuery("select i1, i2 from a where i2 = 3000");

        expColNames = new String [] {"I1", "I2"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] {{"3", "3000"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table a");
        commit();
    }

    // test case for a fixed bug where the problem was that the btree split 
    // would self deadlock while trying to reclaim rows during the split.
    // Fixed by just giving up if btree can't get the locks during the 
    // reclaim try.
    public void testCSBug2590() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("create table foo (a int, b varchar(900), c int)");

        // insert
        st.executeUpdate("insert into foo values (1, PADSTRING('1',900), 1)");
        st.executeUpdate("insert into foo values (2, PADSTRING('2',900), 1)");
        st.executeUpdate("insert into foo values (3, PADSTRING('3',900), 1)");
        st.executeUpdate("insert into foo values (4, PADSTRING('4',900), 1)");
        st.executeUpdate("insert into foo values (5, PADSTRING('5',900), 1)");
        st.executeUpdate("insert into foo values (6, PADSTRING('6',900), 1)");
        st.executeUpdate("insert into foo values (7, PADSTRING('7',900), 1)");
        st.executeUpdate("insert into foo values (8, PADSTRING('8',900), 1)");

        CallableStatement cSt;
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '4096')");
        cSt.execute();
        st.executeUpdate("create index foox on foo (a, b)");
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();
        commit();

        assertUpdateCount(st, 7, "delete from foo where foo.a <> 2");

        ResultSet rs = null;
        String [][] expRS;

        // Test full cursor for update scan over all the rows in the heap,  
        // with default group fetch.  Group fetch should be disabled.
        
        rs = st.executeQuery("select a, b, c from foo for update of c");
        expRS = new String [][] {{"2","2","1"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // Do the same, but use a PreparedStatement.
        PreparedStatement ps_scan_cursor = prepareStatement(
            "select a, b, c from foo for update of c");
        ResultSet scan_cursor = ps_scan_cursor.executeQuery();
        expRS = new String [][] {{"2","2","1"}};
        JDBC.assertFullResultSet(scan_cursor, expRS, true);

        // these inserts would cause a lock wait timeout before 
        // the bug fix.
        st.executeUpdate("insert into foo values (1, PADSTRING('11',900), 1)");
        st.executeUpdate("insert into foo values (1, PADSTRING('12',900), 1)");
        st.executeUpdate("insert into foo values (1, PADSTRING('13',900), 1)");
        st.executeUpdate("insert into foo values (1, PADSTRING('14',900), 1)");
        st.executeUpdate("insert into foo values (1, PADSTRING('15',900), 1)");

        commit();
        st.executeUpdate("drop table foo");
        commit();
    }

    // test case a fixed bug where the problem was that when 
    // the level of btree grew, raw store would incorrectly 
    // report that there was not enough space to move all the   
    // rows from the root page to a newly allocated leaf page, 
    // so the create index operation would fail with a 
    // message saying that a row was too big. create and 
    // load a table with values from 1024 down to 1, the 
    // reverse order is important to reproduce the bug.
    public void testCSBug735() throws Exception
    {

        ResultSet rs = null;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;

        st.executeUpdate("create table foo (a int)");
        st.executeUpdate("insert into foo values (1024)");
        st.executeUpdate("insert into foo (select foo.a - 1   from foo)");
        st.executeUpdate("insert into foo (select foo.a - 2   from foo)");
        st.executeUpdate("insert into foo (select foo.a - 4   from foo)");
        st.executeUpdate("insert into foo (select foo.a - 8   from foo)");
        st.executeUpdate("insert into foo (select foo.a - 16  from foo)");
        st.executeUpdate("insert into foo (select foo.a - 32  from foo)");
        st.executeUpdate("insert into foo (select foo.a - 64  from foo)");
        st.executeUpdate("insert into foo (select foo.a - 128 from foo)");
        st.executeUpdate("insert into foo (select foo.a - 256 from foo)");
        st.executeUpdate("insert into foo (select foo.a - 512 from foo)");
        
        // this create index used to fail.
        assertEquals(0, st.executeUpdate("create index a on foo (a)"));

        // Check the consistency of the indexes
        rs = st.executeQuery(
            "VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'FOO')");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][] {{"1"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // a delete of the whole table also exercises the index well.
        assertUpdateCount(st, 1024, "delete from foo");

        st.executeUpdate("drop table foo");
    }

    // ---------------------------------------------------------
    // stress the conglomerate directory.  
    // abort of an alter table will clear the cache. 
    // ---------------------------------------------------------
    public void test_conglomDirectory() throws Exception
    {
        ResultSet rs = null;
        Statement st = createStatement();

        String [] expColNames;
        setAutoCommit(false);

        st.executeUpdate("create table a (a int)");
        commit();
        st.executeUpdate("alter table a add column c1 int");

        rollback();

        rs = st.executeQuery("select * from a");

        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        st.executeUpdate("drop table a");
        commit();
    }

    // ---------------------------------------------------------
    // ----- test case for partial row runtime statistics. ----- 
    // ---------------------------------------------------------
    /* This test checks correctness of simple runtime statistics.
       It first exercises queries when there's no index present, then with
        an index present; expecting to see a difference between Table Scan 
        and Index Scan. Also of interest is that the qualifiers look right; 
        whether it is using scan start/stop (this is a way to do qualifiers 
        using index).
       Then it does the same 2 actions after some rows have been
        deleted, to exercise the 'deleted rows visited' section in the
        runtime statistics.
       The queries which are cycled through are:
         query1: all columns & rows: "select * from foo"
         query2 - just last column: "select e from foo"
         query3: as subset of columns: "select e, c, a from foo"
         query4: as subset of columns, with qualifier in list: 
             "select e, c, a from foo where foo.e = 5"
         query5: as subset of columns, with qualifier not in list: 
             "select e, c, a from foo where foo.b = 20"
         query6: as subset of columns: "select a, b from foo"
     */
    public void testPartialRowRTStats() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("set ISOLATION to RR");
        setAutoCommit(false);
        
        st.executeUpdate(
            "create table foo (a int, b int, c int, d int, e int)");
        st.executeUpdate("insert into foo values (1, 2, 3, 4, 5)");
        st.executeUpdate("insert into foo values (10, 20, 30, 40, 50)");
        
        // switch on runtime statistics
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        runQueriesNormal(st);

        // now check index scans - force the index just to make sure it 
        // does an index scan. 
        st.executeUpdate("create index foo_cover on foo (e, d, c, b, a)");
        runQueriesWithIndex(st);
        // drop the index...
        st.executeUpdate("drop index foo_cover");
        st.execute("call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','FOO',null)");
        
        // check deleted row feature
        st.executeUpdate("insert into foo values (100, 2, 3, 4, 5)");
        st.executeUpdate("insert into foo values (1000, 2, 3, 4, 5)");
        assertUpdateCount(st, 1, "delete from foo where foo.a = 100");
        assertUpdateCount(st, 1, "delete from foo where foo.a = 1000");
        runQueriesWithDeletedRows(st);
        
        // now check index scans again
        // recreate the index to make sure it does an index scan.
        st.execute("create index foo_cover on foo (e, d, c, b, a)");
        // of course, we'll have to update statistics now before it looks good
        st.execute("call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','FOO',null)");
        // and then we have to re-delete the rows because update statistics would've
        // reset the info about deleted rows.
        st.executeUpdate("insert into foo values (100, 2, 3, 4, 5)");
        st.executeUpdate("insert into foo values (1000, 2, 3, 4, 5)");
        assertUpdateCount(st, 1, "delete from foo where foo.a = 100");
        assertUpdateCount(st, 1, "delete from foo where foo.a = 1000");
        runQueriesWithIndexDeletedRows(st);
        
        st.executeUpdate("drop table foo");
    }
    
    /* method used in testPartialRowRTStats and testCostingCoveredQuery */
    private void assertStatsOK(Statement st, String expectedScan, 
            String expTableInIndexScan, String expIndexInIndexScan, 
            String expBits, String expNumCols, String expDelRowsV,  
            String expPages, String expRowsQ, String expRowsV, 
            String expScanType, String expStartPosition, String expStopPosition,
            String expQualifier, String expQualifierInfo)
    throws SQLException {
        ResultSet rs = st.executeQuery(
            "values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        rs.next();
        RuntimeStatisticsParser rtsp =
            new RuntimeStatisticsParser(rs.getString(1));
        rs.close();

        try {
            if (expectedScan.equals("Table"))
                    assertTrue(rtsp.usedTableScan());
            else if (expectedScan.equals("Index"))
            {
                assertTrue(rtsp.usedIndexScan());
                assertTrue(rtsp.usedSpecificIndexForIndexScan(
                        expTableInIndexScan, expIndexInIndexScan));
            }
            else if (expectedScan.equals("Constraint"))
            {
                assertTrue(rtsp.usedIndexScan());
                assertTrue(rtsp.usedConstraintForIndexScan(
                        expTableInIndexScan));
            }
            assertTrue(rtsp.findString("Bit set of columns fetched="+expBits, 1));
            assertTrue(rtsp.findString("Number of columns fetched="+expNumCols, 1));
            if (expDelRowsV!=null)
                assertTrue(rtsp.findString("Number of deleted rows visited="+expDelRowsV, 1));


            assertTrue(
                "RuntimeStatisticsParser.findstring(Number of pages visited= "
                    + expPages + ") returned false" +
                "full runtime statistics = " + rtsp.toString(),
                rtsp.findString("Number of pages visited=" + expPages, 1));

            assertTrue(rtsp.findString("Number of rows qualified="+expRowsQ, 1));            
            assertTrue(rtsp.findString("Number of rows visited="+expRowsV, 1));
            assertTrue(rtsp.findString("Scan type="+expScanType, 1));
            assertTrue(rtsp.getStartPosition()[1].indexOf(expStartPosition)>1);
            assertTrue(rtsp.getStopPosition()[1].indexOf(expStopPosition)>1);

            if (expQualifier.equals("None"))
                assertTrue(rtsp.hasNoQualifiers());
            else if (expQualifier.equals("Equals"))
                assertTrue(rtsp.hasEqualsQualifier());
            if (expQualifierInfo !=null)
                assertTrue(rtsp.findString(expQualifierInfo, 1));
        } catch (AssertionFailedError e) {
            // One of the assertions failed. Report the full statistics
            // to help debugging.
            fail("Statistics didn't match:\n" + rtsp.toString(), e);
        }
    }
    
    private void runQueriesNormal(Statement st) throws SQLException { 
        doQuery1(st);
        assertStatsOK(st, 
            "Table", null, null, "All", "5", null, "1", "2", "2", 
            "heap","null","null","None", null);
        
        doQuery2(st);
        assertStatsOK(st, 
            "Table", null, null, "{4}", "1", null, "1", "2", "2", 
            "heap","null","null","None", null);

        doQuery3(st);
        assertStatsOK(st, 
            "Table", null, null, "{0, 2, 4}", "3", null, "1", "2", "2", 
            "heap","null","null","None", null);

        doQuery4(st);
        assertStatsOK(st, 
            "Table", null, null, "{0, 2, 4}", "3", null, "1", "1", "2", 
            "heap","null","null","Equals","Column[0][0] Id: 4");

        doQuery5(st);
        assertStatsOK(st, 
            "Table", null, null, "{0, 1, 2, 4}", "4", null, "1", "1", "2", 
            "heap","null","null","Equals","Column[0][0] Id: 1");

        doQuery6(st);
        assertStatsOK(st, 
            "Table", null, null, "{0, 1}", "2", null, "1", "2", "2", 
            "heap","null","null","None",null);
    }
    
    private void runQueriesWithIndex(Statement st) throws SQLException {
        doQuery1(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0, 1, 2, 3, 4}", "5", "0", "1", "2", "2", "btree",
            "None","None","None", null);

        doQuery2(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0}", "1", "0", "1", "2", "2", "btree",
            "None","None","None", null);

        doQuery3(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0, 2, 4}", "3", "0", "1", "2", "2", "btree",
            "None","None","None", null);

        doQuery4(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0, 2, 4}", "3", "0", "1", "1", "2", 
            "btree",">= on first 1 column(s).","> on first 1 column(s).","None", null);

        doQuery5(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0, 2, 3, 4}", "4", "0", "1", "1", "2", "btree",
            "None","None","Equals", "Column[0][0] Id: 3");

        doQuery6(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{3, 4}", "2", "0", "1", "2", "2", "btree",
            "None","None","None", null);
    }
    
    private void runQueriesWithDeletedRows(Statement st) throws SQLException {
        doQuery1(st);
        assertStatsOK(st, 
            "Table", null, null, "All", "5", null, "1", "2", "4", "heap",
            "null","null","None", null);

        doQuery2(st);
        assertStatsOK(st, 
            "Table", null, null, "{4}", "1", null, "1", "2", "4", "heap",
            "null","null","None", null);

        doQuery3(st);
        assertStatsOK(st, 
            "Table", null, null, "{0, 2, 4}", "3", null, "1", "2", "4", "heap",
            "null","null","None", null);

        doQuery4(st);
        assertStatsOK(st, 
            "Table", null, null, "{0, 2, 4}", "3", null, "1", "1", "4", "heap",
            "null","null","Equals","Column[0][0] Id: 4");

        doQuery5(st);
        assertStatsOK(st, 
            "Table", null, null, "{0, 1, 2, 4}", "4", null, "1", "1", "4", "heap",
            "null","null","Equals","Column[0][0] Id: 1");

        doQuery6(st);
        assertStatsOK(st, 
            "Table", null, null, "{0, 1}", "2", null, "1", "2", "4", "heap",
            "null","null","None",null);
    }
    
    private void runQueriesWithIndexDeletedRows(Statement st) throws SQLException {
        doQuery1(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0, 1, 2, 3, 4}", "5", "2", "1", "2", "4", "btree",
            "None","None","None", null);

        doQuery2(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0}", "1", "2", "1", "2", "4", "btree",
            "None","None","None", null);

        doQuery3(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0, 2, 4}", "3", "2", "1", "2", "4", "btree",
            "None","None","None", null);

        doQuery4(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0, 2, 4}", "3", "2", "1", "1", "4", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        doQuery5(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{0, 2, 3, 4}", "4", "2", "1", "1", "4", "btree",
            "None","None","Equals", "Column[0][0] Id: 3");

        doQuery6(st);
        assertStatsOK(st, 
            "Index", "FOO", "FOO_COVER", 
            "{3, 4}", "2", "2", "1", "2", "4", "btree",
            "None","None","None", null);
    }
    
    private void doQuery1(Statement st) throws SQLException {
        // all columns and rows
        doQuery(st, "select * from foo",
                new String [] {"A", "B", "C", "D", "E"},
                new String [][] {
                        {"1", "2", "3", "4", "5"},
                        {"10", "20", "30", "40", "50"}});        
    }
    
    private void doQuery2(Statement st) throws SQLException {
        // just last column - should be 5 and 50 
        doQuery(st, "select e from foo", 
                new String[] {"E"}, 
                new String[][] {{"5"},{"50"}});
    }

    private void doQuery3(Statement st) throws SQLException {
        // as subset of columns - should be 5,3,1 and 50,30,10
        doQuery(st, "select e, c, a from foo",
                new String [] {"E", "C", "A"},
                new String [][] {
                        {"5", "3", "1"},
                        {"50", "30", "10"}});
    }

    private void doQuery4(Statement st) throws SQLException {
        // as subset of columns, with qualifier in list - should be 5,3,1
        doQuery(st, "select e, c, a from foo where foo.e = 5",
                new String [] {"E", "C", "A"},
                new String [][]{{"5", "3", "1"}});        
    }
    
    private void doQuery5(Statement st) throws SQLException {
        // as subset of columns, with qualifier not in list; should be 50,30,10 
        doQuery(st, "select e, c, a from foo where foo.b = 20", 
                new String [] {"E", "C", "A"}, 
                new String [][] {{"50", "30", "10"}});        
    }
    
    private void doQuery6(Statement st) throws SQLException {
        // as subset of columns
        doQuery(st, "select a, b from foo", 
                new String [] {"A", "B"}, 
                new String [][] {{"1", "2"},{"10", "20"}}); 
    }
    
    private void doQuery(Statement st,
            String query, String [] expColNames, String[][] expRS) 
    throws SQLException {
        ResultSet rs = null;
        rs = st.executeQuery(query);
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertFullResultSet(rs, expRS, true);        
    }
    
    // ----------------------------------------------------
    //           -- test case for costing - 
    // make sure optimizer picks obvious covered query. 
    // ----------------------------------------------------
    public void testCostingCoveredQuery() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("set ISOLATION to RR");
        setAutoCommit(false);

        st.executeUpdate("create table base_table (a int, b varchar(1000))");
        st.executeUpdate(
            "insert into base_table values (1, PADSTRING('1',1000))");
        st.executeUpdate(
            "insert into base_table values (2, PADSTRING('2',1000))");
        st.executeUpdate(
            "insert into base_table values (3,  PADSTRING('3',1000))");
        st.executeUpdate(
            "insert into base_table values (4,  PADSTRING('4',1000))");
        st.executeUpdate(
            "insert into base_table values (5,  PADSTRING('5',1000))");
        st.executeUpdate(
            "insert into base_table values (6,  PADSTRING('6',1000))");
        st.executeUpdate(
            "insert into base_table values (7,  PADSTRING('7',1000))");
        st.executeUpdate(
        "insert into base_table values (8,  PADSTRING('8',1000))");
        st.executeUpdate(
            "insert into base_table values (9,  PADSTRING('9',1000))");
        st.executeUpdate(
            "insert into base_table values (10, PADSTRING('10',1000))");
        st.executeUpdate("create index cover_idx on base_table(a)");

        // switch on runtime statistics
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        // make sure covered index is chosen
        doQuery(st, "select a from base_table", 
                new String [] {"A"}, 
                new String [][] {{"1"},{"2"},{"3"},{"4"},{"5"},
                                {"6"},{"7"},{"8"},{"9"},{"10"}});
        assertStatsOK(st, 
                "Index", "BASE_TABLE", "COVER_IDX", 
                "{0}", "1", "0", "1", "10", "10", "btree",
                "None","None","None", null);
    }
    
    // ----------------------------------------------------
    //       -- test for key too big error message. -- 
    // ----------------------------------------------------
    public void testKeyTooBigError() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate ("create table d (id int not null, " +
            "t_bigvarchar varchar(400), unique (id))");
        st.executeUpdate("create index t_bigvarchar_ind on d ( t_bigvarchar)");
        st.executeUpdate(
            "alter table d alter t_bigvarchar set data type varchar(4096)");

        String bigString="1111111";
        for (int i=0 ; i<314 ; i++)
            bigString=bigString+"1234567890";
        bigString=bigString+"123456";
        assertStatementError("XSCB6", st,
            "insert into d (id, t_bigvarchar) values (1, '" + bigString + "')");
    }
    
    // ---------------------------------------------------------
    //                  test space for update 
    // ---------------------------------------------------------
    public void testSpaceForUpdate() throws Exception
    {
        CallableStatement cSt;
        Statement st = createStatement();
        
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '1024')");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.minimumRecordSize', '1')");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageReservedSpace', '0')");
        cSt.execute();

        st.executeUpdate("create table testing (a varchar(100))");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
        "'derby.storage.minimumRecordSize', NULL)");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
        "'derby.storage.minimumRecordSize', NULL)");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageReservedSpace', NULL)");
        cSt.execute();

        st.executeUpdate("insert into testing values ('a')");
        for (int i=0 ; i<7 ; i++)
            st.executeUpdate(
                "insert into testing (select testing.a from testing)");

        assertUpdateCount(st, 128,
            "update testing set a = 'abcd' where a = 'a'");

        st.executeUpdate("create index zz on testing (a)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '1024')");
        cSt.execute();

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.minimumRecordSize', '1')");
        cSt.execute();

        st.executeUpdate("create table t1 (a varchar(100))");

        st.executeUpdate("insert into t1 values ('a')");
        for (int i=0 ; i<7 ; i++)
            st.executeUpdate("insert into t1 (select t1.a from t1)");

        assertUpdateCount(st, 128,
        " update t1 set a = 'abcd' where a = 'a'");

        st.executeUpdate("create index zz1 on t1 (a)");
    }
    
        
    // ---------------------------------------------------------
    //     test load with long columns with index creation 
    // ---------------------------------------------------------
    public void testLoadLongColumnsCreateIndex() throws Exception
    {
        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;
        st.executeUpdate("set ISOLATION to RR");
        setAutoCommit(false);

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '1024')");
        cSt.execute();

        st.executeUpdate(
            "create table long1 (a varchar(32000), b int, c int)");

        st.executeUpdate("insert into long1 values (" +
            "'this is a long row which will get even longer and longer " +
            "to force a stream', 1, 2)");
        st.executeUpdate("insert into long1 values (" +
            "'this is another long row which will get even longer " +
            "and longer to force a stream', 2, 3)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        assertUpdateCount(st, 2, "update long1 set a = a||a||a||a||a||a");
        assertUpdateCount(st, 2, "update long1 set a = a||a||a||a||a||a");
        assertUpdateCount(st, 2, "update long1 set a = a||a");

        rs = st.executeQuery("select LENGTH(a) from long1");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"5328"},{"5760"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '1024')");
        cSt.execute();

        st.executeUpdate(
            "create table long2 (a varchar(16384), b int, c int)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '16384')");
        cSt.execute();

        st.executeUpdate("create index long2i1 on long2 (a)");
        st.executeUpdate("create index long2i2 on long2 (a,b)");
        st.executeUpdate("create index long2i3 on long2 (a,b,c)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        st.executeUpdate("insert into long2 select * from long1");

        rs = st.executeQuery("select LENGTH(a) from long2");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"5328"},{"5760"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select LENGTH(a) from long2 " +
            "/*derby_properties index=long2i2*/");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"5328"},{"5760"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // DefectId CS 1346

        st.executeUpdate("insert into long2 select * from long1");

        rs = st.executeQuery("select LENGTH(a) from long2");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"5328"},{"5328"},{"5760"},{"5760"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select LENGTH(a) from long2 " +
            "/*derby_properties index=long2i2*/");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"5328"},{"5328"},{"5760"},{"5760"}};
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 4, "delete from long2");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '1024')");
        cSt.execute();

        st.executeUpdate("create index long2small on long2 (a, c)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        // this small index should cause the insert to fail

        assertStatementError("XSCB6", st, 
            "insert into long2 select * from long1");

        // DefectId CS 1346 the small index should cause this insert 
        // to also fail

        assertStatementError("XSCB6", st,
            "insert into long2 select * from long1");

        rs = st.executeQuery("select LENGTH(a) from long2");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // test case for track 1346

        st.executeUpdate("drop table long1");
        st.executeUpdate("drop table long2");
    }
    
    public void testCS1346() throws Exception
    {
        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;
        st.executeUpdate("set ISOLATION to RR");
        setAutoCommit(false);
        
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '1024')");
        cSt.execute();
        st.executeUpdate(
            "create table long1 (a varchar(32000), b int, c int)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        st.executeUpdate("insert into long1 values ('this is a long row " +
            "which will get even longer', 1, 2)");
        st.executeUpdate("insert into long1 values ('a second row that will " +
            "also grow very long', 2, 3)");

        assertUpdateCount(st, 2, "update long1 set a = a||a||a||a||a||a");
        assertUpdateCount(st, 2, "update long1 set a = a||a||a||a||a||a");
        assertUpdateCount(st, 2, "update long1 set a = a||a");

        rs = st.executeQuery("select LENGTH(a) as x from long1 order by x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"3024"},{"3240"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '1024')");
        cSt.execute();

        st.executeUpdate(
            "create table long2 (a varchar(30000), b int, c int)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '16384')");
        cSt.execute();

        st.executeUpdate("create index long2i1 on long2 (a)");
        st.executeUpdate("create index long2i2 on long2 (b, a)");
        st.executeUpdate("create index long2i3 on long2 (b, a, c)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        st.executeUpdate("insert into long2 select * from long1");
        st.executeUpdate("insert into long2 select * from long1");

        rs = st.executeQuery("select LENGTH(a) as x from long2 order by x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"3024"},{"3024"},{"3240"},{"3240"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table long1");
        st.executeUpdate("drop table long2");

    }

    public void testCS1346b() throws Exception
    {
        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '1024')");
        cSt.execute();

        st.executeUpdate(
            "create table long1 (a varchar(32000), b int, c int)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        st.executeUpdate("insert into long1 values ('this is a long row " +
            "which will get even longer', 1, 2)");
        st.executeUpdate("insert into long1 values ('a second row that will "
            + "also grow very long', 2, 3)");

        assertUpdateCount(st, 2, "update long1 set a = a||a||a||a||a||a");
        assertUpdateCount(st, 2, "update long1 set a = a||a||a||a||a||a");
        assertUpdateCount(st, 2, "update long1 set a = a||a");

        rs = st.executeQuery("select LENGTH(a) as x from long1 order by x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"3024"},{"3240"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '1024')");
        cSt.execute();

        st.executeUpdate(
            "create table long2 (a varchar(32000), b int, c int)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '16384')");
        cSt.execute();

        st.executeUpdate("create index long2i1 on long2 (a)");
        st.executeUpdate("create index long2i2 on long2 (b, a)");
        st.executeUpdate("create index long2i3 on long2 (b, a, c)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        // insert into the second table multiple times
        for (int i=0 ; i<10 ; i++)
            st.executeUpdate("insert into long2 select * from long1");

        rs = st.executeQuery("select LENGTH(a) as x from long2 order by x");

        expColNames = new String [] {"X"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [20][1];
        //10 rows should have '3024' and 10 '3240 as length
        for (int i=0 ; i<10 ; i++)
            expRS[i][0]="3024";
        for (int i=10 ; i<20 ; i++)
            expRS[i][0]="3240";
        JDBC.assertFullResultSet(rs, expRS, true);
        rs = st.executeQuery("select count(*) from long2");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"20"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table long1");
        st.executeUpdate("drop table long2");
    }

    // regression test case for a Cloudscape era bug, 1552
    // Make sure that a full scan which needs columns not in index
    // does not use the index.
    // Before the fix, access costing would make the optimizer 
    // pick the index because it incorrectly costed rows spanning pages.
    public void testCS1552() throws Exception
    {
        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize','4096')");
        cSt.execute();

        st.executeUpdate("create table a " +
            "(a int, b varchar(4000), c varchar(4000), d varchar(4000))");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        st.executeUpdate("create index a_idx on a (a)");

        st.executeUpdate("insert into a values (5, PADSTRING('a',4000), " + 
            "PADSTRING('a',4000), PADSTRING('a',4000))");
        st.executeUpdate("insert into a values (4, PADSTRING('a',4000), " + 
            "PADSTRING('a',4000), PADSTRING('a',4000))");
        st.executeUpdate("insert into a values (3, PADSTRING('a',4000), " +
            "PADSTRING('a',4000), PADSTRING('a',4000))");
        st.executeUpdate("insert into a values (2, PADSTRING('a',4000), " +
            "PADSTRING('a',4000), PADSTRING('a',4000))");
        st.executeUpdate("insert into a values (1, PADSTRING('a',4000), " +
            "PADSTRING('a',4000), PADSTRING('a',4000))");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        cSt.execute();

        rs = st.executeQuery("select a, d from a");

        expColNames = new String [] {"A", "D"};
        JDBC.assertColumnNames(rs, expColNames);
        String paddeda = Formatters.padString("a", 4000);
        expRS = new String[][] {
            {"5", paddeda},
            {"4", paddeda}, 
            {"3", paddeda}, 
            {"2", paddeda}, 
            {"1", paddeda}}; 
        JDBC.assertFullResultSet(rs, expRS, true);
        assertStatsOK(st, 
            "Table", null, null, "{0, 3}", "2", null, "6", "5", "5", 
            "heap","null","null","None", null);
        
        st.execute("drop table a");
        commit();
    }
    
    // test case for track 2241"};
    // The problem was that when the level of btree grew, 
    // sometimes a long row would be chosen as the branch 
    // delimiter, and the branch code did not throw the 
    // correct error noSpaceForKey error.
    public void testCS2241() throws Exception
    {
        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.minimumRecordSize', NULL)");
        cSt.execute();

        st.executeUpdate("create table b2241 (a int, b varchar(32000))");
        st.executeUpdate("insert into b2241 values (1024, " +
            "'01234567890123456789012345678901234567890123456789')");
        String inshalf1 = "insert into b2241 (select b2241.a + ";
        String inshalf2 = ", b from b2241)";
        st.executeUpdate(inshalf1 + "1" + inshalf2);
        st.executeUpdate(inshalf1 + "2" + inshalf2);
        st.executeUpdate(inshalf1 + "4" + inshalf2);
        st.executeUpdate(inshalf1 + "8" + inshalf2);
        st.executeUpdate(inshalf1 + "16" + inshalf2);
        st.executeUpdate(inshalf1 + "32" + inshalf2);
        st.executeUpdate(inshalf1 + "64" + inshalf2);
        for (int i=0 ; i<5  ; i++)
            assertUpdateCount(st, 128, "update b2241 set b = b||b");
        rs = st.executeQuery("select LENGTH(b) from b2241 where a = 1025");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"1600"}};
        JDBC.assertFullResultSet(rs, expRS, true);
        st.executeUpdate(
                " insert into b2241 (select 1, "
                + "b||b||b||b||b||b||b||b from b2241 where a = 1024)");
        st.executeUpdate(
                " insert into b2241 (select 8000, "
                + "b||b||b||b||b||b||b||b from b2241 where a = 1024)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '4096')");
        cSt.execute();

        // this create index use to fail with an assert - should 
        // fail with key too big error.
        assertStatementError("XSCB6", st, "create index a on b2241 (b, a)");
        // make sure table still accessable, by doing the same statement
        assertStatementError("XSCB6", st, "create index a on b2241 (b, a)");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();

        // delete 2 big records and then index should work.
        assertUpdateCount(st, 1, "delete from b2241 where a = 1");
        assertUpdateCount(st, 1, "delete from b2241 where a = 8000");

        st.executeUpdate("create index a on b2241 (b, a)");

        // Check the consistency of the indexes
        rs = st.executeQuery("VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'B2241')");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"1"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table b2241");
    }

    // o insert bunch of rows with sequential keys.
    // o create an index (non unique or unique)
    // o delete every other one - will make normat post commit not fire.
    // o commit
    // o now reinsert rows into the "holes" which before the fix 
    //   would cause splits, but now will force reclaim space and 
    //   reuse existing space in btree.
    private void reclaimTest(String createIndex, String expectedError) 
    throws SQLException {
        CallableStatement cSt;
        setAutoCommit(false);
        Statement st = createStatement();

        // set page size to default.
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '4096')");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.minimumRecordSize', '1')");
        cSt.execute();
        cSt.close();        
        commit();

        // create and load a table with values from 1024 down to 1,
        st.executeUpdate("create table foo (a int, b char(200), c int)");
        st.executeUpdate("insert into foo values (1024, 'even', 0)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 1, 'odd' , 1 from foo)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 2, foo.b, foo.c from foo)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 4, foo.b, foo.c from foo)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 8, foo.b, foo.c from foo)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 16, foo.b, foo.c from foo)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 32, foo.b, foo.c from foo)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 64, foo.b, foo.c from foo)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 128, foo.b, foo.c from foo)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 256, foo.b, foo.c from foo)");
        st.executeUpdate("insert into foo " +
            "(select foo.a - 512, foo.b, foo.c from foo)");

        // insert into the "holes", but different keys (even2 instead of even)
        st.executeUpdate("create table foo2 (a int, b char(200), c int)");
        st.executeUpdate("insert into foo2 (select * from foo)");

        assertUpdateCount(st, 512, "delete from foo2 where foo2.c = 1");

        // create "packed" index.
        st.executeUpdate(createIndex);

        // delete ever other row
        assertUpdateCount(st, 512, "delete from foo where foo.c = 0");

        // turn all the deletes into "committed deletes"
        commit();
        st.executeUpdate("insert into foo " +
            "(select foo2.a, 'even2', foo2.c from foo2)");
        commit();

        // insert dups
        if (expectedError !=  null)
            assertStatementError("23505", st, "insert into foo " +
            "(select foo2.a, 'even2', foo2.c from foo2)");
        else
            assertUpdateCount(st, 512, "insert into foo " +
            "(select foo2.a, 'even2', foo2.c from foo2)");
        commit();

        // a delete of the whole table also exercises the btree well.
        if (expectedError !=  null)
        {
            assertUpdateCount(st, 1024, "delete from foo");
            assertUpdateCount(st, 512, "delete from foo2");
        }
        else 
        {
            assertUpdateCount(st, 1536, "delete from foo");
            assertUpdateCount(st, 512, "delete from foo2");
        }
        commit();

        st.executeUpdate("drop table foo");
        st.executeUpdate("drop table foo2");
        commit();
    }
    
    // test case for reclaiming deleted rows during split.
    // actual work is done in method reclaimTest()
    // exercise test case with non-unique index
    public void testReclaimDeletedRowsDuringSplit() throws Exception
    {
        reclaimTest("create index a on foo (a, b)", null);
    }

    // as testReclaimDeletedRowsDuringSplit, but with unique index,
    // so when attempting to create the duplicat rows, we should get an
    // error. 
    // actual work is done in method reclaimTest()
    public void testReclaimDeletedRowsUniqueIndex() throws Exception
    {
        reclaimTest("create unique index a on foo (a, b)", "23505");
    }

    // same foo used in the next 3 test fixtures
    private void setupForReclaim2(Statement st) throws SQLException {
        st.executeUpdate("create table foo (a int, b varchar(1100), c int)");
        st.executeUpdate("create index a on foo (a, b)");
        st.executeUpdate("insert into foo values (1, PADSTRING('a',1100), 1)");
        st.executeUpdate("insert into foo values (2, PADSTRING('a',1100), 1)");
        st.executeUpdate("insert into foo values (3, PADSTRING('a',1100), 1)");
    }
    
    private void reclaimDeletedRows2(boolean toCommit) throws SQLException {
        Statement st = createStatement();

        setupForReclaim2(st);
        commit();

        assertUpdateCount(st, 1, "delete from foo where foo.a = 1");
        assertUpdateCount(st, 1, "delete from foo where foo.a = 2");
        
        if (toCommit)
            commit();
        st.executeUpdate("insert into foo values " +
            "(-1, PADSTRING('ab',1100), 1)");
        st.executeUpdate("insert into foo values " +
            "(-2, PADSTRING('ab',1100), 1)");
        rollback();

        st.executeUpdate("drop table foo");
        commit();
    }
    
    // another simple test of reclaim deleted row code paths. 
    // this test should not reclaim rows as deletes are not committed.
    public void testUncommittedDeletesNotReclaimed() throws Exception
    {
        reclaimDeletedRows2(false);
    }

    // another simple test of reclaim deleted row code paths. 
    // this test should reclaim rows as deletes are committed.
    public void testCommittedDeletesReclaim() throws Exception
    {
        reclaimDeletedRows2(true);
    }

    // this test will not reclaim rows because the parent xact 
    // has table level lock.
    public void testAllUncommittedReclaim() throws Exception
    {
        Statement st = createStatement();

        setupForReclaim2(st);
        assertUpdateCount(st, 1, "delete from foo where foo.a = 1");

        st.executeUpdate("insert into foo values (0, PADSTRING('a',1100), 1)");
        st.executeUpdate("insert into foo values (1, PADSTRING('a',1100), 1)");
        rollback();

        st.executeUpdate("drop table foo");
    }

    // regression test case for Cloudscape fixed bug track 2778
    // Make sure that an update which causes a row to go from a non long row 
    // to a long row can be aborted correctly.
    // Prior to this fix the columns moving off the page would be corrupted. 
    // create a base table that contains 2 rows, 19 columns,  
    // that leaves just 1 byte free on the page.
    // freeSpace: 1, spareSpace: 10, PageSize: 2048
    public void testCS2778() throws Exception
    {
        ResultSet rs = null;
        CallableStatement cSt;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '2048')");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageReservedSpace', '10')");
        cSt.execute();

        st.executeUpdate("create table t2778 (" +
            "col00 char(2), col01 char(1), col02 char(99), col03 char(11), " +
            "col04 char(7), col05 char(11), col06 char(6), col07 char(6), " +
            "col08 char(2), col09 char(6), col10 varchar(1000), " +
            "col11 char(2), col12 char(1), col13 char(7), col14 char(24), " +
            "col15 char(1), col16 char(166), col17 char(207), col18 char(2))");

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', NULL)");
        cSt.execute();
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageReservedSpace', NULL)");
        cSt.execute();

        st.executeUpdate("create unique index a_idx on t2778 (col00)");
        commit();

        st.executeUpdate("insert into t2778 values ( '0_', '0', '0_col02', " +
            "'0_col03', '0_col04', '0_col05', '0_06', '0_07', '0_', '0_09', " +
            "'0_col10lllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllll012340_col10lllllllllll', '0_', '0', '0_col13', " +
            "'0_col14', '0', '0_col16', '0_col17', '0_' )");

        st.executeUpdate("insert into t2778 values ( '1_', '1', '1_col02', " +
            "'1_col03', '1_col04', '1_col05', '1_06', '1_07', '1_', '1_09', " +
            "'1_col10lllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllll012340_col10llllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllll012340_col10lllllllllllxxx" +
            "xxxxxxxxxxxxxxxx', '1_', '1', '1_col13', '1_col14', '1', " +
            "'1_col16', '1_col17', '1_' )");
        commit();

        rs = st.executeQuery(" select col16, col17, col18 from t2778");
        expColNames = new String [] {"COL16", "COL17", "COL18"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{
            {"0_col16", "0_col17", "0_"},
            {"1_col16", "1_col17", "1_"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);
        commit();

        assertUpdateCount(st, 1, "update t2778 " +
            "/*derby-properties index=a_idx*/ set col10 = " +
            "'0_col10lllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllll012340_col10lllllllllllxxxxxx' " +
            "where col00 = '0_'");
        rollback();

        // prior to the fix col17 and col18 would come back null.
        rs = st.executeQuery("select " +
            "col01, col02, col03, col04, col05,  col06, " +
            "col07, col08, col09, col10, col11, col12, col13, " +
            "col14, col15, col16, col17, col18 from t2778");

        expColNames = new String [] {"COL01", "COL02", "COL03", "COL04", 
            "COL05", "COL06", "COL07", "COL08", "COL09", "COL10", "COL11",
            "COL12", "COL13", "COL14", "COL15", "COL16", "COL17", "COL18"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"0", "0_col02", "0_col03", "0_col04", 
            "0_col05", "0_06", "0_07", "0_", "0_09", 
            "0_col10llllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "llllllllll012340_col10lllllllllllxxxxxx", 
            "0_", "0", "0_col13", "0_col14", "0", "0_col16", "0_col17", "0_"},
                                {"1", "1_col02", "1_col03", "1_col04", 
            "1_col05", "1_06", "1_07", "1_", "1_09", 
            "1_col10llllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "llllllllll012340_col10lllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" +
            "llllllllllllllllllllllllllllllllllll012340_col10lllllllllllxxxx" +
            "xxxxxxxxxxxxxxx", "1_", "1", "1_col13", "1_col14", "1", 
            "1_col16", "1_col17", "1_"}
                              };
        JDBC.assertFullResultSet(rs, expRS, true);
        commit();

        st.executeUpdate("drop table t2778");
        commit();
    }

    // test case for Cloudscape track 3149, improving max on btree optimization
    public void testCS3149() throws Exception
    {
        ResultSet rs = null;
        Statement st = createStatement();
        String [][] expRS;
        String [] expColNames;        

        setAutoCommit(false);
        st.executeUpdate("create table foo (a int, b varchar(500), c int)");
        
        String insertPart1 = "insert into foo values (";
        String insertPart2 = ", PADSTRING('";
        String insertPart3 = "',500), 1)";
        for (int i=1 ; i<10 ; i++)
        {
            String s = String.valueOf(i);
            st.executeUpdate(insertPart1 + s + insertPart2 + s + insertPart3);
        }
        for (int i=11 ; i<19 ; i++)
        {
            String s = String.valueOf(i);
            st.executeUpdate(insertPart1 + s + insertPart2 + s + insertPart3);
        }
        st.executeUpdate("create index foox on foo (b)");
        commit();

        // normal max optimization, last row in index is not deleted.
        rs = st.executeQuery("select max(b) from foo");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"9"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // new max optimization, last row in index is deleted but 
        // others on page aren't.
        assertUpdateCount(st, 1, "delete from foo where a = 9");

        rs = st.executeQuery("select max(b) from foo");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"8"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // new max optimization, last row in index is deleted but 
        // others on page aren't.
        assertUpdateCount(st, 1, "delete from foo where a = 8");

        rs = st.executeQuery("select max(b) from foo");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"7"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // max optimization does not work - fail over to scan, all 
        // rows on last page are deleted.
        assertUpdateCount(st, 13, "delete from foo where a > 2");

        rs = st.executeQuery("select max(b) from foo");
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"2"}};
        JDBC.assertFullResultSet(rs, expRS, true);
        commit();

        st.executeUpdate("drop table foo");
        commit();
    }

    //---------------------------------------------------------
    //         regression test for Cloudscape bugs 3368, 3370  
    // the bugs arose for the edge case where pageReservedSpace = 100
    // before bug 3368 was fixed, a short row insert caused 2 pages 
    // to be allocated per short row insert.
    public void testCS3368_3370() throws Exception
    {
        CallableStatement cSt;
        Statement st = createStatement();

        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageReservedSpace', '100')");
        cSt.execute();
        st.executeUpdate("create table a (a int)");
        cSt = prepareCall("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageReservedSpace', NULL)");
        cSt.execute();

        st.executeUpdate("insert into a values (1)");

        checkSpaceTable(st, "2");

        st.executeUpdate("insert into a values (2)");

        checkSpaceTable(st, "3");

        st.executeUpdate("insert into a values (1)");

        checkSpaceTable(st, "4");

        st.executeUpdate("insert into a values (2)");

        checkSpaceTable(st, "5");

        st.executeUpdate("drop table a");
    }
    
    private void checkSpaceTable(Statement st, String expValue)
    throws SQLException {
        ResultSet rs = null;
        String [][] expRS;
        
        rs = st.executeQuery("select numallocatedpages from TABLE" +
                "(SYSCS_DIAG.SPACE_TABLE('APP', 'A')) a");
        expRS = new String [][]{{expValue}};
        JDBC.assertFullResultSet(rs, expRS, true);        
    }

    //---------------------------------------------------------
    // regression test for old Cloudscape bug track 4595,
    // following are 2 test cases that get cycled with 3 different indexes:
    // 1. unique index
    // 2. primary key
    // 3. non unique index
    // The 2 test cases are:
    // a. do delete, update and select without any rows (and check statistics)
    //    then insert a row, and do update and delete
    // b. do delete, and update after inserting a row (and check stats)
    //    then do the same selects as in test case a
    private void doTestCaseCS4595A (Statement st, String indexOrConstraint) 
    throws SQLException {
        ResultSet rs = null;
        String [] expColNames;
        
        String indexName;
        if (indexOrConstraint.equals("Index"))
            indexName="FOOX";
        else 
            indexName=null;

        st.executeUpdate("set ISOLATION to RR");
        
        // delete against table with 0 rows.
        assertUpdateCount(st, 0, "delete from foo where a = 1");

        // make sure index used in unique key update even if table has zero rows.
        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "{0, 1}", "2", "0", "1", "0", "0", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        // update against table with 0 rows. 
        
        assertUpdateCount(st, 0, "update foo set b = 1 where a = 2");

        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "All", "2", "0", "1", "0", "0", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        // select * against table with 0 rows. 
        rs = st.executeQuery("select * from foo where a = 2");

        JDBC.assertEmpty(rs);
        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "All", "2", "0", "1", "0", "0", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        // select against table with 0 rows
        rs = st.executeQuery("select a from foo where a = 2");

        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "{0}", "1", "0", "1", "0", "0", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        // select against table with 0 rows.
        // second time should give slightly different statistics; different
        // set of rows fetched.
        rs = st.executeQuery("select a from foo where a = 2");

        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "{0}", "1", "0", "1", "0", "0", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);
        
        // now insert one row and make sure still same plan.  
        // Previous to 4595, 0 row plan was a table scan and it would not 
        // change when 1 row was inserted.
        st.execute("insert into foo values (1, 1)");

        // update against table with 1 row.
        assertUpdateCount(st, 1, "update foo set b = 2 where a = 1");

        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "All", "2", "0", "1", "1", "1", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        // delete against table with 1 row.
        st.execute("delete from foo where a = 1");

        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "{0, 1}", "2", "0", "1", "1", "1", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        st.execute("drop table foo");
        commit();
    }
    
    private void doTestCaseCS4595B(Statement st, String indexOrConstraint) throws SQLException {
        ResultSet rs = null;
        String [] expColNames;

        String indexName;
        if (indexOrConstraint.equals("Index"))
            indexName="FOOX";
        else 
            indexName=null;
        
        commit();
        
        // update against table with 1 row.
        assertUpdateCount(st, 1, "update foo set b = 2 where a = 1");

        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "All", "2", "0", "1", "1", "1", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        // delete against table with 1 row.
        st.execute("delete from foo where a = 1");

        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "{0, 1}", "2", "0", "1", "1", "1", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);
        
        rs = st.executeQuery("select * from foo where a = 2");

        JDBC.assertEmpty(rs);
        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "All", "2", "0", "1", "0", "0", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        // select against table with 0 rows
        rs = st.executeQuery("select a from foo where a = 2");

        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "{0}", "1", "0", "1", "0", "0", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        // select against table with 0 rows.
        // second time should give slightly different statistics; different
        // set of rows fetched.
        rs = st.executeQuery("select a from foo where a = 2");

        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        assertStatsOK(st, 
            indexOrConstraint, "FOO", indexName, 
            "{0}", "1", "0", "1", "0", "0", "btree",
            ">= on first 1 column(s).","> on first 1 column(s).","None", null);

        st.execute("drop table foo");
    }

    public void testCS4595A_UniqueIndex() throws Exception
    {
        Statement st = createStatement();
        
        st.executeUpdate("set ISOLATION to RR");
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        st.executeUpdate("create table foo (a int, b int)");
        st.executeUpdate("create unique index foox on foo (a)");

        doTestCaseCS4595A(st, "Index");
    }

    // try delete/update statement compiled against table with 1 row.
    public void testCS4595B_UniqueIndex() throws Exception
    {
        Statement st = createStatement();

        st.executeUpdate("set ISOLATION to RR");
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        st.executeUpdate("create table foo (a int, b int)");
        // this time, insert a row before creating an index
        st.executeUpdate("insert into foo values (1, 1)");
        st.executeUpdate("create unique index foox on foo (a)");

        doTestCaseCS4595B(st, "Index");
    }

    // repeat set of testCS459_a against table with primary key, 
    // vs. unique index 
    // there should be no difference in plan shape. 
    // try delete/update statement compiled against table with 0 rows
    public void testCS4595A_PrimaryKey() throws Exception
    {
        Statement st = createStatement();

        st.executeUpdate("set ISOLATION to RR");
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        st.executeUpdate(
            "create table foo (a int not null primary key, b int)");
        
        doTestCaseCS4595A(st, "Constraint");
    }

    // try delete/update statement compiled against table with 1 row.
    // With primary key.
    public void testCS4595B_PrimaryKey() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("set ISOLATION to RR");
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        st.executeUpdate(
            "create table foo (a int not null primary key, b int)");
        st.executeUpdate("insert into foo values (1, 1)");
        
        doTestCaseCS4595B(st, "Constraint");
    }

    // repeat set of 4595 tests against table with non-unique index 
    // with no statistics.
    // there should be no difference in plan shape.
    // try delete/update statement compiled against table with 0 rows
    public void testCaseCS4595A_NonUniqueIndex() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("set ISOLATION to RR");
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        st.executeUpdate("create table foo (a int, b int)");
        st.executeUpdate("create index foox on foo (a)");

        doTestCaseCS4595A(st, "Index");
    }

    // try delete/update statement compiled against table with 1 row.
    public void testCaseCS4595B_NonUniqueIndex() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("set ISOLATION to RR");
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        st.executeUpdate("create table foo (a int, b int)");
        st.executeUpdate("create index foox on foo (a)");
        st.executeUpdate("insert into foo values (1, 1)");
        
        doTestCaseCS4595B(st, "Index");
    }

    // ----------------------------------------------------
    //        simple regression test for qualifier work. 
    // ----------------------------------------------------
    public void testQualifiers() throws Exception
    {
        setAutoCommit(false);

        ResultSet rs = null;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;

        st.executeUpdate("create table foo (a int, b int, c int)");
        st.executeUpdate("insert into foo values (1, 10, 100)");
        st.executeUpdate("insert into foo values (2, 20, 200)");
        st.executeUpdate("insert into foo values (3, 30, 300)");

        // should return no rows
        rs = st.executeQuery("select a, b, c from foo where a = 1 and b = 20");

        expColNames = new String [] {"A", "B", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // should return one row
        rs = st.executeQuery("select a, b, c from foo where a = 3 and b = 30");

        expColNames = new String [] {"A", "B", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"3", "30", "300"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select a, b, c from foo where a = 3 or c = 40");

        expColNames = new String [] {"A", "B", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"3", "30", "300"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // should return 2 rows
        rs = st.executeQuery("select a, b, c from foo where a = 1 or b = 20");

        expColNames = new String [] {"A", "B", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"1", "10", "100"}, {"2", "20", "200"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery("select a, b, c from foo where a = 1 or a = 3");

        expColNames = new String [] {"A", "B", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"1", "10", "100"}, {"3", "30", "300"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        rollback();
        st.close();
    }
}
