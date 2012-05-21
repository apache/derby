/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.UpdateStatisticsTest
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.IndexStatsUtil;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;

/**
 * Tests for updating the statistics of one index or all the indexes on a
 * table DERBY-269, DERBY-3788.
 * Tests for dropping the statistics of one index or all the indexes on a
 * table DERBY-4115.
 */
public class UpdateStatisticsTest extends BaseJDBCTestCase {

    public UpdateStatisticsTest(String name) {
        super(name);
    }

    public static Test suite() {
        //       Disable automatic index statistics generation. The generation will be
        //       triggered when preparing a statement and this will interfere
        //       with some of the asserts in testUpdateStatistics.
        //       With automatic generation enabled, testUpdateStatistics may
        //       fail intermittently due to timing, mostly when run
        //       with the client driver.
        Test test = TestConfiguration.defaultSuite(UpdateStatisticsTest.class);
        Test statsDisabled = DatabasePropertyTestSetup.singleProperty
            ( test, "derby.storage.indexStats.auto", "false", true );
        return statsDisabled;
    }

    /**
     * Test that parser can work with column and index named STATISTICS and
     *  does not get confused with non-reserved keyword STATISTICS used by
     *  UPDATE and DROP STATISTICS syntax generated internally for
     *  SYSCS_DROP_STATISTICS and SYSCS_UPDATE_STATISTICS
     */
    public void testIndexAndColumnNamedStatistics() throws SQLException {
        String tbl = "T1";
        // Helper object to obtain information about index statistics.
        IndexStatsUtil stats = new IndexStatsUtil(openDefaultConnection());
        Statement s = createStatement();

        // Get the initial count of statistics in the database.
        int initialStatsCount = stats.getStats().length;

        //Notice the name of one of the columns is STATISTICS
        s.executeUpdate("CREATE TABLE t1 (c11 int, statistics int not null)");
        //Notice that the name of the index is STATISTICS which is same as 
        // one of the column names
        s.executeUpdate("CREATE INDEX statistIcs ON t1(c11)");
        s.executeUpdate("INSERT INTO t1 VALUES(1,1)");
        stats.assertNoStatsTable(tbl);
        //Drop the column named STATISTICS and make sure parser doesn't
        // throw an error
        s.executeUpdate("ALTER TABLE t1 DROP statistics");
        //Should still be able to call update/drop statistics on index 
        // STATISTICS
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','STATISTICS')");
        stats.assertTableStats(tbl, 1);
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T1','STATISTICS')");
        stats.assertNoStatsTable(tbl);
        //Add the column named STATISTICS back
        s.executeUpdate("ALTER TABLE t1 ADD COLUMN statistics int");
        stats.assertNoStatsTable(tbl);
        //Update or drop statistics for index named STATISTICS. Note that there
        // is also a column named STATISTICS in the table
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T1','STATISTICS')");
        stats.assertNoStatsTable(tbl);
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','STATISTICS')");
        stats.assertTableStats(tbl, 1);
        s.executeUpdate("DROP TABLE t1");

        // Check that we haven't created some other statistics as a side-effect.
        assertEquals(initialStatsCount, stats.getStats().length);
    }

    /**
     * Test for update statistics
     */
    public void testUpdateAndDropStatistics() throws SQLException {
        // Helper object to obtain information about index statistics.
        IndexStatsUtil stats = new IndexStatsUtil(openDefaultConnection());
        Statement s = createStatement();

        //Calls to update and drop statistics below should fail because 
        // table APP.T1 does not exist
        dropTable("T1");
        assertStatementError("42Y55", s, 
            "CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T1',null)");
        assertStatementError("42Y55", s, 
            "CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1',null)");

        s.executeUpdate("CREATE TABLE t1 (c11 int, c12 varchar(128))");
        //following will pass now because we have created APP.T1
        s.execute("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T1',null)");
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1',null)");
        
        //following should fail because index I1 does not exist on table APP.T1
        assertStatementError("42X65", s, 
                "CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T1','I1')");
        assertStatementError("42X65", s, 
                "CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I1')");
        
        s.executeUpdate("CREATE INDEX i1 on t1(c12)");
        //following will pass now because we have created index I1 on APP.T1
        s.execute("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T1','I1')");
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I1')");

        //The following set of subtest will ensure that when an index is
        //created on a table when there is no data in the table, then Derby
        //will not generate a row for it in sysstatistics table. If the index
        //is created after the table has data on it, there will be a row for
        //it in sysstatistics table. In order to generate statistics for the
        //first index, users can run the stored procedure 
        //SYSCS_UPDATE_STATISTICS
        //So far the table t1 is empty and we have already created index I1 on 
        //it. Since three was no data in the table when index I1 was created,
        //there will be no row in sysstatistics table
        stats.assertNoStats();
        //Now insert some data into t1 and then create a new index on the 
        //table. This will cause sysstatistics table to have one row for this
        //new index. Old index will still not have a row for it in
        //sysstatistics table
        s.executeUpdate("INSERT INTO T1 VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d')");
        s.executeUpdate("CREATE INDEX i2 ON t1(c11)");
        stats.assertStats(1);
        //Drop the statistics on index I2 and then add it back by calling 
        // update statistics
        s.execute("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T1','I2')");
        //Since we dropped the only statistics that existed for table T1, there
        // will no stats found at this point
        stats.assertNoStats();
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I2')");
        //The statistics for index I2 has been added back
        stats.assertStats(1);
        //Now update the statistics for the old index I1 using the new stored
        //procedure. Doing this should add a row for it in sysstatistics table
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I1')");
        stats.assertStats(2);
        //Drop the statistics on index I1 and then add it back by calling 
        // update statistics
        s.execute("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T1','I1')");
        stats.assertStats(1);
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I1')");
        stats.assertStats(2);
        //Drop all the statistics on table T1 and then recreate all the 
        // statisitcs back again
        s.execute("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T1',null)");
        stats.assertNoStats();
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1',null)");
        stats.assertStats(2);
        //Dropping the index should get rid of it's statistics
        s.executeUpdate("DROP INDEX I1");
        stats.assertStats(1);

        //calls to system procedure for update and drop statistics are
        // internally converted into ALTER TABLE ... sql but that generated
        // sql format is not available to end user to issue directly. Write a 
        // test case for these internal sql syntaxes
        assertStatementError("42X01", s, 
            "ALTER TABLE APP.T1 ALL UPDATE STATISTICS");
        assertStatementError("42X01", s, 
            "ALTER TABLE APP.T1 UPDATE STATISTICS I1");
        assertStatementError("42X01", s, 
                "ALTER TABLE APP.T1 ALL DROP STATISTICS");
        assertStatementError("42X01", s, 
                "ALTER TABLE APP.T1 STATISTICS DROP I1");
        //cleanup
        s.executeUpdate("DROP TABLE t1");

        //Try update and drop statistics on global temporary table
		s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit delete rows not logged");
		s.executeUpdate("insert into session.t1 values(11, 1)");
        //following should fail because update/drop statistics can't be issued
		// on global temporary tables
        assertStatementError("42995", s, 
                "CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('SESSION','T1',null)");
        assertStatementError("42995", s, 
                "CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('SESSION','T1',null)");
        
        //Following test will show that updating the statistics will make a
        //query pickup better index compare to prior to statistics availability.
        //
        //Check statistics update causes most efficient index usage
        //Create a table with 2 non-unique indexes on 2 different columns.
        //The indexes are created when the table is still empty and hence
        //there are no statistics available for them in sys.sysstatistics.
        //The table looks as follows
        //        create table t2(c21 int, c22 char(14), c23 char(200))
        //        create index t2i1 on t2(c21)
        //        create index t2i2 on t2(c22)
        //Load the data into the table and running following query will
        //pickup index t2i1 on column c21
        //        select * from t2 where c21=? and c22=?
        //But once you make the statistics available for t2i2, the query
        //will pickup index t2i2 on column c22 for the query above
        //
        //Start of test case for better index selection after statistics
        //availability
        s.executeUpdate("CREATE TABLE t2(c21 int, c22 char(14), c23 char(200))");
        //No statistics will be created for the 2 indexes because the table is 
        //empty
        s.executeUpdate("CREATE INDEX t2i1 ON t2(c21)");
        s.executeUpdate("CREATE INDEX t2i2 ON t2(c22)");
        stats.assertNoStats();
        
        PreparedStatement ps = prepareStatement("INSERT INTO T2 VALUES(?,?,?)");
        for (int i=0; i<1000; i++) {
        	ps.setInt(1, i%2);
            ps.setString(2, "Tuple " +i);
            ps.setString(3, "any value");
            ps.addBatch();
        }
        ps.executeBatch();

		s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
		
		//Executing the query below and looking at it's plan will show that
		//we picked index T2I1 rather than T2I2 because there are no 
		//statistics available for T2I2 to show that it is a better index
		ps = prepareStatement("SELECT * FROM t2 WHERE c21=? AND c22=?");
    	ps.setInt(1, 0);
        ps.setString(2, "Tuple 4");
        JDBC.assertDrainResults(ps.executeQuery());
		RuntimeStatisticsParser rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedSpecificIndexForIndexScan("T2","T2I1"));

		//Running the update statistics below will create statistics for T2I2
		s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T2','T2I2')");
        stats.assertIndexStats("T2I2", 1);

        //Rerunning the query "SELECT * FROM t2 WHERE c21=? AND c22=?" and
        //looking at it's plan will show that this time it picked up more
        //efficient index which is T2I2.
        JDBC.assertDrainResults(ps.executeQuery());
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedSpecificIndexForIndexScan("T2","T2I2"));

		//Drop statistics for T2I2 and we should see that we go back to using
		// T2I1 rather than T2I2
		s.execute("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','T2','T2I2')");
        stats.assertIndexStats("T2I2", 0);

        //Rerunning the query "SELECT * FROM t2 WHERE c21=? AND c22=?" and
        // looking at it's plan will show that this time it picked up T2I1
        // rather than more efficient index T2I2  because no stats exists
        // for T2I2
        JDBC.assertDrainResults(ps.executeQuery());
		rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
		assertTrue(rtsp.usedSpecificIndexForIndexScan("T2","T2I1"));

        //cleanup
        s.executeUpdate("DROP TABLE t2");
        //End of test case for better index selection after statistics
        //availability
        stats.release();
    }

    /**
     * Test that SYSCS_UPDATE_STATISTICS doesn't obtain exclusive locks on
     * the table or rows in the table (DERBY-4274).
     */
    public void testNoExclusiveLockOnTable() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t (x char(1))");
        s.execute("create index ti on t(x)");
        s.execute("insert into t values 'a','b','c','d'");

        setAutoCommit(false);
        s.execute("lock table t in share mode");

        Connection c2 = openDefaultConnection();
        Statement s2 = c2.createStatement();
        // This call used to time out because SYSCS_UPDATE_STATISTICS tried
        // to lock T exclusively.
        s2.execute("call syscs_util.syscs_update_statistics('APP', 'T', null)");
        s2.close();
        c2.close();

        s.execute("drop table t");
        commit();
    }

    /**
     * Regression test case for DERBY-5153. Compilation in parallel with
     * update of statistics sometimes failed on debug builds.
     */
    public void testParallelCompilationAndUpdate() throws Exception {
        setAutoCommit(false);

        // Create and populate a test table with a multi-column index.
        Statement s = createStatement();
        s.execute("create table derby5153(a int, b int, c int, d int)");
        s.execute("create index idx on derby5153(a,b,c,d)");

        PreparedStatement ins =
                prepareStatement("insert into derby5153 values (1,2,3,4)");
        for (int i = 0; i < 100; i++) {
            ins.execute();
        }

        commit();

        // Start a thread that repeatedly updates the statistics for IDX.
        Connection updateConn = openDefaultConnection();
        IndexUpdateThread t =
                new IndexUpdateThread(updateConn, "APP", "DERBY5153", "IDX");
        t.start();

        try {

            // Compile/execute the query a number of times while the index
            // statistics are being updated. This often failed with an assert
            // failure in debug builds before DERBY-5153.
            for (int i = 0; i < 100; i++) {
                ResultSet rs = s.executeQuery(
                        "select * from derby5153 t1, derby5153 t2 " +
                        "where t1.a = t2.a");
                rs.close();
            }

        } finally {

            // Let the update thread know we're done.
            t.done = true;

        }

        t.join();

        // Check if the update thread failed, and report if it did.
        if (t.exception != null) {
            throw t.exception;
        }

        updateConn.close();

        // Clean-up
        dropTable("derby5153");
        commit();
    }
    
    /**
     * Fixed DERBY-5681(When a foreign key constraint on a table is dropped,
     *  the associated statistics row for the conglomerate is not removed.)
     * @throws Exception
     */
    public void testDERBY5681() throws Exception {
        // Helper object to obtain information about index statistics.
        IndexStatsUtil stats = new IndexStatsUtil(openDefaultConnection());
        Statement s = createStatement();
    	
        //Test - primary key constraint
        s.executeUpdate("CREATE TABLE TEST_TAB_1 (c11 int not null,"+
                "c12 int not null, c13 int)");
        stats.assertNoStatsTable("TEST_TAB_1");
        //Insert data into table with no constraint and there will be no stat
        // for that table at this point
        s.executeUpdate("INSERT INTO TEST_TAB_1 VALUES(1,1,1),(2,2,2)");
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','TEST_TAB_1', null)");
        stats.assertNoStatsTable("TEST_TAB_1");
        //Add primary key constraint to the table and now we should find a 
        // statistics row for it
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_PK_1 "+
        		"PRIMARY KEY (c11)");
        stats.assertTableStats("TEST_TAB_1",1);
        //Dropping primary key constraint will drop the corresponding
        // statistics
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_PK_1");
        stats.assertNoStatsTable("TEST_TAB_1");
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','TEST_TAB_1', null)");
        stats.assertNoStatsTable("TEST_TAB_1");
        //Add the primary key constraint back since it will be used by the next
        // test to create foreign key constraint
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_PK_1 "+
        		"PRIMARY KEY (c11)");
        //The statistics for primary key constraint has been added
        stats.assertTableStats("TEST_TAB_1",1);

        //Test - unique key constraint
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_UNQ_1 "+
        		"UNIQUE (c12)");
        stats.assertTableStats("TEST_TAB_1",2);
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_UNQ_1");
        stats.assertTableStats("TEST_TAB_1",1);
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_PK_1");
        stats.assertNoStatsTable("TEST_TAB_1");
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_PK_1 "+
        		"PRIMARY KEY (c11)");
        stats.assertTableStats("TEST_TAB_1",1);

        //Test - unique key constraint on nullable column & non-nullable column
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_UNQ_2 "+
        		"UNIQUE (c12, c13)");
        stats.assertTableStats("TEST_TAB_1",3);
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_UNQ_2");
        stats.assertTableStats("TEST_TAB_1",1);
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_PK_1");
        stats.assertNoStatsTable("TEST_TAB_1");
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_PK_1 "+
        		"PRIMARY KEY (c11)");
        stats.assertTableStats("TEST_TAB_1",1);
        
        //Test - foreign key but no primary key constraint
        s.executeUpdate("CREATE TABLE TEST_TAB_3 (c31 int not null)");
        s.executeUpdate("INSERT INTO TEST_TAB_3 VALUES(1),(2)");
        s.executeUpdate("ALTER TABLE TEST_TAB_3 "+
                "ADD CONSTRAINT TEST_TAB_3_FK_1 "+
        		"FOREIGN KEY(c31) REFERENCES TEST_TAB_1(c11)");
        stats.assertTableStats("TEST_TAB_3",1);
        s.executeUpdate("ALTER TABLE TEST_TAB_3 "+
                "DROP CONSTRAINT TEST_TAB_3_FK_1");
        stats.assertNoStatsTable("TEST_TAB_3");

        //Test - foreign key and primary key constraint
        s.executeUpdate("CREATE TABLE TEST_TAB_2 (c21 int not null)");
        s.executeUpdate("INSERT INTO TEST_TAB_2 VALUES(1),(2)");
        s.executeUpdate("ALTER TABLE TEST_TAB_2 "+
                "ADD CONSTRAINT TEST_TAB_2_PK_1 "+
        		"PRIMARY KEY (c21)");
        stats.assertTableStats("TEST_TAB_2",1);
        //DERBY-5702 Add a foreign key constraint and now we should find 2 rows
        // of statistics for TEST_TAB_2 - 1 for primary key and other for
        // foreign key constraint
        s.executeUpdate("ALTER TABLE TEST_TAB_2 "+
                "ADD CONSTRAINT TEST_TAB_2_FK_1 "+
        		"FOREIGN KEY(c21) REFERENCES TEST_TAB_1(c11)");
        //DERBY-5702 Like primary key earlier, adding foreign key constraint
        // didn't automatically add a statistics row for it. Have to run update
        // statistics manually to get a row added for it's stat
        stats.assertTableStats("TEST_TAB_2",1);
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','TEST_TAB_2', null)");
        stats.assertTableStats("TEST_TAB_2",2);
        //Number of statistics row for TEST_TAB_1 will remain unchanged since
        // it has only primary key defined on it
        stats.assertTableStats("TEST_TAB_1",1);
        s.executeUpdate("ALTER TABLE TEST_TAB_2 "+
                "DROP CONSTRAINT TEST_TAB_2_FK_1");
        //Dropping the foreign key constraint should remove one of the 
        // statistics row for TEST_TAB_2. 
        stats.assertTableStats("TEST_TAB_2",1);
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','TEST_TAB_2', null)");
        stats.assertTableStats("TEST_TAB_2",1);
        s.execute("CALL SYSCS_UTIL.SYSCS_DROP_STATISTICS('APP','TEST_TAB_2', null)");
        //After DERBY-4115 is implemented, we will see no statistics 
        // for TEST_TAB_2 after calling SYSCS_DROP_STATISTICS on it.
        stats.assertNoStatsTable("TEST_TAB_2");
        s.execute("drop table TEST_TAB_2");
        s.execute("drop table TEST_TAB_1");
        stats.release();
    }

    /**
     * Tests that the functionality that drops disposable statistics leaves
     * useful statistics intact.
     */
    public void testDisposableStatsEagerness()
            throws SQLException {
        setAutoCommit(false);
        String tbl = "DISPOSABLE_STATS_EAGERNESS";
        String tbl_fk = tbl + "_FK";
        String nuIdx = "NU_" + tbl;
        Statement stmt = createStatement();

        // Create and populate the foreign key table.
        stmt.executeUpdate("create table " + tbl_fk + "(" +
                "pk1 int generated always as identity)");
        PreparedStatement ps = prepareStatement(
                "insert into " + tbl_fk + " values (DEFAULT)");
        for (int i=1; i <= 1000; i++) {
            ps.executeUpdate();
        }

        // Create and populate the main table.
        stmt.executeUpdate("create table " + tbl + "(" +
                "pk1 int generated always as identity," +
                "pk2 int not null," +
                "mynonunique int, " +
                "fk int not null)");
        ps = prepareStatement("insert into " + tbl +
                " values (DEFAULT, ?, ?, ?)");
        for (int i=1; i <= 1000; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i % 35);
            ps.setInt(3, i);
            ps.executeUpdate();
        }
        
        // Create the various indexes.
        stmt.executeUpdate("alter table " + tbl_fk + " add constraint PK_" +
                tbl_fk + " primary key (pk1)");
        
        stmt.executeUpdate("alter table " + tbl + " add constraint PK_" + tbl +
                " primary key (pk1, pk2)");
        stmt.executeUpdate("alter table " + tbl + " add constraint FK_" + tbl +
                " foreign key (fk) references " + tbl_fk + "(pk1)");
        stmt.executeUpdate("create index " + nuIdx + " on " + tbl +
                "(mynonunique)");
        commit();
        setAutoCommit(true);
        IndexStatsUtil stats = new IndexStatsUtil(getConnection());
        // Expected FK table: 1 (PK only)
        // Expected main table: 2xPK, 1 non-unique, 1 FK = 4
        stats.assertTableStats(tbl_fk, 1);
        IndexStatsUtil.IdxStats tbl_fk_pk_0 = stats.getStatsTable(tbl_fk)[0];
        stats.assertTableStats(tbl, 4);
        IndexStatsUtil.IdxStats[] tbl_stats_0 = stats.getStatsTable(tbl);
        // Avoid timestamp comparison problems on super-fast machines...
        try {
            Thread.sleep(10);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        // Run the update statistics procedure.
        ps = prepareStatement(
                "call syscs_util.syscs_update_statistics('APP', ?, ?)");
        ps.setNull(2, Types.VARCHAR);
        ps.setString(1, tbl);
        ps.execute();
        ps.setString(1, tbl_fk);
        ps.execute();

        // Check the counts.
        stats.assertTableStats(tbl_fk, 1);
        stats.assertTableStats(tbl, 4);
        // Check the timestamps (i.e. were they actually updated?).
        IndexStatsUtil.IdxStats tbl_fk_pk_1 = stats.getStatsTable(tbl_fk)[0];
        assertTrue(tbl_fk_pk_1.after(tbl_fk_pk_0));
        IndexStatsUtil.IdxStats[] tbl_stats_1 = stats.getStatsTable(tbl);
        assertEquals(tbl_stats_0.length, tbl_stats_1.length);
        for (int i=0; i < tbl_stats_1.length; i++) {
            assertTrue(tbl_stats_1[i].after(tbl_stats_0[i]));
        }

        // Now make sure updating one index doesn't modify the others' stats.
        ps.setString(1, tbl);
        ps.setString(2, nuIdx);
        ps.execute();
        // Just use any of the previous stats as a reference point.
        IndexStatsUtil.IdxStats nonUniqueIdx = stats.getStatsIndex(nuIdx)[0];
        assertTrue(nonUniqueIdx.after(tbl_stats_1[0]));
        // Check the counts again.
        stats.assertTableStats(tbl_fk, 1);
        stats.assertTableStats(tbl, 4);
    }

    /**
     * A thread class that repeatedly calls SYSCS_UTIL.SYSCS_UPDATE_STATISTICS
     * until the flag {@code done} is set to true. Any exception thrown during
     * the lifetime of the thread can be found in the field {@code exception}.
     */
    private static class IndexUpdateThread extends Thread {
        private final CallableStatement updateStats;
        private volatile boolean done;
        private Exception exception;

        private IndexUpdateThread(
                Connection c, String schema, String table, String index)
                throws SQLException {
            updateStats = c.prepareCall(
                    "call syscs_util.syscs_update_statistics(?,?,?)");
            updateStats.setString(1, schema);
            updateStats.setString(2, table);
            updateStats.setString(3, index);
        }

        public void run() {
            try {
                while (!done) {
                    updateStats.execute();
                }
                updateStats.close();
            } catch (Exception e) {
                this.exception = e;
            }
        }
    }
}
