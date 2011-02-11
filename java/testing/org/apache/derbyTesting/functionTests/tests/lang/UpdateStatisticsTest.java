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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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
     * Test for update statistics
     */
    public void testUpdateStatistics() throws SQLException {
        // Helper object to obtain information about index statistics.
        IndexStatsUtil stats = new IndexStatsUtil(openDefaultConnection());
        Statement s = createStatement();
        //following should fail because table APP.T1 does not exist
        assertStatementError("42Y55", s, 
            "CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1',null)");
        s.executeUpdate("CREATE TABLE t1 (c11 int, c12 varchar(128))");
        //following will pass now because we have created APP.T1
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1',null)");
        //following should fail because index I1 does not exist on table APP.T1
        assertStatementError("42X65", s, 
            "CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I1')");
        s.executeUpdate("CREATE INDEX i1 on t1(c12)");
        //following will pass now because we have created index I1 on APP.T1
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
        //Now update the statistics for the old index I1 using the new stored
        //procedure. Doing this should add a row for it in sysstatistics table
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I1')");
        stats.assertStats(2);

        //calls to system procedure for update statistics is internally
        //converted into ALTER TABLE ... sql but that generated sql format
        //is not available to end user to issue directly. Write a test case
        //for that sql syntax
        assertStatementError("42X01", s, 
            "ALTER TABLE APP.T1 ALL UPDATE STATISTICS");
        assertStatementError("42X01", s, 
            "ALTER TABLE APP.T1 UPDATE STATISTICS I1");
        //cleanup
        s.executeUpdate("DROP TABLE t1");

        //Try update statistics on global temporary table
		s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit delete rows not logged");
		s.executeUpdate("insert into session.t1 values(11, 1)");
        //following should fail because update statistics can't be issued on
		//global temporary tables
        assertStatementError("42995", s, 
            "CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('SESSION','T1',null)");
        
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
}
