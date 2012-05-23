/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.helpers.DisposableIndexStatistics

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.upgradeTests.helpers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.derbyTesting.junit.IndexStatsUtil;

/**
 * Helper class encapsulating logic used in the upgrade test for testing
 * functionality dropping, and skipping generation of, disposable statistics
 * entries.
 */
public class DisposableIndexStatistics {

    /**
     * A row count currently chosen at will.
     * <p>
     * Note that if being used for testing the automatic istat daemon, the
     * number of rows must be sufficiently high to trigger statistics creation,
     * and likewise for the deltas when adding more rows to trigger an update.
     */
    private static final int ROW_COUNT = 2000;

    private final Connection con;
    private final String tbl;
    private final String fktbl;
    private final String pktbl;

    /**
     * Creates a new helper instance using the given connection and table.
     *
     * @param con connection
     * @param tableName base table name
     */
    public DisposableIndexStatistics(Connection con, String tableName) {
        this.con = con;
        this.tbl = tableName;
        this.fktbl = tableName + "_FK";
        this.pktbl = tableName + "_PK_2COL";
    }

    /** Creates and populates the test tables. */
    public void createAndPopulateTables()
            throws SQLException {
        con.setAutoCommit(true);
        Statement stmt = con.createStatement();
        // Populate the tables first, then add the indexes/constraints.
        // This ensure the statistics are actually created.
        // Statistics: two entries due to two columns in the index.
        stmt.executeUpdate("create table " + pktbl + "( " +
                    "id1 int generated always as identity, " +
                    "id2 int not null" +
                ")");
        // Statistics: zero entries (single-column primary key), one without
        //             optimization logic.
        stmt.executeUpdate("create table " + fktbl + "( " +
                    "id int not null generated always as identity" +
                ")");
        // Statistics: three with DERBY-5681 fixed and optimization,
        //             otherwise five.
        stmt.executeUpdate("create table " + tbl + "( " +
                    "id int not null generated always as identity, " +
                    "fk_dropped int not null, " +
                    "fk_self int, " +
                    "fk_self_notnull int not null, " +
                    "nonunique int" +
                ")");

        // Insert data
        insertData(con);

        IndexStatsUtil stats = new IndexStatsUtil(con);
        // Add constraints
        stmt.executeUpdate("alter table " + pktbl + " add constraint " +
                "PK_TWOCOL_PKTAB primary key (id1, id2)");
        stats.getStatsTable(pktbl, 2);
        stmt.executeUpdate("alter table " + fktbl + " add constraint " +
                "PK_FKTAB primary key (id)");
        stats.getStatsTable(fktbl, 1);
        stmt.executeUpdate("alter table " + tbl + " add constraint " +
                "PK_MAIN primary key (id)");
        stats.getStatsTable(tbl, 1);
        stmt.executeUpdate("create index DUPS_MAIN on " + tbl + "(nonunique)");
        stats.getStatsTable(tbl, 2);
        stmt.executeUpdate("alter table " + tbl + " add constraint " +
                "FKS_MAIN foreign key (fk_self) references " + tbl + "(id)");
        stats.getStatsTable(tbl, 3);
        stmt.executeUpdate("alter table " + tbl + " add constraint " +
                "FKSNN_MAIN foreign key (fk_self_notnull) references " +
                tbl + "(id)");
        stats.getStatsTable(tbl, 4);

        int preFkAddition = stats.getStatsTable(tbl).length;
        // This doesn't trigger DERBY-5681.
        stmt.executeUpdate("alter table " + tbl + " add constraint " +
                "fk_to_be_dropped foreign key (fk_dropped) " +
                "references " + fktbl + "(id)");
        Assert.assertTrue(stats.getStatsTable(tbl).length == preFkAddition +1);
        stmt.executeUpdate("alter table " + tbl + " drop constraint " +
                "fk_to_be_dropped");
        Assert.assertTrue(stats.getStatsTable(tbl).length == preFkAddition);

        // Trigger DERBY-5681.
        stmt.executeUpdate("alter table " + tbl + " add constraint " +
                "fk_on_pk foreign key (id) " +
                "references " + fktbl + "(id)");
        stmt.executeUpdate("call syscs_util.syscs_update_statistics(" +
                "'APP', '" + tbl + "', null)");
        Assert.assertTrue(stats.getStatsTable(tbl).length == preFkAddition +1);
        stmt.executeUpdate("alter table " + tbl + " drop constraint " +
                "fk_on_pk");
        // Derby failed to drop the statistics when the constraint got dropped.
        Assert.assertTrue(stats.getStatsTable(tbl).length == preFkAddition +1);

        // Do an assert, but since we may be run with both old and new
        // releases allow for two cases.
        Assert.assertEquals(
                getNumTotalPossibleStats(), getAllRelevantStats(null));
    }

    private void insertData(Connection con)
            throws SQLException {
        // Populate the foreign key table.
        boolean oldAutoCommitValue = con.getAutoCommit();
        con.setAutoCommit(false);
        PreparedStatement ps = con.prepareStatement(
                "insert into " + fktbl + " values (DEFAULT)");
        for (int row = 0; row < ROW_COUNT; row++) {
            ps.executeUpdate();
        }
        ps.close();
        con.commit();

        // Populate primary key table (has a multi-column primary key)
        ps = con.prepareStatement(
                "insert into " + pktbl + " values (DEFAULT, ?)");
        for (int row = 0; row < ROW_COUNT; row++) {
            ps.setInt(1, row);
            ps.executeUpdate();
        }
        ps.close();
        con.commit();

        // Populate the main table.
        // The modulo operations are used to vary the number of unique values
        // in the columns and have been chosen at will.
        ps = con.prepareStatement(
                "insert into " + tbl + " values (DEFAULT,?,?,?,?)");
        for (int row = 0; row < ROW_COUNT; row++) {
            ps.setInt(1, (row % ROW_COUNT) +1);
            ps.setInt(2, (row % 2000) +1);
            ps.setInt(3, (row % 19) +1);
            ps.setInt(4, row % 10);
            ps.executeUpdate();
        }
        ps.close();
        con.commit();
        con.setAutoCommit(oldAutoCommitValue);
    }

    /** Returns the names of the tables used by this test. */
    public String[] getTableNames() {
        return new String[] {tbl, fktbl, pktbl};
    }

    /** Asserts the number of statistics entries for all relevant tables. */
    public void assertStatsCount(int expected)
            throws SQLException {
        ArrayList entries = new ArrayList();
        int found = getAllRelevantStats(entries);
        if (found != expected) {
            Assert.assertEquals(
                    IndexStatsUtil.buildStatString(
                        getStatArray(entries),
                        "DisposableIndexStatistics tables"),
                expected, found);
        }
    }

    /** Converts the list of statistics to an array. */
    private IndexStatsUtil.IdxStats[] getStatArray(List list) {
        int size = list.size();
        IndexStatsUtil.IdxStats[] ret = new IndexStatsUtil.IdxStats[size];
        list.toArray(ret);
        return ret;
    }

    /**
     * Fetches all relevant statistics.
     *
     * @param list list to append statistics to (may be {@code null})
     * @return The number of relevant statistics entries found.
     * @throws SQLException if something goes wrong
     */
    private int getAllRelevantStats(List list)
            throws SQLException {
        boolean oldAutoCommitValue = con.getAutoCommit();
        con.setAutoCommit(true);
        IndexStatsUtil stats = new IndexStatsUtil(con);
        String[] tables = getTableNames();
        int count = 0;
        for (int i=0; i < tables.length; i++) {
            IndexStatsUtil.IdxStats[] entries = stats.getStatsTable(tables[i]);
            if (list != null) {
                list.addAll(Arrays.asList(entries));
            }
            count += entries.length;
        }
        stats.release(false);
        con.setAutoCommit(oldAutoCommitValue);
        return count;
    }

    /**
     * Total number of possible statistics entries.
     * <p>
     * This number includes orphaned and unnecessary statistics, and these
     * entries are expected to be purged out when running with the current/
     * newest version of Derby.
     */
    public static int getNumTotalPossibleStats() {
        return 8;
    }

    /** Number of disposable statistics entries. */
    public static int getNumDisposableStats() {
        return 3;
    }
}
