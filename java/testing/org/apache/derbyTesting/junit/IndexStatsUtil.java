/*

   Derby - Class org.apache.derbyTesting.junit.IndexStatsUtil

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

package org.apache.derbyTesting.junit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

/**
 * Helper class for obtaining index statistics and doing asserts on them.
 * <p>
 * This implementation assumes all tables/indexes belong to the current schema.
 */
public class IndexStatsUtil {

    private final Connection con;
    private PreparedStatement psGetTableId;
    private PreparedStatement psGetStatsForTable;
    private PreparedStatement psGetIndexId;
    private PreparedStatement psGetStatsForIndex;
    private PreparedStatement psGetStats;
    private PreparedStatement psGetIdToNameMapConglom;
    private PreparedStatement psGetIdToNameMapTable;

    public IndexStatsUtil(Connection con)
            throws SQLException {
        // Rely on auto-commit to release locks.
        Assert.assertTrue(con.getAutoCommit());
        this.con = con;
    }

    /**
     * Asserts that there are no existing statistics in the database.
     *
     * @throws SQLException if obtaining the statistics fails
     */
    public void assertNoStats()
            throws SQLException {
        assertStats(0);
    }

    /**
     * Asserts that the expected number of statistics exists.
     *
     * @param expectedCount expected number of statistics
     * @throws SQLException if obtaining the statistics fails
     */
    public void assertStats(int expectedCount)
            throws SQLException {
        assertStatCount(getStats(), "<ALL>", expectedCount, false);
    }

    /**
     * Asserts that the expected number of statistics exists for the specified
     * table.
     *
     * @param table the target table
     * @param expectedCount expected number of statistics
     * @throws SQLException if obtaining the statistics fails
     */
    public void assertTableStats(String table, int expectedCount)
            throws SQLException {
        assertStatCount(getStatsTable(table), table, expectedCount, false);
    }

    /**
     * Asserts that the expected number of statistics exists for the specified
     * index.
     *
     * @param index the target index
     * @param expectedCount expected number of statistics
     * @throws SQLException if obtaining the statistics fails
     */
    public void assertIndexStats(String index, int expectedCount)
            throws SQLException {
        assertStatCount(getStatsIndex(index), index, expectedCount, true);
    }

    /**
     * Asserts that the expected number of statistics exists.
     *
     * @param stats statistics
     * @param conglom conglomerate name
     * @param expectedCount expected number of statistics
     * @param isIndex {@code true} if the conglomerate is an index
     */
    private void assertStatCount(IdxStats[] stats, String conglom,
                                 int expectedCount, boolean isIndex) {
        if (stats.length != expectedCount) {
            String name = (isIndex ? "index " : "table ") + "'" + conglom + "'";
            Assert.assertEquals(buildStatString(stats, name),
                    expectedCount, stats.length);
        }
    }

    /**
     * Builds a human readable representation of a list of statistics objects.
     *
     * @param stats a list of statistics (possibly empty)
     * @param name the name of the table(s)/index(es) associated with the stats
     * @return A string representation of the statistics.
     */
    private String buildStatString(IdxStats[] stats, String name) {
        String SEP =
                BaseJDBCTestCase.getSystemProperty("line.separator");
        StringBuffer sb = new StringBuffer(
                "Index statistics for " + name + SEP);
        for (int i=0; i < stats.length; i++) {
            sb.append(i+1).append(": ").append(stats[i].toString()).
                    append(SEP);
        }
        if (stats.length == 0) {
            sb.append(" : <no stats>").append(SEP);
        }
        return sb.toString();
    }

    /**
     * Obtains all existing statistics entries.
     *
     * @return A list of statistics entries (possibly empty).
     * @throws SQLException if obtaining the statistics fail
     */
    public IdxStats[] getStats()
            throws SQLException {
        if (psGetStats == null) {
            psGetStats = con.prepareStatement(
                    "select * from SYS.SYSSTATISTICS");
        }
        return buildStatisticsList(psGetStats.executeQuery(), getIdToNameMap());
    }

    /**
     * Obtains statistics for the specified table.
     *
     * @param table table name
     * @return A list of statistics entries (possibly empty).
     * @throws SQLException if obtaining the statistics fail
     */
    public IdxStats[] getStatsTable(String table)
            throws SQLException {
        if (psGetTableId == null) {
            psGetTableId = con.prepareStatement(
                "select TABLEID from SYS.SYSTABLES where TABLENAME = ?");
        }
        if (psGetStatsForTable == null) {
            psGetStatsForTable = con.prepareStatement(
                "select * from SYS.SYSSTATISTICS where TABLEID = ?");
        }
        psGetTableId.setString(1, table);
        ResultSet rs = psGetTableId.executeQuery();
        Assert.assertTrue("No such table: " + table, rs.next());
        String tableId = rs.getString(1);
        Assert.assertFalse("More than one table named " + table, rs.next());
        rs.close();
        psGetStatsForTable.setString(1, tableId);
        return buildStatisticsList(
                psGetStatsForTable.executeQuery(), getIdToNameMap());
    }

    /**
     * Obtains statistics for the specified index.
     *
     * @param index index name
     * @return A list of statistics entries (possibly empty).
     * @throws SQLException if obtaining the statistics fail
     */
    public IdxStats[] getStatsIndex(String index)
             throws SQLException {
        if (psGetIndexId == null) {
            psGetIndexId = con.prepareStatement(
                    "select CONGLOMERATEID from SYS.SYSCONGLOMERATES where " +
                    "CONGLOMERATENAME = ? and " +
                    "CAST(ISINDEX as VARCHAR(5)) = 'true'");
        }
        if (psGetStatsForIndex == null) {
            psGetStatsForIndex = con.prepareStatement(
                   "select * from SYS.SYSSTATISTICS where REFERENCEID = ?");
        }
        psGetIndexId.setString(1, index);
        ResultSet rs = psGetIndexId.executeQuery();
        Assert.assertTrue("No such index: " + index, rs.next());
        String indexId = rs.getString(1);
        Assert.assertFalse("More than one index named " + index, rs.next());
        rs.close();
        psGetStatsForIndex.setString(1, indexId);
        return buildStatisticsList(
                psGetStatsForIndex.executeQuery(), getIdToNameMap());
    }

    /**
     * Prints all entries in the {@code SYS.SYSSTATISTICS} system table.
     *
     * @throws SQLException if obtaining the statistics fails
     */
    public void printStats()
            throws SQLException {
        System.out.println(buildStatString(getStats(), "all tables"));
    }

    /**
     * Generates a map from ids to names for conglomerates in the database.
     * <p>
     * Convenience method, used for better reporting.
     *
     * @return Mappings from conglomerate id to conglomerate name.
     * @throws SQLException if accessing the system tables fail
     */
    private Map getIdToNameMap()
            throws SQLException {
        if (psGetIdToNameMapConglom == null) {
            psGetIdToNameMapConglom = con.prepareStatement(
                    "select CONGLOMERATEID, CONGLOMERATENAME " +
                    "from SYS.SYSCONGLOMERATES");
        }
        if (psGetIdToNameMapTable == null) {
            psGetIdToNameMapTable = con.prepareStatement(
                    "select TABLEID, TABLENAME from SYS.SYSTABLES");
        }
        Map map = new HashMap();
        ResultSet rs = psGetIdToNameMapConglom.executeQuery();
        while (rs.next()) {
            map.put(rs.getString(1), rs.getString(2));
        }
        rs.close();
        rs = psGetIdToNameMapTable.executeQuery();
        while (rs.next()) {
            map.put(rs.getString(1), rs.getString(2));
        }
        rs.close();
        return map;
    }

    /**
     * Builds an array of statistics objects from data from the
     * {@code SYS.SYSSTATISTICS} system table.
     *
     * @param rs a result set containing rows from {@code SYS.SYSSTATISTICS}
     * @return A list of statistics objects
     * @throws SQLException if accessing the result set fails
     */
    private IdxStats[] buildStatisticsList(ResultSet rs, Map idToName)
            throws SQLException {
        List stats = new ArrayList();
        while (rs.next()) {
            // NOTE: Bad practice to call rs.getString(X) twice, but it works
            //       for Derby with the string type...
            stats.add(new IdxStats(rs.getString(1), rs.getString(2),
                    (String)idToName.get(rs.getString(2)),
                    rs.getString(3),
                    (String)idToName.get(rs.getString(3)),
                    rs.getTimestamp(4), rs.getInt(7),
                    rs.getString(8)));
        }
        rs.close();
        IdxStats[] s = new IdxStats[stats.size()];
        stats.toArray(s);
        return s;
    }

    /**
     * Releases resources.
     */
    public void release() {
        PreparedStatement[] psToClose = new PreparedStatement[] {
            psGetStats, psGetIndexId, psGetStatsForIndex,
            psGetStatsForTable, psGetTableId,
            psGetIdToNameMapConglom, psGetIdToNameMapTable
        };
        for (int i=0; i < psToClose.length; i++) {
            try {
                if (psToClose[i] != null) {
                    psToClose[i].close();
                }
            } catch (SQLException sqle) {
                // Ignore
            }
        }
        try {
            if (!con.isClosed()) {
                con.rollback();
            }
            con.close();
        } catch (SQLException sqle) {
            // Ignore
        }
    }

    /**
     * Immutable class representing index statistics.
     */
    public static final class IdxStats {
        private static final String NA = "<n/a>";
        /** Number of rows in the table / index. */
        public final long rows;
        /** Cardinality of the index. */
        public final long card;
        /** Number of leading columns (in the index) for this stats object. */
        public final int lcols;
        public final String id;
        public final String tableId;
        public final String tableName;
        public final String indexId;
        public final String indexName;
        public final Timestamp created;

        /**
         * Creates a new statistics object with names for the table and the
         * index specified for convenience.
         *
         * @param id statistics identifier
         * @param indexId index identifier
         * @param indexName index name (may be {@code null})
         * @param tableId table identifier
         * @param tableName table name (may be {@code null})
         * @param created creation timestamp
         * @param lcols number of leading columns
         * @param stats the statistics, as reported when doing {@code getString}
         *      on the column {@code STATISTICS} in {@code SYS.SYSSTATISTICS}
         *      (number of unique rows and total rows, for instance
         *      "numunique= 3 numrows= 3")
         */
        public IdxStats(String id, String indexId, String indexName,
                        String tableId, String tableName,
                        Timestamp created, int lcols, String stats) {
            this.id = id;
            this.indexId = indexId;
            this.indexName = indexName != null ? indexName : NA;
            this.tableId = tableId;
            this.tableName = tableName != null ? tableName : NA;
            this.created = created;
            this.lcols = lcols;
            // "numunique= 3 numrows= 3"
            int uniqPos = stats.indexOf('=');
            int space = stats.indexOf(' ', uniqPos+2);
            int rowsPos = stats.indexOf('=', space);
            this.card =
                    Integer.parseInt(stats.substring(uniqPos+1, space).trim());
            this.rows =
                    Integer.parseInt(stats.substring(rowsPos+1).trim());
        }

        public boolean after(IdxStats other) {
            return created.after(other.created);
        }

        public boolean before(IdxStats other) {
            return created.before(other.created);
        }

        public String toString() {
            // Note that not all available information is printed.
            // Add more if required for debugging.
            StringBuffer sb = new StringBuffer(200);
            sb.append("{tableId=").append(tableId).
                    append(", tableName=").append(tableName).
                    append(", indexName=").append(indexName).
                    append(", rows=").append(rows).
                    append(", unique/card=").append(card).
                    append(", created=").append(created).append('}');
            return sb.toString();
        }
    }
}
