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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

/**
 * Helper class for obtaining index statistics and doing asserts on them.
 * <p>
 * This implementation assumes all tables/indexes belong to the current schema.
 * <p>
 * The <em>timeout</em> value is used to make the utility more resilient to
 * differences in timing due to varying scheduling decisions, processor speeds,
 * etc. If the system table contains the wrong number of statistics objects for
 * the query, it will be queried repeatedly until the right number of statistics
 * objects is obtained or the query times out.
 */
public class IndexStatsUtil {

    private static final boolean INDEX = false;
    private static final boolean TABLE = true;
    private static final int NO_EXPECTATION = -1;
    private static final String SEP =
                BaseJDBCTestCase.getSystemProperty("line.separator");
//IC see: https://issues.apache.org/jira/browse/DERBY-4834

    private final Connection con;
    /** Timeout in milliseconds. */
    private final long timeout;
    private PreparedStatement psGetTableId;
    private PreparedStatement psGetStatsForTable;
    private PreparedStatement psGetIndexId;
    private PreparedStatement psGetStatsForIndex;
    private PreparedStatement psGetStats;
    private PreparedStatement psGetIdToNameMapConglom;
    private PreparedStatement psGetIdToNameMapTable;

    /**
     * Creates an instance querying the given database with no timeout set.
     * <p>
     * Querying with no timeout means that if there are too few or too many
     * statistics objects matching the query, a failure will be raised
     * immediately.
     *
     * @param con connection to the database to query
     */
    public IndexStatsUtil(Connection con) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
        this(con, 0L);
    }

    /**
     * Creates an instance querying the given database with the specified
     * timeout value.
     *
     * @param con connection to the database to query
     * @param timeout the longest time to wait to see if the expectations for a
     *      query are met (milliseconds)
     */
    public IndexStatsUtil(Connection con, long timeout) {
        // Rely on auto-commit to release locks.
        try {
            Assert.assertTrue(con.getAutoCommit());
        } catch (SQLException sqle) {
            Assert.fail("Failed to get auto commit: " + sqle.getMessage());
        }
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout cannot be negative");
        }
        this.con = con;
        this.timeout = timeout;
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
     * Asserts that there are no existing statistics for the specified table.
     *
     * @throws SQLException if obtaining the statistics fails
     */
    public void assertNoStatsTable(String table)
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
            throws SQLException {
        assertTableStats(table, 0);
    }

    /**
     * Asserts that the expected number of statistics exists.
     *
     * @param expectedCount expected number of statistics
     * @throws SQLException if obtaining the statistics fails
     */
    public void assertStats(int expectedCount)
            throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
        IdxStats[] ret = getStats();
        if (ret.length != expectedCount) {
            Assert.assertEquals(buildStatString(ret, "<ALL TABLES>"),
                    expectedCount, ret.length);
        }
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
        getStatsTable(table, expectedCount);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
        getStatsIndex(index, expectedCount);
    }

    /**
     * Builds a human readable representation of a list of statistics objects.
     *
     * @param stats a list of statistics (possibly empty)
     * @param name the name of the table(s)/index(es) associated with the stats
     * @return A string representation of the statistics.
     */
    public static String buildStatString(IdxStats[] stats, String name) {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
                    "select * from SYS.SYSSTATISTICS " +
                    "order by TABLEID, REFERENCEID, COLCOUNT");
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
        return getStatsTable(table, NO_EXPECTATION);
    }

    /**
     * Obtains statistics for the specified table, fails if the number of
     * statistics objects isn't as expected within the timeout.
     *
     * @param table table name
     * @param expectedCount number of expected statistics objects
     * @return A list of statistics entries (possibly empty).
     * @throws SQLException if obtaining the statistics fail
     */
    public IdxStats[] getStatsTable(String table, int expectedCount)
            throws SQLException {
        if (psGetTableId == null) {
            psGetTableId = con.prepareStatement(
                "select TABLEID from SYS.SYSTABLES where TABLENAME = ?");
        }
        psGetTableId.setString(1, table);
        ResultSet rs = psGetTableId.executeQuery();
        Assert.assertTrue("No such table: " + table, rs.next());
        String tableId = rs.getString(1);
        Assert.assertFalse("More than one table named " + table, rs.next());
        rs.close();

        IdxStats[] ret = querySystemTable(tableId, TABLE, expectedCount);
        // Avoid building the stat string if not necessary.
        if (expectedCount != NO_EXPECTATION && ret.length != expectedCount) {
            Assert.assertEquals("failed to get statistics for table " + table +
                    " (#expected=" + expectedCount + ", timeout=" + timeout +
                    ")" + SEP + buildStatString(ret, table),
                    expectedCount, ret.length);
        }
        return ret;
    }

    /**
     * Waits for the current statistics to disappear and expects to fetch the
     * same number of new statistics for the table.
     *
     * @param table the table to get statistics for
     * @param currentStats the current statistics
     * @return The new statistics.
     * @throws SQLException if obtaining statistics fails
     */
    public IdxStats[] getNewStatsTable(String table, IdxStats[] currentStats)
//IC see: https://issues.apache.org/jira/browse/DERBY-3790
            throws SQLException {
        if (timeout == 0) {
            throw new IllegalStateException(
                    "no timeout specified in the constructor");
        }
        awaitChange(currentStats, timeout);
        return getStatsTable(table, currentStats.length);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
        return getStatsIndex(index, NO_EXPECTATION);
    }

    /**
     * Obtains statistics for the specified index, fails if the number of
     * statistics objects isn't as expected within the timeout.
     *
     * @param index index name
     * @param expectedCount number of expected statistics objects
     * @return A list of statistics entries (possibly empty).
     * @throws SQLException if obtaining the statistics fail
     */
    public IdxStats[] getStatsIndex(String index, int expectedCount)
             throws SQLException {
        if (psGetIndexId == null) {
            psGetIndexId = con.prepareStatement(
                    "select CONGLOMERATEID from SYS.SYSCONGLOMERATES where " +
                    "CONGLOMERATENAME = ? and " +
                    "CAST(ISINDEX as VARCHAR(5)) = 'true'");
        }
        psGetIndexId.setString(1, index);
        ResultSet rs = psGetIndexId.executeQuery();
        Assert.assertTrue("No such index: " + index, rs.next());
        String indexId = rs.getString(1);
        Assert.assertFalse("More than one index named " + index, rs.next());
        rs.close();

        IdxStats[] ret = querySystemTable(indexId, INDEX, expectedCount);
        // Avoid building the stat string if not necessary.
        if (expectedCount != NO_EXPECTATION && ret.length != expectedCount) {
            Assert.assertEquals("failed to get statistics for index " + index +
                    " (#expected=" + expectedCount + ", timeout=" + timeout +
                    ")" + SEP + buildStatString(ret, index),
                    expectedCount, ret.length);
        }
        return ret;
    }

    /**
     * Queries the system table {@code SYS.SYSSTATISTICS} for statistics
     * associated with a specific table or index.
     *
     * @param conglomId conglomerate id (UUID)
     * @param isTable tells if the conglomerate is a table or an index
     * @param expectedCount the number of statistics objects expected, use
     *      {@code NO_EXPECTATION} to return whatever matches the query
     *      immediately
     */
    private IdxStats[] querySystemTable(String conglomId, boolean isTable,
                                        int expectedCount)
            throws SQLException {
        // Assign the correct prepared statement.
        PreparedStatement ps;
        if (isTable) {
            if (psGetStatsForTable == null) {
                psGetStatsForTable = con.prepareStatement(
                        "select * from SYS.SYSSTATISTICS " +
                            "where TABLEID = ? " +
                            "order by REFERENCEID, COLCOUNT");
            }
            ps = psGetStatsForTable;
        } else {
            if (psGetStatsForIndex == null) {
                psGetStatsForIndex = con.prepareStatement(
                        "select * from SYS.SYSSTATISTICS " +
                            "where REFERENCEID = ? " +
                            "order by COLCOUNT");
            }
            ps = psGetStatsForIndex;
        }
        ps.setString(1, conglomId);

        long started = System.currentTimeMillis();
        long waited = -1;
        IdxStats[] ret = null;
        while (waited < timeout) {
            // Don't wait the first time.
            if (ret != null) {
                Utilities.sleep(Math.min(250L, timeout - waited));
            }
            ret = buildStatisticsList(ps.executeQuery(), getIdToNameMap());
            if (expectedCount == NO_EXPECTATION || ret.length == expectedCount){
                break;
            }
            waited = System.currentTimeMillis() - started;
        }
        return ret;
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
    private Map<String, String> getIdToNameMap()
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        Map<String, String> map = new HashMap<String, String>();
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
    private IdxStats[] buildStatisticsList(
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            ResultSet rs, Map<String, String> idToName)
            throws SQLException {
        List<IdxStats> stats = new ArrayList<IdxStats>();
        while (rs.next()) {
            // NOTE: Bad practice to call rs.getString(X) twice, but it works
            //       for Derby with the string type...
            stats.add(new IdxStats(rs.getString(1), rs.getString(2),
                    idToName.get(rs.getString(2)),
                    rs.getString(3),
                    idToName.get(rs.getString(3)),
                    rs.getTimestamp(4), rs.getInt(7),
                    rs.getString(8)));
        }
        rs.close();
        return stats.toArray(new IdxStats[stats.size()]);
    }

    /**
     * Releases resources and closes the associated connection.
     */
    public void release() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5759
        release(true);
    }

    /**
     * Releases resources.
     *
     * @param closeConnection whether to close the associated connection
     */
    public void release(boolean closeConnection) {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5759
            if (closeConnection) {
                con.close();
            }
        } catch (SQLException sqle) {
            // Ignore
        }
    }

    /**
     * Waits until all given statistics entries have been changed, or until
     * the call times out.
     * <p>
     * <em>NOTE</em>: The method is built on the assumption that the UUIDs of
     * statistics objects aren't reused. That is, when statistics are updated,
     * the old row in SYS.SYSSTATISTICS will be dropped and a new row will be
     * inserted.
     *
     * @param current the statistics that must change / be replaced
     * @param timeout maximum number of milliseconds to wait before giving up
     * @throws SQLException if obtaining statistics fails
     */
    private void awaitChange(IdxStats[] current, long timeout)
//IC see: https://issues.apache.org/jira/browse/DERBY-3790
            throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        Set<IdxStats> oldStats = new HashSet<IdxStats>(Arrays.asList(current));
        Set<IdxStats> newStats = null;
        long start = System.currentTimeMillis();
        // Make sure we run at least once.
        while (System.currentTimeMillis() - start < timeout ||
                newStats == null) {
            newStats = new HashSet<IdxStats>(Arrays.asList(getStats()));
            newStats.retainAll(oldStats);
            if (newStats.isEmpty()) {
                return;
            }
            Utilities.sleep(200);
        }
        IdxStats[] outstanding = new IdxStats[newStats.size()];
        newStats.toArray(outstanding);
        Assert.fail(outstanding.length + " missing statistics changes " +
                "(timeout=" + timeout + "ms): " +
                buildStatString(outstanding, "<unchanged statistics>"));
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
                    append(", lcols=").append(lcols).
                    append(", rows=").append(rows).
                    append(", unique/card=").append(card).
                    append(", created=").append(created).append('}');
            return sb.toString();
        }

        /**
         * Equality is based on the statistics entry UUID.
         *
         * @param obj other object
         * @return {@code true} if the other object is considered equal to this
         */
        public boolean equals(Object obj) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4834
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final IdxStats other = (IdxStats) obj;
            return this.id.equals(other.id);
        }

        public int hashCode() {
            int hash = 7;
            hash = 17 * hash + this.id.hashCode();
            return hash;
        }
    }
}
