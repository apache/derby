/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.AutomaticIndexStatisticsTest

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
package org.apache.derbyTesting.functionTests.tests.store;

import java.io.File;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.IndexStatsUtil;
import org.apache.derbyTesting.junit.IndexStatsUtil.IdxStats;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.Utilities;

/**
 * Tests various aspects of the automatic index statistics update feature,
 * which is designed to run as a background task.
 * <p>
 * Some of the tests ensure that the daemon is able to recover after
 * encountering various kinds of "expected errors", for instance if the table
 * is dropped while being scanned by the daemon.
 */
public class AutomaticIndexStatisticsTest
    extends BaseJDBCTestCase {

    private static final String MASTERDB = "masterDb";
    private static final String BIG_TABLE = "BIG_TABLE";
    private static final long DEFAULT_TIMEOUT = 20*1000;

    private static final String[] TYPES = new String[] {"TABLE", "VIEW"};

    private static boolean dbCreated;
    // Change this if running test cases in parallel...
    private static IndexStatsUtil stats;

    public AutomaticIndexStatisticsTest(String name) {

        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(AutomaticIndexStatisticsTest.class);
        return TestConfiguration.additionalDatabaseDecorator(suite, MASTERDB);
    }

    /** Initialize the default statistics helper object. */
    public void setUp()
            throws SQLException {
        if (stats != null) {
            stats.release();
        }
        stats = new IndexStatsUtil(openDefaultConnection(), DEFAULT_TIMEOUT);
    }

    /** Release the default statistics helper object. */
    public void tearDown()
            throws Exception {
        if (stats != null) {
            stats.release();
        }
        stats = null;
        super.tearDown();
    }

    /**
     * Make sure stats are created when the table grows, and that the database
     * can be deleted after automated stats create/update -
     * that is verify that there are no open file handles left behind in the
     * daemon.
     */
    public void testStatsCreatedOnGrowthThenDeleteDb()
            throws SQLException {
        String db = "singleUse/newCleanDb";
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", db);
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        Connection con = ds.getConnection();
        String TAB = "TEST_GROWTH_EMPTY";
        createAndInsertSimple(con, TAB, 300);
        // This should trigger creation of statistics.
        PreparedStatement ps = con.prepareStatement(
                "select * from " + TAB + " where id = ?");
        ps.close();

        // Get statistics
        IdxStats[] myStats = new IndexStatsUtil(
                ds.getConnection(), DEFAULT_TIMEOUT).getStatsTable(TAB, 1);
        assertEquals(1, myStats.length);
        assertTrue(myStats[0].rows == 300);
        assertTrue(myStats[0].card == 300);

        // Shutdown database and try to delete it.
        JDBCDataSource.shutdownDatabase(ds);
        assertDirectoryDeleted(constructDbPath(db));
    }

    /** Make sure stats are updated when the table grows. */
    public void testStatsUpdatedOnGrowth()
            throws SQLException {
        String TAB = "TEST_GROWTH";
        createAndInsertSimple(TAB, 10000);
        // This should trigger creation of statistics.
        prepareStatement("select * from " + TAB + " where id = ?");

        // Get statistics
        IdxStats[] statsPre = stats.getStatsTable(TAB, 1);
        assertEquals(1, statsPre.length);

        // Insert more rows.
        setAutoCommit(false);
        insertSimple(TAB, 50000, 10000);
        // Force a checkpoint to update the row estimates.
        forceRowCountEstimateUpdate(TAB);
        prepareStatement("select * from " + TAB + " where 1=1");
        IdxStats[] statsPost = getFilteredTableStats(TAB, 1, statsPre);
        assertEquals(1, statsPost.length);
        assertFalse(statsPre[0].equals(statsPost[0]));
        assertFalse(statsPre[0].after(statsPost[0]));
        // Insert a few more rows, should not trigger a new update
        insertSimple(TAB, 1000, 60000);
        forceRowCountEstimateUpdate(TAB);
        prepareStatement("select * from " + TAB + " where 2=2");
        // Manual wait to see if the existing stats are replaced - they should
        // not be
        Utilities.sleep(1500);
        IdxStats[] statsPost1 = stats.getStatsTable(TAB, 1);
        assertTrue(statsPost[0].equals(statsPost1[0]));
        assertFalse(statsPost1[0].after(statsPost[0]));
    }

    /**
     * Shuts down database while the daemon is scanning a table, and then
     * makes sure the database directory can be deleted.
     */
    public void testShutdownWhileScanningThenDelete()
            throws IOException, SQLException {
        // Need a big enough table to get the timing right.
        String db = "singleUse/copyShutdown";
        copyDb(db);
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", db);
        Connection con = ds.getConnection();
        String TAB = BIG_TABLE;

        // Trigger statistics creation.
        PreparedStatement ps = con.prepareStatement(
                "select * from " + TAB + " where id = ?");
        ps.close();

        // Wait to make sure the scan starts.
        Utilities.sleep(150);
        // Now shut down the database.
        JDBCDataSource.shutdownDatabase(ds);
        assertDirectoryDeleted(constructDbPath(db));
    }

    /**
     * Drops table while the daemon is scanning it, then triggers update of
     * statistics of a different table, and finally shuts down and deletes the
     * database.
     */
    public void testDropWhileScanningThenDelete()
            throws IOException, SQLException {
        // Need a big enough table to get the timing right.
        String TAB1 = BIG_TABLE;
        String TAB2 = "SECONDARY_TABLE";
        String db = "singleUse/copyDrop";
        copyDb(db);
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", db);
        Connection con = ds.getConnection();
        // Create secondary table.
        createAndInsertSimple(con, TAB2, 20000);

        // Trigger statistics creation.
        PreparedStatement ps = con.prepareStatement(
                "select * from " + TAB1 + " where id = ?");
        ps.close();
        // Wait to make sure the scan starts.
        Utilities.sleep(150);
        println("dropping table...");
        Statement stmt = con.createStatement();
        stmt.executeUpdate("drop table " + TAB1);
        stmt.close();

        // Trigger stats update on secondary table.
        IndexStatsUtil myStats =
                new IndexStatsUtil(ds.getConnection(), DEFAULT_TIMEOUT);
        myStats.assertNoStatsTable(TAB2);
        ps = con.prepareStatement("select * from " + TAB2 + " where id = ?");
        myStats.assertTableStats(TAB2, 1);
        myStats.release();

        // Shutdown, then delete database directory.
        JDBCDataSource.shutdownDatabase(ds);
        assertDirectoryDeleted(constructDbPath(db));
    }

    /**
     * Tests that compressing the table while scanned makes the daemon fail
     * gracefully, and that the daemon can do other work afterwords.
     */
    public void testCompressWhileScanning()
            throws IOException, SQLException {
        String TAB1 = BIG_TABLE;
        String TAB2 = "SECONDARY_TABLE";
        String db = "singleUse/copyCompress";
        copyDb(db);
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", db);
        Connection con = ds.getConnection();
        // Create secondary table.
        createAndInsertSimple(con, TAB2, 20000);

        // Trigger statistics creation.
        PreparedStatement ps = con.prepareStatement(
                "select * from " + TAB1 + " where id = ?");
        ps.close();
        // Wait to make sure the scan starts.
        Utilities.sleep(150);
        println("compressing table...");
        Statement stmt = con.createStatement();
        stmt.executeUpdate("call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', '" +
                TAB1 + "', 0)");
        stmt.close();
        // There should still be a statistics object written during the
        // compress operation.
        IndexStatsUtil myStats =
                new IndexStatsUtil(ds.getConnection(), DEFAULT_TIMEOUT);
        myStats.assertTableStats(TAB1, 1);

        // Trigger stats update on secondary table, make sure the daemon can
        // still process work.
        myStats.assertNoStatsTable(TAB2);
        ps = con.prepareStatement("select * from " + TAB2 + " where id = ?");
        myStats.assertTableStats(TAB2, 1);
        myStats.release();
    }

    /**
     * Tests that the statistics computed are correct.
     *
     * @throws SQLException if something goes wrong
     */
    public void testStatisticsCorrectness()
            throws SQLException {
        // Create table.
        String TAB = "STAT_CORR";
        dropTable(TAB);
        Statement stmt = createStatement();
        stmt.executeUpdate("create table " + TAB +
                " (id1 int, id2 int, id3 int, val int, " +
                "primary key (id1, id2, id3))");
        stats.assertNoStatsTable(TAB);
        // Insert rows.
        PreparedStatement ps = prepareStatement("insert into " + TAB +
                " values (?,?,?,?)");
        // # rows = 100 * 50 * 10 = 50'000
        setAutoCommit(false);
        final int rows = 100*50*10;
        for (int i=1; i <= 100; i++) {
            ps.setInt(1, i);
            for (int j=1; j <= 50; j++) {
                ps.setInt(2, j);
                for (int k=1; k <= 10; k++) {
                    ps.setInt(3, k);
                    ps.setInt(4, i*j*k % 750);
                    ps.executeUpdate();
                }
            }
        }
        commit();
        setAutoCommit(true);

        // Select to trigger statistics generation.
        forceRowCountEstimateUpdate(TAB);
        JDBC.assertDrainResults(
                prepareStatement("select * from " + TAB + " where id1 = 10")
                .executeQuery());

        // We expect three stats objects for the single index; one per
        // leading columns combination (c1, c1-c2, and c1-c2-c3).
        IdxStats statsObj[] = stats.getStatsTable(TAB, 3);
        assertEquals(3, statsObj.length);

        Timestamp now = new Timestamp(new Date().getTime());
        for (int i=0; i < statsObj.length; i++) {
            IdxStats s = statsObj[i];
            assertEquals(rows, s.rows);
            assertTrue(s.created.before(now));
            switch (s.lcols) {
                case 1:
                    assertEquals(100, s.card);
                    break;
                case 2:
                    assertEquals(5000, s.card);
                    break;
                case 3:
                    assertEquals(50000, s.card);
                    break;
            default:
                fail("unexpected number of leading columns: " + s.lcols);
            }
        }

        // Now create a second index in the opposite order, check stats.
        stmt.executeUpdate("create index IDXREV on " + TAB + "(id3, id2, id1)");
        statsObj = stats.getStatsIndex("IDXREV", 3);
        assertEquals(3, statsObj.length);
        Timestamp earlier = now;
        now = new Timestamp(new Date().getTime());
        for (int i=0; i < statsObj.length; i++) {
            IdxStats s = statsObj[i];
            assertEquals(rows, s.rows);
            assertTrue("current stats created " + s.created +
                    ", previous stats created " + earlier,
                    s.created.after(earlier));
            // Stats cannot have been created after the current time (future).
            assertFalse(s.created.compareTo(now) > 0);
            switch (s.lcols) {
                case 1:
                    assertEquals(10, s.card);
                    break;
                case 2:
                    assertEquals(500, s.card);
                    break;
                case 3:
                    assertEquals(50000, s.card);
                    break;
            default:
                fail("unexpected number of leading columns: " + s.lcols);
            }
        }

        // Finally, create a unique index on the val column.
        stmt.executeUpdate("create index IDXVAL on " + TAB + "(val)");
        ResultSet rs = stmt.executeQuery(
                "select val from " + TAB + " order by val");
        // Calculate number of unique values.
        int uniqueVals = 0;
        int prev = -1;
        while (rs.next()) {
            int curr = rs.getInt(1);
            if (curr != prev) {
                uniqueVals++;
                prev = curr;
            }
        }
        rs.close();
        // Get stats and check the associated values.
        IdxStats[] valStat = stats.getStatsIndex("IDXVAL", 1);
        assertEquals(1, valStat.length);
        assertEquals(uniqueVals, valStat[0].card);
        assertEquals(rows, valStat[0].rows);
    }

    public void testSelectFromSimpleView()
            throws SQLException {
        // First create a table with a few columns.
        String table = "VIEW_BASE_TABLE";
        String view = "MY_VIEW";

        dropIfExists(getConnection(), view);
        dropIfExists(getConnection(), table);
        Statement s = createStatement();
        s.execute("create table " + table + " (" +
                "id int primary key, col1 int, col2 int)");
        s.execute("create index COL2_IDX on " + table + "(col2)");
        PreparedStatement ps = prepareStatement("insert into " + table +
                " values (?,?,?)");
        setAutoCommit(false);
        for (int i=0; i < 30000; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i % 15);
            ps.setInt(3, i % 25);
            ps.executeUpdate();
            // Commit periodically
            if (i % 5000 == 0) {
                commit();
            }
        }
        commit();
        setAutoCommit(true);
        ps.close();

        // Create the view.
        s.execute("create view " + view + "(vcol_1, vcol2) " +
                "AS select id, col2 from " + table);

        // Select from the view, using index.
        stats.assertNoStatsTable(table);
        ps = prepareStatement("select * from " + view + " where vcol2 = 7");
        stats.assertNoStatsTable(table);
        // Trigger update of the base table.
        ps = prepareStatement("select * from " + table + " where col2 = 7");
        stats.assertTableStats(table, 2);
    }

    // Utility methods

    /**
     * Creates a copy of the master db.
     *
     * @param newDbName name of the copy
     * @throws IOException if something goes wrong
     * @throws SQLException if something goes wrong
     */
    private void copyDb(String newDbName)
            throws IOException, SQLException {
        if (!dbCreated) {
            createMasterDb();
        }

        File master = constructDbPath(
                TestConfiguration.getCurrent().getPhysicalDatabaseName(
                    MASTERDB));
        final File dest = constructDbPath(newDbName);

        // Make sure the directory containing the database directory exists.
        // We expect the security manager to stop us from creating directories
        // where we aren't supposed to.
        if (!PrivilegedFileOpsForTests.exists(dest.getParentFile())) {
            AccessController.doPrivileged(new PrivilegedAction() {

                public Object run() {
                    assertTrue(dest.getParentFile().mkdirs());
                    return null;
                }
            });
        }

        PrivilegedFileOpsForTests.copy(master, dest);
    }

    /**
     * Creates the default/master db with a larger number of rows.
     *
     * @throws SQLException if creating the database fails
     */
    private void createMasterDb()
            throws SQLException {
        long start = System.currentTimeMillis();
        String table = BIG_TABLE;
        int rows = 1*1000*1000;
        DataSource ds1 = JDBCDataSource.getDataSourceLogical(MASTERDB);
        JDBCDataSource.setBeanProperty(ds1, "createDatabase", "create");
        Connection con = ds1.getConnection();
        // Check if the table exists, if so, drop it.
        dropIfExists(con, table);
        // Create the table.
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table " + table + "(id int primary key)");
        stmt.close();

        // Insert data
        con.setAutoCommit(false);
        PreparedStatement ps = con.prepareStatement("insert into " + table +
                " values ?");
        for (int i=0; i < rows; i++) {
            ps.setInt(1, i);
            ps.addBatch();
            if (i % 5000 == 0) {
                ps.executeBatch();
                con.commit();
            }
        }
        ps.executeBatch();
        con.commit();
        con.close();
        println("created master db with " + rows + " rows in " +
                ((System.currentTimeMillis() - start) / 1000) + " seconds");

        // Shut down the master db, we will copy it later.
        JDBCDataSource.shutdownDatabase(
                JDBCDataSource.getDataSourceLogical(MASTERDB));

        dbCreated = true;

    }

    /**
     * Forces Derby to update the row count estimate by doing a full table
     * scan and then invoking a checkpoint.
     *
     * @param table target table
     * @throws SQLException if something goes wrong
     */
    private void forceRowCountEstimateUpdate(String table)
            throws SQLException {
        Statement stmt = createStatement();
        JDBC.assertDrainResults(
                stmt.executeQuery("select count(*) from " + table));
        stmt.execute("call SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()");
        stmt.close();
    }

    /**
     * Constructs the path to the database base directory.
     *
     * @param relDbDirName the database name (relative)
     * @return The path to the database.
     */
    private File constructDbPath(String relDbDirName) {
        // Example:
        //     "singleUse/readOnly" -> "<derby.system.home>/singleUse/readOnly"
        File f = new File(getSystemProperty("derby.system.home"));
        return new File(f, relDbDirName);
    }

    /**
     * Default method to create and populate a simple test table.
     * <p>
     * The table consists of a single integer column, which is also the primary
     * key of the table.
     *
     * @param table target table
     * @param rows number of rows to insert
     * @throws SQLException if creating/populating the table fails
     */
    private void createAndInsertSimple(String table, int rows)
            throws SQLException {
        createAndInsertSimple(null, table, rows);
    }

    /**
     * Default method to create and populate a simple test table.
     * <p>
     * The table consists of a single integer column, which is also the primary
     * key of the table.
     *
     * @param con the connection to use (may be {@code null}, in which case
     *      the default connection will be used)
     * @param table target table
     * @param rows number of rows to insert
     * @throws SQLException if creating/populating the table fails
     */
    private void createAndInsertSimple(Connection con, String table, int rows)
            throws SQLException {
        Statement s;
        IndexStatsUtil myStats;
        if (con == null) {
            con = getConnection();
            s = createStatement();
            myStats = stats;
        } else {
            s = con.createStatement();
            myStats = new IndexStatsUtil(con);
        }
        // See if the table exists, and if so, drop it.
        dropIfExists(con, table);
        // Create table.
        s.executeUpdate("create table " + table + "(id int primary key)");

        myStats.assertNoStatsTable(table);

        // Insert data
        long start = System.currentTimeMillis();
        println("created " + table + ", inserting " + rows + " rows");
        insertSimple(con, table, rows, 0);
        println("completed in " + (System.currentTimeMillis() - start) + " ms");
    }

    /**
     * Inserts the specified number of rows into the table, using an increasing
     * integer as the value.
     *
     * @param table target table
     * @param rows number of rows
     * @param start starting value for the first inserted row
     * @throws SQLException if something goes wrong
     */
    private void insertSimple(String table, int rows, int start)
            throws SQLException {
        // Use the default connection.
        insertSimple(getConnection(), table, rows, start);
    }

    /**
     * Inserts the specified number of rows into the table, using an increasing
     * integer as the value.
     *
     * @param con the connection to use
     * @param table target table
     * @param rows number of rows
     * @param start starting value for the first inserted row
     * @throws SQLException if something goes wrong
     * @throws NullPointerException if {@code con} is {@code null}
     */
    private void insertSimple(Connection con, String table, int rows, int start)
            throws SQLException {
        PreparedStatement ps = con.prepareStatement(
                                    "insert into " + table + " values ?");
        boolean autoCommit = con.getAutoCommit();
        con.setAutoCommit(false);
        for (int i=start; i < start+rows; i++) {
            ps.setInt(1, i);
            ps.addBatch();
            if (i % 5000 == 0) {
                ps.executeBatch();
                con.commit();
            }
        }
        ps.executeBatch();
        con.commit();
        con.setAutoCommit(autoCommit);
    }

    /**
     * Obtains the statistics for all indexes associated with the given table in
     * the default database.
     *
     * @param table base table
     * @param expectedCount number of statistics objects to obtain
     * @param oldStats statistics objects to ignore
     * @return An array with the statistics objects obtained.
     * @throws SQLException if something goes wrong
     * @throws AssertionError if the number of statistics objects expected
     *      can't be obtained within the time limit
     *      ({@linkplain #DEFAULT_TIMEOUT})
     */
    private IdxStats[] getFilteredTableStats(String table, int expectedCount,
                                             IdxStats[] oldStats)
            throws SQLException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < DEFAULT_TIMEOUT) {
            IdxStats[] ret = stats.getStatsTable(table, expectedCount);
            boolean doReturn = true;
            // Check if we have new stats (if filtering is asked for).
            if (oldStats != null) {
                for (int i=0; i < ret.length; i++) {
                    for (int j=0; j < oldStats.length; j++) {
                        if (ret[i].equals(oldStats[j])) {
                            doReturn = false;
                            break;
                        }
                    }
                }
            }
            if (doReturn) {
                return ret;
            }
            Utilities.sleep(250);
        }
        fail("getting stats for table " + table + " timed out (#expected=" +
                expectedCount + ", #oldStats=" +
                (oldStats == null ? 0 : oldStats.length) + ")");
        // Silence the compiler.
        return null;
    }

    // Static utility methods

    /**
     * Drops the specified entity if it exists.
     *
     * @param con connection to the database
     * @param entity the entity to drop (i.e. table or view)
     */
    private static void dropIfExists(Connection con, String entity)
            throws SQLException {
        ResultSet tables = con.getMetaData().getTables(
                null, null, entity, TYPES);
        while (tables.next()) {
            String type = tables.getString(4);
            if (type.equals("TABLE")) {
                dropTable(con, entity);
            } else if (type.equals("VIEW")) {
                con.createStatement().executeUpdate("drop view " + entity);
            } else {
                fail("entity " + entity + " of unsupported type: " + type);
            }
        }
        tables.close();
    }
}
