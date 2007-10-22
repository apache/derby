/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.InListMultiProbeTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;

/**
 * Test to verify that Derby will perform "multi-probing" of an index
 * when all of the following are true:
 *
 *   1. User query has an "IN" clause whose left operand is a column
 *      reference on which one or more indexes exist, AND
 *
 *   2. The IN list has more than one value in it, excluding duplicates,
 *      and is comprised solely of constant and/or parameter nodes, AND
 *
 *   3. The number of elements in the IN list is significantly less
 *      than the number of rows in the target table (in this test
 *      the data rows are not unique w.r.t the IN list values, so
 *      the size of the IN list should generally be <= 1/10th of the
 *      number rows in the table).
 *
 * If all three of these are true then we expect that Derby will perform
 * multiple execution-time "probes" on the index (one for each non-
 * duplicate value in the IN list) instead of doing a range index scan.
 * This use of multi-probing helps to eliminate unnecessary scanning
 * of index rows, which can be costly.  See esp. DERBY-47.
 *
 * This test was built by taking pieces from Derby47PeformanceTest.java
 * as attached to DERBY-47.
 */
public class InListMultiProbeTest extends BaseJDBCTestCase {

    private final static String DATA_TABLE = "CHANGES";

    private final static String COLUMN_NAMES = 
        "KIND, ITEM_UUID, ITEM_TYPE, BEFORE, AFTER, FOREIGN_KEY_UUID, ID";

    private final static String CREATE_DATA_TABLE =
        "CREATE TABLE " + DATA_TABLE + " (" +
        "ID BIGINT NOT NULL ," +
        "KIND VARCHAR(250) NOT NULL ," +
        "ITEM_UUID CHAR(23) NOT NULL ," +
        "ITEM_TYPE VARCHAR(250) NOT NULL ," +
        "BEFORE CHAR(23), " +
        "AFTER CHAR(23)," +
        "FOREIGN_KEY_UUID CHAR(23) NOT NULL" +
        ")";

    private final static String SELECT_ALL =
        "Select " + COLUMN_NAMES + " From " + DATA_TABLE;

    private final static String SELECT_ALL_WHERE_IN = 
        SELECT_ALL + " where FOREIGN_KEY_UUID in (";

    private final static String ORDER_BY = ") order by ID";

    private final static String RUNTIME_STATS_ON_QUERY =
        "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)";

    private final static String RUNTIME_STATS_OFF_QUERY =
        "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)";

    private final static String GET_RUNTIME_STATS_QUERY =
        "VALUES SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()";

    private static char uuid_chars[] =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

    /* Number of rows to put into the table.  We want to check that multi-
     * probing works for as many as 2500 IN list values, so we want at least
     * 10 * 2500 rows in the table.  We max out at 2500 because anything
     * more would lead to truncation of the log query plan (max length
     * of the query plan is 32767 chars) and then we wouldn't be able to
     * retrieve the required scan information for that plan.
     */
    private final static int NUM_ROWS = 30000;

    /* Array of the "ids" to which our IN list queries will apply. */
    protected String allIds [];

    /* This is an in-memory map of "foreign_key_uuid -> row(s)" that
     * will hold all rows in the target table.  We load this using a
     * simple "select *" query and keep it in memory.  We can then use
     * this mapping to help us determine whether or not the various
     * queries return the expected results.
     *
     * Note that there can be multiple rows for a single foreign key
     * id.  And since all of the IN queries are w.r.t to the foreign
     * key id, that means an IN list with "N" values in it will return
     * greater than N rows.
     */
    protected TreeMap foreignIdToRowsMap;

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public InListMultiProbeTest(String name)
    {
        super(name);
    }
    
    /* 
     * Null out foreignIdToRowsMap so it doesn't hang around after test
     * completes
     * @see org.apache.derbyTesting.junit.BaseJDBCTestCase#tearDown()
     */
    protected void tearDown() throws Exception 
    {
        foreignIdToRowsMap = null;
        super.tearDown();
    }
    
    /**
     * Return a suite that runs the relevant multi-probing tests.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("IN-list MultiProbe Suite");

        /* This is a language/optimization test so behavior will be the
         * same for embedded and client/server.  Therefore we only need
         * to run the test against one or the other; we choose embedded.
         */
        suite.addTest(
            TestConfiguration.embeddedSuite(InListMultiProbeTest.class));

        /* Wrap the suite in a CleanDatabaseTestSetup that will create
         * and populate the test table.
         */
        return new CleanDatabaseTestSetup(suite) 
        {
            /**
            * Create and populate the test table.
            */
            protected void decorateSQL(Statement s) throws SQLException
            {
                // Create the test table.
                s.executeUpdate(CREATE_DATA_TABLE);

                // Insert test data.

                final int BATCH_SIZE = 1000;
                int numDataRows = NUM_ROWS;
                Random random = new Random(1);

                while (numDataRows >= BATCH_SIZE)
                {
                    insertNDataRows(s.getConnection(), BATCH_SIZE, random);
                    numDataRows -= BATCH_SIZE;
                }

                if (numDataRows > 0)
                    insertNDataRows(s.getConnection(), numDataRows, random);

                // Create the indices for the test table.

                String ddl =
                    "CREATE INDEX " + DATA_TABLE + "_NDX1 ON " + DATA_TABLE +
                    "(FOREIGN_KEY_UUID, ID)";
                s.executeUpdate(ddl);

                ddl =
                    "ALTER TABLE " + DATA_TABLE +
                    " ADD CONSTRAINT " + DATA_TABLE + "_PK " +
                    "PRIMARY KEY (ID)";
                s.executeUpdate(ddl);
            }
        };
    }

    /**
     * The one test fixture for this test.  Executes three different types
     * of queries ("strategies") repeatedly with an increasing number of
     * values in the IN list.  Underneath we will check the query plan
     * for each query to make sure that Derby is doing multi-probing as
     * expected.
     */
    public void testMultiProbing() throws Exception
    {
        /* Load all rows into an in-memory map, which is used for checking
         * correctness of the query results.
         */
        readAllRows(createStatement());

        List strategies = new ArrayList();
        Random ran = new Random(2);
        Connection c = getConnection();

        strategies.add(new MarkersStrategy(c, ran));
        strategies.add(new LiteralsStrategy(c, ran));
        strategies.add(new MixedIdsStrategy(c, ran));

        Statement st = createStatement();
        st.execute(RUNTIME_STATS_ON_QUERY);

        for (int size = 2; size <= 10; size += 2)
            testOneSize(strategies, size);

        for (int size = 20; size <= 100; size += 20)
            testOneSize(strategies, size);

        for (int size = 200; size <= 1000; size += 200)
            testOneSize(strategies, size);

        /* The way we check for multi-probing is to search for scan info in
         * the string returned by SYSCS_GET_RUNTIMESTATISTICS().  That string
         * has a max len of 32767; anything larger will be truncated.  So
         * if we try to use more than 1000 "ids" and the ids are specified
         * as literals, the length of the query text alone takes up almost
         * the full 32k, thereby leading to truncation of the scan info and
         * making it impossible to figure out if we actually did multi-
         * probing.  So when we have that many ids the only strategy we
         * use is "Markers", where every id is represented by a "?" and
         * thus we can still retrieve the scan info from runtime stats.
         *
         * The following two calls to "remove" will remove the "Literals"
         * and "MixedIds" strategies from the list.
         */
        strategies.remove(2);
        strategies.remove(1);

        for (int size = 1250; size <= 2500; size += 250)
            testOneSize(strategies, size);

        st.execute(RUNTIME_STATS_OFF_QUERY);
        st.close();
        c = null;
    }

    /**
     * Make sure that we get the correct results when the optimizer chooses
     * to do index multi-probing *and* there are multiple start/stop preds.
     * That is to say, there are predicates other than the probe predicate
     * that can be used as start and/or stop keys on the index, as well.
     * DERBY-2470, DERBY-3061.
     */
    public void testMultipleStartStopPreds() throws Exception
    {
        Statement st = createStatement();

        // Following DDL inserts 80 rows.
        st.execute("create table ct (i int, c1 char(5), c2 char(10))");
        st.execute("insert into ct(i) values 1, 2, 3, 4, 5, 6, 7, 8, 9");
        st.execute("insert into ct(i) values 0, 10, 11, 12, 13, 14, 15");
        st.execute("insert into ct(i) values 16, 17, 18, 19");
        st.execute("insert into ct(i) select 7 * i from ct");
        st.execute("insert into ct(i) select 13 * i from ct");
        st.execute("update ct set c1 = cast(i as char(25))");
        st.execute("update ct set c2 = c1 || c1");

        /* Now insert two more duplicates with different "C2" values.
         * These are used to verify that all predicates are working
         * correctly.
         */
        st.execute("insert into ct values (91, '91', '1234')");
        st.execute("insert into ct values (91, '91', '212398')");

        /* Create an index that has TWO columns; the fact that there
         * is more one that column in the index is essential to the
         * issue being tested here.
         */
        st.execute("create index idx2 on ct (c1, c2)");

        String [][] expRS =
            new String [][]
            {
                {"1","1    ","1    1    "},
                {"2","2    ","2    2    "}
            };

        /* Turn on runtime stats so we can verify that multi-probing
         * is occuring.
         */
        st.execute(RUNTIME_STATS_ON_QUERY);

        // Run some some simple queries as sanity check.

        PreparedStatement ps = prepareStatement(
            "select i,c1,c2 from ct where c1 in (?,?) and c2 like '%'");

        ps.setString(1, "1");
        ps.setString(2, "2");
        assertResultsAndQueryPlan(ps.executeQuery(), expRS, st);

        ps = prepareStatement(
            "select i,c1,c2 from ct where c1 in ('2','1') and c2 like '%'");

        assertResultsAndQueryPlan(ps.executeQuery(), expRS, st);

        /* Parameter in the LIKE leads to generation of additional
         * start/stop predicates on C2.  So run some queries with
         * that.
         */

        ps = prepareStatement("select i,c1,c2 from ct where " +
            "c1 in (?,?) and c2 like ? order by i");

        ps.setString(1, "1");
        ps.setString(2, "2");
        ps.setString(3, "%");
        assertResultsAndQueryPlan(ps.executeQuery(), expRS, st);

        ps = prepareStatement("select i,c1,c2 from ct where " +
            "c1 in (?,?) and c2 like ?");

        ps.setString(1, "1");
        ps.setString(2, "2");
        ps.setString(3, "%");
        assertResultsAndQueryPlan(ps.executeQuery(), expRS, st);

        ps = prepareStatement("select i,c1,c2 from ct where " +
            "c1 in ('2','1') and c2 like ?");

        ps.setString(1, "%");
        assertResultsAndQueryPlan(ps.executeQuery(), expRS, st);

        ps = prepareStatement("select i,c1,c2 from ct where " +
            "c1 in ('2',?) and c2 like ?");

        ps.setString(1, "1");
        ps.setString(2, "%");
        assertResultsAndQueryPlan(ps.executeQuery(), expRS, st);

        // Run some tests that execute directly (no params required).

        assertResultsAndQueryPlan(st.executeQuery(
            "select i,c1,c2 from ct " +
            "where c1 in ('1','2') and c2 like '%' order by i"),
            expRS, st);

        /* The rest of these explicitly specify predicates on C2
         * (instead of relying on "LIKE ?" to generate the additional
         * predicates).  Should see similar behavior, though.  The
         * think we want to check here is that the extra predicates
         * are being enforced correctly, too.
         */

        // 1 additional START key.
        assertResultsAndQueryPlan(st.executeQuery(
            "select i,c1,c2 from ct where c1 in ('1','2') and " +
            "c2 >= '_' order by i"),
            null, st);

        // 1 additional STOP key.
        assertResultsAndQueryPlan(st.executeQuery(
            "select i,c1,c2 from ct where c1 in ('14','91') and " +
            "c2 < '_' order by i"),
            new String [][] {
                {"14","14   ","14   14   "},
                {"14","14   ","14   14   "},
                {"91","91   ","91   91   "},
                {"91","91   ","91   91   "},
                {"91","91   ","91   91   "},
                {"91","91   ","212398    "},
                {"91","91   ","1234      "}
            }, st);

        // 1 additional STOP key.
        assertResultsAndQueryPlan(st.executeQuery(
            "select i,c1,c2 from ct where c1 in ('14','91') and " +
            "c2 < '9' order by i"),
            new String [][] {
                {"14","14   ","14   14   "},
                {"14","14   ","14   14   "},
                {"91","91   ","212398    "},
                {"91","91   ","1234      "}
            }, st);

        // 1 additional START key and 1 additional STOP key.
        assertResultsAndQueryPlan(st.executeQuery(
            "select i,c1,c2 from ct where c1 in ('14','91') and " +
            "c2 < '9' and c2 > '13' order by i"),
            new String [][] {
                {"14","14   ","14   14   "},
                {"14","14   ","14   14   "},
                {"91","91   ","212398    "}
            }, st);

        st.execute("drop table ct");

        /* DERBY-3061: Slightly different scenario in which the
         * less-than predicate was being chosen as the stop key
         * while the probe predicate was being chosen as the start
         * key.  That was leading to incorrect results.
         */

        st.execute("create table mytable (id int primary key)");
        st.execute("insert into mytable (id) values " +
            "0, 1, 2, 3, 4, 5, 6, 7, 8, 9");

        st.execute("insert into mytable select id + 10 from mytable");
        st.execute("insert into mytable select id + 20 from mytable");
        st.execute("insert into mytable select id + 40 from mytable");
        st.execute("insert into mytable select id + 100 from mytable");

        // Sanity check: single less than predicate. Expect 80 rows.
        JDBC.assertDrainResults(st.executeQuery(
            "select mytable.id from mytable where mytable.id < 100"),
            80);

        // Sanity check: single IN predicate.
        JDBC.assertUnorderedResultSet(st.executeQuery(
            "select mytable.id from mytable where " +
            "mytable.id in ( 2, 15, 19, 20, 21, 48, 49 )"),
            new String [][] {
                {"2"}, {"15"}, {"19"}, {"20"}, {"21"}, {"48"}, {"49"}
            });

        /* Now both predicates combined; check to make sure we're
         * getting the correct results.
         */
        JDBC.assertUnorderedResultSet(st.executeQuery(
            "select mytable.id from mytable where mytable.id < 100 " +
            "and mytable.id in ( 2, 15, 19, 20, 21, 48, 49 )"),
            new String [][] {
                {"2"}, {"15"}, {"19"}, {"20"}, {"21"}, {"48"}, {"49"}
            });

        /* Same as previous query, but put the probe predicate first; this
         * can affect sorting so we need to make sure things work in this
         * case as well.
         */
        JDBC.assertUnorderedResultSet(st.executeQuery(
            "select mytable.id from mytable where " +
            "mytable.id in ( 2, 15, 19, 20, 21, 48, 49 ) " +
            "and mytable.id < 100"),
            new String [][] {
                {"2"}, {"15"}, {"19"}, {"20"}, {"21"}, {"48"}, {"49"}
            });

        /* Similar to previous query but make the "other" predicate an
         * equality predicate, as well.  In this case we end up choosing
         * the "other" predicate for start/stop key instead of the probe
         * predicate.  Make sure that we still get the correct results
         * in that case...
         */

        JDBC.assertEmpty(st.executeQuery(
            "select mytable.id from mytable where " +
            "mytable.id in ( 2, 15, 19, 20, 21, 48, 49 ) " +
            "and mytable.id = 100"));

        JDBC.assertUnorderedResultSet(st.executeQuery(
            "select mytable.id from mytable where " +
            "mytable.id in ( 2, 15, 19, 20, 21, 48, 49 ) " +
            "and mytable.id = 21"),
            new String [][] {{"21"}});

        // Cleanup.

        st.execute(RUNTIME_STATS_OFF_QUERY);
        st.execute("drop table mytable");

        ps.close();
        st.close();
    }

    /**
     * Insert the received number of rows into DATA_TABLE via
     * batch processing.
     */
    private static void insertNDataRows(Connection conn, int numRows,
        Random random) throws SQLException
    {
           PreparedStatement stmt = conn.prepareStatement(
            "insert into " + DATA_TABLE + " (" + COLUMN_NAMES +
            ") VALUES (?, ?, ?, ?, ?, ?, ?)");

        String foreignKey = null;

        /* Randomly determined size of a "group", meaning how many
         * rows will have the current foreignKey.
         */
        int numRowsInGroup = 0;

        while (numRows-- > 0)
        {
            if (numRowsInGroup <= 0)
            {
                numRowsInGroup =
                    (int)(1.5 + Math.abs(2.0 * random.nextGaussian()));
                foreignKey = genUUIDValue(random);
            }

            DataRow dr = new DataRow(random, foreignKey);
            dr.setParameters(stmt);
            stmt.addBatch();

            numRowsInGroup--;
        }

        int results[] = stmt.executeBatch();

        // Sanity check to make sure all of the inserts went as planned.
        for (int i = 0; i < results.length; i++)
        {
            if (results[i] != 1)
                fail("Failed to insert rows into " + DATA_TABLE);
        }

        stmt.close();
        return;
    }

    /**
     * Iterates through the received list of query strategies and executes
     * a single SELECT statement with an IN list of size "cnt" for each
     * strategy.  In each case this method makes a call to validate the
     * query results and then checks to see if the query plan chosen by
     * the optimizer demonstrates "multi-probing".  If the either the
     * results or the query plan is wrong, fail.
     *
     * @param strategies Different query strategies to execute
     * @param cnt Size of the IN list with which to query.
     */
    private void testOneSize(List strategies, int cnt) throws SQLException
    {
        if (cnt > allIds.length)
            return;

        String failedStrategy = null;
        Statement st = createStatement();
        for (Iterator iter = strategies.iterator(); iter.hasNext();)
        {
            QueryStrategy strategy = (QueryStrategy) iter.next();
            int numRows = strategy.testSize(cnt);

            ResultSet rs = st.executeQuery(GET_RUNTIME_STATS_QUERY);
            if (!checkMultiProbeQueryPlan(rs, numRows))
            {
                failedStrategy = strategy.getName();
                break;
            }

            rs.close();
        }

        st.close();
        if (failedStrategy != null)
        {
            fail("Execution of '" + failedStrategy + "' strategy with " +
                cnt + " id(s) should have resulted in index multi-probing, " +
                "but did not.");
        }

        return;
    }

    /**
     * Select all rows from DATA_TABLE and store them into an in-memory
     * map of "foreign_uuid -> rows".  So any given foreign_key_uuid can
     * be mapped to one or more rows from the table.
     *
     * We use the in-memory map to verify that all queries executed
     * in this test return the expected results.  See the "validate()"
     * method of QueryStrategy for more.
     *
     * This method also creates an in-memory String array that holds all
     * foreign_key_uuid's found in DATA_TABLE.  That array serves as
     * the basis from which we dynamically generate the query IN lists.
     */
    private void readAllRows(Statement stmt)
        throws SQLException
    {
        ResultSet rs = stmt.executeQuery(SELECT_ALL);
        foreignIdToRowsMap = new TreeMap();
        while (rs.next())
        {
            DataRow c = new DataRow(rs);
            List list = (List) foreignIdToRowsMap.get(c.foreign_key_uuid);
            if (list == null)
            {
                list = new ArrayList();
                foreignIdToRowsMap.put(c.foreign_key_uuid, list);
            }
            list.add(c);
        }

        rs.close();
        stmt.close();

        allIds = new String[foreignIdToRowsMap.size()];
        foreignIdToRowsMap.keySet().toArray(allIds);
        return;
    }

    /**
     * Generate a "fake" UUID value (i.e. the real database we work with has
     * UUIDs stored in CHAR(23) fields, so lets generate a random 23 character
     * string here).
     */
    private static String genUUIDValue(Random random)
    {
        char[] chars = new char[23];
        chars[0] = '_';
        for (int ndx = 1; ndx < chars.length; ndx++)
        {
            int offset = random.nextInt(uuid_chars.length);
            chars[ndx] = uuid_chars[offset];
        }

        return new String(chars);
    }

    /**
     * Assert that the received ResultSet matches the expected results,
     * and then make sure the optimizer actually chose to do multi-
     * probing.  If the received expRS array is null, we take that
     * to mean we expect an empty result set.
     */
    private void assertResultsAndQueryPlan(ResultSet rs,
        String [][] expRS, Statement st) throws SQLException
    {
        int numRows = 0;
        if ((expRS == null) || (expRS.length == 0))
            JDBC.assertEmpty(rs);
        else
        {
            JDBC.assertUnorderedResultSet(rs, expRS);
            numRows = expRS.length;
        }

        /* Now assert the query plan.  We're checking to make sure that
         * the optimizer actually chose to do index multi-probing; otherwise
         * this test is somewhat meaningless...
         */

        ResultSet statRS = st.executeQuery(GET_RUNTIME_STATS_QUERY);
        if (!checkMultiProbeQueryPlan(statRS, numRows))
        {
            fail("Expected multi-probing index scan but query plan showed " +
                "something else.");
        }

        statRS.close();
    }

    /**
     * Take the received ResultSet, which is assumed to hold runtime
     * statistics from the most recently-executed query, and verify
     * that the optimizer chose to do index multi-probing.
     *
     * We determine that "multi-probing" was in effect by looking at
     * the query plan and verifying two things:
     *
     * 1. We used an IndexRowToBaseRow ResultSet on the target
     *    table, AND
     * 2. We did an index scan on the target table AND
     * 3. The number of rows that "qualified" is equal to the
     *    number of rows that were actually returned for the query.
     *    If we did *not* do multi-probing then we would scan all or
     *    part of the index and then apply the IN-list restriction
     *    after reading the rows.  That means that the number of
     *    rows "qualified" for the scan would be greater than the
     *    number of rows returned from the query.  But if we do
     *    multi-probing we will probe for rows that we know satsify
     *    the restriction, thus the number of rows that we "fetch"
     *    (i.e. "rows qualified") should exactly match the number
     *    of rows in the result set.
     */
    private boolean checkMultiProbeQueryPlan(ResultSet rStat,
        int expRowCount) throws SQLException
    {
        if (!rStat.next())
            return false;

        RuntimeStatisticsParser rsp =
            new RuntimeStatisticsParser(rStat.getString(1));

        return (rsp.usedIndexRowToBaseRow() && rsp.usedIndexScan()
            && (rsp.rowsQualifiedEquals(expRowCount)));
    }

    /**
     * Helper class: An instance of DataRow represents a single row
     * in DATA_TABLE.  We use this class to store in-memory versions
     * of the rows, which are helpful for inserting the rows and for
     * checking query results.
     */
    private static class DataRow
    {
        static long nextId = 1;

        /* These fields correspond to the columns of DATA_TABLE. */

        final String kind;
        final String item_uuid;
        final String item_type;
        final String before;
        final String after;
        final String foreign_key_uuid;
        final long id;

        /**
         * Use the received random object and foreign key to generate a
         * "row" that can be inserted into DATA_TABLE.
          */
        DataRow(Random random, String foreignKey)
        {
            int kindChoice = random.nextInt(3);
            switch (kindChoice)
            {
                case 0:
                     kind = "ADD";
                      break;
                case 1:
                    kind = "MOD";
                    break;
                default:
                    kind = "DEL";
                    break;
            }

            item_uuid = genUUIDValue(random);

            // Choose a url for some EMF type 
            item_type =
                random.nextBoolean()
                    ? "http://companyA.com/divB/x.y.z/packageC#typeD"
                    : "http://companyE.com/divF/i.j.k/packageG#typeH";

            before = genUUIDValue(random);
            after = genUUIDValue(random);
            foreign_key_uuid = foreignKey;
            id = nextId++;
        }

        /**
         * Initialize our "columns" (fields) based on the current
         * row of the received result set, which is assumed to be
         * a row from DATA_TABLE.
          */
        DataRow(ResultSet rs) throws SQLException
        {
            kind = rs.getString(1);
            item_uuid = rs.getString(2);
            item_type = rs.getString(3);
            before = rs.getString(4);
            after = rs.getString(5);
            foreign_key_uuid = rs.getString(6);
            id = rs.getLong(7);
        }

        /**
         * Set the parameters of the received PreparedStatement based on
         * the values in this DataRow's columns.  Assumption is that
         * received statement was prepared with the correct number of
         * parameters.
         */
        void setParameters(PreparedStatement ps) throws SQLException
        {
            ps.setString(1, kind);
            ps.setString(2, item_uuid);
            ps.setString(3, item_type);
            ps.setString(4, before);
            ps.setString(5, after);
            ps.setString(6, foreign_key_uuid);
            ps.setLong(7, id);
        }

        /**
         * Return this DataRow's columns as an array of Strings.  This
         * method is used when building the 2-D String array that holds
         * the "expected" query results.
         */
        String [] getColumns()
        {
            // Order here must match the order of COLUMN_NAMES.
            return new String []
            {
                kind,
                item_uuid,
                item_type,
                before,
                after,
                foreign_key_uuid,
                Long.toString(id)
            };
        }
    }

    /**
     * Helper class.  An instance of QueryStrategy represents some specific
     * form of an IN-list query.  Each strategy is responsible for building
     * its own IN-list query, executing it, and then making the necessary
     * calls to validate the results.
     *
     * This class is not static because it references non-static variables
     * in the parent class (InListMultiProbeTest).
     */
    abstract class QueryStrategy
    {
        private Random random;
        protected Connection conn;

        /* When checking query results, row order matters.  The query itself
         * includes an ORDER BY ID clause ("ORDER_BY"), so when we go to
         * check the results our in-memory "table" has to be sorted on ID,
         * as well.  This comparator object allows that sort to happen using
         * the JVM's own sort algorithm.
         */
        Comparator rowComparator = new Comparator()
        {
            public int compare(Object o1, Object o2)
            {
                /* "6" here is the index of the "id" field w.r.t the array
                 * returned from DataRow.getColumns().
                 */
                long id1 = Long.valueOf(((String[])o1)[6]).longValue();
                long id2 = Long.valueOf(((String[])o2)[6]).longValue();

                if (id1 < id2)
                    return -1;
                else if (id1 == id2)
                    return 0;
                else
                    return 1;
            }
        };

        /**
         * Constructor: just take the received objects and save them
         * locally.
         */
        public QueryStrategy(Connection conn, Random random)
        {
            this.conn = conn;
            this.random = random;
        }

        /**
         * Build a list of ids to be used as the IN values for the query.
         * Then execute this strategy's query, validate the results, and
         * return the number of expected rows.
         *
         * @param size The size of the IN list that we want to build.
         */
        public final int testSize(int size)
            throws SQLException
        {
            Set s = new HashSet();

            /* A Set contains no duplicate elements.  So if we, in our
             * randomness, try to insert a duplicate value, it will be
             * ignored--which is fine (this just means that our IN-lists
             * won't have duplicate values in them).  But it also means
             * that we can't just do a normal "for" loop with "size"
             * iterations.  The reason is that we need the set to have
             * "size" values, i.e. we keep going until we've actually
             * inserted "size" _different_ values.  The way to do that is
             * to check the size of the set on each iteration and only
             * quit when the set contains the desired number of elements.
             */
            while (s.size() < size)
            {
                int index = random.nextInt(allIds.length);
                s.add(allIds[index]);
            }

            String[] ids = new String[size];
            s.toArray(ids);

            return fetchDataRows(ids);
        }

        /**
         * Take a list of foreign_key_ids that correlate to the IN
         * list for the most recently-executed query and verify that
         * the received query results are correct.
         *
         * When we get here foreignIdToRowsMap holds an in-memory version
         * of *all* rows that exist in DATA_TABLE.  So in order to figure
         * out what the "expected" results are, we take each id from the
         * IN list (i.e. from the "foreignIds" array), look it up in the
         * in-memory map, and add the corresponding row to a list of
         * "expected" rows.  Since each foreignId maps to one or more
         * rows, we iterate through all rows for the foreignId and add
         * each one to the "expected" list.
         *
         * Then we sort the list on the "id" column so that it matches the
         * ordering of the query result set.  And finally, we "unravel"
         * the list into a two-dimensional array of Strings, which can
         * then be compared with the received ResultSet using the normal
         * JDBC assertion methods.
         */
        protected int validate(String[] foreignIds, ResultSet results)
            throws SQLException
        {
            // This will be a list of String arrays.
            List expected = new ArrayList(foreignIdToRowsMap.size());

            // Search the in-memory map to find all expected rows.
            for (int i = 0; i < foreignIds.length; i++)
            {
                List list = (List)foreignIdToRowsMap.get(foreignIds[i]);
                for (int j = list.size() - 1; j >= 0; j--)
                    expected.add(((DataRow)list.get(j)).getColumns());
            }

            // Sort the rows.
            Collections.sort(expected, rowComparator);

            /* Unravel the expected result set, which is currently a List
             * of one-dimensional String arrays, into a two-dimensional
             * String array.
             */
            Object[] expArray = expected.toArray();
            String [][] expRS = new String [expArray.length][];
            for (int i = 0; i < expArray.length; i++)
                expRS[i] = (String[])expArray[i];
            
            // And finally, check the results.
            JDBC.assertFullResultSet(results, expRS);

            /* If we get here all results are ok; return the number of
             * rows that we found.
             */
            return expRS.length;
        }

        /**
         * Execute whatever query is associated with this QueryStrategy,
         * using the received ids as the values for the IN list.
         */
        protected abstract int fetchDataRows(String[] ids)
            throws SQLException;

        /**
         * Return the name of this query strategy (used for reporting
         * failures).
         */
        protected abstract String getName();
    }

    /**
     * "Literals" strategy, in which we execute a single query
     * with a single IN list having all literal values:
     *
     *   "...WHERE column IN ('literal1', ..., 'literalN')
     */
    class LiteralsStrategy extends QueryStrategy
    {
        /**
         * Constructor just defers to the parent.
         */
        public LiteralsStrategy(Connection conn, Random random)
        {
            super(conn, random);
        }

        /** @see QueryStrategy#getName */
        protected String getName()
        {
            return "Literals";
        }

        /** @see QueryStrategy#fetchDataRows */
        protected int fetchDataRows(String[] ids) throws SQLException
        {
            StringBuffer query = new StringBuffer(SELECT_ALL_WHERE_IN);

            query.append("'");
            query.append(ids[0]);
            query.append("'");

            for (int i = 1; i < ids.length; i++)
            {
                query.append(", '");
                query.append(ids[i]);
                query.append("'");
            }

            query.append(ORDER_BY);

            PreparedStatement stmt = conn.prepareStatement(query.toString());
            int totalDataRows = validate(ids, stmt.executeQuery());
            stmt.close();

            return totalDataRows;
        }
    }

    /**
     * "Markers" strategy, in which we execute a single query with
     * a single IN list having all parameter markers:
     * 
     *  "... WHERE column IN (?, ..., ?)".
     */
    class MarkersStrategy extends QueryStrategy
    {
        /**
         * Constructor just defers to the parent.
         */
        public MarkersStrategy(Connection conn, Random random)
        {
            super(conn, random);
        }

        /** @see QueryStrategy#getName */
        protected String getName()
        {
            return "Markers";
        }

        /** @see QueryStrategy#fetchDataRows */
        protected int fetchDataRows(String[] ids) throws SQLException
        {
            StringBuffer query = new StringBuffer(SELECT_ALL_WHERE_IN);

            query.append("?");
            for (int i = 1; i < ids.length; i++)
                query.append(", ?");

            query.append(ORDER_BY);
            PreparedStatement stmt = conn.prepareStatement(query.toString());
            for (int i = 0; i < ids.length; i++)
                stmt.setString(i + 1, ids[i]);

            int totalDataRows = validate(ids, stmt.executeQuery());
            stmt.close();

            return totalDataRows;
        }
    }

    /**
     * "MixedIds" strategy, in which we execute as single query with
     * a single IN list having a mix of parameter markers and literal
     * values:
     * 
     *  "... WHERE col IN
     *     (?, 'literal2', ?, 'literal4', ..., ?, 'literalN')"
     */
    class MixedIdsStrategy extends QueryStrategy
    {
        /**
         * Constructor just defers to the parent.
         */
        public MixedIdsStrategy(Connection conn, Random random)
        {
            super(conn, random);
        }

        /** @see QueryStrategy#getName */
        protected String getName()
        {
            return "MixedIds";
        }

        /** @see QueryStrategy#fetchDataRows */
        protected int fetchDataRows(String[] ids) throws SQLException
        {
            StringBuffer query = new StringBuffer(SELECT_ALL_WHERE_IN);

            query.append("?");
            for (int i = 1; i < ids.length; i++)
            {
                if ((i % 2) == 1)
                {
                    query.append(", '");
                    query.append(ids[i]);
                    query.append("'");
                }
                else
                    query.append(", ?");
            }

            query.append(ORDER_BY);
            PreparedStatement stmt = conn.prepareStatement(query.toString());

            int p = 0;
            for (int i = 0; i < ids.length; i++)
            {
                if ((i % 2) == 0)
                    stmt.setString(++p, ids[i]);
            }

            int totalDataRows = validate(ids, stmt.executeQuery());
            stmt.close();

            return totalDataRows;
        }
    }
}
