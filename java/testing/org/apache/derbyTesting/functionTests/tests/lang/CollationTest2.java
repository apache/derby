/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CollationTest2
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

import javax.sql.DataSource;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
Junit test targeted at testing language based Collation.

Test the following with data that shows different ordering between default
collation and different language based collation:
T0: (DONE) Heap based compare using predicate pushing
T1: (DONE) Index based compare start/stop predicates on index
T2: (TODO) Index based compare using predicate pushing
T3: (DONE) order by on heap using in memory sorter
T4: (TODO) order by on heap using disk based sorter
T5: (TODO) system catalogs should not be collated
T6: (TODO) create a heap table in collated 
T7: (TODO) test create conglomerate triggered by DiskHashtable code
T8: (TODO) test create conglomerate triggered by DataDictionaryImpl
T9: (TODO) test create conglomerate triggered by java/engine/org/apache/derby/impl/sql/conn/GenericLanguageConnectionContext.java
T10: (DONE) alter table compress with indexes
T11: (DONE) alter table drop column with indexes
T12: (DONE) alter table add column with index
T13: (TODO) bulk insert into empty table, with and without indexes
T13: (TODO) bulk insert replace, with and without indexes

T13: (TODO) java/engine/org/apache/derby/impl/sql/execute/MaterializedResultSet.java
T14: (TODO) /java/engine/org/apache/derby/impl/sql/execute/TemporaryRowHolderImpl.java
T15: (TODO) /java/engine/org/apache/derby/impl/store/access/PropertyConglomerate.java

**/

public class CollationTest2 extends BaseJDBCTestCase 
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    

    private static final int    TEST_DEFAULT = 0;
    private static final int    TEST_ENGLISH = 1;
    private static final int    TEST_POLISH  = 2;
    private static final int    TEST_NORWAY  = 3;

    /**
     * logical database names to use for the DataSource connection.
     * <p>
     * Order of array is important, each entry should map to the logical
     * database name associated with the TEST_* constants.  So for example
     * the logical name for the ENGLISH database should be in 
     * TEST_DATABASE[TEST_ENGLISH].
     **/
    private static final String[] TEST_DATABASE = 
    {
        "defaultdb2",
        "enddb2",
        "poldb2",
        "nordb2"
    };


    /**
     * connection attribute to use to specify the territory.
     * <p>
     * Order of array is important, each entry should map to the territory
     * for the associated TEST_* constants.  So for example
     * the territory id POLISH database should be in 
     * TEST_DATABASE[TEST_POLISH].
     **/
    private static final String[] TEST_CONNECTION_ATTRIBUTE =
    {
        null,
        "en",
        "pl",
        "no"
    };


    private static final String[] NAMES =
    {
        // Just Smith, Zebra, Acorn with alternate A,S and Z
        "Smith",
        "Zebra",
        "\u0104corn",
        "\u017Bebra",
        "Acorn",
        "\u015Amith",
        "aacorn"
    };

    private static final int[] DEFAULT_NAME_ORDER =
    {
        4, // Acorn
        0, // Smith
        1, // Zebra
        6, // aacorn
        2, // \u0104corn
        5, // \u015Amith
        3  // \u017Bebra
    };

    private static final int[] ENGLISH_NAME_ORDER =
    {
        6, // aacorn
        4, // Acorn
        2, // \u0104corn
        0, // Smith
        5, // \u015Amith
        1, // Zebra
        3  // \u017Bebra
    };

    private static final int[] POLISH_NAME_ORDER =
    {
        6, // aacorn
        4, // Acorn
        2, // \u0104corn
        0, // Smith
        5, // \u015Amith
        1, // Zebra
        3  // \u017Bebra
    };

    private static final int[] NORWAY_NAME_ORDER =
    {
        4, // Acorn
        2, // \u0104corn
        0, // Smith
        5, // \u015Amith
        1, // Zebra
        3, // \u017Bebra
        6  // aacorn
    };

    private static final int[][] EXPECTED_NAME_ORDER = 
    {
        DEFAULT_NAME_ORDER,
        ENGLISH_NAME_ORDER,
        POLISH_NAME_ORDER,
        NORWAY_NAME_ORDER
    };


    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    public CollationTest2(String name) 
    {
        super(name);
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**
     * Produce an expect row set given the order and asc/desc info.
     * <p>
     * Input array gives the offset into the NAMES array of data of the
     * expected order of rows to return for this test.
     *
     * @param expected_order    Expected order of rows in this language.
     * @param ascending_order   true if rows are in order, else rows are in
     *                          reverse order.
     **/
    private String[][] full_row_set(
    int[]   expected_order,
    int     start_offset,
    int     stop_offset,
    boolean ascending_order)
    {
        String[][] ret_order = null;

        int num_vals = stop_offset - start_offset + 1;

        if (num_vals > 0)
        {
            ret_order = new String[num_vals][2];

            if (ascending_order)
            {
                int dest = 0;
                for (int src = start_offset; src <= stop_offset; src++)
                {
                    ret_order[dest][0] = String.valueOf(expected_order[src]);
                    ret_order[dest][1] = NAMES[expected_order[src]];
                    dest++;
                }
            }
            else
            {
                // rows are expected in reverse order from what is passsed in,
                // so swap them to create the output expected result array.
                int dest = 0;
                for (int src = stop_offset; src >= start_offset; src--)
                {
                    ret_order[dest][0] = String.valueOf(expected_order[src]);
                    ret_order[dest][1] = NAMES[expected_order[src]];
                    dest++;
                }
            }
        }

        return(ret_order);
    }

    private void checkLangBasedQuery(
    Connection  conn,
    String      query, 
    String[][]  expectedResult) 
        throws SQLException 
    {
        Statement s  = conn.createStatement();
        ResultSet rs = s.executeQuery(query);

        if (expectedResult == null) //expecting empty resultset from the query
            JDBC.assertEmpty(rs);
        else
            JDBC.assertFullResultSet(rs,expectedResult);

        s.close();
        conn.commit();
    }

    private void checkOneParamQuery(
    Connection  conn,
    String      query, 
    String      param,
    String[][]  expectedResult) 
        throws SQLException 
    {
        PreparedStatement   ps = conn.prepareStatement(query);
        ps.setString(1, param);
        ResultSet           rs = ps.executeQuery();

        if (expectedResult == null) //expecting empty resultset from the query
            JDBC.assertEmpty(rs);
        else
            JDBC.assertFullResultSet(rs,expectedResult);


        // re-execute it to test path through the cache
        ps.setString(1, param);
        rs = ps.executeQuery();

        if (expectedResult == null) //expecting empty resultset from the query
            JDBC.assertEmpty(rs);
        else
            JDBC.assertFullResultSet(rs,expectedResult);

        rs.close();
        ps.close();
        conn.commit();
    }


    /**
     * Check simple boolean compare of string constant to column value.
     * <p>
     * Check <, <=, =, >=, > of constant to column, ie. of the form
     *     select * from table where col boolean constant
     *
     *
     * @throws SQLException
     **/
    private void checkSimpleCompare(
    Connection  conn,
    int[]       expected_order)
        throws SQLException
    {
        // loop through all the rows using each as the descriminator, this
        // gives us low, high and middle special cases.  Expect the number
        // of rows for this test case to be low.
        for (int i = 0; i < expected_order.length; i++)
        {
            // '<' test
            checkLangBasedQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME < '" + 
                    NAMES[expected_order[i]] + "' ORDER BY NAME",
                full_row_set(
                    expected_order, 
                    0, 
                    i - 1,
                    true));

            // '<=' test
            checkLangBasedQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME <= '" + 
                    NAMES[expected_order[i]] + "' ORDER BY NAME",
                full_row_set(
                    expected_order, 
                    0, 
                    i,
                    true));

            // '=' test
            checkLangBasedQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME = '" + 
                    NAMES[expected_order[i]] + "' ORDER BY NAME",
                full_row_set(
                    expected_order, 
                    i, 
                    i,
                    true));

            // '>=' test
            checkLangBasedQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME >= '" + 
                    NAMES[expected_order[i]] + "' ORDER BY NAME",
                full_row_set(
                    expected_order, 
                    i, 
                    expected_order.length - 1,
                    true));


            // '>' test
            checkLangBasedQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME > '" + 
                    NAMES[expected_order[i]] + "' ORDER BY NAME",
                full_row_set(
                    expected_order, 
                    i + 1, 
                    expected_order.length - 1,
                    true));

            // now check prepared query

            // '<' test
            checkOneParamQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME < ? ORDER BY NAME",
                NAMES[expected_order[i]],
                full_row_set(
                    expected_order, 
                    0, 
                    i - 1,
                    true));

            // '<=' test
            checkOneParamQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME <= ? ORDER BY NAME",
                NAMES[expected_order[i]],
                full_row_set(
                    expected_order, 
                    0, 
                    i,
                    true));

            // '=' test
            checkOneParamQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME = ? ORDER BY NAME",
                NAMES[expected_order[i]],
                full_row_set(
                    expected_order, 
                    i, 
                    i,
                    true));

            // '>=' test
            checkOneParamQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME >= ? ORDER BY NAME",
                NAMES[expected_order[i]],
                full_row_set(
                    expected_order, 
                    i, 
                    expected_order.length - 1,
                    true));

            // '>' test
            checkOneParamQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME > ? ORDER BY NAME",
                NAMES[expected_order[i]],
                full_row_set(
                    expected_order, 
                    i + 1, 
                    expected_order.length - 1,
                    true));
        }
    }


    private void setUpTable(Connection conn) throws SQLException 
    {
        Statement s = conn.createStatement();
        s.execute(
            "CREATE TABLE CUSTOMER(" +
                "D1 CHAR(200), D2 CHAR(200), D3 CHAR(200), D4 INT, " + 
                "ID INT, NAME VARCHAR(40), NAME2 CHAR(40))");

        conn.setAutoCommit(false);
        PreparedStatement ps = 
            conn.prepareStatement("INSERT INTO CUSTOMER VALUES(?,?,?,?,?,?,?)");

        for (int i = 0; i < NAMES.length; i++)
        {
            ps.setString(1, NAMES[i]);
            ps.setString(2, NAMES[i]);
            ps.setString(3, NAMES[i]);
            ps.setInt(   4, i);
            ps.setInt(   5, i);
            ps.setString(6, NAMES[i]);
            ps.setString(7, NAMES[i]);
            ps.executeUpdate();
        }

        conn.commit();
        ps.close();
        s.close();
    }

    private void dropTable(Connection conn) throws SQLException 
    {
        Statement s = conn.createStatement();
	
        s.execute("DROP TABLE CUSTOMER");     
        s.close();
    }

    private void runQueries(
    Connection  conn,
    int         db_index,
    String      create_idx_qry,
    String      idx_name)
        throws SQLException 
    {
        Statement s = conn.createStatement();

        if (create_idx_qry != null)
        {
            s.execute(create_idx_qry);
            conn.commit();
        }

        // Simple check of getting all rows back in order
        checkLangBasedQuery(
            conn, 
            "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME",
            full_row_set(
                EXPECTED_NAME_ORDER[db_index], 
                0, 
                EXPECTED_NAME_ORDER[db_index].length - 1, 
                true));

        // Simple check of getting all rows back in order
        checkLangBasedQuery(
            conn, 
            "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME, ID",
            full_row_set(
                EXPECTED_NAME_ORDER[db_index], 
                0, 
                EXPECTED_NAME_ORDER[db_index].length - 1, 
                true));

        // Simple check of getting all rows back in opposite order
        checkLangBasedQuery(
            conn, 
            "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME DESC",
            full_row_set(
                EXPECTED_NAME_ORDER[db_index], 
                0, 
                EXPECTED_NAME_ORDER[db_index].length - 1, 
                false));

        // Check <, <=, =, >=, > operators on constant vs. column
        checkSimpleCompare(conn, EXPECTED_NAME_ORDER[db_index]);

        if (create_idx_qry != null)
            s.execute("DROP INDEX " + idx_name);

        conn.commit();
    }


    /**
     * test paths through alter table compress
     *
     * Tests:
     * T10: alter table compress with indexes
     **/
    private void runAlterTableCompress(
    Connection  conn,
    int         db_index)
        throws SQLException 
    {
        Statement s = conn.createStatement();

        conn.commit();

        s.execute("CREATE INDEX IDX1 ON CUSTOMER (NAME)");
        s.execute("CREATE INDEX IDX2 ON CUSTOMER (NAME, ID)");
        s.execute("CREATE INDEX IDX3 ON CUSTOMER (ID,   NAME)");
        s.execute("CREATE INDEX IDX4 ON CUSTOMER (ID)");
        s.execute("CREATE INDEX IDX5 ON CUSTOMER (ID, NAME, D1, D2, D3)");

        conn.commit();

        // execute alter table compress which will build all new indexes and
        // base conglomerates, verify collation info correctly gets into new
        // entities.
        CallableStatement call_stmt = conn.prepareCall(
            " call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('APP', 'CUSTOMER', 1)");
        assertUpdateCount(call_stmt, 0);
 
        conn.commit();

        runQueries(conn, db_index, null, null);

        s.execute("DROP INDEX IDX1 ");
        s.execute("DROP INDEX IDX2 ");
        s.execute("DROP INDEX IDX3 ");
        s.execute("DROP INDEX IDX4 ");
        s.execute("DROP INDEX IDX5 ");

        // let's test abort get's back to right collation also.
        conn.rollback();

        runQueries(conn, db_index, null, null);

        conn.commit();
    }

    /**
     * Drop column test.
     * <p>
     * Drop column will drop and recreate base table and associated indexes,
     * need to test to make sure correct colation ids get passed to new
     * containers. 
     *
     * Tests:
     * T11: alter table drop column with indexes
     **/
    private void runAlterTableDropColumn(
    Connection  conn,
    int         db_index)
        throws SQLException 
    {
        Statement s = conn.createStatement();

        conn.commit();

        s.execute("ALTER TABLE CUSTOMER DROP COLUMN D1");
        runQueries(conn, db_index, null, null);

        s.execute("CREATE INDEX IDX1 ON CUSTOMER (NAME)");
        s.execute("ALTER TABLE CUSTOMER DROP COLUMN D2");
        runQueries(conn, db_index, null, null);
        conn.rollback();

        conn.commit();
    }

    /**
     * Add column test.
     * <p>
     * Add column adds a new template column which requires a collation
     * info related store update.  Test that added column had right 
     * collation setting.
     *
     * Tests:
     * T12: alter table add column with index
     **/
    private void runAlterTableAddColumn(
    Connection  conn,
    int         db_index)
        throws SQLException 
    {
        Statement s = conn.createStatement();

        conn.commit();

        s.execute("ALTER TABLE CUSTOMER DROP COLUMN NAME");
        s.execute("ALTER TABLE CUSTOMER ADD COLUMN NAME CHAR(40)");
        s.execute("UPDATE CUSTOMER SET NAME = D1");
        runQueries(conn, db_index, null, null);

        s.execute("CREATE INDEX IDX1 ON CUSTOMER (NAME)");
        runQueries(conn, db_index, null, null);
        conn.rollback();

        conn.commit();
    }

    /**
     * Shared code to run all test cases against a single collation.
     * <p>
     * Pass in the index of which TEST_DATABASE database to test.  So
     * for instance to run the default, pass in 0.
     * <p>
     *
     * @return The identifier to be used to open the conglomerate later.
     *
     * @param db_index  index of which test to run.
     *
     * @exception  SQLException
     **/
    private void runTestIter(int db_index) throws SQLException 
    {
        DataSource ds = 
            JDBCDataSource.getDataSourceLogical(TEST_DATABASE[db_index]);

        String conn_string = 
            "create=true" + 
                ((TEST_CONNECTION_ATTRIBUTE[db_index] == null) ? 
                     "" : 
                     ";territory=" + 
                     TEST_CONNECTION_ATTRIBUTE[db_index] + 
                     ";collation=TERRITORY_BASED");

        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", conn_string);

        Connection conn = ds.getConnection();
        Statement s = conn.createStatement();

        setUpTable(conn);

        // run tests against base table no index, exercise heap path
        // Tests the following:
        // T0: Heap based compare using predicate pushing
        // T3: order by on heap using in memory sorter
        runQueries(conn, db_index, null, null);

        // run tests against base table with non unique index
        // Tests the following:
        // T1: (DONE) Index based compare start/stop predicates on index
        runQueries(
            conn, db_index, 
            "CREATE INDEX NAME_IDX ON CUSTOMER (NAME)", "NAME_IDX");

        // run tests against base table with only unique index
        runQueries(
            conn, db_index, 
            "CREATE UNIQUE INDEX IDX ON CUSTOMER (NAME)", "IDX");

        // run tests against base table with non unique descending index
        runQueries(
            conn, db_index, 
            "CREATE INDEX NAME_IDX ON CUSTOMER (NAME DESC)", "NAME_IDX");

        // run tests against base table with unique descending index
        runQueries(
            conn, db_index, 
            "CREATE UNIQUE INDEX IDX ON CUSTOMER (NAME DESC)", "IDX");

        // run tests against base table with unique composite key
        runQueries(
            conn, db_index, 
            "CREATE UNIQUE INDEX IDX ON CUSTOMER (NAME, ID)", "IDX");

        runAlterTableCompress(conn, db_index);

        runAlterTableAddColumn(conn, db_index);

        runAlterTableDropColumn(conn, db_index);

        dropTable(conn);

        conn.commit();
        conn.close();
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    public void testDefaultCollation() throws SQLException
    {
        runTestIter(TEST_DEFAULT);
    }
    public void testEnglishCollation() throws SQLException
    {
        runTestIter(TEST_ENGLISH);
    }
    public void testPolishCollation() throws SQLException
    {
        runTestIter(TEST_POLISH);
    }
    public void testNorwayCollation() throws SQLException
    {
        runTestIter(TEST_NORWAY);
    }
    
    
    public static Test suite() {

        Test test =  TestConfiguration.defaultSuite(CollationTest2.class);
        test = TestConfiguration.additionalDatabaseDecorator(
                    test, TEST_DATABASE[TEST_DEFAULT]);
        test = TestConfiguration.additionalDatabaseDecorator(
                    test, TEST_DATABASE[TEST_ENGLISH]);
        test = TestConfiguration.additionalDatabaseDecorator(
                    test, TEST_DATABASE[TEST_POLISH]);
        test = TestConfiguration.additionalDatabaseDecorator(
                    test, TEST_DATABASE[TEST_NORWAY]);
        return test;
    }
}
