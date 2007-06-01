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

import java.text.Collator;
import java.text.RuleBasedCollator;

import java.util.Locale;
import java.util.Properties; 

import javax.sql.DataSource;

import junit.framework.Assert;
import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.TestConfiguration;

import org.apache.derby.iapi.services.sanity.SanityManager;


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
T6: (DONE) test like
T7: (TODO) test create conglomerate triggered by DiskHashtable code
T8: (TODO) test create conglomerate triggered by DataDictionaryImpl
T9: (TODO) test create conglomerate triggered by java/engine/org/apache/derby/impl/sql/conn/GenericLanguageConnectionContext.java
T10: (DONE) alter table compress with indexes
T11: (DONE) alter table drop column with indexes
T12: (DONE) alter table add column with index
T13: (DONE) bulk insert into empty table, with and without indexes
T14: (DONE) bulk insert replace, with and without indexes

T15: (TODO) java/engine/org/apache/derby/impl/sql/execute/MaterializedResultSet.java
T16: (TODO) /java/engine/org/apache/derby/impl/sql/execute/TemporaryRowHolderImpl.java
T17: (TODO) /java/engine/org/apache/derby/impl/store/access/PropertyConglomerate.java
T18: (TODO) upgrade tests - may be changes to upgrade suite rather than here.
T19: (TODO) recovery testing - may be old function harness changes as no one has
            suggested how to do this in junit harness.
T20: (TODO) For both a newly created 10.3 database and an upgraded 10.3 
            database, make sure that metadata continues to show the scale for 
            character datatypes as 0 (rather than the collation type value). 
            That is, test that the scale of the character datatypes is always
            0 and it didn't get impacted negatively by the overloading of scale
            field as collation type in TypeDescriptor. 
T21: (TODO) Testing with views
T22: (TODO) Testing with global temporary tables
T23: (TODO) Alter table testing. Two specific cases 1)add a new character column and 2)increase the length of an existing character
T24: (DONE) DERBY-2669 If no territory attribute is specified at create 
            database time, then create collated db based on default territory
            of Database.


**/

public class CollationTest2 extends BaseJDBCTestCase 
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */


    /**
     * Set to get output if something in the test is failing and you want
     * more information about what was going on.
     **/
    private static final boolean    verbose_debug = false;

    private static final int    TEST_DEFAULT            = 0;
    private static final int    TEST_ENGLISH            = 1;
    private static final int    TEST_POLISH             = 2;
    private static final int    TEST_NORWAY             = 3;
    private static final int    TEST_DEFAULT_TERRITORY  = 4;

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
        "nordb2",
        "defaultdb3"
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
        "no_NO",
        null
    };

    private static final String[] TEST_COLLATION_ATTRIBUTE =
    {
        null,
        ";collation=TERRITORY_BASED",
        ";collation=TERRITORY_BASED",
        ";collation=TERRITORY_BASED",
        ";collation=TERRITORY_BASED"
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

    
    /**
     * set up LIKE test cases, configured for all languages by 
     * the TEST_* constants.
     * <p>
     * Insert all data to tested against into LIKE_NAMES. A customer table
     * will be filled with this data.
     * {p>
     * Insert test cases for like string into the LIKE_TEST_CASES, results
     * are expected only to return a single row.
     * <p>
     * Insert actual string expected back for each language, for each test
     * case in the {LANG}_LIKE_RESULT array.  Insert null if no match is
     * expected.
     * <p>
     * Current test tries all 4 datatypes, CHAR will blank pad making the
     * results different than the other datatypes if data is shorter than
     * type, thus a different set of LIKE clauses needs to be entered in the
     * LIKE_CHAR_TEST_CASES which should match the same results in a CHAR
     * field as does the corresponding LIKE_TEST_CASES test.  
     *
     **/
    private static final String[] LIKE_NAMES =
    {
        "Waagan",      // 0
        "Smith",       // 1
        "Zebra",       // 2
        "xcorn",       // 3
        "aBebra",      // 4
        "Acorn",       // 5
        "Amith",       // 6
        "aacorn",      // 7
        "xxxaa",       // 8
        "aaxxx",       // 9
        "yyyaa y",     // 10
    };

    private static final String[] LIKE_TEST_CASES = 
    {
        "Waagan",
        "W_gan",
        "aaxxx",
        "_xxx",
        "xxxaa",
        "xxx_",
        "xxx_%",
        "yyy_%"
    };
    private static final String[] LIKE_CHAR_TEST_CASES = 
    {
        "Waagan    ",
        "W_gan    ",
        "aaxxx%",
        "_xxx%",
        "xxx%",
        "xxx_ %",
        "xxx%",
        "yyy_%"
    };

    private static final int[] DEFAULT_LIKE_RESULT =
    {
        0,
        -1,
        9,
        -1,
        8,
        -1,
        8,
        10
    };
        
    private static final int[] ENGLISH_LIKE_RESULT =
    {
        0,
        -1,
        9,
        -1,
        8,
        -1,
        8,
        10
    };

    private static final int[] POLISH_LIKE_RESULT =
    {
        0,
        -1,
        9,
        -1,
        8,
        -1,
        8,
        10
    };

    private static final int[] NORWAY_LIKE_RESULT =
    {
        0,
        0,
        9,
        9,
        8,
        8,
        8,
        10
    };

    private static final int[][] EXPECTED_LIKE_RESULTS = 
    {
        DEFAULT_LIKE_RESULT,
        ENGLISH_LIKE_RESULT,
        POLISH_LIKE_RESULT,
        NORWAY_LIKE_RESULT
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
     * Private/Protected setup/utility methods of This class:
     **************************************************************************
     */

    /**
     * Assert that the query does not compile and throws
     * a SQLException with the expected state.
     * 
     * @param conn      Connection to correct datatabase.
     * @param sqlState  expected sql state.
     * @param query     the query to compile.
     */
    private void assertCompileError(
    Connection  conn,
    String      sqlState, 
    String      query) 
    {
        try 
        {
            PreparedStatement pSt = conn.prepareStatement(query);

            if (usingDerbyNet())
            {
                /* For JCC the prepares are deferred until execution,
                 * so we have to actually execute in order to see the
                 * expected error.  Note that we don't need to worry
                 * about binding the parameters (if any); the compile
                 * error should occur before the execution-time error
                 * about unbound parameters.
                 */
                pSt.execute();
            }
            fail("expected compile error: " + sqlState);
            pSt.close();
        } 
        catch (SQLException se) 
        {
            assertSQLState(sqlState, se);
        }
    }

    /**
     * RESOLVE - unfinished LIKE test with dataset of all unicode characters
     **/
    private static final void printRuleBasedCollator()
    {
        // get en_US Collator rules
        RuleBasedCollator en_USCollator = 
            (RuleBasedCollator)Collator.getInstance(Locale.US);
        String en_rules = en_USCollator.getRules();

        System.out.println("ENGLISH RULES: " + en_rules);
        System.out.println("ENGLISH RULES: " + formatString(en_rules, true));
        System.out.println("ENGLISH RULES: " + formatString(en_rules, false));
    }

    /**
     * RESOLVE - unfinished LIKE test with dataset of all unicode characters
     **/
    private static final String formatString(
    String  str,
    boolean all)
    {
        // format it as \u0000(x)\u0001(x)...
        String ret_val = "";

        for (int i = 0; i < str.length(); i++)
        {
            char ch = str.charAt(i);

            if (!all && (ch <= 128))
            {
                ret_val += Character.toString(ch);
            }
            else
            {
                ret_val += 
                    "\\u" + Integer.toString(ch, 16) + 
                    "(" + Character.toString(ch) + ")";
            }
        }
        return(ret_val);
    }

    /**
     * RESOLVE - unfinished LIKE test with dataset of all unicode characters
     **/
    private final void formatLikeResults(
    Connection  conn,
    String      query)
        throws SQLException
    {
        Statement s     = conn.createStatement();
        ResultSet rs    = s.executeQuery(query);


        String    txt_str   = null;
        String    ucode_str = null;
        String    mixed_str = null;

        int       count     = 0;

        while (rs.next())
        {
            count++;

            if (count == 1)
            {
                txt_str   = "{";
                ucode_str = "{";
                mixed_str = "{";
            }
            else
            {
                txt_str   += ", ";
                ucode_str += ", ";
                mixed_str += ", ";
            }

            String ret_val = rs.getString(1);

            txt_str   += "\"" + ret_val + "\"";
            
            // string using \u0104 format for chars
            ucode_str += formatString(ret_val, true);

            mixed_str += "{\"" + formatString(ret_val, false) + "\"}";
        }

        if (count != 0)
        {
            txt_str   += "}";
            ucode_str += "}";
            mixed_str += "}";
        }

        System.out.println(
            "Query: " + query + 
            "\nnumber rows  :" + count     +
            "\nString Result:" + txt_str   +
            "\nUcode  Result:" + ucode_str +
            "\nmixed  Result:" + mixed_str);

        rs.close();
        s.close();
    }

    /**
     * RESOLVE - unfinished LIKE test with dataset of all unicode characters
     **/
    private final void printLikeResults(Connection conn)
        throws SQLException
    {
        /*
        RESOLVE-COMMENTED OUT

        for (int i = 0; i < LIKE_ALLVALS_TEST.length; i++)
        {
            if (verbose_debug)
            {
                System.out.println(
                    "Running like allvals test[" + i + "] = " + 
                    LIKE_ALLVALS_TEST[i]);
            }

            formatLikeResults(
                conn,
                "SELECT STR_VARCHAR FROM ALLVALS WHERE STR_VARCHAR LIKE " +
                    "'" + LIKE_ALLVALS_TEST[i] + "'");

            formatLikeResults(
                conn,
                "SELECT STR_LONGVARCHAR FROM ALLVALS WHERE STR_LONGVARCHAR LIKE " +
                    "'" + LIKE_ALLVALS_TEST[i] + "'");

            formatLikeResults(
                conn,
                "SELECT STR_CLOB FROM ALLVALS WHERE STR_CLOB LIKE " +
                    "'" + LIKE_ALLVALS_TEST[i] + "'");

            formatLikeResults(
                conn,
                "SELECT STR_CHAR FROM ALLVALS WHERE STR_CHAR LIKE " +
                    "'" + LIKE_ALLVALS_TEST[i] + "'");
        }
        */
    }

    private void checkLangBasedQuery(
    Connection  conn,
    String      query, 
    String[][]  expectedResult,
    boolean     ordered) 
        throws SQLException 
    {
        Statement s  = conn.createStatement();
        ResultSet rs = s.executeQuery(query);

        if (verbose_debug)
        {
            System.out.println("executed query: " + query);
        }

        if (expectedResult == null) //expecting empty resultset from the query
        {
            if (verbose_debug)
                System.out.println(
                    "executed query expecting no results: " + query);

            JDBC.assertEmpty(rs);
        }
        else
        {
            if (ordered)
            {
                if (verbose_debug)
                    System.out.println(
                        "executed query expecting ordered results: " + query);

                JDBC.assertFullResultSet(rs, expectedResult);
            }
            else
            {
                if (verbose_debug)
                    System.out.println(
                        "executed query expecting unordered results: " + query);

                JDBC.assertUnorderedResultSet(rs, expectedResult);
            }
        }

        rs.close();
        s.close();
    }

    private void checkOneParamQuery(
    Connection  conn,
    String      query, 
    String      param,
    String[][]  expectedResult, 
    boolean     ordered) 
        throws SQLException 
    {
        PreparedStatement   ps = conn.prepareStatement(query);
        ps.setString(1, param);
        ResultSet           rs = ps.executeQuery();

        if (expectedResult == null) //expecting empty resultset from the query
        {
            JDBC.assertEmpty(rs);
        }
        else
        {
            if (ordered)
                JDBC.assertFullResultSet(rs,expectedResult);
            else
                JDBC.assertUnorderedResultSet(rs, expectedResult);
        }


        // re-execute it to test path through the cache
        ps.setString(1, param);
        rs = ps.executeQuery();

        if (expectedResult == null) //expecting empty resultset from the query
        {
            JDBC.assertEmpty(rs);
        }
        else
        {
            if (ordered)
                JDBC.assertFullResultSet(rs,expectedResult);
            else
                JDBC.assertUnorderedResultSet(rs, expectedResult);
        }

        rs.close();
        ps.close();
        conn.commit();
    }

    /**
     * Perform export using SYSCS_UTIL.SYSCS_EXPORT_TABLE procedure.
     */
    protected void doExportTable(
    Connection  conn,
    String      schemaName, 
    String      tableName, 
    String      fileName, 
    String      colDel , 
    String      charDel, 
    String      codeset) 
        throws SQLException 
    {
        CallableStatement ps = 
            conn.prepareCall(
                "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (? , ? , ? , ?, ? , ?)");
        ps.setString(1, schemaName);
        ps.setString(2, tableName);
        ps.setString(3, fileName);
        ps.setString(4, colDel);
        ps.setString(5, charDel);
        ps.setString(6, codeset);
        ps.executeUpdate();
        ps.close();
    }

    /**
     * Perform import using SYSCS_UTIL.SYSCS_IMPORT_TABLE procedure.
     */
    protected void doImportTable(
    Connection  conn,
    String      schemaName, 
    String      tableName, 
    String      fileName, 
    String      colDel, 
    String      charDel, 
    String      codeset,
    int         replace) 
        throws SQLException 
    {
        CallableStatement ps = 
            conn.prepareCall(
                "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (?, ?, ?, ?, ?, ?, ?)");
        ps.setString(1, schemaName);
        ps.setString(2, tableName);
        ps.setString(3, fileName);
        ps.setString(4, colDel);
        ps.setString(5, charDel);
        ps.setString(6, codeset);
        ps.setInt(   7, replace);
        ps.executeUpdate();
        ps.close();
    }

    /**
     * Produce an expect row set given the order and asc/desc info.
     * <p>
     * Given the expected order of rows, the offset of first and last row
     * to return, and whether rows will be ascending or descending produce
     * a 2d expected row set.  Each row in the row set represents a row 
     * with 2 columns (ID, NAME) from the CUSTOMER table used throughout
     * this test.
     *
     * @param expected_order    Expected order of rows in this language.
     * @param start_offset      expect rows starting at 
     *                          expected_order[start_offset] up to and including
     *                          expected_order[stop_offset].
     * @param stop_offset       expect rows starting at 
     *                          expected_order[start_offset] up to and including
     *                          expected_order[stop_offset].
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

    /**
     * Produce an expect row set given list and offset of row in list.
     * <p>
     * Given the list of rows and offset of the expected row in the list
     * produce a 2d expected row set.  If expected_row is -1 then no row
     * set is returned.  Each row in the row set represents a row 
     * with 2 columns (ID, NAME) from the CUSTOMER table used throughout
     * this test.
     *
     * @param expected_row      -1 if no expected row, else 
     *                          ret_list[expected_row] is single value expected.
     * @param ret_list          list of strings in data set.
     **/
    private String[][] full_row_single_value(
    int         expected_row,
    String[]    ret_list)
    {
        String[][] ret_order = null;

        if (expected_row != -1)
        {
            // if not -1 then exactly one row expected.
            ret_order = new String[1][2];
            ret_order[0][0] = String.valueOf(expected_row);
            ret_order[0][1] = ret_list[expected_row];
        }

        return(ret_order);
    }

	private boolean is142JVM() 
	{
        // bug shows up in ibm142, sun142, 
        // and j9 (J2ME Foundation Specification v1.1)
	    String java_version = System.getProperty("java.version");

        // only match java versions with leading 1.4.2
        String leading_version_no = java_version.substring(0, 5);

        /*
        System.out.println("java_version = " + java_version);
        System.out.println("leading_version_no = " + leading_version_no);
        */


        return(leading_version_no.equals("1.4.2") || 
               java_version.equals("J2ME Foundation Specification v1.1"));
	}

    private boolean isDatabaseBasicCollation(Connection conn)
        throws SQLException
    {
        PreparedStatement ps = 
            conn.prepareStatement(
                "VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY" + 
                    "('derby.database.collation')");

        ResultSet rs = ps.executeQuery();

        Assert.assertTrue(rs.next());

        String collation = rs.getString(1);

        Assert.assertFalse(rs.next());

        rs.close();
        ps.close();

        return(collation.equals("UCS_BASIC"));
    }


    /**************************************************************************
     * Set up and clean up routines.
     **************************************************************************
     */

    /**
     * Creates a database and return connection to database.
     * <p>
     * Creates a database with territory/collation as indicated by db_index
     * test case.  
     * Database name:       TEST_DATABASE[db_index]
     * territory attribute: TEST_CONNECTION_ATTRIBUTE[db_index]
     **/
    private Connection setUpDBandOpenConnection(int db_index) 
        throws SQLException 
    {
        DataSource ds = 
            JDBCDataSource.getDataSourceLogical(TEST_DATABASE[db_index]);

        // what territory, if any to specify
        String territory_str = 
            (TEST_CONNECTION_ATTRIBUTE[db_index] == null) ?  
                "" : ";territory=" + TEST_CONNECTION_ATTRIBUTE[db_index];

        String collation_str = 
            (TEST_COLLATION_ATTRIBUTE[db_index] == null) ?  
                "" : ";collation=TERRITORY_BASED";

        // build strings of the form:
        // create=true
        // create=true;territory=en;collation=TERRITORY_BASED
        // create=true;collation=TERRITORY_BASED
        String conn_string = "create=true" + territory_str + collation_str;

        if (verbose_debug)
            System.out.println("connection attribute string: " + conn_string);

        JDBCDataSource.setBeanProperty(ds, "connectionAttributes", conn_string);

        Connection conn = ds.getConnection();

        return(conn);
    }

    private void setUpTable(Connection conn) throws SQLException 
    {
        Statement s = conn.createStatement();
        s.execute(
            "CREATE TABLE CUSTOMER(" +
                "D1 CHAR(200), D2 CHAR(200), D3 CHAR(200), D4 INT, " + 
                "ID INT, NAME VARCHAR(40), NAME2 VARCHAR(40))");

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

    private void setUpLikeTable(Connection conn) throws SQLException 
    {
        conn.setAutoCommit(false);

        Statement s = conn.createStatement();
        s.execute(
            "CREATE TABLE CUSTOMER ("              +
                "NAME_CHAR          CHAR(10), "    +
                "NAME_VARCHAR       VARCHAR(40),"  +
                "NAME_LONGVARCHAR   LONG VARCHAR," +
                "NAME_CLOB          CLOB,"         +
                "ID                 INT)");

        PreparedStatement ps = 
            conn.prepareStatement("INSERT INTO CUSTOMER VALUES(?,?,?,?,?)");

        for (int i = 0; i < LIKE_NAMES.length; i++)
        {
            ps.setString(1, LIKE_NAMES[i]);
            ps.setString(2, LIKE_NAMES[i]);
            ps.setString(3, LIKE_NAMES[i]);
            ps.setString(4, LIKE_NAMES[i]);
            ps.setInt(   5, i);
            ps.executeUpdate();
        }

        ps.close();
        s.close();
    }

    
    /**
     * RESOLVE - unfinished LIKE test with dataset of all unicode characters
     **/
    private void setUpALLVALS(Connection conn) 
        throws SQLException 
    {
        Statement s = conn.createStatement();

        s.execute(
            "CREATE TABLE ALLVALS ("              +
                "STR_CHAR          CHAR(3), "    +
                "STR_VARCHAR       VARCHAR(40),"  +
                "STR_LONGVARCHAR   LONG VARCHAR," +
                "STR_CLOB          CLOB,"         +
                "ID                INT)");

        PreparedStatement ps = 
            conn.prepareStatement("INSERT INTO ALLVALS VALUES(?,?,?,?,?)");

        char[]  single_char = new char[1];
        char[]  leading_b   = new char[2];
        char[]  trailing_b  = new char[2];
        char[]  middle_b    = new char[3];

        leading_b[0]                        = 'b';
        trailing_b[trailing_b.length - 1]   = 'b';
        middle_b[1]                         = 'b';

        int max_char = (int) Character.MAX_VALUE;

        long before_load_ms = System.currentTimeMillis();

        for (int i = Character.MIN_VALUE; i <= max_char; i++)
        {
            // insert a row with string value of a single unicode char
            single_char[0] = (char) i;
            String  str_val = String.valueOf(single_char);

            ps.setString(1, str_val);
            ps.setString(2, str_val);
            ps.setString(3, str_val);
            ps.setString(4, str_val);
            ps.setInt(   5, i);
            ps.executeUpdate();

            // insert a row with 'b' followed by unicode value followed by 'b'
            leading_b[1] = (char) i;
            str_val = String.valueOf(leading_b);

            ps.setString(1, str_val);
            ps.setString(2, str_val);
            ps.setString(3, str_val);
            ps.setString(4, str_val);
            ps.setInt(   5, i);
            ps.executeUpdate();

            // insert a row with unicode value followed by 'b' 
            trailing_b[0] = (char) i;
            str_val = String.valueOf(trailing_b);

            ps.setString(1, str_val);
            ps.setString(2, str_val);
            ps.setString(3, str_val);
            ps.setString(4, str_val);
            ps.setInt(   5, i);
            ps.executeUpdate();

            // insert a row with unicode value followed by 'b' 
            // followed by unicode value
            middle_b[0] = (char) i;
            middle_b[2] = (char) i;
            str_val = String.valueOf(middle_b);

            ps.setString(1, str_val);
            ps.setString(2, str_val);
            ps.setString(3, str_val);
            ps.setString(4, str_val);
            ps.setInt(   5, i);
            ps.executeUpdate();
        }

        long after_load_ms = System.currentTimeMillis();

        conn.commit();

        long after_commit_ms = System.currentTimeMillis();

        if (verbose_debug)
        {

            System.out.println("Loaded and committed ALLVALS table:");
            System.out.println(
                "load time = "   + (after_load_ms   - before_load_ms) +
                "commit time = " + (after_commit_ms - after_load_ms));
        }

        ps.close();
        s.close();
    }


    private void dropLikeTable(Connection conn) 
        throws SQLException 
    {
        Statement s = conn.createStatement();
	
        s.execute("DROP TABLE CUSTOMER");     
        s.close();

    }

    /**************************************************************************
     * run*() tests, called from the actual test*() tests.
     **************************************************************************
     */


    /**
     * Test simple call to DatabaseMetaData.getColumns()
     * <p>
     * This test is the same form of the getColumns() call that 
     * the IMPORT and EXPORT system procedures depend on. 
     * Currently on ibm and sun 1.4.2 jvm's this test fails.
     **/
    private void runDERBY_2703(Connection conn, int db_index)
        throws SQLException
    {
        // DERBY-2703, get columns does not work in collated dbs under sun
        // and ibm 142 jvm's.
        if (is142JVM())
            return;

        setUpTable(conn);

        ResultSet rs = 
            conn.getMetaData().getColumns(null, "APP", "CUSTOMER", "%");

        Assert.assertTrue("catch bug where no rows are returned.", rs.next());

        if (verbose_debug)
            System.out.println("column =" + rs.getString(4));

        while (rs.next())
        {
            if (verbose_debug)
                System.out.println("column =" + rs.getString(4));
        }

        // TODO should verify all columns are returned.

        rs.close();

        dropTable(conn);

    }

    /**************************************************************************
     * Private/Protected tests of This class:
     **************************************************************************
     */

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
                    true),
                true);

            // '<=' test
            checkLangBasedQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME <= '" + 
                    NAMES[expected_order[i]] + "' ORDER BY NAME",
                full_row_set(
                    expected_order, 
                    0, 
                    i,
                    true),
                true);

            // '=' test
            checkLangBasedQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME = '" + 
                    NAMES[expected_order[i]] + "' ORDER BY NAME",
                full_row_set(
                    expected_order, 
                    i, 
                    i,
                    true),
                true);

            // '>=' test
            checkLangBasedQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME >= '" + 
                    NAMES[expected_order[i]] + "' ORDER BY NAME",
                full_row_set(
                    expected_order, 
                    i, 
                    expected_order.length - 1,
                    true),
                true);


            // '>' test
            checkLangBasedQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME > '" + 
                    NAMES[expected_order[i]] + "' ORDER BY NAME",
                full_row_set(
                    expected_order, 
                    i + 1, 
                    expected_order.length - 1,
                    true),
                true);

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
                    true),
                true);

            // '<=' test
            checkOneParamQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME <= ? ORDER BY NAME",
                NAMES[expected_order[i]],
                full_row_set(
                    expected_order, 
                    0, 
                    i,
                    true),
                true);

            // '=' test
            checkOneParamQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME = ? ORDER BY NAME",
                NAMES[expected_order[i]],
                full_row_set(
                    expected_order, 
                    i, 
                    i,
                    true),
                true);

            // '>=' test
            checkOneParamQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME >= ? ORDER BY NAME",
                NAMES[expected_order[i]],
                full_row_set(
                    expected_order, 
                    i, 
                    expected_order.length - 1,
                    true),
                true);

            // '>' test
            checkOneParamQuery(
                conn, 
                "SELECT ID, NAME FROM CUSTOMER where NAME > ? ORDER BY NAME",
                NAMES[expected_order[i]],
                full_row_set(
                    expected_order, 
                    i + 1, 
                    expected_order.length - 1,
                    true),
                true);
        }
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
    private void checkTwoPersistentCompare(
    Connection  conn,
    int[]       expected_order)
        throws SQLException
    {
        Statement s  = conn.createStatement();

        conn.commit();
        s.execute(
            "ALTER TABLE CUSTOMER ADD COLUMN TWO_CHECK_CHAR CHAR(40)");
        s.execute(
            "ALTER TABLE CUSTOMER ADD COLUMN TWO_CHECK_VARCHAR VARCHAR(400)");

        // Set CHAR field to be third item im expected order array
        PreparedStatement   ps = 
            conn.prepareStatement("UPDATE CUSTOMER SET TWO_CHECK_CHAR = ?"); 
        ps.setString(1, NAMES[expected_order[3]]);
        ps.executeUpdate();

        // Set VARCHAR field to be third item im expected order array
        ps = 
            conn.prepareStatement("UPDATE CUSTOMER SET TWO_CHECK_VARCHAR = ?"); 

        ps.setString(1, NAMES[expected_order[3]]);
        ps.executeUpdate();

        // check persistent compared to persistent - VARCHAR TO CHAR, 
        // should return rows bigger than 3rd in expected order.
        checkLangBasedQuery(
            conn, 
            "SELECT ID, NAME FROM CUSTOMER WHERE NAME > TWO_CHECK_CHAR ORDER BY NAME",
            full_row_set(
                expected_order,
                4, 
                expected_order.length - 1,
                true),
            true);

        // check persistent compared to persistent - CHAR TO VARCHAR, 
        // should return rows bigger than 3rd in expected order.
        checkLangBasedQuery(
            conn, 
            "SELECT ID, NAME FROM CUSTOMER WHERE TWO_CHECK_CHAR < NAME ORDER BY NAME",
            full_row_set(
                expected_order,
                4, 
                expected_order.length - 1,
                true),
            true);

        // check persistent compared to persistent - VARCHAR TO VARCHAR, 
        // should return rows bigger than 3rd in expected order.
        checkLangBasedQuery(
            conn, 
            "SELECT ID, NAME FROM CUSTOMER WHERE NAME > TWO_CHECK_VARCHAR ORDER BY NAME",
            full_row_set(
                expected_order,
                4, 
                expected_order.length - 1,
                true),
            true);

        // check persistent compared to persistent - CHAR TO CHAR, 
        // should return rows bigger than 3rd in expected order.
        checkLangBasedQuery(
            conn, 
            "SELECT ID, NAME FROM CUSTOMER WHERE D3 > TWO_CHECK_CHAR ORDER BY NAME",
            full_row_set(
                expected_order,
                4, 
                expected_order.length - 1,
                true),
            true);

        // put back data the way it was on entry to test.
        conn.rollback();
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
                true),
            true);

        // Simple check of getting all rows back in order
        checkLangBasedQuery(
            conn, 
            "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME, ID",
            full_row_set(
                EXPECTED_NAME_ORDER[db_index], 
                0, 
                EXPECTED_NAME_ORDER[db_index].length - 1, 
                true),
            true);

        // Simple check of getting all rows back in opposite order
        checkLangBasedQuery(
            conn, 
            "SELECT ID, NAME FROM CUSTOMER ORDER BY NAME DESC",
            full_row_set(
                EXPECTED_NAME_ORDER[db_index], 
                0, 
                EXPECTED_NAME_ORDER[db_index].length - 1, 
                false),
            true);

        // Check <, <=, =, >=, > operators on constant vs. column
        checkSimpleCompare(conn, EXPECTED_NAME_ORDER[db_index]);

        // Check compare of 2 persistent values, using join
        checkTwoPersistentCompare(conn, EXPECTED_NAME_ORDER[db_index]);

        if (create_idx_qry != null)
            s.execute("DROP INDEX " + idx_name);

        conn.commit();
    }

    /**
     * Test various like expressions against all string datatypes.
     *
     * T6: (DONE) test like
     * @throws SQLException
     **/
    private void runLikeTests(
    Connection  conn,
    int         db_index)
        throws SQLException
    {
        setUpLikeTable(conn);

        for (int i = 0; i < LIKE_TEST_CASES.length; i++)
        {
            if (verbose_debug)
            {
                System.out.println(
                    "Running like test[" + i + "] = " + LIKE_TEST_CASES[i]);
            }

            // varchar column - constant pattern
            checkLangBasedQuery(
                conn,
                "SELECT ID, NAME_VARCHAR FROM CUSTOMER " + 
                    "WHERE NAME_VARCHAR LIKE '" + LIKE_TEST_CASES[i] + "'",
                full_row_single_value(
                    EXPECTED_LIKE_RESULTS[db_index][i],
                    LIKE_NAMES),
                true);

            // varchar column - parameter pattern
            checkOneParamQuery(
                conn,
                "SELECT ID, NAME_VARCHAR FROM CUSTOMER " + 
                    "WHERE NAME_VARCHAR LIKE ?",
                LIKE_TEST_CASES[i],
                full_row_single_value(
                    EXPECTED_LIKE_RESULTS[db_index][i],
                    LIKE_NAMES),
                true);

            // long varchar column - constant
            checkLangBasedQuery(
                conn,
                "SELECT ID, NAME_LONGVARCHAR FROM CUSTOMER " + 
                    "WHERE NAME_LONGVARCHAR LIKE '" + LIKE_TEST_CASES[i] + "'",
                full_row_single_value(
                    EXPECTED_LIKE_RESULTS[db_index][i],
                    LIKE_NAMES),
                true);

            // long varchar column - parameter
            checkOneParamQuery(
                conn,
                "SELECT ID, NAME_LONGVARCHAR FROM CUSTOMER " + 
                    "WHERE NAME_LONGVARCHAR LIKE ?",
                LIKE_TEST_CASES[i],
                full_row_single_value(
                    EXPECTED_LIKE_RESULTS[db_index][i],
                    LIKE_NAMES),
                true);

            // clob column - constant
            checkLangBasedQuery(
                conn,
                "SELECT ID, NAME_CLOB FROM CUSTOMER WHERE NAME_CLOB LIKE " +
                    "'" + LIKE_TEST_CASES[i] + "'",
                full_row_single_value(
                    EXPECTED_LIKE_RESULTS[db_index][i],
                    LIKE_NAMES),
                true);

            // clob column - parameter
            checkOneParamQuery(
                conn,
                "SELECT ID, NAME_CLOB FROM CUSTOMER WHERE NAME_CLOB LIKE ?",
                LIKE_TEST_CASES[i],
                full_row_single_value(
                    EXPECTED_LIKE_RESULTS[db_index][i],
                    LIKE_NAMES),
                true);

            // char column, char includes blank padding so alter all these
            // tests cases to match for blanks at end also.
            checkLangBasedQuery(
                conn,
                "SELECT ID, NAME_CHAR FROM CUSTOMER WHERE NAME_CHAR LIKE " + 
                    "'" + LIKE_CHAR_TEST_CASES[i] + "%'",
                full_row_single_value(
                    EXPECTED_LIKE_RESULTS[db_index][i],
                    LIKE_NAMES),
                true);

            // char column, char includes blank padding so alter all these
            // tests cases to match for blanks at end also.
            checkOneParamQuery(
                conn,
                "SELECT ID, NAME_CHAR FROM CUSTOMER WHERE NAME_CHAR LIKE ?",
                LIKE_CHAR_TEST_CASES[i] + "%",
                full_row_single_value(
                    EXPECTED_LIKE_RESULTS[db_index][i],
                    LIKE_NAMES),
                true);
        }

        // test error thrown from LIKE on mismatched collation 
        String zero_row_syscat_query1 = 
            "SELECT * from SYS.SYSCOLUMNS where COLUMNNAME like 'nonmatchiing'";
        String zero_row_syscat_query2 = 
            "SELECT * from SYS.SYSCOLUMNS where 'nonmatchiing' like COLUMNNAME";
        String zero_row_syscat_query_param1 = 
            "SELECT * from SYS.SYSCOLUMNS where COLUMNNAME like ?";
        String zero_row_syscat_query_param2 = 
            "SELECT * from SYS.SYSCOLUMNS where ? like COLUMNNAME";

        if (!isDatabaseBasicCollation(conn))
        {
            // collation of 'fred' picked up from current schema which is
            // territory based collation, but system column will have basic
            // collation.

            assertCompileError(conn, "42ZA2", zero_row_syscat_query1);
            assertCompileError(conn, "42ZA2", zero_row_syscat_query2);
            assertCompileError(conn, "42ZA2", zero_row_syscat_query_param1);
            assertCompileError(conn, "42ZA2", zero_row_syscat_query_param2);
        }
        else
        {
            checkLangBasedQuery(conn, zero_row_syscat_query1, null, true);
            checkLangBasedQuery(conn, zero_row_syscat_query2, null, true);
            checkOneParamQuery(
                conn, zero_row_syscat_query_param1, "nonmatchiing", null, true);
            checkOneParamQuery(
                conn, zero_row_syscat_query_param2, "nonmatchiing", null, true);
        }

        dropLikeTable(conn);
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

        setUpTable(conn);
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

        dropTable(conn);
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

        setUpTable(conn);
        conn.commit();

        s.execute("ALTER TABLE CUSTOMER DROP COLUMN D1");
        runQueries(conn, db_index, null, null);

        s.execute("CREATE INDEX IDX1 ON CUSTOMER (NAME)");
        s.execute("ALTER TABLE CUSTOMER DROP COLUMN D2");
        runQueries(conn, db_index, null, null);
        conn.rollback();

        dropTable(conn);
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

        setUpTable(conn);

        conn.commit();

        s.execute("ALTER TABLE CUSTOMER DROP COLUMN NAME");
        s.execute("ALTER TABLE CUSTOMER ADD COLUMN NAME CHAR(40)");
        s.execute("UPDATE CUSTOMER SET NAME = D1");
        runQueries(conn, db_index, null, null);

        s.execute("CREATE INDEX IDX1 ON CUSTOMER (NAME)");
        runQueries(conn, db_index, null, null);

        dropTable(conn);

        conn.commit();
    }

    /**
     * Bulk insert test.
     * <p>
     * Tests code path through create conglomerate code executed as part of
     * a bulk table insert.  In empty table and replace case the bulk table
     * code will create new conglomerates for the base table and index table
     * and this tests the code that the correct collation is associated with
     * the new tables/indexes.
     *
     * Tests:
     * T13: (DONE) bulk insert into empty table, with and without indexes
     * T14: (DONE) bulk insert replace, with and without indexes
     **/
    private void runBulkInsert(
    Connection  conn,
    int         db_index)
        throws SQLException 
    {
        // DERBY-2703, get columns does not work in collated dbs under sun
        // and ibm 142 jvm's.
        if (is142JVM())
            return;

        Statement s = conn.createStatement();

        setUpTable(conn);

        // export CUSTOMER date to names.dat
        String fileName =
            (SupportFilesSetup.getReadWrite("names.dat")).getPath();

        doExportTable(conn, "APP", "CUSTOMER", fileName, null, null, "UTF-16");

        conn.commit();


        // bulk insert to empty table, no indexes without replace 
        // (last arg 0 = no replace).
        s.execute("DELETE FROM CUSTOMER");
        conn.commit();

        // checkGetColumn(conn);

        doImportTable(
            conn, "APP", "CUSTOMER", fileName, null, null, "UTF-16", 0);
        runQueries(conn, db_index, null, null);

        // bulk insert to empty table, with indexes without replace 
        // (last arg 0 = no replace).
        s.execute("DELETE FROM CUSTOMER");
        s.execute("CREATE INDEX IDX1 ON CUSTOMER (NAME)");
        s.execute("CREATE INDEX IDX2 ON CUSTOMER (NAME, ID)");
        s.execute("CREATE INDEX IDX3 ON CUSTOMER (ID,   NAME)");
        s.execute("CREATE INDEX IDX4 ON CUSTOMER (ID)");
        s.execute("CREATE INDEX IDX5 ON CUSTOMER (ID, NAME, D1, D2, D3)");
        doImportTable(
            conn, "APP", "CUSTOMER", fileName, null, null, "UTF-16", 0);
        runQueries(conn, db_index, null, null);
        s.execute("DROP INDEX IDX1 ");
        s.execute("DROP INDEX IDX2 ");
        s.execute("DROP INDEX IDX3 ");
        s.execute("DROP INDEX IDX4 ");
        s.execute("DROP INDEX IDX5 ");

        // bulk insert to non-empty table, no indexes with replace, call 
        // import first to double the rows in the table.
        // (last arg to Import 1 = replace).
        doImportTable(
            conn, "APP", "CUSTOMER", fileName, null, null, "UTF-16", 0);
        doImportTable(
            conn, "APP", "CUSTOMER", fileName, null, null, "UTF-16", 1);
        runQueries(conn, db_index, null, null);

        // bulk insert to non-empty table, indexes with replace, call 
        // import first to double the rows in the table.
        // (last arg to Import 1 = replace).
        s.execute("CREATE INDEX IDX1 ON CUSTOMER (NAME)");
        s.execute("CREATE INDEX IDX2 ON CUSTOMER (NAME, ID)");
        s.execute("CREATE INDEX IDX3 ON CUSTOMER (ID,   NAME)");
        s.execute("CREATE INDEX IDX4 ON CUSTOMER (ID)");
        s.execute("CREATE INDEX IDX5 ON CUSTOMER (ID, NAME, D1, D2, D3)");
        doImportTable(
            conn, "APP", "CUSTOMER", fileName, null, null, "UTF-16", 0);
        doImportTable(
            conn, "APP", "CUSTOMER", fileName, null, null, "UTF-16", 1);
        runQueries(conn, db_index, null, null);
        s.execute("DROP INDEX IDX1 ");
        s.execute("DROP INDEX IDX2 ");
        s.execute("DROP INDEX IDX3 ");
        s.execute("DROP INDEX IDX4 ");
        s.execute("DROP INDEX IDX5 ");

        dropTable(conn);

        conn.commit();
    }


    private static final String[] derby2670_pattern =
    {
        "%",
        "a%",
        "b%",
        "c%",
        "%a%",
        "%b%",
        "%c%",
        "%a",
        "%b",
        "%c"
    };

    private static final String[][][] derby2670_pattern_result = 
    {
        // pattern = % 
        { {"a"}, 
          {"A"}, 
          {" a"}, 
          {"-a"}, 
          {"\u00ADa"}, 
          {"b"}, 
          {"B"}, 
          {" b"}, 
          {"-b"}, 
          {"\u00ADb"}, 
          {"C"},
          {"ekstra\u00ADarbeid"}, 
          {"ekstrabetaling"}, 
          {"ekstraarbeid"}, 
          {"Wanvik"}, 
          {"W\u00E5gan"},
          {"Waagan"}, 
          {"W\u00E5han"}
        },
        // pattern = a% 
        { {"a"} },
        // pattern = b% 
        { {"b"} },
        // pattern = c% 
        null,
        // pattern = %a% 
        { {"a"}, 
          {" a"}, 
          {"-a"}, 
          {"\u00ADa"}, 
          {"ekstra\u00ADarbeid"}, 
          {"ekstrabetaling"}, 
          {"Wanvik"}, 
          {"W\u00E5gan"},
          {"Waagan"}, 
          {"W\u00E5han"}
        },
        // pattern = %b% 
        { {"b"}, 
          {" b"}, 
          {"-b"}, 
          {"\u00ADb"}, 
          {"ekstra\u00ADarbeid"}, 
          {"ekstrabetaling"}, 
          {"ekstraarbeid"}
        }, 
        // pattern = %c% 
        null,
        // pattern = %a
        { {"a"}, 
          {" a"}, 
          {"-a"}, 
          {"\u00ADa"} 
        }, 
        // pattern = %b
        { {"b"}, 
          {" b"}, 
          {"-b"}, 
          {"\u00ADb"}
        }, 
        // pattern = %c
        null
    };

    /**
     * Test case for DERBY-2670 - problem with like in no like processing.
     * <p>
     * Before fix, the table/query below would return results like B and
     * C, obviously wrong for like %a%.  The code was incorrectly caching
     * collation key info in a DataValueDescriptor across the reuse of the
     * holder object from one row to the next.
     * <p>
     * Added more patterns to also test DERBY-2710 and DERBY-2706, both
     * to do with bad like optimization which can not be applied to collation
     * based like.
     **/
    private void runDerby2670(
    Connection conn)
        throws SQLException
    {
        Statement s = conn.createStatement();

        String[] rows = 
            { "Waagan", "W\u00E5han", "Wanvik", "W\u00E5gan", "ekstrabetaling",
              "ekstraarbeid", "ekstra\u00ADarbeid", "\u00ADa", "a", "\u00ADb", 
              "b", "-a", "-b", " a", " b", "A", "B", "C" 
            };


        s.executeUpdate("create table t (x varchar(20))");
        PreparedStatement ps = conn.prepareStatement("insert into t values ?");
        for (int i = 0; i < rows.length; i++) {
            ps.setString(1, rows[i]);
            ps.executeUpdate();
        }
        ps.close();

        Assert.assertTrue(
            "source and result arrays do not match for derby2670",
            derby2670_pattern_result.length == derby2670_pattern.length);

        String like_qry = "select * from t where x like ";
        PreparedStatement ps_like = 
            conn.prepareStatement("select * from t where x like ?");
        PreparedStatement ps_like_orderby = 
            conn.prepareStatement("select * from t where x like ? order by x");

        for (int i = 0; i < derby2670_pattern.length; i++)
        {
            // Try just unordered like with constant pattern
            String qry = like_qry + "'" + derby2670_pattern[i] + "'";

            checkLangBasedQuery(
                conn, qry, derby2670_pattern_result[i], false);

            // add an order by 
            qry += " order by x";

            checkLangBasedQuery(
                conn, qry, derby2670_pattern_result[i], false);

            // try parameter for pattern
            ps_like.setString(1, derby2670_pattern[i]);
            ResultSet rs = ps_like.executeQuery();

            if (derby2670_pattern_result[i] == null)
                JDBC.assertEmpty(rs);
            else
                JDBC.assertUnorderedResultSet(rs, derby2670_pattern_result[i]);

            rs.close();
            rs = null;

            // try parameter for pattern
            ps_like_orderby.setString(1, derby2670_pattern[i]);
            rs = ps_like_orderby.executeQuery();

            if (derby2670_pattern_result[i] == null)
                JDBC.assertEmpty(rs);
            else
                JDBC.assertFullResultSet(rs, derby2670_pattern_result[i]);

            rs.close();
            rs = null;
        }

        // add an index and try it again.
        s.executeUpdate("create index t_idx on t (x)");
        like_qry = "select * from t where x like ";

        for (int i = 0; i < derby2670_pattern.length; i++)
        {
            // Try just unordered like with constant pattern
            String qry = like_qry + "'" + derby2670_pattern[i] + "'";

            checkLangBasedQuery(
                conn, qry, derby2670_pattern_result[i], false);

            // add an order by 
            qry += " order by x";

            checkLangBasedQuery(
                conn, qry, derby2670_pattern_result[i], false);

            // try parameter for pattern
            ps_like.setString(1, derby2670_pattern[i]);
            ResultSet rs = ps_like.executeQuery();

            if (derby2670_pattern_result[i] == null)
                JDBC.assertEmpty(rs);
            else
                JDBC.assertUnorderedResultSet(rs, derby2670_pattern_result[i]);

            rs.close();
            rs = null;

            // try parameter for pattern
            ps_like_orderby.setString(1, derby2670_pattern[i]);
            rs = ps_like_orderby.executeQuery();


            if (derby2670_pattern_result[i] == null)
                JDBC.assertEmpty(rs);
            else
                JDBC.assertFullResultSet(rs, derby2670_pattern_result[i]);

            rs.close();
            rs = null;
        }



        s.executeUpdate("drop table t");

        conn.commit();
        
        // cleanup
        ps_like_orderby.close();
        ps_like.close();
        s.close();
    }

    /**
     * Shared code to run all test cases against a single collation.
     * <p>
     * Pass in the index of which TEST_DATABASE database to test.  So
     * for instance to run the default, pass in 0.
     * <p>
     *
     * @param db_index  index of which test to run.
     *
     * @exception  SQLException
     **/
    private void runTestIter(
    Connection  conn, 
    int         db_index) 
        throws SQLException 
    {
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

        dropTable(conn);

        // the following tests mess with column values and ddl, so they
        // are going to drop and recreate the small test data table.
        runDERBY_2703(conn, db_index);

        runAlterTableAddColumn(conn, db_index);

        runAlterTableCompress(conn, db_index);

        runBulkInsert(conn, db_index);

        runLikeTests(conn, db_index);


        /*
        TODO -MIKEM, this test does not work yet.
        runAlterTableDropColumn(conn, db_index);
        */

        conn.commit();
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    public void testDefaultCollation() throws SQLException
    {
        Connection conn = setUpDBandOpenConnection(TEST_DEFAULT);
        Assert.assertTrue(isDatabaseBasicCollation(conn));
        runTestIter(conn, TEST_DEFAULT);
        conn.close();
    }
    public void testEnglishCollation() throws SQLException
    {
        Connection conn = setUpDBandOpenConnection(TEST_ENGLISH);
        Assert.assertTrue(!isDatabaseBasicCollation(conn));
        runTestIter(conn, TEST_ENGLISH);
        conn.close();
    }

    public void testPolishCollation() throws SQLException
    {
        Connection conn = setUpDBandOpenConnection(TEST_POLISH);
        Assert.assertTrue(!isDatabaseBasicCollation(conn));
        runTestIter(conn, TEST_POLISH);
        conn.close();
    }
    public void testNorwayCollation() throws SQLException
    {
        Connection conn = setUpDBandOpenConnection(TEST_NORWAY);
        Assert.assertTrue(!isDatabaseBasicCollation(conn));
        runDerby2670(conn);
        runTestIter(conn, TEST_NORWAY);
        conn.close();
    }
    /**
     * Test creating a TERRITORY_BASED collated database by only setting
     * the collation attribute.  The Territory will be picked up from the
     * default territory of the JVM.
     *
     * Tests:
     * T24: DERBY-2669 If no territory attribute is specified at create 
     *      database time, then create collated db based on default 
     *      territory of Database.
     **/
    public void testDefaultJVMTerritoryCollation() throws SQLException
    {
        boolean run_test = false;
        int     db_index = -1;

        Locale locale = Locale.getDefault();

        if (locale.getLanguage().equals("en"))
        {
            run_test = true;
            db_index = TEST_ENGLISH;
        }
        else if (locale.getLanguage().equals("no"))
        {
            run_test = true;
            db_index = TEST_NORWAY;
        }
        else if (locale.getLanguage().equals("po"))
        {
            run_test = true;
            db_index = TEST_POLISH;
        }

        if (run_test)
        {
            Connection conn = setUpDBandOpenConnection(TEST_DEFAULT_TERRITORY);
            Assert.assertTrue(!isDatabaseBasicCollation(conn));
            runTestIter(conn, db_index);
            conn.close();
        }
    }
    
    public static Test suite() 
    {
        // only test in embedded mode, all tests are server side actions.
        Test test =  
               TestConfiguration.embeddedSuite(CollationTest2.class);

        // add support to use external files for import/export calls.
        test = new SupportFilesSetup(test);

        // turn on log statement text for sequence of statements in derby.log.  
        if (verbose_debug)
        {
            Properties props = new Properties();
            props.setProperty("derby.language.logStatementText", "true");
            test = new SystemPropertyTestSetup(test, props);
        }

        // database to use for testing default collation.
        test = TestConfiguration.additionalDatabaseDecorator(
                    test, TEST_DATABASE[TEST_DEFAULT]);

        // database to use for testing collation, english territory.
        test = TestConfiguration.additionalDatabaseDecorator(
                    test, TEST_DATABASE[TEST_ENGLISH]);

        // database to use for testing collation, polish territory.
        test = TestConfiguration.additionalDatabaseDecorator(
                    test, TEST_DATABASE[TEST_POLISH]);

        // database to use for testing collation, norway territory.
        test = TestConfiguration.additionalDatabaseDecorator(
                    test, TEST_DATABASE[TEST_NORWAY]);

        // database to use for testing collation, default jvm territory
        test = TestConfiguration.additionalDatabaseDecorator(
                    test, TEST_DATABASE[TEST_DEFAULT_TERRITORY]);

        return test;
    }
}
