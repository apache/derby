package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/*
Class org.apache.derbyTesting.functionTests.tests.jdbc4.Derby3650Test

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

/** 
 * These are tests to test the cases for DERBY-3650.
 * The tests won't pass until that bug is fixed. 
 */

public class Derby3650Test extends BaseJDBCTestCase 
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    // DERBY-3749 causes tests that commit after looking at the stream to 
    // break while trying to access the streams in subsequent rows.  When
    // that bug gets fixed, enable these tests.
    private static final boolean runDerby3749tests = false;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    public Derby3650Test(String name) 
    {
        super(name);
    }
    
    public void setUp() 
        throws SQLException
    {
        getConnection().setAutoCommit(false);
    }


    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**
     * Test select of multiple rows containing single clob column.
     * <p>
     * Expects input query to return 3 column's per row, which should be:
     * (id, length of clob, clob)  
     *
     * Will verify clob using verifyClob().
     * <p>
     * Runs the query 4 times testing the following combinations:
     *     free clob on each row = true,  commit xact after each row = true
     *     free clob on each row = true,  commit xact after each row = false
     *     free clob on each row = false, commit xact after each row = true
     *     free clob on each row = false, commit xact after each row = false
     *
     * @param query                 query to run.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    private void runQueryCasesClob(
    String query)
        throws SQLException, IOException
    {
        if (runDerby3749tests)
            runQueryClob(query, true,  true);
        runQueryClob(query, true,  false);
        if (runDerby3749tests)
            runQueryClob(query, false, true);
        runQueryClob(query, false, false);
    }

    /**
     * Test select of multiple rows containing single clob column.
     * <p>
     * Expects input query to return 3 column's per row, which should be:
     * (id, length of clob, clob)  
     *
     * Will verify clob using verifyClob().
     * <p>
     *
     * @param query                 query to run.
     * @param freelob               true if we should free the lob after it has
     *                              been retrieved and verified.
     * @param commitAfterLobVerify  true if we should commit after the lob has
     *                              been retrieved and verified.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    private void runQueryClob(
    String  query,
    boolean freelob,
    boolean commitAfterLobVerify)
        throws SQLException, IOException
    {
        PreparedStatement ps = prepareStatement(query);

        ResultSet rs = ps.executeQuery();
        while (rs.next()) 
        {
            int  id     = rs.getInt(1);
            int  length = rs.getInt(2);
            Clob clob   = rs.getClob(3);

            // verify that stream can be read and is right.
            verifyClob(
                clob.getCharacterStream(), 
                length,
                new LoopingAlphabetReader(length));

            if (freelob)
                clob.free();
            if (commitAfterLobVerify)
                commit();
        }
        rs.close();
        commit();

        rs = ps.executeQuery();
        while (rs.next()) 
        {
            int  id     = rs.getInt(1);
            int  length = rs.getInt(2);

            // verify that stream can be read and is right.
            verifyClob(
                rs.getCharacterStream(3),
                length,
                new LoopingAlphabetReader(length));

            if (commitAfterLobVerify)
                commit();
        }
        rs.close();
        commit();

        ps.close();

    }

    /**
     * Test select of multiple rows containing single blob column.
     * <p>
     * Expects input query to return 3 column's per row, which should be:
     * (id, length of clob, clob)  
     * Will verify blob using verifyBlob().
     * <p>
     * Runs the query 4 times testing the following combinations:
     *     free blob on each row = true,  commit xact after each row = true
     *     free blob on each row = true,  commit xact after each row = false
     *     free blob on each row = false, commit xact after each row = true
     *     free blob on each row = false, commit xact after each row = false
     *
     * @param query                 query to run.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    private void runQueryCasesBlob(
    String query)
        throws SQLException, IOException
    {
        if (runDerby3749tests)
            runQueryBlob(query, true,  true);
        runQueryBlob(query, true,  false);
        if (runDerby3749tests)
            runQueryBlob(query, false, true);
        runQueryBlob(query, false, false);
    }

    /**
     * Test select of multiple rows containing single blob column.
     * <p>
     * Expects input query to return single column per row, which is a blob.
     * Will verify blob using verifyBlob().
     * <p>
     *
     * @param query                 query to run.
     * @param freelob               true if we should free the lob after it has
     *                              been retrieved and verified.
     * @param commitAfterLobVerify  true if we should commit after the lob has
     *                              been retrieved and verified.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    private void runQueryBlob(
    String  query,
    boolean freelob,
    boolean commitAfterLobVerify)
        throws SQLException, IOException
    {     
        PreparedStatement ps = prepareStatement(query);

        ResultSet rs = ps.executeQuery();
        while (rs.next()) 
        {
            Blob blob = rs.getBlob(3);
            verifyBlob(blob.getBinaryStream(), rs.getInt(2), rs.getInt(1));
            if (freelob)
                blob.free();
            if (commitAfterLobVerify)
                commit();
        }
        rs.close();
        rollback();

        rs = ps.executeQuery();
        while (rs.next()) 
        {
            // note, the order of "getXXX" is important.  This routine will
            // fail in network client if the 3rd arg is requested before the
            // 1st arg.  In that case attempts to read from the stream get
            // a closed error.  This is why the values are retrieved first
            // and then passed to the call.
            int         id      = rs.getInt(1);
            int         length  = rs.getInt(2);
            InputStream stream  = rs.getBinaryStream(3);

            verifyBlob(stream, length, id);

            if (commitAfterLobVerify)
                commit();
        }
        rs.close();
        commit();

        ps.close();
    }

    private void verifyClob(Reader input, int length, Reader expected) 
        throws SQLException, IOException 
    {
        int input_char;
        int expect_char;
        int charcount = 0;

        do 
        {
            input_char  = input.read();
            expect_char = expected.read();

            if (input_char != -1) 
            {
                charcount++;
                if ((char) input_char != expect_char) 
                {
                    fail("Unexpected Character " + (char) input_char + 
                            " expected " + (char) expect_char);
                }
            }

        } while (input_char != -1) ;

        if (charcount != length)
        {
           fail("Unexpected character count " + charcount + 
                   "expected: " + length);
        }
    }
    
    private void verifyBlob(InputStream is, int length, int id) 
        throws SQLException, IOException 
    {
        int b;
        int bytecount = 0;
        do 
        {
            b = is.read();
            if (b != -1) 
            {
                bytecount++;
                if ((byte) b != id) 
                {
                    fail("Unexpected byte value " + (byte) b + 
                            " expected: " + id);
                }
            }
        } while (b != -1);

        if (bytecount != length)
        {
            fail("Unexpected byte count, got " + bytecount + 
                    "  expected " + length);
        }
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */


    /**
     * Test a nested loop join for clobs.
     * <p>
     * Test case of a 1 to many row join where the 1 row contains and returns
     * a clob as a stream.  Before fix for DERBY-3650 each row returned from
     * this join would contain a reference to the same stream which would fail
     * in various ways depending on commit, free, and reading the stream.
     *
     * @throws SQLException
     * @throws IOException
     */
    public void test1ToManyJoinClob() 
        throws SQLException, IOException     
    {           
        runQueryCasesClob(
            "select testClob.id, length, c from testClob " + 
                "join jointab on jointab.id = testClob.id");
    }

    /**
     * Test a nested loop join for blobs.
     * <p>
     * Test case of a 1 to many row join where the 1 row contains and returns
     * a blob as a stream.  Before fix for DERBY-3650 each row returned from
     * this join would contain a reference to the same stream which would fail
     * in various ways depending on commit, free, and reading the stream.
     *
     * @throws SQLException
     * @throws IOException
     */
    public void test1ToManyJoinBlob() 
        throws SQLException, IOException     
    {     
        runQueryCasesBlob(
            "select testBlob.id, length, c from testBlob " +
                "join jointab on jointab.id = testBlob.id");
    }


    public void test1ToManyHashJoinClob() 
        throws SQLException, IOException     
    {           
        runQueryCasesClob(
            "select testClob.id, length, c from " + 
            "--DERBY-PROPERTIES joinOrder=FIXED \n" + 
            "testClob --DERBY-PROPERTIES joinStrategy=HASH \n" + 
            "join jointab on jointab.id = testClob.id");

        runQueryCasesClob(
            "select jointab.id, length, c from " + 
            "--DERBY-PROPERTIES joinOrder=FIXED \n" + 
            "jointab --DERBY-PROPERTIES joinStrategy=HASH \n" + 
            "join testClob on jointab.id = testClob.id");
    }

    public void test1ToManyHashJoinBlob() 
        throws SQLException, IOException     
    {           
        runQueryCasesBlob(
            "select testBlob.id, length, c from " + 
            "--DERBY-PROPERTIES joinOrder=FIXED \n" + 
            "testBlob --DERBY-PROPERTIES joinStrategy=HASH \n" + 
            "join jointab on jointab.id = testBlob.id");

        runQueryCasesBlob(
            "select testBlob.id, length, c from " + 
            "--DERBY-PROPERTIES joinOrder=FIXED \n" + 
            "jointab --DERBY-PROPERTIES joinStrategy=HASH \n" + 
            "join testBlob on jointab.id = testBlob.id");
    }

    public void test1ToManyleftOuterJoinClob() 
        throws SQLException, IOException     
    {           
        runQueryCasesClob(
            "select testClob.id, length, c from testClob " + 
                "left outer join jointab on jointab.id = testClob.id");

        runQueryCasesClob(
            "select jointab.id, length, c from jointab " + 
                "left outer join testClob on jointab.id = testClob.id");
    }

    public void test1ToManyleftOuterJoinBlob() 
        throws SQLException, IOException     
    {           
        runQueryCasesBlob(
            "select testBlob.id, length, c from testBlob " + 
                "left outer join jointab on jointab.id = testBlob.id");
        runQueryCasesBlob(
            "select jointab.id, length, c from jointab " + 
                "left outer join testBlob on jointab.id = testBlob.id");
    }


    /**
     * Test straight select from a heap scan of multiple rows containing clobs.
     *
     * @throws SQLException
     * @throws IOException
     */
    public void testClobSelect() 
        throws SQLException, IOException     
    {           
        runQueryCasesClob("select id, length, c from testMultipleClob");
    }

    /**
     * Test straight select from a heap scan of multiple rows containing blobs.
     *
     * @throws SQLException
     * @throws IOException
     */
    public void testBlobSelect() 
        throws SQLException, IOException     
    {     
        runQueryCasesBlob("select id, length, c from testMultipleBlob");
    }
    
    private static void initializeClobTables(Statement stmt) 
        throws SQLException, IOException
    {
        // CLOB TEST SETUP...........................................
        stmt.executeUpdate(
            "CREATE TABLE testClob (id int, length int, c CLOB(2M))");

        Connection conn = stmt.getConnection();
        PreparedStatement ps = 
            conn.prepareStatement("INSERT INTO TestClob VALUES(?,?,?)");

        // insert 4 rows into "left" table containing clobs of join: 
        //     (1, clob), (1, clob), (2, clob), (2, clob)
        ps.setInt(            1, 1);
        ps.setInt(            2, 40000);
        ps.setCharacterStream(3, new LoopingAlphabetReader(40000));
        ps.executeUpdate();

        ps.setInt(            1, 1);
        ps.setInt(            2, 40001);
        ps.setCharacterStream(3, new LoopingAlphabetReader(40001));
        ps.executeUpdate();

        ps.setInt(            1, 2);
        ps.setInt(            2, 40002);
        ps.setCharacterStream(3, new LoopingAlphabetReader(40002));
        ps.executeUpdate();

        ps.setInt(            1, 2);
        ps.setInt(            2, 40003);
        ps.setCharacterStream(3, new LoopingAlphabetReader(40003));
        ps.executeUpdate();
        ps.close();

        stmt.executeUpdate(
            "CREATE TABLE testMultipleClob (id int, length int, c CLOB(2M))");
        ps = conn.prepareStatement(
                "INSERT INTO testMultipleClob VALUES(?,?,?)");

        for (int i = 0; i < 100; i++)
        {
            ps.setInt(            1, i);
            ps.setInt(            2, 40000 + i);
            ps.setCharacterStream(3, new LoopingAlphabetReader(40000 + i));
            ps.executeUpdate();
        }
        ps.close();
        conn.commit();
    }

    private static void initializeBlobTables(Statement stmt) 
        throws SQLException, IOException
    {
        // BLOB TEST SETUP...........................................
        stmt.executeUpdate(
                "CREATE TABLE testBlob (id int, length int, c BLOB(2M))");

        Connection conn = stmt.getConnection();
        PreparedStatement ps = 
            conn.prepareStatement("INSERT INTO TestBlob VALUES(?,?,?)");

        // insert 4 rows into "left" blob of join: 
        //     (1, 40000, blob), (1, 40001, blob), 
        //     (2, 40002, blob), (2, 40003, blob)
        byte[] mybytes = new byte[40000];
        Arrays.fill(mybytes, (byte) 1);
        ps.setInt(  1, 1);
        ps.setInt(  2, 40000);
        ps.setBytes(3, mybytes);
        ps.executeUpdate();

        mybytes = new byte[40001];
        Arrays.fill(mybytes, (byte) 1);
        ps.setInt(  1, 1);
        ps.setInt(  2, 40001);
        ps.setBytes(3, mybytes);
        ps.executeUpdate();

        mybytes = new byte[40002];
        Arrays.fill(mybytes, (byte) 2);
        ps.setInt(  1, 2);
        ps.setInt(  2, 40002);
        ps.setBytes(3, mybytes);
        ps.executeUpdate();

        mybytes = new byte[40003];
        Arrays.fill(mybytes, (byte) 2);
        ps.setInt(  1, 2);
        ps.setInt(  2, 40003);
        ps.setBytes(3, mybytes);
        ps.executeUpdate();

        ps.close();

        // insert 4 rows into "right" table of join: 
        stmt.executeUpdate("CREATE TABLE jointab (id int)");
        stmt.executeUpdate("INSERT INTO jointab values(1)");
        stmt.executeUpdate("INSERT INTO jointab values(1)");
        stmt.executeUpdate("INSERT INTO jointab values(2)");
        stmt.executeUpdate("INSERT INTO jointab values(2)");

        stmt.executeUpdate(
            "CREATE TABLE testMultipleBlob (id int, length int, c BLOB(2M))");
        ps = conn.prepareStatement(
                "INSERT INTO testMultipleBlob VALUES(?,?,?)");

        for (int i = 0; i < 100; i++)
        {
            mybytes = new byte[40000 + i];
            Arrays.fill(mybytes, (byte) i);

            ps.setInt(  1, i);
            ps.setInt(  2, 40000 + i);
            ps.setBytes(3,mybytes);
            ps.executeUpdate();
        }
        ps.close();
        conn.commit();
    }

    
    protected static Test baseSuite(String name) {
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(Derby3650Test.class);
        return new CleanDatabaseTestSetup(
                DatabasePropertyTestSetup.setLockTimeouts(suite, 2, 4)) 
        {
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                try
                {
                    initializeClobTables(stmt);
                    initializeBlobTables(stmt);
                }
                catch (IOException ioe)
                {
                    fail("Unexpected I/O exception during setup: " + ioe);
                }
            }
        };
    }


    public static Test suite() 
    {
        BaseTestSuite suite = new BaseTestSuite("Derby3650Test");
        suite.addTest(baseSuite("Derby3650Test:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
            baseSuite("Derby3650Test:client")));
        return suite;

    }
}
