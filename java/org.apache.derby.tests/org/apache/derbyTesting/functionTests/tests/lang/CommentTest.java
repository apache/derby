/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CommentTest

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
*/
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Types;

import junit.framework.Assert;
import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test for comments, and a few tests related to parsing non-comment SQL.
 */
public final class CommentTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public CommentTest(String name)
    {
        super(name);
    }

    /**
     * Create a suite of tests.
    */
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4338
        return TestConfiguration.defaultSuite(CommentTest.class);
    }

    /**
     * Some simple tests of bracketed comments.
     */
    public void testBracketedComments() throws Exception
    {
        Statement stmt = createStatement();
        
        JDBC.assertFullResultSet(
            stmt.executeQuery("/* a comment */ VALUES 1"), 
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
//IC see: https://issues.apache.org/jira/browse/DERBY-4338
            stmt.executeQuery("-- eof comment\nVALUES 1"),
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery("VALUES 1 /* a comment */"),
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery("VALUES /* a comment */ 1"),
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery("VALUES /* a comment \n with newline */ 1"),
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery("VALUES /* SELECT * from FOO */ 1"),
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery("VALUES /* a comment /* nested comment */ */ 1"), 
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery(
                "VALUES /*/* XXX /*/*/* deeply nested comment */*/*/YYY*/*/ 1"),
            new String [][] {{"1"}});

        // mix with eol-comments
        JDBC.assertFullResultSet(
            stmt.executeQuery(
                "VALUES 1 --/*/* XXX /*/*/* deeply nested comment */*/*/YYY*/*/ 1"),
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery(
                "VALUES 1 --/*/* XXX /*/*/* deeply nested comment */*/*/YYY*/*/ 1--/*"),
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery("VALUES /* a comment --\n with newline */ 1"),
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery("VALUES /* a comment -- */ 1"),
            new String [][] {{"1"}});

        JDBC.assertFullResultSet(
            stmt.executeQuery("VALUES /* a comment \n-- */ 1"),
            new String [][] {{"1"}});

        // mix with string quotes
        JDBC.assertFullResultSet(
            stmt.executeQuery("VALUES '/* a comment \n-- */'"),
            new String [][] {{"/* a comment \n-- */"}});

        // unterminated comments generate lexical errors
        assertCompileError("42X02", "VALUES 1 /*");
        assertCompileError("42X02", "VALUES 1 /* comment");
        assertCompileError("42X02", "VALUES 1 /* comment /*");
        assertCompileError("42X02", "VALUES 1 /* comment /* nested */");

        // just comments generates syntax error
//IC see: https://issues.apache.org/jira/browse/DERBY-3242
        assertCompileError("42X01", "/* this is a comment */");
//IC see: https://issues.apache.org/jira/browse/DERBY-4338
        assertCompileError("42X01", "/* this is a comment */ /* /* foo */ */");
        assertCompileError(
            "42X01",
            "\n\r\r\n/* Weird newlines in front of a comment */" +
                " /* /* foo */ */");
        assertCompileError("42X01", "-- this is a comment \n");

        // sole comment error
        assertCompileError("42X02", "/* this is not quite a comment");
    }


    /**
     * Test that an initial bracketed comment doesn't affect the checks for
     * executeQuery(executeUpdate
     */
    public void testInitialComment_derby4338() throws Exception
    {
        Statement s = createStatement();

        JDBC.assertDrainResults(
            s.executeQuery("/* comment */ select * from sys.systables"));
        JDBC.assertDrainResults(
            s.executeQuery("/* */\nSELECT * from sys.systables"));
        JDBC.assertDrainResults(
            s.executeQuery("/* --*/\n\rSELECT * from sys.systables"));
        JDBC.assertDrainResults(
            s.executeQuery("--\nselect * from sys.systables"));

        s.executeUpdate("/* /* foo*/ */ create table t (i int)");
        s.executeUpdate("--\n drop table t");

        PreparedStatement ps = prepareStatement(
            "{call syscs_util." +
            "syscs_set_database_property('foo', ?)}");
        ps.setString(1, "bar");
        ps.execute();

        if (usingEmbedded()) {
            Assert.assertTrue(ps.getUpdateCount() == 0);
        } else {
            // Change to 0 when DERBY-211 is fixed.
            Assert.assertTrue(ps.getUpdateCount() == -1);
        }

        // The escape after the comment below was not handled correctly prior
        // to DERBY-4338, i.e. the statement was not classified as a "call"
        // statement.
        ps = prepareStatement(
            "--\n{call syscs_util." +
            "syscs_set_database_property('foo', ?)}");
        ps.setString(1, "bar");
        ps.execute();

        // The assert blows up for the client prior to fix of DERBY-4338.
        if (usingEmbedded()) {
            Assert.assertEquals(0, ps.getUpdateCount());
        } else {
            // Change to 0 when DERBY-211 is fixed.
            Assert.assertEquals(-1, ps.getUpdateCount());
        }

        ps.setNull(1, Types.VARCHAR); // clean up setting
        ps.execute();
    }

    /**
     * Test that an statement classifier in client doesn't get confused over
     * keywords that end in *, ' and ". This is not strictly a comment test,
     * but was fixed as part of DERBY-4338.
     */
    public void testWrongKeywordLexing_derby4338() throws Exception
    {
        Statement s = createStatement();

        JDBC.assertDrainResults(
            s.executeQuery("select* from sys.systables"));
        JDBC.assertDrainResults(
            s.executeQuery("select'a' from sys.systables"));
        JDBC.assertDrainResults(
            s.executeQuery("select\"TABLEID\" from sys.systables"));

        // Added for DERBY-4748.
//IC see: https://issues.apache.org/jira/browse/DERBY-4748
        assertCompileError("42X01", "commit");
        assertCompileError("42X01", "commit;");
    }

    /**
     * Default connections to auto-commit false.
     */
    protected void initializeConnection(Connection conn) throws SQLException
    {
        conn.setAutoCommit(false);
    }
}
