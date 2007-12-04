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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test for comments.
 */
public final class CommentTest extends BaseJDBCTestCase {
    private Statement stmt;

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
        return TestConfiguration.embeddedSuite(CommentTest.class);
    }

    /**
     * Some simple tests of bracketed comments.
     */
    public void testBracketedComments() throws Exception
    {
        JDBC.assertFullResultSet(
            stmt.executeQuery("/* a comment */ VALUES 1"), 
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

        // unterminated comments
        assertCallError("42X03", getConnection(), "VALUES 1 /*");
        assertCallError("42X03", getConnection(), "VALUES 1 /* comment");
        assertCallError("42X03", getConnection(), "VALUES 1 /* comment /*");
        assertCallError("42X03", getConnection(), "VALUES 1 /* comment /* nested */");
    }
    
    /**
     * Set the fixture up.
     */
    protected void setUp() throws SQLException
    {    
        getConnection().setAutoCommit(false);
        stmt = createStatement();
    }
    
    /**
     * Tear-down the fixture.
     */
    protected void tearDown() throws Exception
    {
        stmt.close();
        super.tearDown();
    }
}
