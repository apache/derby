/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DateTimeTest

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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the builtin date/time types assumes these builtin types 
 * exist: int, smallint, char, varchar, real other things we might test: 
 * interaction with UUID and other user defined types 
 * compatibility with dynamic parameters and JDBC getDate etc. methods.
 */
public final class DateTimeTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public DateTimeTest(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("DateTimeTest");
        suite.addTest(baseSuite("DateTimeTest:Embedded"));
        suite.addTest(TestConfiguration
                .clientServerDecorator(baseSuite("DateTimeTest:Client")));
        return suite;
    }

    protected static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(DateTimeTest.class);
        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException {
                createTableForArithmeticTest(stmt);
                createTableForSyntaxTest(stmt);
                createTableForConversionTest(stmt);
                createTableForISOFormatTest(stmt);
            }
        };
    }
    
    private static void createTableForISOFormatTest(Statement st)
    throws SQLException{
        st.executeUpdate(" create table ts (ts1 timestamp, ts2 timestamp)");   
    }
    
    
    private static void createTableForConversionTest(Statement st) throws SQLException{
        st.executeUpdate(
                " create table convtest(d date, t time, ts timestamp)");
        st.executeUpdate(" insert into convtest values(date('1932-03-21'),  "
                + "time('23:49:52'), timestamp('1832-09-24 10:11:43.32'))");
        st.executeUpdate(" insert into convtest values(date('0001-03-21'),  "
                + "time('5:22:59'), timestamp('9999-12-31 23:59:59.999999'))");
        st.executeUpdate(" insert into convtest values(null, null, null)");
    }
    
    private static void createTableForSyntaxTest(Statement stmt) 
    throws SQLException{
        stmt.executeUpdate("create table source (i int, s smallint, c char(10), "
                + "v varchar(50), d double precision, r real, e date, "
                + "t time, p timestamp)");

        stmt.executeUpdate(" create table target (e date not null, t time not "
                + "null, p timestamp not null)");
    }
        
    private static void createTableForArithmeticTest(Statement stmt)
    throws SQLException{
        stmt.executeUpdate("create table t (i int, s smallint, " +
                "c char(10), v varchar(50), d double precision," +
                " r real, e date, t time, p timestamp)");

        stmt.executeUpdate(" insert into t values (null, null, " +
                "null, null, null, null, null, null, null)");

        stmt.executeUpdate(" insert into t values (0, 100, 'hello', " +
                "'everyone is here', 200.0e0, 300.0e0, " +
                "date('1992-01-01'), time('12:30:30'), " +
                "timestamp('1992-01-01 12:30:30'))");

        stmt.executeUpdate(" insert into t values (-1, -100, " +
                "'goodbye', 'everyone is there', -200.0e0, " +
                "-300.0e0, date('1992-01-01'), time('12:30:30'), " +
                "timestamp('1992-01-01 12:30:45'))");
    }
    
    /**
     * date/times don't support math, show each combination.
     */
    public void testArithOpers_math() throws SQLException{
        Statement st = createStatement();
        
        assertStatementError("42Y95", st, "select e + e from t");

        assertStatementError("42Y95", st, " select i + e from t");

        assertStatementError("42Y95", st, " select p / p from t");

        assertStatementError("42Y95", st, " select p * s from t");

        assertStatementError("42Y95", st, " select t - t from t");

        assertStatementError("42X37", st, " select -t from t");

        assertStatementError("42X37", st, " select +e from t");
        
        st.close();
    }
    
    public void testArithOpers_Comarision() throws SQLException{
        ResultSet rs = null;
        Statement st = createStatement();

        rs = st.executeQuery("select e from t where e = date('1992-01-01')");
        JDBC.assertColumnNames(rs, new String[] { "E" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1992-01-01" }, { "1992-01-01" } }, true);

        rs = st.executeQuery(" select e from t where date('1992-01-01') = e");
        JDBC.assertColumnNames(rs, new String[] { "E" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1992-01-01" }, { "1992-01-01" } }, true);

        rs = st.executeQuery(" select t from t where t > time('09:30:15')");
        JDBC.assertColumnNames(rs, new String[] { "T" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "12:30:30" }, { "12:30:30" } }, true);

        rs = st.executeQuery(" select t from t where time('09:30:15') < t");
        JDBC.assertColumnNames(rs, new String[] { "T" });
        JDBC.assertFullResultSet(rs,
                new String[][] { { "12:30:30" }, { "12:30:30" } }, true);

        rs = st.executeQuery(
                "select p from t where p < timestamp('1997-06-30 01:01:01')");
        JDBC.assertColumnNames(rs, new String[] { "P" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1992-01-01 12:30:30.0" },
                { "1992-01-01 12:30:45.0" } }, true);

        rs = st.executeQuery(
                "select p from t where timestamp('1997-06-30 01:01:01' )> p");
        JDBC.assertColumnNames(rs, new String[] { "P" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1992-01-01 12:30:30.0" },
                { "1992-01-01 12:30:45.0" } }, true);
        
        rs = st.executeQuery("select e from t where e >= date('1990-01-01')");
        JDBC.assertColumnNames(rs, new String[] { "E" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1992-01-01" }, { "1992-01-01" } }, true);

        rs = st.executeQuery(" select e from t where date('1990-01-01')<= e");
        JDBC.assertColumnNames(rs, new String[] { "E" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1992-01-01" }, { "1992-01-01" } }, true);

        rs = st.executeQuery(" select t from t where t <= time('09:30:15')");
        JDBC.assertColumnNames(rs, new String[] { "T" });
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(" select t from t where time('09:30:15') >= t");
        JDBC.assertColumnNames(rs, new String[] { "T" });
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
                "select p from t where p <> timestamp('1997-06-30 01:01:01')");
        JDBC.assertColumnNames(rs, new String[] { "P" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1992-01-01 12:30:30.0" },
                { "1992-01-01 12:30:45.0" } }, true);

        rs = st.executeQuery(
                "select p from t where timestamp('1997-06-30 01:01:01' )<> p");
        JDBC.assertColumnNames(rs, new String[] { "P" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1992-01-01 12:30:30.0" },
                { "1992-01-01 12:30:45.0" } }, true);
        
        st.close();
    }
    
    /**
     * Show comparisons with mixed types don't work.
     */
    public void testArithOpers_CompraionOnMixedTypes() throws SQLException{
        Statement st = createStatement();
        
        assertStatementError("42818", st, "select e from t where e <= i");

        assertStatementError("42818", st, " select e from t where t < s");

        assertStatementError("42818", st, " select e from t where p > d");

        assertStatementError("42818", st, " select e from t where e >= t");

        assertStatementError("42818", st, " select e from t where t <> p");

        assertStatementError("42818", st, " select e from t where p = e");

        st.close();
    }
    
    /**
     * Look for a value that isn't in the table.
     */
    public void testArithOpers_CompraionOnNotExistingValue() 
    throws SQLException{
        ResultSet rs = null;
        Statement st = createStatement();
        
        rs = st.executeQuery("select e from t where e <> date('1992-01-01')");
        JDBC.assertColumnNames(rs, new String[] { "E" });
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery("select e from t where date('1992-01-01') <> e");
        JDBC.assertColumnNames(rs, new String[] { "E" });
        JDBC.assertDrainResults(rs, 0);

        st.close();
    }
    
    /**
     * Show garbage in == errors out
     */
    public void testArithOpers_ComparisonOnGarbage() throws SQLException {
        Statement st = createStatement();
        
        assertStatementError("22008", st, 
                "select date( 'xxxx') from t where p is null");

        assertStatementError("22007", st, 
                " select time( '') from t where p is null");

        assertStatementError("22008", st,
                " select timestamp( 'is there anything here?' )from " +
                "t where p is null");

        assertStatementError("22008", st,
                " select timestamp( '1992-01- there anything here?')" +
                "from t where p is null");

        assertStatementError("22008", st,
                " select timestamp( '--::' )from t where p is null");

        assertStatementError("22007", st, 
                " select time('::::') from t where p is null");
        
        st.close();
    }
    
    /**
     * Check limit values.
     */
    public void testArithOpers_ComparisonOnLimits() throws SQLException {
        ResultSet rs = null;
        Statement st = createStatement();

        rs = st.executeQuery("values( date('0001-1-1'), date('9999-12-31'), "
                + "date('2/29/2000'), date('29.2.2004'))");
        JDBC.assertColumnNames(rs, new String[] { "1", "2", "3", "4" });
        JDBC.assertFullResultSet(rs, new String[][] { { "0001-01-01", 
                "9999-12-31", "2000-02-29", "2004-02-29" } }, true);

        rs = st.executeQuery(" values( time('00:00:00'), time('23:59:59'))");
        JDBC.assertColumnNames(rs, new String[] { "1", "2" });
        JDBC.assertFullResultSet(rs,
                new String[][] { { "00:00:00", "23:59:59" } }, true);

        rs = st.executeQuery(" values( time('00 AM'), time( '12:59 AM')," +
        		" time('1 PM'), time('12:59 PM'))");
        JDBC.assertColumnNames(rs, new String[] { "1", "2", "3", "4" });
        JDBC.assertFullResultSet(rs, new String[][] { { "00:00:00", 
                "00:59:00", "13:00:00", "12:59:00" } }, true);

        rs = st.executeQuery(" values( time('00.00.00'), time('23.59.59'), " +
                "time('24.00.00'))");
        JDBC.assertColumnNames(rs, new String[] { "1", "2", "3" });
        JDBC.assertFullResultSet(rs, new String[][] { { "00:00:00", 
                "23:59:59", "00:00:00" } }, true);

        rs = st.executeQuery(" values( timestamp('0001-1-1 00:00:00'), "
                + "timestamp('9999-12-31 23:59:59.999999'))");
        JDBC.assertColumnNames(rs, new String[] { "1", "2" });
        JDBC.assertFullResultSet(rs, new String[][] { { 
            "0001-01-01 00:00:00.0", "9999-12-31 23:59:59.999999" } }, true);
        
        st.close();
    }
    
    /**
     * Show that overflow and underflow are not allowed
     *  (SQL92 would have these report errors).
     */
    public void testArithOpers_ComparisonOnBeyondLimits() throws SQLException {
        Statement st = createStatement();
        
        assertStatementError("22008", st , "values( date('0000-01-01'))");

        assertStatementError("22008", st, " values( date('2000-00-01'))");

        assertStatementError("22008", st, " values( date('2000-01-00'))");

        assertStatementError("22008", st, " values( date('10000-01-01'))");

        assertStatementError("22008", st, " values( date('2000-13-01'))");

        assertStatementError("22008", st, " values( date('2000-01-32'))");

        assertStatementError("22008", st, " values( date('1900-02-29'))");

        assertStatementError("22008", st, " values( date('2001-02-29'))");

        assertStatementError("22007", st, " values( time('25.00.00'))");

        assertStatementError("22007", st, " values( time('24.00.01'))");

        assertStatementError("22007", st, " values( time('0:60:00'))");

        assertStatementError("22007", st, " values( time('00:00:60'))");

        st.close();
    }
    
    public void testArithOpers_ComparisonOnNullAndNonNull() 
    throws SQLException {
        ResultSet rs = null;
        Statement st = createStatement();
        
        rs = st.executeQuery("select e, t, p from t " +
        		"where e = e or t = t or p = p");
        JDBC.assertColumnNames(rs, new String[] { "E", "T", "P" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1992-01-01", 
                "12:30:30", "1992-01-01 12:30:30.0" }, { "1992-01-01",
                "12:30:30", "1992-01-01 12:30:45.0" } }, true);
        
        rs = st.executeQuery("select * from t where e is not null " +
        		"and t is not " + "null and p is not null");
        JDBC.assertColumnNames(rs, new String[] { "I", "S", "C", "V",
                "D", "R", "E", "T", "P" });
        JDBC.assertFullResultSet(rs,  new String[][] {
                { "0", "100", "hello", "everyone is here", "200.0", "300.0",
                    "1992-01-01", "12:30:30", "1992-01-01 12:30:30.0" },
                { "-1", "-100", "goodbye", "everyone is there", "-200.0",
                    "-300.0", "1992-01-01", "12:30:30", 
                    "1992-01-01 12:30:45.0" } }, true);
        
        st.close();
    }

    /**
     * Test =SQ .
     */
    public void testArithOpers_ComparisonOnEqualSQ() throws SQLException{
        ResultSet rs = null;
        Statement st = createStatement();

        assertStatementError("21000", st,
                "select 'fail' from t where e = (select e from t)");

        rs = st.executeQuery("select 'pass' from t " +
        		"where e = (select e from t where d=200)");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs,
                new String[][] { { "pass" }, { "pass" } }, true);

        assertStatementError("21000", st,
                "select 'fail' from t where t = (select t from t)");

        rs = st.executeQuery("select 'pass' from t " +
        		"where t = (select t from t where d=200)");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "pass" }, { "pass" } }, true);

        assertStatementError("21000", st,
                "select 'fail' from t where p = (select p from t)");

        rs = st.executeQuery("select 'pass' from t " +
        		"where p = (select p from t where d=200)");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "pass" } }, true);

        st.close();
    }
    
    /**
     * Test syntax: precision cannot be specified.
     */
    public void testSyntax_SpecifiedPrecision() throws SQLException{        
        Statement st = createStatement();
        
        assertStatementError("42X01", st , 
                "create table wrong (t time(-100))");

        assertStatementError("42X01", st, 
                " create table wrong (t time(0))");

        assertStatementError("42X01", st, 
                " create table wrong (t time(23))");

        assertStatementError("42X01", st,
                " create table wrong (t timestamp(-100))");

        assertStatementError("42X01", st,
                " create table wrong (t timestamp(0))");

        assertStatementError("42X01", st,
                " create table wrong (t timestamp(6))");

        assertStatementError("42X01", st,
                " create table wrong (t timestamp(9))");

        assertStatementError("42X01", st,
                " create table wrong (t timestamp(23))");
        
        st.close();
    }
    

    /**
     * Test a variety of inserts.
     */
    public void testSyntax_Insert() throws SQLException{
        Statement st = createStatement();
        
        st.executeUpdate("insert into source values (1, 2, '3', '4', 5, 6, "
                + "date('1997-07-07'), "
                + "time('08:08:08'),timestamp('1999-09-09 09:09:09'))");
        
        st.executeUpdate("insert into target select e,t,p from source");
        
        //wrong columns should fail
        assertStatementError("42821", st,
                "insert into target select p,e,t from source");
        
        assertStatementError("42821", st,
        " insert into target select i,s,d from source");

        assertStatementError("42821", st,
            " insert into target (t,p) select c,r from source");

        assertUpdateCount(st, 1, " delete from source");
        
        
        st.executeUpdate(" insert into source values (null, null, null, null, "
                + "null, null, null, null, null)");

        // these fail because the target won't take a null -- of any type
        assertStatementError("23502", st,
                "insert into target values(null, null, null)");

        assertStatementError("23502", st,
                " insert into target select e,t,p from source");
        
        //these still fail with type errors:
        assertStatementError("42821", st,
                "insert into target select p,e,t from source");

        assertStatementError("42821", st,
                " insert into target select i,s,d from source");

        assertStatementError("42821", st,
                " insert into target (t,p)select c,r from source");

        //expect 1 row in target.
        ResultSet rs = st.executeQuery("select * from target");
        JDBC.assertColumnNames(rs, new String[] { "E", "T", "P" });
        JDBC.assertFullResultSet(rs, new String[][] { {
                "1997-07-07", "08:08:08", "1999-09-09 09:09:09.0" } }, true);
        
        st.close();
    }
    
    /**
     * Test a variety of updates.
     */
    public void testSyntax_Update() throws SQLException{
        Statement st = createStatement();
        
        //expect 1 row in target.
        ResultSet rs = st.executeQuery("select * from target");
        JDBC.assertColumnNames(rs, new String[] { "E", "T", "P" });
        JDBC.assertFullResultSet(rs, new String[][] { {
                "1997-07-07", "08:08:08", "1999-09-09 09:09:09.0" } }, true);
        
        // unchanged:
        assertUpdateCount(st, 1, "update target set e = e, t = t, p = p");
        rs = st.executeQuery(" select * from target");
        JDBC.assertColumnNames(rs, new String[] { "E", "T", "P" });
        JDBC.assertFullResultSet(rs, new String[][] { { 
                "1997-07-07", "08:08:08", "1999-09-09 09:09:09.0" } }, true);

        // alters the row:
        assertUpdateCount(st, 1, "update target set e = date('1990-01-01')");
        rs = st.executeQuery(" select * from target");
        JDBC.assertColumnNames(rs, new String[] { "E", "T", "P" });
        JDBC.assertFullResultSet(rs, new String[][] { {
                "1990-01-01", "08:08:08", "1999-09-09 09:09:09.0" } }, true);
        
        //not settable to null
        assertStatementError("23502", st, "update target set e = null");
        rs = st.executeQuery(" select * from target");
        JDBC.assertColumnNames(rs, new String[] { "E", "T", "P" });
        JDBC.assertFullResultSet(rs, new String[][] { { 
                "1990-01-01", "08:08:08", "1999-09-09 09:09:09.0" } }, true);
        
        // nullable col can be set to null:
        assertUpdateCount(st, 1, "update source set e = date('1492-10-01')");
        rs = st.executeQuery(" select e from source");
        JDBC.assertColumnNames(rs, new String[] { "E" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1492-10-01" } }, true);

        assertUpdateCount(st, 1, " update source set e = null");
        rs = st.executeQuery(" select e from source");
        JDBC.assertColumnNames(rs,  new String[] { "E" });
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);
        
        // these should get type errors
        assertStatementError("42821", st, "update target set e = 1");
        assertStatementError("42821", st, " update source set p = 1.4e10");
        assertStatementError("42821", st,
                " update source set i = date('1001-01-01')");
        
        st.close();
    }
    
    public void testSyntax_CurrentFunctions() throws SQLException{
        ResultSet rs = null;
        Statement st = createStatement();
        
        // tests with current functions:
        assertUpdateCount(st, 1, "delete from source");
        assertUpdateCount(st, 1, " delete from target");

        st.executeUpdate(" insert into source values (1, 2, '3', " +
        		"'4', 5, 6, date('1997-06-07'), time('08:08:08'), " +
        		"timestamp('9999-09-09 09:09:09'))");

        // these tests are 'funny' so that the masters won't show 
        // a diff every time.
        rs = st.executeQuery("select 'pass' from source where current_date = "
                + "current_date and current_time = current_time and "
                + "current_timestamp = current_timestamp");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "pass" } }, true);

        rs = st.executeQuery(" select 'pass' from source where current_date > "
                + "date('1996-12-31') and current_time <= "
                + "time(        '23:59:59') -- may oopsie on leap second days "
                + "and current_timestamp <> timestamp( -- this comment "
                + "is just more whitespace '1996-12-31 00:00:00')");
        JDBC.assertColumnNames(rs, new String[] { "1" });        
        JDBC.assertFullResultSet(rs, new String[][] { { "pass" } }, true);

        // test with DB2 compatible syntax
        rs = st.executeQuery("select 'pass' from source where current date = "
                + "current date and current time = current time and "
                + "current timestamp = current timestamp");
        JDBC.assertFullResultSet(rs, new String[][] { { "pass" } }, true);

        rs = st.executeQuery(" select 'pass' from source where current date > "
                + "date('1996-12-31') and current time <= "
                + "time(        '23:59:59') -- may oopsie on leap second days "
                + "and current timestamp <> timestamp( -- this comment "
                + "is just more whitespace '1996-12-31 00:00:00')");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "pass" } }, true);
    }
    
    public void testSyntax_EscapedFunctions() throws SQLException{
        ResultSet rs = null;
        Statement st = createStatement();
        
        //CURRENT_DATE escaped function not supported in DB2 UDB 
        //CURRENT_TIME escaped function not supported in DB2 UDB
        assertStatementError("42X01", st, "select 'pass' from source " +
        		"where current_date = {fn current_date()} " +
        		"and current_time = {fn current_time()} " +
        		"and current_timestamp = current_timestamp");

        rs = st.executeQuery(" select 'pass' from source " +
        		"where current_date = {fn curdate()} " +
        		"and current_time = {fn curtime()} " +
        		"and current_timestamp = current_timestamp");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "pass" } }, true);

        
        // current_date() and current_time() not valid in DB2. 
        // curdate() and curtime() are as escaped functions only.
        assertStatementError("42Y03", st, "values curdate()");

        assertStatementError("42Y03", st, " values curtime()");

        assertStatementError("42X01", st, " values current_date()");

        assertStatementError("42X01", st, " values current_time()");

        assertStatementError("42X01", st, " values {fn current_date()}");

        assertStatementError("42X01", st, " values {fn current_time()}");

        
        // DB2 UDB compatible test for escaped functions
        rs = st.executeQuery("select 'pass' from source " +
        		"where hour(current_time) = {fn hour(current_time)} " +
        		"and minute(current_time) = {fn minute(current_time)}" +
        		" and second(current_time) = {fn second(current_time)} " +
        		"and year(current_date)   = {fn year(current_date)}");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "pass" } }, true);

        
        // valid jdbc date and time escaped functions
        rs = st.executeQuery("values {fn hour('23:38:10')}");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "23" } }, true);

        rs = st.executeQuery(" values {fn minute('23:38:10')}");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "38" } }, true);

        rs = st.executeQuery(" values {fn second('23:38:10')}");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "10" } }, true);

        rs = st.executeQuery(" values {fn year('2004-03-22')}");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "2004" } }, true);

        
        // currents do have types, these inserts fail:
        assertStatementError("42821", st,
                "insert into source values (0, 0, '0', '0', 0, 0, "
                        + "current_time, current_time, current_timestamp)");

        assertStatementError("42821", st, " insert into source values" +
        		" (0, 0, '0', '0', 0, 0, current_date, " +
        		"current_timestamp, current_timestamp)");

        assertStatementError("42821", st,
                " insert into source values (0, 0, '0', '0', 0, 0, "
                        + "current_date, current_time, current_date)");

        // this insert works
        st.executeUpdate("insert into source values (0, 0, '0', '0', 0, 0, "
                + "current_date, current_time, current_timestamp)");

        // test with DB2 syntax this insert works
        st.executeUpdate("insert into source values (0, 0, '0', '0', 0, 0, "
                + "current date, current time, current timestamp)");

        // this test will diff if the select is run just after 
        // midnight, and the insert above was run just before 
        // midnight...
        rs = st.executeQuery("select * from source " +
        		"where e <> current_date and p <> current_timestamp");
        JDBC.assertColumnNames(rs, new String[] { "I", "S", "C", "V", "D", 
                "R", "E", "T", "P" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1", "2", 
            "3", "4", "5.0", "6.0", "1997-06-07", "08:08:08",
            "9999-09-09 09:09:09.0" } }, true);

        
        // test with DB2 syntax
        rs = st.executeQuery("select * from source " +
        		"where e <> current date and p <> current timestamp");
        JDBC.assertColumnNames(rs, new String[] { "I", "S", "C", "V", 
                "D", "R", "E", "T", "P" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1", "2", "3",
            "4", "5.0", "6.0", "1997-06-07", "08:08:08", 
            "9999-09-09 09:09:09.0" } }, true);

        rs = st.executeQuery(" select 'pass' from source " +
        		"where e <= current_date and p <= current_timestamp");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "pass" }, { "pass" } }, true);

        // reduce it back to one row
        assertUpdateCount(st, 2, "delete from source where i=0");
        
        st.close();
    }
    
    public void testSyntax_Extract() throws SQLException{
        Statement st = createStatement();
        
        ResultSet rs = st.executeQuery("select year( e), month( e), day( date( "
                + "'1997-01-15')), hour( t), minute( t), second( time( "
                + "'01:01:42')), year( p), month( p), day( p), hour( "
                + "timestamp( '1992-01-01 14:11:23')), minute( p), "
                + "second( p) from source");
        JDBC.assertColumnNames(rs, new String[] { "1", "2", "3", "4", 
                "5", "6", "7", "8", "9", "10", "11", "12" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1997", "6", "15", 
            "8", "8", "42", "9999", "9", "9", "14", "9", "9.0" } }, true);

        // extract won't work on other types
        assertStatementError("42X25", st, "select month( i) from source");

        assertStatementError("42X25", st, " select hour( d) from source");

        // extract won't work on certain field/type combos
        assertStatementError("42X25", st, "select month( t) from source");

        assertStatementError("42X25", st, " select day( t) from source");

        assertStatementError("42X25", st, " select year( t) from source");

        assertStatementError("42X25", st, " select hour( e) from source");

        assertStatementError("42X25", st, " select minute( e) from source");

        assertStatementError("42X25", st, " select second( e) from source");

        assertUpdateCount(st, 1,
                " update source set i=month( e), s=minute( t), d=second( p)");

        // should be true and atomics should match field named as 
        // label in date/times
        rs = st.executeQuery("select i,e as \"month\",s,t " +
                "as \"minute\",d,p as \"second\" from source " +
                "where (i = month(e)) and (s = minute(t)) " +
                "and (d = second(p))");
        JDBC.assertColumnNames(rs, new String[] { "I", "month", 
                "S", "minute", "D", "second" });
        JDBC.assertFullResultSet(rs, new String[][] { { "6", "1997-06-07", 
                "8", "08:08:08", "9.0", "9999-09-09 09:09:09.0" } }, true);

        // fields should match the fields in the date (in order)
        rs = st.executeQuery("select p, year( p) as \"year\", month( p) as "
                + "\"month\", day( p) as \"day\", hour( p) as "
                + "\"hour\", minute( p) as \"minute\", second( p) as "
                + "\"second\" from source");
        JDBC.assertColumnNames(rs, new String[] { "P", "year", "month", 
                "day", "hour", "minute", "second" });
        JDBC.assertFullResultSet(rs, new String[][] { { 
            "9999-09-09 09:09:09.0", "9999", "9", "9",
            "9", "9", "9.0" } }, true);

        
        // jdbc escape sequences
        rs = st.executeQuery("values ({d '1999-01-12'}, {t '11:26:35'}, {ts "
                + "'1999-01-12 11:26:51'})");
        JDBC.assertColumnNames(rs, new String[] { "1", "2", "3" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1999-01-12", 
                "11:26:35", "1999-01-12 11:26:51.0" } }, true);

        rs = st.executeQuery(" values year( {d '1999-01-12'})");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1999" } }, true);

        rs = st.executeQuery(" values hour( {t '11:28:10'})");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "11" } }, true);

        rs = st.executeQuery(" values day( {ts '1999-01-12 11:28:23'})");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "12" } }, true);
        
        st.close();
    }
    
    /**
     * Random tests for date.
     */
    public void testRandom() throws SQLException{        
        Statement st = createStatement();
        
        st.executeUpdate("create table sertest(d date, s Date, o Date)");
        st.executeUpdate(" insert into sertest values (date('1992-01-03'), " +
        		"null, null)");
        
        ResultSet rs = st.executeQuery(" select * from sertest");
        JDBC.assertColumnNames(rs, new String[] { "D", "S", "O" });
        JDBC.assertFullResultSet(rs,
                new String[][] { { "1992-01-03", null, null } }, true);
        
        assertUpdateCount(st, 1, " update sertest set s=d");
        assertUpdateCount(st, 1, " update sertest set o=d");
        st.executeUpdate(" insert into sertest values (date( '3245-09-09'), "
                + "date( '1001-06-07'), date( '1999-01-05'))");
        
        rs = st.executeQuery(" select * from sertest");
        JDBC.assertColumnNames(rs, new String[] { "D", "S", "O" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1992-01-03", 
                "1992-01-03", "1992-01-03" }, { "3245-09-09", "1001-06-07", 
                    "1999-01-05" } }, true);

        rs = st.executeQuery(" select * from sertest where d > s");
        JDBC.assertColumnNames(rs, new String[] { "D", "S", "O" });
        JDBC.assertFullResultSet(rs, new String[][] { { "3245-09-09", 
                "1001-06-07", "1999-01-05" } }, true);

        assertUpdateCount(st, 2, " update sertest set d=s");

        // should get type errors:
        assertStatementError("42821", st,
                "insert into sertest values (date('3245-09-09'), "
                        + "time('09:30:25'), null)");

        assertStatementError("42821", st,
                " insert into sertest values (null, null, time('09:30:25'))");

        assertStatementError("42821", st,
                " insert into sertest values (null, null, "
                        + "timestamp('1745-01-01 09:30:25'))");

        assertUpdateCount(st, 2, "update sertest set d=o");

        rs = st.executeQuery(" select * from sertest where s is null " +
        		"and o is not null");
        JDBC.assertColumnNames(rs, new String[] { "D", "S", "O" });
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery("select month(s) from sertest " +
        		"where s is not null");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1" }, { "6" } },
                true);

        rs = st.executeQuery(" select day(o) from sertest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "3" }, { "5" } }, 
                true);

        dropTable("sertest");
    }
    
    public void testConvertFromString() throws SQLException{        
        Statement st = createStatement();
        
        st.executeUpdate("create table convstrtest(d varchar(30), t char(30), "
                + "ts long varchar)");
        st.executeUpdate(" insert into convstrtest values('1932-03-21',  "
                + "'23:49:52', '1832-09-24 10:11:43.32')");
        st.executeUpdate(" insert into convstrtest values(null, null, null)");

        assertStatementError("22007", st,
                "select CAST (d AS time) from convstrtest");

        assertStatementError("22007", st,
                " select CAST (t AS date) from convstrtest");

        assertStatementError("42846", st,
                " select CAST (ts AS time) from convstrtest");

        assertStatementError("42846", st,
                " select CAST (ts AS date) from convstrtest");


        ResultSet rs = st.executeQuery("select CAST (t AS time) from convstrtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "23:49:52" }, 
                { null } }, true);

        rs = st.executeQuery(" select CAST (d AS date) from convstrtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1932-03-21" }, { null } }, true);

        // show time and date separately as timestamp will be 
        // filtered out
        assertStatementError("42846", st, "select " +
                "CAST(CAST (ts AS timestamp) AS date), " +
                "CAST(CAST (ts AS timestamp) AS time) from convstrtest");

        dropTable("convstrtest");
        
        st.close();
    }

    /**
     * Regression test case for DERBY-4621, which caused the conversion of
     * timestamp and time values to varchar to generate wrong results when
     * a Calendar object was supplied.
     */
    public void testConversionToString() throws SQLException {
        String timestampString = "2010-04-20 15:17:36.0";
        String timeString = "15:17:36";
        String dateString = "2010-04-20";

        Timestamp ts = Timestamp.valueOf(timestampString);
        Time t = Time.valueOf(timeString);
        Date d = Date.valueOf(dateString);

        PreparedStatement ps =
                prepareStatement("VALUES CAST(? AS VARCHAR(40))");

        ps.setTimestamp(1, ts);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), timestampString);

        // Used to give wrong result - 2010-04-20 03:17:36
        ps.setTimestamp(1, ts, Calendar.getInstance());
        JDBC.assertSingleValueResultSet(ps.executeQuery(), timestampString);

        ps.setTime(1, t);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), timeString);

        // Used to give wrong result - 03:17:36
        ps.setTime(1, t, Calendar.getInstance());
        JDBC.assertSingleValueResultSet(ps.executeQuery(), timeString);

        ps.setDate(1, d);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), dateString);

        ps.setDate(1, d, Calendar.getInstance());
        JDBC.assertSingleValueResultSet(ps.executeQuery(), dateString);
    }
    
    public void testConversion_Aggregates() throws SQLException{
        Statement st = createStatement();
        
        //test aggregates sum should fail
        assertStatementError("42Y22", st, "select sum(d) from convtest");

        assertStatementError("42Y22", st, " select sum(t) from convtest");

        assertStatementError("42Y22", st, " select sum(ts) from convtest");

        // these should work
        ResultSet rs = st.executeQuery("select count(d) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "2" }, }, true);

        rs = st.executeQuery(" select count(t) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "2" }, }, true);

        rs = st.executeQuery(" select count(ts) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "2" }, }, true);

        st.executeUpdate(" insert into convtest values(date('0001-03-21'),  "
                + "time('5:22:59'), timestamp('9999-12-31 23:59:59.999999'))");

        // distinct count should be 2 not 3
        rs = st.executeQuery("select count(distinct d) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "2" }, }, true);

        rs = st.executeQuery(" select count(distinct t) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "2" }, }, true);

        rs = st.executeQuery(" select count(distinct ts) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "2" }, }, true);

        // min should not be null!!!!!!!!
        rs = st.executeQuery("select min(d) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "0001-03-21" }, }, 
                true);

        rs = st.executeQuery(" select min(t) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "05:22:59" }, }, true);

        // show time and date separately as timestamp will be 
        // filtered out
        rs = st.executeQuery("select " +
                "CAST(CAST (min(ts) AS timestamp) AS date), " +
                "CAST(CAST (min(ts) AS timestamp) AS time) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1", "2" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1832-09-24", 
            "10:11:43" }, }, true);

        rs = st.executeQuery(" select max(d) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1932-03-21" }, }, true);

        rs = st.executeQuery(" select max(t) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs,
                new String[][] { { "23:49:52" }, }, true);

        // show time and date separately as timestamp will be 
        // filtered out
        rs = st.executeQuery("select " +
                "CAST(CAST (max(ts) AS timestamp) AS date), " +
                "CAST(CAST (max(ts) AS timestamp) AS time) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1", "2" });
        JDBC.assertFullResultSet(rs,
                new String[][] { { "9999-12-31", "23:59:59" }, }, true);
        
        //just to recover the test environment
        dropTable("convtest");
        createTableForConversionTest(st);

        st.close();
    }
    
    public void testConversion() throws SQLException{        
        Statement st = createStatement();
        
        // these should fail
        assertStatementError("42846", st, 
                "select CAST (d AS time) from convtest");

        assertStatementError("42846", st,
                " select CAST (t AS date) from convtest");

        // these should work
        ResultSet rs = st.executeQuery(
                "select CAST (t AS time) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "23:49:52" }, 
                { "05:22:59" }, { null } }, true);

        rs = st.executeQuery(" select CAST (d AS date) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1932-03-21" },
                { "0001-03-21" }, { null } }, true);

        rs = st.executeQuery(" select CAST (ts AS time) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "10:11:43" }, 
                { "23:59:59" }, { null } }, true);

        rs = st.executeQuery(" select CAST (ts AS date) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1832-09-24" }, 
                { "9999-12-31" }, { null } }, true);

        // show time and date separately as timestamp will be 
        // filtered out
        rs = st.executeQuery("select CAST(CAST (ts AS timestamp) AS date), " +
        		"CAST(CAST (ts AS timestamp) AS time) from convtest");
        JDBC.assertColumnNames(rs, new String[] { "1", "2" });

        JDBC.assertFullResultSet(rs, new String[][] { { "1832-09-24", 
                "10:11:43" }, { "9999-12-31", "23:59:59" }, 
                { null, null } }, true);

        // casting from a time to a timestamp sets the date to 
        // current date
        assertStatementError("42846", st, "select 'pass', " +
        		"CAST (CAST(t AS timestamp) AS time) from convtest " +
        		"where CAST(CAST(t AS timestamp) AS date)=current_date");

        // time should be 0
        assertStatementError("42846", st, "select " +
        		"CAST (CAST (d AS timestamp) AS date), " +
        		"CAST(CAST(d AS timestamp) AS time) from convtest");
        
        st.close();
    }
    
    /**
     * leading zeros may be omitted from the month, 
     * day and part of the timestamp.
     */
    public void testISOFormat_OmitLeadingZero() throws SQLException{
        Statement st = createStatement();

        assertEquals(1, st.executeUpdate(
                "insert into ts values ('2003-03-05-17.05.43.111111', " +
                "'2003-03-05 17:05:43.111111')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-03-17.05.43.111111', '2003-3-03 17:05:43.111111')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-2-17.05.43.111111', '2003-3-2 17:05:43.111111')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-03-2-17.05.43.111111', '2003-03-2 17:05:43.111111')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-1-17.05.43.1', '2003-3-1 17:05:43.1')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-1-17.05.43.12', '2003-3-1 17:05:43.12')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-1-17.05.43.123', '2003-3-1 17:05:43.123')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-1-17.05.43.1234', '2003-3-1 17:05:43.1234')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-1-17.05.43.12345', '2003-3-1 17:05:43.12345')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-1-17.05.43.123456', '2003-3-1 17:05:43.123456')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-1-17.05.43', '2003-3-1 17:05:43')"));
        
        st.close();
    }    
    
    /**
     *Trailing blanks are allowed.
     */
    public void testISOFormat_TrailingBlanks() throws SQLException{
        Statement st = createStatement();
        
        assertEquals(1, st.executeUpdate("insert into ts values " +
        		"('2002-03-05-17.05.43.111111  ', " +
        		"'2002-03-05 17:05:43.111111   ')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2002-03-05-17.05.43.1   ', '2002-03-05 17:05:43.1   ')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2002-03-05-17.05.43    ', '2002-03-05 17:05:43    ')"));
        
        st.close();
    }
    
    /**
     *  UDB allows this by "appending a zero"; so, cloudscape follows.
     */
    public void testISOFormat_TrailingZero() throws SQLException{
        Statement st = createStatement();
        
        assertEquals(1, st.executeUpdate("insert into ts values " +
        		"('2003-3-1-17.05.43.', '2003-3-1 17:05:43')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('2003-3-1-17.05.43.0', '2003-3-1 17:05:43.0')"));

        assertEquals(1, st.executeUpdate(" insert into ts values " +
        		"('0003-03-05-17.05.43.111111'," +
        		" '0003-03-05 17:05:43.111111')"));
        
        ResultSet rs = st.executeQuery(" select count(*) from ts " +
        		"where ts1=ts2");
        rs.next();
        int rows = rs.getInt(1);
        rs = st.executeQuery(" select count(*) from ts ");
        rs.next();
        assertEquals(rows, rs.getInt(1));
        
        st.executeUpdate(" delete from ts");
        
        st.close();
    }
    
    /**
     * Should be rejected because leading zero in year is missing.
     */
    public void testISOFormat_LeadingZero() throws SQLException{
        Statement st = createStatement();
                
        assertStatementError("22007", st,
                "insert into ts (ts1) values ('03-03-05-17.05.43.111111')");

        assertStatementError("22007", st,
                " insert into ts (ts1) values ('103-03-05-17.05.43.111111')");

        assertStatementError("22007", st,
                " insert into ts (ts1) values ('3-03-05-17.05.43.111111')");
        
        st.close();
    }
    
    /**
     * not valid Time format in the timestamp strings:  cloudscape rejects
     */
    public void testISOFormat_WrongTimestampFormat() throws SQLException{
        Statement st = createStatement();
        
        assertStatementError("22007", st,
                "insert into ts (ts1) values ('2003-3-24-13.1.02.566999')");

        assertStatementError("22007", st,
                " insert into ts (ts1) values ('2003-3-24-13.1.1.569')");

        assertStatementError("22007", st,
                " insert into ts (ts1) values ('2003-3-24-1.1.1.56')");

        assertStatementError("22007", st,
                " insert into ts (ts1) values ('2003-3-24-1.1.1')");

        assertStatementError("22007", st,
                " insert into ts (ts1) values ('2003-3-1-17.05.4.')");

        assertEquals(1, st.executeUpdate(" insert into ts (ts1) values " +
        		"('2003-03-05-7.05.43.111111')"));

        // invalid ISO format: cloudscape rejects

        assertStatementError("22007", st,
                "insert into ts (ts1) values ('2003-3-1 17.05.43.123456')");
        
        st.close();
    }
    
    /**
     *  Don't allow more than microseconds in ISO format: cloudscape rejects.
     */
    public void testISOFormat_MoreThanMicroseconds() throws SQLException{
        Statement st = createStatement();
        
        assertStatementError("22007", st, "insert into ts (ts1) values "
                + "('2003-03-05-17.05.43.999999999')");

        assertStatementError("22007", st, " insert into ts (ts1) values "
                + "('2003-03-05-17.05.43.999999000')");

        st.close();
    }
    
    /**
     * Test the timestamp( d, t) function.
     */
    public void testTimeStampFunction() throws SQLException{
        Statement st = createStatement();
        
        st .executeUpdate("create table tt " +
        		"(datecol date, dateStr varchar(16), timecol time, " +
        		"timeStr varchar(16), expected timestamp)");

        st.executeUpdate(" insert into tt ( dateStr, timeStr) " +
        		"values( '2004-03-04', '12:01:02')");

        st.executeUpdate(" insert into tt ( dateStr, timeStr) " +
        		"values( null, '12:01:03')");

        st.executeUpdate(" insert into tt ( dateStr, timeStr) " +
        		"values( '2004-03-05', null)");

        assertUpdateCount(st, 3, " update tt  set datecol = date( dateStr), " +
        		"timecol = time( timeStr)");

        assertUpdateCount(st, 1, " update tt  set expected = " +
        		"timestamp( dateStr || ' ' || timeStr) " +
        		"where dateStr is not null and timeStr is not null");

        ResultSet rs = st.executeQuery(" select dateStr, timeStr from tt " +
        		"where (expected is not null and (expected <> " +
        		"timestamp( dateCol, timeCol) or " +
        		"timestamp( dateCol, timeCol) is null)) or " +
        		"(expected is null and " +
        		"timestamp( dateCol, timeCol) is not null)");
        JDBC.assertColumnNames(rs, new String[] { "DATESTR", "TIMESTR" });
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(" select dateStr, timeStr from tt " +
        		"where (expected is not null and (expected <> " +
        		"timestamp( dateStr, timeStr) or " +
        		"timestamp( dateStr, timeStr) is null)) " +
        		"or (expected is null and " +
        		"timestamp( dateStr, timeStr) is not null)");

        JDBC.assertColumnNames(rs, new String[] { "DATESTR", "TIMESTR" });
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(" select dateStr, timeStr from tt " +
        		"where (expected is not null and " +
        		"timestamp( dateStr, timeStr) <> timestamp( dateCol, timeCol))" +
        		" or (expected is null and " +
        		"timestamp( dateStr, timeStr) is not null)");

        JDBC.assertColumnNames(rs, new String[] { "DATESTR", "TIMESTR" });
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(" select dateStr, timeStr from tt " +
        		"where expected is not null and " +
        		"date( timestamp( dateCol, timeCol)) <> dateCol");
        JDBC.assertColumnNames(rs, new String[] { "DATESTR", "TIMESTR" });
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(" select dateStr, timeStr from tt " +
        		"where expected is not null and " +
        		"time( timestamp( dateCol, timeCol)) <> timeCol");
        JDBC.assertColumnNames(rs, new String[] { "DATESTR", "TIMESTR" });
        JDBC.assertDrainResults(rs, 0);

        // Error cases
        assertStatementError("42Y95", st,
                "select timestamp( dateCol, dateCol) from tt where "
                        + "dateCol is not null");

        assertStatementError("42Y95", st,
                " select timestamp( timeCol, timeCol) from tt where "
                        + "timeCol is not null");

        assertStatementError("22007", st,
                "values timestamp( 'xyz', '12:01:02')");

        assertStatementError("22007", st,
                " values timestamp( '2004-03-04', 'xyz')");

        dropTable("tt");
        
        st.close();
    }
    
    public void testFormat() throws SQLException{
        Statement st = createStatement();
        
        st.executeUpdate(" create table t_format (t time)");

        // ISO format: UDB is okay.
        assertEquals(1, 
                st.executeUpdate("insert into t_format values ('17.05.44')"));

        assertEquals(1, 
                st.executeUpdate(" insert into t_format values ('17.05.00')"));

        assertEquals(1, 
                st.executeUpdate(" insert into t_format values ('00.05.43')"));

        assertEquals(1, 
                st.executeUpdate(" insert into t_format values ('00.00.00')"));

        // DB2 keeps '24:00:00' but Cloudcape returns '00:00:00'
        assertEquals(1, 
                st.executeUpdate("insert into t_format values ('24.00.00')"));

        // trailing blanks are allowed
        assertEquals(1, 
                st.executeUpdate("insert into t_format values ('17.05.11  ')"));

        assertEquals(1, 
                st.executeUpdate(" insert into t_format values ('17:05:11  ')"));

        // seconds can be omitted
        assertEquals(1, 
                st.executeUpdate("insert into t_format values ('1:01')"));

        assertEquals(1, 
                st.executeUpdate(" insert into t_format values ('1:02 ')"));

        assertEquals(1, 
                st.executeUpdate(" insert into t_format values ('2.01')"));

        assertEquals(1, 
                st.executeUpdate(" insert into t_format values ('2.02 ')"));

        // 11 rows
        ResultSet rs = st.executeQuery("select * from t_format");
        JDBC.assertColumnNames(rs, new String[] { "T" });

        JDBC.assertFullResultSet(rs, 
                new String[][] { { "17:05:44" }, { "17:05:00" },
                { "00:05:43" }, { "00:00:00" }, { "00:00:00" }, { "17:05:11" },
                { "17:05:11" }, { "01:01:00" }, { "01:02:00" }, { "02:01:00" },
                { "02:02:00" } }, true);
        assertUpdateCount(st, 11, " delete from t_format");

        // end value tests...
        assertStatementError("22007", st, "insert into t_format values ('24.60.60')");

        assertStatementError("22007", st, " insert into t_format values ('04.00.60')");

        assertStatementError("22007", st, " insert into t_format values ('03.60.00')");

        // not valid Time string ISO format: HH.MM.SS
        assertStatementError("22007", st, 
                "insert into t_format values ('07.5.44')");

        assertStatementError("22007", st, 
                " insert into t_format values ('07.05.4')");

        assertStatementError("22007", st, 
                " insert into t_format values ('7.5.44')");

        assertStatementError("22007", st, 
                " insert into t_format values ('7.5.4')");

        assertStatementError("22007", st, 
                " insert into t_format values ('7.5.0')");

        assertStatementError("22007", st, 
                " insert into t_format values ('-4.00.00')");

        assertStatementError("22007", st, 
                " insert into t_format values ('A4.00.00')");

        assertStatementError("22007", st, 
                " insert into t_format values ('7.5.999')");

        assertStatementError("22007", st, 
                " insert into t_format values ('07.05.111')");

        assertStatementError("22007", st, 
                " insert into t_format values ('111.05.11')");

        assertStatementError("22007", st, 
                " insert into t_format values ('11.115.00')");

        // no row
        rs = st.executeQuery("select * from t_format");
        JDBC.assertColumnNames(rs, new String[] { "T" });
        JDBC.assertDrainResults(rs, 0);

        dropTable("t_format");
        
        st.close();
    }

    public void testFormat_Additional() throws SQLException{
        Statement st = createStatement();
        
        ResultSet rs = st .executeQuery(
        		"values time('2004-04-15 16:15:32.387')");
        JDBC.assertFullResultSet(rs, new String[][] { { "16:15:32" } }, true);

        rs = st.executeQuery(" values time('2004-04-15-16.15.32.387')");
        JDBC.assertFullResultSet(rs, new String[][] { { "16:15:32" } }, true);

        assertStatementError("22007", st,
                " values time('2004-04-15-16.15.32.387 zz')");

        assertStatementError("22007", st,
                " values time('x-04-15-16.15.32.387')");

        rs = st.executeQuery(" values date('2004-04-15 16:15:32.387')");
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "2004-04-15" } }, true);

        rs = st.executeQuery(" values date('2004-04-15-16.15.32.387')");
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "2004-04-15" } }, true);

        assertStatementError("22008", st,
                " values date('2004-04-15-16.15.32.387 zz')");

        assertStatementError("22008", st,
                " values date('2004-04-15-16.15.32.y')");

        rs = st.executeQuery(" values time('13:59')");
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "13:59:00" } }, true);

        rs = st.executeQuery(" values time('1:00')");
        JDBC.assertFullResultSet(rs, new String[][] { { "01:00:00" } }, true);
        
        st.close();
    }
    
    /**
     * Test unary date and datetime functions. 
     * Test with both constant and variable arguments.
     */
    public void test_DateAndDatetimeFunctionsMore() throws Exception {
        ResultSet rs = null;
        ResultSetMetaData rsmd;
        PreparedStatement pSt;
        Statement st = createStatement();

        String[][] expRS;
        String[] expColNames;      

        setAutoCommit(false);
        // test date(integer)
        st.executeUpdate("create table t_func( i int, d date)");

        commit();
        st.executeUpdate(" insert into t_func values( 1, date(1)),(10, " + 
                "date(10.1)),(365,date(365.1e0)),(366,date(366)),(789" + 
                ",date(789)),(790,date(790)),(791,date(791))");

        // should fail
        assertStatementError("22008", st, "insert into t_func values( 0, date(0))");

        assertStatementError("22008", st,
                " insert into t_func values( -1, date(-1))");

        assertStatementError("22008", st,
                " insert into t_func values( 3652060, date( 3652060))");

        rs = st.executeQuery(" select i,d,date(i),date(d) from t_func order by i");
        JDBC.assertColumnNames(rs, new String[] { "I", "D", "3", "4" });
        JDBC.assertFullResultSet(rs, new String[][] {
                { "1", "1970-01-01", "1970-01-01", "1970-01-01" },
                { "10", "1970-01-10", "1970-01-10", "1970-01-10" },
                { "365", "1970-12-31", "1970-12-31", "1970-12-31" },
                { "366", "1971-01-01", "1971-01-01", "1971-01-01" },
                { "789", "1972-02-28", "1972-02-28", "1972-02-28" },
                { "790", "1972-02-29", "1972-02-29", "1972-02-29" },
                { "791", "1972-03-01", "1972-03-01", "1972-03-01" } }, true);

        rollback();

        assertEquals(1, st.executeUpdate(" insert into t_func(i) values( 0)"));
        
        assertStatementError("22008", st, " select date(i) from t_func");

        rollback();

        st.executeUpdate(" insert into t_func(i) values( -1)");

        assertStatementError("22008", st, " select date(i) from t_func");
        rollback();

        st.executeUpdate(" insert into t_func(i) values( 3652060)");
        assertStatementError("22008", st, " select date(i) from t_func");
        rollback();

        st.executeUpdate(" drop table t_func");
        
        st.executeUpdate(" create table t_func( s varchar(32), d date)");

        commit();
        assertEquals(6, st.executeUpdate(" insert into t_func " +
        		"values('1900060', date('1900060')), ('1904060', " +
        		"date('1904060')), ('1904366', date('1904366')), " +
        		"('2000060', date('2000060')), ('2001060'," +
        		" date('2001060')), ('2001365', date('2001365'))"));

        rs = st.executeQuery(" select s,d,date(s) from t_func order by s");
        JDBC.assertColumnNames(rs, new String[] { "S", "D", "3" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1900060", "1900-03-01", "1900-03-01" },
                { "1904060", "1904-02-29", "1904-02-29" },
                { "1904366", "1904-12-31", "1904-12-31" },
                { "2000060", "2000-02-29", "2000-02-29" },
                { "2001060", "2001-03-01", "2001-03-01" },
                { "2001365", "2001-12-31", "2001-12-31" } }, true);

        rollback();

        // failure cases
        assertStatementError("22008", st, "values( date('2001000'))");

        assertStatementError("22008", st, " values( date('2001366'))");

        assertStatementError("22008", st, " values( date('2000367'))");

        assertStatementError("22008", st, " values( date('xxxxxxx'))");

        st.executeUpdate(" insert into t_func(s) values( '2001000')");

        assertStatementError("22008", st, "select date(s) from t_func");
        rollback();

        st.executeUpdate(" insert into t_func(s) values( '2001366')");

        assertStatementError("22008", st, "select date(s) from t_func");
        rollback();

        st.executeUpdate(" insert into t_func(s) values( '2000367')");

        assertStatementError("22008", st, "select date(s) from t_func");
        rollback();

        st.executeUpdate(" insert into t_func(s) values( 'xxxxxxx')");

        assertStatementError("22008", st, "select date(s) from t_func");
        rollback();

        // test parameter
        pSt = prepareStatement("values( date(cast(? as integer))," +
        		"timestamp(cast(? as varchar(32))))");
        rs = st.executeQuery("values(cast(1 as integer), " +
        		"'2003-03-05-17.05.43.111111')");

        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));
        
        rs = pSt.executeQuery();
        JDBC.assertColumnNames(rs, new String[] { "1", "2" });
        // See DERBY-3856 - there's a diff between Embedded and DerbyNetClient
        // in how the cast returns the following...
        if (usingDerbyNetClient())
            JDBC.assertFullResultSet(rs, new String[][] { { "1970-01-01",
            "2003-03-05 17:05:43.111111" } }, true);
        else
            JDBC.assertFullResultSet(rs, new String[][] { { "1970-01-01",
                "2003-03-05-17.05.43.111111" } }, true);

        rs = st.executeQuery("values(2, '20030422190200')");
        rs.next();
        rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++)
            pSt.setObject(i, rs.getObject(i));

        rs = pSt.executeQuery();
        JDBC.assertColumnNames(rs, new String[] { "1", "2" });
        JDBC.assertFullResultSet(rs, new String[][] { { "1970-01-02", 
                "2003-04-22 19:02:00.0" } }, true);

        rs = st.executeQuery(" values( date(date(1)), "
                + "date(timestamp('2003-03-05-17.05.43.111111')))");
        JDBC.assertColumnNames(rs, new String[] { "1", "2" });
        JDBC.assertFullResultSet(rs, 
                new String[][] { { "1970-01-01", "2003-03-05" } }, true);

        st.executeUpdate(" drop table t_func");
        
        
        st.executeUpdate(" create table t_func( s varchar(32), ts timestamp, " + 
                "expected timestamp)");

        commit();
        st.executeUpdate(" insert into t_func(ts) values( " +
                "timestamp('2003-03-05-17.05.43.111111'))");

        rs = st.executeQuery(" select date(ts) from t_func");

        expColNames = new String[] { "1" };
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String[][] { { "2003-03-05" } };

        JDBC.assertFullResultSet(rs, expRS, true);

        rollback();

        // Test special unary timestamp function rules: yyyyxxddhhmmss
        st.executeUpdate("insert into t_func values('20000228235959', " + 
                "timestamp('20000228235959'), '2000-02-28-23.59.59'), " +
                "('20000229000000', timestamp('20000229000000'), " +
                "'2000-02-29-00.00.00')");

        rs = st.executeQuery(" select s from t_func where ts <> expected or " + 
                "timestamp(s) <> expected or timestamp(ts) <> expected");
        JDBC.assertColumnNames(rs, new String[] { "S" });
        JDBC.assertDrainResults(rs, 0);

        rollback();

        // invalid
        assertStatementError("22008", st,
                "values( timestamp('2000 1 1 0 0 0'))");

        assertStatementError("22008", st,
                " values( timestamp('aaaaaaaaaaaaaa'))");

        assertEquals(1, st.executeUpdate(
                " insert into t_func(s) values('2000 1 1 0 0 0')"));
        
        assertStatementError("22008", st, " select timestamp(s) from t_func");

        rollback();
        assertEquals(1, st.executeUpdate(
                " insert into t_func(s) values('aaaaaaaaaaaaaa')"));
        assertStatementError("22008", st, " select timestamp(s) from t_func");
        rollback();

        commit();

        getConnection().rollback();
        st.close();
    }
    
    /**
     *  Null values in datetime scalar functions.
     */
    public void testNulls() throws SQLException{
        Statement st = createStatement();
        
        st.executeUpdate("create table nulls (t time, d date, ts timestamp)");

        st.executeUpdate(" insert into nulls values (null,null,null)");

        commit();
        assertStatementError("42X25", st, " select year(t) from nulls");

        assertStatementError("42X25", st, " select month(t) from nulls");

        assertStatementError("42X25", st, " select day(t) from nulls");

        ResultSet rs = st.executeQuery(" select hour(t) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select minute(t) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select second(t) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select year(d) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select month(d) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select day(d) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        assertStatementError("42X25", st, " select hour(d) from nulls");

        assertStatementError("42X25", st, " select minute(d) from nulls");

        assertStatementError("42X25", st, " select second(d) from nulls");

        rs = st.executeQuery(" select year(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select month(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select day(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select hour(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select minute(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        rs = st.executeQuery(" select second(ts) from nulls");
        JDBC.assertFullResultSet(rs, new String[][] { { null } }, true);

        st.executeUpdate(" drop table nulls");
        
        st.close();
    }
}
