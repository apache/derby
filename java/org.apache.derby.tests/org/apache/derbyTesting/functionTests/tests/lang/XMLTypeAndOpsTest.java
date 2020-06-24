/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.XMLTypeAndOpsTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.XML;

/**
 * XMLTypeAndOpsTest this test is the JUnit equivalent to what used
 * to be the "lang/xml_general.sql" test, which was canon-based.
 * Since the .sql test had different masters for embedded, JCC, and 
 * Derby Client, and since it performed some "sed'ing" to ensure
 * consistent results across JVMs, it was not sufficient to just
 * wrap the test in a JUnit ScriptTestCase (because ScriptTestCase
 * doesn't deal with multiple masters nor with sed'ing).  Hence the
 * creation of this pure JUnit version of the test.
 */
public final class XMLTypeAndOpsTest extends BaseJDBCTestCase {
    
    /* For the test methods in this class, "expRS" refers to a
     * two-dimensional array representing an expected result set when
     * executing queries.  The "rows" and "columns" in this array
     * are compard with those from a SQL ResultSet.  Note that all
     * values are represented as Strings here; we don't actually
     * check the *types* of the columns; just their values.  This
     * is because this test was created from a .sql test, where
     * results are similarly treated (i.e. in an ij test with a
     * .sql file, the test passes if the values "look the same";
     * there's no checking of specific value types for most query
     * results.  So we do the same for this JUnit test).
     */

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public XMLTypeAndOpsTest(String name)
    {
        super(name);
    }

    /**
     * Return a suite that runs a set of tests which are meant to
     * be the equivalent to the test cases in the old xml_general.sql.
     * But only return such a suite IF the testing classpath has the
     * required XML classes.  Otherwise just return an empty suite.
     */
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("XML Type and Operators Suite\n");

//IC see: https://issues.apache.org/jira/browse/DERBY-1758
        if (!XML.classpathMeetsXMLReqs())
            return suite;

        /* "false" in the next line means that we will *not* clean the
         * database before the embedded and client suites.  This ensures
         * that we do not remove the objects created by XMLTestSetup.
         */
//IC see: https://issues.apache.org/jira/browse/DERBY-1758
        suite.addTest(
            TestConfiguration.defaultSuite(XMLTypeAndOpsTest.class, false));

        return (new XMLTestSetup(suite));
    }

    /**
     * Test creation of XML columns.
     */
    public void testXMLColCreation() throws Exception
    {
        // If the column's definition doesn't make sense for XML,
        // then we should throw the correct error.

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42894",
            "create table fail1 (i int, x xml default 'oops')");

        assertCompileError("42894",
            "create table fail2 (i int, x xml default 8)");

        assertCompileError("42818",
            "create table fail3 (i int, x xml check (x != 0))");

        // These should all work.

        Statement st = createStatement();
        st.executeUpdate("create table tc1 (i int, x xml)");
        st.executeUpdate("create table tc2 (i int, x xml not null)");
        st.executeUpdate("create table tc3 (i int, x xml default null)");
        st.executeUpdate("create table tc4 (x2 xml not null)");
        st.executeUpdate("alter table tc4 add column x1 xml");

        // Cleanup.

        st.executeUpdate("drop table tc1");
        st.executeUpdate("drop table tc2");
        st.executeUpdate("drop table tc3");
        st.executeUpdate("drop table tc4");
        st.close();
    }

    /**
     * Check insertion of null values into XML columns.  This
     * test just checks the negative cases--i.e. cases where
     * we expect the insertions to fail.  The positive cases
     * are tested implicitly as part of XMLTestSetup.setUp()
     * when we load the test data.
     */
    public void testIllegalNullInserts() throws Exception
    {
        // These should fail because target column is declared
        // as non-null.

        Statement st = createStatement();
        st.executeUpdate("create table tc2 (i int, x xml not null)");
        assertStatementError("23502", st, "insert into tc2 values (1, null)");
        assertStatementError("23502", st,
            "insert into tc2 values (2, cast (null as xml))");
        st.executeUpdate("drop table tc2");
        st.close();
    }

    /**
     * Test insertion of non-XML values into XML columns.  These
     * should all fail because such an operation is not allowed.
     */
    public void testXMLColsWithNonXMLVals() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42821", "insert into t1 values (3, 'hmm')");
        assertCompileError("42821", "insert into t1 values (1, 2)");
        assertCompileError("42821", "insert into t1 values (1, 123.456)");

        assertCompileError("42821", "insert into t1 values (1, x'01')");
        assertCompileError("42821", "insert into t1 values (1, x'ab')");
        assertCompileError("42821", "insert into t1 values (1, current date)");

        assertCompileError("42821", "insert into t1 values (1, current time)");

        assertCompileError("42821",
            " insert into t1 values (1, current timestamp)");

        assertCompileError("42821",
            " insert into t1 values (1, ('hmm' || 'andstuff'))");
    }

    /**
     * Test insertion of XML values into non-XML columns.  These
     * should all fail because such an operation is not allowed.
     */
    public void testNonXMLColsWithXMLVals() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate(
            "create table nonXTable (si smallint, i int, bi bigint, vcb "
            + "varchar (32) for bit data, nu numeric(10,2), f "
            + "float, d double, vc varchar(20), da date, ti time, "
            + "ts timestamp, cl clob, bl blob)");

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42821",
            "insert into nonXTable (si) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (i) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (bi) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (vcb) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (nu) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (f) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (d) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (vc) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (da) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (ti) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (ts) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (cl) values (cast (null as xml))");
        
        assertCompileError("42821",
            "insert into nonXTable (bl) values (cast (null as xml))");

        // And just to be safe, try to insert a non-null XML
        // value.  This should fail, too.

        assertCompileError("42821",
            "insert into nonXTable (cl) values (xmlparse(document " +
            "'</simp>' preserve whitespace))");

        st.executeUpdate("drop table nonXTable");
        st.close();
    }

    /**
     * Test casting of values to type XML.  These should all
     * fail because such casting is not allowed.
     */
    public void testXMLCasting() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42846",
            "insert into t1 values (1, cast ('hmm' as xml))");
        
        assertCompileError("42846",
            "insert into t1 values (1, cast (2 as xml))");
        
        assertCompileError("42846",
            "insert into t1 values (1, cast (123.456 as xml))");
        
        assertCompileError("42846",
            "insert into t1 values (1, cast (x'01' as xml))");
        
        assertCompileError("42846",
            "insert into t1 values (1, cast (x'ab' as xml))");
        
        assertCompileError("42846",
            "insert into t1 values (1, cast (current date as xml))");
        
        assertCompileError("42846",
            "insert into t1 values (1, cast (current time as xml))");
        
        assertCompileError("42846",
            "insert into t1 values (1, cast (current timestamp as xml))");
        
        assertCompileError("42846",
            "insert into t1 values (1, cast (('hmm' || "
            + "'andstuff') as xml))");

        // And try to cast an XML value into something else.
        // These should fail, too.

        Statement st = createStatement();
        st.executeUpdate("create table nonXTable (i int, cl clob)");

        assertCompileError("42846",
            "insert into nonXTable (cl) values (cast ((xmlparse(document " +
            "'</simp>' preserve whitespace)) as clob))");

        assertCompileError("42846",
            "insert into nonXTable (i) values (cast ((xmlparse(document " +
            "'</simp>' preserve whitespace)) as int))");

        st.executeUpdate("drop table nonXTable");
        st.close();
    }

    /**
     * Try to use XML values in non-XML operations.  These
     * should all fail (the only operations allowed with XML
     * are the specified XML operations (xmlparse, xmlserialize,
     * xmlexists, xmlquery)).
     */
    public void testXMLInNonXMLOps() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42Y95", "select i + x from t1");
        assertCompileError("42Y95", "select i * x from t1");
        assertCompileError("42Y95", "select i / x from t1");
        assertCompileError("42Y95", "select i - x from t1");
        assertCompileError("42X37", "select -x from t1");
        assertCompileError("42846", "select 'hi' || x from t1");
        assertCompileError("42X25", "select substr(x, 0) from t1");
        assertCompileError("42Y22", "select max(x) from t1");
        assertCompileError("42Y22", "select min(x) from t1");
        assertCompileError("42X25", "select length(x) from t1");
        assertCompileError("42884", "select i from t1 where x like 'hmm'");
    }

    /**
     * Test simple comparisons with XML.  These should all fail
     * because no such comparisons are allowed.
     */
    public void testXMLComparisons() throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42818", "select i from t1 where x = 'hmm'");
        assertCompileError("42818", "select i from t1 where x > 0");
        assertCompileError("42818", "select i from t1 where x < x");
        assertCompileError("42818", "select i from t1 where x <> 'some char'");
    }

    /**
     * Test additional restrictions on use of XML values.
     * These should all fail.
     */
    public void testIllegalOps() throws Exception
    {
        // Indexing/ordering on XML cols is not allowed.

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("X0X67", "create index oops_ix on t1(x)");
        assertCompileError("X0X67",
            "select i from t1 where x is null order by x");
        
        // XML cannot be imported or exported (DERBY-1892).

        CallableStatement cSt = prepareCall(
            "CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE ("
            + "  null, 'T1', 'extinout/xmlexport.del', null, null, null)");

//IC see: https://issues.apache.org/jira/browse/DERBY-1440
//IC see: https://issues.apache.org/jira/browse/DERBY-2472
        assertStatementError("42Z71", cSt);
        
        cSt = prepareCall(
            " CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY("
            + "  'select x from t1', 'extinout/xmlexport.del', null, null, null)");
        assertStatementError("42Z71", cSt);

        cSt = prepareCall(
            " CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE ("
            + "  null, 'T1', 'extinout/shouldntmatter.del', null, null, null, 0)");
        assertStatementError("XIE0B", cSt);
        
        cSt = prepareCall(
            " CALL SYSCS_UTIL.SYSCS_IMPORT_DATA ("
            + "  NULL, 'T1', null, '2', 'extinout/shouldntmatter.del', "
            + "null, null, null,0)");
        assertStatementError("XIE0B", cSt);

        // Done with cSt.
        cSt.close();
        
        // XML cannot be used with procedures/functions.

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42962",
            "create procedure hmmproc (in i int, in x xml)"
            + "  parameter style java language java external name "
            + "'hi.there'");
        
        assertCompileError("42962",
            " create function hmmfunc (i int, x xml) returns int"
            + "  parameter style java language java external name "
            + "'hi.there'");
        
        // XML columns cannot be used for global temporary 
        // tables.

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42962",
            "declare global temporary table SESSION.xglobal (myx XML)"
            + "  not logged on commit preserve rows");
    }

    /**
     * Test use of XML columns in a trigger's "SET" clause.  Should
     * work so long as the target value has type XML.
     */
    public void testTriggerSetXML() throws Exception
    {
        // This should fail.
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42821",
            "create trigger tr2 after insert on t1 for each row "
            + "mode db2sql update t1 set x = 'hmm'");

        // This should succeed.
        Statement st = createStatement();
        st.executeUpdate("create trigger tr1 after insert on t1 for each row "
            + "mode db2sql update t1 set x = null");

        st.executeUpdate(" drop trigger tr1");
        st.close();
    }

    /**
     * Various tests for the XMLPARSE operator.  Note that this
     * test primarily checks the negative cases--i.e. cases where
     * we expect the XMLPARSE op to fail.  The positive cases
     * were tested implicitly as part of XMLTestSetup.setUp()
     * when we loaded the test data.
     */
    public void testXMLParse() throws Exception
    {
        // These should fail with various parse errors.

        Statement st = createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42Z74",
            "insert into t1 values (1, xmlparse(document "
            + "'<hmm/>' strip whitespace))");
        
        assertCompileError("42Z72",
            " insert into t1 values (1, xmlparse(document '<hmm/>'))");
        
        assertCompileError("42Z72",
            " insert into t1 values (1, xmlparse('<hmm/>' "
            + "preserve whitespace))");
        
        assertCompileError("42Z74",
            " insert into t1 values (1, xmlparse(content "
            + "'<hmm/>' preserve whitespace))");
        
        assertCompileError("42X25",
            " select xmlparse(document xmlparse(document "
            + "'<hein/>' preserve whitespace) preserve whitespace) from t1");
        
        assertCompileError("42X19",
            " select i from t1 where xmlparse(document '<hein/>' "
            + "preserve whitespace)");

        // This should fail because operand does not constitute
        // well-formed XML.
        
        assertStatementError("2200M", st,
            " insert into t1 values (1, xmlparse(document "
            + "'<oops>' preserve whitespace))");

        // This should fail because use of a parameter for the operand
        // requires an explicit CAST to a char type.

        assertCompileError("42Z79",
            "insert into t1(x) values XMLPARSE(document ? "
            + "preserve whitespace)");
        
        // Creation of a table with a default as XMLPARSE should throw
        // an error--use of functions as a default is not allowed
        // by the Derby syntax.

        assertCompileError("42894",
            "create table fail1 (i int, x xml default xmlparse("
            + "document '<my>default col</my>' preserve whitespace))");

        // XMLPARSE is valid operand for "is [not] null" so
        // this should work (and we should see a row for every
        // successful "insert" statement that we executed on T1).

        ResultSet rs = st.executeQuery(
            " select i from t1 where xmlparse(document '<hein/>' "
            + "preserve whitespace) is not null");

        String [] expColNames = new String [] { "I" };
        JDBC.assertColumnNames(rs, expColNames);
        
        String [][] expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"4"},
            {"3"},
            {"5"},
            {"6"},
            {"7"},
            {"8"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Ensure that order by on a table with an XML column in it
        // works correctly.
        
        rs = st.executeQuery(
            " select i from t1 where xmlparse(document '<hein/>' "
            + "preserve whitespace) is not null order by i");

        expColNames = new String [] { "I" };
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"5"},
            {"6"},
            {"7"},
            {"8"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Insertions using XMLPARSE with a parameter that is cast
        // to a character type should work.

        st.execute("create table paramInsert(x xml)");
        PreparedStatement pSt = prepareStatement(
            "insert into paramInsert values XMLPARSE(document "
            + "cast (? as CLOB) preserve whitespace)");
        
        pSt.setString(1, "<ay>caramba</ay>");
        assertUpdateCount(pSt, 1);
        pSt.close();

        // Run a select to view everything that was inserted.

        rs = st.executeQuery("select xmlserialize(x as clob) from t1");

        expColNames = new String [] { "1" };
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"<update2> document was inserted as part of an "
                + "UPDATE </update2>"},
            {null},
            {null},
            {null},
            {"<hmm/>"},
            {"<half> <masted> bass </masted> boosted. </half>"},
            {"<umm> decl check </umm>"},
            {"<lets> <try> this out </try> </lets>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        rs = st.executeQuery(
            "select xmlserialize(x as clob) from paramInsert");

        expColNames = new String [] { "1" };
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"<ay>caramba</ay>"}
        };

        // Cleanup.

        st.executeUpdate("drop table paramInsert");
        st.close();
    }

    /**
     * Test use of the "is [not] null" clause with XML values.
     * These should work.
     */
    public void testIsNull() throws Exception
    {
        Statement st = createStatement();
        ResultSet rs = st.executeQuery(
            "select i from t1 where x is not null");
        
        String [] expColNames = new String [] { "I" };
        JDBC.assertColumnNames(rs, expColNames);
        
        String [][] expRS = new String [][]
        {
            {"1"},
            {"5"},
            {"6"},
            {"7"},
            {"8"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(" select i from t1 where x is null");

        expColNames = new String [] { "I" };
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2"},
            {"4"},
            {"3"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        st.close();
    }

    /**
     * Derby doesn't currently support XML values in a top-level
     * result set.  So make sure that doesn't work.  These should
     * all fail.
     */
    public void testTopLevelSelect() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("create table vcTab (vc varchar(100))");

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42Z71", "select x from t1");
        assertCompileError("42Z71", "select * from t1");
        assertCompileError("42Z71",
            " select xmlparse(document vc preserve whitespace) from vcTab");
        
        assertCompileError("42Z71",
            " values xmlparse(document '<bye/>' preserve whitespace)");
        
        assertCompileError("42Z71",
            " values xmlparse(document '<hel' || 'lo/>' preserve "
            + "whitespace)");

        st.executeUpdate("drop table vcTab");
        st.close();
    }

    /**
     * Various tests for the XMLSERIALIZE operator.
     */
    public void testXMLSerialize() throws Exception
    {
        // Test setup.

        Statement st = createStatement();
        st.executeUpdate("create table vcTab (vc varchar(100))");
        assertUpdateCount(st, 1, "insert into vcTab values ('<hmm/>')");
        assertUpdateCount(st, 1, "insert into vcTab values 'no good'");
        
        // These should fail with various parse errors.

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42Z72", "select xmlserialize(x) from t1");
        assertCompileError("42X01", "select xmlserialize(x as) from t1");
        assertCompileError("42Z73",
            " select xmlserialize(x as int) from t1");

        assertCompileError("42Z73",
            " select xmlserialize(x as boolean) from t1");
        
        assertCompileError("42Z73",
            " select xmlserialize(x as varchar(20) for bit data) from t1");
        
        assertCompileError("42X04",
            " select xmlserialize(y as char(10)) from t1");
        
        assertCompileError("42X25",
            " select xmlserialize(xmlserialize(x as clob) as "
            + "clob) from t1");
        
        assertCompileError("42X25",
            " values xmlserialize('<okay> dokie </okay>' as clob)");
        
        // These should succeed.
        
        ResultSet rs = st.executeQuery("select xmlserialize(x as clob) from t1");

        String [] expColNames = new String [] { "1" };
        JDBC.assertColumnNames(rs, expColNames);
        
        String [][] expRS = new String [][]
        {
            {"<update2> document was inserted as part of an "
                + "UPDATE </update2>"},
            {null},
            {null},
            {null},
            {"<hmm/>"},
            {"<half> <masted> bass </masted> boosted. </half>"},
            {"<umm> decl check </umm>"},
            {"<lets> <try> this out </try> </lets>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlserialize(x1 as clob), xmlserialize(x2 as "
            + "clob) from t2");
        
        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {null, "<notnull/>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlserialize(x as char(100)) from t1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"<update2> document was inserted as part of an "
                + "UPDATE </update2>"},
            {null},
            {null},
            {null},
            {"<hmm/>"},
            {"<half> <masted> bass </masted> boosted. </half>"},
            {"<umm> decl check </umm>"},
            {"<lets> <try> this out </try> </lets>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlserialize(x as varchar(300)) from t1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"<update2> document was inserted as part of an "
                + "UPDATE </update2>"},
            {null},
            {null},
            {null},
            {"<hmm/>"},
            {"<half> <masted> bass </masted> boosted. </half>"},
            {"<umm> decl check </umm>"},
            {"<lets> <try> this out </try> </lets>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // This should succeed at the XMLPARSE level, but fail 
        // with parse/truncation errors at the XMLSERIALIZE.
        
        assertStatementError("2200M", st,
            "select xmlserialize(xmlparse(document vc preserve "
            + "whitespace) as char(10)) from vcTab");
        
        // These should all fail with truncation errors.

        assertStatementError("22001", st,
            " select xmlserialize(x as char) from t1");
        
        assertStatementError("22001", st,
            " select xmlserialize(x as clob(10)) from t1");
        
        assertStatementError("22001", st,
            " select xmlserialize(x as char(1)) from t1");
        
        assertStatementError("22001", st,
            " select length(xmlserialize(x as char(1))) from t1");
        
        assertStatementError("22001", st,
            " select xmlserialize(x as varchar(1)) from t1");
        
        assertStatementError("22001", st,
            " select length(xmlserialize(x as varchar(1))) from t1");
        
        // These checks verify that the XMLSERIALIZE result is the 
        // correct type.

        rs = st.executeQuery("select xmlserialize(x as char(100)) from t1");
        
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("Incorrect XMLSERIALIZE result type:",
            Types.CHAR, rsmd.getColumnType(1));

        rs = st.executeQuery("select xmlserialize(x as varchar(100)) from t1");
        
        rsmd = rs.getMetaData();
        assertEquals("Incorrect XMLSERIALIZE result type:",
            Types.VARCHAR, rsmd.getColumnType(1));

        rs = st.executeQuery("select xmlserialize(x as long varchar) from t1");
        
        rsmd = rs.getMetaData();
        assertEquals("Incorrect XMLSERIALIZE result type:",
            Types.LONGVARCHAR, rsmd.getColumnType(1));

        rs = st.executeQuery("select xmlserialize(x as clob(100)) from t1");
        
        rsmd = rs.getMetaData();
        assertEquals("Incorrect XMLSERIALIZE result type:",
            Types.CLOB, rsmd.getColumnType(1));

        // Cleanup.

        rs.close();
        st.executeUpdate("drop table vcTab");
        st.close();
    }

    /**
     * Various tests with XMLPARSE and XMLSERIALIZE combinations.
     */
    public void testXMLParseSerializeCombos() throws Exception
    {
        // These should fail at the XMLPARSE level.

        Statement st = createStatement();
        assertStatementError("2200M", st,
            "select xmlserialize(xmlparse(document '<hmm>' "
            + "preserve whitespace) as clob) from t1");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42X25",
            " select xmlserialize(xmlparse(document x preserve "
            + "whitespace) as char(100)) from t1");
        
        // These should succeed.
        
        ResultSet rs = st.executeQuery(
            "select xmlserialize(xmlparse(document '<hmm/>' "
            + "preserve whitespace) as clob) from t1 where i = 1");
        
        String [] expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String [][] expRS = new String [][]
        {
            {"<hmm/>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlserialize(xmlparse(document "
            + "xmlserialize(x as clob) preserve whitespace) as "
            + "clob) from t1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"<update2> document was inserted as part of an "
                + "UPDATE </update2>"},
            {null},
            {null},
            {null},
            {"<hmm/>"},
            {"<half> <masted> bass </masted> boosted. </half>"},
            {"<umm> decl check </umm>"},
            {"<lets> <try> this out </try> </lets>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values xmlserialize(xmlparse(document '<okay> dokie "
            + "</okay>' preserve whitespace) as clob)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"<okay> dokie </okay>"}
        };
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i from t1 where xmlparse(document "
            + "xmlserialize(x as clob) preserve whitespace) is not "
            + "null order by i");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"5"},
            {"6"},
            {"7"},
            {"8"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        st.close();
    }

    /**
     * Various tests for the XMLEXISTS operator.
     */
    public void testXMLExists() throws Exception
    {
        // These should fail with various parse errors.

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42X01",
            "select i from t1 where xmlexists(x)");
        
        assertCompileError("42X01",
            "select i from t1 where xmlexists(i)");
        
        assertCompileError("42X01",
            "select i from t1 where xmlexists('//*')");
        
        assertCompileError("42X01",
            "select i from t1 where xmlexists('//*' x)");
        
        assertCompileError("42X01",
            "select i from t1 where xmlexists('//*' passing x)");
        
        assertCompileError("42Z74",
            "select i from t1 where xmlexists('//*' passing by value x)");
        
        assertCompileError("42Z77",
            "select i from t1 where xmlexists('//*' passing by ref i)");
        
        assertCompileError("42Z75",
            "select i from t1 where xmlexists(i passing by ref x)");
        
        assertCompileError("42Z76",
            "select i from t1 where xmlexists(i passing by ref x, x)");
        
        // These should succeed.
        
        Statement st = createStatement();
        ResultSet rs = st.executeQuery(
            "select i from t1 where xmlexists('//*' passing by ref x)");
        
        String [] expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String [][] expRS = new String [][]
        {
            {"1"},
            {"5"},
            {"6"},
            {"7"},
            {"8"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // This should succeed but return no rows.

        rs = st.executeQuery(
            "select i from t1 where xmlexists('//person' passing "
            + "by ref x)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        // This should return one row.
        
        rs = st.executeQuery(
            "select i from t1 where xmlexists('//lets' passing by ref x)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"8"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // XMLEXISTS should return null if the operand is null.

        rs = st.executeQuery(
            "select xmlexists('//lets' passing by ref x) from t1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {null},
            {null},
            {null},
            {"false"},
            {"false"},
            {"false"},
            {"true"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlexists('//try[text()='' this out '']' "
            + "passing by ref x) from t1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {null},
            {null},
            {null},
            {"false"},
            {"false"},
            {"false"},
            {"true"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlexists('//let' passing by ref x) from t1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {null},
            {null},
            {null},
            {"false"},
            {"false"},
            {"false"},
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlexists('//try[text()='' this in '']' "
            + "passing by ref x) from t1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {null},
            {null},
            {null},
            {"false"},
            {"false"},
            {"false"},
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Make sure selection of other columns along with XMLEXISTS
        // still works.

        rs = st.executeQuery(
            "select i, xmlexists('//let' passing by ref x) from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "false"},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", "false"},
            {"6", "false"},
            {"7", "false"},
            {"8", "false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i, xmlexists('//lets' passing by ref x) from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "false"},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", "false"},
            {"6", "false"},
            {"7", "false"},
            {"8", "true"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // XMLEXISTS should work in a VALUES clause, too.
        
        rs = st.executeQuery(
            "values xmlexists('//let' passing by ref "
            + "xmlparse(document '<lets> try this </lets>' "
            + "preserve whitespace))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values xmlexists('//lets' passing by ref "
            + "xmlparse(document '<lets> try this </lets>' "
            + "preserve whitespace))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"true"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Simple check for attribute existence.
        
        rs = st.executeQuery(
            "values xmlexists('//lets/@doit' passing by ref "
            + "xmlparse(document '<lets doit=\"true\"> try this "
            + "</lets>' preserve whitespace))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"true"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values xmlexists('//lets/@dot' passing by ref "
            + "xmlparse(document '<lets doit=\"true\"> try this "
            + "</lets>' preserve whitespace))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // XMLEXISTS in a WHERE clause.
        
        rs = st.executeQuery(
            "select xmlserialize(x1 as clob) from t2 where "
            + "xmlexists('//*' passing by ref x1)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery(
            "select xmlserialize(x2 as clob) from t2 where "
            + "xmlexists('//*' passing by ref x2)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"<notnull/>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlserialize(x1 as clob), xmlexists('//*' "
            + "passing by ref xmlparse(document '<badboy/>' "
            + "preserve whitespace)) from t2");
        
        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "true"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlserialize(x1 as clob), "
            + "xmlexists('//goodboy' passing by ref "
            + "xmlparse(document '<badboy/>' preserve whitespace)) from t2");
        
        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {null, "false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Add some more test tables/data.

        st.executeUpdate(
            "create table xqExists2 (i int, x1 xml, x2 xml not null)");
        
        assertUpdateCount(st, 1,
            " insert into xqExists2 values (1, null, xmlparse(document "
            + "'<ok/>' preserve whitespace))");
        
        rs = st.executeQuery(
            "select i, xmlserialize(x1 as char(10)), "
            + "xmlserialize (x2 as char(10)) from xqExists2");
        
        expColNames = new String [] {"I", "2", "3"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", null, "<ok/>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Now run some XMLEXISTS queries on xqExists2 using boolean
        // operations ('and', 'or') on the XMLEXISTS result.
        
        rs = st.executeQuery(
            "select i from xqExists2 where xmlexists('/ok' passing by "
            + "ref x1) and xmlexists('/ok' passing by ref x2)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st.executeQuery(
            "select i from xqExists2 where xmlexists('/ok' passing by "
            + "ref x1) or xmlexists('/ok' passing by ref x2)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // XMLEXISTS can be used wherever a boolean function is 
        // allowed, for ex, a check constraint...
        
        st.executeUpdate(
            "create table xqExists1 (i int, x xml check "
            + "(xmlexists('//should' passing by ref x)))");
        
        assertUpdateCount(st, 1,
            " insert into xqExists1 values (1, xmlparse(document "
            + "'<should/>' preserve whitespace))");
        
        assertStatementError("23513", st,
            " insert into xqExists1 values (1, xmlparse(document "
            + "'<shouldnt/>' preserve whitespace))");
        
        rs = st.executeQuery(
            "select xmlserialize(x as char(20)) from xqExists1");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"<should/>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Do some namespace queries/examples.
        
        st.executeUpdate("create table xqExists3 (i int, x xml)");

        assertUpdateCount(st, 1,
            " insert into xqExists3 values (1, xmlparse(document '<a:hi "
            + "xmlns:a=\"http://www.hi.there\"/>' preserve whitespace))");
        
        assertUpdateCount(st, 1,
            " insert into xqExists3 values (2, xmlparse(document '<b:hi "
            + "xmlns:b=\"http://www.hi.there\"/>' preserve whitespace))");
        
        assertUpdateCount(st, 1,
            " insert into xqExists3 values (3, xmlparse(document "
            + "'<a:bye xmlns:a=\"http://www.good.bye\"/>' preserve "
            + "whitespace))");
        
        assertUpdateCount(st, 1,
            " insert into xqExists3 values (4, xmlparse(document "
            + "'<b:bye xmlns:b=\"http://www.hi.there\"/>' preserve "
            + "whitespace))");
        
        assertUpdateCount(st, 1,
            " insert into xqExists3 values (5, xmlparse(document "
            + "'<hi/>' preserve whitespace))");
        
        rs = st.executeQuery(
            "select xmlexists('//child::*[name()=\"none\"]' "
            + "passing by ref x) from xqExists3");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {"false"},
            {"false"},
            {"false"},
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlexists('//child::*[name()=''hi'']' "
            + "passing by ref x) from xqExists3");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {"false"},
            {"false"},
            {"false"},
            {"true"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlexists('//child::*[local-name()=''hi'']' "
            + "passing by ref x) from xqExists3");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"true"},
            {"true"},
            {"false"},
            {"false"},
            {"true"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlexists('//child::*[local-name()=''bye'']' "
            + "passing by ref x) from xqExists3");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {"false"},
            {"true"},
            {"true"},
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select "
            + "xmlexists('//*[namespace::*[string()=''http://www.hi"
            + ".there'']]' passing by ref x) from xqExists3");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"true"},
            {"true"},
            {"false"},
            {"true"},
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select "
            + "xmlexists('//*[namespace::*[string()=''http://www.go"
            + "od.bye'']]' passing by ref x) from xqExists3");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {"false"},
            {"true"},
            {"false"},
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlexists('//child::*[local-name()=''hi'' "
            + "and "
            + "namespace::*[string()=''http://www.hi.there'']]' "
            + "passing by ref x) from xqExists3");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"true"},
            {"true"},
            {"false"},
            {"false"},
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlexists('//child::*[local-name()=''bye'' "
            + "and "
            + "namespace::*[string()=''http://www.good.bye'']]' "
            + "passing by ref x) from xqExists3");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {"false"},
            {"true"},
            {"false"},
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select xmlexists('//child::*[local-name()=''bye'' "
            + "and "
            + "namespace::*[string()=''http://www.hi.there'']]' "
            + "passing by ref x) from xqExists3");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"false"},
            {"false"},
            {"false"},
            {"true"},
            {"false"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // If the query returns an atomic value (not a sequence), the XMLEXISTS
        // operator should return TRUE.

//IC see: https://issues.apache.org/jira/browse/DERBY-6634
        expRS = new String[][] {
            { "true" },
            { "true" },
            { "true" },
            { "true" },
            { "true" },
        };

        JDBC.assertFullResultSet(
            st.executeQuery(
                "select xmlexists('1+1' passing by ref x) from xqExists3"),
            expRS);

        JDBC.assertFullResultSet(
            st.executeQuery(
                "select xmlexists('1=2' passing by ref x) from xqExists3"),
            expRS);

        // Cleanup.

        st.executeUpdate("drop table xqExists1");
        st.executeUpdate("drop table xqExists2");
        st.executeUpdate("drop table xqExists3");
        st.close();
    }

    /**
     * Various tests for the XMLQUERY operator.
     */
    public void testXMLQuery() throws Exception
    {
        // These should fail w/ syntax errors.

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("42X01", "select i, xmlquery('//*') from t1");
        assertCompileError("42X01",
            " select i, xmlquery('//*' passing) from t1");
        
        assertCompileError("42X01",
            " select i, xmlquery('//*' passing by ref x) from t1");
        
        assertCompileError("42X01",
            " select i, xmlquery('//*' passing by ref x "
            + "returning sequence) from t1");
        
        assertCompileError("42X01",
            " select i, xmlquery(passing by ref x empty on empty) from t1");
        
        assertCompileError("42X01",
            " select i, xmlquery(xmlquery('//*' returning "
            + "sequence empty on empty) as char(75)) from t1");
        
        // These should fail with "not supported" errors.
        
        assertCompileError("42Z74",
            "select i, xmlquery('//*' passing by ref x returning "
            + "sequence null on empty) from t1");
        
        assertCompileError("42Z74",
            " select i, xmlquery('//*' passing by ref x "
            + "returning content empty on empty) from t1");
        
        // This should fail because XMLQUERY returns an XML value 
        // which is not allowed in top-level result set.

        assertCompileError("42Z71",
            "select i, xmlquery('//*' passing by ref x empty on "
            + "empty) from t1");
        
        // These should fail because context item must be XML.

        assertCompileError("42Z77",
            "select i, xmlquery('//*' passing by ref i empty on "
            + "empty) from t1");
        
        assertCompileError("42Z77",
            " select i, xmlquery('//*' passing by ref 'hello' "
            + "empty on empty) from t1");
        
        assertCompileError("42Z77",
            " select i, xmlquery('//*' passing by ref cast "
            + "('hello' as clob) empty on empty) from t1");
        
        // This should fail because the function is not recognized 
        // by Xalan. The failure should be an error from Xalan 
        // saying what the problem is; it should *NOT* be a NPE, 
        // which is what we were seeing before DERBY-688 was completed.

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        assertCompileError("10000",
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('data(//@*)' passing by ref x "
            + "returning sequence empty on empty)"
            + "  as char(70))"
            + "from t1");

        // This should also fail because the function is not recognized.
        // In addition, we have prefixed the function with an unrecognized
        // namespace. Verify that it fails with an SQLException and that there
        // isn't any NPE in the exception chain.
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        try {
            prepareStatement(
                    "select i,"
                    + "  xmlserialize("
                    + "    xmlquery('myns:data(//@*)' passing by ref x "
                    + "returning sequence empty on empty)"
                    + "  as char(70))"
                    + "from t1");
            fail("Compilation should fail because of unrecognized namespace");
        } catch (SQLException sqle) {
            assertSQLState("10000", sqle);
            Throwable t = sqle;
            while ((t = t.getCause()) != null) {
                if (t instanceof NullPointerException) {
                    fail("No NPE, please!", t);
                }
            }
        }

        // These should all succeed.  Since it's Xalan that's 
        // actually doing the query evaluation we don't need to 
        // test very many queries; we just want to make sure we get 
        // the correct results when there is an empty sequence, 
        // when the xml context is null, and when there is a 
        // sequence with one or more nodes/items in it.  So we just 
        // try out some queries and look at the results.  The 
        // selection of queries is random and is not meant to be 
        // exhaustive.
        
        Statement st = createStatement();
        ResultSet rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('2+2' passing by ref x returning "
            + "sequence empty on empty)"
            + "  as char(70))"
            + "from t1");
        
        String [] expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        String [][] expRS = new String [][]
        {
            {"1", "4"},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", "4"},
            {"6", "4"},
            {"7", "4"},
            {"8", "4"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('./notthere' passing by ref x "
            + "returning sequence empty on empty)"
            + "  as char(70))"
            + "from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", ""},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", ""},
            {"6", ""},
            {"7", ""},
            {"8", ""}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('//*' passing by ref x empty on empty)"
            + "  as char(70))"
            + "from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "<update2> document was inserted as part of an "
                + "UPDATE </update2>"},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", "<hmm/>"},
            {"6", "<half> <masted> bass </masted> boosted. "
                + "</half><masted> bass </masted>"},
            {"7", "<umm> decl check </umm>"},
            {"8", "<lets> <try> this out </try> </lets><try> this out </try>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('//*[text() = \" bass \"]' passing by "
            + "ref x empty on empty)"
            + "  as char(70))"
            + "from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", ""},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", ""},
            {"6", "<masted> bass </masted>"},
            {"7", ""},
            {"8", ""}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('//lets' passing by ref x empty on empty)"
            + "  as char(70))"
            + "from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", ""},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", ""},
            {"6", ""},
            {"7", ""},
            {"8", "<lets> <try> this out </try> </lets>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('//text()' passing by ref x empty on empty)"
            + "  as char(70))"
            + "from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "document was inserted as part of an UPDATE"},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", ""},
            {"6", "bass  boosted."},
            {"7", "decl check"},
            {"8", "this out"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('//try[text()='' this out '']' passing "
            + "by ref x empty on empty)"
            + "  as char(70))"
            + "from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", ""},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", ""},
            {"6", ""},
            {"7", ""},
            {"8", "<try> this out </try>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('//try[text()='' this in '']' passing "
            + "by ref x empty on empty)"
            + "  as char(70))"
            + "from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", ""},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", ""},
            {"6", ""},
            {"7", ""},
            {"8", ""}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('2+.//try' passing by ref x returning "
            + "sequence empty on empty)"
            + "  as char(70))"
            + "from t1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "NaN"},
            {"2", null},
            {"4", null},
            {"3", null},
            {"5", "NaN"},
            {"6", "NaN"},
            {"7", "NaN"},
            {"8", "NaN"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values ('x', xmlserialize("
            + "  xmlquery('//let' passing by ref"
            + "    xmlparse(document '<lets> try this </lets>' "
            + "preserve whitespace)"
            + "  empty on empty)"
            + "as char(30)))");
        
        expColNames = new String [] {"1", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"x", ""}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "values xmlserialize("
            + "  xmlquery('//lets' passing by ref"
            + "    xmlparse(document '<lets> try this </lets>' "
            + "preserve whitespace)"
            + "  empty on empty)"
            + "as char(30))");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"<lets> try this </lets>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        st.close();
    }

    /* Check insertion of XMLQUERY result into a table.  Should 
     * only allow results that are a sequence of exactly one 
     * Document node.
     */
    public void testXMLQueryInsert() throws Exception
    {
        // Create test table and data specific to this test.

        Statement st = createStatement();
        st.executeUpdate("create table xqInsert1 (i int, x xml not null)");

        assertUpdateCount(st, 1,
            " insert into xqInsert1 values (1, xmlparse(document "
            + "'<should> work as planned </should>' preserve whitespace))");
        
        st.executeUpdate("create table xqInsert2 (i int, x xml default null)");
        
        assertUpdateCount(st, 1,
            "insert into xqInsert2 values ("
            + "  9,"
            + "  xmlparse(document '<here><is><my "
            + "height=\"4.4\">attribute</my></is></here>' preserve "
            + "whitespace)"
            + ")");
        
        assertUpdateCount(st, 1,
            " insert into xqInsert2 values ("
            + "  0,"
            + "  xmlparse(document '<there><goes><my "
            + "weight=\"180\">attribute</my></goes></there>' "
            + "preserve whitespace)"
            + ")");
        
        // Show target tables before insertions.
        
        ResultSet rs = st.executeQuery(
            "select i, xmlserialize(x as char(75)) from xqInsert1");
        
        String [] expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        String [][] expRS = new String [][]
        {
            {"1", "<should> work as planned </should>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i, xmlserialize(x as char(75)) from xqInsert2");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"9", "<here><is><my height=\"4.4\">attribute</my></is></here>"},
            {"0", "<there><goes><my "
                + "weight=\"180\">attribute</my></goes></there>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // These should all fail because the result of the XMLQUERY 
        // op is not a valid document (it's either an empty 
        // sequence, a node that is not a Document node, some 
        // undefined value, or a sequence with more than one item 
        // in it).

        assertStatementError("2200L", st,
            "insert into xqInsert1 (i, x) values ("
            + "  20, "
            + "  (select"
            + "    xmlquery('./notthere' passing by ref x "
            + "returning sequence empty on empty)"
            + "    from xqInsert2 where i = 9"
            + "  )"
            + ")");
        
        assertStatementError("2200L", st,
            " insert into xqInsert1 (i, x) values ("
            + "  21,"
            + "  (select"
            + "    xmlquery('//@*' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 9"
            + "  )"
            + ")");
        
        assertStatementError("2200L", st,
            " insert into xqInsert1 (i, x) values ("
            + "  22,"
            + "  (select"
            + "    xmlquery('. + 2' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 9"
            + "  )"
            + ")");
        
        assertStatementError("2200L", st,
            " insert into xqInsert1 (i, x) values ("
            + "  23,"
            + "  (select"
            + "    xmlquery('//*' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 9"
            + "  )"
            + ")");
        
        assertStatementError("2200L", st,
            " insert into xqInsert1 (i, x) values ("
            + "  24,"
            + "  (select"
            + "    xmlquery('//*[//@*]' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 9"
            + "  )"
            + ")");
        
        assertStatementError("2200L", st,
            " insert into xqInsert1 (i, x) values ("
            + "  25,"
            + "  (select"
            + "    xmlquery('//is' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 9"
            + "  )"
            + ")");
        
        assertStatementError("2200L", st,
            " insert into xqInsert1 (i, x) values ("
            + "  26,"
            + "  (select"
            + "    xmlquery('//*[@*]' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 9"
            + "  )"
            + ")");
        
        // These should succeed.
        
        assertUpdateCount(st, 1,
            "insert into xqInsert1 (i, x) values ("
            + "  27,"
            + "  (select"
            + "    xmlquery('.' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 9"
            + "  )"
            + ")");
        
        assertUpdateCount(st, 1,
            " insert into xqInsert1 (i, x) values ("
            + "  28,"
            + "  (select"
            + "    xmlquery('/here/..' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 9"
            + "  )"
            + ")");
        
        // Verify results.
        
        rs = st.executeQuery(
            "select i, xmlserialize(x as char(75)) from xqInsert1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "<should> work as planned </should>"},
            {"27", "<here><is><my height=\"4.4\">attribute</my></is></here>"},
            {"28", "<here><is><my height=\"4.4\">attribute</my></is></here>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Next two should _both_ succeed because there's no row 
        // with i = 100 in xqInsert2, thus the SELECT will return null
        // and XMLQUERY operator should never get executed.  x will be 
        // NULL in these cases.
        
        assertUpdateCount(st, 1,
            "insert into xqInsert2 (i, x) values ("
            + "  29,"
            + "  (select"
            + "    xmlquery('2+2' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 100"
            + "  )"
            + ")");
        
        assertUpdateCount(st, 1,
            " insert into xqInsert2 (i, x) values ("
            + "  30,"
            + "  (select"
            + "    xmlquery('.' passing by ref x returning "
            + "sequence empty on empty)"
            + "    from xqInsert2 where i = 100"
            + "  )"
            + ")");
        
        // Verify results.
        
        rs = st.executeQuery(
            "select i, xmlserialize(x as char(75)) from xqInsert2");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"9", "<here><is><my height=\"4.4\">attribute</my></is></here>"},
            {"0", "<there><goes><my "
                + "weight=\"180\">attribute</my></goes></there>"},
            {"29", null},
            {"30", null}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Cleanup.

        st.executeUpdate("drop table xqInsert1");
        st.executeUpdate("drop table xqInsert2");
        st.close();
    }

    /* Check updates using XMLQUERY results.  Should only allow 
     * results that constitute a valid DOCUMENT node (i.e. that 
     * can be parsed by the XMLPARSE operator).
     */
        
    public void testXMLQueryUpdate() throws Exception
    {
        // Create test table and data.

        Statement st = createStatement();
        st.executeUpdate("create table xqUpdate (i int, x xml default null)");
        assertUpdateCount(st, 2, "insert into xqUpdate (i) values 29, 30");
        assertUpdateCount(st, 1,
            "insert into xqUpdate values ("
            + "  9,"
            + "  xmlparse(document '<here><is><my "
            + "height=\"4.4\">attribute</my></is></here>' preserve "
            + "whitespace)"
            + ")");
        
        // These updates should succeed.

        assertUpdateCount(st, 1,
            "update xqUpdate"
            + "  set x = "
            + "    xmlquery('.' passing by ref"
            + "      xmlparse(document '<none><here/></none>' "
            + "preserve whitespace)"
            + "    returning sequence empty on empty)"
            + "where i = 29");
        
        assertUpdateCount(st, 1,
            " update xqUpdate"
            + "  set x = "
            + "    xmlquery('self::node()[//@height]' passing by ref"
            + "      (select"
            + "        xmlquery('.' passing by ref x empty on empty)"
            + "        from xqUpdate"
            + "        where i = 9"
            + "      )"
            + "    empty on empty)"
            + "where i = 30");
        
        // These should fail because result of XMLQUERY isn't a 
        // DOCUMENT.
        
        assertStatementError("2200L", st,
            "update xqUpdate"
            + "  set x = xmlquery('.//*' passing by ref x empty on empty)"
            + "where i = 29");
        
        assertStatementError("2200L", st,
            " update xqUpdate"
            + "  set x = xmlquery('./notthere' passing by ref x "
            + "empty on empty)"
            + "where i = 30");
        
        assertStatementError("2200L", st,
            " update xqUpdate"
            + "  set x ="
            + "    xmlquery('//*[@weight]' passing by ref"
            + "      (select"
            + "        xmlquery('.' passing by ref x empty on empty)"
            + "        from xqUpdate"
            + "        where i = 9"
            + "      )"
            + "    empty on empty)"
            + "where i = 30");
        
        assertStatementError("2200L", st,
            " update xqUpdate"
            + "  set x ="
            + "    xmlquery('//*/@height' passing by ref"
            + "      (select"
            + "        xmlquery('.' passing by ref x empty on empty)"
            + "        from xqUpdate"
            + "        where i = 9"
            + "      )"
            + "    empty on empty)"
            + "where i = 30");
        
        // Next two should succeed because there's no row with i = 
        // 100 in xqUpdate and thus xqUpdate should remain unchanged after 
        // these updates.

        assertUpdateCount(st, 0,
            "update xqUpdate"
            + "  set x = xmlquery('//*' passing by ref x empty on empty)"
            + "where i = 100");
        
        assertUpdateCount(st, 0,
            " update xqUpdate"
            + "  set x = xmlquery('4+4' passing by ref x empty on empty)"
            + "where i = 100");
        
        // Verify results.
        
        ResultSet rs = st.executeQuery(
            "select i, xmlserialize(x as char(75)) from xqUpdate");
        
        String [] expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        String [][] expRS = new String [][]
        {
            {"29", "<none><here/></none>"},
            {"30", "<here><is><my height=\"4.4\">attribute</my></is></here>"},
            {"9", "<here><is><my height=\"4.4\">attribute</my></is></here>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Cleanup.
        st.executeUpdate("drop table xqUpdate");
        st.close();
    }

    /* Pass results of an XMLQUERY op into another XMLQUERY op. 
     * Should work so long as results of the first op constitute
     * a valid document.
     */
    public void testNestedXMLQuery() throws Exception
    {
        // Should fail because result of inner XMLQUERY op
        // isn't a valid document.

//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        Statement st = createStatement();
        assertStatementError("2200V", st,
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('//lets/@*' passing by ref"
            + "      xmlquery('/okay/text()' passing by ref"
            + "        xmlparse(document '<okay><lets "
            + "boki=\"inigo\"/></okay>' preserve whitespace)"
            + "      empty on empty)"
            + "    empty on empty)"
            + "  as char(100))"
            + "from t1 where i > 5");
        
        assertStatementError("2200V", st,
            " select i,"
            + "  xmlserialize("
            + "    xmlquery('.' passing by ref"
            + "      xmlquery('//lets' passing by ref"
            + "        xmlparse(document '<okay><lets "
            + "boki=\"inigo\"/></okay>' preserve whitespace)"
            + "      empty on empty)"
            + "    empty on empty)"
            + "  as char(100))"
            + "from t1 where i > 5");
        
        assertStatementError("2200V", st,
            " select i,"
            + "  xmlexists('.' passing by ref"
            + "    xmlquery('/okay' passing by ref"
            + "      xmlparse(document '<okay><lets "
            + "boki=\"inigo\"/></okay>' preserve whitespace)"
            + "    empty on empty)"
            + "  )"
            + "from t1 where i > 5");
        
        // Should succeed but result is empty sequence.
        
        ResultSet rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('/not' passing by ref"
            + "      xmlquery('.' passing by ref"
            + "        xmlparse(document '<okay><lets "
            + "boki=\"inigo\"/></okay>' preserve whitespace)"
            + "      empty on empty)"
            + "    empty on empty)"
            + "  as char(100))"
            + "from t1 where i > 5");
        
        String [] expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        String [][] expRS = new String [][]
        {
            {"6", ""},
            {"7", ""},
            {"8", ""}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Should succeed with various results.
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('//lets' passing by ref"
            + "      xmlquery('.' passing by ref"
            + "        xmlparse(document '<okay><lets "
            + "boki=\"inigo\"/></okay>' preserve whitespace)"
            + "      empty on empty)"
            + "    empty on empty)"
            + "  as char(100))"
            + "from t1 where i > 5");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "<lets boki=\"inigo\"/>"},
            {"7", "<lets boki=\"inigo\"/>"},
            {"8", "<lets boki=\"inigo\"/>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('string(//@boki)' passing by ref"
            + "      xmlquery('/okay/..' passing by ref"
            + "        xmlparse(document '<okay><lets "
            + "boki=\"inigo\"/></okay>' preserve whitespace)"
            + "      empty on empty)"
            + "    empty on empty)"
            + "  as char(100))"
            + "from t1 where i > 5");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "inigo"},
            {"7", "inigo"},
            {"8", "inigo"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlserialize("
            + "    xmlquery('/half/masted/text()' passing by ref"
            + "      xmlquery('.' passing by ref x empty on empty)"
            + "    empty on empty)"
            + "  as char(100))"
            + "from t1 where i = 6");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "bass"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i,"
            + "  xmlexists('/half/masted/text()' passing by ref"
            + "    xmlquery('.' passing by ref x empty on empty)"
            + "  )"
            + "from t1 where i = 6");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"6", "true"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        st.close();
    }

    /**
     *  DERBY-1759: Serialization of attribute nodes.
     */
    public void testAttrSerialization() throws Exception
    {
        // Create test table and one row of data.

        Statement st = createStatement();
        st.executeUpdate("create table attserTable (i int, x xml)");
        assertUpdateCount(st, 1, "insert into attserTable values (0, null)");
        assertUpdateCount(st, 1,
            "insert into attserTable values (10,"
            + "  xmlparse(document"
            + "    '<threeatts first=\"1\" second=\"two\" "
            + "third=\"le 3 trois\"/>'"
            + "    preserve whitespace"
            + "  ))");
        
        // Echo attserTable rows for reference.
        
        ResultSet rs = st.executeQuery(
            "select i, xmlserialize(x as char(75)) from attserTable");
        
        String [] expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        String [][] expRS = new String [][]
        {
            {"0", null},
            {"10", "<threeatts first=\"1\" second=\"two\" third=\"le 3 trois\"/>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // This should fail because XML serialization dictates that 
        // we throw an error if an attempt is made to serialize a 
        // sequence that has one or more top-level attributes nodes.

        assertStatementError("2200W", st,
            "select"
            + "  xmlserialize("
            + "    xmlquery("
            + "      '//@*' passing by ref x empty on empty"
            + "    )"
            + "  as char(50))"
            + " from attserTable"
            + " where xmlexists('//@*' passing by ref x)");
        
        // Demonstrate that Xalan "string" function only returns 
        // string value of first attribute and thus cannot be used 
        // to retrieve a sequence of att values.
        
        rs = st.executeQuery(
            "select"
            + "  xmlserialize("
            + "    xmlquery("
            + "      'string(//@*)'"
            + "      passing by ref x empty on empty"
            + "    )"
            + "  as char(50))"
            + " from attserTable"
            + " where xmlexists('//@*' passing by ref x)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Xalan doesn't have a function that allows retrieval of a 
        // sequence of attribute values.  One can only retrieve a 
        // sequence of attribute *nodes*, but since those can't be 
        // serialized (because of SQL/XML rules) the user has no 
        // way to get them.  The following is a very (VERY) ugly 
        // two-part workaround that one could use until something 
        // better is available.  First, get the max number of 
        // attributes in the table.
        
        rs = st.executeQuery(
            "select"
            + "  max("
            + "    cast("
            + "      xmlserialize("
            + "        xmlquery('count(//@*)' passing by ref x "
            + "empty on empty)"
            + "      as char(50))"
            + "    as int)"
            + "  )"
            + " from attserTable");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"3"}
        };

//IC see: https://issues.apache.org/jira/browse/DERBY-2502
        JDBC.assertFullResultSet(rs, expRS, true, false);
        
        // The use of MAX in the previous query throws a warning because
        // the table T1 has null values.  Just for sanity check for that
        // warning if we're in embedded mode (warnings are not returned
        // in client/server mode--see DERBY-159).

        if (usingEmbedded())
        {
            SQLWarning sqlWarn = rs.getWarnings();
            if (sqlWarn == null)
                sqlWarn = st.getWarnings();
            if (sqlWarn == null)
                sqlWarn = getConnection().getWarnings();
            assertTrue("Expected warning but found none.", (sqlWarn != null));
            assertSQLState("01003", sqlWarn);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-2502
        rs.close();
        
        // Then use XPath position syntax to retrieve the 
        // attributes and concatenate them.  We need one call to 
        // string(//@[i]) for every for every i between 1 and the 
        // value found in the preceding query.  In this case we 
        // know the max is three, so use that.
        
        rs = st.executeQuery(
            "select"
            + "  xmlserialize("
            + "    xmlquery("
            + "      'concat(string(//@*[1]), \" \","
            + "        string(//@*[2]), \" \","
            + "        string(//@*[3]))'"
            + "      passing by ref x empty on empty"
            + "    )"
            + "  as char(50))"
            + " from attserTable"
            + " where xmlexists('//@*' passing by ref x)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1 two le 3 trois"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // Cleanup.
        st.executeUpdate("drop table attserTable");
        st.close();
    }

    /**
     * DERBY-1718 create trigger fails when SPS contains XML 
     * related op.
     */
    public void testTriggerSPSWithXML() throws Exception
    {
        Statement st = createStatement();
        st.executeUpdate("create table trigSPS1 (i int, x xml)");
        st.executeUpdate("create table trigSPS2 (i int, x xml)");

        assertUpdateCount(st, 1,
            " insert into trigSPS1 values (1, xmlparse(document "
            + "'<name> john </name>' preserve whitespace))");

        st.executeUpdate(
            "create trigger tx after insert on trigSPS1 for each "
            + "statement mode db2sql insert into trigSPS2 values "
            + "(1, xmlparse(document '<name> jane </name>' "
            + "preserve whitespace))");
        
        assertUpdateCount(st, 1,
            " insert into trigSPS1 values (2, xmlparse(document "
            + "'<name> ally </name>' preserve whitespace))");
        
        ResultSet rs = st.executeQuery(
            "select i, xmlserialize(x as varchar(20)) from trigSPS1");
        
        String [] expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        String [][] expRS = new String [][]
        {
            {"1", "<name> john </name>"},
            {"2", "<name> ally </name>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i, xmlserialize(x as varchar(20)) from trigSPS2");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "<name> jane </name>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st, 2, "insert into trigSPS1 select * from trigSPS1");
        
        rs = st.executeQuery(
            "select i, xmlserialize(x as varchar(20)) from trigSPS1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "<name> john </name>"},
            {"2", "<name> ally </name>"},
            {"1", "<name> john </name>"},
            {"2", "<name> ally </name>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i, xmlserialize(x as varchar(20)) from trigSPS2");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "<name> jane </name>"},
            {"1", "<name> jane </name>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate("drop trigger tx");
        assertUpdateCount(st, 4, "delete from trigSPS1");
        assertUpdateCount(st, 2, "delete from trigSPS2");
        assertUpdateCount(st, 1,
            " insert into trigSPS1 values (1, xmlparse(document "
            + "'<name> john </name>' preserve whitespace))");
        
        st.executeUpdate(
            "create trigger tx after insert on trigSPS1 for each "
            + "statement mode db2sql insert into trigSPS2 values "
            + "(1, (select xmlquery('.' passing by ref x "
            + "returning sequence empty on empty) from trigSPS1 "
            + "where i = 1))");
        
        assertUpdateCount(st, 1,
            " insert into trigSPS1 values (2, xmlparse(document "
            + "'<name> ally </name>' preserve whitespace))");
        
        rs = st.executeQuery(
            "select i, xmlserialize(x as varchar(20)) from trigSPS1");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "<name> john </name>"},
            {"2", "<name> ally </name>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            "select i, xmlserialize(x as varchar(20)) from trigSPS2");
        
        expColNames = new String [] {"I", "2"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "<name> john </name>"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);
        st.executeUpdate("drop trigger tx");

        st.executeUpdate("drop table trigSPS1");
        st.executeUpdate("drop table trigSPS2");
        st.close();
    }

    /**
     * Test how numeric values returned by XPath queries are formatted.
     */
    public void testNumericReturnValues() throws SQLException {
        // Array of XPath queries and their expected return values
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
        String[][] queries = {
            // Long.MAX_VALUE. We lose some precision.
            { "9223372036854775807", "9223372036854776000" },
            // We can also have numbers larger than Long.MAX_VALUE, but we
            // don't get higher precision.
            { "9223372036854775807123456789", "9223372036854776000000000000" },
            // Expect plain format, not scientific notation like 1.23E-10
            { "123 div 1000000000000", "0.000000000123" },
            // Trailing zeros after decimal point should be stripped away
            { "1", "1" },
            { "1.0", "1" },
            { "1.000", "1" },
            { "1.00010", "1.0001" },
            // -0 should be normalized to 0
            { "-0", "0" },
            { "-0.0", "0" },
            { "-0.00", "0" },
            // Division by zero yields Not A Number or +/- Infinity
//IC see: https://issues.apache.org/jira/browse/DERBY-2739
            { "0 div 0", "NaN" },
            { "3.14 div 0", "Infinity" },
            { "-3.14 div 0", "-Infinity" },
            // Not strictly numeric, but let's test boolean too
//IC see: https://issues.apache.org/jira/browse/DERBY-6634
            { "1=1", "true" },
            { "1=2", "false" },
        };

        Statement s = createStatement();

        for (int i = 0; i < queries.length; i++) {
            String xpath = queries[i][0];
            String expected = queries[i][1];

            String sql = "select xmlserialize(xmlquery('" + xpath +
                    "' passing by ref x empty on empty) as clob) " +
                    "from t1 where i = 1";

            JDBC.assertSingleValueResultSet(s.executeQuery(sql), expected);
        }
    }

    /**
     * Wrapper for the tests in XMLTypeAndOpsTest.  We have some
     * fixture tables/data that we want to create a single time
     * before the tests run and then which we want to clean up
     * when the tests complete.  This class acts the "wrapper"
     * that does this one-time setup and teardown.  (Actually,
     * we do it two times: once for running in embedded mode
     * and once for running in client/server mode--we create an
     * instance of this class for each mode.)
     */
    private static class XMLTestSetup extends BaseJDBCTestSetup
    {
        public XMLTestSetup(BaseTestSuite tSuite) {
            super(tSuite);
        }

        /**
         * Before running the tests in XMLTypeAndOps we create two
         * base tables and insert the common "fixture" data.
         */
        public void setUp() throws Exception
        {
            Connection c = getConnection();
            Statement s = c.createStatement();

            /* Create test tables as a fixture for this test.  Note
             * that we're implicitly testing the creation of XML columns
             * as part of this setup.  All of the following should
             * succeed; see testXMLColCreation() for some tests where
             * column creation is expected to fail.
             */

            s.executeUpdate("create table t1 (i int, x xml)");
            s.executeUpdate("create table t2 (x2 xml not null)");
            s.executeUpdate("alter table t2 add column x1 xml");

            /* Insert test data.  Here we're implicitly tesing
             * the XMLPARSE operator in situations where it should
             * succeed.  Negative test cases are tesed in the
             * testIllegalNullInserts() and testXMLParse() methods
             * of the XMLTypeAndOps class.
             */

            // Null values.

            assertUpdateCount(s, 1, "insert into t1 values (1, null)");
            assertUpdateCount(s, 1,
                "insert into t1 values (2, cast (null as xml))");

            assertUpdateCount(s, 1, "insert into t1 (i) values (4)");
            assertUpdateCount(s, 1, "insert into t1 values (3, default)");

            // Non-null values.

            assertUpdateCount(s, 1,
                "insert into t1 values (5, xmlparse(document "
                + "'<hmm/>' preserve whitespace))");
            
            assertUpdateCount(s, 1,
                " insert into t1 values (6, xmlparse(document "
                + "'<half> <masted> bass </masted> boosted. </half>' "
                + "preserve whitespace))");
            
            assertUpdateCount(s, 1,
                " insert into t2 (x1, x2) values (null, "
                + "xmlparse(document '<notnull/>' preserve whitespace))");
            
            assertUpdateCount(s, 1,
                " insert into t1 values (7, xmlparse(document '<?xml "
                + "version=\"1.0\" encoding= \"UTF-8\"?><umm> decl "
                + "check </umm>' preserve whitespace))");
            
            assertUpdateCount(s, 1,
                "insert into t1 values (8, xmlparse(document '<lets> "
                + "<try> this out </try> </lets>' preserve whitespace))");

            assertUpdateCount(s, 1,
                " update t1 set x = xmlparse(document '<update> "
                + "document was inserted as part of an UPDATE "
                + "</update>' preserve whitespace) where i = 1");
            
            assertUpdateCount(s, 1,
                " update t1 set x = xmlparse(document '<update2> "
                + "document was inserted as part of an UPDATE "
                + "</update2>' preserve whitespace) where "
                + "xmlexists('/update' passing by ref x)");

            s.close();
            c.close();
            s = null;
            c= null;
        }

        /**
         * For test teardown we just drop the two tables we created in
         * test setup and then clean up local objects if needed.
         */
        public void tearDown() throws Exception
        {
            Connection c = getConnection();
            Statement s = c.createStatement();

            s.executeUpdate("drop table t1");
            s.executeUpdate("drop table t2");
            s.close();
            c.close();

            s = null;
            c = null; 
            super.tearDown();
        }
    }
}
