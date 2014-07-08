/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.SelectivityTest
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;

public class SelectivityTest extends BaseJDBCTestCase {

    public SelectivityTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        Properties props = new Properties();
        // first disable the automatic statistics gathering so we get
        // clean statistics
        // then switch the statement cache size to 0, so that doesn't
        // interfere and previous tests' left-overs are gone.
        props.setProperty("derby.storage.indexStats.auto", "false");
        props.setProperty("derby.language.statementCacheSize", "0");
        // set the props, and boot the db
        Test test = new DatabasePropertyTestSetup(
            new BaseTestSuite(SelectivityTest.class), props, true);
        
        return new CleanDatabaseTestSetup(test) {
            protected void decorateSQL(Statement s) throws SQLException
            {        
                s.executeUpdate("create table two (x int)");
                s.executeUpdate("insert into two values (1),(2)");
                s.executeUpdate("create table ten (x int)");
                s
                        .executeUpdate("insert into ten values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
                s.executeUpdate("create table twenty (x int)");
                s
                        .executeUpdate("insert into twenty values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20)");
                s
                        .executeUpdate("create table hundred (x int generated always as identity, dc int)");
                s
                        .executeUpdate("insert into hundred (dc) select t1.x from ten t1, ten t2");
                s
                        .executeUpdate("create table template (id int not null generated always as identity, two int, twenty int, hundred int)");
                // 4000 rows
                s
                        .executeUpdate("insert into template (two, twenty, hundred) select two.x, twenty.x, hundred.x from two, twenty, hundred");
                s.executeUpdate("create index template_two on template(two)");
                s
                        .executeUpdate("create index template_twenty on template(twenty)");
                // 20 distinct values
                s
                        .executeUpdate("create index template_22 on template(twenty,two)");
                s
                        .executeUpdate("create unique index template_id on template(id)");
                s
                        .executeUpdate("create index template_102 on template(hundred,two)");
                s
                        .executeUpdate("create table test (id int, two int, twenty int, hundred int)");
                s.executeUpdate("create index test_id on test(id)");
                s.executeUpdate("insert into test select * from template");

                s.executeUpdate("create view showstats as "
                                + "select cast (conglomeratename as varchar(20)) indexname, "
                                + "cast (statistics as varchar(40)) stats, "
                                + "creationtimestamp createtime, "
                                + "colcount ncols "
                                + "from sys.sysstatistics, sys.sysconglomerates "
                                + "where conglomerateid = referenceid");
                ResultSet statsrs = s
                        .executeQuery("select indexname, stats, ncols from showstats order by indexname, stats, createtime, ncols");
                JDBC.assertFullResultSet(statsrs, new String[][] {
                        {"TEMPLATE_102","numunique= 100 numrows= 4000","1"},
                        {"TEMPLATE_102","numunique= 200 numrows= 4000","2"},
                        {"TEMPLATE_22","numunique= 20 numrows= 4000","1"},
                        {"TEMPLATE_22","numunique= 40 numrows= 4000","2"},
                        {"TEMPLATE_TWENTY","numunique= 20 numrows= 4000","1"},
                        {"TEMPLATE_TWO","numunique= 2 numrows= 4000","1"}});               
                s
                        .executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','TEMPLATE',null)");
                s
                        .executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','TEST',null)");
                statsrs = s
                        .executeQuery("select indexname, stats, ncols from showstats order by indexname, stats, createtime, ncols");
                JDBC.assertFullResultSet(statsrs, new String[][] {
                        {"TEMPLATE_102","numunique= 100 numrows= 4000","1"},
                        {"TEMPLATE_102","numunique= 200 numrows= 4000","2"},
                        {"TEMPLATE_22","numunique= 20 numrows= 4000","1"},
                        {"TEMPLATE_22","numunique= 40 numrows= 4000","2"},
                        {"TEMPLATE_TWENTY","numunique= 20 numrows= 4000","1"},
                        {"TEMPLATE_TWO","numunique= 2 numrows= 4000","1"},
                        {"TEST_ID","numunique= 4000 numrows= 4000","1"},
                        });
                
                s.executeUpdate("create table t1 " +
                		"(id int generated always as identity, " +
                		"two int, twenty int, hundred varchar(3))");
                s.executeUpdate("insert into t1 (hundred, twenty, two) " +
                		"select CAST(CHAR(hundred.x) AS VARCHAR(3)), " +
                		"twenty.x, two.x from hundred, twenty, two");
                s.executeUpdate("create table t2 " +
                		"(id int generated always as identity, " +
                		"two int, twenty int, hundred varchar(3))");
                s.executeUpdate("insert into t2 (hundred, twenty, two) " +
                		"select CAST(CHAR(hundred.x) AS VARCHAR(3)) , " +
                		"twenty.x, two.x from hundred, twenty, two");
                s.executeUpdate("create table t3 " +
                		"(id int generated always as identity, " +
                		"two int, twenty int, hundred varchar(3))");
                s.executeUpdate("insert into t3 (hundred, twenty, two) " +
                		"select CAST(CHAR(hundred.x) AS VARCHAR(3)), " +
                		"twenty.x, two.x from hundred, twenty, two");
                s.executeUpdate("create index t1_hundred on t1(hundred)");
                s.executeUpdate("create index t1_two_twenty on t1(two,twenty)");
                s.executeUpdate("create index " +
                		"t1_twenty_hundred on t1(twenty, hundred)");
                s.executeUpdate("create index t2_hundred on t2(hundred)");
                s.executeUpdate("create index t2_two_twenty on t2(two,twenty)");
                s.executeUpdate("create index t2_twenty_hundred on t2(twenty, hundred)");
                s.executeUpdate("create index t3_hundred on t3(hundred)");
                s.executeUpdate("create index t3_two_twenty on t3(two,twenty)");
                s.executeUpdate("create index t3_twenty_hundred on t3(twenty, hundred)");
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                        "('APP','T1',null)");
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                        "('APP','T2',null)");
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                        "('APP','T3',null)");

                statsrs = s.executeQuery(
                        "select indexname, stats, ncols from showstats " +
                        "where indexname like 'T1%' " +
                        "order by indexname, stats");
                JDBC.assertFullResultSet(statsrs, new String[][] {
                        {"T1_HUNDRED","numunique= 100 numrows= 4000","1"},
                        {"T1_TWENTY_HUNDRED","numunique= 20 numrows= 4000","1"},
                        {"T1_TWENTY_HUNDRED","numunique= 2000 numrows= 4000","2"},
                        {"T1_TWO_TWENTY","numunique= 2 numrows= 4000","1"},
                        {"T1_TWO_TWENTY","numunique= 40 numrows= 4000","2"}});
                statsrs = s.executeQuery(
                        "select indexname, stats, ncols from showstats " +
                        "where indexname like 'T2%' order by indexname, stats");
                JDBC.assertFullResultSet(statsrs, new String[][] {
                        {"T2_HUNDRED","numunique= 100 numrows= 4000","1"},
                        {"T2_TWENTY_HUNDRED","numunique= 20 numrows= 4000","1"},
                        {"T2_TWENTY_HUNDRED","numunique= 2000 numrows= 4000","2"},
                        {"T2_TWO_TWENTY","numunique= 2 numrows= 4000","1"},
                        {"T2_TWO_TWENTY","numunique= 40 numrows= 4000","2"}});
                statsrs = s.executeQuery(
                        "select indexname, stats, ncols from showstats " +
                        "where indexname like 'T3%' order by indexname, stats");
                JDBC.assertFullResultSet(statsrs, new String[][] {
                        {"T3_HUNDRED","numunique= 100 numrows= 4000","1"},
                        {"T3_TWENTY_HUNDRED","numunique= 20 numrows= 4000","1"},
                        {"T3_TWENTY_HUNDRED","numunique= 2000 numrows= 4000","2"},
                        {"T3_TWO_TWENTY","numunique= 2 numrows= 4000","1"},
                        {"T3_TWO_TWENTY","numunique= 40 numrows= 4000","2"}});
                
                s.executeUpdate("create table scratch_table" +
                        "(id int, two int, twenty int, hundred int)");
                s.executeUpdate("insert into scratch_table select " +
                        "id, two, twenty, CAST(CHAR(hundred) AS INTEGER) " +
                        "from t1");
                s.executeUpdate("create index st_all on scratch_table" +
                        "(two, twenty, hundred)");
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                        "('APP','SCRATCH_TABLE',null)");
                
                s.executeUpdate("create table complex" +
                        "(id int generated always as identity, " +
                        "two int, twenty int, hundred int, a int, b int)");
                s.executeUpdate(
                        "insert into complex (two, twenty, hundred, a, b) " +
                        "select two.x, twenty.x, hundred.x, two.x, twenty.x " +
                        "from two, twenty, hundred");
                s.executeUpdate("create index complexind on complex" +
                        "(two, twenty, hundred, a, b)");
                s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                        "('APP','COMPLEX',null)");

            }
        };
    }
    
    public void testSingleColumnSelectivity() throws SQLException {
        // choose whatever plan you want but the row estimate should be.
        //(n * n) * 0.5
        // join on two, template inner, all rows.
        Connection conn = getConnection();
        Statement s = createStatement();
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TEMPLATE',NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TEST',NULL)");
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        s.executeQuery("select template.id from --DERBY-PROPERTIES joinOrder=fixed\n" 
                + "test, template where test.two = template.two").close();
        checkEstimatedRowCount(conn,8020012.5);
        
        // choose hash join. Selectivity should be the same
        // join on two. template inner, hash join
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=hash \n" +
                "where test.two = template.two").close();
        checkEstimatedRowCount(conn,8020012.5);
        RuntimeStatisticsParser rtsp = 
              SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        
        // choose NL join, no index. Selectivity should be the same
        // join on two. template inner, NL, no index, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=null \n" +
                "where test.two = template.two").close();
        checkEstimatedRowCount(conn,8020012.5);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertFalse(rtsp.usedHashJoin());
        
        // choose NL join, index template_two. Selectivity should be the same
        // join on two. template inner, NL, index=two, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_two \n" +
                "where test.two = template.two").close();
        checkEstimatedRowCount(conn,8020012.5);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_TWO"));
        
        // do joins on 20
        // first NL
        // join on twenty. template inner, NL, index=template_twenty, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_twenty \n" +
                "where test.twenty = template.twenty").close();
        // Rowcount should be same as testSingleColumnSelectivityHash
        checkEstimatedRowCount(conn,802001.25);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_TWENTY"));
        
        // join on 20 but use index 20_2
        // cost as well as selectivity should be divided using selectivity
        // cost should same as template_twenty, or just a shade more...
        // join on twenty. template inner, NL, index=template_22, all rows
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_22 \n" +
                "where test.twenty = template.twenty").close();
        checkEstimatedRowCount(conn,802001.25);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_22"));
        
        // join on twenty but no index
        // note: the original test had this comment:
        // 'rc should be divided using selectivity. cost should be way different'
        // however, it seems the ec is identical.
        // join on twenty, template inner, NL, index=null, all rows
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=null \n" +
                "where test.twenty = template.twenty").close();
        checkEstimatedRowCount(conn,802001.25);
        
        // still single column, try stuff on 100 but with extra qualification
        // on outer table.
        // row count is 100 * 4000 * 0.01 = 4000
        // join on hundred. 
        // template inner, NL, index=template_102, 100 rows from outer
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_102 \n" +
                "where test.hundred = template.hundred and test.id <= 100").close();
        // note: original cloudscape result was expecting 3884.85 here.
        checkEstimatedRowCount(conn,3924.9);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEST", "TEST_ID"));
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_102"));
        
        // join on hundred. 
        // template inner, NL, index=null, 100 rows from outer
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=null \n" +
                "where test.hundred = template.hundred and test.id <= 100").close();
        checkEstimatedRowCount(conn,3924.9);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEST", "TEST_ID"));
        
        // join on hundred. 
        // template inner, hash, index=null, 100 rows from outer.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=hash, " +
                "index=null \n" +
                "where test.hundred = template.hundred and test.id <= 100").close();
        checkEstimatedRowCount(conn,3924.9);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEST", "TEST_ID"));
    }
    
    public void testMultiPredicate() throws SQLException {
        // multi predicate tests.
        // first do a oin involving twenty and two
        // forde use of a simngle column index to do the join
        // the row count should involve statistics from both 10 and 2 though...
        
        // row count should 4K * 4K * 1/40 = 400,000
        // cost doesn't show up in output but should depend on the index
        // being used (verify by hand before checking in.)
        setAutoCommit(false);
        Connection conn = getConnection();
        Statement s = createStatement();
        
        // join on twenty/two. template inner, hash, index=null, all rows.
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TEST',NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TEMPLATE',NULL)");
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=hash, " +
                "index=null \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,401000.625);
        RuntimeStatisticsParser rtsp = 
                SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        
        // join on twenty/two. template inner, NL, index=template_two, all rows
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_two \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,401000.625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_TWO"));
        
        // join on twenty/two. 
        // template inner, NL, index=template_twenty, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_twenty \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,401000.625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_TWENTY"));
        
        // join on twenty/two. template inner, NL, index=template_22, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_22 \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,401000.625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_22"));
        
        // multi predicate tests continued
        // drop index twenty, two -- use above predicates
        // should be smart enough to figure out the selectivity by
        // combining twenty and two.
        s.executeUpdate("drop index template_22");
        
        // join on twenty/two. index twenty_two dropped. 
        // template inner, hash, index=null, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=hash, " +
                "index=null \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,401000.625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        
        // join on twenty/two. index twenty_two dropped. 
        // template inner, NL, index=template_two, all rows.'
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_two \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,401000.625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_TWO"));
        
        // join on twenty/two. index twenty_two dropped. 
        // template inner, NL, index=template_twenty, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_twenty \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,401000.625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_TWENTY"));
        
        s.executeUpdate("drop index template_two");
        
        // we only have index template_twenty
        // for the second predicate we should use 0.1 instead of 0.5
        // thus reducing earlier row count by a factor of 5
        // 80,000 instead of 400,000
        
        // join on twenty/two. index twenty_two and two dropped. 
        // template inner, NL, index=null, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=null \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,80200.12500000001);
        
        // join on twenty/two. index twenty_two and two dropped. 
        // template inner, NL, index=template_twenty, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_twenty \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,80200.12500000001);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_TWENTY"));
        
        // now drop index template_twenty
        // selectivity should become 0.1 * 0.1 = 0.01
        // 16 * 10^6 * .01 = 160,000
        
        s.executeUpdate("drop index template_twenty");
        
        // join on twenty/two. all indexes dropped.
        // template inner, NL, index=null, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=null \n" +
                "where test.twenty = template.twenty " +
                "and test.two = template.two").close();
        checkEstimatedRowCount(conn,160400.25000000003);
        rollback();
    } 
    
    public void testTwoWayJoins() throws SQLException {
        setAutoCommit(false);
        Connection conn = getConnection();
        Statement s = createStatement();
        
        // throw in aditional predicates
        // see that the optimizer does the right thing
        
        // index on template_102. join on hundred, constant predicate on two. 
        // should be able to use statistics for hundred_two to com up with
        // row estimate.
        
        // selectivity should be 0.01 * 0.5 = 0.005
        // row count is 16*10^6 * 0.005 = 8*10^4.
        
        // join on hundred. constant pred on two. NL, index=null, all rows.
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TEMPLATE',NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TEST',NULL)");
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=null \n" +
                "where test.hundred = template.hundred " +
                "and 1 = template.two").close();
        checkEstimatedRowCount(conn,80200.125);
        
        // just retry above query with different access paths
        // row count shouldn't change!
        // join on hundred. constant pred on two. 
        // NL, index=template_102, all rows.
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_102 \n" +
                "where test.hundred = template.hundred " +
                "and 1 = template.two").close();
        checkEstimatedRowCount(conn,80200.125);
        RuntimeStatisticsParser rtsp = 
                SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_102"));
        
        // hundred and twenty
        // we can use statistics for 100,2 to get selectivity for 100 and
        // twenty and twenty to get selectivity for 20
        // selectivity should 0.01 * 0.05 = 0.0005 -> 80,000
        // join on hundred. constant pred on twenty. 
        // NL, index=null, all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=null \n" +
                "where test.hundred = template.hundred " +
                "and 1 = template.twenty").close();
        checkEstimatedRowCount(conn,8020.0125);
        
        // 'join on hundred. constant pred on twenty. 
        // NL, index=template_102 all rows.
        s.executeQuery("select template.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "test, template --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                "index=template_102 \n" +
                "where test.hundred = template.hundred " +
                "and 1 = template.twenty").close();
        checkEstimatedRowCount(conn,8020.0125);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_102"));
    }
    
    public void testThreeWayJoins() throws SQLException {
        setAutoCommit(false);
        Connection conn = getConnection();
        Statement s = createStatement();
        
        // t1 x t2 yields 8000 rows.
        // x t3 yields 8*4 * 10^6 /2 = 16*10^6
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','T1',NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','T2',NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','T3',NULL)");
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, t2, t3 " +
                "where t1.hundred = t2.hundred " +
                "and t1.twenty = t2.twenty and " +
                "t2.two = t3.two").close();
        checkEstimatedRowCount(conn,1.606007503125E7);
        RuntimeStatisticsParser rtsp = 
                SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 2, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 2, 3}", 1));
        assertTrue(rtsp.findString(
                "Hash Scan ResultSet for T3 using index T3_TWO_TWENTY", 1));
        
        // t1 x t2 -> 16 * 10^4.
        // x t3    -> 32 * 10^7
        // additional pred -> 32 * 10^5
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, t2, t3 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two and " +
                "t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        
        // variations on above query; try different join strategies
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, t2, t3 --DERBY-PROPERTIES joinStrategy=hash \n" +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, t2, t3 --DERBY-PROPERTIES joinStrategy=nestedLoop \n" +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T3", "T3_HUNDRED"));
        assertTrue(rtsp.findString("Bit set of columns fetched=All", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, t2 --DERBY-PROPERTIES joinStrategy=hash \n, t3 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, t2 --DERBY-PROPERTIES joinStrategy=hash \n, t3 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        
        // duplicate predicates; this time t1.hundred=?
        // will show up twice when t1 is optimized at the end
        // selectivity should be same as above
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t2, t3, t1 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        
        // variations on above query; try different join strategies
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t3, t2, t1 --DERBY-PROPERTIES joinStrategy=hash \n" +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t3, t2, t1 --DERBY-PROPERTIES joinStrategy=nestedLoop \n" +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_HUNDRED"));
        assertTrue(rtsp.findString("Bit set of columns fetched=All", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t2, t3 --DERBY-PROPERTIES joinStrategy=nestedLoop \n, t1 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T3", "T3_HUNDRED"));
        assertTrue(rtsp.findString("Bit set of columns fetched=All", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t3, t2 --DERBY-PROPERTIES joinStrategy=hash \n, t1 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        
        // some more variations on the above theme
        // some constant predicates thrown in.
        // remember hundred is a char column
        // -- for some reason if you give the constant 
        // as a numeric argument it doesn't recognize that 
        // as a constant start/stop value for the index 
        // The error is that the types must be comparable.
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t2, t3, t1 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred " +
                "and t1.hundred='1'").close();
        checkEstimatedRowCount(conn,30458.025);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T2", "T2_HUNDRED"));
        assertTrue(rtsp.findString("Bit set of columns fetched=All", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_HUNDRED"));
        assertTrue(rtsp.findString("Bit set of columns fetched=All", 1));
        
        // we have t1.100=t2.100 and t1.100=t3.100, so 
        // t2.100=t3.100 is redundant. 
        // row count shouldn't factor in the redundant predicate.
        // row count should be 3200000.0
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t2, t3, t1 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t1.hundred = t3.hundred " +
                "and t2.hundred = t3.hundred").close();
        checkEstimatedRowCount(conn,3212015.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T3", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        
        // slightly different join predicates-- use composite stats.
        // t1 x t2            --> 16 * 10.4.
        //         x t3       --> 16 * 10.4 * 4000 * 1/40 = 16*10.6
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t2, t3, t1 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t2.twenty = t3.twenty").close();
        checkEstimatedRowCount(conn,1.606007503125E7);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 2, 3}", 1));
        assertTrue(rtsp.findString(
                "Hash Scan ResultSet for T3 using index T3_TWO_TWENTY", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 1}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        
        // same as above but muck around with join order.
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, t2, t3 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t2.twenty = t3.twenty").close();
        checkEstimatedRowCount(conn,1.606007503125E7);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 2, 3}", 1));
        assertTrue(rtsp.findString(
                "Hash Scan ResultSet for T3 using index T3_TWO_TWENTY", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 1}", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t2, t1, t3 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t2.twenty = t3.twenty").close();
        checkEstimatedRowCount(conn,1.606007503125E7);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 2, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.findString(
                "Hash Scan ResultSet for T3 using index T3_TWO_TWENTY", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 1}", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, t3, t2 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t2.twenty = t3.twenty").close();
        checkEstimatedRowCount(conn,1.606007503125E7);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T3", "T3_TWO_TWENTY"));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 1}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 2, 3}", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t3, t2, t1 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t2.twenty = t3.twenty").close();
        checkEstimatedRowCount(conn,1.606007503125E7);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T3", "T3_TWO_TWENTY"));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 1}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 2, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t3, t1, t2 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t2.twenty = t3.twenty").close();
        checkEstimatedRowCount(conn,1.606007503125E7);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T3", "T3_TWO_TWENTY"));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 1}", 1));
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 2, 3}", 1));
        
        // and just for fun, what would we have gotten without statistics.
        s.executeQuery("select t1.id from " +
                "--DERBY-PROPERTIES useStatistics=false, joinOrder=fixed \n" + 
                "t3, t1, t2 " +
                "where t1.hundred = t2.hundred " +
                "and t2.two = t3.two " +
                "and t2.twenty = t3.twenty").close();
        checkEstimatedRowCount(conn,6.4240300125000015E7);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T3", "T3_TWO_TWENTY"));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 1}", 1));
        assertTrue(rtsp.findString("Table Scan ResultSet for T1", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={0, 3}", 1));
        assertTrue(rtsp.findString("Hash Scan ResultSet for T2", 1));
        assertTrue(rtsp.findString("Bit set of columns fetched={1, 2, 3}", 1));
    }
    
    public void testScratch() throws SQLException {
        // make sure we do a good job of stats on 1/3
        setAutoCommit(false);
        Connection conn = getConnection();
        Statement s = createStatement();
        
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','T1',NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','SCRATCH_TABLE',NULL)");
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        // Note: The original test did the first query *after* the following:
        // since the statistics (rowEstimates) are not precise, force a 
        // checkpoint to force out all the row counts to the container header,
        // and for good measure do a count which will update the row counts 
        // exactly.
        // s.executeUpdate("CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()");
        // But if that's executed, the estimatedRowCount becomes: 2582648.45
        // Without the checkpoint, the following select counts are unnecessary.
        // assertTableRowCount("T1", 4000);
        // assertTableRowCount("SCRATCH_TABLE", 4000);
        
        // preds are on columns 1 and 3
        // should use default stats for 100 (0.1) and 0.5 for two
        
        // 16*10.6 * 5*10.-2 = 80*10.4
        
        s.executeQuery("select s.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, scratch_table s " +
                "where t1.two = s.two " +
                "and s.hundred = CAST(CHAR(t1.hundred) AS INTEGER)").close();
        checkEstimatedRowCount(conn,802001.25);
        RuntimeStatisticsParser rtsp = 
                SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        
        // preds are on column 2.
        // 0.1 -> 16*10.5
        s.executeQuery("select s.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, scratch_table s " +
                "where t1.twenty = s.twenty").close();
        checkEstimatedRowCount(conn,1604002.5);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_TWO_TWENTY"));
        assertTrue(rtsp.usedHashJoin());
        
        // preds are on column 2,3.
        // 0.01 -> 16*10.4
        s.executeQuery("select s.id from " +
                "--DERBY-PROPERTIES joinOrder=fixed \n" + 
                "t1, scratch_table s " +
                "where t1.twenty = s.twenty " +
                "and s.hundred = CAST(CHAR(t1.hundred) AS INTEGER)").close();
        checkEstimatedRowCount(conn,160400.2500000);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("T1", "T1_TWENTY_HUNDRED"));
        assertTrue(rtsp.usedHashJoin());
    }
    
    public void testStatMatcher() throws SQLException {
        // test of statistics matcher algorithm; make sure that we choose the
        // best statistics (the weight stuff in predicatelist)
        
        setAutoCommit(false);
        Connection conn = getConnection();
        Statement s = createStatement();
        
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','T1',NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','T2',NULL)");
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        // 2,20,100
        s.executeQuery("select t1.id from t1, t2 " +
                "where t1.two = t2.two " +
                "and t1.twenty = t2.twenty " +
                "and t1.hundred = t2.hundred").close();
        checkEstimatedRowCount(conn,4010.00625);
        RuntimeStatisticsParser rtsp = 
                SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        
        // now muck around with the order of the predicates
        // 2,100,20
        s.executeQuery("select t1.id from t1, t2 " +
                "where t1.two = t2.two " +
                "and t1.hundred = t2.hundred " +
                "and t1.twenty = t2.twenty").close();
        checkEstimatedRowCount(conn,4010.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        
        // 100,20,2
        s.executeQuery("select t1.id from t1, t2 " +
                "where t1.hundred = t2.hundred " +
                "and t1.twenty = t2.twenty " +
                "and t1.two = t2.two").close();
        checkEstimatedRowCount(conn,4010.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        
        // 100,2,20
        s.executeQuery("select t1.id from t1, t2 " +
                "where t1.hundred = t2.hundred " +
                "and t1.two = t2.two " +
                "and t1.twenty = t2.twenty").close();
        checkEstimatedRowCount(conn,4010.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        
        s.executeQuery("select t1.id from t1, t2 " +
                "where t1.twenty = t2.twenty " +
                "and t1.hundred = t2.hundred " +
                "and t1.two = t2.two").close();
        checkEstimatedRowCount(conn,4010.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
        
        s.executeQuery("select t1.id from t1, t2 " +
                "where t1.twenty = t2.twenty " +
                "and t1.two = t2.two " +
                "and t1.hundred = t2.hundred").close();
        checkEstimatedRowCount(conn,4010.00625);
        rtsp = SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedHashJoin());
    }
    
    // Beetle was the bug system for Cloudscape, the forerunner
    // of Derby. The bug report described a query that was hitting an Error:
    // XJ001: Java exception: '2 >=2: java.lang.ArrayIndexOutOfBoundsException
    // on a specific query; when running the same query with DERBY-PROPERTIES
    // useStatistics=false the same query worked correctly.
    // The fix is in org.apache.derby.impl.sql.compile.PredicateList
    // referencing beetle 4321.
    public void testBeetle4321() throws SQLException {
        // test of statistics matcher algorithm; make sure that we choose the
        // best statistics (the weight stuff in predicatelist)
        
        setAutoCommit(false);
        Connection conn = getConnection();
        Statement s = createStatement();
        
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','COMPLEX',NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TEMPLATE',NULL)");
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        s.executeQuery("select t10.two from complex t10, template t20 " +
                "where t10.two = 1 " +
                "and t10.hundred = 2 " +
                "and t10.a = 2 " +
                "and t10.b = 2").close();
        checkEstimatedRowCount(conn,7945.920000000);
        RuntimeStatisticsParser rtsp = 
                SQLUtilities.getRuntimeStatisticsParser(s);
        assertTrue(rtsp.usedSpecificIndexForIndexScan("COMPLEX", "COMPLEXIND"));
        assertTrue(rtsp.usedSpecificIndexForIndexScan("TEMPLATE", "TEMPLATE_TWO"));
    }
    
    public void testBasic() throws SQLException {
        // basic test for update statistics; make sure that statistics with
        // correct values are created and dropped and such.
        setAutoCommit(false);
        Statement s = createStatement();
        
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        // first on int, multi-column
        s.executeUpdate("create table tbasic1 " +
                "(c1 int generated always as identity, c2 int, c3 int)");
        for (int i=1; i<5 ; i++)
        {
            for (int j=1 ; j<3 ; j++)
            {
                for (int c=0; c<2 ; c++)
                    s.executeUpdate(
                        "insert into tbasic1 values " +
                        "(default, " + i + ", " + j + ")");
            }
        }
        
        // create index should automatically create stats. 
        s.executeUpdate("create index t1_c1c2 on tbasic1 (c1, c2)");
        ResultSet statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'T1_C1C2%' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"T1_C1C2","numunique= 16 numrows= 16","1"},
                {"T1_C1C2","numunique= 16 numrows= 16","2"}});
        // index dropped stats should be dropped.
        s.executeUpdate("drop index t1_c1c2");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'T1_C1C2%' order by indexname");
        JDBC.assertEmpty(statsrs);
        
        // second part of the test.
        // check a few extra types.
        s.executeUpdate("create table tbasic2 " +
                "(i int not null, " +
                "vc varchar(32) not null, " +
                "dt date, ch char(20), " +
                "constraint pk primary key (i, vc))");
        s.executeUpdate("create index tbasic2_i on tbasic2(i)");
        s.executeUpdate("create index tbasic2_ch_dt on tbasic2(ch, dt)");
        s.executeUpdate("create index tbasic2_dt_vc on tbasic2(dt, vc)");
        // do normal inserts. 
        s.executeUpdate(
                "insert into tbasic2 values (1, 'one', '2001-01-01', 'one')");
        s.executeUpdate(
                "insert into tbasic2 values (2, 'two', '2001-01-02', 'two')");
        s.executeUpdate(
                "insert into tbasic2 values (3, 'three', '2001-01-03', 'three')");
        s.executeUpdate(
                "insert into tbasic2 values (1, 'two', '2001-01-02', 'one')");
        s.executeUpdate(
                "insert into tbasic2 values (1, 'three', '2001-01-03', 'one')");
        s.executeUpdate(
                "insert into tbasic2 values (2, 'one', '2001-01-01', 'two')");
        
        // figure out the name of the primary key's backing index
        statsrs = s.executeQuery(
                "select conglomeratename from sys.sysconglomerates " +
                "where conglomeratename like 'SQL%'");
        statsrs.next();
        String backIndName = statsrs.getString("conglomeratename");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TBASIC2','" + backIndName + "')");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'SQL%' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {backIndName,"numunique= 3 numrows= 6","1"},
                {backIndName,"numunique= 6 numrows= 6","2"}});
        
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TBASIC2','TBASIC2_I')");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname = 'TBASIC2_I' order by indexname");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC2_I","numunique= 3 numrows= 6","1"}});
        
        // do another insert then just updstat for whole table.
        s.executeUpdate(
                "insert into tbasic2 values(2, 'three', '2001-01-03', 'two')");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TBASIC2',null)");
        
        // make sure that stats are correct.
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname = 'TBASIC2_I' order by indexname");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC2_I","numunique= 3 numrows= 7","1"}});
        statsrs = s.executeQuery(
                "select count(*) from (select distinct i from tbasic2) t");
        JDBC.assertFullResultSet(statsrs, new String[][] {{"3"}});
        
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname = 'TBASIC2_CH_DT' order by indexname, stats");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC2_CH_DT","numunique= 3 numrows= 7","1"},
                {"TBASIC2_CH_DT","numunique= 7 numrows= 7","2"}});
        
        statsrs = s.executeQuery(
                "select count(*) from (select distinct ch from tbasic2) t");
        JDBC.assertFullResultSet(statsrs, new String[][] {{"3"}});
        statsrs = s.executeQuery(
                "select count(*) from (select distinct ch, dt from tbasic2) t");
        JDBC.assertFullResultSet(statsrs, new String[][] {{"7"}});
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname = 'TBASIC2_DT_VC' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC2_DT_VC","numunique= 3 numrows= 7","1"},
                {"TBASIC2_DT_VC","numunique= 3 numrows= 7","2"}});
        statsrs = s.executeQuery(
                "select count(*) from (select distinct dt from tbasic2) t");
        JDBC.assertFullResultSet(statsrs, new String[][] {{"3"}});
        statsrs = s.executeQuery(
                "select count(*) from (select distinct dt, vc from tbasic2) t");
        JDBC.assertFullResultSet(statsrs, new String[][] {{"3"}});
        statsrs = s.executeQuery(
                "select stats, ncols from showstats " +
                "where indexname like 'SQL%' order by stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"numunique= 3 numrows= 7","1"},
                {"numunique= 7 numrows= 7","2"}});
        
        // delete everything from t2, do bulkinsert see what happens.
        assertUpdateCount(s, 7, "delete from tbasic2");
        
        // no material impact on stats
        // note; the test didn't actually confirm, here's the expected now
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'TBASIC2%' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC2_CH_DT","numunique= 3 numrows= 7","1"},
                {"TBASIC2_CH_DT","numunique= 7 numrows= 7","2"},
                {"TBASIC2_DT_VC","numunique= 3 numrows= 7","1"},
                {"TBASIC2_DT_VC","numunique= 3 numrows= 7","2"},
                {"TBASIC2_I","numunique= 3 numrows= 7","1"}});
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TBASIC2',null)");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'TBASIC2%' " +
                "order by indexname, stats, ncols");
        JDBC.assertEmpty(statsrs);
        
        // Note: the original (Cloudscape) test did a 'bulkinsert'.
        // this is now only possible internally, and is used in
        // the SYSCS_IMPORT_DATA system procedures.
        // Possibly this test can be added onto by moving the
        // value intended to be inserted into a data file, and calling
        // SYSCS_UTIL.SYSCS_IMPORT_DATA using that.
        // The row inserted was:
        //        "values (2, 'one', '2001-01-01', 'two')");
        // subsequently, there was a bulk insert-replace, this is
        // no longer supported. We could update the row.
        // the replacement was of row:
        //        "(2, 'one', '2001-01-01', 'two'), " +
        // by
        //        "(1, 'one', '2001-01-01', 'two')");
        s.executeUpdate("drop table tbasic2");
        
        // various alter table operations to ensure correctness.
        // 1. add and drop constraint.
        s.executeUpdate("create table tbasic3 " +
                "(x int not null generated always as identity," +
                " y int not null, z int)");
        s.executeUpdate(
                "insert into tbasic3 (y,z) values " +
                "(1,1),(1,2),(1,3),(1,null),(2,1),(2,2),(2,3),(2,null)");
        // first alter table to add primary key;
        s.executeUpdate("alter table tbasic3 " +
                "add constraint pk_tbasic3 primary key (x,y)");
        statsrs = s.executeQuery(
                "select conglomeratename from sys.sysconglomerates " +
                "where conglomeratename like 'SQL%'");
        statsrs.next();
        backIndName = statsrs.getString("conglomeratename");
        statsrs = s.executeQuery(
                "select stats, ncols from showstats " +
                "where indexname like '" + backIndName + "' " +
                "order by stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"numunique= 8 numrows= 8","1"},
                {"numunique= 8 numrows= 8","2"}});
        // now drop the constraint
        s.executeUpdate("alter table tbasic3 drop constraint pk_tbasic3");
        statsrs = s.executeQuery(
                "select stats, ncols from showstats " +
                "where indexname like '" + backIndName + "' " +
                "order by stats, ncols");
        JDBC.assertEmpty(statsrs);
        
        // try compress with tons of rows. you can never tell 
        // what a few extra pages can do :)
        for (int i=0; i<9 ; i++)
            s.executeUpdate("insert into tbasic3(y,z) select y,z from tbasic3");
        statsrs = s.executeQuery("select count(*) from tbasic3");
        JDBC.assertFullResultSet(statsrs, new String[][] {{"4096"}});
        s.executeUpdate("create index tbasic3_xy on tbasic3(x,y)");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'TBASIC3_XY%' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC3_XY","numunique= 4096 numrows= 4096","1"},
                {"TBASIC3_XY","numunique= 4096 numrows= 4096","2"}});
        s.executeUpdate("delete from tbasic3 where z is null");
        s.executeUpdate("call SYSCS_UTIL.SYSCS_COMPRESS_TABLE" +
                "('APP', 'TBASIC3', 0)");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'TBASIC3_XY%' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC3_XY","numunique= 3072 numrows= 3072","1"},
                {"TBASIC3_XY","numunique= 3072 numrows= 3072","2"}});
        s.executeUpdate("drop table tbasic3");
        
        s.executeUpdate("create table tbasic4 " +
                "(x int, y int, z int)");
        s.executeUpdate("insert into tbasic4 values (1,1,1)");
        s.executeUpdate("insert into tbasic4 values (1,2,1)");
        s.executeUpdate("insert into tbasic4 values (1,1,2)");
        
        s.executeUpdate("create index tbasic4_x on tbasic4(x)");
        s.executeUpdate("create index tbasic4_xy on tbasic4(x,y)");
        s.executeUpdate("create index tbasic4_yz on tbasic4(y,z)");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'TBASIC4%' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC4_X","numunique= 1 numrows= 3","1"},
                {"TBASIC4_XY","numunique= 1 numrows= 3","1"},
                {"TBASIC4_XY","numunique= 2 numrows= 3","2"},
                {"TBASIC4_YZ","numunique= 2 numrows= 3","1"},
                {"TBASIC4_YZ","numunique= 3 numrows= 3","2"}});

        // if we drop column x, then stats for tbasic4_x should get dropped
        // index tbasic4_xy should get rebuilt to only be on y. so one of the
        // stats should be recreated. and tbasic4_yz shouldn remain in its
        // entirety.
        s.executeUpdate("alter table tbasic4 drop column x");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'TBASIC4%' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC4_XY","numunique= 2 numrows= 3","1"},
                {"TBASIC4_YZ","numunique= 2 numrows= 3","1"},
                {"TBASIC4_YZ","numunique= 3 numrows= 3","2"}});
        s.executeUpdate("drop table tbasic4");
        
        // test re tbasic5 were intended to exercise Cloudscape's
        // stored prepared statements. This is not supported in Derby
        // it also does some drop statistics, but there are already 
        // other tests that do this. So on to tbasic6.
        
        s.executeUpdate("create table tbasic6 " +
                "(i int generated always as identity," +
                " j varchar(10))");
        s.executeUpdate("create index tbasic6_i on tbasic6(i)");
        s.executeUpdate("create index tbasic6_j on tbasic6(j)");
        s.executeUpdate("create index tbasic6_ji on tbasic6(j,i)");
        char[] alphabet = {'a','b','c','d','e','f','g','h','i'};
        for (int i=0; i<alphabet.length-1 ; i++)
            s.executeUpdate("insert into tbasic6 " +
                    "values (default, '" + alphabet[i] + "')");
        for (int i=0; i<alphabet.length ; i++)
            s.executeUpdate("insert into tbasic6 " +
                    "values (default, '" + alphabet[i] + "')");
        for (int i=0; i<alphabet.length-1 ; i++)
            s.executeUpdate("insert into tbasic6 " +
                    "values (default, '" + alphabet[i] + "')");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TBASIC6','TBASIC6_J')");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TBASIC6',NULL)");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'TBASIC6%' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC6_I","numunique= 25 numrows= 25","1"},
                {"TBASIC6_J","numunique= 9 numrows= 25","1"},
                {"TBASIC6_JI","numunique= 25 numrows= 25","2"},
                {"TBASIC6_JI","numunique= 9 numrows= 25","1"}});

        s.executeUpdate("delete from TBASIC6");
        // make the 17th row the same as the 16th;
        // make sure when we switch to the next group fetch
        // we handle the case correctly.
        for (int i=0; i<17 ; i++)
            s.executeUpdate("insert into tbasic6 values (default, 'a')");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','TBASIC6',NULL)");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'TBASIC6%' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"TBASIC6_I","numunique= 17 numrows= 17","1"},
                {"TBASIC6_J","numunique= 1 numrows= 17","1"},
                {"TBASIC6_JI","numunique= 1 numrows= 17","1"},
                {"TBASIC6_JI","numunique= 17 numrows= 17","2"}});
        s.executeUpdate("drop table tbasic6");
        
        // table with no rows.
        s.executeUpdate("create table et (x int, y int)");
        s.executeUpdate("create index etx on et(x)");
        s.executeUpdate("create index ety on et(y)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','ET','ETX')");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','ET',NULL)");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname like 'ET%' " +
                "order by indexname, stats, ncols");
        JDBC.assertEmpty(statsrs);
        s.executeUpdate("drop table et");
        
        // tests for nulls.
        s.executeUpdate("create table null_table (x int, y varchar(2))");
        s.executeUpdate("create index nt_x on null_table(x desc)");
        for (int i=1; i<4 ; i++)
            s.executeUpdate("insert into null_table " +
                    "values (" + i + ", '" + alphabet[i-1] + "')");
        for (int c=0; c<2 ; c++)
        {
            for (int i=1; i<4 ; i++)
                s.executeUpdate("insert into null_table " +
                        "values (null, '" + alphabet[i-1] + "')");
        }
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                "('APP','NULL_TABLE',NULL)");
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname = 'NT_X' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"NT_X","numunique= 9 numrows= 9","1"}});
        // try composite null keys (1,null) is unique from (1,null)
        // as is (null,1) from (null,1)
        s.executeUpdate("drop index nt_x");
        s.executeUpdate("create index nt_yx on null_table(y,x)");
        // the first key y has 3 unique values.
        // the second key y,x has 9 unique values because of nulls.
        statsrs = s.executeQuery(
                "select indexname, stats, ncols from showstats " +
                "where indexname = 'NT_YX' " +
                "order by indexname, stats, ncols");
        JDBC.assertFullResultSet(statsrs, new String[][] {
                {"NT_YX","numunique= 3 numrows= 9","1"},
                {"NT_YX","numunique= 9 numrows= 9","2"}});
    }
    
    // drop any tables created during testBasic
    protected void tearDown() throws Exception {
        Statement s = createStatement();
        try {
            s.execute("drop table tbasic1");
            s.execute("drop table tbasic2");
            s.execute("drop table tbasic3");
            s.execute("drop table tbasic4");
            s.execute("drop table tbasic6");
            s.execute("drop table et");
            s.execute("drop table null_table");
        } catch (SQLException sqle) {
            // if it doesn't work, never mind, we'll assume the
            // cleanDatabaseSetup will deal with it.
        }
        s.close();
        super.tearDown();
    }
}
