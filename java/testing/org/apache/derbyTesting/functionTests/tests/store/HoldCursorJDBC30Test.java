/*
 * 
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.HoldCursorJDBC30Test
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 */

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public final class HoldCursorJDBC30Test extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public HoldCursorJDBC30Test(String name) {
        super(name);
    }

    /**
     * This will allow us to decorate the test for each driver separately
     */
    private static Test decorateTest(Test test) {
        return new CleanDatabaseTestSetup(test){
            protected void decorateSQL(Statement s)
            throws SQLException {
                /* getConnection().setAutoCommit(false); */

                /* testBasicHeapScanZeroRows */
                s.execute("create table foo1 (a int, data int)");

                /* testBasicHeapScanMultiRows */
                s.execute("create table foo2 (a int, data int)");

                /* testBasicBtreeScanForZeroRowsUpdateNonkeyfield */
                s.execute("create table foo3 (a int, data int)");
                s.execute("create index foox_3 on foo3 (a)");

                /* testBasicBtreeScanTestsForMultipleRowsOrUpdateNonkeyField */
                s.execute("create table foo4 (a int, data int)");

                /* testBasicBtreeScanForZeroRowsReadOnlyNoGroupFetch */
                s.execute("create table foo5 (a int, data int)");
                s.execute("create index foox_5 on foo5 (a)");

                /* testBasicBtreeScanTestsForMultipleRowsOrReadOnly */
                s.execute("create table foo6 (a int, data int)");

                /* testOrder */
                s.execute("create table foo7 (a int, data int)");
                s.execute("create index foox_7 on foo7 (a)");

                /* testDistinctScalarAggregateResultSet */
                s.execute("create table t1_8 (c1 int, c2 int)");

                /* testDistinctScalarAggregateResultSetGrouped */
                s.execute("create table t1_9 (c1 int, c2 int)");

                /* testPositionPurgedRow */
                s.execute("create table t1_10 (c1 int, c2 int)");
                s.execute("create index tx_10 on t1_10 (c1)");

                /* testPositionPurgedPage - Index created on test here */
                s.execute("create table t1_11 (c1 varchar(1000), c2 int)");

                /* testBeetle4902 */
                s.execute("create table t1_12 (t1_i1 int, t1_i2 int)");
                s.execute("create table t2_12 (t2_i1 int, t2_i2 int)");
                s.execute("create index t1_idx_12 on t1_12 (t1_i1)");
                s.execute("create index t2_idx_12 on t2_12 (t2_i1)");

                /* testBeetle4902WithBulkFetchDefaultSet */
                s.execute("create table t1_13 (t1_i1 int, t1_i2 int)");
                s.execute("create table t2_13 (t2_i1 int, t2_i2 int)");
                s.execute("create index t1_idx_13 on t1_13 (t1_i1)");
                s.execute("create index t2_idx_13 on t2_13 (t2_i1)");
            }
        };
    }

    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("HoldCursorJDBC30Test");
        suite.addTest(decorateTest(TestConfiguration.embeddedSuite(HoldCursorJDBC30Test.class)));
        suite.addTest(decorateTest(TestConfiguration.clientServerSuite(HoldCursorJDBC30Test.class)));
        
        return suite;
    }
       
    /**
     * Disabling auto-commit for all tests
     * @throws java.sql.SQLException
     */
    protected void setUp() throws SQLException {
        setAutoCommit(false);
        createStatement().execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                             "'derby.language.bulkFetchDefault', '1')");
    }

    /**
     * The following tests that no matter where commit comes in the state of
     * the scan that the scan will continue after the commit.  Tests various
     * states of scan like: before first next, after first next, before close,
     * after close.
     */
    public void testBasicHeapScanZeroRows() throws SQLException{ 
        Statement stUtil = createStatement();
                
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        ResultSet test1 = st.executeQuery("select * from foo1");
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
       
        test1 = st.executeQuery("select * from foo1 for update");
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
         
        test1 = st.executeQuery("select * from foo1 for update");
        commit();
        assertFalse(test1.next());
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo1 for update");
        assertFalse(test1.next());
        commit();     
        assertFalse(test1.next());
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo1 for update");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo1 for update");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo1 for update");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo1");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        commit();
        
        st.close();
        stUtil.close();
    }
    
    /**
     * The following tests that no matter where commit comes in the state of 
     * the scan that the scan will continue after the commit. 
     * Tests various states of scan like: before first next, 
     * after first next, before close,after close.
     */
    public void testBasicHeapScanMultiRows() throws SQLException{ 
        Statement stUtil = createStatement();

        stUtil.addBatch("insert into foo2 values (1, 10)");
        stUtil.addBatch("insert into foo2 values (1, 20)");
        stUtil.addBatch("insert into foo2 values (1, 30)");
        stUtil.addBatch("insert into foo2 values (1, 40)");
        stUtil.addBatch("insert into foo2 values (1, 50)");
        stUtil.executeBatch();

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);
        
        /*the following for update cursors will all use group fetch = 1,
         thus each next passes straight through to store. */
        ResultSet test1 = st.executeQuery("select * from foo2 for update");
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo2 for update");
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
         
        test1 = st.executeQuery("select * from foo2 for update");
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo2 for update");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo2 for update");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo2 for update");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo2 for update");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        /* beyond the total num. */
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        

        test1 = st.executeQuery("select * from foo2 for update");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        commit();
        
        st.close();
        stUtil.close();
    }   
    
    /**
     *  basic btree scan tests (zero rows/update nonkey field)
     *  The following tests that no matter where commit comes in the state of
     *  the scan that the scan will continue after the commit.  Tests various
     *  states of scan like: before first next, after first next, before close,
     *  after close.
     */
    public void testBasicBtreeScanForZeroRowsUpdateNonkeyfield() throws SQLException{ 
        Statement stUtil = createStatement();

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);
        
        /* the following for update cursors will all use group fetch = 1,
           thus each next passes straight through to store. */
        ResultSet test1 = st.executeQuery("select * from foo3 for update of data");
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo3 for update of data");
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
         
        test1 = st.executeQuery("select * from foo3 for update of data");
        commit();
        assertFalse(test1.next());
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo3 for update of data");
        assertFalse(test1.next());
        commit();     
        assertFalse(test1.next());
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo3 for update of data");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo3 for update of data");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo3 for update of data");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo3 for update of data");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        commit();
	
        st.close();
        stUtil.close();
    }
    
    /**
     * The following tests that no matter where commit comes in the state of
     * the scan that the scan will continue after the commit.  Tests various
     * states of scan like: before first next, after first next, before close,
     * after close.
     * @throws SQLException 
     */
    public void testBasicBtreeScanTestsForMultipleRowsOrUpdateNonkeyField()
    throws SQLException{ 
        Statement stUtil = createStatement();

        stUtil.addBatch("insert into foo4 values (1, 10)");
        stUtil.addBatch("insert into foo4 values (1, 20)");
        stUtil.addBatch("insert into foo4 values (1, 30)");
        stUtil.addBatch("insert into foo4 values (1, 40)");
        stUtil.addBatch("insert into foo4 values (1, 50)");
        stUtil.executeBatch();

        /* the following for update of data cursors
           will all use group fetch = 1, thus each
           next passes straight through to store */
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        /* the following for update cursors will all use group fetch = 1,
           thus each next passes straight through to store. */
        ResultSet test1 = st.executeQuery("select * from foo4 for update of data");
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo4 for update of data");
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
         
        test1 = st.executeQuery("select * from foo4 for update of data");
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo4 for update of data");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo4 for update of data");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo4 for update of data");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo4 for update of data");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        /* beyond the total num. */
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }

        test1 = st.executeQuery("select * from foo4 for update of data");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        /* test negative case of trying non next operations after commit */
        test1 = st.executeQuery("select * from foo4 for update of data");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        try {
            test1.deleteRow();
            fail("Should be unable to delete row!");
        } catch (SQLException e) { assertSQLState("XJ083", e); }
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        try {
            test1.updateInt("data", -3000);
            fail("Should be unable to update row!");
        } catch (SQLException e) { assertSQLState("XJ083", e); }
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }

        /* This is a workaround for DERBY-4184 */
        st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_UPDATABLE,
                             ResultSet.HOLD_CURSORS_OVER_COMMIT);
        /* test positive case of trying delete/update after commit and next. */

        /* ATTENTION! Add the following ' for update of data' to the query
         * once DERBY-4198 is fixed! */
        test1 = st.executeQuery("select * from foo4");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        test1.deleteRow();
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        test1.updateInt("data", -3000);
        test1.updateRow();
        
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        /* make sure above deletes/updates worked. */
        test1 = st.executeQuery("select * from foo4 for update of data");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(-3000,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        assertFalse(test1.next());
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        commit();
        
        st.close();	
        stUtil.close();
    }    
    
    /**
     * The following tests that no matter where commit comes in the state of
     * the scan that the scan will continue after the commit.  Tests various
     * states of scan like: before first next, after first next, before close,
     * after close.
     */
    public void testBasicBtreeScanForZeroRowsReadOnlyNoGroupFetch()
    throws SQLException{ 
        Statement stUtil = createStatement();

        /* the following for read cursors will all use group fetch = 1,
           thus each next passes straight through to store.
           This select should only use the
           index with no interaction with the base table. */
        Statement st = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        /* the following for update cursors will all use group fetch = 1,
           thus each next passes straight through to store. */
        ResultSet test1 = st.executeQuery("select * from foo5");
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo5");
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
         
        test1 = st.executeQuery("select * from foo5");
        commit();
        assertFalse(test1.next());
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo5");
        assertFalse(test1.next());
        commit();     
        assertFalse(test1.next());
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo5");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo5");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo5");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo5");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        commit();
	
        st.close();
        stUtil.close();
    }
    
    /**
     * basic btree scan tests (multiple rows/read only/no group fetch)
     * The following tests that no matter where commit comes in the state of
     * the scan that the scan will continue after the commit.  Tests various
     * states of scan like: before first next, after first next, before close,
     * after close.
     */
    public void testBasicBtreeScanTestsForMultipleRowsOrReadOnly()
    throws SQLException{ 
        Statement stUtil = createStatement();

        stUtil.addBatch("insert into foo6 values (1, 10)");
        stUtil.addBatch("insert into foo6 values (1, 20)");
        stUtil.addBatch("insert into foo6 values (1, 30)");
        stUtil.addBatch("insert into foo6 values (1, 40)");
        stUtil.addBatch("insert into foo6 values (1, 50)");
        stUtil.executeBatch();

        /* the following for read cursors will all use group fetch = 1,
           thus each next passes straight through to store.
           This select should only use the
           index with no interaction with the base table */
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        /* the following for update cursors will all use group fetch = 1,
           thus each next passes straight through to store. */
        ResultSet test1 = st.executeQuery("select * from foo6");
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo6");
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
         
        test1 = st.executeQuery("select * from foo6");
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo6");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo6");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo6");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select * from foo6");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        /* beyond the total num. */
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }

        test1 = st.executeQuery("select * from foo6");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        commit();
        
        st.close();	
        stUtil.close();
    }
    
    /**
     * basic tests for cursors with order by 
     * The following tests that no matter where commit comes in the state of
     * the scan that the scan will continue after the commit.  Tests various
     * states of scan like: before first next, after first next, before close,
     * after close.
     */
    public void testOrder() throws SQLException{ 
        Statement stUtil = createStatement();

        /* the following for read cursors will all use group fetch = 1,
           thus each next passes straight through to store.
           This select should only use the
           index with no interaction with the base table. */
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);
        
        /* the following for update cursors will all use group fetch = 1,
           thus each next passes straight through to store. */
        ResultSet test1 = st.executeQuery("select a,data from foo7 order by data desc");
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
         
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        commit();
        assertFalse(test1.next());
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertFalse(test1.next());
        commit();     
        assertFalse(test1.next());
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        commit();
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        commit();
        
        stUtil.addBatch("insert into foo7 values (1, 10)");
        stUtil.addBatch("insert into foo7 values (1, 20)");
        stUtil.addBatch("insert into foo7 values (1, 30)");
        stUtil.addBatch("insert into foo7 values (1, 40)");
        stUtil.addBatch("insert into foo7 values (1, 50)");
        stUtil.executeBatch();

        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
         
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        /* beyond the total num. */
        assertFalse(test1.next());
        commit();
        test1.close();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }

        test1 = st.executeQuery("select a,data from foo7 order by data desc");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(50,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(40,test1.getInt("data"));
        commit();
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(30,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(20,test1.getInt("data"));
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("a"));
        assertEquals(10,test1.getInt("data"));
        assertFalse(test1.next());
        test1.close();
        commit();
        try {
            test1.next();
            fail("Should be unable to establish cursor!");
        } catch (SQLException e) { assertSQLState("XCL16", e); }
        
        commit();
    
        st.close();
        stUtil.close();
    }
    
    /**
     *  test of hold cursor code in DistinctScalarAggregateResultSet.java
     *  Directed test of hold cursor as applies to sort scans opened by
     *  DistinctScalarAggregateResultSet.java.
     */
    public void testDistinctScalarAggregateResultSet() throws SQLException{ 
        Statement stUtil = createStatement();

        stUtil.executeUpdate("insert into t1_8 values (null, null), (1,1), " +
                             "(null, null), (2,1), (3,1), (10,10)");
        JDBC.assertFullResultSet(stUtil.executeQuery("select * from t1_8"),
            new String[][]{
                    {null, null}, {"1", "1"}, {null, null},
                    {"2", "1"}, {"3", "1"}, {"10", "10"},
        });

        JDBC.assertFullResultSet(
            stUtil.executeQuery("select sum(distinct c1) from t1_8"),
            new String[][]{{"16"},}
        );

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        ResultSet test1 = st.executeQuery("select sum(distinct c1) from t1_8");
        commit();
        assertTrue(test1.next());
        assertEquals(16,test1.getInt(1));
        test1.close();

        test1 = st.executeQuery("select sum(distinct c1) from t1_8");
        assertTrue(test1.next());
        assertEquals(16,test1.getInt(1));
        commit();
        assertFalse(test1.next());
        commit();
        test1.close();

        commit();

        st.close();
        stUtil.close();
    }  
    
    /**
     * test of hold cursor code in GroupedAggregateResultSet.java
     * Directed test of hold cursor as applies to sort scans opened by
     * GroupedAggregateResultSet.java.
     */
    public void testDistinctScalarAggregateResultSetGrouped()
    throws SQLException{ 
        Statement stUtil = createStatement();

        stUtil.executeUpdate("insert into t1_9 values (null, null), (1,1), " +
                "(null, null), (2,1), (3,1), (10,10)");
        JDBC.assertFullResultSet(stUtil.executeQuery("select * from t1_9"),
            new String[][]{
                    {null, null}, {"1", "1"}, {null, null},
                    {"2", "1"}, {"3", "1"}, {"10", "10"},
        });

        JDBC.assertFullResultSet(
            stUtil.executeQuery(
                "select sum(distinct c1) from t1_9 group by c2"),
            new String[][]{
                    {"6"},
                    {"10"},
                    {null},
                }
        );
        commit();

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        ResultSet test1 = st.executeQuery("select sum(distinct c1) from t1_9 group by c2");
        commit();
        assertTrue(test1.next());
        assertEquals(6,test1.getInt(1));
        assertTrue(test1.next());
        assertEquals(10,test1.getInt(1));
        commit();
        assertTrue(test1.next());
        test1.getInt(1);
        assertTrue(test1.wasNull());
        test1.close();

        test1 = st.executeQuery("select sum(distinct c1) from t1_9 group by c2");
        assertTrue(test1.next());
        assertEquals(6,test1.getInt(1));
        commit();
        assertTrue(test1.next());
        assertEquals(10,test1.getInt(1));
        commit();
        assertTrue(test1.next());
        test1.getInt(1);
        assertTrue(test1.wasNull());
        test1.close();

        commit();

        st.close();
        stUtil.close();
    }  
    
    /**
     * test scan positioned on a row which has been purged.
     */
    public void testPositionPurgedRow() throws SQLException{
        Statement stUtil = createStatement();

        stUtil.executeUpdate("insert into t1_10 values (1, 1), (2, 2), " +
                             "(3, 3), (4, 4), (5, 5), (6, 6)");

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_UPDATABLE,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        ResultSet test1 = st.executeQuery("select c1 from t1_10");
        assertTrue(test1.next());
        assertEquals(1, test1.getInt("c1"));
        commit();

        /* at this point the btree scan is positioned by "key" on (1,1).
           Make sure deleting this key doesn't cause any problems. */
        assertUpdateCount(stUtil, 2, "delete from t1_10 where c1 = 1 or c1 = 2");

        /* This IF works around a difference in behaviours between the client
         * and the embedded drivers. Check DERBY-3839 for comments on this. */
        if (usingEmbedded()){
            assertTrue(test1.next());
            assertEquals(3, test1.getInt("c1"));

            /* -- at this point the btree scan is positioned on (3, 3),
               let's see what happens if we delete (3,3) and look at current scan. */
            assertUpdateCount(stUtil, 1, "delete from t1_10 where c1 = 3");

            /* position on (4,4) */
            assertTrue(test1.next());
            assertEquals(4, test1.getInt("c1"));

            commit();

            /* delete all the rows and hopefully get all rows to be purged
               by the time the scan does the next. */
            assertUpdateCount(stUtil, 3, "delete from t1_10");
            commit();

            assertFalse(test1.next());
        } else { /* Client has a different behaviour. Maybe a bug! */
            assertTrue(test1.next());
            assertEquals(2, test1.getInt("c1"));

            /* -- at this point the btree scan is positioned on (3, 3),
               let's see what happens if we delete (3,3) and look at current scan. */
            assertUpdateCount(stUtil, 1, "delete from t1_10 where c1 = 3");

            /* position on (4,4) */
            assertTrue(test1.next());
            assertEquals(3, test1.getInt("c1"));

            commit();

            /* delete all the rows and hopefully get all rows to be purged
               by the time the scan does the next. */
            assertUpdateCount(stUtil, 3, "delete from t1_10");
            commit();

            /* The records are all still there, even though the above assert
             * passed with the correct update count */
            assertTrue(test1.next());
            assertEquals(4, test1.getInt("c1"));
            assertTrue(test1.next());
            assertEquals(5, test1.getInt("c1"));
            assertTrue(test1.next());
            assertEquals(6, test1.getInt("c1"));

            assertFalse(test1.next());
        }
        
        test1.close();
        stUtil.close();
    }
    
    /**
     * test scan positioned on a page which has been purged (should really
     * not be any different than a row being purged).
     */
    public void testPositionPurgedPage() throws SQLException{ 
        /**
         * rows[0] = 1 with 1000 padding
         * rows[1] = 2 with 1000 padding
         * ...
         */
        String[] rows = new String[7];
        for(int i = 1; i <= rows.length; i++){
            rows[i - 1] = Formatters.padString("" + i, 1000);
        }

        Statement stUtil = createStatement();

        stUtil.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', '4096')");
        stUtil.executeUpdate("create index tx_11 on t1_11 (c1)");
        stUtil.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.storage.pageSize', Null)");

        PreparedStatement ps = prepareStatement(
            "insert into t1_11 values(?,1), (?,2), (?,3), " +
            "(?,4), (?,5), (?,6), (?,7)");
        for(int i = 0; i < rows.length; i++){
            ps.setString(i + 1, rows[i]);
        }
        ps.executeUpdate();
        ps.close();

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        ResultSet test1 = st.executeQuery("select c1 from t1_11");
        assertTrue(test1.next());
        assertEquals(rows[0], test1.getString("c1"));
        commit();

        /* at this point the btree scan is positioned by "key" on (1,1).
           Make sure deleting this key doesn't cause any problems. */
        ps = prepareStatement("delete from t1_11 where c1 = ? or c1 = ?");
        ps.setString(1, rows[0]);
        ps.setString(2, rows[1]);
        assertUpdateCount(ps, 2);
        ps.close();

        /* This IF works around a difference in behaviours between the client
         * and the embedded drivers. Check DERBY-3839 for comments on this. */
        if (usingEmbedded()) {
            assertTrue(test1.next());
            assertEquals(rows[2], test1.getString("c1"));

            /* -- at this point the btree scan is positioned on (3, 3),
               let's see what happens if we delete (3,3) and look at current scan. */
            ps = prepareStatement("delete from t1_11 where c1 = ?");
            ps.setString(1, rows[2]);
            assertUpdateCount(ps, 1);
            ps.close();

            /* position on (4,4) */
            assertTrue(test1.next());
            assertEquals(rows[3], test1.getString("c1"));

            commit();

            /* delete all the rows and hopefully get all rows to be purged
               by the time the scan does the next. */
            assertUpdateCount(stUtil, 4, "delete from t1_11");
            commit();

            assertFalse(test1.next());
        } else {
            assertTrue(test1.next());
            assertEquals(rows[1], test1.getString("c1"));

            /* -- at this point the btree scan is positioned on (3, 3),
               let's see what happens if we delete (3,3) and look at current scan. */
            ps = prepareStatement("delete from t1_11 where c1 = ?");
            ps.setString(1, rows[2]);
            assertUpdateCount(ps, 1);
            ps.close();

            /* position on (4,4) */
            assertTrue(test1.next());
            assertEquals(rows[2], test1.getString("c1"));

            commit();

            /* delete all the rows and hopefully get all rows to be purged
               by the time the scan does the next. */
            assertUpdateCount(stUtil, 4, "delete from t1_11");
            commit();

            /**
             * The records are all still there, even though the above assert
             * passed with the correct update count.
             */
            assertTrue(test1.next());
            assertEquals(rows[3], test1.getString("c1"));
            assertTrue(test1.next());
            assertEquals(rows[4], test1.getString("c1"));
            assertTrue(test1.next());
            assertEquals(rows[5], test1.getString("c1"));
            assertTrue(test1.next());
            assertEquals(rows[6], test1.getString("c1"));

            assertFalse(test1.next());
        }

        test1.close();
        st.close();
        stUtil.close();
    }
    
    /**
     * test query plans which use reopenScan() on a btree to 
     * do the inner table processing of a join.  Prior to the fix a null
     * pointer exception would be thrown after the commit, as the code
     * did not handle keeping the resultset used for the inner table
     * open across commits in this case.
     */
    public void testBeetle4902() throws SQLException{ 
        Statement stUtil = createStatement();

        stUtil.addBatch("insert into t1_12 values (1, 10), (2, 20), " +
                        "(3, 30), (4, 40), (5, 50)");
        stUtil.addBatch("insert into t2_12 values (1, 10), (2, 20), " +
                        "(4, 40), (5, 50)");
        stUtil.executeBatch();
        commit();

        /* force nestedLoop to make sure reopenScan() is used on inner table. */
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);
        
        ResultSet test11 = st.executeQuery("select * from t1_12, t2_12 where t1_12.t1_i1 = t2_12.t2_i1");

        commit();
        assertTrue(test11.next());
        assertEquals(1, test11.getInt("t1_i1"));
        assertEquals(10, test11.getInt("t1_i2"));
        assertEquals(1, test11.getInt("t2_i1"));
        assertEquals(10, test11.getInt("t2_i2"));
        commit();
        assertTrue(test11.next());
        assertEquals(2, test11.getInt("t1_i1"));
        assertEquals(20, test11.getInt("t1_i2"));
        assertEquals(2, test11.getInt("t2_i1"));
        assertEquals(20, test11.getInt("t2_i2"));
        commit();
        assertTrue(test11.next());
        assertEquals(4, test11.getInt("t1_i1"));
        assertEquals(40, test11.getInt("t1_i2"));
        assertEquals(4, test11.getInt("t2_i1"));
        assertEquals(40, test11.getInt("t2_i2"));
        assertTrue(test11.next());
        assertEquals(5, test11.getInt("t1_i1"));
        assertEquals(50, test11.getInt("t1_i2"));
        assertEquals(5, test11.getInt("t2_i1"));
        assertEquals(50, test11.getInt("t2_i2"));
        commit();
        commit();
        assertFalse(test11.next());
        commit();

        test11.close();

        st.close();
        stUtil.close();
    }
    

    /**
     * test query plans which use reopenScan() on a btree to 
     * do the inner table processing of a join.  Prior to the fix a null
     * pointer exception would be thrown after the commit, as the code
     * did not handle keeping the resultset used for the inner table
     * open across commits in this case.
     */
    public void testBeetle4902WithBulkFetchDefaultSet() throws SQLException{
        Statement stUtil = createStatement();

        stUtil.addBatch("insert into t1_13 values (1, 10), (2, 20), " +
                        "(3, 30), (4, 40), (5, 50)");
        stUtil.addBatch("insert into t2_13 values (1, 10), (2, 20), " +
                        "(4, 40), (5, 50)");
        stUtil.executeBatch();
        commit();

        /* force nestedLoop to make sure reopenScan() is used on inner table. */
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);
        ResultSet test12 = st.executeQuery("select * from t1_13, t2_13 where t1_13.t1_i1 = t2_13.t2_i1");
        stUtil.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                             "'derby.language.bulkFetchDefault', '16')");

        commit();

        assertTrue(test12.next());
        assertEquals(1, test12.getInt("t1_i1"));
        assertEquals(10, test12.getInt("t1_i2"));
        assertEquals(1, test12.getInt("t2_i1"));
        assertEquals(10, test12.getInt("t2_i2"));
        commit();
        assertTrue(test12.next());
        assertEquals(2, test12.getInt("t1_i1"));
        assertEquals(20, test12.getInt("t1_i2"));
        assertEquals(2, test12.getInt("t2_i1"));
        assertEquals(20, test12.getInt("t2_i2"));
        commit();
        assertTrue(test12.next());
        assertEquals(4, test12.getInt("t1_i1"));
        assertEquals(40, test12.getInt("t1_i2"));
        assertEquals(4, test12.getInt("t2_i1"));
        assertEquals(40, test12.getInt("t2_i2"));
        assertTrue(test12.next());
        assertEquals(5, test12.getInt("t1_i1"));
        assertEquals(50, test12.getInt("t1_i2"));
        assertEquals(5, test12.getInt("t2_i1"));
        assertEquals(50, test12.getInt("t2_i2"));
        commit();
        commit();
        assertFalse(test12.next());
        commit();

        test12.close();

        st.close();
        stUtil.close();
    }
}
