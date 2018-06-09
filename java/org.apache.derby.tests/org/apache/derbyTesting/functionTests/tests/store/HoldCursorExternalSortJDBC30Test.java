/*
 * 
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.HoldCursorExternalSortJDBC30Test
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Properties;
import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * TEST CASES SPECIFIC TO STORE IMPLEMENTATION OF HOLD CURSOR (external sort).
 */
public class HoldCursorExternalSortJDBC30Test extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public HoldCursorExternalSortJDBC30Test(String name) {
        super(name);
    }
    
    public static Test suite() {
        Properties sysProps = new Properties();
        sysProps.put("derby.storage.sortBufferMax", "5");
        sysProps.put("derby.debug.true", "testSort");

        Test suite = TestConfiguration.embeddedSuite(HoldCursorExternalSortJDBC30Test.class);
        return new CleanDatabaseTestSetup(new SystemPropertyTestSetup(suite, sysProps, true)) {
            /**
             * Creates the table used in the test cases.
             *
             */
            protected void decorateSQL(Statement s) throws SQLException {
                Connection conn = s.getConnection();
                conn.setAutoCommit(false);

                /* This table is used by testOrder_Hold and testOrder_NoHold */
                s.executeUpdate("create table foo (a int, data varchar(2000))");

                /* This one is specific for testOrderWithMultipleLevel since
                 * it requires some more records to be inserted */
                s.executeUpdate("create table bar (a int, data varchar(2000))");

                PreparedStatement ps = conn.prepareStatement(
                                    "insert into foo values(?,?), (?,?), (?,?), (?,?), (?,?), " +
                                    "(?,?), (?,?), (?,?), (?,?), (?,?)"
                                    );

                for(int i = 0; i <= 9; i++){
                    ps.setInt(i*2+1, i+1);
                    ps.setString(i*2+2, Formatters.padString("" + (i+1), 2000));
                }
                ps.executeUpdate();
                ps.close();
                
                s.execute("INSERT INTO bar SELECT * FROM foo");
            }

        };
    }
    
    /**
     * test hold cursor with external sort (order by).
     * Cutover to external sort has been set to 4 rows by the test property 
     * file so with 10 rows we get a 1 level external sort.  This tests that
     * temp files will be held open across the commit if the cursor is held
     * open.
     */
    public void testOrder_Hold() throws SQLException{
         setAutoCommit(false);

        Statement stUtil = createStatement();
        stUtil.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                        "'derby.language.bulkFetchDefault', '1')");
        
        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        /* Enable statistics */
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        ResultSet test1 = st.executeQuery("select * from foo order by a");
        stUtil.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.language.bulkFetchDefault', '16')");

        /* Commit pattern for the cursor navigation */
        boolean[] doCommitAfter = {true, false, false, false, true, false, false, false, true, false};

        for(int i=0; i<10; i++) {
            assertTrue(test1.next());

            /* Match both key and the padded value */
            assertEquals(i+1, test1.getInt("a"));
            assertEquals(Formatters.padString(""+(i+1), 2000),
                        test1.getString("data"));

            if (doCommitAfter[i]) {
                commit();
            }
        }

        /* No more records */
        assertFalse(test1.next());

        test1.close();
        commit();

        /* Confirm that an external sort occured */
        RuntimeStatisticsParser parser = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(parser.usedExternalSort());

        st.close();
        stUtil.close();
    }
    
    /**
     * test hold cursor with external sort (order by).
     * Cutover to external sort has been set to 4 rows by the test property 
     * file so with 10 rows we get a 1 level external sort.  This tests that
     * temp files will be held open across the commit if the cursor is held
     * open.
     */
    public void testOrder_NoHold() throws SQLException{        
        setAutoCommit(false);

        Statement stUtil = createStatement();

        stUtil.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        //exercise the non-held cursor path also.
        stUtil.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.language.bulkFetchDefault', '1')");

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.CLOSE_CURSORS_AT_COMMIT);

        /* Enable statistics */
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        ResultSet test1 = st.executeQuery("select * from foo order by a");

        stUtil.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.language.bulkFetchDefault', '16')");        
        
        for(int i=0; i<10; i++) {
            assertTrue(test1.next());

            /* Match both key and the padded value */
            assertEquals(i+1, test1.getInt("a"));
            assertEquals(Formatters.padString(""+(i+1), 2000),
                        test1.getString("data"));
        }
        
        /* No more records */
        assertFalse(test1.next());

        test1.close();
        commit();

        /* Confirm that an external sort occured */
        RuntimeStatisticsParser parser = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(parser.usedExternalSort());

        st.close();
        stUtil.close();
    }
    
    /**
     * test hold cursor with multi-level external sort (order by).
     * Cutover to external sort has been set to 4 rows by the test property 
     * file so with 10 rows we get a 1 level external sort.  This tests that
     * temp files will be held open across the commit if the cursor is held
     * open.
     */
    public void testOrderWthMultipleLevel() throws SQLException{
        setAutoCommit(false);
        
        Statement stUtil = createStatement();
        
        stUtil.addBatch("insert into bar select a + 100, data from bar");
        stUtil.addBatch("insert into bar select a + 10,  data from bar");
        stUtil.addBatch("insert into bar select a + 200, data from bar");
        stUtil.addBatch("insert into bar select a + 200, data from bar");
        stUtil.executeBatch();

        stUtil.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
        "'derby.language.bulkFetchDefault', '1')");

        Statement st = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_READ_ONLY,
                                       ResultSet.HOLD_CURSORS_OVER_COMMIT);

        /* Enable statistics */
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        ResultSet test1 = st.executeQuery("select * from bar order by a");
        stUtil.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.language.bulkFetchDefault', '16')");

        /* This pattern is repeated twice below */
        boolean[] doCommitAfter = {true, false, false, false, true, false, false, false, true, false,
                                   true, false, false, false, true, false, false, false, true, false};

        /* Do asserts where a=[1,20] */
        for(int i=0; i<20; i++) {
            assertTrue(test1.next());

            /* The actual data ranges from 1 to 10. This enforces it based on "i" */
            String data = ((i+1) % 10 == 0 ? 10 : (i+1) % 10)+"";

            /* Match both key and the padded value */
            assertEquals(i+1, test1.getInt("a"));
            assertEquals(Formatters.padString(data, 2000),
                        test1.getString("data"));

            /* Check whether we want a commit */
            if (doCommitAfter[i%20]) {
                commit();
            }
        }

        /* Do asserts where a=[101,120] */
        for(int i=100; i<120; i++) {
            assertTrue(test1.next());

            /* The actual data ranges from 1 to 10. This enforces it based on "i" */
            String data = ((i+1) % 10 == 0 ? 10 : (i+1) % 10)+"";

            /* Match both key and the padded value */
            assertEquals(i+1, test1.getInt("a"));
            assertEquals(Formatters.padString(data, 2000),
                        test1.getString("data"));

            /* Check whether we want a commit */
            if (doCommitAfter[i%20]) {
                commit();
            }
        }

        /* Do the last assert and commit */
        assertTrue(test1.next());
            
        assertEquals(201, test1.getInt("a"));
        assertEquals(Formatters.padString("1", 2000),
                    test1.getString("data"));

        commit();
        test1.close();
        
        /* Confirm that an external sort occured */
        RuntimeStatisticsParser parser = SQLUtilities.getRuntimeStatisticsParser(st);
        assertTrue(parser.usedExternalSort());

        stUtil.close();
        st.close();
    }
}
