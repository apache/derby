/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.store.RowLockBasicTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Very basic single user testing of row locking, verify that the right locks
 * are obtained for simple operations.  This test only looks at table and
 * row logical locks, it does not verify physical latches or lock ordering.
 * The basic methodology is:
 *       start transaction
 *       simple operation
 *       print lock table which should match the master
 *       end transation 
 *
 */
public class RowLockBasicTest extends BaseJDBCTestCase {
    public RowLockBasicTest(String name) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5282
        super(name);
    }
    
    public static Test suite() {
        return TestConfiguration.defaultSuite(RowLockBasicTest.class);
    }
    
    protected void setUp() throws SQLException {
            getConnection().setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            Statement s = createStatement();
            s.execute("CREATE FUNCTION  PADSTRING (DATA VARCHAR(32000), "
                    + "LENGTH INTEGER) RETURNS VARCHAR(32000) EXTERNAL NAME "
                    + "'org.apache.derbyTesting.functionTests.util.Formatters"
                    + ".padString' LANGUAGE JAVA PARAMETER STYLE JAVA");
            createLockTableQueryEntries(s);
            
            s.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY"
                    + "('derby.storage.pageSize', '4096')");
            s.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY"
                    + "('derby.storage.rowLocking', 'true')");
            
            s.close();
            
            getConnection().setAutoCommit(false);
            
            dropTable("a");
    }
    
    private void createLockTableQueryEntries(Statement s) throws SQLException {
        s.execute("create view lock_table as "
                + "select "
                + "cast(username as char(8)) as username,"
                + "cast(t.type as char(8)) as trantype,"
                + "cast(l.type as char(8)) as type,"
                + "cast(lockcount as char(3)) as cnt,"
                + "mode,"
                + "cast(tablename as char(12)) as tabname,"
                + "cast(lockname as char(10)) as lockname,"
                + "state,"
                + "status "
                + "from "
                + "syscs_diag.lock_table l  right outer join syscs_diag.transaction_table t "
                + "on l.xid = t.xid where l.tableType <> 'S' and t.type='UserTransaction'");
        
        //lock table with system catalog locks included.
        s.execute("create view full_lock_table as "
                + "select "
                + "cast(username as char(8)) as username, "
                + "cast(t.type as char(8)) as trantype,"
                + "cast(l.type as char(8)) as type,"
                + "cast(lockcount as char(3)) as cnt,"
                + "mode, "
                + "cast(tablename as char(12)) as tabname,"
                + "cast(lockname as char(10)) as lockname,"
                + "state,"
                + "status "
                + "from "
                + "syscs_diag.lock_table l right outer join syscs_diag.transaction_table t "
                + "on l.xid = t.xid where l.tableType <> 'S' ");
        
        //lock table with no join.
        s.execute("create view lock_table2 as "
                + "select "
                + "cast(l.xid as char(8)) as xid,"
                + "cast(l.type as char(8)) as type,"
                + "cast(lockcount as char(3)) as cnt,"
                + "mode,"
                + "cast(tablename as char(12)) as tabname,"
                + "cast(lockname as char(10)) as lockname,"
                + "state "
                + "from "
                + "syscs_diag.lock_table l "
                + "where l.tableType <> 'S' ");
        
        //-- transaction table with no join.
        s.execute("create view tran_table as "
                + "select "
                + "* "
                + "from "
                + "syscs_diag.transaction_table");        
    }

    public void tearDown() throws Exception {
        Statement st = createStatement();
        st.executeUpdate("DROP FUNCTION PADSTRING");
        
        st.execute("drop view lock_table");
        st.execute("drop view full_lock_table");
        st.execute("drop view lock_table2");
        st.execute("drop view tran_table");
        
        st.close();
        dropTable("a");
        commit();
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5282
        super.tearDown();
    }
    
    public void testInsertIntoHeap() throws SQLException {
        Statement st = createStatement();
        st.execute("create table a (a int)");
        commit();    
        
        //Test insert into empty heap, should just get row lock on row being inserted
        st.execute("insert into a values (1)");        
        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IX", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "X", "A", "(1,7)", "GRANT", "ACTIVE"}
        });
        commit();
        
        //Test insert into heap with one row, just get row lock on row being inserted
        st.execute("insert into a values (2)");        
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IX", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "X", "A", "(1,8)", "GRANT", "ACTIVE"}
        });
        commit();
        
        dropTable("a");
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "3", "X", "*** TRANSIEN", "Tablelock", "GRANT", "ACTIVE"}
        });
        commit();    
                
        st.close();        
    }
    
    public void testInsertIntoBtree() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table a (a int, b varchar(1000))");
        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "X", "A", "Tablelock", "GRANT", "ACTIVE"}
        });
        commit();

        st.execute("create index a_idx on a (a, b)");
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "4", "S", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "TABLE", "1", "X", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "TABLE", "1", "X", "A", "Tablelock", "GRANT", "ACTIVE"}
        });
        commit();
 
        //Test insert into empty btree, should just get row lock on row being 
        //inserted and an instant duration lock on "first key in table" row (id 3).
        st.execute("insert into a values (1, PADSTRING('a',1000))");
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "2", "IX", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "X", "A", "(1,7)", "GRANT", "ACTIVE"}
        });
        commit();
        
        //Test insert into non-empty btree, should get row lock on row being 
        //inserted and an instant duration lock on the one before it.
        st.execute("insert into a values (2, PADSTRING('b',1000))");
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "2", "IX", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "X", "A", "(1,8)", "GRANT", "ACTIVE"}
        });
        commit();

        //Cause split and check locks that are obtained as part of inserting after
        //the split.  This causes the code to get a previous lock on a previous page.
         
        //RESOLVE (mikem) - the row lock on (1,9) is a result of raw store getting
        //a lock while it tries to fit the row on the original page record id, but
        // there is not enough room, so it eventually allocates a new page/row and 
        // locks that one - but the old lock is left around.
        //
        // btree just before commit:
        // leftmost leaf: (1,6), (1,7)
        // next leaf:     (1,8), (2,6)
        st.execute("insert into a values (3, PADSTRING('c',1000))");
        commit();        
        st.execute("insert into a values (4, PADSTRING('d',1000))");        
        
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "2", "IX", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "X", "A", "(1,10)", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "X", "A", "(2,6)", "GRANT", "ACTIVE"}
        });
        commit();
    
        st.close();
    }
    
    /**
     *Cause an insert on a new page that inserts into the 1st slot on the btree 
     *page.
     * btree just before commit:
     * leftmost leaf: (1,6), (1,7)
     * next leaf:     (2,7), (2,6) 
     */
    public void testInsertOnNewPage() throws SQLException {
        Statement st = createStatement();        
        st.execute("create table a (a int, b varchar(1000))");
        st.execute("create unique index a_idx on a (a, b)");
        st.execute("insert into a values (1, PADSTRING('a',1000))");
        st.execute("insert into a values (2, PADSTRING('b',1000))");
        st.execute("insert into a values (3, PADSTRING('c',1000))");
        st.execute("insert into a values (4, PADSTRING('d',1000))");
        
        ResultSet rs = st.executeQuery("select a from a");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
                {"2"},
                {"3"},
                {"4"}
        });
        
        st.execute("delete from a where a = 3");
        rs = st.executeQuery("select a from a");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
                {"2"},
                {"4"}
        });        
        commit();
        
        st.execute("insert into a values (3, PADSTRING('c',1000))");
        rs = st.executeQuery(
                "select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "2", "IX", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "X", "A", "(1,9)", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "X", "A", "(2,7)", "GRANT", "ACTIVE"}
        });

        st.close();
    }
    
    /**
     * Do full covered index scan.
     */
    public void testFullCoveredIndexScan() throws SQLException {
        Statement st = createStatement();
        createTableAandUniqueIndex(st);
        
        ResultSet rs = st.executeQuery("select a from a");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
                {"2"},
                {"3"},
                {"4"}
        });
        
        rs = st.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "S", "A", "Tablelock", "GRANT", "ACTIVE"}
        });
        commit();
        
        st.close();
    }

    private void createTableAandUniqueIndex(Statement st) throws SQLException {
        st.execute("create table a (a int, b char(200))");
        st.execute("create unique index a_idx on a (a)");
        st.execute("insert into a values (1, 'a')");
        st.execute("insert into a values (2, 'b')");
        st.execute("insert into a values (3, 'c')");
        st.execute("insert into a values (4, 'd')");
        commit();        
    }
    
    /**
     * Do single probe into covered index (first key in table).
     * 
     */
    public void testSingleProbeIntoFirstKey() throws SQLException {
        Statement st = createStatement();
        createTableAandUniqueIndex(st);
        
        ResultSet rs = st.executeQuery("select a from a where a = 1");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
        });
        
        rs = st.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IS", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,7)", "GRANT", "ACTIVE"}
        });
        commit();
        
        st.close();
    }
    
    /**
     * Do single probe into covered index (last key in table).
     */
    public void testSingleProbeIntoLastKey() throws SQLException {
        Statement st = createStatement();
        createTableAandUniqueIndex(st);
        
        ResultSet rs = st.executeQuery("select a from a where a = 4");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"4"},
        });
        
        rs = st.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IS", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,10)", "GRANT", "ACTIVE"}
        });
        commit();
        
        st.close();
    }
    
    /**
     * Do set of range scans that all return 1 row from covered index.
     */
    public void testSetOfRangeScanFor1RowReturn() throws SQLException {
        Statement st = createStatement();
        createTableAandUniqueIndex(st);
        
        ResultSet rs = st.executeQuery("select a from a where a <= 1");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"}
        });
        
        rs = st.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IS", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,3)", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,7)", "GRANT", "ACTIVE"}
        });
        
        commit();
        
        
        rs = st.executeQuery("select a from a where a >= 2 and a < 3");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"2"}
        });
        
        rs = st.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IS", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,8)", "GRANT", "ACTIVE"}
        });
        
        commit();
        
        
        rs = st.executeQuery("select a from a where a > 3");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"4"}
        });
        
        rs = st.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IS", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,10)", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,9)", "GRANT", "ACTIVE"}
        });
        
        commit();
        
        st.close();        
    }
    
    /**
     * Do set of range scans that all return 0 row from covered index.
     */
    public void testSetOfRangeScanFor0RowReturn() throws SQLException {
        Statement st = createStatement();
        createTableAandUniqueIndex(st);
        
        ResultSet rs = st.executeQuery("select a from a where a < 1");
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IS", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,3)", "GRANT", "ACTIVE"}
        });
        
        commit();
        

        rs = st.executeQuery("select a from a where a > 4");
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IS", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,10)", "GRANT", "ACTIVE"}
        });
        
        commit();
        
        
        rs = st.executeQuery("select a from a where a > 2 and a < 3");
        JDBC.assertDrainResults(rs, 0);
        
        rs = st.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "IS", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "ROW", "1", "S", "A", "(1,8)", "GRANT", "ACTIVE"}
        });
        
        commit();
        
        st.close();        
    }
    
    /**
     * Verify that create index does table level locking.
     */
    public void testCreateIndexDoesTableLevelLocking() throws SQLException {
        Statement st = createStatement();
        st.execute("create table a (a int, b char(200))");
        st.execute("create table b (a int, b char(200))");
        st.execute("insert into a values (1, 'a')");
        st.execute("insert into a values (2, 'b')");
        st.execute("insert into a values (3, 'c')");
        st.execute("insert into a values (4, 'd')");
        commit();
        
        st.execute("create unique index a_idx on a (a)");
        
        ResultSet rs = st.executeQuery("select * from lock_table "
                + "order by tabname, type desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "4", "S", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "TABLE", "1", "X", "A", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "TABLE", "1", "X", "A", "Tablelock", "GRANT", "ACTIVE"}
        });
        commit();

        rs = st.executeQuery("select a from a");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
                {"2"},
                {"3"},
                {"4"}
        });
        
        rs = st.executeQuery("select a from b");
        JDBC.assertDrainResults(rs, 0);
        
        commit();
        
        st.execute("drop index a_idx");
        dropTable("a");
        dropTable("b");
        
        
        st.close();
    }
}
