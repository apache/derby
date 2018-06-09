/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.store.TableLockBasicTest

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
* Very basic single user testing of table locking, verify that the right locks
* are obtained for simple operations.  This test only looks at table and
* row logical locks, it does not verify physical latches or lock ordering.
--
* The basic methodology is:
*    start transaction
*    simple operation
*    print lock table which should match the master
*    end transation
 *
 */
public class TableLockBasicTest extends BaseJDBCTestCase {
    public TableLockBasicTest(String name) {
        super(name);
    }
   
    public static Test suite() {        
        Test test = TestConfiguration.defaultSuite(TableLockBasicTest.class);
        test = DatabasePropertyTestSetup.singleProperty(test, 
                "derby.storage.rowLocking", "false", true);
        test = new LocaleTestSetup(test, Locale.ENGLISH);
        return TestConfiguration.singleUseDatabaseDecorator(test);
    }
    
    protected void setUp() throws SQLException {            
            Statement s = createStatement();
            s.execute("CREATE FUNCTION  PADSTRING (DATA VARCHAR(32000), "
                    + "LENGTH INTEGER) RETURNS VARCHAR(32000) EXTERNAL NAME "
                    + "'org.apache.derbyTesting.functionTests.util.Formatters"
                    + ".padString' LANGUAGE JAVA PARAMETER STYLE JAVA");
            createLockTableQueryEntries(s);
            s.close();
            
            getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            getConnection().setAutoCommit(false);
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
        commit();
        
        super.tearDown();
    }
    
    public void testInsertIntoHeap() throws SQLException {
        Statement st = createStatement();
        st.execute("create table heap_only (a int)");
        commit();    

        //Test insert into empty heap, should just get table lock
        st.execute("insert into heap_only values (1)");        
        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "X", "HEAP_ONLY", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();
        
        
        //Test insert into heap with one row, just get table lock
        st.execute("insert into heap_only values (2)");        
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "X", "HEAP_ONLY", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();
        
        
        st.close();        
        dropTable("heap_only");
    }

    /**
     * Test select from a heap, should get shared table lock.
     */
    public void testSelectFromHeap () throws SQLException { 
        Statement st = createStatement();        
        constructHeap(st);
                
        ResultSet rs = st.executeQuery("select a from heap_only where a = 1");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
        });
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "S", "HEAP_ONLY", "Tablelock", "GRANT", "ACTIVE"}
        });
        commit();

        st.close();
        dropTable("heap_only");
    }

    private void constructHeap(Statement st) throws SQLException {
        st.execute("create table heap_only (a int)");
        commit();    
        
        st.execute("insert into heap_only values (1)");
        commit();
        st.execute("insert into heap_only values (2)");
        commit();
    }

    /**
     * Test delete from a heap, should get exclusive table lock.
     */
    public void testDelectFromHeap () throws SQLException { 
        Statement st = createStatement();
        constructHeap(st);
        
        st.execute("delete from heap_only where a = 1");
        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "2", "X", "HEAP_ONLY", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();

        st.close();
        dropTable("heap_only");
    }
    
    /**
     * Test update to heap, should get exclusive table lock.
     */
    public void testUpdateToHeap () throws SQLException { 
        Statement st = createStatement();
        constructHeap(st);
        
        st.execute("update heap_only set a = 1000 where a = 2");
        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "2", "X", "HEAP_ONLY", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();

        st.close();
        dropTable("heap_only");
    }

    /**
     * Test drop of heap, should get exclusive table lock.
     */
    public void testDropHeap () throws SQLException { 
        Statement st = createStatement();
        constructHeap(st);
        
        st.execute("drop table heap_only");
        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "3", "X", "*** TRANSIEN", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();

        st.close();
        dropTable("heap_only");
    }
    
    public void testCreateIndex() throws SQLException {
        Statement st = createStatement();
        
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096')");
        st.execute("create table indexed_heap (a int, b varchar(1000))");
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");
        
        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "X", "INDEXED_HEAP", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();
                
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096')");
        st.execute("create index a_idx on indexed_heap (a, b)");
        st.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL)");
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "4", "S", "INDEXED_HEAP", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "TABLE", "1", "X", "INDEXED_HEAP", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "TABLE", "1", "X", "INDEXED_HEAP", "Tablelock", "GRANT", "ACTIVE"},        });
        commit();
        
        st.close();
        
        dropTable("indexed_heap");
    }

    public void testInsertIntoEmtpyIndexedHeaP() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table indexed_heap (a int, b varchar(1000))");
        commit();
        st.execute("create index a_idx on indexed_heap (a, b)");
        commit();

        // Test insert into indexed heap, should just get table lock
        st.execute("insert into indexed_heap (a) values (1)");
        ResultSet rs = st.executeQuery(" select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][] { 
                {"APP", "UserTran", "TABLE", "2", "X", "INDEXED_HEAP", "Tablelock", "GRANT", "ACTIVE"}, 
        });
        commit();

        // Test insert into indexed heap with one row, just get table lock
        st.execute("insert into indexed_heap (a) values (2)");
        rs = st.executeQuery(" select * from lock_table order by tabname, type "
                        + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][] { 
                {"APP", "UserTran", "TABLE", "2", "X", "INDEXED_HEAP", "Tablelock", "GRANT", "ACTIVE"} 
        });
        commit();

        st.close();
        
        dropTable("indexed_heap");
    }
    
    /**
     * Test select from a indexed heap, should get shared table lock.
     */
    public void testSelectFromIndexedHeap () throws SQLException { 
        Statement st = createStatement();
        constructIndexedHeap(st);
               
        ResultSet rs = st.executeQuery("select a from indexed_heap where a = 1");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"},
        });
        
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "S", "INDEXED_HEAP", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();

        st.close();
        
        dropTable("indexed_heap");
    }

    private void constructIndexedHeap(Statement st) throws SQLException {
        st.execute("create table indexed_heap (a int, b varchar(1000))");
        st.execute("create index a_idx on indexed_heap (a, b)");

        st.execute("insert into indexed_heap (a) values (1)");
        st.execute("insert into indexed_heap (a) values (2)");
        
        commit();
    }
    
    /**
     * Test delete from a indexed heap, should get exclusive table lock.
     */
    public void testDeleteFromIndexedHeap () throws SQLException { 
        Statement st = createStatement();
        constructIndexedHeap(st);
               
        st.execute("delete from indexed_heap where a = 1");

        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "3", "X", "INDEXED_HEAP", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();

        st.close();
        
        dropTable("indexed_heap");
    }

    /**
     * Test update to indexed heap, should get exclusive table lock.
     */
    public void testUpdateToIndexedHeap () throws SQLException { 
        Statement st = createStatement();
        constructIndexedHeap(st);
               
        st.execute("update indexed_heap set a = 1000 where a = 2");

        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "4", "X", "INDEXED_HEAP", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();

        st.close();
        
        dropTable("indexed_heap");
    }
    
    /**
     * Test drop of indexed heap, should get exclusive table lock.
     */
    public void testDropIndexedHeap () throws SQLException { 
        Statement st = createStatement();
        constructIndexedHeap(st);
               
        st.execute("drop table indexed_heap");

        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "X", "*** TRANSIEN", "Tablelock", "GRANT", "ACTIVE"},
                {"APP", "UserTran", "TABLE", "4", "X", "*** TRANSIEN", "Tablelock", "GRANT", "ACTIVE"},
        });
        commit();

        st.close();
    }
    
    /**
     * Test LOCK TABLE statement
     */
    public void testLockTableStatement() throws SQLException {
        Statement st = createStatement();
        
        st.execute("create table t1(c1 int)");
        commit();        
        
        PreparedStatement pst = prepareStatement("lock table t1 in exclusive mode");
        pst.execute();
        
        ResultSet rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "X", "T1", "Tablelock", "GRANT", "ACTIVE"},
        });
        
        //verify that statement gets recompiled correctly
        st.execute("drop table t1");
        st.execute("create table t1(c1 int)");
        pst.execute();
        commit();
        
        st.execute("lock table t1 in share mode");
        rs = st.executeQuery(
                " select * from lock_table order by tabname, type "
                + "desc, mode, cnt, lockname");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"APP", "UserTran", "TABLE", "1", "S", "T1", "Tablelock", "GRANT", "ACTIVE"},
        });
        
        st.execute("drop table t1");
        commit();
        
        st.close();
    }
    
    /**
     * verify that lock table not allowed in sys schema
     */
    public void testLockTableInSysSchema() throws SQLException {
        Statement st = createStatement();
        
        assertStatementError("42X62", st,
                "lock table sys.systables in exclusive mode");
        
        st.close();
    }
    
    /**
     * Test RTS output when table locking configured
     */
    public void testRTSOutput() throws SQLException {
        Statement st = createStatement();
        
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        st.execute("create table rts(c1 int)");
        st.execute("insert into rts values 1");
        commit();
        
        ResultSet rs = st.executeQuery("select * from rts with cs");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"1"}
        });
        
        rs =  st.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        
        JDBC.assertFullResultSet(rs, new String[][]{
                {"Statement Name: \n"
                    + "	" + (getTestConfiguration().getJDBCClient().isEmbedded() ? "null\n" : "SQL_CURLH000C3\n")
                    + "Statement Text: \n"
                    + "	select * from rts with cs\n"
                    + "Parse Time: 0\n"
                    + "Bind Time: 0\n"
                    + "Optimize Time: 0\n"
                    + "Generate Time: 0\n"
                    + "Compile Time: 0\n"
                    + "Execute Time: 0\n"
                    + "Begin Compilation Timestamp : null\n"
                    + "End Compilation Timestamp : null\n"
                    + "Begin Execution Timestamp : null\n"
                    + "End Execution Timestamp : null\n"
                    + "Statement Execution Plan Text: \n"
                    + "Table Scan ResultSet for RTS at read committed isolation level using instantaneous share row locking chosen by the optimizer (Actual locking used: table level locking.)\n"
                    + "Number of opens = 1\n"
                    + "Rows seen = 1\n"
                    + "Rows filtered = 0\n"
                    + "Fetch Size = 16\n"
                    + "	constructor time (milliseconds) = 0\n"
                    + "	open time (milliseconds) = 0\n"
                    + "	next time (milliseconds) = 0\n"
                    + "	close time (milliseconds) = 0\n"
                    + "	next time in milliseconds/row = 0\n\n"
                    + "scan information:\n"
                    + "	Bit set of columns fetched=All\n"
                    + "	Number of columns fetched=1\n"
                    + "	Number of pages visited=1\n"
                    + "	Number of rows qualified=1\n"
                    + "	Number of rows visited=1\n"
                    + "	Scan type=heap\n"
                    + "	start position:\n"
                    + "		null\n"
                    + "	stop position:\n"
                    + "		null\n"
                    + "	qualifiers:\n"
                    + "		None\n"
                    + "	optimizer estimated row count: 6.00\n"
                    + "	optimizer estimated cost: 100.40"
                },
        });

        st.execute("drop table rts");
        commit();
        
        st.close();
    }

    public void testDDLTableLockMode() throws SQLException {
        Statement st = createStatement();
        st.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        
        st.execute("create table default_granularity(c1 int)");
        st.execute("create table row_granularity(c1 int)");
        st.execute("alter table row_granularity locksize row");
        st.execute("create table table_granularity(c1 int)");
        st.execute("alter table table_granularity locksize table");
        
        ResultSet rs = st.executeQuery("select * from default_granularity with cs");
        JDBC.assertEmpty(rs);
        
        rs = st.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"Statement Name: \n"
                    + "	" + (getTestConfiguration().getJDBCClient().isEmbedded() ? "null\n" : "SQL_CURLH000C2\n")
                    + "Statement Text: \n"
                    + "	select * from default_granularity with cs\n"
                    + "Parse Time: 0\n"
                    + "Bind Time: 0\n"
                    + "Optimize Time: 0\n"
                    + "Generate Time: 0\n"
                    + "Compile Time: 0\n"
                    + "Execute Time: 0\n"
                    + "Begin Compilation Timestamp : null\n"
                    + "End Compilation Timestamp : null\n"
                    + "Begin Execution Timestamp : null\n"
                    + "End Execution Timestamp : null\n"
                    + "Statement Execution Plan Text: \n"
                    + "Table Scan ResultSet for DEFAULT_GRANULARITY at read committed isolation level using instantaneous share row locking chosen by the optimizer (Actual locking used: table level locking.)\n"
                    + "Number of opens = 1\n"
                    + "Rows seen = 0\n"
                    + "Rows filtered = 0\n"
                    + "Fetch Size = 16\n"
                    + "	constructor time (milliseconds) = 0\n"
                    + "	open time (milliseconds) = 0\n"
                    + "	next time (milliseconds) = 0\n"
                    + "	close time (milliseconds) = 0\n\n"
                    + "scan information:\n"
                    + "	Bit set of columns fetched=All\n"
                    + "	Number of columns fetched=1\n"
                    + "	Number of pages visited=1\n"
                    + "	Number of rows qualified=0\n"
                    + "	Number of rows visited=0\n"
                    + "	Scan type=heap\n"
                    + "	start position:\n"
                    + "		null\n"
                    + "	stop position:\n"
                    + "		null\n"
                    + "	qualifiers:\n"
                    + "		None\n"
                    + "	optimizer estimated row count: 6.00\n"
                    + "	optimizer estimated cost: 100.40"
                }
        }        
        );
        
        
        rs = st.executeQuery("select * from default_granularity with rr");
        JDBC.assertEmpty(rs);
        
        rs = st.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"Statement Name: \n"
                    + "	" + (getTestConfiguration().getJDBCClient().isEmbedded() ? "null\n" : "SQL_CURLH000C2\n")
                    + "Statement Text: \n"
                    + "	select * from default_granularity with rr\n"
                    + "Parse Time: 0\n"
                    + "Bind Time: 0\n"
                    + "Optimize Time: 0\n"
                    + "Generate Time: 0\n"
                    + "Compile Time: 0\n"
                    + "Execute Time: 0\n"
                    + "Begin Compilation Timestamp : null\n"
                    + "End Compilation Timestamp : null\n"
                    + "Begin Execution Timestamp : null\n"
                    + "End Execution Timestamp : null\n"
                    + "Statement Execution Plan Text: \n"
                    + "Table Scan ResultSet for DEFAULT_GRANULARITY at serializable isolation level using share table locking chosen by the optimizer\n"
                    + "Number of opens = 1\n"
                    + "Rows seen = 0\n"
                    + "Rows filtered = 0\n"
                    + "Fetch Size = 16\n"
                    + "	constructor time (milliseconds) = 0\n"
                    + "	open time (milliseconds) = 0\n"
                    + "	next time (milliseconds) = 0\n"
                    + "	close time (milliseconds) = 0\n\n"
                    + "scan information:\n"
                    + "	Bit set of columns fetched=All\n"
                    + "	Number of columns fetched=1\n"
                    + "	Number of pages visited=1\n"
                    + "	Number of rows qualified=0\n"
                    + "	Number of rows visited=0\n"
                    + "	Scan type=heap\n"
                    + "	start position:\n"
                    + "		null\n"
                    + "	stop position:\n"
                    + "		null\n"
                    + "	qualifiers:\n"
                    + "		None\n"
                    + "	optimizer estimated row count: 6.00\n"
                    + "	optimizer estimated cost: 100.40"
                }
        }        
        );
        
        rs = st.executeQuery("select * from default_granularity with cs");
        JDBC.assertEmpty(rs);
        
        rs = st.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"Statement Name: \n"
                    + "	" + (getTestConfiguration().getJDBCClient().isEmbedded() ? "null\n" : "SQL_CURLH000C2\n")
                    + "Statement Text: \n"
                    + "	select * from default_granularity with cs\n"
                    + "Parse Time: 0\n"
                    + "Bind Time: 0\n"
                    + "Optimize Time: 0\n"
                    + "Generate Time: 0\n"
                    + "Compile Time: 0\n"
                    + "Execute Time: 0\n"
                    + "Begin Compilation Timestamp : null\n"
                    + "End Compilation Timestamp : null\n"
                    + "Begin Execution Timestamp : null\n"
                    + "End Execution Timestamp : null\n"
                    + "Statement Execution Plan Text: \n"
                    + "Table Scan ResultSet for DEFAULT_GRANULARITY at read committed isolation level using instantaneous share row locking chosen by the optimizer (Actual locking used: table level locking.)\n"
                    + "Number of opens = 1\n"
                    + "Rows seen = 0\n"
                    + "Rows filtered = 0\n"
                    + "Fetch Size = 16\n"
                    + "	constructor time (milliseconds) = 0\n"
                    + "	open time (milliseconds) = 0\n"
                    + "	next time (milliseconds) = 0\n"
                    + "	close time (milliseconds) = 0\n\n"
                    + "scan information:\n"
                    + "	Bit set of columns fetched=All\n"
                    + "	Number of columns fetched=1\n"
                    + "	Number of pages visited=1\n"
                    + "	Number of rows qualified=0\n"
                    + "	Number of rows visited=0\n"
                    + "	Scan type=heap\n"
                    + "	start position:\n"
                    + "		null\n"
                    + "	stop position:\n"
                    + "		null\n"
                    + "	qualifiers:\n"
                    + "		None\n"
                    + "	optimizer estimated row count: 6.00\n"
                    + "	optimizer estimated cost: 100.40"
                }
        }        
        );
        
        
        rs = st.executeQuery("select * from row_granularity with rr");
        JDBC.assertEmpty(rs);
        
        rs = st.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"Statement Name: \n"
                    + "	" + (getTestConfiguration().getJDBCClient().isEmbedded() ? "null\n" : "SQL_CURLH000C2\n")
                    + "Statement Text: \n"
                    + "	select * from row_granularity with rr\n"
                    + "Parse Time: 0\n"
                    + "Bind Time: 0\n"
                    + "Optimize Time: 0\n"
                    + "Generate Time: 0\n"
                    + "Compile Time: 0\n"
                    + "Execute Time: 0\n"
                    + "Begin Compilation Timestamp : null\n"
                    + "End Compilation Timestamp : null\n"
                    + "Begin Execution Timestamp : null\n"
                    + "End Execution Timestamp : null\n"
                    + "Statement Execution Plan Text: \n"
                    + "Table Scan ResultSet for ROW_GRANULARITY at serializable isolation level using share table locking chosen by the optimizer\n"
                    + "Number of opens = 1\n"
                    + "Rows seen = 0\n"
                    + "Rows filtered = 0\n"
                    + "Fetch Size = 16\n"
                    + "	constructor time (milliseconds) = 0\n"
                    + "	open time (milliseconds) = 0\n"
                    + "	next time (milliseconds) = 0\n"
                    + "	close time (milliseconds) = 0\n\n"
                    + "scan information:\n"
                    + "	Bit set of columns fetched=All\n"
                    + "	Number of columns fetched=1\n"
                    + "	Number of pages visited=1\n"
                    + "	Number of rows qualified=0\n"
                    + "	Number of rows visited=0\n"
                    + "	Scan type=heap\n"
                    + "	start position:\n"
                    + "		null\n"
                    + "	stop position:\n"
                    + "		null\n"
                    + "	qualifiers:\n"
                    + "		None\n"
                    + "	optimizer estimated row count: 6.00\n"
                    + "	optimizer estimated cost: 100.40"
                }
        }        
        );
        
        
        rs = st.executeQuery("select * from table_granularity with cs");
        JDBC.assertEmpty(rs);
        
        rs = st.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"Statement Name: \n"
                    + "	" + (getTestConfiguration().getJDBCClient().isEmbedded() ? "null\n" : "SQL_CURLH000C2\n")
                    + "Statement Text: \n"
                    + "	select * from table_granularity with cs\n"
                    + "Parse Time: 0\n"
                    + "Bind Time: 0\n"
                    + "Optimize Time: 0\n"
                    + "Generate Time: 0\n"
                    + "Compile Time: 0\n"
                    + "Execute Time: 0\n"
                    + "Begin Compilation Timestamp : null\n"
                    + "End Compilation Timestamp : null\n"
                    + "Begin Execution Timestamp : null\n"
                    + "End Execution Timestamp : null\n"
                    + "Statement Execution Plan Text: \n"
                    + "Table Scan ResultSet for TABLE_GRANULARITY at read committed isolation level using instantaneous share table locking chosen by the optimizer\n"
                    + "Number of opens = 1\n"
                    + "Rows seen = 0\n"
                    + "Rows filtered = 0\n"
                    + "Fetch Size = 16\n"
                    + "	constructor time (milliseconds) = 0\n"
                    + "	open time (milliseconds) = 0\n"
                    + "	next time (milliseconds) = 0\n"
                    + "	close time (milliseconds) = 0\n\n"
                    + "scan information:\n"
                    + "	Bit set of columns fetched=All\n"
                    + "	Number of columns fetched=1\n"
                    + "	Number of pages visited=1\n"
                    + "	Number of rows qualified=0\n"
                    + "	Number of rows visited=0\n"
                    + "	Scan type=heap\n"
                    + "	start position:\n"
                    + "		null\n"
                    + "	stop position:\n"
                    + "		null\n"
                    + "	qualifiers:\n"
                    + "		None\n"
                    + "	optimizer estimated row count: 6.00\n"
                    + "	optimizer estimated cost: 100.40"
                }
        }        
        );
        
        
        rs = st.executeQuery("select * from table_granularity with rr");
        JDBC.assertEmpty(rs);
        
        rs = st.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        JDBC.assertFullResultSet(rs, new String[][]{
                {"Statement Name: \n"
                    + "	" + (getTestConfiguration().getJDBCClient().isEmbedded() ? "null\n" : "SQL_CURLH000C2\n")
                    + "Statement Text: \n"
                    + "	select * from table_granularity with rr\n"
                    + "Parse Time: 0\n"
                    + "Bind Time: 0\n"
                    + "Optimize Time: 0\n"
                    + "Generate Time: 0\n"
                    + "Compile Time: 0\n"
                    + "Execute Time: 0\n"
                    + "Begin Compilation Timestamp : null\n"
                    + "End Compilation Timestamp : null\n"
                    + "Begin Execution Timestamp : null\n"
                    + "End Execution Timestamp : null\n"
                    + "Statement Execution Plan Text: \n"
                    + "Table Scan ResultSet for TABLE_GRANULARITY at serializable isolation level using share table locking chosen by the optimizer\n"
                    + "Number of opens = 1\n"
                    + "Rows seen = 0\n"
                    + "Rows filtered = 0\n"
                    + "Fetch Size = 16\n"
                    + "	constructor time (milliseconds) = 0\n"
                    + "	open time (milliseconds) = 0\n"
                    + "	next time (milliseconds) = 0\n"
                    + "	close time (milliseconds) = 0\n\n"
                    + "scan information:\n"
                    + "	Bit set of columns fetched=All\n"
                    + "	Number of columns fetched=1\n"
                    + "	Number of pages visited=1\n"
                    + "	Number of rows qualified=0\n"
                    + "	Number of rows visited=0\n"
                    + "	Scan type=heap\n"
                    + "	start position:\n"
                    + "		null\n"
                    + "	stop position:\n"
                    + "		null\n"
                    + "	qualifiers:\n"
                    + "		None\n"
                    + "	optimizer estimated row count: 6.00\n"
                    + "	optimizer estimated cost: 100.40"
                }
        }        
        );
        

        st.close();
        rollback();
    }

}
