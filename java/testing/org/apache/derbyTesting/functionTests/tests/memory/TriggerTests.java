/**
 * Repro for DERBY-1482:
 * Update triggers on tables with blob columns stream blobs
 * into memory even when the blobs are not referenced/accessed.
 */

package org.apache.derbyTesting.functionTests.tests.memory;
import java.sql.*;
import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

public class TriggerTests extends BaseJDBCTestCase {

	final int lobsize = 50000*1024;
	boolean testWithLargeDataInLOB = true;
	
	/**
	 * Insert trigger tests
	 * ****************
	 * 1)test1InsertAfterTrigger
	 * 	This test creates an AFTER INSERT trigger which inserts non-lob
	 * columns into another table.
	 * ****************
	 * 2)test1InsertAfterTriggerStoredProc
	 * 	The test case is exactly like test1InsertAfterTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1InsertAfterTrigger gets done inside the stored procedure
	 * for this test.
	 * ****************
	 * 3)test1InsertBeforeTrigger
	 * 	This test creates a BEFORE INSERT trigger which selects 
	 * columns from another table using "new" non-lob column for 
	 * join clause. 
	 * ****************
	 * 4)test1InsertBeforeTriggerStoredProc
	 * 	The test case is exactly like test1InsertBeforeTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1InsertBeforeTrigger gets done inside the stored procedure
	 * for this test.
	 * ****************
	 * Can't write stored procedure calls for trigger actions for test2
	 * because I will need to pass LOBs as parameters to the stored
	 * procedure which is not possible at this point.
	 * ****************
	 * 5)test2InsertAfterTriggerAccessLOB
	 * 	This test creates an AFTER INSERT trigger which in it's trigger action
	 * inserts lob columns from triggering table into another table. So, this
	 * test does access the LOB from the triggering table inside the trigger
	 * action. 
	 * ****************
	 * 6)test2InsertAfterTriggerUpdatedLOB
	 * 	This test creates an AFTER INSERT trigger which in it's trigger action
	 * updates a lob column from the row just inserted. So, this test does
	 * update the LOB from the triggering table inside the trigger
	 * action. 
	 * ****************
	 * 7)test2InsertBeforeTriggerAccessLOB
	 * 	This test creates a BEFORE INSERT trigger which selects "new"
	 * lob column from just inserted row. This test does access the
	 * LOB.
	 * ****************
	 * 8)test5InsertAfterTriggerNoReferencingClause
	 * 	This test creates an AFTER INSERT trigger but has not REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action. 
	 * ****************
	 * 9)test5InsertBeforeTriggerNoReferencingClause
	 * 	This test creates an BEFORE INSERT trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action.
	 * ****************
	 * 
	 * 
	 * 
	 * 
	 * 
	 * Delete trigger tests
	 * ****************
	 * 1)test1DeleteAfterTrigger
	 * 	This test creates an AFTER DELETE trigger which delets from another
	 * table using non-lob from the triggering table in the where clause.
	 * ****************
	 * 2)test1DeleteAfterTriggerStoredProc
	 * 	The test case is exactly like test1DeleteAfterTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1DeleteAfterTrigger gets done inside the stored procedure
	 * for this test.
	 * ****************
	 * 3)test1DeleteBeforeTrigger
	 * 	This test creates a BEFORE DELETE trigger which selects 
	 * columns from another table using "new" non-lob column for 
	 * join clause.
	 * ****************
	 * 4)test1DeleteBeforeTriggerStoredProc
	 * 	The test case is exactly like test1DeleteBeforeTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1DeleteBeforeTrigger gets done inside the stored procedure
	 * for this test.
	 * ****************
	 * Can't write stored procedure calls for trigger actions for test2
	 * because I will need to pass LOBs as parameters to the stored
	 * procedure which is not possible at this point.
	 * ****************
	 * 5)test2DeleteAfterTriggerAccessLOB
	 * 	This test creates an AFTER DELETE trigger which in it's trigger action
	 * deletes row from another table using triggering table's "new" LOB value
	 * in the join clause. So, this test does access the LOB from the 
	 * triggering table inside the trigger action.
	 * ****************
	 * ****************
	 * 6)test2DeleteBeforeTriggerAccessLOB
	 * 	This test creates a BEFORE DELETE trigger which selects "old"
	 * lob column from just deleted row. This test does access the
	 * LOB.
	 * ****************
	 * 7)test5DeleteAfterTriggerNoReferencingClause
	 * 	This test creates an AFTER DELETE trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action.
	 * ****************
	 * 8)test5DeleteBeforeTriggerNoReferencingClause
	 * 	This test creates an BEFORE DELETE trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action. 
	 * ****************
	 * 
	 * 
	 * 
	 * 
	 * Update trigger tests
	 * ****************
	 * 1)test1UpdateAfterTrigger -
	 * 	This test creates an AFTER UPDATE trigger which is declared on a
	 * non-LOB column. The trigger action does not access the LOB column.
	 * ****************
	 * 2)test1UpdateAfterTriggerStoredProc
	 * 	The test case is exactly like test1UpdateAfterTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1UpdateAfterTrigger gets done inside the stored procedure
	 * for this test.
	 * ****************
	 * 3)test1UpdateBeforeTrigger
	 * 	This test creates a BEFORE UPDATE trigger which is declared
	 * on a non-LOB column. The trigger action selects columns from 
	 * another table using "new" non-lob column for join clause. 
	 * ****************
	 * 4)test1UpdateBeforeTriggerStoredProc
	 * 	The test case is exactly like test1UpdateBeforeTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1UpdateBeforeTrigger gets done inside the stored procedure
	 * for this test.
	 * ****************
	 * Can't write stored procedure calls for trigger actions for test2
	 * because I will need to pass LOBs as parameters to the stored
	 * procedure which is not possible at this point.
	 * ****************
	 * 5)test2UpdateAfterTriggerAccessLOB
	 * 	The after update trigger on non-LOB column but the LOB column is
	 * referenced in the trigger action. So, this test does access the LOB 
	 * from the triggering table inside the trigger action. 
	 * ****************
	 * 6)test2UpdateAfterTriggerUpdatedLOB
	 * 	This test creates an AFTER UPDATE trigger which in it's trigger action
	 * updates a lob column from the row that just got updated. So, this test 
	 * does update the LOB from the triggering table inside the trigger
	 * action. 
	 * ****************
	 * 7)test2UpdateBeforeTriggerAccessLOB
	 * 	This test creates a BEFORE UPDATE trigger which selects "new"
	 * lob column from just updated row. This test does access the
	 * LOB. 
	 * ****************
	 * 8)test3UpdateAfterTrigger
	 * 	The after update trigger is defined on LOB column but the LOB column 
	 * is not referenced in the trigger action.
	 * ****************
	 * 9)test3UpdateAfterTriggerStoredProc
	 *  The test case is exactly like test3UpdateAfterTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test3UpdateAfterTrigger gets done inside the stored procedure
	 * for this test.
	 * ****************
	 * 10)test3UpdateBeforeTrigger
	 * 	This test creates a BEFORE UPDATE trigger which selects a row
	 * from another table using "new" non-LOB column from the triggering
	 * table. This test has update trigger defined on the LOB column
	 * but does not access/update that LOB column in the trigger action.
	 * ****************
	 * 11)test3UpdateBeforeTriggerStoredProc
	 * 	The test case is exactly like test3UpdateBeforeTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test3UpdateBeforeTrigger gets done inside the stored procedure
	 * for this test.
	 * ****************
	 * 12)test4UpdateAfterTriggerAccessLOB
	 * 	The after update trigger on LOB column but the LOB column is referenced 
	 * in the trigger action. This is one case though where we do need to keep 
	 * before and after image since the LOB got updated and it is being used in 
	 * trigger action.
	 * ****************
	 * 13)test4UpdateAfterTriggerUpdatedLOB
	 * 	The after update trigger on LOB column which then gets updated in the
	 * trigger action. So this test updates the LOB in the trigger action
	 * and is also the cause of the update trigger to fire. 
	 * ****************
	 * 14)test4UpdateBeforeTrigger
	 * ****************
	 * 15)test5UpdateAfterTriggerNoReferencingClause
	 * 	This test creates an AFTER UPDATE trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action. 
	 * ****************
	 * 16)test5UpdateBeforeTriggerNoReferencingClause
	 * 	This test creates an BEFORE UPDATE trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action. 
	 * ****************
	 * 17)test6UpdateAfterTriggerNoTriggerColumn
	 *  This test create an AFTER UPDATE trigger but does not identify any
	 * trigger columns. It has REFERENCING clause. Void of trigger columns
	 * will cause all the columns to be read into memory.
	 * ****************
	 */
    public TriggerTests(String name) {
        super(name);
    }
	
    public static Test suite() {
        Test suite = new CleanDatabaseTestSetup(TestConfiguration
                .embeddedSuite(TriggerTests.class));
        Properties p = new Properties();
        // use small pageCacheSize so we don't run out of memory on the insert
        // of large LOB columns.
        p.setProperty("derby.storage.pageCacheSize", "100");
        return new SystemPropertyTestSetup(suite,p);
    }

	/**
	 * Create the basic tables and data expected by almost all the tests. If a
	 * particular test needs anything else, that test will take care of it.
	 * @throws SQLException
	 */
	public void basicSetup() throws SQLException{
        Statement s = createStatement();
		try {
			s.execute("drop table table1");
		} catch (SQLException sqle) {}

		try {
			s.execute("drop table table2");
		} catch (SQLException sqle) {}

		try {
			s.execute("drop table table3");
		} catch (SQLException sqle) {}

		try {
			s.execute("drop trigger trigger1");
		} catch (SQLException sqle) {}

		try {
			s.execute("drop trigger trigger2");
		} catch (SQLException sqle) {}

		//table1 is the main table on which all the testing is done and it 
		//uses table2 at times to do DMLs as part of it's trigger action.
		s.execute("create table table1 (id int, status smallint, bl blob(2G))");
		s.execute("create index i1 on table1(id)");
		//table2 is mostly used as part of the trigger action for table1
		s.execute("create table table2 (id int, updates int default 0)");
		s.execute("create index i2 on table2(id)");
		//table3 does not have lob. It is mostly used to show how things work
		//fine when they may not for table1 since table1 has LOB column.
		s.execute("create table table3 (id int, status smallint, score int)");
		s.execute("create index i3 on table3(id)");

		//load data in table1
		PreparedStatement ps = prepareStatement(
		"insert into table1 values (?, 0, ?)");
		ps.setInt(1, 1);
        ps.setBinaryStream(2, new LoopingAlphabetStream(lobsize), lobsize);
        ps.executeUpdate();

		//load data in table2
		ps = prepareStatement(
			"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
        ps.executeUpdate();

		//load data in table3
		ps = prepareStatement(
			"insert into table3 values (?, 0, ?)");
		ps.setInt(1, 1);
		ps.setInt(2, 2);
        ps.executeUpdate();

		commit();
	}

	/**
	 * This test creates an AFTER INSERT trigger which inserts non-lob
	 * columns into another table. The triggering INSERT does not insert
	 * any value into LOB column
	 * @throws SQLException
	 */
	public void test1InsertAfterTrigger() throws SQLException{	
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 AFTER INSERT on table1 referencing " +
			"new as n_row for each row " +
			"insert into table2(id, updates) values (n_row.id, -1)");
		commit();
   		runtest1InsertTriggerTest();		       	
	}

	/**
	 * The test case is exactly like test1InsertAfterTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1InsertAfterTrigger gets done inside the stored procedure
	 * for this test.
	 * @throws SQLException
	 */
	public void test1InsertAfterTriggerStoredProc() throws SQLException{
        // JSR169 cannot run with tests with stored procedures
        // that do database access - for they require a
        // DriverManager connection to jdbc:default:connection;
        // DriverManager is not supported with JSR169.
        if (JDBC.vmSupportsJSR169()) 
        	return;
        basicSetup();
        Statement s = createStatement();
        s.execute("create procedure proc_test1_InsertAfterTrigger_update_table " +
        		"(p1 int) parameter style java language "+
        		"java MODIFIES SQL DATA external name "+
        		"'org.apache.derbyTesting.functionTests.tests.memory.TriggerTests.proc_test1_InsertAfterTrigger_update_table'");
		s.execute("create trigger trigger1 after INSERT on table1 referencing " +
			"new as n_row for each row " +
			"call proc_test1_InsertAfterTrigger_update_table(n_row.id)");
		commit();
   		runtest1InsertTriggerTest();		       	
	}

	/**
	 * The is the stored procedure which gets called by the after insert 
	 * trigger action for the test test1InsertAfterTriggerStoredProc
	 * @param p1 new value of table1.id after the row gets inserted
	 * @throws SQLException
	 */
	public static void proc_test1_InsertAfterTrigger_update_table(int p1) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement(
        		"insert into table2(id, updates) values (" + p1 + ",-1)");
        ps.executeUpdate();
        conn.close();
	}

	/**
	 * This test creates an AFTER DELETE trigger which delets from another
	 * table using non-lob from the triggering table in the where clause.
	 * 
	 * DELETE triggers read all the columns from the trigger table. Following
	 * test is on a trigger table with large data in LOB columns and hence it
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test1DeleteAfterTrigger() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 after DELETE on table1 referencing " +
				"old as o_row for each row " +
				"delete from table2 where id=o_row.id");
		commit();
		runDeleteTriggerTest();		       	
	}

	/**
	 * The test case is exactly like test1DeleteAfterTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1DeleteAfterTrigger gets done inside the stored procedure
	 * for this test.
	 * 
	 * DELETE triggers read all the columns from the trigger table. Following
	 * test is on a trigger table with large data in LOB columns and hence it
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test1DeleteAfterTriggerStoredProc() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();
        s.execute("create procedure proc_test1_DeleteAfterTrigger_update_table " +
        		"(p1 int) parameter style java language "+
        		"java MODIFIES SQL DATA external name "+
        		"'org.apache.derbyTesting.functionTests.tests.memory.TriggerTests.proc_test1_DeleteAfterTrigger_update_table'");

		s.execute("create trigger trigger1 after DELETE on table1 referencing " +
				"old as o_row for each row " +
				"call proc_test1_DeleteAfterTrigger_update_table(o_row.id)");
		commit();
		runDeleteTriggerTest();		       	
	}

	/**
	 * The is the stored procedure which gets called by the after delete 
	 * trigger action for the test test1DeleteAfterTriggerStoredProc
	 * @param p1 old value of table1.id before the row gets deleted
	 * @throws SQLException
	 */
	public static void proc_test1_DeleteAfterTrigger_update_table(int p1) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement(
        		"delete from table1 where id=" + p1);
        ps.executeUpdate();
        conn.close();
	}
	
	/**
	 * This test creates an AFTER UPDATE trigger which is declared on a
	 * non-LOB column. The trigger action does not access the LOB column.
	 * 
	 * It uses 2 tables to demonstrate the problem. 
	 * table1 has a BLOB column
	 * table2 gets updated as part of AFTER UPDATE trigger of 
	 * 	non-BLOB column on table1
	 * 
	 * table1 has an after update trigger defined on column "status" so
	 * 	that table2 will get updated as part of trigger action
	 * 
	 * Notice that the trigger does not reference the BLOB column in 
	 * 	table1 and update that caused the trigger is not updating the 
	 * 	BLOB column
	 * 
	 * @throws SQLException
	 */
	public void test1UpdateAfterTrigger() throws SQLException{
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 after update of status on table1 referencing " +
			"new as n_row for each row " +
			"update table2 set updates = updates + 1 where table2.id = n_row.id");
		commit();
		runtest1UpdateTrigger();
	}

	/**
	 * The test case is exactly like test1UpdateAfterTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1UpdateAfterTrigger gets done inside the stored procedure
	 * for this test.
	 * @throws SQLException
	 */
	public void test1UpdateAfterTriggerStoredProc() throws SQLException{
        // JSR169 cannot run with tests with stored procedures
        // that do database access - for they require a
        // DriverManager connection to jdbc:default:connection;
        // DriverManager is not supported with JSR169.
        if (JDBC.vmSupportsJSR169()) 
        	return;
        basicSetup();
        Statement s = createStatement();
        s.execute("create procedure proc_test1_UpdateAfterTrigger_update_table " +
        		"(p1 int) parameter style java language "+
        		"java MODIFIES SQL DATA external name "+
        		"'org.apache.derbyTesting.functionTests.tests.memory.TriggerTests.proc_test1_UpdateAfterTrigger_update_table'");

		s.execute("create trigger trigger1 after update of status on table1 REFERENCING " +
				"NEW as n_row for each row call proc_test1_UpdateAfterTrigger_update_table(n_row.id)");
		commit();
   		runtest1UpdateTrigger();
	}

	/**
	 * The is the stored procedure which gets called by the after update 
	 * trigger action for the test test1UpdateAfterTriggerStoredProc
	 * @param p1 new value of table1.id after the row gets updated
	 * @throws SQLException
	 */
	public static void proc_test1_UpdateAfterTrigger_update_table(int p1) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement("update table2 "+
        		"set updates = updates + 1 where table2.id = " + p1);
        ps.executeUpdate();
        conn.close();
	}

	/**
	 * This test creates a BEFORE INSERT trigger which selects 
	 * columns from another table using "new" non-lob column for 
	 * join clause.
	 * @throws SQLException
	 */
	public void test1InsertBeforeTrigger() throws SQLException{
		
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 no cascade before INSERT on table1 referencing " +
			"new as n_row for each row " +
			"select updates from table2 where table2.id = n_row.id");
		commit();
   		runtest1InsertTriggerTest();		       	
	}

	/**
	 * The test case is exactly like test1InsertBeforeTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1InsertBeforeTrigger gets done inside the stored procedure
	 * for this test.
	 * @throws SQLException
	 */
	public void test1InsertBeforeTriggerStoredProc() throws SQLException{
        // JSR169 cannot run with tests with stored procedures
        // that do database access - for they require a
        // DriverManager connection to jdbc:default:connection;
        // DriverManager is not supported with JSR169.
        if (JDBC.vmSupportsJSR169()) 
        	return;
        basicSetup();
        Statement s = createStatement();
        s.execute("create procedure proc_test1_InsertBeforeTrigger_select_table " +
        		"(p1 int) parameter style java language "+
        		"java READS SQL DATA external name "+
        		"'org.apache.derbyTesting.functionTests.tests.memory.TriggerTests.proc_test1_InsertBeforeTrigger_select_table'");
		s.execute("create trigger trigger1 no cascade before INSERT on table1 referencing " +
			"new as n_row for each row call proc_test1_InsertBeforeTrigger_select_table(n_row.id)");
		commit();
		runtest1InsertTriggerTest();
	}
	
	/**
	 * The is the stored procedure which gets called by the before insert 
	 * trigger action for the test test1InsertBeforeTriggerStoredProc
	 * @param p1 new value of table1.id after the row gets inserted
	 * @throws SQLException
	 */
	public static void proc_test1_InsertBeforeTrigger_select_table(int p1) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement("select updates from " +
        		"table2 where table2.id = " + p1);
        ps.executeQuery();
        conn.close();
	}

	/**
	 * This test creates a BEFORE DELETE trigger which selects 
	 * columns from another table using "new" non-lob column for 
	 * join clause.
	 * 
	 * DELETE triggers read all the columns from the trigger table. Following
	 * test is on a trigger table with large data in LOB columns and hence it
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test1DeleteBeforeTrigger() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 no cascade before DELETE on table1 referencing " +
				"old as o_row for each row " +
				"select updates from table2 where table2.id = o_row.id");
		commit();
   		runDeleteTriggerTest();		       	
	}

	/**
	 * The test case is exactly like test1DeleteBeforeTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1DeleteBeforeTrigger gets done inside the stored procedure
	 * for this test.
	 * 
	 * DELETE triggers read all the columns from the trigger table. Following
	 * test is on a trigger table with large data in LOB columns and hence it
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test1DeleteBeforeTriggerStoredProc() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
       basicSetup();
        Statement s = createStatement();

        s.execute("create procedure proc_test1_DeleteBeforeTrigger_select_table " +
        		"(p1 int) parameter style java language "+
        		"java READS SQL DATA external name "+
        		"'org.apache.derbyTesting.functionTests.tests.memory.TriggerTests.proc_test1_DeleteBeforeTrigger_select_table'");

        s.execute("create trigger trigger1 no cascade before DELETE on table1 referencing " +
				"old as o_row for each row call proc_test1_DeleteBeforeTrigger_select_table(o_row.id)");
		commit();
   		runDeleteTriggerTest();		       	
	}
	
	/**
	 * The is the stored procedure which gets called by the before delete 
	 * trigger action for the test test1DeleteBeforeTriggerStoredProc
	 * @param p1 old value of table1.id before the row gets deleted
	 * @throws SQLException
	 */
	public static void proc_test1_DeleteBeforeTrigger_select_table(int p1) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement("select updates from " +
        		"table2 where table2.id = " + p1);
        ps.executeQuery();
        conn.close();
	}

	/**
	 * This test creates a BEFORE UPDATE trigger which is declared
	 * on a non-LOB column. The trigger action selects columns from 
	 * another table using "new" non-lob column for join clause. 
	 * 
	 * It uses 2 tables to demonstrate the problem. 
	 * table1 has a BLOB column
	 * table2 gets updated as part of AFTER UPDATE trigger of 
	 * 	non-BLOB column on table1
	 * 
	 * table1 has a before update trigger defined on column "status" so
	 * that there will be a select done from table2 as part of trigger 
	 * action.
	 * 
	 * Notice that the trigger does not reference the BLOB column in 
	 * 	table1 and update that caused the trigger is not updating the 
	 * 	BLOB column
	 * 
	 * @throws SQLException
	 */
	public void test1UpdateBeforeTrigger() throws SQLException{
        basicSetup();
        Statement s = createStatement();

		s.execute("create trigger trigger1 no cascade before update of status on table1 referencing " +
			"new as n_row for each row " +
			"select updates from table2 where table2.id = n_row.id");
		commit();
   		runtest1UpdateTrigger();
	}

	/**
	 * The test case is exactly like test1UpdateBeforeTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test1UpdateBeforeTrigger gets done inside the stored procedure
	 * for this test.
	 * @throws SQLException
	 */
	public void test1UpdateBeforeTriggerStoredProc() throws SQLException{
        // JSR169 cannot run with tests with stored procedures
        // that do database access - for they require a
        // DriverManager connection to jdbc:default:connection;
        // DriverManager is not supported with JSR169.
        if (JDBC.vmSupportsJSR169()) 
        	return;
        basicSetup();
        Statement s = createStatement();
        s.execute("create procedure proc_test1_UpdateBeforeTrigger_select_table " +
        		"(p1 int) parameter style java language "+
        		"java READS SQL DATA external name "+
        		"'org.apache.derbyTesting.functionTests.tests.memory.TriggerTests.proc_test1_UpdateBeforeTrigger_select_table'");

		s.execute("create trigger trigger1 no cascade before update of status on table1 REFERENCING " +
				"NEW as n_row for each row call proc_test1_UpdateBeforeTrigger_select_table(n_row.id)");
		commit();
   		runtest1UpdateTrigger();
	}
	
	/**
	 * The is the stored procedure which gets called by the before update 
	 * trigger action for the test test1UpdateBeforeTriggerStoredProc
	 * @param p1 new value of table1.id after the row gets updated
	 * @throws SQLException
	 */
	public static void proc_test1_UpdateBeforeTrigger_select_table(int p1) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement("select updates from " +
        		"table2 where table2.id = " + p1);
        ps.executeQuery();
        conn.close();
	}

	/**
	 * This test creates an AFTER INSERT trigger which in it's trigger action
	 * inserts lob columns from triggering table into another table. So, this
	 * test does access the LOB from the triggering table inside the trigger
	 * action.
	 * 
	 * INSERT trigger in this test is inserting large data in the LOB column
	 * which will be used in the INSERT trigger and hence it will run out of 
	 * memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test2InsertAfterTriggerAccessLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();

        //The default table2 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table2");
		s.execute("create table table2 (id int, bl_table2 blob(2G))");

		PreparedStatement ps = prepareStatement(
			"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
        ps.executeUpdate();

		s.execute("create trigger trigger1 after INSERT on table1 referencing " +
				"new as n_row for each row " +
				"insert into table2(id, bl_table2) values (n_row.id, n_row.bl)");
		commit();
   		runtest2InsertTriggerTest();
	}

	/**
	 * This test creates an AFTER DELETE trigger which in it's trigger action
	 * deletes row from another table using triggering table's "new" LOB value
	 * in the join clause. So, this test does access the LOB from the 
	 * triggering table inside the trigger action.
	 * 
	 * DELETE triggers read all the columns from the trigger table. Following
	 * test is on a trigger table with large data in LOB columns and hence it
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test2DeleteAfterTriggerAccessLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();
        //The default table2 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table2");
		s.execute("create table table2 (id int, bl_table2 blob(2G))");

		PreparedStatement ps = prepareStatement(
			"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
        ps.executeUpdate();
		commit();

		s.execute("create trigger trigger1 after DELETE on table1 referencing " +
				"old as o_row for each row " +
				"delete from table2 where id = o_row.id and o_row.bl is not null");
		commit();
   		runDeleteTriggerTest();
	}

	/**
	 * The after update trigger on non-LOB column but the LOB column is
	 * referenced in the trigger action. So, this test does access the LOB 
	 * from the triggering table inside the trigger action. 
	 *  
	 * It uses 2 tables to demonstrate the problem. 
	 * table1 has a BLOB column
	 * table2 gets updated with LOB column from triggering table
	 * 	eventhough the UPDATE which caused the trigger to fire didn't
	 * 	update the LOB. The trigger got fired for update of non-LOB
	 * 	column on the triggering table. 
	 * 
	 * table1 has an after update trigger defined on column "status" so
	 * 	that the trigger action will update table2 with LOB value from 
	 * 	table1.
	 * 
	 * Notice that the trigger action DOES reference the BLOB column in 
	 * 	table1 but the update that caused the trigger is not updating 
	 * 	the BLOB column
	 * 
	 * UPDATE trigger in this test is working with large data in the LOB column
	 * inside the trigger action and hence it will run out of memory. For that 
	 * reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test2UpdateAfterTriggerAccessLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();
        //The default table2 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table2");
		s.execute("create table table2 (id int, bl_table2 blob(2G))");

		s.execute("create trigger trigger1 after update of status on table1 referencing " +
			"new as n_row for each row " +
			"update table2 set bl_table2 = n_row.bl where table2.id = n_row.id");

		PreparedStatement ps = prepareStatement(
			"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
        ps.executeUpdate();
		commit();
   		runtest1UpdateTrigger();
	}

	/**
	 * This test creates an AFTER INSERT trigger which in it's trigger action
	 * updates a lob column from the row just inserted. So, this test does
	 * update the LOB from the triggering table inside the trigger
	 * action.
	 * 
	 * INSERT trigger in this test is inserting large data in the LOB column
	 * which will be used in the INSERT trigger and hence it will run out of 
	 * memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test2InsertAfterTriggerUpdatedLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();

        //The default table1 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table1");
		s.execute("create table table1 (id int, status smallint, bl blob(2G), bl_null blob(2G))");

		PreparedStatement ps = prepareStatement(
			"insert into table1 values (?, 0, ?, null)");
		ps.setInt(1, 1);
        ps.setBinaryStream(2, new LoopingAlphabetStream(lobsize), lobsize);
        ps.executeUpdate();

        s.execute("create trigger trigger1 after INSERT on table1 referencing " +
				"new as n_row for each row " +
				"update table1 set bl_null=n_row.bl where bl_null is null");
		commit();
   		runtest2InsertTriggerTest();
	}

	/**
	 * This test creates an AFTER UPDATE trigger which in it's trigger action
	 * updates a lob column from the row that just got updated. So, this test 
	 * does update the LOB from the triggering table inside the trigger
	 * action. 
	 * 
	 * UPDATE trigger in this test is working with large data in the LOB column
	 * inside the trigger action and hence it will run out of memory. For that 
	 * reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test2UpdateAfterTriggerUpdatedLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();

        //The default table1 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table1");
		s.execute("create table table1 (id int, status smallint, bl blob(2G), bl_null blob(2G))");

        s.execute("create trigger trigger1 after update of status on table1 referencing " +
    			"new as n_row for each row " +
    			"update table1 set bl_null=n_row.bl where bl_null is null");

		PreparedStatement ps = prepareStatement(
			"insert into table1 values (?, 0, ?, null)");
		ps.setInt(1, 1);
        ps.setBinaryStream(2, new LoopingAlphabetStream(lobsize), lobsize);
        ps.executeUpdate();
		commit();
   		runtest1UpdateTrigger();
	}

	/**
	 * This test creates a BEFORE INSERT trigger which selects "new"
	 * lob column from just inserted row. This test does access the
	 * LOB.
	 * 
	 * INSERT trigger in this test is inserting large data in the LOB column
	 * which will be used in the INSERT trigger action and hence it will run  
	 * out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test2InsertBeforeTriggerAccessLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();
        //The default table2 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table2");
		s.execute("create table table2 (id int, bl_table2 blob(2G))");

		s.execute("create trigger trigger1 no cascade before INSERT on table1 referencing " +
			"new as n_row for each row " +
			"values(n_row.bl)");		
		
		PreparedStatement ps = prepareStatement(
			"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
        ps.executeUpdate();
		commit();
   		runtest2InsertTriggerTest();
	}
	
	/**
	 * This test creates a BEFORE DELETE trigger which selects "old"
	 * lob column from just deleted row. This test does access the
	 * LOB.
	 * 
	 * DELETE triggers read all the columns from the trigger table. Following
	 * test is on a trigger table with large data in LOB columns and hence it
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test2DeleteBeforeTriggerAccessLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();

        //The default table2 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table2");
		s.execute("create table table2 (id int, bl_table2 blob(2G))");

		s.execute("create trigger trigger1 no cascade before DELETE on table1 referencing " +
			"old as o_row for each row " +
			"values(o_row.bl)");		
		
		PreparedStatement ps = prepareStatement(
			"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
        ps.executeUpdate();
		commit();
   		runDeleteTriggerTest();
	}

	/**
	 * This test creates a BEFORE UPDATE trigger which selects "new"
	 * lob column from just updated row. This test does access the
	 * LOB. 
	 * 
	 * UPDATE trigger in this test is working with large data in the LOB column
	 * inside the trigger action and hence it will run out of memory. For that 
	 * reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test2UpdateBeforeTriggerAccessLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();

        //The default table2 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table2");
		s.execute("create table table2 (id int, bl_table2 blob(2G))");

		s.execute("create trigger trigger1 no cascade before update of status on table1 referencing " +
			"new as n_row for each row " +
			"values(n_row.bl)");		
		
		PreparedStatement ps = prepareStatement(
			"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
        ps.executeUpdate();
		commit();
   		runtest1UpdateTrigger();
	}

	/**
	 * The after update trigger is defined on LOB column but the LOB column 
	 * is not referenced in the trigger action.
	 * 
	 * It used 2 tables to demonstrate the problem.
	 * table1 has a BLOB column
	 * table2 gets updated with non-LOB column from triggering table
	 * 	eventhough the UPDATE which caused the trigger to fire updated a LOB
	 * column. The trigger got fired for update of LOB column on the triggering
	 * table.
	 * 
	 * table1 has an after update trigger defined on LOB column so that 
	 * 	the trigger action will update table2 with non-LOB value from
	 * 	table1
	 * 
	 * UPDATE trigger is defined on LOB column with large data and hence it 
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test3UpdateAfterTrigger() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 after update of bl on table1 referencing " +
				"new as n_row for each row " +
				"update table2 set updates = n_row.status where table2.id = n_row.id");
		commit();
   		runtest2UpdateTrigger();
	}

	/**
	 * The test case is exactly like test3UpdateAfterTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test3UpdateAfterTrigger gets done inside the stored procedure
	 * for this test.
	 * 
	 * UPDATE trigger is defined on LOB column with large data and hence it 
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test3UpdateAfterTriggerStoredProc() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();
	
        s.execute("create procedure proc_test3_UpdateAfterTrigger_update_table " +
        		"(p1 int, p2 int) parameter style java language "+
        		"java MODIFIES SQL DATA external name "+
        		"'org.apache.derbyTesting.functionTests.tests.memory.TriggerTests.proc_test3_UpdateAfterTrigger_update_table'");

		s.execute("create trigger trigger1 after update of bl on table1 REFERENCING " +
				"NEW as n_row for each row call proc_test3_UpdateAfterTrigger_update_table(n_row.status, n_row.id)");
		commit();
   		runtest2UpdateTrigger();
	}

	/**
	 * The is the stored procedure which gets called by the after delete 
	 * trigger action for the test test3UpdateAfterTriggerStoredProc
	 * @param p1 new value of table1.status after the row gets inserted
	 * @param p2 new value of table1.id after the row gets inserted
	 * @throws SQLException
	 */
	public static void proc_test3_UpdateAfterTrigger_update_table(int p1, int p2) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement("update table2 "+
        		"set updates = " + p1 + " where table2.id = " + p2);
        ps.executeUpdate();
        conn.close();
	}

	/**
	 * This test creates a BEFORE UPDATE trigger which selects a row
	 * from another table using "new" non-LOB column from the triggering
	 * table. This test has update trigger defined on the LOB column
	 * but does not access/update that LOB column in the trigger action.
	 * 
	 * UPDATE trigger is defined on LOB column with large data and hence it 
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test3UpdateBeforeTrigger() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();

		s.execute("create trigger trigger1 no cascade before update of bl on table1 referencing " +
				"new as n_row for each row " +
				"select updates from table2 where table2.id = n_row.id");
		commit();
   		runtest2UpdateTrigger();
	}

	/**
	 * The test case is exactly like test3UpdateBeforeTrigger except that the
	 * trigger action is a stored procedure call. The work done by the trigger
	 * action SQL in test3UpdateBeforeTrigger gets done inside the stored procedure
	 * for this test.
	 * 
	 * UPDATE trigger is defined on LOB column with large data and hence it 
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test3UpdateBeforeTriggerStoredProc() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();

        s.execute("create procedure proc_test3_UpdateBeforeTrigger_select_table " +
        		"(p1 int) parameter style java language "+
        		"java READS SQL DATA external name "+
        		"'org.apache.derbyTesting.functionTests.tests.memory.TriggerTests.proc_test3_UpdateBeforeTrigger_select_table'");

		s.execute("create trigger trigger1 no cascade before update of bl on table1 REFERENCING " +
				"NEW as n_row for each row call proc_test3_UpdateBeforeTrigger_select_table(n_row.id)");
		commit();
   		runtest2UpdateTrigger();
	}
	
	/**
	 * The is the stored procedure which gets called by the before delete 
	 * trigger action for the test test3UpdateBeforeTriggerStoredProc
	 * @param p1 new value of table1.id after the row gets inserted
	 * @throws SQLException
	 */
	public static void proc_test3_UpdateBeforeTrigger_select_table(int p1) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement("select updates from " +
        		"table2 where table2.id = " + p1);
        ps.executeQuery();
        conn.close();
	}
	
	/**
	 * The after update trigger on LOB column but the LOB column is referenced 
	 * in the trigger action. This is one case though where we do need to keep 
	 * before and after image since the LOB got updated and it is being used 
	 * in trigger action. 
	 * 
	 * It used 2 tables to demonstrate the problem.
	 * table1 has a BLOB column
	 * table2 gets updated with LOB column value from triggering table,
	 * 	the same LOB which got UPDATEd and caused the trigger to fire. The 
	 * 	trigger got fired for update of LOB column on the triggering
	 * 	table.
	 * 
	 * UPDATE trigger is defined on LOB column with large data and hence it 
	 * will run out of memory. For that reason, the test is disabled.
	 * 
	 * @throws SQLException
	 */
	public void test4UpdateAfterTriggerAccessLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
	    Statement s = createStatement();

        //The default table2 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table2");
		s.execute("create table table2 (id int, bl_table2 blob(2G))");

		s.execute("create trigger trigger1 after update of bl on table1 referencing " +
			"new as n_row for each row " +
			"update table2 set bl_table2 = n_row.bl where table2.id = n_row.id");

		PreparedStatement ps = prepareStatement(
			"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
        ps.executeUpdate();
		commit();		
   		runtest2UpdateTrigger();
	}
	
	/**
	 * The after update trigger on LOB column which then gets updated in the
	 * trigger action. So this test updates the LOB in the trigger action
	 * and is also the cause of the update trigger to fire.
	 * 
	 * The UPDATE trigger access the large data in LOB column inside the 
	 * trigger action which will cause the test to run out of memory. For
	 * this reason, this test is disabled.
	 *  
	 * @throws SQLException
	 */ 
	public void test4UpdateAfterTriggerUpdatedLOB() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
        Statement s = createStatement();

        //The default table1 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table1");
		s.execute("create table table1 (id int, status smallint, bl blob(2G), bl_null blob(2G))");

        s.execute("create trigger trigger1 after update of bl_null on table1 referencing " +
    			"new as n_row for each row " +
    			"update table1 set bl_null=n_row.bl where bl_null is null");

		PreparedStatement ps = prepareStatement(
			"insert into table1 values (?, 0, ?, ?)");

		ps.setInt(1, 1);
        ps.setBinaryStream(2, new LoopingAlphabetStream(lobsize), lobsize);
        ps.setBinaryStream(3, new LoopingAlphabetStream(lobsize), lobsize);
        ps.executeUpdate();
		commit();
   		runtest3UpdateTrigger();
	}
	
	/**
	 * This test creates a BEFORE UPDATE trigger on LOB column and
	 * the trigger action selects "new" lob column from just updated 
	 * row. This test does access the LOB. 
	 * 
	 * The UPDATE trigger access the large data in LOB column inside the 
	 * trigger action and it is defined on the LOB column which will cause 
	 * the test to run out of memory. For this reason, this test is disabled.
	 *  
	 * @throws SQLException
	 */
	public void test4UpdateBeforeTrigger() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
	    Statement s = createStatement();

        //The default table2 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table2");
		s.execute("create table table2 (id int, bl_table2 blob(2G))");

		s.execute("create trigger trigger1 no cascade before update of bl on table1 referencing " +
			"new as n_row for each row " +
			"values(n_row.bl)");

		PreparedStatement ps = prepareStatement(
			"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
        ps.executeUpdate();
		commit();		
   		runtest2UpdateTrigger();
	}
	 
	/**
	 * This test creates an AFTER INSERT trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action.
	 * @throws SQLException
	 */
	public void test5InsertAfterTriggerNoReferencingClause() throws SQLException{
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 AFTER INSERT on table1 " +
			"insert into table2(id, updates) values (100, -1)");
		commit();
   		runtest1InsertTriggerTest();		       	
	}
	 
	/**
	 * This test creates an BEFORE INSERT trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action.
	 * @throws SQLException
	 */
	public void test5InsertBeforeTriggerNoReferencingClause() throws SQLException{
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 NO CASCADE BEFORE INSERT on table1 " +
			"select updates from table2 where table2.id = 1");
		commit();
   		runtest1InsertTriggerTest();		       	
	}
	 
	/**
	 * This test creates an AFTER DELETE trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action.
	 * @throws SQLException
	 */
	public void test5DeleteAfterTriggerNoReferencingClause() throws SQLException{
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 AFTER DELETE on table1 " +
				"delete from table2 where id=1");
		commit();
   		runDeleteTriggerTest();		       	
	}
	
	/**
	 * This test creates an BEFORE DELETE trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action. 
	 * @throws SQLException
	 */
	public void test5DeleteBeforeTriggerNoReferencingClause() throws SQLException{
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 NO CASCADE BEFORE DELETE on table1 " +
				"select updates from table2 where table2.id = 1");
		commit();
   		runDeleteTriggerTest();		       	
	}

	/**
	 * This test creates an AFTER UPDATE trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action. 
	 * @throws SQLException
	 */
	public void test5UpdateAfterTriggerNoReferencingClause() throws SQLException{
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 AFTER UPDATE of status on table1 " +
				"update table2 set updates = updates + 1 where table2.id = 1");
		commit();
   		runtest1UpdateTrigger();		       	
	}
	
	/**
	 * This test creates an BEFORE UPDATE trigger but has no REFERENCING
	 * clause, meaning that before and after values are not available to
	 * the trigger action. 
	 * @throws SQLException
	 */
	public void test5UpdateBeforeTriggerNoReferencingClause() throws SQLException{
        basicSetup();
        Statement s = createStatement();
		s.execute("create trigger trigger1 NO CASCADE BEFORE UPDATE of status on table1 " +
				"select updates from table2 where table2.id = 1");
		commit();
   		runtest1UpdateTrigger();		       	
	}

	/**
	 * This test create an AFTER UPDATE trigger but does not identify any
	 * trigger columns. It has REFERENCING clause. Void of trigger columns
	 * will cause all the columns to be read into memory.
	 * 
	 * When no trigger columns are defined for an UPDATE trigger, all the 
	 * columns get read into memory. Since the trigger table has large data
	 * in LOB columns, it will run out of memory. For that reason, the test 
	 * is disabled.
	 */
	public void test6UpdateAfterTriggerNoTriggerColumn() throws SQLException{
		if (testWithLargeDataInLOB)
			return;
		
        basicSetup();
	    Statement s = createStatement();

        //The default table2 created by basicSetup does not match the 
        //requirement of this test so dropping and recreating it.
        s.execute("drop table table2");
		s.execute("create table table2 (id int, bl_table2 blob(2G))");
		s.execute("create trigger trigger1 after update on table1 referencing " +
				"new as n_row for each row " +
				"update table2 set bl_table2 = n_row.bl where table2.id = n_row.id");

		PreparedStatement ps = prepareStatement(
				"insert into table2 (id) values (?)");
		ps.setInt(1, 1);
	    ps.executeUpdate();
		commit();		
 		runtest2UpdateTrigger();
	}

	/**
	 * Following will do an insert into table1 which will cause insert 
	 * trigger to fire. The insert does not involve the LOB column.
	 *
	 * @throws SQLException
	 */
	public void runtest1InsertTriggerTest() throws SQLException{
		PreparedStatement ps = prepareStatement(
				"insert into table1(id, status) values(101, 0)");
        ps.executeUpdate();
        commit();
	}
	
	/**
	 * Following will do an insert into table1 which will cause insert 
	 * trigger to fire. The insert involves the LOB column.
	 *
	 * @throws SQLException
	 */
	public void runtest2InsertTriggerTest() throws SQLException{
		PreparedStatement ps = prepareStatement(
				"insert into table1(id, status, bl) values(101, 0, ?)");
        ps.setBinaryStream(1, new LoopingAlphabetStream(lobsize), lobsize);
        ps.executeUpdate();
        commit();
	}
	
	/**
	 * Following will update a row in table1 which will cause update 
	 * trigger to fire. The update does not involve the LOB column.
	 *
	 * @throws SQLException
	 */
	public void runtest1UpdateTrigger() throws SQLException{
		PreparedStatement ps = prepareStatement(
				"update table1 set status = 1 where id = 1");
        ps.executeUpdate();
        commit();
	}
	
	/**
	 * Following will update a row in table1 which will cause update 
	 * trigger to fire. The update involves the LOB column.
	 *
	 * @throws SQLException
	 */
	public void runtest2UpdateTrigger() throws SQLException{
		PreparedStatement ps = prepareStatement(
				"update table1 set bl = ? where id = 1");
        ps.setBinaryStream(1, new LoopingAlphabetStream(lobsize), lobsize);
        ps.executeUpdate();
        commit();
	}
	
	/**
	 * Following will update a row in table1 which will cause update 
	 * trigger to fire. The update involves the LOB column.
	 *
	 * @throws SQLException
	 */
	public void runtest3UpdateTrigger() throws SQLException{
		PreparedStatement ps = prepareStatement(
				"update table1 set bl_null=null where id = 1");
        ps.executeUpdate();
        commit();
	}
	
	/**
	 * Following will delete a row from table1 which will cause delete 
	 * trigger to fire. 
	 *
	 * @throws SQLException
	 */
	public void runDeleteTriggerTest() throws SQLException{
		PreparedStatement ps = prepareStatement(
				"delete from table1 where id=1");
        ps.executeUpdate();
        commit();
	}
}
