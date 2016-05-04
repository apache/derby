/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.TriggerTest

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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.IOException;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest;
import org.apache.derbyTesting.functionTests.util.streams.ByteAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.functionTests.util.streams.ReadOnceByteArrayInputStream;
import org.apache.derbyTesting.functionTests.util.streams.StringReaderWithLength;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.XML;

/**
 * Test triggers.
 *
 */
public class TriggerTest extends BaseJDBCTestCase {

    private static final String SYNTAX_ERROR = "42X01";
    private static final String HAS_DEPENDENT_SPS = "X0Y24";
    private static final String HAS_DEPENDENT_TRIGGER = "X0Y25";
    private static final String TRIGGER_DROPPED = "01502";
    private static final String FOREIGN_KEY_VIOLATION = "23503";
   
    /**
     * Thread local that a trigger can access to
     * allow recording information about the firing.
     */
    private static ThreadLocal<List<String>> TRIGGER_INFO =
            new ThreadLocal<List<String>>();
    StringBuffer listOfCreatedTriggers = new StringBuffer();


    public TriggerTest(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }
    
    /**
     * Run only in embedded as TRIGGERs are server side logic.
     * Also the use of a ThreadLocal to check state requires
     * embedded. 
     */
    public static Test suite() {
        return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(TriggerTest.class));
        
    }
    
    protected void initializeConnection(Connection conn) throws SQLException
    {
        conn.setAutoCommit(false);
    }
    
    protected void setUp() throws Exception
    {
        //DERBY-5866( testFiringConstraintOrder(
        // org.apache.derbyTesting.functionTests.tests.lang.TriggerTest)
        // junit.framework.AssertionFailedError: 
        // matching triggers need to be fired in order creation:
        // 1,NO CASCADE BEFORE,DELETE,ROW )
        //Do the cleanup here rather than in tearDown. This way, if a test
        // fixture fails, we will have the left over wombat database with
        // the schema and data used by the failing fixture.
        JDBC.dropSchema(getConnection().getMetaData(),
                getTestConfiguration().getUserName());
        Statement s = createStatement();
        s.executeUpdate("CREATE PROCEDURE TRIGGER_LOG_INFO(" +
                "O VARCHAR(255)) " +
                "NO SQL PARAMETER STYLE JAVA LANGUAGE JAVA " +
                "EXTERNAL NAME " +
                "'" + getClass().getName() + ".logTriggerInfo'");
        s.close();

    }

    protected void tearDown() throws Exception
    {
        TRIGGER_INFO.set(null);
        super.tearDown();
    }

    /**
     * DERBY-6383(Update trigger defined on one column fires on update 
     * of other columns). This regression is caused by DERBY-4874(Trigger 
     * does not recognize new size of VARCHAR column expanded with 
     * ALTER TABLE. It fails with ERROR 22001: A truncation error was 
     * encountered trying to shrink VARCHAR)
     *  The regression is for Statement level triggers. The trigger
     *  gets fired for any column update rather than just the column
     *  specified in the UPDATE of column clause. Following test
     *  confirms that fix for DERBY-6383 fixes the issue.
     * 
     * @throws SQLException 
     * 
     */
    public void testDerby6383StatementTriggerBugTst1() throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE DERBY_6368_TAB1 (X INTEGER, Y INTEGER)");
        s.executeUpdate("CREATE TABLE DERBY_6368_TAB2 (X INTEGER, Y INTEGER)");
        s.executeUpdate("INSERT INTO  DERBY_6368_TAB1 VALUES(1, 2)");
        //Create statement trigger on a specific column "X" on DERBY_6368_TAB1
        s.executeUpdate("CREATE TRIGGER t1 AFTER UPDATE OF x "+
            "ON DERBY_6368_TAB1 REFERENCING old table AS old " +
            "INSERT INTO DERBY_6368_TAB2 SELECT * FROM old");
        assertTableRowCount("DERBY_6368_TAB2", 0);
        
        //Following should not fire the trigger since following UPDATE is on
        // column "Y" whereas trigger is defined on column "X"
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET y = y + 1");
        assertTableRowCount("DERBY_6368_TAB2", 0);

        //Create row trigger on a specific column "X" on DERBY_6368_TAB1
        s.executeUpdate("CREATE TRIGGER t2 AFTER UPDATE OF x "+
            "ON DERBY_6368_TAB1 REFERENCING old AS old_row " +
            "for each row " +
            "INSERT INTO DERBY_6368_TAB2(x) values(old_row.x)");

        //Following should not fire any trigger since following UPDATE is on
        // column "Y" whereas triggers are defined on column "X"
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET y = y + 1");
        assertTableRowCount("DERBY_6368_TAB2", 0);

        //Following should fire both triggers since following UPDATE is on
        // column "X" which has two triggers defined on it
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET x = x + 1");
        assertTableRowCount("DERBY_6368_TAB2", 2);

        //Create statement trigger at table level for DERBY_6368_TAB1
        s.executeUpdate("CREATE TRIGGER t3 AFTER UPDATE "+
                "ON DERBY_6368_TAB1 REFERENCING old table AS old " +
                "INSERT INTO DERBY_6368_TAB2 SELECT * FROM old");

        //Following should fire trigger t3 which is supposed to fire for
        // any column update
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET y = y + 1");
        assertTableRowCount("DERBY_6368_TAB2", 3);

        //Following should fire all the triggers since following UPDATE is on
        // column "X" which has two triggers defined on it
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET x = x + 1");
        assertTableRowCount("DERBY_6368_TAB2", 6);
        
        //Add a new column to table
        s.executeUpdate("ALTER TABLE DERBY_6368_TAB1 ADD COLUMN Z int");
        s.executeUpdate("ALTER TABLE DERBY_6368_TAB2 ADD COLUMN Z int");
        
        //Following should fire trigger t3 since any column update should fire
        // trigger t3
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET z = z + 1");
        assertTableRowCount("DERBY_6368_TAB2", 7);
        
        //Following should fire trigger t3 since any column update should fire
        // trigger t3
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET y = y + 1");
        assertTableRowCount("DERBY_6368_TAB2", 8);

        //Following should fire all the triggers since following UPDATE is on
        // column "X" which has two triggers defined on it
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET x = x + 1");
        assertTableRowCount("DERBY_6368_TAB2", 11);

        //drop statement trigger defined on specific column
        s.executeUpdate("drop TRIGGER T1");

        //Following should only fire trigger t3 since any column update should
        // fire trigger t3
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET y = y + 1");
        assertTableRowCount("DERBY_6368_TAB2", 12);

        //Following should fire triggers t2 and t3 since following UPDATE is on
        // column "X" which has row trigger defined on it and a statement 
        // trigger(at table level) defined on it
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET x = x + 1");
        assertTableRowCount("DERBY_6368_TAB2", 14);
        
        //Following should fire trigger t3 since any column update should fire
        // trigger t3
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET z = z + 1");
        assertTableRowCount("DERBY_6368_TAB2", 15);

        //Drop a column from the table. Following will drop trigger t3
        // because it depends on column being dropped. But trigger t2
        // will remain intact since it does not have dependency on
        // column being dropped. So only trigger left at this point
        // will be t2 after the following column drop
        s.executeUpdate("ALTER TABLE DERBY_6368_TAB1 DROP COLUMN Y");
        s.executeUpdate("ALTER TABLE DERBY_6368_TAB2 DROP COLUMN Y");

        //Following should fire triggers t2 since following UPDATE is on
        // column "X" which has row trigger defined on it
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET x = x + 1");
        assertTableRowCount("DERBY_6368_TAB2", 16);
        
        //Following should not fire trigger t2
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET z = z + 1");
        assertTableRowCount("DERBY_6368_TAB2", 16);

        //clean up after the test
        s.executeUpdate("drop table DERBY_6368_TAB1");
        s.executeUpdate("drop table DERBY_6368_TAB2");
    }

    /**
     * DERBY-6383(Update trigger defined on one column fires on update 
     * of other columns). This regression is caused by DERBY-4874(Trigger 
     * does not recognize new size of VARCHAR column expanded with 
     * ALTER TABLE. It fails with ERROR 22001: A truncation error was 
     * encountered trying to shrink VARCHAR)
     * After an update statement level trigger is defined at the table level,
     *  when a new column is added, trigger should fire on update of that
     *  newly added column
     */
    public void testDerby6383StatementTriggerBugTst2() throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE DERBY_6368_TAB1 (X INTEGER, Y INTEGER)");
        s.executeUpdate("CREATE TABLE DERBY_6368_TAB2 (X INTEGER, Y INTEGER)");
        s.executeUpdate("INSERT INTO  DERBY_6368_TAB1 VALUES(1, 2)");

        //Create statement trigger at table level for DERBY_6368_TAB1
        s.executeUpdate("CREATE TRIGGER t1 AFTER UPDATE "+
            "ON DERBY_6368_TAB1 REFERENCING old table AS old " +
            "INSERT INTO DERBY_6368_TAB2 SELECT * FROM old");
        assertTableRowCount("DERBY_6368_TAB2", 0);

        //Following should fire trigger since any column update should fire
        // trigger t1
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET x = x + 1");
        assertTableRowCount("DERBY_6368_TAB2", 1);

        //Following should fire trigger since any column update should fire
        // trigger t1
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET y = y + 1");
        assertTableRowCount("DERBY_6368_TAB2", 2);

        //Add a new column to table
        s.executeUpdate("ALTER TABLE DERBY_6368_TAB1 ADD COLUMN Z int");
        s.executeUpdate("ALTER TABLE DERBY_6368_TAB2 ADD COLUMN Z int");
        //Following should fire trigger since any column update should fire
        // trigger t1
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET z = z + 1");
        assertTableRowCount("DERBY_6368_TAB2", 3);
        
        //Drop a column from the table. this will drop the statement
        // trigger defined at the table level for DERBY_6368_TAB1
        s.executeUpdate("ALTER TABLE DERBY_6368_TAB1 DROP COLUMN X");
        s.executeUpdate("ALTER TABLE DERBY_6368_TAB2 DROP COLUMN X");
        //No triggers left to fire
        s.executeUpdate("UPDATE DERBY_6368_TAB1 SET z = z + 1");
        assertTableRowCount("DERBY_6368_TAB2", 3);

        //clean up after the test
        s.executeUpdate("drop table DERBY_6368_TAB1");
        s.executeUpdate("drop table DERBY_6368_TAB2");
    }

/**
     * Test that invalidating stored statements marks the statement invalid
     *  in SYS.SYSSTATEMENTS. And when one of those invalid statements is
     *  executed next, it is recompiled and as part of that process, it gets
     *  marked valid in SYS.SYSSTATEMENTS.
     * 
     * @throws SQLException 
     * 
     */
    public void testDerby5578InvalidateAllStatementsProc() throws SQLException
    {
        Statement s = createStatement();
        CallableStatement cSt;

        //Invalidate all the statements in SYS.SYSSTATEMENTS.
        cSt = prepareCall(
                "call SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS()");
        assertUpdateCount(cSt, 0);
        cSt.close();
        int numOfRowsInSystatementsBeforeTestStart =
        		numberOfRowsInSysstatements(s);
        int numOfInvalidSystatementsBeforeTestStart =
        		numberOfInvalidStatementsInSysstatements(s);
        int numOfValidSystatementsBeforeTestStart =
        		numberOfValidStatementsInSysstatements(s);

        assertEquals("All statements should be invalid in SYS.SYSSTATEMENTS ",
        		numOfInvalidSystatementsBeforeTestStart,
        		numOfRowsInSystatementsBeforeTestStart);
        assertEquals("No statement should be valid in SYS.SYSSTATEMENTS ",
        		numOfValidSystatementsBeforeTestStart,
        		0);

        //Create the required tables with data. There will be no change to 
        // SYS.SYSSTATEMENTS as of yet.
        s.executeUpdate("create table atdc_16_tab1 (a1 integer, b1 integer, c1 integer)");
        s.executeUpdate("create table atdc_16_tab2 (a2 integer, b2 integer, c2 integer)");
        s.executeUpdate("insert into atdc_16_tab1 values(1,11,111)");
        s.executeUpdate("insert into atdc_16_tab2 values(1,11,111)");
        assertEquals("# of valid statements in SYS.SYSSTATEMENTS should not change",
        		numberOfValidStatementsInSysstatements(s),
        		numOfValidSystatementsBeforeTestStart);
        assertEquals("# of invalid statements in SYS.SYSSTATEMENTS should not change",
        		numberOfInvalidStatementsInSysstatements(s),
        		numOfInvalidSystatementsBeforeTestStart);

        //Create a trigger. Its trigger action plan will result into a new 
        // stored statement in SYS.SYSSTATEMENTS. The stored statement for
        // trigger action will have a valid status
        s.executeUpdate("create trigger atdc_16_trigger_1 "+ 
                "after update of b1 on atdc_16_tab1 " +
                "REFERENCING NEW AS newt "+
                "for each row "+
                "update atdc_16_tab2 set c2 = newt.c1");
        assertEquals("# of valid rows in SYS.SYSSTATEMENTS should be up by "+
                "1 for trigger",
        		numberOfValidStatementsInSysstatements(s),
        		numOfValidSystatementsBeforeTestStart+1);
        assertEquals("# of invalid statements in SYS.SYSSTATEMENTS should not change",
        		numberOfInvalidStatementsInSysstatements(s),
        		numOfInvalidSystatementsBeforeTestStart);

        //Following procedure call will mark all the stored statements 
        // including the one for trigger action plan invalid
        cSt = prepareCall(
                "call SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS()");
        assertUpdateCount(cSt, 0);
        cSt.close();
        assertEquals("All statements should be invalid in SYS.SYSSTATEMENTS ",
        		numberOfInvalidStatementsInSysstatements(s),
        		numOfRowsInSystatementsBeforeTestStart+1);

        //Following will cause the trigger to fire. Since it is in invalid
        // state, it will be compiled and marked valid again in 
        // SYS.SYSSTATEMENTS table
        s.executeUpdate("update atdc_16_tab1 set b1=22,c1=222");
        assertEquals("# of valid rows in SYS.SYSSTATEMENTS should only be 1 ",
        		numberOfValidStatementsInSysstatements(s),
        		1);
        assertEquals("# of invalid statements in SYS.SYSSTATEMENTS should not change",
        		numberOfInvalidStatementsInSysstatements(s),
        		numOfInvalidSystatementsBeforeTestStart);

        //Now let's test metadata related stored statement. Executing it will
        // cause the metadata's stored statement to compile and hence it will
        // be marked valid in SYS.SYSSTATEMENTS table. Now, we will have two
        // stored statements in valid state, one for getTables() metadata and
        // other for trigger action plan
        DatabaseMetaData dbmd = getConnection().getMetaData();
        JDBC.assertDrainResults(dbmd.getTables(null, "APP", "ATDC_16_TAB1", null));
        assertEquals("# of valid rows in SYS.SYSSTATEMENTS should only be 2 ",
        		numberOfValidStatementsInSysstatements(s),
        		2);
        assertEquals("# of invalid statements in SYS.SYSSTATEMENTS should not change",
        		numberOfInvalidStatementsInSysstatements(s),
        		numOfInvalidSystatementsBeforeTestStart-1);
        
        s.executeUpdate("drop table ATDC_16_TAB1");
    }

    //Get a count of number of invalid statements in SYS.SYSSTATEMENTS
    private int numberOfInvalidStatementsInSysstatements(Statement st)
    		throws SQLException {
    	int num;
    	ResultSet rs = st.executeQuery(
    			"SELECT COUNT(*) FROM SYS.SYSSTATEMENTS "+
        		"WHERE VALID = false");
    	rs.next();
    	num = rs.getInt(1);
    	rs.close();
    	return(num);
    }

    //Get a count of number of valid statements in SYS.SYSSTATEMENTS
    private int numberOfValidStatementsInSysstatements(Statement st)
    		throws SQLException {
    	int num;
    	ResultSet rs = st.executeQuery(
    			"SELECT COUNT(*) FROM SYS.SYSSTATEMENTS "+
        		"WHERE VALID = TRUE");
    	rs.next();
    	num = rs.getInt(1);
    	rs.close();
    	return(num);
    }

    //Get a count of number of rows in SYS.SYSSTATEMENTS
    private int numberOfRowsInSysstatements(Statement st)
    		throws SQLException {
    	int num;
    	ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM SYS.SYSSTATEMENTS");
    	rs.next();
    	num = rs.getInt(1);
    	rs.close();
    	return(num);
    }
    
    /**
     * Altering the column length should regenerate the trigger
     * action plan which is saved in SYSSTATEMENTS. DERBY-4874
     * 
     * @throws SQLException 
     * 
     */
    public void testAlerColumnLength() throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE TestAlterTable( " +
        		"element_id INTEGER NOT NULL, "+
        		"altered_id VARCHAR(30) NOT NULL, "+
        		"counter SMALLINT NOT NULL DEFAULT 0, "+
        		"timets TIMESTAMP NOT NULL)");
        s.executeUpdate("CREATE TRIGGER mytrig "+
        		"AFTER UPDATE ON TestAlterTable "+
        		"REFERENCING NEW AS newt OLD AS oldt "+
        		"FOR EACH ROW MODE DB2SQL "+
        		"  UPDATE TestAlterTable set "+
        		"  TestAlterTable.counter = CASE WHEN "+
        		"  (oldt.counter < 32767) THEN (oldt.counter + 1) ELSE 1 END "+
        		"  WHERE ((newt.counter is null) or "+
        		"  (oldt.counter = newt.counter)) " +
        		"  AND newt.element_id = TestAlterTable.element_id "+
        		"  AND newt.altered_id = TestAlterTable.altered_id");
        s.executeUpdate("ALTER TABLE TestAlterTable ALTER altered_id "+
        		"SET DATA TYPE VARCHAR(64)");
        s.executeUpdate("insert into TestAlterTable values (99, "+
        		"'012345678901234567890123456789001234567890',"+
        		"1,CURRENT_TIMESTAMP)");

        ResultSet rs = s.executeQuery("SELECT element_id, counter "+
        		"FROM TestAlterTable");
        JDBC.assertFullResultSet(rs, 
        		new String[][] {{"99", "1"}});
        
        s.executeUpdate("update TestAlterTable "+
        		"set timets = CURRENT_TIMESTAMP "+
        		"where ELEMENT_ID = 99");
        rs = s.executeQuery("SELECT element_id, counter "+
        		"FROM TestAlterTable");
        JDBC.assertFullResultSet(rs, 
        		new String[][] {{"99", "2"}});

        s.executeUpdate("DROP TABLE TestAlterTable");
    }
    
    /**
     * Test the firing order of triggers. Should be:
     * 
     * Before operations
     * after operations
     * 
     * For multiple triggers within the same group (before or after)
     * firing order is determined by create order.
     * @throws SQLException 
     *
     */
    public void testFiringOrder() throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE T(ID INT)");
        
        int triggerCount = createRandomTriggers()[0];
        
        List<String> info = new ArrayList<String>();
        TRIGGER_INFO.set(info);
        
        // Check ordering with a single row.
        s.execute("INSERT INTO T VALUES 1");
        commit();
        int fireCount = assertFiringOrder("INSERT", 1);
        info.clear();
        
        s.execute("UPDATE T SET ID = 2");
        commit();
        fireCount += assertFiringOrder("UPDATE", 1);
        info.clear();
        
        s.execute("DELETE FROM T");
        commit();
        fireCount += assertFiringOrder("DELETE", 1);
        info.clear();
           
        assertEquals("All triggers fired?", triggerCount, fireCount);

        // and now with multiple rows
        s.execute("INSERT INTO T VALUES 1,2,3");
        commit();
        fireCount = assertFiringOrder("INSERT", 3);
        info.clear();
        
        s.execute("UPDATE T SET ID = 2");
        commit();
        fireCount += assertFiringOrder("UPDATE", 3);
        info.clear();
        
        s.execute("DELETE FROM T");
        commit();
        fireCount += assertFiringOrder("DELETE", 3);
        info.clear();
        
        // cannot assume row triggers were created so can only
        // say that at least all the triggers were fired.
        assertTrue("Sufficient triggers fired?", fireCount >= triggerCount);
        
        
        // and then with no rows
        assertTableRowCount("T", 0);
        s.execute("INSERT INTO T SELECT ID FROM T");
        commit();
        fireCount = assertFiringOrder("INSERT", 0);
        info.clear();
        
        s.execute("UPDATE T SET ID = 2");
        commit();
        fireCount += assertFiringOrder("UPDATE", 0);
        info.clear();
        
        s.execute("DELETE FROM T");
        commit();
        fireCount += assertFiringOrder("DELETE", 0);
        info.clear();
        
        // can't assert anthing about fireCount, could be all row triggers.
            
        s.close();

    }
    
    private int[] createRandomTriggers() throws SQLException
    {
        Statement s = createStatement();
        
        int beforeCount = 0;
        int afterCount = 0;
        
        Random r = new Random();
        // Randomly generate a number of triggers.
        // There are 12 types (B/A, I/U/D, R,S)
        // so pick enough triggers to get some
        // distribution across all 12.
        int triggerCount = r.nextInt(45) + 45;
        listOfCreatedTriggers = new StringBuffer();
        for (int i = 0; i < triggerCount; i++)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("CREATE TRIGGER TR");
            sb.append(i);
            sb.append(" ");
            
            String before;
            if (r.nextInt(2) == 0) {
                before = "NO CASCADE BEFORE";
                beforeCount++;
            } else {
                before = "AFTER";
                afterCount++;
            }
            sb.append(before);
            sb.append(" ");
            
            int type = r.nextInt(3);
            String iud;
            if (type == 0)
                iud = "INSERT";
            else if (type == 1)
                iud = "UPDATE";
            else
                iud = "DELETE";
            sb.append(iud);
            
            sb.append(" ON T FOR EACH ");
            
            String row;
            if (r.nextInt(2) == 0)
                row = "ROW";
            else
                row = "STATEMENT";
            sb.append(row);
            sb.append(" ");
            
            sb.append("CALL TRIGGER_LOG_INFO('");
            sb.append(i);
            sb.append(",");
            sb.append(before);
            sb.append(",");
            sb.append(iud);
            sb.append(",");
            sb.append(row);
            sb.append("')");

            s.execute(sb.toString());
            listOfCreatedTriggers.append(sb.toString());
        }
        commit();
        s.close();
        return new int[] {triggerCount, beforeCount, afterCount};
    }
    
    
    /**
     * Test that a order of firing is before triggers,
     * constraint checking and after triggers.
     * @throws SQLException 
     *
     */
    public void testFiringConstraintOrder() throws SQLException
    {
        Statement s = createStatement();
        s.execute("CREATE TABLE T (I INT PRIMARY KEY," +
                "U INT NOT NULL UNIQUE, C INT CHECK (C < 20))");
        s.execute("INSERT INTO T VALUES(1,5,10)");
        s.execute("INSERT INTO T VALUES(11,19,3)");
        s.execute("CREATE TABLE TCHILD (I INT, FOREIGN KEY (I) REFERENCES T)");
        s.execute("INSERT INTO TCHILD VALUES 1");
        commit();
        
        int beforeCount = createRandomTriggers()[1];
        
        List<String> info = new ArrayList<String>();
        TRIGGER_INFO.set(info);
        
        // constraint violation on primary key
        assertStatementError("23505", s, "INSERT INTO T VALUES (1,6,10)");
        assertFiringOrder("INSERT", 1, true);        
        info.clear();
        assertStatementError("23505", s, "UPDATE T SET I=1 WHERE I = 11");
        assertFiringOrder("UPDATE", 1, true);        
        info.clear();
        rollback();
        
        // constraint violation on unique key
        assertStatementError("23505", s, "INSERT INTO T VALUES (2,5,10)");
        assertFiringOrder("INSERT", 1, true);        
        info.clear();
        assertStatementError("23505", s, "UPDATE T SET U=5 WHERE I = 11");
        assertFiringOrder("UPDATE", 1, true);        
        info.clear();
        rollback();
        
        // check constraint
        assertStatementError("23513", s, "INSERT INTO T VALUES (2,6,22)");
        assertFiringOrder("INSERT", 1, true);        
        info.clear();
        assertStatementError("23513", s, "UPDATE T SET C=C+40 WHERE I = 11");
        assertFiringOrder("UPDATE", 1, true);        
        info.clear();
        rollback();
        
        // Foreign key constraint
        assertStatementError("23503", s, "DELETE FROM T WHERE I = 1");
        assertFiringOrder("DELETE", 1, true);        
        
        s.close();
        commit();
    }
    
    /**
     * Look at the ordered information in the thread local
     * and ensure it reflects correct sequenceing of
     * triggers created in testFiringOrder.
     * @param iud
     * @return the number of triggers checked
     */
    private int assertFiringOrder(String iud, int modifiedRowCount)
    {
        return assertFiringOrder(iud, modifiedRowCount, false);
    }
    private int assertFiringOrder(String iud, int modifiedRowCount,
            boolean noAfter)
    {
        List fires = (List) TRIGGER_INFO.get();
        
        int lastOrder = -1;
        String lastBefore = null;
        for (Iterator i = fires.iterator(); i.hasNext(); )
        {
            String info = i.next().toString();
            StringTokenizer st = new StringTokenizer(info, ",");
            assertEquals(4, st.countTokens());
            st.hasMoreTokens();
            int order = Integer.valueOf(st.nextToken()).intValue();
            st.hasMoreTokens();
            String before = st.nextToken();
            st.hasMoreTokens();
            String fiud = st.nextToken();
            st.hasMoreTokens();
            String row = st.nextToken();
            
            assertEquals("Incorrect trigger firing:"+info, iud, fiud);
            if (modifiedRowCount == 0)
               assertEquals("Row trigger firing on no rows",
                       "STATEMENT", row);
            if (noAfter)
                assertFalse("No AFTER triggers", "AFTER".equals(before));
            
            // First trigger.
            if (lastOrder == -1)
            {
                lastOrder = order;
                lastBefore = before;
                continue;
            }
            
            // Same trigger as last one.
            if (lastBefore.equals(before))
            {
                // for multiple rows the trigger can match the previous one.
                boolean orderOk =
                    modifiedRowCount > 1 ? (order >= lastOrder) :
                        (order > lastOrder);
                assertTrue("matching triggers need to be fired in order creation:"
                        +info+". Triggers got fired in this order:"+
                        TRIGGER_INFO.get().toString()+
                        ". Tiggers got created in this order:"+
                        listOfCreatedTriggers.toString(), orderOk);
                lastOrder = order;
                continue;
            }
            
            
            // switching from a before trigger to an after trigger.
            assertEquals("BEFORE before AFTER:"+info,
                    "NO CASCADE BEFORE", lastBefore);
            assertEquals("then AFTER:"+info,
                    "AFTER", before);
            
            lastBefore = before;
            lastOrder = order;
            
        }
        
        return fires.size();
    }
    
    /**
     * Record the trigger information in the thread local.
     * Called as a SQL procedure.
     * @param info trigger information
      */
    public static void logTriggerInfo(String info)
    {
        TRIGGER_INFO.get().add(info);
    }

    /** 
     * Test for DERBY-3718 NPE when a trigger is fired
     * 
     * @throws SQLException
     */
    public void testNPEinTriggerFire() throws SQLException
    {
        Statement s = createStatement();
        
    	String sql = " CREATE TABLE TRADE(ID INT PRIMARY KEY GENERATED "+
    	"BY DEFAULT AS IDENTITY (START WITH 1000), BUYID INT NOT NULL," +
    	"QTY FLOAT(2) NOT NULL)";
        s.executeUpdate(sql);

        sql = "CREATE TABLE TOTAL(BUYID INT NOT NULL, TOTALQTY FLOAT(2) NOT NULL)";
        s.executeUpdate(sql);
        
        sql = "CREATE TRIGGER TRADE_INSERT AFTER INSERT ON TRADE REFERENCING "+ 
        "NEW AS NEWROW FOR EACH ROW MODE DB2SQL UPDATE TOTAL SET TOTALQTY "+
        "= NEWROW.QTY WHERE BUYID = NEWROW.BUYID"; 
        s.executeUpdate(sql);
        
        s.executeUpdate("INSERT INTO TOTAL VALUES (1, 0)");
        //Before DERBY-3718 was fixed, following would cause NPE in 10.4 and 
        //trunk. This happened because starting 10.4, rather than saving the
        //TypeId of the DataTypeDescriptor (in writeExternal method), we rely
        //on reconstructing TypeId (in readExternal) by using the Types.xxx 
        //information(DERBY-2917 revision r619995). This approach does not
        //work for internal datatype REF, because we use Types.OTHER for REF
        //datatypes. Types.OTHER is not enough to know that the type to be 
        //constructed is REF. 
        //To get around the problem, for reconstructing TypeId, we will
        //use the type name rather than Types.xxx. Since we have the correct
        //type name for internal datatype REF, we can successfully reconstruct
        //REF datatype. 
        s.executeUpdate("INSERT INTO TRADE VALUES(1, 1, 10)");
        commit();      
    }

    //DERBY-1482
    public void testReadRequiredColumnsOnlyFromTriggerTable() throws SQLException, IOException {
        Statement s = createStatement();

        s.executeUpdate("CREATE TABLE table1 (c11 int, c12 int, c13 int, c14 int, c15 int)");
        s.executeUpdate("INSERT INTO table1 VALUES(1,2,3,4,5)");
        s.executeUpdate("CREATE TABLE table2 (c21 int, c22 int, c23 int, c24 int, c25 int)");
        s.executeUpdate("INSERT INTO table2 VALUES(2,2,3,-1,5)");
        //Notice that following trigger references columns from trigger table
        //randomly ie columns c12 and c14 are not the 1st 2 columns in trigger
        //table but they will be the first 2 columns in the resultset generated
        //for the trigger. The internal code generation for CreateTriggerNode
        //has been written to handle this mismatch of column numbering
        //between trigger table and trigger runtime resultset
        s.executeUpdate("CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 " +
        		" REFERENCING OLD AS oldt NEW AS newt" +
        		" FOR EACH ROW UPDATE table2 SET c24=oldt.c14");
        commit();      

        s.executeUpdate("update table1 set c12 = -9 where c11=1");
        ResultSet rs = s.executeQuery("SELECT * FROM table2");
        String[][] result = {
                {"2","2","3","4","5"},
            };
        JDBC.assertFullResultSet(rs, result);
            
        //couple negative test
        //give invalid column in trigger column
        String triggerStmt = "CREATE TRIGGER tr1 AFTER UPDATE OF c12xxx ON table1 " +
        		" REFERENCING OLD AS oldt NEW AS newt" +
        		" FOR EACH ROW UPDATE table2 SET c24=oldt.c14";
        assertStatementError("42X14", s, triggerStmt);
        
        //give invalid column in trigger action
        triggerStmt = "CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 " +
		" REFERENCING OLD AS oldt NEW AS newt" +
		" FOR EACH ROW UPDATE table2 SET c24=oldt.c14xxx";
        assertStatementError("42X04", s, triggerStmt);
        
        //Test case involving before and after values of LOB columns
        s.executeUpdate("create table derby1482_lob1 (str1 Varchar(80), " +
        		"c_lob CLOB(50M))");
        s.executeUpdate("create table derby1482_lob1_log(oldvalue CLOB(50M), " +
        		"newvalue  CLOB(50M), " +
        		"chng_time timestamp default current_timestamp)");
        s.executeUpdate("create trigger tr1_derby1482_lob1 after update of c_lob " +
        		"on derby1482_lob1 REFERENCING OLD AS old NEW AS new " +
        		"FOR EACH ROW MODE DB2SQL "+
        		"insert into derby1482_lob1_log(oldvalue, newvalue) values " +
        		"(old.c_lob, new.c_lob)");
        s.executeUpdate("INSERT INTO derby1482_lob1 VALUES ('1',null)");
        s.executeUpdate("update derby1482_lob1 set c_lob = null");
        rs = s.executeQuery("SELECT oldvalue, newvalue FROM derby1482_lob1_log");
        result = new String [][] {{null, null}};
        JDBC.assertFullResultSet(rs, result);
        
        //Test case involving a trigger which updates the trigger table
        s.executeUpdate("create table derby1482_selfUpdate (i int, j int)");
        s.executeUpdate("insert into derby1482_selfUpdate values (1,10)");
        s.executeUpdate("create trigger tr_derby1482_selfUpdate " + 
        		"after update of i on derby1482_selfUpdate " +
        		"referencing old as old for each row " +
        		"update derby1482_selfUpdate set j = old.j+1");
        s.executeUpdate("update derby1482_selfUpdate set i=i+1");
        rs = s.executeQuery("SELECT * FROM derby1482_selfUpdate");
        result = new String [][] {{"2","11"}};
        JDBC.assertFullResultSet(rs, result);
        
        //Test case where trigger definition uses REFERENCING clause but does
        //not use those columns in trigger action
        s.executeUpdate("create table t1_noTriggerActionColumn "+
        		"(id int, status smallint)");
        s.executeUpdate("insert into t1_noTriggerActionColumn values(11,1)");
        s.executeUpdate("create table t2_noTriggerActionColumn " +
        		"(id int, updates int default 0)");
        s.executeUpdate("insert into t2_noTriggerActionColumn values(1,1)");
        s.executeUpdate("create trigger tr1_noTriggerActionColumn " +
        		"after update of status on t1_noTriggerActionColumn " +
        		"referencing new as n_row for each row " +
        		"update t2_noTriggerActionColumn set " +
        		"updates = updates + 1 " +
        		"where t2_noTriggerActionColumn.id = 1");
        s.executeUpdate("update t1_noTriggerActionColumn set status=-1");
        rs =s.executeQuery("SELECT * FROM t2_noTriggerActionColumn");
        result = new String [][] {{"1","2"}};
        JDBC.assertFullResultSet(rs, result);
    }
    
    public void testDERBY5121() throws SQLException
    {
        Statement s = createStatement();

        s.executeUpdate("CREATE TABLE T1 (A1 int)");
        s.executeUpdate("CREATE TABLE T2 (B1 int, B2 int, B3 int)");
        s.executeUpdate("CREATE TRIGGER t2UpdateTrigger "+
        		"after UPDATE of b1 on t2 " +
        		"referencing new row as nr for each ROW " +
        		"insert into t1 values ( nr.b3 ) ");
        s.executeUpdate("INSERT INTO T2 VALUES(0,0,0)");
        s.executeUpdate("update t2 set b1 = 100 , b2 = 1");
        ResultSet rs =s.executeQuery("SELECT * FROM T1");
        JDBC.assertFullResultSet(rs, new String[][] {{"0"}});

        s.executeUpdate("CREATE TABLE T3 (A1 int)");
        s.executeUpdate("CREATE TABLE T4 (B1 int, B2 int, B3 int)");
        s.executeUpdate("CREATE TRIGGER t4UpdateTrigger "+
        		"after UPDATE of b1 on t4 " +
        		"referencing new table as nt for each STATEMENT " +
        		"insert into t3 select b3 from nt");
        s.executeUpdate("INSERT INTO T4 VALUES(0,0,0)");
        s.executeUpdate("update t4 set b1 = 100 , b2 = 1");
        rs =s.executeQuery("SELECT * FROM T3");
        JDBC.assertFullResultSet(rs, new String[][] {{"0"}});

    }
    
    /** 
     * Test for DERBY-3238 trigger fails with IOException if triggering table has large lob.
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testClobInTriggerTable() throws SQLException, IOException
    {
    	testClobInTriggerTable(1024);
        testClobInTriggerTable(16384);
         
        testClobInTriggerTable(1024 *32 -1);
        testClobInTriggerTable(1024 *32);
        testClobInTriggerTable(1024 *32+1);
        testClobInTriggerTable(1024 *64 -1);
        testClobInTriggerTable(1024 *64);
        testClobInTriggerTable(1024 *64+1);
        
    }
   
    /**
     * Create a table with after update trigger on non-lob column.
     * Insert clob of size clobSize into table and perform update
     * on str1 column to fire trigger. Helper method called from 
     * testClobInTriggerTable
     * @param clobSize size of clob to test
     * @throws SQLException
     * @throws IOException
     */
    private void testClobInTriggerTable(int clobSize) throws SQLException, IOException {
        // Alphabet used when inserting a CLOB.
        CharAlphabet a1 = CharAlphabet.singleChar('a');
        // Alphabet used when updating a CLOB.
        CharAlphabet a2 = CharAlphabet.singleChar('b');
    	
    	// --- add a clob
    	String trig = " create trigger t_lob1 after update of str1 on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.str1, new.str1)";

        Statement s = createStatement();
        
        s.executeUpdate("create table LOB1 (str1 Varchar(80), c_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue varchar(80), newvalue varchar(80), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        PreparedStatement ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, clobSize +"");

        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(clobSize, a1), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        s.executeUpdate("update LOB1 set str1 = str1 || ' '");
       
        
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");

        // now referencing the lob column
        trig = " create trigger t_lob1 after update of c_lob on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.c_lob, new.c_lob)";

        s.executeUpdate("create table LOB1 (str1 Varchar(80), c_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue CLOB(50M), newvalue  CLOB(50M), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, clobSize +"");

        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(clobSize, a1), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        ps = prepareStatement("update LOB1 set c_lob = ?");
        ps.setCharacterStream(1,
                new LoopingAlphabetReader(clobSize, a2), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();        

        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
        
        //      now referencing the lob column twice
        trig = " create trigger t_lob1 after update of c_lob on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue, oldvalue_again, newvalue_again) values (old.c_lob, new.c_lob, old.c_lob, new.c_lob)";

        s.executeUpdate("create table LOB1 (str1 Varchar(80), c_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue CLOB(50M), newvalue  CLOB(50M), oldvalue_again CLOB(50M), newvalue_again CLOB(50M), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, clobSize +"");

        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(clobSize, a1), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        ps = prepareStatement("update LOB1 set c_lob = ?");
        ps.setCharacterStream(1,
                new LoopingAlphabetReader(clobSize, a2), clobSize);
        ps.execute();
        closeStatement(ps);
        commit();
        
        // check log table.
        ResultSet rs = s.executeQuery("SELECT * from t_lob1_log");
        rs.next();
               
        assertEquals(new LoopingAlphabetReader(clobSize, a1),
                     rs.getCharacterStream(1));

        assertEquals(new LoopingAlphabetReader(clobSize, a2),
                     rs.getCharacterStream(2));

        assertEquals(new LoopingAlphabetReader(clobSize, a1),
                     rs.getCharacterStream(3));

        assertEquals(new LoopingAlphabetReader(clobSize, a2),
                     rs.getCharacterStream(4));

        rs.close();
        
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
        
       }

    /** 
     * Test for DERBY-3238 trigger fails with IOException if triggering table has large lob.
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testBlobInTriggerTable() throws SQLException, IOException
    {        
    	testBlobInTriggerTable(1024);
        testBlobInTriggerTable(16384);
         
        testBlobInTriggerTable(1024 *32 -1);
        testBlobInTriggerTable(1024 *32);
        testBlobInTriggerTable(1024 *32+1);
        testBlobInTriggerTable(1024 *64 -1);
        testBlobInTriggerTable(1024 *64);
        testBlobInTriggerTable(1024 *64+1);
        testBlobInTriggerTable(1024 *1024* 7);
    }
    
    
    /**
     * Create a table with after update trigger on non-lob column.
     * Insert two blobs of size blobSize into table and perform update
     * on str1 column to fire trigger. Helper method called from 
     * testBlobInTriggerTable
     * 
     * @param blobSize  size of blob to test.
     * @throws SQLException
     * @throws IOException
     */
    private  void testBlobInTriggerTable(int blobSize) throws SQLException, IOException {
        // Alphabet used when inserting a BLOB.
        ByteAlphabet a1 = ByteAlphabet.singleByte((byte) 8);
        // Alphabet used when updating a BLOB.
        ByteAlphabet a2 = ByteAlphabet.singleByte((byte) 9);

        String trig = " create trigger t_lob1 after update of str1 on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.str1, new.str1)";

        Statement s = createStatement();
        
        s.executeUpdate("create table LOB1 (str1 Varchar(80), b_lob BLOB(50M), b_lob2 BLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue varchar(80), newvalue varchar(80), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

    	// --- add a blob
        PreparedStatement ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?, ?)");
        
        ps.setString(1, blobSize +"");

        // - set the value of the input parameter to the input stream
        // use a couple blobs so we are sure it works with multiple lobs
        ps.setBinaryStream(2,
                new LoopingAlphabetStream(blobSize, a1), blobSize);
        ps.setBinaryStream(3,
                new LoopingAlphabetStream(blobSize, a1), blobSize);
        ps.execute();
        closeStatement(ps);
        
        commit();
        // Now executing update to fire trigger
        s.executeUpdate("update LOB1 set str1 = str1 || ' '");
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
   
        // now referencing the lob column
        trig = " create trigger t_lob1 after update of b_lob on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.b_lob, new.b_lob)";

        s.executeUpdate("create table LOB1 (str1 Varchar(80), b_lob BLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue BLOB(50M), newvalue  BLOB(50M), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, blobSize +"");


        // - set the value of the input parameter to the input stream
        ps.setBinaryStream(2,
                new LoopingAlphabetStream(blobSize, a1), blobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        ps = prepareStatement("update LOB1 set b_lob = ?");
        ps.setBinaryStream(1,
                new LoopingAlphabetStream(blobSize, a2), blobSize);
        ps.execute();
        closeStatement(ps);
        commit();        

        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
        
        //      now referencing the lob column twice
        trig = " create trigger t_lob1 after update of b_lob on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue, oldvalue_again, newvalue_again) values (old.b_lob, new.b_lob, old.b_lob, new.b_lob)";

        s.executeUpdate("create table LOB1 (str1 Varchar(80), b_lob BLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue BLOB(50M), newvalue  BLOB(50M), oldvalue_again BLOB(50M), newvalue_again BLOB(50M), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        commit();      

        ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, blobSize +"");


        // - set the value of the input parameter to the input stream
        ps.setBinaryStream(2,
                new LoopingAlphabetStream(blobSize, a1), blobSize);
        ps.execute();
        closeStatement(ps);
        commit();

        // Now executing update to fire trigger
        ps = prepareStatement("update LOB1 set b_lob = ?");
        ps.setBinaryStream(1,
                new LoopingAlphabetStream(blobSize, a2), blobSize);
        ps.execute();
        closeStatement(ps);
        commit();
        
        // check log table.
        ResultSet rs = s.executeQuery("SELECT * from t_lob1_log");
        rs.next();

        assertEquals(new LoopingAlphabetStream(blobSize, a1),
                     rs.getBinaryStream(1));

        assertEquals(new LoopingAlphabetStream(blobSize, a2),
                     rs.getBinaryStream(2));

        assertEquals(new LoopingAlphabetStream(blobSize, a1),
                     rs.getBinaryStream(3));

        assertEquals(new LoopingAlphabetStream(blobSize, a2),
                     rs.getBinaryStream(4));
        
        rs.close();

        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");

    }

    /* 
     * Test an update trigger on a Clob column
     * 
     */
    public void testUpdateTriggerOnClobColumn() throws SQLException, IOException
    {
        // Alphabet used when inserting a CLOB.
        CharAlphabet a1 = CharAlphabet.singleChar('a');
        // Alphabet used when updating a CLOB.
        CharAlphabet a2 = CharAlphabet.singleChar('b');

    	Connection conn = getConnection();
    	Statement s = createStatement();
    	String trig = " create trigger t_lob1 after update of str1 on lob1 ";
    	trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
    	trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.str1, new.str1)";
    	s.executeUpdate("create table LOB1 (str1 Varchar(80), C_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue varchar(80), newvalue varchar(80), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        conn.commit();
        PreparedStatement ps = prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        int clobSize = 1024*64+1;
        ps.setString(1, clobSize +"");


        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2,
                new LoopingAlphabetReader(clobSize, a1), clobSize);
        ps.execute();
        conn.commit();


        PreparedStatement ps2 = prepareStatement("update LOB1 set c_lob = ? where str1 = '" + clobSize + "'");
        ps2.setCharacterStream(1,
                new LoopingAlphabetReader(clobSize, a2), clobSize);
        ps2.executeUpdate();
        conn.commit();
        // 	--- reading the clob make sure it was updated
        ResultSet rs = s.executeQuery("SELECT * FROM LOB1 where str1 = '" + clobSize + "'");
        rs.next();
        
        assertEquals(new LoopingAlphabetReader(clobSize, a2),
                     rs.getCharacterStream(2));
        rs.close();
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
        
	  
    }

    /**
     * Test that the action statement of a trigger
     * can work with all datatypes.
     * @throws SQLException
     * @throws IOException 
     */
    public void testTypesInActionStatement() throws SQLException, IOException
    {
        List types = DatabaseMetaDataTest.getSQLTypes(getConnection());
        
        if (!XML.classpathMeetsXMLReqs())
            types.remove("XML");
        
        // JSR 169 doesn't support DECIMAL in triggers.
        if (!JDBC.vmSupportsJDBC3())
        {           
            for (Iterator i = types.iterator(); i.hasNext(); )
            {
                String type = i.next().toString();
                if (type.startsWith("DECIMAL") || type.startsWith("NUMERIC"))
                    i.remove();
            }
        }
        
        for (Iterator i = types.iterator(); i.hasNext(); )
        {
            actionTypeTest(i.next().toString());
        }
    }
    
    /**
     * Test that the action statement of a trigger
     * can work with a specific datatype.
     * @param type SQL type to be tested
     * @throws SQLException 
     * @throws IOException 
     */
    private void actionTypeTest(String type) throws SQLException, IOException
    {
        println("actionTypeTest:"+type);
        Statement s = createStatement(); 
        
        actionTypesSetup(type);
        
        actionTypesInsertTest(type); 
        
        actionTypesUpdateTest(type);
        
        actionTypesDeleteTest(type);
               
        s.executeUpdate("DROP TABLE T_MAIN");
        s.executeUpdate("DROP TABLE T_ACTION_ROW");
        s.executeUpdate("DROP TABLE T_ACTION_STATEMENT");
        s.close();
        
        commit();
        

    }
   
    /**
     * Setup the tables and triggers for a single type for actionTypeTest
     */
    private void actionTypesSetup(String type) throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE T_MAIN(" +
                "ID INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
                "V " + type + " )");
        s.executeUpdate("CREATE TABLE T_ACTION_ROW(ID INT, A CHAR(1), " +
                "V1 " + type + ", V2 " + type + " )"); 
        s.executeUpdate("CREATE TABLE T_ACTION_STATEMENT(ID INT, A CHAR(1), " +
                "V1 " + type + ", V2 " + type + " )"); 
       
        // ON INSERT copy the typed value V into the action table.
        // Use V twice to ensure there are no issues with values
        // that can be streamed.
        // Two identical actions,  per row and per statement.
        s.executeUpdate("CREATE TRIGGER AIR " +
                "AFTER INSERT ON T_MAIN " +
                "REFERENCING NEW AS N " +
                "FOR EACH ROW " +      
                "INSERT INTO T_ACTION_ROW(A, V1, ID, V2) VALUES ('I', N.V, N.ID, N.V)");
        
        s.executeUpdate("CREATE TRIGGER AIS " +
                "AFTER INSERT ON T_MAIN " +
                "REFERENCING NEW TABLE AS N " +
                "FOR EACH STATEMENT " +      
                "INSERT INTO T_ACTION_STATEMENT(A, V1, ID, V2) " +
                "SELECT 'I', V, ID, V FROM N");
        
        // ON update copy the old and new value into the action table.
        // Two identical actions,  per row and per statement.
        s.executeUpdate("CREATE TRIGGER AUR " +
                "AFTER UPDATE OF V ON T_MAIN " +
                "REFERENCING NEW AS N OLD AS O " +
                "FOR EACH ROW " +      
                "INSERT INTO T_ACTION_ROW(A, V1, ID, V2) VALUES ('U', N.V, N.ID, O.V)");
        
        s.executeUpdate("CREATE TRIGGER AUS " +
                "AFTER UPDATE OF V ON T_MAIN " +
                "REFERENCING NEW TABLE AS N OLD TABLE AS O " +
                "FOR EACH STATEMENT " +      
                "INSERT INTO T_ACTION_STATEMENT(A, V1, ID, V2) " +
                "SELECT 'U', N.V, N.ID, O.V FROM N,O WHERE O.ID = N.ID");
        
        // ON DELETE copy the old value into the action table.
        // Two identical actions,  per row and per statement.
        s.executeUpdate("CREATE TRIGGER ADR " +
                "AFTER DELETE ON T_MAIN " +
                "REFERENCING OLD AS O " +
                "FOR EACH ROW " +      
                "INSERT INTO T_ACTION_ROW(A, V1, ID, V2) VALUES ('D', O.V, O.ID, O.V)");
        
        s.executeUpdate("CREATE TRIGGER ADS " +
                "AFTER DELETE ON T_MAIN " +
                "REFERENCING OLD TABLE AS O " +
                "FOR EACH STATEMENT " +      
                "INSERT INTO T_ACTION_STATEMENT(A, V1, ID, V2) " +
                "SELECT 'D', O.V, O.ID, O.V FROM O");        
        

        s.close();
        commit();
    }
    
    /**
     * Execute three insert statements.
     * NULL as the value for the type
     * one row insert with random value for the type
     * three row insert with random values for the type
     * 
     * Check that the data in the action table matches the main
     * table (see the after insert trigger definitions).
     * @param type
     * @throws SQLException
     * @throws IOException
     * 
     */
    private void actionTypesInsertTest(String type)
        throws SQLException, IOException
    {  
        Statement s = createStatement();
        s.executeUpdate("INSERT INTO T_MAIN(V) VALUES NULL");
        s.close();
        actionTypesCompareMainToAction(1, type);

        int jdbcType = DatabaseMetaDataTest.getJDBCType(type);
        int precision = DatabaseMetaDataTest.getPrecision(jdbcType, type);

        // BUG DERBY-2349 - remove this check & return to see the issue.
        if (jdbcType == Types.BLOB)
            return; 
        
        Random r = new Random();
        
        String ins1 = "INSERT INTO T_MAIN(V) VALUES (?)";
        String ins3 = "INSERT INTO T_MAIN(V) VALUES (?), (?), (?)";
        
        // Can't directly insert into XML columns from JDBC.
        if (jdbcType == JDBC.SQLXML)
        {
            ins1 = "INSERT INTO T_MAIN(V) VALUES " +
                    "XMLPARSE (DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)";
            ins3 = "INSERT INTO T_MAIN(V) VALUES " +
                    "XMLPARSE (DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)," +
                    "XMLPARSE (DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)," +
                    "XMLPARSE (DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)";
        }
        
        PreparedStatement ps;
        ps = prepareStatement(ins1);
        setRandomValue(r, ps, 1, jdbcType, precision);
        ps.executeUpdate();
        ps.close();

        actionTypesCompareMainToAction(2, type);

        ps = prepareStatement(ins3);
        setRandomValue(r, ps, 1, jdbcType, precision);
        setRandomValue(r, ps, 2, jdbcType, precision);
        setRandomValue(r, ps, 3, jdbcType, precision);
        ps.executeUpdate();
        ps.close();

        actionTypesCompareMainToAction(5, type);    
    }
    
    /**
     * Test updates of the specified types in the action statement.
     * @param type
     * @throws SQLException
     * @throws IOException
     */
    private void actionTypesUpdateTest(String type)
        throws SQLException, IOException
    {
        int jdbcType = DatabaseMetaDataTest.getJDBCType(type);
        int precision = DatabaseMetaDataTest.getPrecision(jdbcType, type);

        // BUG DERBY-2349 - need insert case to work first
        if (jdbcType == Types.BLOB)
            return;
        
        Statement s = createStatement();
        s.executeUpdate("UPDATE T_MAIN SET V = NULL WHERE ID = 2");
        s.close();
        commit();
        actionTypesCompareMainToActionForUpdate(type, 2);

        Random r = new Random();
        
        PreparedStatement ps = prepareStatement(
            (jdbcType == JDBC.SQLXML
                ? "UPDATE T_MAIN SET V = " +
                  "XMLPARSE(DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)"
                : "UPDATE T_MAIN SET V = ?")
            + " WHERE ID >= ? AND ID <= ?");
        
        // Single row update of row 3
        setRandomValue(r, ps, 1, jdbcType, precision);
        ps.setInt(2, 3);
        ps.setInt(3, 3);
        assertUpdateCount(ps, 1);
        commit();
        actionTypesCompareMainToActionForUpdate(type, 3);
        
        // Bug DERBY-2358 - skip multi-row updates for streaming input.
        switch (jdbcType) {
        case Types.BLOB:
        case Types.CLOB:
        case Types.LONGVARBINARY:
        case Types.LONGVARCHAR:
            ps.close();
            return;
        }
        
        // multi-row update of 4,5
        setRandomValue(r, ps, 1, jdbcType, precision);
        ps.setInt(2, 4);
        ps.setInt(3, 5);
        assertUpdateCount(ps, 2);
        commit();
        actionTypesCompareMainToActionForUpdate(type, 4);
        actionTypesCompareMainToActionForUpdate(type, 5);

        ps.close();
        
    }
    
    /**
     * Compare the values for an update trigger.
     * @param type
     * @param id
     * @throws SQLException
     * @throws IOException
     */
    private void actionTypesCompareMainToActionForUpdate(String type,
            int id) throws SQLException, IOException {
        
        String sqlMain = "SELECT M.V, R.V1 FROM T_MAIN M, T_ACTION_ROW R " +
            "WHERE M.ID = ? AND R.A = 'I' AND M.ID = R.ID";
        String sqlRow = "SELECT V1, V2 FROM T_ACTION_ROW " +
            "WHERE A = 'U' AND ID = ?";
        String sqlStmt = "SELECT V1, V2 FROM T_ACTION_STATEMENT " +
            "WHERE A = 'U' AND ID = ?";
        
        if ("XML".equals(type)) {
            // XMLSERIALIZE(V AS CLOB)
            sqlMain = "SELECT XMLSERIALIZE(M.V AS CLOB), " +
                "XMLSERIALIZE(R.V1 AS CLOB) FROM T_MAIN M, T_ACTION_ROW R " +
                "WHERE M.ID = ? AND R.A = 'I' AND M.ID = R.ID";
            sqlRow = "SELECT XMLSERIALIZE(V1 AS CLOB), " +
                "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_ROW " +
                "WHERE A = 'U' AND ID = ?";
            sqlStmt = "SELECT XMLSERIALIZE(V1 AS CLOB), " +
                "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_STATEMENT " +
                "WHERE A = 'U' AND ID = ?";
        }
        
        // Get the new value from main and old from the action table 
        PreparedStatement psMain = prepareStatement(sqlMain);
              
        // new (V1) & old (V2) value as copied by the trigger
        PreparedStatement psActionRow = prepareStatement(sqlRow);
        PreparedStatement psActionStmt = prepareStatement(sqlStmt);
        
        psMain.setInt(1, id);
        psActionRow.setInt(1, id);
        psActionStmt.setInt(1, id);
        
        JDBC.assertSameContents(psMain.executeQuery(),
                psActionRow.executeQuery());
        JDBC.assertSameContents(psMain.executeQuery(),
                psActionStmt.executeQuery());
         
        psMain.close();
        psActionRow.close();
        psActionStmt.close();
        
        commit();
    }
    
    /**
     * Test deletes with the specified types in the action statement.
     * @param type
     * @throws SQLException
     * @throws IOException
     */
    private void actionTypesDeleteTest(String type)
        throws SQLException, IOException
    {
        int jdbcType = DatabaseMetaDataTest.getJDBCType(type);
        int precision = DatabaseMetaDataTest.getPrecision(jdbcType, type);

        // BUG DERBY-2349 - need insert case to work first
        if (jdbcType == Types.BLOB)
            return;
        
        Statement s = createStatement();
        // Single row delete
        assertUpdateCount(s, 1, "DELETE FROM T_MAIN WHERE ID = 3");
        commit();
        
        // multi-row delete
        assertUpdateCount(s, 4, "DELETE FROM T_MAIN");
        commit();
        
        s.close();
    }
    
    
    /**
     * Compare the contents of the main table to the action table.
     * See the trigger defintions for details.
     * @param actionCount
     * @param type
     * @throws SQLException
     * @throws IOException
     */
    private void actionTypesCompareMainToAction(int actionCount,
            String type) throws SQLException, IOException {
        
        Statement s1 = createStatement();
        Statement s2 = createStatement();
        
        String sqlMain = "SELECT ID, V, V FROM T_MAIN ORDER BY 1";
        String sqlActionRow = "SELECT ID, V1, V2 FROM T_ACTION_ROW ORDER BY 1";
        String sqlActionStatement = "SELECT ID, V1, V2 FROM T_ACTION_STATEMENT ORDER BY 1";
        
        // Derby does not (yet) allow a XML column in select list 
        if ("XML".equals(type)) {
            sqlMain = "SELECT ID, XMLSERIALIZE(V AS CLOB), " +
                    "XMLSERIALIZE(V AS CLOB) FROM T_MAIN ORDER BY 1";
            sqlActionRow = "SELECT ID, XMLSERIALIZE(V1 AS CLOB), " +
                    "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_ROW ORDER BY 1";
            sqlActionStatement = "SELECT ID, XMLSERIALIZE(V1 AS CLOB), " +
                "XMLSERIALIZE(V2 AS CLOB) FROM T_ACTION_STATEMENT ORDER BY 1";
        }
        
        ResultSet rsMain = s1.executeQuery(sqlMain);
        ResultSet rsAction = s2.executeQuery(sqlActionRow);        
        JDBC.assertSameContents(rsMain, rsAction);
        
        rsMain = s1.executeQuery(sqlMain);
        rsAction = s2.executeQuery(sqlActionStatement);        
        JDBC.assertSameContents(rsMain, rsAction);
        
        
        assertTableRowCount("T_ACTION_ROW", actionCount);
        assertTableRowCount("T_ACTION_STATEMENT", actionCount);
        
        s1.close();
        s2.close();
    }
    
    public static void setRandomValue(Random r,
            PreparedStatement ps, int column, int jdbcType, int precision)
    throws SQLException, IOException
    {
        Object val = getRandomValue(r, jdbcType, precision);
        if (val instanceof StringReaderWithLength) {
            StringReaderWithLength rd = (StringReaderWithLength) val;
            ps.setCharacterStream(column, rd, rd.getLength());
        } else if (val instanceof InputStream) {
            InputStream in = (InputStream) val;
            ps.setBinaryStream(column, in, in.available());
        } else
            ps.setObject(column, val, jdbcType);
    }
    
    /**
     * Generate a random object (never null) for
     * a given JDBC type. Object is suitable for
     * PreparedStatement.setObject() either
     * with or without passing in jdbcType to setObject.
     * <BR>
     * For character types a String object or a
     * StringReaderWithLength is returned.
     * <BR>
     * For binary types a byte[] or an instance of InputStream
     * is returned. If an inputstream is returned then it can
     * only be read once and in.available() returns the total
     * number of bytes available to read.
     * For BLOB types a random value is returned up to
     * either the passed in precision or 256k. This is
     * to provide a general purpose value that can be
     * more than a page.
     * <P>
     * Caller should check the return type using instanceof
     * and use setCharacterStream() for Reader objects and
     * setBinaryStream for InputStreams.
     * (work in progress)
     * @throws IOException 
     */
    public static Object getRandomValue(Random r, int jdbcType, 
            int precision) throws IOException
    {
        switch (jdbcType)
        {
        case Types.SMALLINT:
          return (short) r.nextInt();
        case Types.INTEGER:
            return r.nextInt();
            
        case Types.BIGINT:
            return r.nextLong();
            
        case Types.FLOAT:
        case Types.REAL:
            return r.nextFloat();
            
        case Types.DOUBLE:
            return r.nextDouble();

        case Types.DATE:
            long d = r.nextLong();
            if (d < 0)
                d = -d;
            d = d / (24L * 60L * 60L * 1000L);
            d = d % (4000L * 365L); // limit year to a reasonable value.
            d = d * (24L * 60L * 60L * 1000L);
            return new Date(d);
            
        case Types.TIME:
            long t = r.nextLong();
            if (t < 0)
                t = -t;
             return new Time(t % (24L * 60L * 60L * 1000L));
             
        case Types.TIMESTAMP:
            // limit year to a reasonable value
            long ts = r.nextLong();
            if (ts < 0)
                ts = -ts;
            ts = ts % (4000L * 365L * 24L * 60L * 60L * 1000L);
            return new Timestamp(ts);
            
        case Types.VARCHAR:
        case Types.CHAR:
            return randomString(r, r.nextInt(precision + 1));
            
        case Types.LONGVARCHAR:
            return new StringReaderWithLength(
                    randomString(r, r.nextInt(32700 + 1)));
            
        case Types.CLOB:
            if (precision > 256*1024)
                precision = 256*1024;
            return new StringReaderWithLength(
                    randomString(r, r.nextInt(precision)));

        case Types.BINARY:
        case Types.VARBINARY:
            return randomBinary(r, r.nextInt(precision + 1));

        case Types.LONGVARBINARY:
            return new ReadOnceByteArrayInputStream(
                    randomBinary(r, r.nextInt(32701)));
            
        case Types.BLOB:
            if (precision > 256*1024)
                precision = 256*1024;
            return new ReadOnceByteArrayInputStream(
                    randomBinary(r, r.nextInt(precision)));
            
        case JDBC.SQLXML:
            // Not random yet, but was blocked by DEBRY-2350
            // so just didn't put effort into generating 
            // a random size XML document.
            return new StringReaderWithLength("<a><b>text</b></a>");
            
             
       }
            
        // fail("unexpected JDBC Type " + jdbcType);
        return null;
    }
    
    private static byte[] randomBinary(Random r, int len)
    {
        byte[] bb = new byte[len];
        for (int i = 0; i < bb.length; i++)
            bb[i] = (byte) r.nextInt();
        return bb;
 
    }
    private static String randomString(Random r, int len)
    {
        char[] cb = new char[len];
        for (int i = 0; i < cb.length; i++)
            cb[i] = (char) r.nextInt(Character.MAX_VALUE);
              
        return new String(cb);
                
    }


    /**
     * Test that a nested loop join that accesses the 
     * TriggerOldTransitionRowsVTI can reopen the ResultSet properly 
     * when it re-executes.
     * @throws SQLException
     */
    public void testDerby4095OldTriggerRows() throws SQLException {
        Statement s = createStatement();
        
        s.executeUpdate("CREATE TABLE APP.TAB (I INT)");
        s.executeUpdate("CREATE TABLE APP.LOG (I INT, NAME VARCHAR(30), DELTIME TIMESTAMP)");
        s.executeUpdate("CREATE TABLE APP.NAMES(ID INT, NAME VARCHAR(30))");

        
        s.executeUpdate("CREATE TRIGGER  APP.MYTRIG AFTER DELETE ON APP.TAB REFERENCING OLD_TABLE AS OLDROWS FOR EACH STATEMENT INSERT INTO APP.LOG(i,name,deltime) SELECT OLDROWS.I, NAMES.NAME, CURRENT_TIMESTAMP FROM --DERBY-PROPERTIES joinOrder=FIXED\n NAMES, OLDROWS --DERBY-PROPERTIES joinStrategy = NESTEDLOOP\n WHERE (OLDROWS.i = NAMES.ID) AND (1 = 1)");
        
        s.executeUpdate("insert into APP.tab values(1)");
        s.executeUpdate("insert into APP.tab values(2)");
        s.executeUpdate("insert into APP.tab values(3)");

        s.executeUpdate("insert into APP.names values(1,'Charlie')");
        s.executeUpdate("insert into APP.names values(2,'Hugh')");
        s.executeUpdate("insert into APP.names values(3,'Alex')");

        // Now delete a row so we fire the trigger.
        s.executeUpdate("delete from tab where i = 1");
        // Check the log to make sure the trigger fired ok
        ResultSet loggedDeletes = s.executeQuery("SELECT * FROM APP.LOG");
        JDBC.assertDrainResults(loggedDeletes, 1);
                 
        s.executeUpdate("DROP TABLE APP.TAB");
        s.executeUpdate("DROP TABLE APP.LOG");
        s.executeUpdate("DROP TABLE APP.NAMES");
        
    }
    
    /**
     * Test that a nested loop join that accesses the 
     * TriggerNewTransitionRowsVTI can reopen the ResultSet properly 
     * when it re-executes.
     * @throws SQLException
     */
    public void testDerby4095NewTriggerRows() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE APP.TAB (I INT)");
        s.executeUpdate("CREATE TABLE APP.LOG (I INT, NAME VARCHAR(30), UPDTIME TIMESTAMP, NEWVALUE INT)");
        s.executeUpdate("CREATE TABLE APP.NAMES(ID INT, NAME VARCHAR(30))");

        
        s.executeUpdate("CREATE TRIGGER  APP.MYTRIG AFTER UPDATE ON APP.TAB REFERENCING OLD_TABLE AS OLDROWS NEW_TABLE AS NEWROWS FOR EACH STATEMENT INSERT INTO APP.LOG(i,name,updtime,newvalue) SELECT OLDROWS.I, NAMES.NAME, CURRENT_TIMESTAMP, NEWROWS.I  FROM --DERBY-PROPERTIES joinOrder=FIXED\n NAMES, NEWROWS --DERBY-PROPERTIES joinStrategy = NESTEDLOOP\n ,OLDROWS WHERE (NEWROWS.i = NAMES.ID) AND (1 = 1)");
        
        s.executeUpdate("insert into tab values(1)");
        s.executeUpdate("insert into tab values(2)");
        s.executeUpdate("insert into tab values(3)");

        s.executeUpdate("insert into names values(1,'Charlie')");
        s.executeUpdate("insert into names values(2,'Hugh')");
        s.executeUpdate("insert into names values(3,'Alex')");

        // Now update a row to fire the trigger
        s.executeUpdate("update tab set i=1 where i = 1");

        // Check the log to make sure the trigger fired ok
        ResultSet loggedUpdates = s.executeQuery("SELECT * FROM APP.LOG");
        JDBC.assertDrainResults(loggedUpdates, 1);
        
        
        s.executeUpdate("DROP TABLE APP.TAB");
        s.executeUpdate("DROP TABLE APP.LOG");
        s.executeUpdate("DROP TABLE APP.NAMES");
    }

    /**
     * Regression test case for DERBY-4610, where a DELETE statement failed
     * because a trigger used the wrong meta-data and mixed up the data types.
     */
    public void testDerby4610WrongDataType() throws SQLException {
        Statement s = createStatement();
        s.execute("create table testtable " +
                  "(id integer, name varchar(20), primary key(id))");
        s.execute("create table testchild (" +
                  "id integer constraint fk_id " +
                  "references testtable on delete cascade, " +
                  "ordernum int, primary key(id))");
        s.execute("create procedure testproc (str varchar(20)) " +
                  "PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME '" +
                  getClass().getName() + ".derby4610proc'");
        s.execute("create trigger testtabletrigger after delete on testtable " +
                  "referencing old as old " +
                  "for each row mode db2sql call testproc(char(old.id))");
        s.execute("create trigger testchildtrigger after delete on testchild " +
                  "referencing old as old " +
                  "for each row mode db2sql call testproc(char(old.ordernum))");
        s.execute("insert into testtable values (1, 'test1')");
        s.execute("insert into testchild values (1, 10)");

        // Used to fail with ERROR XCL12: An attempt was made to put a data
        // value of type 'java.lang.String' into a data value of type 'INTEGER'.
        assertUpdateCount(s, 1, "delete from testtable where id = 1");
    }

    /**
     * Procedure that does nothing. Called as a stored procedure in the
     * regression test case for DERBY-4610.
     */
    public static void derby4610proc(String str) {
        // do nothing
    }

    /**
     * Regression test case for DERBY-6351, where CREATE TRIGGER would fail
     * with a syntax error if the triggered SQL statement referenced a
     * transition table using a correlation name, and that correlation name
     * was equal to the transition table name.
     */
    public void testDerby6351TransitionTableCorrelation() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(x varchar(10), y int)");

        // The correlation name is equal to the name of the transition table.
        // This used to fail with a syntax error.
        s.execute("create trigger tr1 after insert on t1 "
                + "referencing new table as n "
                + "insert into t2 select 'tr1', x from n n");
        s.execute("create trigger tr2 after update on t1 "
                + "referencing old table as o "
                + "insert into t2 select 'tr2', x from o o");

        // For completeness, also verify that no correlation name and a
        // distinct correlation name work as expected.
        s.execute("create trigger tr3 after insert on t1 "
                + "referencing new table as n "
                + "insert into t2 select 'tr3', x from n");
        s.execute("create trigger tr4 after update on t1 "
                + "referencing old table as o "
                + "insert into t2 select 'tr4', x from o");
        s.execute("create trigger tr5 after insert on t1 "
                + "referencing new table as n "
                + "insert into t2 select 'tr5', n1.x from n n1");
        s.execute("create trigger tr6 after update on t1 "
                + "referencing old table as o "
                + "insert into t2 select 'tr6', o1.x from o o1");

        // Fire the insert triggers and verify that they worked.
        s.execute("insert into t1 values 1,2");
        JDBC.assertFullResultSet(
                s.executeQuery("select * from t2 order by x, y"),
                new String[][] {
                    { "tr1", "1" },
                    { "tr1", "2" },
                    { "tr3", "1" },
                    { "tr3", "2" },
                    { "tr5", "1" },
                    { "tr5", "2" },
                });

        // Fire the update triggers and verify that they worked.
        s.execute("delete from t2"); // clean up first
        s.execute("update t1 set x = x + 1 where x = 1");
        JDBC.assertFullResultSet(
                s.executeQuery("select * from t2 order by x, y"),
                new String[][] {
                    { "tr2", "1" },
                    { "tr4", "1" },
                    { "tr6", "1" },
                });
    }

    /**
     * Verify that CREATE TRIGGER fails if a temporary table is referenced.
     * Regression test case for DERBY-6357.
     */
    public void testDerby6357TempTable() throws SQLException {
        Statement s = createStatement();
        s.execute("declare global temporary table temptable(x int) not logged");
        s.execute("create table t1(x int)");
        s.execute("create table t2(i int, b boolean)");

        assertCompileError("XCL51",
                "create trigger tr1 after insert on session.temptable "
                + "referencing new table as new "
                + "insert into t1(i) select x from new");

        assertCompileError("XCL51",
                "create trigger tr2 after insert on t1 "
                + "insert into t2(i) select x from session.temptable");

        assertCompileError("XCL51",
                "create trigger tr3 after insert on t1 "
                + "insert into session.temptable values 1");

        // Used to fail
        assertCompileError("XCL51",
                "create trigger tr4 after insert on t1 "
                + "insert into t2(b) values exists("
                + "select * from session.temptable)");

        // Used to fail
        assertCompileError("XCL51",
                "create trigger tr5 after insert on t1 "
                + "insert into t2(i) values case when "
                + "exists(select * from session.temptable) then 1 else 2 end");

        // Used to fail
        assertCompileError("XCL51",
                "create trigger tr6 after insert on t1 "
                + "insert into t2(b) values "
                + "(select count(*) from session.temptable) = "
                + "(select count(*) from sysibm.sysdummy1)");

        // DERBY-6705
        assertCompileError("XCL51",
                "create trigger tr7 after insert on t1 "
                + "merge into t2 using session.temptable on i=x "
                + "when matched then delete");

        // DERBY-6705
        assertCompileError("XCL51",
                "create trigger tr8 after insert on t1 "
                + "merge into session.temptable using t2 on i=x "
                + "when matched then delete");

        // DERBY-6705
        assertCompileError("XCL51",
                "create trigger tr9 after insert on t1 "
                + "merge into t2 using t1 "
                + "on exists(select * from session.temptable where t1.x=t2.i) "
                + "when matched then delete");
    }

    /**
     * Verify the fix for DERBY-6371. The dependency checking done when
     * dropping a column used the wrong compilation schema and sometimes
     * incorrectly reported that a trigger depended on the column.
     */
    public void testDerby6371DropColumn() throws SQLException {
        Statement s = createStatement();
        s.execute("create schema d6371_s1");
        s.execute("create schema d6371_s2");
        s.execute("create table d6371_s1.t1(x int, y int)");
        s.execute("create table d6371_s1.t2(x int, y int)");
        s.execute("set schema 'D6371_S1'");

        commit();

        s.execute("create trigger d6371_s2.tr1 after update of x on t1 "
                + "for each row insert into t2(x) select x from t1");

        // Should not be allowed to drop column X, which is referenced by
        // the trigger.
        assertStatementError("X0Y25", s,
                             "alter table t1 drop column x restrict");
        assertStatementError("X0Y25", s,
                             "alter table t2 drop column x restrict");

        // Now drop a column that is not referenced from the trigger. Used
        // to fail with a message saying the trigger TR1 depended on it.
        s.execute("alter table t1 drop column y restrict");
        s.execute("alter table t2 drop column y restrict");

        // Verify that the trigger still works.
        s.execute("insert into t1 values 1");
        s.execute("update t1 set x = x + 1");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t2"), "2");

        // Go back to a clean set of tables.
        rollback();

        /* ---- End of the regression test case for the actual bug. ---- */

        // Extra check for a new code path that could be taken after the
        // fix. If the trigger is created by a user that has no schema, and
        // no explicit schema has been set in the connection, the trigger's
        // compilation schema will be NULL. Make sure that such a trigger
        // doesn't get into trouble during dependency validation. In
        // particular, we would like to avoid problems such as those in
        // DERBY-6361.
        Connection c2 =
                openDefaultConnection("D6371_USER_WITHOUT_SCHEMA", "secret");
        Statement s2 = c2.createStatement();
        s2.execute("create trigger d6371_s1.tr2 "
                + "after update of x on d6371_s1.t1 for each row "
                + "insert into d6371_s1.t2(x) select x from d6371_s1.t1");
        s2.close();
        c2.commit();
        c2.close();

        // Now exercise the dependency checking, both with columns that are
        // referenced by the trigger and columns that are not referenced.
        assertStatementError("X0Y25", s,
                             "alter table t1 drop column x restrict");
        assertStatementError("X0Y25", s,
                             "alter table t2 drop column x restrict");
        s.execute("alter table t1 drop column y restrict");
        s.execute("alter table t2 drop column y restrict");

        // And verify that the trigger works.
        s.execute("insert into t1 values 1");
        s.execute("update t1 set x = x + 1");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t2"), "2");
    }

    public void testDerby6348() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table d6348(x int)");
        s.execute("insert into d6348 values 1");
        s.execute("create trigger d6348_tr1 after update on d6348 values 1");
        s.execute("create trigger d6348_tr2 after update on d6348 "
                + "for each row update d6348 set x = x + 1 where x < 3");

        // Used to fail with assert failure or NullPointerException before
        // DERBY-6348.
        s.execute("update d6348 set x = x + 1");

        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from d6348"),
                "3");

        rollback();

        s.execute("create table d6348(x int)");
        s.execute("create trigger d6348_tr1 after insert on d6348 "
                + "values current_user");
        s.execute("create trigger d6348_tr2 after insert on d6348 "
                + "values current_user");

        // Used to fail with assert failure or NullPointerException before
        // DERBY-6348.
        s.execute("insert into d6348 values 1");
    }

    /**
     * Test that DROP operations detect if there are triggers depending on
     * the object being dropped, and either fail (if RESTRICT semantics) or
     * drop the trigger (if CASCADE semantics).
     */
    public void testDerby2041DropDependencies() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t1(x int, y int, z int)");
        s.execute("create table t2(x int, y int, z int)");
        s.execute("create table syn_table(x int, y int, z int)");
        s.execute("create table view_table(x int, y int, z int)");

        s.execute("create function f(x int) returns int language java "
                + "parameter style java external name 'java.lang.Math.abs'");
        s.execute("create procedure p() language java parameter style java "
                + "external name '" + getClass().getName()
                + ".dummyProc' no sql");
        s.execute("create function tf() returns table (x int) "
                + "language java parameter style derby_jdbc_result_set "
                + "external name '" + getClass().getName()
                + ".dummyTableFunction' no sql");
        s.execute("create derby aggregate intmode for int external name '"
                + ModeAggregate.class.getName() + "'");
        s.execute("create sequence seq");
        s.execute("create synonym syn for syn_table");
        s.execute("create view v(x) as select x from view_table");
        s.execute("create type tp external name 'java.util.List' language java");

        s.execute("create trigger tr_t2 after insert on t1 select x from t2");
        s.execute("create trigger tr_f after insert on t1 values f(1)");
        s.execute("create trigger tr_p after insert on t1 call p()");
        s.execute("create trigger tr_tf after insert on t1 "
                + "select * from table(tf()) t");
        s.execute("create trigger tr_intmode after insert on t1 "
                + "select intmode(x) from (values 1,2,3) v(x)");
        s.execute("create trigger tr_seq after insert on t1 "
                + "values next value for seq");
        s.execute("create trigger tr_syn after insert on t1 select * from syn");
        s.execute("create trigger tr_v after insert on t1 select * from v");
        s.execute("create trigger tr_tp after insert on t1 "
                + "values cast(null as tp)");

        PreparedStatement checkTrigger = prepareStatement(
            "select triggername from sys.systriggers join sys.sysschemas "
            + "using (schemaid) where triggername = ? and schemaname = ?");
        checkTrigger.setString(2, getTestConfiguration().getUserName());

        // DROP TABLE should fail because T2 is used in TR_T2.
        assertStatementError(HAS_DEPENDENT_TRIGGER, s, "drop table t2");
        checkTrigger.setString(1, "TR_T2");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_T2");

        // DROP FUNCTION should fail because F is used in TR_F.
        assertStatementError(HAS_DEPENDENT_TRIGGER, s, "drop function f");
        checkTrigger.setString(1, "TR_F");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_F");

        // DROP PROCEDURE should fail because P is used in TR_P.
        assertStatementError(HAS_DEPENDENT_TRIGGER, s, "drop procedure p");
        checkTrigger.setString(1, "TR_P");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_P");

        // DROP FUNCTION should fail because the table function TF is
        // used in TR_TF.
        assertStatementError(HAS_DEPENDENT_TRIGGER, s, "drop function tf");
        checkTrigger.setString(1, "TR_TF");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_TF");

        // DROP DERBY AGGREGATE only supports RESTRICT for now.
        assertStatementError(SYNTAX_ERROR, s,
                             "drop derby aggregate intmode cascade");
        assertStatementError(HAS_DEPENDENT_SPS, s,
                             "drop derby aggregate intmode restrict");
        checkTrigger.setString(1, "TR_INTMODE");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(),
                                        "TR_INTMODE");

        // DROP SEQUENCE only supports RESTRICT for now.
        assertStatementError(SYNTAX_ERROR, s, "drop sequence seq cascade");
        assertStatementError(HAS_DEPENDENT_SPS, s, "drop sequence seq restrict");
        checkTrigger.setString(1, "TR_SEQ");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_SEQ");

        // DROP SYNONYM should fail because SYN is used in TR_SYN.
        assertStatementError(HAS_DEPENDENT_TRIGGER, s, "drop synonym syn");
        checkTrigger.setString(1, "TR_SYN");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_SYN");

        // DROP VIEW should fail because V is used in TR_V.
        assertStatementError(HAS_DEPENDENT_TRIGGER, s, "drop view v");
        checkTrigger.setString(1, "TR_V");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_V");

        // DROP TYPE only supports RESTRICT for now.
        assertStatementError(SYNTAX_ERROR, s, "drop type tp cascade");
        assertStatementError(HAS_DEPENDENT_SPS, s, "drop type tp restrict");
        checkTrigger.setString(1, "TR_TP");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_TP");

        // DROP COLUMN should fail because TR_T2 uses column X.
        assertStatementError(HAS_DEPENDENT_TRIGGER, s,
                             "alter table t2 drop column x restrict");
        checkTrigger.setString(1, "TR_T2");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_T2");

        // DROP COLUMN should succeed in this case, since no trigger uses
        // column Y.
        s.execute("alter table t2 drop column y restrict");
        assertNull(s.getWarnings());
        checkTrigger.setString(1, "TR_T2");
        JDBC.assertSingleValueResultSet(checkTrigger.executeQuery(), "TR_T2");
        JDBC.assertColumnNames(s.executeQuery("select * from t2"),
                               new String[] {"X", "Z"});

        // DROP COLUMN should succeed because CASCADE is specified. Should
        // also remove the dependent trigger and produce a warning.
        s.execute("alter table t2 drop column x cascade");
        assertSQLState(TRIGGER_DROPPED, s.getWarnings());
        checkTrigger.setString(1, "TR_T2");
        JDBC.assertEmpty(checkTrigger.executeQuery());
        JDBC.assertColumnNames(s.executeQuery("select * from t2"),
                               new String[] {"Z"});
    }

    /**
     * Test that the fix for DERBY-2041 isn't too strict. Verify that some
     * operations only cause the dependent triggered statement to get
     * recompiled, and don't fail or cascade.
     */
    public void testDerby2041RecompileOnly() throws SQLException {
        Statement s = createStatement();

        PreparedStatement spsValid = prepareStatement("select valid from "
            + "sys.sysschemas join sys.systriggers using (schemaid) "
            + "join sys.sysstatements on stmtid = actionstmtid "
            + "where schemaname = ? and triggername = ?");
        spsValid.setString(1, getTestConfiguration().getUserName());
        spsValid.setString(2, "TR");

        // Dropping an index used by a trigger should not fail, and the
        // trigger should not be dropped.
        s.execute("create table t1(x int not null)");
        s.execute("create table t2(x int not null)");
        s.execute("create index idx on t2(x)");
        s.execute("create trigger tr after insert on t1 "
                + "insert into t2 values 1");
        // SPS should be valid before index is dropped.
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "true");
        s.execute("drop index idx");
        // SPS should be invalid after index is dropped, but the trigger
        // should still exist and work.
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "false");
        s.execute("insert into t1 values 1");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t2"), "1");
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "true");

        // Truncating a table referenced by a trigger should also be OK.
        s.execute("truncate table t2");
        assertTableRowCount("T2", 0);
        // SPS should be invalid after truncation, but the trigger should
        // still exist and work.
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "false");
        s.execute("insert into t1 values 1");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from t2"), "1");
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "true");

        // Now create a table T3 that has a foreign key constraint referencing
        // T2. Create a trigger on T1 that deletes from T2. The triggered
        // statement depends on T3 because it needs to check that the foreign
        // key constraint is not violated when rows are deleted from T2. Since
        // the trigger doesn't reference T3 directly, it should be possible to
        // drop T3 and simply recompile the triggered statement. Currently,
        // dropping the table fails because of the triggered statement's
        // dependency.
        s.execute("drop trigger tr");
        s.execute("alter table t2 add constraint t2_pk primary key (x)");
        s.execute("create table t3(x int, "
                + "y int references t2 on delete cascade)");
        s.execute("create trigger tr after delete on t1 delete from t2");
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "true");
        // Ideally, dropping T3 should be allowed, and the triggered
        // statement should have been marked as not valid (needs recompile).
        // Currently, it fails.
        assertStatementError(HAS_DEPENDENT_TRIGGER, s, "drop table t3");
        JDBC.assertSingleValueResultSet(spsValid.executeQuery(), "true");
    }

    /** Used as stored procedure in testDerby2041DropDependencies(). */
    public static void dummyProc() {
    }

    /** Used as table function in testDerby2041DropDependencies(). */
    public static ResultSet dummyTableFunction() {
        return null;
    }

    public void testDerby6540TransitionTableNameClash() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table d6540_t1(x int)");
        s.execute("create table d6540_t2(y int)");
        s.execute("create table d6540_t3(z int)");

        // Test name clash for statement level triggers.

        // The following statement used to fail before DERBY-6540 because
        // APP.D6540_T2 was mistaken for the transition table D6540_T2. Since
        // the transition table does not have a column Y, it would fail with
        // an error message saying that column Y is not in any table in the
        // FROM list.
        s.execute("create trigger d6540_tr after insert on d6540_t1 "
                + "referencing new table as d6540_t2 "
                + "insert into d6540_t3 select x from d6540_t2 "
                + "union all select y from app.d6540_t2");

        // Verify that the trigger does what it is supposed to do.
        PreparedStatement selT3
                = prepareStatement("select * from d6540_t3 order by z");
        JDBC.assertEmpty(selT3.executeQuery());

        s.execute("insert into d6540_t1 values 1");
        JDBC.assertSingleValueResultSet(selT3.executeQuery(), "1");

        s.execute("insert into d6540_t2 values 2, 3");
        s.execute("insert into d6540_t1 values 4, 5");
        JDBC.assertFullResultSet(
                selT3.executeQuery(),
                new String[][] { {"1"}, {"2"}, {"3"}, {"4"}, {"5"} });

        // Revert tables to clean state before we go on.
        s.execute("truncate table d6540_t1");
        s.execute("truncate table d6540_t2");
        s.execute("truncate table d6540_t3");
        s.execute("drop trigger d6540_tr");

        // Test name clash for row level triggers.

        // The following statement used to fail before DERBY-6540, with an
        // error message saying that column Y was not in any of the tables
        // in the FROM list.
        s.execute("create trigger d6540_tr after insert on d6540_t1 "
                + "referencing new as d6540_t2 for each row "
                + "insert into d6540_t3 select * from app.d6540_t2 "
                + "where d6540_t2.x = app.d6540_t2.y");

        // Verify that the trigger works.
        JDBC.assertEmpty(selT3.executeQuery());

        s.execute("insert into d6540_t1 values 1");
        JDBC.assertEmpty(selT3.executeQuery());

        s.execute("insert into d6540_t2 values 1, 2, 3");
        s.execute("insert into d6540_t1 values 2, 3, 4");
        JDBC.assertFullResultSet(
                selT3.executeQuery(), new String[][] { {"2"}, {"3"} });

        // Verify that row level triggers still don't need to qualify
        // table names that are the same as a transition variable, if they
        // appear in the from list (since the transition variable cannot be
        // used in the from list, so there is no ambiguity).
        s.execute("drop trigger d6540_tr");
        s.execute("create table d6540_t4(c1 int, c2 int)");
        s.execute("create trigger d6540_tr after insert on d6540_t1 "
                + "referencing new as d6540_t2 for each row "
                + "insert into d6540_t4 select y, d6540_t2.x from d6540_t2");
        s.execute("insert into d6540_t1 values 1");
        JDBC.assertFullResultSet(
                s.executeQuery("select * from d6540_t4 order by c1"),
                new String[][] {
                    { "1", "1" },
                    { "2", "1" },
                    { "3", "1" },
                });

        // Finally, verify that a transition table or transition variable
        // cannot have a schema.
        assertCompileError(SYNTAX_ERROR,
                "create trigger d6540_tr1 after insert on d6540_t1 "
                + "referencing new table as app.n values 1");
        assertCompileError(SYNTAX_ERROR,
                "create trigger d6540_tr2 after insert on d6540_t1 "
                + "referencing new as app.n for each row values 1");
    }

    /**
     * DERBY-6543: If a reference to a transition variable had blanks around
     * the period sign that separated the transition variable and the column
     * name, such as {@code NEW . X} instead of {@code NEW.X}, it would fail
     * with a syntax error.
     */
    public void testDerby6543() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table d6543_1(x int)");
        s.execute("create table d6543_2(x int)");

        // Used to fail with syntax error.
        s.execute("create trigger d6543_tr after insert on d6543_1 "
                + "referencing new as new for each row insert into d6543_2 "
                + "select x from d6543_1 where new . x = x");

        // Verify trigger works.
        assertUpdateCount(s, 4, "insert into d6543_1 values 1, 2, 2, 3");
        JDBC.assertFullResultSet(
                s.executeQuery("select * from d6543_2 order by x"),
                new String[][] { {"1"}, {"2"}, {"2"}, {"2"}, {"2"}, {"3"} });
    }

    /**
     * DERBY-6370: Test that trigger actions are stored with qualified names
     * in SYSTRIGGERS and SYSSTATEMENTS.
     */
    public void testQualifiedNamesInSystemTables() throws SQLException {
        Statement s = createStatement();
        s.execute("create schema d6370");
        s.execute("set schema d6370");
        s.execute("create table t1(x int, y int, z int)");
        s.execute("create table t2(x int, y int, z int)");
        s.execute("create table t3(x int, y int, z int)");
        s.execute("create table syn_table(x int, y int, z int)");
        s.execute("create table view_table(x int, y int, z int)");

        s.execute("create function f(x int) returns int language java "
                + "parameter style java external name 'java.lang.Math.abs'");
        s.execute("create procedure p() language java parameter style java "
                + "external name '" + getClass().getName()
                + ".dummyProc' no sql");
        s.execute("create function tf() returns table (x int) "
                + "language java parameter style derby_jdbc_result_set "
                + "external name '" + getClass().getName()
                + ".dummyTableFunction' no sql");
        s.execute("create derby aggregate intmode for int external name '"
                + ModeAggregate.class.getName() + "'");
        s.execute("create sequence seq");
        s.execute("create synonym syn for syn_table");
        s.execute("create view v(x) as select x from view_table");
        s.execute("create type tp external name 'java.util.List' language java");
        s.execute("create table tp_t1(x tp)");
        s.execute("create table tp_t2(x tp)");

        // Create triggers referencing all of the objects above.
        s.execute("create trigger tr01 no cascade before insert on t1 "
                + "when (exists(select f(y) from v join t1 t on v.x = t.x)) "
                + "call p()");
        s.execute("create trigger tr02 after insert on t1 "
                + "when (exists(select * from table(tf()) t)) "
                + "insert into t2(z) select 1 from t1");
        s.execute("create trigger tr03 after delete on t1 "
                + "insert into t2(z) select intmode(x) from syn");
        s.execute("create trigger tr04 after insert on tp_t1 "
                + "referencing new as new for each row "
                + "insert into tp_t2 values new.x, cast(null as tp)");
        s.execute("create trigger tr05 after insert on t1 "
                + "referencing new table as new "
                + "when (next value for seq < 1000) "
                + "insert into t2(y) select a.z from new a, t1 b, new c, t1 d");

        // Table names in the SET clause of an UPDATE statement don't get
        // qualified because of oddities in the way such statements are
        // bound. Probably related to DERBY-6558.
        s.execute("create trigger tr06 after insert on t1 "
                + "update t2 set t2.x = t2.y");
        // Same with target columns in INSERT statements.
        s.execute("create trigger tr07 after insert on t1 "
                + "insert into t2 (t2.x, t2.y) values (1, default)");

        s.execute("create trigger tr08 after update on t1 "
                + "delete from t2 where t2.x = t2.y");
        s.execute("create trigger tr09 after delete on t1 "
                + "merge into t2 using t3 on t2.x = t3.x "
                + "when matched and t3.y = 5 then update set t2.x = t3.y "
                + "when not matched then insert values (t3.x, t3.y, t3.z)");
        s.execute("create trigger tr10 after insert on t1 "
                + "referencing new as new for each row update t2 set x = "
                + "(select count(*) from t1 where new.x = t2.x)");

        // Now create two triggers that both reference the SIN function
        // without specifying the schema. Create a SIN function in the
        // current schema between the creation of the two triggers. The
        // first trigger should reference SYSFUN.SIN, and the second one
        // should reference D6370.SIN. See also DERBY-5901.
        s.execute("create trigger tr11 after insert on t1 for each row "
                + "values sin(0)");
        s.execute("create function sin(x double) returns double language java "
                + "parameter style java external name 'java.lang.Math.sin'");
        s.execute("create trigger tr12 after insert on t1 for each row "
                + "values sin(0)");

        String[][] expectedRows = {
            {"TR01", "exists(select \"D6370\".\"F\"(y) from \"D6370\".\"V\" join \"D6370\".\"T1\" t on \"V\".x = \"T\".x)", "VALUES exists(select \"D6370\".\"F\"(y) from \"D6370\".\"V\" join \"D6370\".\"T1\" t on \"V\".x = \"T\".x)", "call \"D6370\".\"P\"()", "call \"D6370\".\"P\"()"},
            {"TR02", "exists(select * from table(\"D6370\".\"TF\"()) t)", "VALUES exists(select * from table(\"D6370\".\"TF\"()) t)", "insert into \"D6370\".\"T2\"(z) select 1 from \"D6370\".\"T1\"", "insert into \"D6370\".\"T2\"(z) select 1 from \"D6370\".\"T1\""},
            {"TR03", null, null, "insert into \"D6370\".\"T2\"(z) select \"D6370\".\"INTMODE\"(x) from \"D6370\".\"SYN\"", "insert into \"D6370\".\"T2\"(z) select \"D6370\".\"INTMODE\"(x) from \"D6370\".\"SYN\""},
            {"TR04", null, null, "insert into \"D6370\".\"TP_T2\" values new.x, cast(null as \"D6370\".\"TP\")", "insert into \"D6370\".\"TP_T2\" values CAST (org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().getNewRow().getObject(1) AS \"D6370\".\"TP\") , cast(null as \"D6370\".\"TP\")"},
            {"TR05", "next value for \"D6370\".\"SEQ\" < 1000", "VALUES next value for \"D6370\".\"SEQ\" < 1000", "insert into \"D6370\".\"T2\"(y) select \"A\".z from new a, \"D6370\".\"T1\" b, new c, \"D6370\".\"T1\" d", "insert into \"D6370\".\"T2\"(y) select \"A\".z from new org.apache.derby.catalog.TriggerNewTransitionRows()  a, \"D6370\".\"T1\" b, new org.apache.derby.catalog.TriggerNewTransitionRows()  c, \"D6370\".\"T1\" d"},
            {"TR06", null, null, "update \"D6370\".\"T2\" set t2.x = \"T2\".y", "update \"D6370\".\"T2\" set t2.x = \"T2\".y"},
            {"TR07", null, null, "insert into \"D6370\".\"T2\" (t2.x, t2.y) values (1, default)", "insert into \"D6370\".\"T2\" (t2.x, t2.y) values (1, default)"},
            {"TR08", null, null, "delete from \"D6370\".\"T2\" where \"D6370\".\"T2\".x = \"D6370\".\"T2\".y", "delete from \"D6370\".\"T2\" where \"D6370\".\"T2\".x = \"D6370\".\"T2\".y"},
            {"TR09", null, null, "merge into \"D6370\".\"T2\" using \"D6370\".\"T3\" on \"D6370\".\"T2\".x = \"D6370\".\"T3\".x when matched and \"D6370\".\"T3\".y = 5 then update set t2.x = \"D6370\".\"T3\".y when not matched then insert values (\"D6370\".\"T3\".x, \"D6370\".\"T3\".y, \"D6370\".\"T3\".z)", "merge into \"D6370\".\"T2\" using \"D6370\".\"T3\" on \"D6370\".\"T2\".x = \"D6370\".\"T3\".x when matched and \"D6370\".\"T3\".y = 5 then update set t2.x = \"D6370\".\"T3\".y when not matched then insert values (\"D6370\".\"T3\".x, \"D6370\".\"T3\".y, \"D6370\".\"T3\".z)"},
            {"TR10", null, null, "update \"D6370\".\"T2\" set x = (select count(*) from \"D6370\".\"T1\" where new.x = \"D6370\".\"T2\".x)", "update \"D6370\".\"T2\" set x = (select count(*) from \"D6370\".\"T1\" where CAST (org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().getNewRow().getObject(1) AS INTEGER)  = \"D6370\".\"T2\".x)"},
            {"TR11", null, null, "values \"SYSFUN\".\"SIN\"(0)", "values \"SYSFUN\".\"SIN\"(0)"},
            {"TR12", null, null, "values \"D6370\".\"SIN\"(0)", "values \"D6370\".\"SIN\"(0)"},
        };
        ResultSet rs = s.executeQuery(
                "select triggername, whenclausetext, "
                + "s1.text, triggerdefinition, s2.text "
                + "from sys.systriggers join sys.sysschemas using (schemaid) "
                + "left join sys.sysstatements s1 on whenstmtid = stmtid "
                + "join sys.sysstatements s2 on actionstmtid = s2.stmtid "
                + "where schemaname = 'D6370' order by triggername");
        JDBC.assertFullResultSet(rs, expectedRows);

        // Fire the triggers.
        // disabled due to DERBY-6554
        //s.execute("insert into t1 values (1,2,3)");
        s.execute("insert into tp_t1 values cast(null as tp)");
    }

    /**
     * Regression test case for DERBY-6663 (NPE when a trigger tries to
     * insert into a table with a foreign key).
     */
    public void testDerby6663() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table d6663_t1(pk int primary key)");
        s.execute("create table d6663_t2(x int references d6663_t1)");
        s.execute("create table d6663_t3(y int)");
        s.execute("create trigger d6663_tr after insert on d6663_t3 "
                + "referencing new as new for each row "
                + "insert into d6663_t2 values new.y");

        // Used to fail with NPE instead of foreign key violation.
        assertStatementError(
                FOREIGN_KEY_VIOLATION, s, "insert into d6663_t3 values 1");

        // Verify that trigger executes successfully if there is no
        // foreign key violation.
        s.execute("insert into d6663_t1 values 1");
        s.execute("insert into d6663_t3 values 1");
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select * from d6663_t2"),
                "1");
    }
}
