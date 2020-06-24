package org.apache.derbyTesting.functionTests.tests.largedata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;


/*
Class org.apache.derbyTesting.functionTests.tests.largedata.Derby6317Test


Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/


/**
Test to reproduce DERBY-6317(Optmizer can choose the wrong path when 
BTreeCostController.java returns an estimate cost and row count of 0.0)

This test creates three tables and creates primary keys and foreign
key constraints on them. Then we insert really large data in those
three tables. Prior to DERBY-6317, there were times when we would do
table scan rather than using index on a give join query. It turned out
that the reason was that estimated count was less than 0.5 for such
queries and hence it got rounded to 0 which caused the optimizer to
do a table scan instead of index scan.

Query plan for a table scan - buggy scenario -
    notice t0.Table3_ID is assigned 5189284
    
Statement Text: 
	SELECT * FROM Table1 T1,Table2 t0 WHERE t1.ID = t0.Table1_ID and t0.Table3_ID = 5189284
Number of opens = 1
Rows seen = 0
Rows filtered = 0
restriction = false
projection = true
	constructor time (milliseconds) = 0
	open time (milliseconds) = 0
	next time (milliseconds) = 0
	close time (milliseconds) = 0
	restriction time (milliseconds) = 0
	projection time (milliseconds) = 0
	optimizer estimated row count: 0.00
	optimizer estimated cost: 6.33
Source result set:
	Nested Loop Join ResultSet:
	Number of opens = 1
	Rows seen from the left = 1
	Rows seen from the right = 0
	Rows filtered = 0
	Rows returned = 0
		constructor time (milliseconds) = 0
		open time (milliseconds) = 0
		next time (milliseconds) = 0
		close time (milliseconds) = 0
		optimizer estimated row count: 0.00
		optimizer estimated cost: 6.33
	Left result set:
		Index Row to Base Row ResultSet for TABLE2:
		Number of opens = 1
		Rows seen = 1
		Columns accessed from heap = {0, 1}
			constructor time (milliseconds) = 0
			open time (milliseconds) = 0
			next time (milliseconds) = 0
			close time (milliseconds) = 0
			optimizer estimated row count: 0.00
			optimizer estimated cost: 6.33
			Index Scan ResultSet for TABLE2 using constraint TABLE2_FK_2 at read committed isolation level using instantaneous share row locking chosen by the optimizer
			Number of opens = 1
			Rows seen = 1
			Rows filtered = 0
			Fetch Size = 16
				constructor time (milliseconds) = 0
				open time (milliseconds) = 0
				next time (milliseconds) = 0
				close time (milliseconds) = 0
				next time in milliseconds/row = 0

			scan information:
				Bit set of columns fetched=All
				Number of columns fetched=2
				Number of deleted rows visited=0
				Number of pages visited=4
				Number of rows qualified=1
				Number of rows visited=2
				Scan type=btree
				Tree height=4
				start position:
					&gt;= on first 1 column(s).
					Ordered null semantics on the following columns: 
					0 
				stop position:
					&gt; on first 1 column(s).
					Ordered null semantics on the following columns: 
					0 
				qualifiers:
					None
				optimizer estimated row count: 0.00
				optimizer estimated cost: 6.33

	Right result set:
		Table Scan ResultSet for TABLE1 at read committed isolation level using instantaneous share row locking chosen by the optimizer
		Number of opens = 1
		Rows seen = 0
		Rows filtered = 0
		Fetch Size = 16
			constructor time (milliseconds) = 0
			open time (milliseconds) = 0
			next time (milliseconds) = 0
			close time (milliseconds) = 0

		scan information:
			Bit set of columns fetched=All
			Number of columns fetched=1
			Number of pages visited=0
			Number of rows qualified=0
			Number of rows visited=0
			Scan type=heap
			start position:
				null
			stop position:
				null
			qualifiers:
				Column[0][0] Id: 0
				Operator: =
				Ordered nulls: false
				Unknown return value: false
				Negate comparison result: false
			optimizer estimated row count: 0.00
			optimizer estimated cost: 0.00



Query plan for index scan - the correct behavior - 
    notice t0.Table3_ID is assigned 5189285

Statement Text: 
	SELECT * FROM Table1 T1,Table2 t0 WHERE t1.ID = t0.Table1_ID and t0.Table3_ID = 5189285
Number of opens = 1
Rows seen = 1
Rows filtered = 0
restriction = false
projection = true
	constructor time (milliseconds) = 0
	open time (milliseconds) = 0
	next time (milliseconds) = 0
	close time (milliseconds) = 0
	restriction time (milliseconds) = 0
	projection time (milliseconds) = 0
	optimizer estimated row count: 1.00
	optimizer estimated cost: 12.72
Source result set:
	Scalar Aggregate ResultSet:
	Number of opens = 1
	Rows input = 1
		constructor time (milliseconds) = 0
		open time (milliseconds) = 0
		next time (milliseconds) = 0
		close time (milliseconds) = 0
		optimizer estimated row count: 1.00
		optimizer estimated cost: 12.72
	Index Key Optimization = false
	Source result set:
		Project-Restrict ResultSet (6):
		Number of opens = 1
		Rows seen = 1
		Rows filtered = 0
		restriction = false
		projection = true
			constructor time (milliseconds) = 0
			open time (milliseconds) = 0
			next time (milliseconds) = 0
			close time (milliseconds) = 0
			restriction time (milliseconds) = 0
			projection time (milliseconds) = 0
			optimizer estimated row count: 1.00
			optimizer estimated cost: 12.72
		Source result set:
			Nested Loop Exists Join ResultSet:
			Number of opens = 1
			Rows seen from the left = 1
			Rows seen from the right = 1
			Rows filtered = 0
			Rows returned = 1
				constructor time (milliseconds) = 0
				open time (milliseconds) = 0
				next time (milliseconds) = 0
				close time (milliseconds) = 0
				optimizer estimated row count: 1.00
				optimizer estimated cost: 12.72
			Left result set:
				Index Row to Base Row ResultSet for TABLE2:
				Number of opens = 1
				Rows seen = 1
				Columns accessed from heap = {0, 1}
					constructor time (milliseconds) = 0
					open time (milliseconds) = 0
					next time (milliseconds) = 0
					close time (milliseconds) = 0
					optimizer estimated row count: 1.00
					optimizer estimated cost: 8.01
					Index Scan ResultSet for TABLE2 using constraint TABLE2_FK_2 at read committed isolation level using instantaneous share row locking chosen by the optimizer
					Number of opens = 1
					Rows seen = 1
					Rows filtered = 0
					Fetch Size = 16
						constructor time (milliseconds) = 0
						open time (milliseconds) = 0
						next time (milliseconds) = 0
						close time (milliseconds) = 0
						next time in milliseconds/row = 0

					scan information:
						Bit set of columns fetched=All
						Number of columns fetched=2
						Number of deleted rows visited=0
						Number of pages visited=4
						Number of rows qualified=1
						Number of rows visited=2
						Scan type=btree
						Tree height=-1
						start position:
							&gt;= on first 1 column(s).
							Ordered null semantics on the following columns: 
							0 
						stop position:
							&gt; on first 1 column(s).
							Ordered null semantics on the following columns: 
							0 
						qualifiers:
							None
						optimizer estimated row count: 1.00
						optimizer estimated cost: 8.01

			Right result set:
				Index Scan ResultSet for TABLE1 using constraint SQL130904105604940 at read committed isolation level using share row locking chosen by the optimizer
				Number of opens = 1
				Rows seen = 1
				Rows filtered = 0
				Fetch Size = 1
					constructor time (milliseconds) = 0
					open time (milliseconds) = 0
					next time (milliseconds) = 0
					close time (milliseconds) = 0
					next time in milliseconds/row = 0

				scan information:
					Bit set of columns fetched={0}
					Number of columns fetched=1
					Number of deleted rows visited=0
					Number of pages visited=3
					Number of rows qualified=1
					Number of rows visited=1
					Scan type=btree
					Tree height=3
					start position:
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
						&gt;= on first 1 column(s).
						Ordered null semantics on the following columns: 
						0 
					stop position:
						&gt; on first 1 column(s).
						Ordered null semantics on the following columns: 
						0 
					qualifiers:
						None
					optimizer estimated row count: 1.00
					optimizer estimated cost: 4.71



**/

public class Derby6317Test extends BaseJDBCTestCase
{
    final static int DATABASE_SCALE = 1000000;
    final static int TABLE_ONE_ROW_COUNT = DATABASE_SCALE;
    final static int TABLE_THREE_ROW_COUNT = DATABASE_SCALE*8;
    final static int SELECT_ROWS_COUNT = DATABASE_SCALE*8;

    final String testSelect=
        "SELECT * FROM "+
        "Table1 T1,"+
        "Table2 t0 "+
        "WHERE t1.ID = t0.Table1_ID and "+
        "t0.Table3_ID = "; 

    public Derby6317Test(String name) 
    {
        super(name);
    }

    public static Test suite() 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("Derby6317Test");
        suite.addTest(baseSuite("Derby6317Test:embedded"));
        return suite;
    }
    
    //Confirm that both the tables involved in the SELECT statement are using
    // index scan. Prior to fix for DERBY-6317, some SOME_CONSTANT values
    // would do table scan on TABLE1 rather than an index scan.
    // SELECT * FROM Table1 T1, Table2 t0 
    //  WHERE t1.ID = t0.Table1_ID and t0.Table3_ID = SOME_CONSTANT
    private void confirmIndexScanUsage(Statement stmt, int some_constant)
        throws SQLException {
        RuntimeStatisticsParser rtsp;
        boolean constraintUsed;
        rtsp = SQLUtilities.getRuntimeStatisticsParser(stmt);
        constraintUsed = rtsp.usedConstraintForIndexScan("TABLE1");
        if (!constraintUsed){
            assertTrue("Should have done index scan but did table scan on " + 
                "TABLE1 for t0.Table3_ID = "+some_constant, constraintUsed);
        }
        constraintUsed = rtsp.usedConstraintForIndexScan("TABLE2");
        if (!constraintUsed){
            assertTrue("Should have done index scan but did table scan on " +
                "TABLE2 for t0.Table3_ID = "+some_constant, constraintUsed);
        }    	
    }
    
    //Test just one specific value in the SELECT and see what kind of plan
    // is picked up for it.
    //5189284 value incorrectly picked TABLE scan prior to DERBY-6317 fix
    public void testDERBY_6317_value1()
        throws SQLException {
        Statement stmt = createStatement();
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        stmt.execute(testSelect + 5189284);
        confirmIndexScanUsage(stmt, 5189284);
    }
    
    //Test just one specific value in the SELECT and see what kind of plan
    // is picked up for it.
    //6035610 value incorrectly picked TABLE scan prior to DERBY-6317 fix
    public void testDERBY_6317_value2()
        throws SQLException {
        Statement stmt = createStatement();
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        stmt.execute(testSelect + 6035610);
        confirmIndexScanUsage(stmt, 6035610);
    }
    
    //Test just one specific value in the SELECT and see what kind of plan
    // is picked up for it.
    //6031628 value incorrectly picked TABLE scan prior to DERBY-6317 fix
    public void testDERBY_6317_value3()
        throws SQLException {
        Statement stmt = createStatement();
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        stmt.execute(testSelect + 6031628);
        confirmIndexScanUsage(stmt, 6031628);
    }
    
    //Test just one specific value in the SELECT and see what kind of plan
    // is picked up for it.
    //5189284 value always picked INDEX scan ie even prior to DERBY-6317 fix
    public void testDERBY_6317_value4()
        throws SQLException {
        Statement stmt = createStatement();
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        stmt.execute(testSelect + 5189285);
        confirmIndexScanUsage(stmt, 5189285);
    }
    
    //Test just one specific value in the SELECT and see what kind of plan
    // is picked up for it.
    //6035609 value always picked INDEX scan ie even prior to DERBY-6317 fix
    public void testDERBY_6317_value6()
        throws SQLException {
        Statement stmt = createStatement();
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        stmt.execute(testSelect + 6035609);
        confirmIndexScanUsage(stmt, 6035609);
    }
    
    //Test just one specific value in the SELECT and see what kind of plan
    // is picked up for it.
    //1 value always picked INDEX scan ie even prior to DERBY-6317 fix
    public void testDERBY_6317_value5()
        throws SQLException {
        Statement stmt = createStatement();
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        stmt.execute(testSelect + 1);
        confirmIndexScanUsage(stmt, 1);
    }

    //This test is really really time consuming because it is going through
    // 8million selects individually. Before DERBY-6317, it would take 
    // about 4-5hrs to finish and stop right after the first failure which
    // was on 5,189,284. The leftover selects out of 8million did not get
    // run because of the failure. But once DERBY-6317 is fixed, the test
    // takes even longer because it will go through 8 million rows rather
    // than stop after about 5million rows. The time it takes for the 
    // test to finish after DERBY-6317 is about 10hrs
    //
    // TODO - test name does not start with "test" so not run by default
    // in the largedata suite, due to time it takes to run.  May make sense
    // to run this test once a release.  The other fixtures currently
    // test the problem area, but even a subtle change to the layout of
    // records in the btree could make the particular values chosen not
    // repro the previous bug.
    public void dontrun_testDERBY_6317()
        throws SQLException {
        Statement stmt = createStatement();
        stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        for (int i = 0; i < SELECT_ROWS_COUNT; i++) { 
            stmt.execute(testSelect+i);
            confirmIndexScanUsage(stmt,i);
        }
    }

    protected static Test baseSuite(String name) 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(Derby6317Test.class);
        return new CleanDatabaseTestSetup(suite)
        {
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                Connection conn = stmt.getConnection();

                stmt.executeUpdate("CREATE TABLE Table1 ("+
                		"ID int PRIMARY KEY NOT NULL)");

                stmt.executeUpdate("CREATE TABLE Table2 ("+
                		"Table1_ID int NOT NULL,"+
                		"Table3_ID int NOT NULL,"+
                		"CONSTRAINT TABLE2_PK PRIMARY KEY "+
                		"(Table1_ID,Table3_ID))");
                
                stmt.executeUpdate("CREATE TABLE Table3 ("+
                		"ID int PRIMARY KEY NOT NULL)"); 

                stmt.executeUpdate("ALTER TABLE table2 "+
                        "ADD CONSTRAINT TABLE2_FK_1 "+
                        "FOREIGN KEY (Table1_ID) "+
                        "REFERENCES TABLE1(ID)");
                stmt.executeUpdate("ALTER TABLE table2 "+
                        "ADD CONSTRAINT TABLE2_FK_2 "+
                        "FOREIGN KEY (Table3_ID) "+
                        "REFERENCES TABLE3(ID)");

                conn.setAutoCommit(false);
                
                PreparedStatement insertPS = getConnection().prepareStatement(
            		  "INSERT INTO table1 VALUES (?)");
                for (int i = 0; i < TABLE_ONE_ROW_COUNT; i++) { 
                    insertPS.setInt(1,i);
                    insertPS.execute();
                    if (i%10000 == 0)
      	    		    conn.commit();
                } 
                conn.commit();
                int count = TABLE_THREE_ROW_COUNT; 
                insertPS = conn.prepareStatement("INSERT INTO table3 VALUES (?)");
                for (int i = 0; i < count; i++) { 
                    insertPS.setInt(1,i);
                    insertPS.execute();
          	        if (i%10000 == 0)
          	            conn.commit();
                } 
                conn.commit();
                //In TABLE2, we will insert 8 million rows.
                insertPS = conn.prepareStatement("INSERT INTO table2 VALUES (?,?)");
                for (int i = 0, j=0; i < TABLE_ONE_ROW_COUNT; i++) { 
                	insertPS.setInt(1,i);
       	            for (int k=0; k<8; k++,j++) {
                        insertPS.setInt(2,j);
          	            insertPS.execute();
                    }
              	    if (i%10000 == 0)
              	        conn.commit();
                }
                conn.commit();
            }
        };
    }
}
