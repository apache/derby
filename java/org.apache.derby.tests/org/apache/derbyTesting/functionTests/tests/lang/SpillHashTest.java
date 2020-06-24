/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SpillHashTest
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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;

/**
 * Test BackingStoreHashtable spilling to disk.
 * BackingStoreHashtable is used to implement hash joins, distinct, scroll insensitive cursors,
 * outer joins, and the HAVING clause.
 */
public class SpillHashTest extends BaseJDBCTestCase {

    private static final String[] prep = {
        "create table ta (ca1 integer, ca2 char(200))",
        "create table tb (cb1 integer, cb2 char(200))",
        "insert into ta(ca1,ca2) values(null, 'Anull')",
        "insert into tb(cb1,cb2) values(null, 'Bnull')"
    };	
    
    private static final String[][] initDupVals = {
        { "0a", "0b"},
        { "1a", "1b"},
        { "2a"}
    };
    
    private static final String[][] spillDupVals = { 
    	{}, 
    	{ "1c" }, 
    	{ "2b" },
		{ "3a", "3b", "3c" } 
    };
    
    private static final int LOTS_OF_ROWS = 10000;
    
    private PreparedStatement joinStmt;
    private PreparedStatement distinctStmt;    
	
	/**
	 * Basic constructor.
	 */
	public SpillHashTest(String name) {
		super(name);
	}
	
	/**
	 * Sets the auto commit to false.
	 */
	protected void initializeConnection(Connection conn) throws SQLException {
		conn.setAutoCommit(false);
	}
		
	/**
	 * Returns the implemented tests.
	 * 
	 * @return An instance of <code>Test</code> with the implemented tests to
	 *         run.
	 */
	public static Test suite() {
        // suite of tests with light load on the tables
        BaseTestSuite light = new BaseTestSuite();
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        // suite of tests with heavy load on the tables
        BaseTestSuite heavy = new BaseTestSuite();
		
		light.addTest(new SpillHashTest("testJoinLight"));
		light.addTest(new SpillHashTest("testDistinctLight"));
		light.addTest(new SpillHashTest("testCursorLight"));
		heavy.addTest(new SpillHashTest("testJoinHeavy"));
		heavy.addTest(new SpillHashTest("testDistinctHeavy"));
		heavy.addTest(new SpillHashTest("testCursorHeavy"));		
		
		Test lightSetup = new BaseJDBCTestSetup(light) {
			protected void setUp() throws Exception {
				super.setUp();
				Statement stmt = getConnection().createStatement();
	            PreparedStatement insA = stmt.getConnection().prepareStatement("insert into ta(ca1,ca2) values(?,?)");
	            PreparedStatement insB = stmt.getConnection().prepareStatement("insert into tb(cb1,cb2) values(?,?)");
	            	            
	            insertDups(insA, insB, initDupVals);
	            getConnection().commit();
	            stmt.close();
	            
	            //System.out.println("2");
			}
		};
		
		Test heavySetup = new BaseJDBCTestSetup(heavy) {
			protected void setUp() throws Exception {
				super.setUp();
				Statement stmt = getConnection().createStatement();
	            PreparedStatement insA = stmt.getConnection().prepareStatement("insert into ta(ca1,ca2) values(?,?)");
	            PreparedStatement insB = stmt.getConnection().prepareStatement("insert into tb(cb1,cb2) values(?,?)");				
	            
	            for (int i = 1; i <= LOTS_OF_ROWS; i++) {
					insA.setInt(1, i);
					insA.setString(2, ca2Val(i));
					insA.executeUpdate();
					insB.setInt(1, i);
					insB.setString(2, cb2Val(i));
					insB.executeUpdate();

					if ((i & 0xff) == 0)
						stmt.getConnection().commit();
				}
				insertDups(insA, insB, spillDupVals);
				
				getConnection().commit();
				stmt.close();
				
				//System.out.println("3");				
			}
		};
		
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite mainSuite = new BaseTestSuite();
		
		mainSuite.addTest(lightSetup);
		mainSuite.addTest(heavySetup);
		
		return new CleanDatabaseTestSetup(mainSuite) {
			protected void decorateSQL(Statement stmt) throws SQLException {
	            for(int i = 0; i < prep.length; i++) {
	            	stmt.executeUpdate(prep[i]);
	            }
	            
	            //System.out.println("1");
			}						
		};
	}

	protected void setUp() throws Exception {
        joinStmt = getConnection().prepareStatement("select ta.ca1, ta.ca2, tb.cb2 from ta, tb where ca1 = cb1");
		distinctStmt = getConnection().prepareStatement("select distinct ca1 from ta");
	}

	protected void tearDown() throws Exception {
		joinStmt.close();
		distinctStmt.close();
        joinStmt = null;
        distinctStmt = null;
		super.tearDown();
	}

	public void testJoinLight() throws SQLException {
		runJoin(getConnection(), 0, new String[][][] {initDupVals});
	}
	
	public void testDistinctLight() throws SQLException {
		runDistinct(getConnection(), 0, new String[][][] {initDupVals});
	}
	
	public void testCursorLight() throws SQLException {
		runCursor(getConnection(), 0, new String[][][] {initDupVals});
	}

	public void testJoinHeavy() throws SQLException {
		runJoin(getConnection(), LOTS_OF_ROWS, new String[][][] {initDupVals, spillDupVals});
	}
	
	public void testDistinctHeavy() throws SQLException {
		runDistinct(getConnection(), LOTS_OF_ROWS, new String[][][] {initDupVals, spillDupVals});
	}
	
	public void testCursorHeavy() throws SQLException {
		runCursor(getConnection(), LOTS_OF_ROWS, new String[][][] {initDupVals, spillDupVals});
	}
	
	private static void insertDups(PreparedStatement insA,
			PreparedStatement insB, String[][] dupVals) throws SQLException {
		for (int i = 0; i < dupVals.length; i++) {
			insA.setInt(1, -i);
			insB.setInt(1, -i);
			String[] vals = dupVals[i];
			for (int j = 0; j < vals.length; j++) {
				insA.setString(2, "A" + vals[j]);
				insA.executeUpdate();
				insB.setString(2, "B" + vals[j]);
				insB.executeUpdate();
			}
		}
	} 
	
    private static String ca2Val(int col1Val) {
		return "A" + col1Val;
	}

	private static String cb2Val(int col1Val) {
		return "B" + col1Val;
	}
	
    private void runJoin(Connection conn, int maxColValue,
			String[][][] dupVals) throws SQLException {
		int expectedRowCount = maxColValue; // plus expected duplicates, to be counted below
		ResultSet rs = joinStmt.executeQuery();
		BitSet joinRowFound = new BitSet(maxColValue);
		int dupKeyCount = 0;

		for (int i = 0; i < dupVals.length; i++) {
			if (dupVals[i].length > dupKeyCount)
				dupKeyCount = dupVals[i].length;
		}
		BitSet[] dupsFound = new BitSet[dupKeyCount];
		int[] dupCount = new int[dupKeyCount];
		for (int i = 0; i < dupKeyCount; i++) {
			// count the number of rows with column(1) == -i
			dupCount[i] = 0;
			for (int j = 0; j < dupVals.length; j++) {
				if (i < dupVals[j].length)
					dupCount[i] += dupVals[j][i].length;
			}
			dupsFound[i] = new BitSet(dupCount[i] * dupCount[i]);
			expectedRowCount += dupCount[i] * dupCount[i];
		}

		int count;
		for (count = 0; rs.next(); count++) {
			int col1Val = rs.getInt(1);
			
			assertFalse("Null in join column.", rs.wasNull());
			assertFalse("Invalid value in first join column.", col1Val > maxColValue);
			if (col1Val > 0) {
				assertFalse("Multiple rows for value " + col1Val, joinRowFound.get(col1Val - 1));
				joinRowFound.set(col1Val - 1);
				String col2Val = trim(rs.getString(2));
				String col3Val = trim(rs.getString(3));
				assertFalse("Incorrect value in column 2 or 3 of join.", 
						!(ca2Val(col1Val).equals(col2Val) && cb2Val(col1Val).equals(col3Val)));
			} else {
				// col1Val <= 0, there are duplicates in the source tables
				int dupKeyIdx = -col1Val;
				int col2Idx = findDupVal(rs, 2, 'A', dupKeyIdx, dupVals);
				int col3Idx = findDupVal(rs, 3, 'B', dupKeyIdx, dupVals);
				if (col2Idx < 0 || col3Idx < 0) {
					continue;
				}
				int idx = col2Idx + dupCount[dupKeyIdx] * col3Idx;
				assertFalse("Repeat of row with key value 0", dupsFound[dupKeyIdx].get(idx));
				dupsFound[dupKeyIdx].set(idx);
			}
		}
		
		assertEquals("Incorrect number of rows in join.", expectedRowCount, count);
		rs.close();
	}

	private static int findDupVal(ResultSet rs, int col, char prefix,
			int keyIdx, String[][][] dupVals) throws SQLException {
		String colVal = rs.getString(col);
		if (colVal != null && colVal.length() > 1 || colVal.charAt(0) == prefix) {
			colVal = trim(colVal.substring(1));
			int dupIdx = 0;
			for (int i = 0; i < dupVals.length; i++) {
				if (keyIdx < dupVals[i].length) {
					for (int j = 0; j < dupVals[i][keyIdx].length; j++, dupIdx++) {
						if (colVal.equals(dupVals[i][keyIdx][j]))
							return dupIdx;
					}
				}
			}
		}
		fail("Incorrect value in column " + col + " of join with duplicate keys.");
		return -1;
	} // end of findDupVal

	private static String trim(String str) {
		if (str == null)
			return str;
		return str.trim();
	}

	private void runDistinct(Connection conn, int maxColValue,
			String[][][] dupVals) throws SQLException {
		//System.out.println("Running distinct");
		ResultSet rs = distinctStmt.executeQuery();
		checkAllCa1(rs, false, false, maxColValue, dupVals, "DISTINCT");
		rs.close();
	}

	private static void runCursor(Connection conn, int maxColValue,
			String[][][] dupVals) throws SQLException {
		//System.out.println("Running scroll insensitive cursor");
		DatabaseMetaData dmd = conn.getMetaData();
		boolean holdOverCommit = dmd.supportsOpenCursorsAcrossCommit();
		Statement stmt;
		if (holdOverCommit)
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY,
					ResultSet.HOLD_CURSORS_OVER_COMMIT);
		else
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = stmt.executeQuery("SELECT ca1 FROM ta");
		checkAllCa1(rs, true, holdOverCommit, maxColValue, dupVals,
				"scroll insensitive cursor");
		rs.close();
	}

	private static void checkAllCa1(ResultSet rs, boolean expectDups,
			boolean holdOverCommit, int maxColValue, String[][][] dupVals,
			String label) throws SQLException {
		int dupKeyCount = 0;
		for (int i = 0; i < dupVals.length; i++) {
			if (dupVals[i].length > dupKeyCount)
				dupKeyCount = dupVals[i].length;
		}
		int[] expectedDupCount = new int[dupKeyCount];
		int[] dupFoundCount = new int[dupKeyCount];
		for (int i = 0; i < dupKeyCount; i++) {

			dupFoundCount[i] = 0;
			if (!expectDups)
				expectedDupCount[i] = 1;
			else {
				expectedDupCount[i] = 0;
				for (int j = 0; j < dupVals.length; j++) {
					if (i < dupVals[j].length)
						expectedDupCount[i] += dupVals[j][i].length;
				}
			}
		}
		BitSet found = new BitSet(maxColValue);
		int count = 0;
		boolean nullFound = false;

		for (count = 0; rs.next();) {
			int col1Val = rs.getInt(1);
			if (rs.wasNull()) {
				if (nullFound) {
					fail("Too many nulls returned by " + label);
				}
				nullFound = true;
				continue;
			}
			assertFalse("Invalid value returned by " + label,
					col1Val <= -dupKeyCount || col1Val > maxColValue);
			if (col1Val <= 0) {
				dupFoundCount[-col1Val]++;
				if (!expectDups) {
					assertFalse(label + " returned a duplicate.",
							dupFoundCount[-col1Val] > 1);
				} else {
					assertFalse(
							label + " returned too many duplicates.",
							dupFoundCount[-col1Val] > expectedDupCount[-col1Val]);
				}
			} else {
				assertFalse(label + " returned a duplicate.", found
						.get(col1Val));
				found.set(col1Val);
				count++;
			}
			if (holdOverCommit) {
				rs.getStatement().getConnection().commit();
				holdOverCommit = false;
			}
		}
		assertFalse("Incorrect number of rows in " + label,
				count != maxColValue);
		for (int i = 0; i < dupFoundCount.length; i++) {
			assertFalse("A duplicate key row is missing in " + label,
					dupFoundCount[i] != expectedDupCount[i]);
		}
	}
}
