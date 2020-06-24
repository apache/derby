/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.TimestampArithTest
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Test the JDBC TIMESTAMPADD and TIMESTAMPDIFF escape functions.
 *
 * Things to test:
 *   + Test each interval type with timestamp, date, and time inputs.
 *   + Test diff with all 9 combinations of datetime input types (timestamp - timestamp, timestamp - date, etc).
 *   + Test PreparedStatements with parameters, '?', in each argument, and Statements. (Statements are prepared
 *     internally so we do not also have to test PrepardStatements without parameters).
 *   + Test with null inputs.
 *   + Test with input string that is convertible to timestamp.
 *   + Test with invalid interval type.
 *   + Test with invalid arguments in the date time arguments.
 *   + Test TIMESTAMPADD with an invalid type in the count argument.
 *   + Test overflow cases.
 */
public class TimestampArithTest extends BaseJDBCTestCase {
	
	/** Abstract class that factors out all the common code for the timestamps tests. */
	private static abstract class OneTest {
		final int interval; // FRAC_SECOND_INTERVAL, SECOND_INTERVAL, ... or
		final String expectedSQLState; // Null if no SQLException is expected
		final String expectedMsg; // Null if no SQLException is expected
		String sql;

		OneTest(int interval, String expectedSQLState, String expectedMsg) {
			this.interval = interval;
			this.expectedSQLState = expectedSQLState;
			this.expectedMsg = expectedMsg;
		}

		void runTest() throws SQLException {
			ResultSet rs = null;
			
			sql = composeSQL();		
			try {
				rs = stmt.executeQuery(sql);
				checkResultSet(rs, sql);
				if (expectedSQLState != null) {
					fail("Statement '" + sql + "' did not generate an exception");
				}				
			} catch (SQLException sqle) {
				if (expectedSQLState == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4665
                    fail("Unexpected exception from statement '" + sql + "'",
                         sqle);
				}
				assertSQLState("Incorrect SQLState from statement '" + sql + "'", expectedSQLState, sqle);
			} 
			if (rs != null) {
			        rs.close();
				rs = null;
			}

			try {
				rs = executePS();
				checkResultSet(rs, sql);
				if (expectedSQLState != null) {
					fail("Prepared Statement '" + sql + "' did not generate an exception");
				}
			} catch (SQLException sqle) {
				if (expectedSQLState == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4665
                    fail("Unexpected exception from prepared statement '" +
                         sql + "'", sqle);
				}
				assertSQLState("Incorrect SQLState from prepared statement '" + sql + "'", expectedSQLState, sqle);
			} 
			if (rs != null) {
                            rs.close();
                            rs = null;
			}
		} 

		private void checkResultSet(ResultSet rs, String sql) throws SQLException {
			assertTrue("'" + sql + "' did not return any rows.", rs.next());
			checkResultRow(rs, sql);
			assertFalse("'" + sql + "' returned more than one row.", rs.next());
		}

		abstract String composeSQL();
		abstract void checkResultRow(ResultSet rs, String sql) throws SQLException;
		abstract ResultSet executePS() throws SQLException;
	}
	
	private static class OneDiffTest extends OneTest {
		private final java.util.Date ts1;
		private final java.util.Date ts2;
		private final int expectedDiff;
		protected boolean expectNull;
		
		OneDiffTest(int interval, java.util.Date ts1, java.util.Date ts2,
				int expectedDiff, String expectedSQLState, String expectedMsg) {
			super(interval, expectedSQLState, expectedMsg);
			this.ts1 = ts1;
			this.ts2 = ts2;
			this.expectedDiff = expectedDiff;
			expectNull = (ts1 == null) || (ts2 == null);
		}

		void checkResultRow(ResultSet rs, String sql) throws SQLException {
			int actualDiff = rs.getInt(1);
			assertFalse("Unexpected null result from '" + sql + "'.", rs.wasNull() && !expectNull);
			assertFalse("Expected null result from '" + sql + "'.", !rs.wasNull() && expectNull);
			assertEquals("Unexpected result from '" + sql + "'.", expectedDiff, actualDiff);			
		}

		String composeSQL() {
			return composeSqlStr("DIFF", interval, dateTimeToLiteral(ts1),
					dateTimeToLiteral(ts2));
		}

		ResultSet executePS() throws SQLException {
			setDateTime(tsDiffPS[interval], 1, ts1);
			setDateTime(tsDiffPS[interval], 2, ts2);
			return tsDiffPS[interval].executeQuery();
		}		
	}
	
	private static class OneStringDiffTest extends OneDiffTest {
		private final String ts1;
		private final String ts2;
		
		OneStringDiffTest(int interval, String ts1, String ts2,
				int expectedDiff, String expectedSQLState, String expectedMsg) {
			super(interval, (java.util.Date) null, (java.util.Date) null,
					expectedDiff, expectedSQLState, expectedMsg);
			this.ts1 = ts1;
			this.ts2 = ts2;
			expectNull = (ts1 == null) || (ts2 == null);
		}
		
		String composeSQL() {
			return composeSqlStr("DIFF", interval, dateTimeToLiteral(ts1),
					dateTimeToLiteral(ts2));
		}
		
		ResultSet executePS() throws SQLException {
			tsDiffPS[interval].setString(1, ts1);
			tsDiffPS[interval].setString(2, ts2);
			return tsDiffPS[interval].executeQuery();
		}		
	}
	
	private static class OneAddTest extends OneTest {
		private final java.util.Date ts;
		final int count;
		final java.sql.Timestamp expected;
		
		OneAddTest(int interval, int count, java.util.Date ts,
				java.sql.Timestamp expected, String expectedSQLState,
				String expectedMsg) {
			super(interval, expectedSQLState, expectedMsg);
			this.count = count;
			this.ts = ts;
			this.expected = expected;
		}
		
		String composeSQL() {
			return composeSqlStr("ADD", interval, String.valueOf(count),
					dateTimeToLiteral(ts));
		}

		void checkResultRow(ResultSet rs, String sql) throws SQLException {
			java.sql.Timestamp actual = rs.getTimestamp(1);
			assertFalse("Unexpected null result from '" + sql + "'.", (rs.wasNull() || actual == null) && expected != null);
			assertFalse("Expected null result from '" + sql + "'.", !(rs.wasNull() || actual == null) && expected == null);
			assertEquals("Unexpected result from '" + sql + "'.", expected, actual);
		}

		ResultSet executePS() throws SQLException {
			tsAddPS[interval].setInt(1, count);
			setDateTime(tsAddPS[interval], 2, ts);
			return tsAddPS[interval].executeQuery();
		}		
	}
	
	private static class OneStringAddTest extends OneAddTest {
		private final String ts;

		OneStringAddTest(int interval, int count, String ts,
				java.sql.Timestamp expected, String expectedSQLState,
				String expectedMsg) {
			super(interval, count, (java.util.Date) null, expected,
					expectedSQLState, expectedMsg);
			this.ts = ts;
		}

		String composeSQL() {
			return composeSqlStr("ADD", interval, String.valueOf(count),
					dateTimeToLiteral(ts));
		}

		ResultSet executePS() throws SQLException {
			tsAddPS[interval].setInt(1, count);
			tsAddPS[interval].setString(2, ts);
			return tsAddPS[interval].executeQuery();
		}		
	}
	
    private static final int FRAC_SECOND_INTERVAL = 0;
    private static final int SECOND_INTERVAL = 1;
    private static final int MINUTE_INTERVAL = 2;
    private static final int HOUR_INTERVAL = 3;
    private static final int DAY_INTERVAL = 4;
    private static final int WEEK_INTERVAL = 5;
    private static final int MONTH_INTERVAL = 6;
    private static final int QUARTER_INTERVAL = 7;
    private static final int YEAR_INTERVAL = 8;

	/** timestamp - timestamp */
	private static final OneDiffTest[] diffBetweenTsTests = {
			new OneDiffTest(FRAC_SECOND_INTERVAL, ts("2005-05-10 08:25:00"), ts("2005-05-10 08:25:00.000001"), 1000, null, null),
			new OneDiffTest(SECOND_INTERVAL, ts("2005-05-10 08:25:01"), ts("2005-05-10 08:25:00"), -1, null, null),
			new OneDiffTest(SECOND_INTERVAL, ts("2005-05-10 08:25:00.1"), ts("2005-05-10 08:25:00"), 0, null, null),
			new OneDiffTest(SECOND_INTERVAL, ts("2005-05-10 08:25:00"), ts("2005-05-10 08:26:00"), 60, null, null),
			new OneDiffTest(MINUTE_INTERVAL, ts("2005-05-11 08:25:00"), ts("2005-05-10 08:25:00"), -24 * 60, null, null),
			new OneDiffTest(HOUR_INTERVAL, ts("2005-05-10 08:25:00"), ts("2005-05-11 08:25:00"), 24, null, null),
			new OneDiffTest(DAY_INTERVAL, ts("2005-05-10 08:25:00"), ts("2005-05-11 08:25:00"), 1, null, null),
			new OneDiffTest(DAY_INTERVAL, ts("2005-05-10 08:25:01"), ts("2005-05-11 08:25:00"), 0, null, null),
			new OneDiffTest(WEEK_INTERVAL, ts("2005-02-23 08:25:00"), ts("2005-03-01 08:25:00"), 0, null, null),
			new OneDiffTest(MONTH_INTERVAL, ts("2005-02-23 08:25:00"), ts("2005-03-23 08:25:00"), 1, null, null),
			new OneDiffTest(MONTH_INTERVAL, ts("2005-02-23 08:25:01"), ts("2005-03-23 08:25:00"), 0, null, null),
			new OneDiffTest(QUARTER_INTERVAL, ts("2005-02-23 08:25:00"), ts("2005-05-23 08:25:00"), 1, null, null),
			new OneDiffTest(QUARTER_INTERVAL, ts("2005-02-23 08:25:01"), ts("2005-05-23 08:25:00"), 0, null, null),
			new OneDiffTest(YEAR_INTERVAL, ts("2005-02-23 08:25:00"), ts("2005-05-23 08:25:00"), 0, null, null),
			new OneDiffTest(YEAR_INTERVAL, ts("2005-02-23 08:25:00"), ts("2006-02-23 08:25:00"), 1, null, null)			
	};
	
	/** timestamp - date */
	private static final OneDiffTest[] diffBetweenTsAndDateTests = {
			new OneDiffTest(FRAC_SECOND_INTERVAL, ts("2004-05-10 00:00:00.123456"), dt("2004-05-10"), -123456000, null, null),
			new OneDiffTest(SECOND_INTERVAL, ts("2004-05-10 08:25:01"), dt("2004-05-10"), -(1 + 60 * (25 + 60 * 8)), null, null),
			new OneDiffTest(MINUTE_INTERVAL, ts("2004-05-11 08:25:00"), dt("2004-05-10"), -(24 * 60 + 8 * 60 + 25), null, null),
			new OneDiffTest(HOUR_INTERVAL, ts("2004-02-28 08:25:00"), dt("2004-03-01"), 39, null, null),
			new OneDiffTest(DAY_INTERVAL, ts("2004-05-10 08:25:00"), dt("2004-05-11"), 0, null, null),
			new OneDiffTest(WEEK_INTERVAL, ts("2004-02-23 00:00:00"), dt("2004-03-01"), 1, null, null),
			new OneDiffTest(MONTH_INTERVAL, ts("2004-02-23 08:25:00"), dt("2004-03-24"), 1, null, null),
			new OneDiffTest(QUARTER_INTERVAL, ts("2004-02-23 08:25:00"), dt("2004-05-24"), 1, null, null),
			new OneDiffTest(YEAR_INTERVAL, ts("2004-02-23 08:25:00"), dt("2004-05-23"), 0, null, null)
	};
	
	/** date - timestamp */
	private static final OneDiffTest[] diffBetweenDateAndTsTests = {
			new OneDiffTest(FRAC_SECOND_INTERVAL, dt("2004-05-10"), ts("2004-05-10 00:00:00.123456"), 123456000, null, null),
			new OneDiffTest(SECOND_INTERVAL, dt("2004-05-10"), ts("2004-05-09 23:59:00"), -60, null, null),
			new OneDiffTest(MINUTE_INTERVAL, dt("2004-05-10"), ts("2004-05-11 08:25:00"), 24 * 60 + 8 * 60 + 25, null, null),
			new OneDiffTest(HOUR_INTERVAL, dt("2005-03-01"), ts("2005-02-28 08:25:00"), -15, null, null),
			new OneDiffTest(DAY_INTERVAL, dt("2004-05-10"), ts("2004-05-11 08:25:00"), 1, null, null),
			new OneDiffTest(WEEK_INTERVAL, dt("2004-03-01"), ts("2004-02-23 00:00:00"), -1, null, null),
			new OneDiffTest(MONTH_INTERVAL, dt("2005-03-24"), ts("2004-02-23 08:25:00"), -13, null, null),
			new OneDiffTest(QUARTER_INTERVAL, dt("2004-05-23"), ts("2004-02-23 08:25:01"), 0, null, null),
			new OneDiffTest(YEAR_INTERVAL, dt("2004-05-23"), ts("2003-02-23 08:25:00"), -1, null, null)
	};
	
	/** timestamp + timestamp */
	private static final OneAddTest[] addBetweenTsTests = {
			new OneAddTest(FRAC_SECOND_INTERVAL, 1000, ts("2005-05-11 15:55:00"), ts("2005-05-11 15:55:00.000001"), null, null),
			new OneAddTest(SECOND_INTERVAL, 60, ts("2005-05-11 15:55:00"), ts("2005-05-11 15:56:00"), null, null),
			new OneAddTest(MINUTE_INTERVAL, -1, ts("2005-05-11 15:55:00"), ts("2005-05-11 15:54:00"), null, null),
			new OneAddTest(HOUR_INTERVAL, 2, ts("2005-05-11 15:55:00"), ts("2005-05-11 17:55:00"), null, null),
			new OneAddTest(DAY_INTERVAL, 1, ts("2005-05-11 15:55:00"), ts("2005-05-12 15:55:00"), null, null),
			new OneAddTest(WEEK_INTERVAL, 1, ts("2005-05-11 15:55:00"), ts("2005-05-18 15:55:00"), null, null),
			new OneAddTest(MONTH_INTERVAL, 1, ts("2005-05-11 15:55:00"), ts("2005-06-11 15:55:00"), null, null),
			new OneAddTest(QUARTER_INTERVAL, 1, ts("2005-10-11 15:55:00"), ts("2006-01-11 15:55:00"), null, null),
			new OneAddTest(YEAR_INTERVAL, -10, ts("2005-10-11 15:55:00"), ts("1995-10-11 15:55:00"), null, null)
	}; 
	
	/** date + timestamp */
	private static final OneAddTest[] addBetweenDateAndTsTests = {
			// following gives an error with J2ME j9_foundation 1.1 (DERBY-2225):
			new OneAddTest(FRAC_SECOND_INTERVAL, -1000, dt("2005-05-11"), ts("2005-05-10 23:59:59.999999"), null, null),
			new OneAddTest(SECOND_INTERVAL, 60, dt("2005-05-11"), ts("2005-05-11 00:01:00"), null, null),
			new OneAddTest(MINUTE_INTERVAL, 1, dt("2005-05-11"), ts("2005-05-11 00:01:00"), null, null),
			new OneAddTest(HOUR_INTERVAL, -2, dt("2005-05-11"), ts("2005-05-10 22:00:00"), null, null),
			new OneAddTest(DAY_INTERVAL, 1, dt("2005-05-11"), ts("2005-05-12 00:00:00"), null, null),
			new OneAddTest(WEEK_INTERVAL, 1, dt("2005-05-11"), ts("2005-05-18 00:00:00"), null, null),
			new OneAddTest(MONTH_INTERVAL, -1, dt("2005-03-29"), ts("2005-02-28 00:00:00"), null, null),
			new OneAddTest(QUARTER_INTERVAL, -2, dt("2005-05-05"), ts("2004-11-05 00:00:00"), null, null),
			new OneAddTest(YEAR_INTERVAL, 2, dt("2005-05-05"), ts("2007-05-05 00:00:00"), null, null)			
	};
	
	private static final OneStringDiffTest[] diffBetweenStringTests = {
			new OneStringDiffTest(SECOND_INTERVAL, "2005-05-10 08:25:00", "2005-05-10 08:26:00", 60, null, null)
	};
	
	private static final OneStringAddTest[] addBetweenStringTests = {
			new OneStringAddTest(DAY_INTERVAL, 1, "2005-05-11 15:55:00", ts("2005-05-12 15:55:00"), null, null)		
	};
	
	/** check overflow conditions */
	private static final OneTest[] overflowTests = {
			new OneDiffTest(FRAC_SECOND_INTERVAL, ts("2004-05-10 00:00:00.123456"), ts("2004-05-10 00:00:10.123456"), 0, "22003",
					"The resulting value is outside the range for the data type INTEGER."),
			new OneDiffTest(FRAC_SECOND_INTERVAL, ts("2004-05-10 00:00:00.123456"), ts("2005-05-10 00:00:00.123456"), 0, "22003",
					"The resulting value is outside the range for the data type INTEGER."),
			new OneDiffTest(SECOND_INTERVAL, ts("1904-05-10 00:00:00"),	ts("2205-05-10 00:00:00"), 0, "22003",
					"The resulting value is outside the range for the data type INTEGER."),
			new OneAddTest(YEAR_INTERVAL, 99999, ts("2004-05-10 00:00:00.123456"), null, "22003",
					"The resulting value is outside the range for the data type TIMESTAMP.") 			
	};
	
    private static final String[][] invalid = {
        {"values( {fn TIMESTAMPDIFF( SECOND, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)})", "42X01",
         "Syntax error: Encountered \"SECOND\" at line 1, column 28."},
        {"values( {fn TIMESTAMPDIFF( , CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)})", "42X01",
         "Syntax error: Encountered \",\" at line 1, column 28."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 5)})", "42X01",
         "Syntax error: Encountered \",\" at line 1, column 80."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND, CURRENT_TIMESTAMP, 'x')})", "42X45",
         "CHAR is an invalid type for argument number 3 of TIMESTAMPDIFF."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND, 'x', CURRENT_TIMESTAMP)})", "42X45",
         "CHAR is an invalid type for argument number 2 of TIMESTAMPDIFF."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND, CURRENT_TIMESTAMP)})", "42X01",
         "Syntax error: Encountered \")\" at line 1, column 61."},
        {"values( {fn TIMESTAMPDIFF( SQL_TSI_SECOND)})", "42X01",
         "Syntax error: Encountered \")\" at line 1, column 42."},
        {"values( {fn TIMESTAMPADD( x, 1, CURRENT_TIMESTAMP)})", "42X01",
           "Syntax error: Encountered \"x\" at line 1, column 27."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, CURRENT_DATE, CURRENT_TIMESTAMP)})", "42X45",
           "DATE is an invalid type for argument number 2 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 'XX', CURRENT_TIMESTAMP)})", "42X45",
           "CHAR is an invalid type for argument number 2 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 1.1, CURRENT_TIMESTAMP)})", "42X45",
           "DECIMAL is an invalid type for argument number 2 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 1, 2.1)})", "42X45",
           "DECIMAL is an invalid type for argument number 3 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 1, 'XX')})", "42X45",
           "CHAR is an invalid type for argument number 3 of TIMESTAMPADD."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND, 1)})", "42X01",
           "Syntax error: Encountered \")\" at line 1, column 44."},
        {"values( {fn TIMESTAMPADD( SQL_TSI_SECOND)})", "42X01",
           "Syntax error: Encountered \")\" at line 1, column 41."}
    };

	private static final String[] intervalJdbcNames = { "SQL_TSI_FRAC_SECOND",
		"SQL_TSI_SECOND", "SQL_TSI_MINUTE", "SQL_TSI_HOUR", "SQL_TSI_DAY",
		"SQL_TSI_WEEK", "SQL_TSI_MONTH", "SQL_TSI_QUARTER", "SQL_TSI_YEAR" };
    
    private static Statement stmt;
	private static PreparedStatement[] tsAddPS = new PreparedStatement[intervalJdbcNames.length];
	private static PreparedStatement[] tsDiffPS = new PreparedStatement[intervalJdbcNames.length];

	/**
	 * Basic constructor.
	 */
	public TimestampArithTest(String name) {
		super(name);
	}

	protected void initializeConnection(Connection conn) throws SQLException {
		conn.setAutoCommit(false);		
	}
	
	public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        return new BaseJDBCTestSetup(
            new BaseTestSuite(
                TimestampArithTest.class, "TimestampArithTest")) {

			protected void setUp() throws Exception {
				super.setUp();

				for (int i = 0; i < intervalJdbcNames.length; i++) {
					tsAddPS[i] = getConnection().prepareStatement(
							composeSqlStr("ADD", i, "?", "?"));
					tsDiffPS[i] = getConnection().prepareStatement(
							composeSqlStr("DIFF", i, "?", "?"));
				}

				stmt = getConnection().createStatement();
			}

            protected void tearDown() throws Exception {
//IC see: https://issues.apache.org/jira/browse/DERBY-5716
                closeAll(tsAddPS);
                tsAddPS = null;
                closeAll(tsDiffPS);
                tsDiffPS = null;
                stmt.close();
                stmt = null;
                super.tearDown();
            }
		};
	}

    /** Close all statements in an array. */
    private static void closeAll(Statement[] statements) throws SQLException {
        for (int i = 0; i < statements.length; i++) {
            statements[i].close();
        }
    }
	
	public void testDiffBetweenTimestamp() throws SQLException {
		getConnection();
		
		for (int i = 0; i < diffBetweenTsTests.length; i++) {
			diffBetweenTsTests[i].runTest();
		}
	}
	
	public void testDiffBetweenTimestampAndDate() throws SQLException {
		for (int i = 0; i < diffBetweenTsAndDateTests.length; i++) {
			diffBetweenTsAndDateTests[i].runTest();
		}		
	}
	
	public void testDiffBetweenDateAndTimestamp() throws SQLException {
		for (int i = 0; i < diffBetweenDateAndTsTests.length; i++) {
			diffBetweenDateAndTsTests[i].runTest();
		}				
	}
	
	public void testAddBetweenTimestamp() throws SQLException {
		for (int i = 0; i < addBetweenTsTests.length; i++) {
			addBetweenTsTests[i].runTest();
		}						
	}
	
	public void testAddBetweenDateAndTimestamps() throws SQLException {
		for (int i = 0; i < addBetweenDateAndTsTests.length; i++) {
			addBetweenDateAndTsTests[i].runTest();
		}								
	}
	
	public void testDiffBetweenString() throws SQLException {
		for (int i = 0; i < diffBetweenStringTests.length; i++) {
			diffBetweenStringTests[i].runTest();
		}										
	}
	
	public void testAddBetweenString() throws SQLException {
		for (int i = 0; i < addBetweenStringTests.length; i++) {
			addBetweenStringTests[i].runTest();
		}												
	}
	
	public void testOverflow() throws SQLException {
		for (int i = 0; i < overflowTests.length; i++) {
			overflowTests[i].runTest();
		}														
	}

	/**
	 * Tests null inputs, each position, each type.
	 * 
	 * @throws SQLException
	 */
	public void testNullInputs() throws SQLException {		
        tsDiffPS[HOUR_INTERVAL].setTimestamp(1, ts( "2005-05-11 15:26:00"));
        tsDiffPS[HOUR_INTERVAL].setNull(2, Types.TIMESTAMP);

        // TIMESTAMPDIFF with null timestamp in third argument
        expectNullResult(tsDiffPS[HOUR_INTERVAL]);
//IC see: https://issues.apache.org/jira/browse/DERBY-4665

        // TIMESTAMPDIFF with null date in third argument
        tsDiffPS[HOUR_INTERVAL].setNull(2, Types.DATE);
        expectNullResult(tsDiffPS[HOUR_INTERVAL]);

        // TIMESTAMPDIFF with null timestamp in second argument
        tsDiffPS[HOUR_INTERVAL].setTimestamp(2, ts( "2005-05-11 15:26:00"));
        tsDiffPS[HOUR_INTERVAL].setNull(1, Types.TIMESTAMP);
        expectNullResult(tsDiffPS[HOUR_INTERVAL]);

        // TIMESTAMPDIFF with null date in second argument
        tsDiffPS[HOUR_INTERVAL].setNull(1, Types.DATE);
        expectNullResult(tsDiffPS[HOUR_INTERVAL]);

        // TIMESTAMPADD with null integer in second argument
        tsAddPS[MINUTE_INTERVAL].setTimestamp(2, ts( "2005-05-11 15:26:00"));
        tsAddPS[MINUTE_INTERVAL].setNull(1, Types.INTEGER);
        expectNullResult(tsAddPS[MINUTE_INTERVAL]);

        // TIMESTAMPADD with null timestamp in third argument
        tsAddPS[MINUTE_INTERVAL].setInt(1, 1);
        tsAddPS[MINUTE_INTERVAL].setNull(2, Types.TIMESTAMP);
        expectNullResult(tsAddPS[MINUTE_INTERVAL]);

        // TIMESTAMPADD with null date in third argument
        tsAddPS[MINUTE_INTERVAL].setNull(2, Types.DATE);
        expectNullResult(tsAddPS[MINUTE_INTERVAL]);
	}

	public void testInvalidLengths() throws SQLException {
		ResultSet rs;
		
		for (int i = 0; i < invalid.length; i++) {			
			try {
				rs = stmt.executeQuery(invalid[i][0]);
				rs.next();
				fail(invalid[i][0] + " did not throw an exception.");
			} catch (SQLException sqle) {
				assertSQLState("Unexpected SQLState from " + invalid[i][0], invalid[i][1], sqle);
			}
		}
	}
	
	public void testInvalidArgTypes() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        expectException( tsDiffPS[ HOUR_INTERVAL], ts( "2005-05-21 15:26:00"), 2.0, "XCL12",
                "TIMESTAMPDIFF with double ts2");
        expectException( tsDiffPS[ HOUR_INTERVAL], 2.0, ts( "2005-05-11 15:26:00"), "XCL12",
                "TIMESTAMPDIFF with double ts1");
        expectException( tsAddPS[ MINUTE_INTERVAL], 1, -1, "XCL12",
                "TIMESTAMPADD with int ts");
        expectException( tsAddPS[ MINUTE_INTERVAL], ts( "2005-05-11 15:26:00"), ts( "2005-05-11 15:26:00"), "XCL12",
                "TIMESTAMPADD with timestamp count");		
	}
	
	private static void expectException(PreparedStatement ps, Object obj1, Object obj2, 
			String expectedSQLState, String label) {
		 ResultSet rs;
		
        try {
        	ps.setObject(1, obj1);
			ps.setObject(2, obj2);
			rs = ps.executeQuery();
			rs.next();
			fail(label + " did not throw an exception.");			
		} catch (SQLException sqle) {
			assertSQLState("Unexpected SQLState from " + label, expectedSQLState, sqle);
		}
	}

    private static void expectNullResult(PreparedStatement ps)
//IC see: https://issues.apache.org/jira/browse/DERBY-4665
            throws SQLException {
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
    }

	private static String dateTimeToLiteral(Object ts) {
		if (ts instanceof java.sql.Timestamp)
			return "{ts '" + ((java.sql.Timestamp) ts).toString() + "'}";
		else if (ts instanceof java.sql.Time)
			return "{t '" + ((java.sql.Time) ts).toString() + "'}";
		else if (ts instanceof java.sql.Date)
			return "{d '" + ((java.sql.Date) ts).toString() + "'}";
		else if (ts instanceof String)
			return "TIMESTAMP( '" + ((String) ts) + "')";
		else
			return ts.toString();
	}
	
	private static String composeSqlStr(String fn, int interval, String parm1, String parm2) {
		return "values( {fn TIMESTAMP" + fn + "( "
				+ intervalJdbcNames[interval] + ", " + parm1 + "," + parm2
				+ ")})";
	}
	
	private static void setDateTime(PreparedStatement ps, int parameterIdx,
			java.util.Date dateTime) throws SQLException {
		if (dateTime instanceof java.sql.Timestamp)
			ps.setTimestamp(parameterIdx, (java.sql.Timestamp) dateTime);
		else if (dateTime instanceof java.sql.Date)
			ps.setDate(parameterIdx, (java.sql.Date) dateTime);
		else if (dateTime instanceof java.sql.Time)
			ps.setTime(parameterIdx, (java.sql.Time) dateTime);
		else
			ps.setTimestamp(parameterIdx, (java.sql.Timestamp) dateTime);
	}
	
	private static java.sql.Timestamp ts(String s) {
		// Timestamp format must be yyyy-mm-dd hh:mm:ss.fffffffff
		if (s.length() < 29) {
			// Pad out the fraction with zeros
			StringBuffer sb = new StringBuffer(s);
			if (s.length() == 19)
				sb.append('.');
			while (sb.length() < 29)
				sb.append('0');
			s = sb.toString();
		}
//IC see: https://issues.apache.org/jira/browse/DERBY-4665
        return java.sql.Timestamp.valueOf(s);
	}
	
	private static java.sql.Date dt(String s) {
		return java.sql.Date.valueOf(s);
	}
}
