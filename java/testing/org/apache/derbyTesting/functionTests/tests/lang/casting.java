/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.casting

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.lang;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derby.tools.JDBCDisplayUtil;

import java.sql.*;
import java.math.*;
import java.io.*;


public class casting {

	public static String VALID_DATE_STRING = "'2000-01-01'";
	public static String VALID_TIME_STRING = "'15:30:20'";
	public static String VALID_TIMESTAMP_STRING = "'2000-01-01 15:30:20'";
	public static String NULL_VALUE="NULL";

	public static String ILLEGAL_CAST_EXCEPTION_SQLSTATE = "42846";
	public static String LANG_NOT_STORABLE_SQLSTATE  = "42821";
	public static String LANG_NOT_COMPARABLE_SQLSTATE = "42818";
	public static String METHOD_NOT_FOUND_SQLSTATE = "42884";
	public static String LANG_FORMAT_EXCEPTION_SQLSTATE = "22018";

	public static int SQLTYPE_ARRAY_SIZE = 17 ;
	public static int SMALLINT_OFFSET = 0;
	public static int INTEGER_OFFSET = 1;
	public static int BIGINT_OFFSET = 2;
	public static int DECIMAL_OFFSET = 3;
	public static int REAL_OFFSET = 4;
	public static int DOUBLE_OFFSET = 5;
	public static int CHAR_OFFSET = 6;
	public static int VARCHAR_OFFSET = 7;
	public static int LONGVARCHAR_OFFSET = 8;
	public static int CHAR_FOR_BIT_OFFSET = 9;
	public static int VARCHAR_FOR_BIT_OFFSET = 10;
	public static int LONGVARCHAR_FOR_BIT_OFFSET = 11;
	public static int CLOB_OFFSET = 12;
	public static int DATE_OFFSET = 13;
	public static int TIME_OFFSET = 14;
	public static int TIMESTAMP_OFFSET = 15;
	public static int BLOB_OFFSET = 16;


	public static String[] SQLTypes =
	{
		"SMALLINT",
		"INTEGER",
		"BIGINT",
		"DECIMAL(10,5)",
		"REAL",
		"DOUBLE",
		"CHAR(60)",
		"VARCHAR(60)",
		"LONG VARCHAR",
		"CHAR(60) FOR BIT DATA",
		"VARCHAR(60) FOR BIT DATA",
		"LONG VARCHAR FOR BIT DATA",
		"CLOB(1k)",
		"DATE",
		"TIME",
		"TIMESTAMP",
		"BLOB(1k)",
	};


	public static int NULL_DATA_OFFSET = 0;  // offset of NULL value
	public static int VALID_DATA_OFFSET = 1;  // offset of NULL value

	// rows are data types.
	// data is NULL_VALUE, VALID_VALUE
	// Should add Minimum, Maximum and out of range.
 public static String[][]SQLData =
	{
		{NULL_VALUE, "0"},       // SMALLINT
		{NULL_VALUE,"11"},       // INTEGER
		{NULL_VALUE,"22"},       // BIGINT
		{NULL_VALUE,"3.3"},      // DECIMAL(10,5)
		{NULL_VALUE,"4.4"},      // REAL,
		{NULL_VALUE,"5.5"},      // DOUBLE
		{NULL_VALUE,"'7'"},      // CHAR(60)
		{NULL_VALUE,"'8'"},      //VARCHAR(60)",
		{NULL_VALUE,"'9'"},      // LONG VARCHAR
		{NULL_VALUE,"X'10aa'"},  // CHAR(60)  FOR BIT DATA
		{NULL_VALUE,"X'10bb'"},  // VARCHAR(60) FOR BIT DATA
		{NULL_VALUE,"X'10cc'"},  //LONG VARCHAR FOR BIT DATA
		{NULL_VALUE,"'13'"},     //CLOB(1k)
		{NULL_VALUE,VALID_DATE_STRING},        // DATE
		{NULL_VALUE,VALID_TIME_STRING},        // TIME
		{NULL_VALUE,VALID_TIMESTAMP_STRING},   // TIMESTAMP
		{NULL_VALUE,"X'01dd'"}                 // BLOB
	};
 



	public static final boolean _ = false;
	public static final boolean X = true;

	/**
	   DB2 Table 146 - Supported explicit casts between Built-in DataTypes

	   This table has THE FOR BIT DATA TYPES broken out into separate columns
	   for clarity and testing
	**/


	public static final boolean[][]  T_146 = {
		
    // Types.             S  I  B  D  R  D  C  V  L  C  V  L  C  D  T  T  B
	//                    M  N  I  E  E  O  H  A  O  H  A  O  L  A  I  I  L
	//                    A  T  G  C  A  U  A  R  N  A  R  N  O  T  M  M  O
    //                    L  E  I  I  L  B  R  C  G  R  C  G  B  E  E  E  B
    //                    L  G  N  M     L     H  V  .  H  V           S
    //                    I  E  T  A     E     A  A  B  .  A           T
    //                    N  R     L           R  R  I  B  R           A
	//                    T                       C  T  I  .           M
    //                                            H     T  B           P
	//                                            A        I
	//                                            R        T
/* 0 SMALLINT */        { X, X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _ },
/* 1 INTEGER  */        { X, X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _ },
/* 2 BIGINT   */        { X, X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _ },
/* 3 DECIMAL  */        { X, X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _ },
/* 4 REAL     */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 5 DOUBLE   */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 6 CHAR     */        { X, X, X, X, _, _, X, X, X, _, _, _, X, X, X, X, _ },
/* 7 VARCHAR  */        { X, X, X, X, _, _, X, X, X, _, _, _, X, X, X, X, _ },
/* 8 LONGVARCHAR */     { _, _, _, _, _, _, X, X, X, _, _, _, X, _, _, _, _ },
/* 9 CHAR FOR BIT */    { _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X },
/* 10 VARCH. BIT   */   { _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X },
/* 11 LONGVAR. BIT */   { _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, X },
/* 12 CLOB         */   { _, _, _, _, _, _, X, X, X, _, _, _, X, _, _, _, _ },
/* 13 DATE         */   { _, _, _, _, _, _, X, X, _, _, _, _, _, X, _, _, _ },
/* 14 TIME         */   { _, _, _, _, _, _, X, X, _, _, _, _, _, _, X, _, _ },
/* 15 TIMESTAMP    */   { _, _, _, _, _, _, X, X, _, _, _, _, _, X, X, X, _ },
/* 16 BLOB         */   { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X },

	};

	/**
	 * DB2 uses Table 147 to describe  Data Type Compatibility for Assignments 
	 *
	 * The table 147a covers the assignments as they do differ somewhat 
	 *  from comparisons which can be found in 147b
	   
	 * This table has DATA TYPES for operands rather than lumping types
	 * together. Here is the mapping from DB2 Table 147 to this table.
	 * Binary Integer = SMALLINT, INTEGER, BIGINT
	 * Decimal Number = DECIMAL/(NUMERIC)
	 * Floating Point = REAL, DOUBLE/(FLOAT)
	 * Character String = CHAR, VARCHAR, LONGVARCHAR
	 * Binary String = CHAR FOR BIT DATA, VARCHAR FOR BIT DATA, LONG VARCHAR FOR
	 * BIT DATA
	 * Graphic String = not suppported
	 * Date = DATE
	 * Time = TIME
	 * TimeStamp = TIMESTAMP
	 * Binary String = literal hexadecimal, CHAR FOR BIT DATA, VARCHAR FOR BIT
	 *                 DATA, LONG VARCHAR FOR BIT
	 * CLOB and BLOB are not covered in Table 147 but are included here 
	 * for clarity and testing
	**/

	public static final boolean[][]  T_147a = {
		
    // Types.             S  I  B  D  R  D  C  V  L  C  V  L  C  D  T  T  B
	//                    M  N  I  E  E  O  H  A  O  H  A  O  L  A  I  I  L
	//                    A  T  G  C  A  U  A  R  N  A  R  N  O  T  M  M  O
    //                    L  E  I  I  L  B  R  C  G  R  C  G  B  E  E  E  B
    //                    L  G  N  M     L     H  V  .  H  V           S
    //                    I  E  T  A     E     A  A  B  .  A           T
    //                    N  R     L           R  R  I  B  R           A
	//                    T                       C  T  I  .           M
    //                                            H     T  B           P
	//                                            A        I
	//                                            R        T
/* 0 SMALLINT */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 1 INTEGER  */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 2 BIGINT   */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 3 DECIMAL  */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 4 REAL     */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 5 DOUBLE   */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 6 CHAR     */        { _, _, _, _, _, _, X, X, X, _, _, _, X, X, X, X, _ },
/* 7 VARCHAR  */        { _, _, _, _, _, _, X, X, X, _, _, _, X, X, X, X, _ },
/* 8 LONGVARCHAR */     { _, _, _, _, _, _, X, X, X, _, _, _, X, _, _, _, _ },
/* 9 CHAR FOR BIT */    { _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _ },
/* 10 VARCH. BIT   */   { _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _ },
/* 11 LONGVAR. BIT */   { _, _, _, _, _, _, _, _, _, X, X, X, _, _, _, _, _ },
/* 12 CLOB         */   { _, _, _, _, _, _, X, X, X, _, _, _, X, _, _, _, _ },
/* 13 DATE         */   { _, _, _, _, _, _, X, X, _, _, _, _, _, X, _, _, _ },
/* 14 TIME         */   { _, _, _, _, _, _, X, X, _, _, _, _, _, _, X, _, _ },
/* 15 TIMESTAMP    */   { _, _, _, _, _, _, X, X, _, _, _, _, _, _, _, X, _ },
/* 16 BLOB         */   { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, X },

	};


	// Comparisons table
	// Comparison's are different than assignments because
	// Long types cannot be compared.
	public static final boolean[][]  T_147b = {
		
    // Types.             S  I  B  D  R  D  C  V  L  C  V  L  C  D  T  T  B
	//                    M  N  I  E  E  O  H  A  O  H  A  O  L  A  I  I  L
	//                    A  T  G  C  A  U  A  R  N  A  R  N  O  T  M  M  O
    //                    L  E  I  I  L  B  R  C  G  R  C  G  B  E  E  E  B
    //                    L  G  N  M     L     H  V  .  H  V           S
    //                    I  E  T  A     E     A  A  B  .  A           T
    //                    N  R     L           R  R  I  B  R           A
	//                    T                       C  T  I  .           M
    //                                            H     T  B           P
	//                                            A        I
	//                                            R        T
/* 0 SMALLINT */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 1 INTEGER  */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 2 BIGINT   */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 3 DECIMAL  */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 4 REAL     */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 5 DOUBLE   */        { X, X, X, X, X, X, _, _, _, _, _, _, _, _, _, _, _ },
/* 6 CHAR     */        { _, _, _, _, _, _, X, X, _, _, _, _, _, X, X, X, _ },
/* 7 VARCHAR  */        { _, _, _, _, _, _, X, X, _, _, _, _, _, X, X, X, _ },
/* 8 LONGVARCHAR */     { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ },
/* 9 CHAR FOR BIT */    { _, _, _, _, _, _, _, _, _, X, X, _, _, _, _, _, _ },
/* 10 VARCH. BIT   */   { _, _, _, _, _, _, _, _, _, X, X, _, _, _, _, _, _ },
/* 11 LONGVAR. BIT */   { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ },
/* 12 CLOB         */   { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ },
/* 13 DATE         */   { _, _, _, _, _, _, X, X, _, _, _, _, _, X, _, _, _ },
/* 14 TIME         */   { _, _, _, _, _, _, X, X, _, _, _, _, _, _, X, _, _ },
/* 15 TIMESTAMP    */   { _, _, _, _, _, _, X, X, _, _, _, _, _, _, _, X, _ },
/* 16 BLOB         */   { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ },

	};

	

	public static boolean isDerbyNet;
	public static boolean isDB2;
	//testNum just print and increments with each query display
	public static int testNum = 1;
 
	public static void main(String[] args) throws Exception {
		String framework = System.getProperty("framework");
		if (framework != null && framework.toUpperCase().equals("DERBYNET"))
			isDerbyNet = true;

		if (framework != null && framework.toUpperCase().equals("DB2JCC"))
			isDB2 = true;

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			 ij.getPropertyArg(args);
			 Connection conn = ij.startJBMS();
			 conn.setAutoCommit(false);
			 createTables(conn);
			 testAssignments(conn);
			 testExplicitCasts(conn);
			 testComparisons(conn);
			 conn.close();
		}
		catch (SQLException sqle) {
			unexpectedException(sqle);
		}
		catch (Throwable t) {
			t.printStackTrace(System.out);
		}
	}

	public static void testExplicitCasts(Connection conn)
		throws SQLException, Throwable
	{
		
		System.out.println("**testExplicitCasts starting");
		
		ResultSet rs = null;
		
		// Try Casts from each type to the 
		for (int sourceType = 0; sourceType < SQLTypes.length; sourceType++) {
			
			String sourceTypeName = SQLTypes[sourceType];
			for (int dataOffset = 0; dataOffset < SQLData[0].length; dataOffset++)
				for (int targetType =0; targetType < SQLTypes.length; targetType++)
				{
				try {
					String convertString = null;
					String targetTypeName = SQLTypes[targetType];
								// For casts from Character types use strings that can
								// be converted to the targetType.
					
					convertString = getCompatibleString(sourceType,
														targetType,dataOffset);


					String query = "VALUES CAST (CAST (" + convertString +
						" AS " + SQLTypes[sourceType] + ") AS " +
						SQLTypes[targetType] + " )";
					executeQueryAndDisplay(conn,query);
					checkSupportedCast(sourceType,targetType);
				} catch (SQLException se)
				{
					String sqlState = se.getSQLState();
					if (!isSupportedCast(sourceType,targetType))
					{
						if(isCastException(se))
							System.out.println("EXPECTED CASTING EXCEPTION: " +
											   se.getMessage());
						else
							gotWrongException(se);
					}
					else 
						unexpectedException(se);
				}
			}
		}

		conn.commit();			 			
			
	}

	public static void createTables(Connection conn)
		throws SQLException, Throwable
	
	{
		System.out.println("**createTables  starting");

		Statement scb = conn.createStatement();

		for (int type = 0; type < SQLTypes.length; type++) 
		{
			String typeName = SQLTypes[type];
			String tableName = getTableName(type);

			try {
				scb.executeUpdate("DROP TABLE " + tableName);
			}
			catch(SQLException se)
			{// ignore drop error
			}
			String createSQL = "create table "+ 
				tableName + " (c " +
				typeName+ " )";
			System.out.println(createSQL);
			scb.executeUpdate(createSQL);
		}

		scb.close();
		conn.commit();
	}



	public static void testAssignments(Connection conn)
		throws SQLException, Throwable
	{
		
		System.out.println("**testAssignments starting");
		Statement scb = conn.createStatement();		
		ResultSet rs = null;
		

		// Insert's using literals
		System.out.println("* testing literal inserts");
		
		for (int dataOffset = 0; dataOffset < SQLData[0].length; dataOffset++)
			for (int type = 0; type < SQLTypes.length; type++) {
			try {
				String tableName = getTableName(type);
				
				String insertSQL = "insert into " + tableName + " values( " +
					SQLData[type][dataOffset] + ")";
				System.out.println(insertSQL);
				scb.executeUpdate(insertSQL);
			}
			catch (SQLException se)
			{
				// literal inserts are ok for everything but BLOB
			if (type == BLOB_OFFSET)
				System.out.println("EXPECTED EXCEPTION inserting literal into BLOB . " + se.getMessage());
			else 
				gotWrongException(se);
			}
		}
		
		// Try to insert each sourceType into the targetType table
		for (int dataOffset = 0; dataOffset < SQLData[0].length; dataOffset++)
			for (int sourceType = 0; sourceType < SQLTypes.length; sourceType++) {
				String sourceTypeName = SQLTypes[sourceType];
				for (int targetType =0; targetType < SQLTypes.length; targetType++)
				{
				try {
					String convertString = null;
					String targetTableName = getTableName(targetType);

					// For assignments  Character types use strings that can
					// be converted to the targetType.
					convertString = getCompatibleString(sourceType, targetType,dataOffset);
					
					String insertValuesString = " VALUES CAST(" +
					convertString + " AS " + sourceTypeName + ")";
					
					
					String insertSQL = "INSERT INTO " + targetTableName + 
						insertValuesString;
					System.out.println(insertSQL);
					scb.executeUpdate(insertSQL);
					checkSupportedAssignment(sourceType, targetType);

				} catch (SQLException se)
				{
					String sqlState = se.getSQLState();
					if (!isSupportedAssignment(sourceType,targetType) &&
						isNotStorableException(se) ||
						isCastException(se))
						System.out.println("EXPECTED EXCEPTION: " + 
										   sqlState + ":" + se.getMessage());
					else 
						gotWrongException(se);
				}
				}
			}
		scb.close();
		conn.commit();			 			
			
	}



	public static void testComparisons(Connection conn)
		throws SQLException, Throwable
	{
		
		System.out.println("**testComparisons starting");
		Statement scb = conn.createStatement();		
		ResultSet rs = null;
		

		// Comparison's  using literals
		System.out.println("* testing literal comparisons");


		for (int type = 0; type < SQLTypes.length; type++) {
			try {
				int dataOffset = 1; // don't use null values
				String tableName = getTableName(type);
				
				String compareSQL = "SELECT distinct c FROM " + tableName + 
					" WHERE c = " + SQLData[type][dataOffset];
				System.out.println(compareSQL);
				rs = scb.executeQuery(compareSQL);
				JDBCDisplayUtil.DisplayResults(System.out,rs,conn);
			}
			catch (SQLException se)
			{
				// literal comparisons are ok for everything but BLOB
				if (isLongType(type))
					System.out.println("EXPECTED EXCEPTION comparing long type. " + se.getMessage());
				else 
					gotWrongException(se);
			}
		}
		
		
		// Try to compare  each sourceType with the targetType
		for (int dataOffset = 0; dataOffset < SQLData[0].length; dataOffset++)
			for (int sourceType = 0; sourceType < SQLTypes.length; sourceType++) {
				String sourceTypeName = SQLTypes[sourceType];
				for (int targetType =0; targetType < SQLTypes.length; targetType++)
				{
				try {
					String convertString = null;
					String targetTableName = getTableName(targetType);
					
					

					// For assignments  Character types use strings that can
					// be converted to the targetType.
					convertString = getCompatibleString(sourceType,
														targetType,dataOffset);
					
					// Make sure table has just compatible data
					scb.executeUpdate("DELETE FROM " + targetTableName);
					String insertValuesString = " VALUES CAST(" +
						convertString + " AS " + sourceTypeName + ")";
					
					String insertSQL = "INSERT INTO " + targetTableName + 
						insertValuesString;
					
					String compareSQL = "select c from " + 
						targetTableName + " WHERE c = CAST(" + convertString
						+ " AS " + sourceTypeName + ")";	

					System.out.println(compareSQL);
					rs = scb.executeQuery(compareSQL);
					JDBCDisplayUtil.DisplayResults(System.out,rs,conn);
					checkSupportedComparison(sourceType, targetType);

				} catch (SQLException se)
				{
					String sqlState = se.getSQLState();
					if (!isSupportedComparison(sourceType,targetType) &&
						isNotComparableException(se) ||
						isCastException(se))
						System.out.println("EXPECTED EXCEPTION: " + 
										   sqlState + ":" + se.getMessage());
					else 
						gotWrongException(se);
				}
				}
			}
		scb.close();
		conn.commit();			 			
			
	}


	public static boolean isSupportedCast(int sourceType, int targetType)
	{
		return T_146[sourceType][targetType];
	}

	public static boolean isSupportedAssignment(int sourceType, int targetType)
	{
		return T_147a[sourceType][targetType];
	}

	public static boolean isSupportedComparison(int sourceType, int targetType)
	{
		return T_147b[sourceType][targetType];
	}


	public static boolean isCastException (SQLException se)
	{
		return sqlStateMatches(se,ILLEGAL_CAST_EXCEPTION_SQLSTATE);
	}

	public static boolean isMethodNotFoundException (SQLException se)
	{
		return sqlStateMatches(se, METHOD_NOT_FOUND_SQLSTATE);
	}

	public static boolean sqlStateMatches(SQLException se, 
										   String expectedValue)
	{
		String sqlState = se.getSQLState();
		if ((sqlState != null) &&
			(sqlState.equals(expectedValue)))
			return true;
		return false;
	}

	public static boolean isNotStorableException(SQLException se)
	{
		String sqlState = se.getSQLState();
		if ((sqlState != null) &&
			(sqlState.equals(LANG_NOT_STORABLE_SQLSTATE)))
			return true;
		return false;

	}

	public static boolean isNotComparableException(SQLException se)
	{
		String sqlState = se.getSQLState();
		if ((sqlState != null) &&
			(sqlState.equals(LANG_NOT_COMPARABLE_SQLSTATE)))
			return true;
		return false;
	}

	public static void unexpectedException(SQLException sqle) {

		String sqlState = sqle.getSQLState();
		
		if (isDB2  && (sqlState != null) &&
			sqlState.equals("22003"))
		{
			System.out.print("WARNING: DB2 overflow exception -");
		}
		else
			System.out.print("FAIL unexpected exception - ");
		
		showException(sqle);
		sqle.printStackTrace(System.out);
	}


	/**
	 * We got an exception when one was expected, but it was the
	 * wrong one.  For DB2 we will just print a warning.
	 * @param sqle
	 */
	public static void gotWrongException(SQLException sqle) {
		if (isDB2)
		{
			System.out.print("WARNING: DB2 exception different from Derby-" );
			showException(sqle);
		}
		else unexpectedException(sqle);
	}

	/**
	 * Show an expected exception
	 * @param sqle SQL Exception
	 */
	public static void expectedException(SQLException sqle) {
			System.out.print("EXPECTED EXCEPTION:" );
			showException(sqle);
			System.out.println("\n");
	}


	public static void showException(SQLException sqle) {
		do {
			String state = sqle.getSQLState();
			if (state == null)
				state = "?????";

			String msg = sqle.getMessage();
			if  (msg == null)
				msg = "?? no message ??";

			System.out.print(" (" + state + "):" + msg);
			sqle = sqle.getNextException();
		} while (sqle != null);
	}


	/**
	 * Display Query , execute and display results.
	 * @param conn  Connection to use
	 * @param query to execute
	 */	

	public static void executeQueryAndDisplay(Connection conn,
													String query)
													throws SQLException 
	{
		Statement stmt = conn.createStatement();
		ResultSet rs;		

		System.out.println("Test #" + testNum++);
		System.out.println(query );
		rs = stmt.executeQuery(query);
		JDBCDisplayUtil.DisplayResults(System.out,rs,conn);

		stmt.close();
	}


	public static boolean isLongType( int typeOffset)
	{
		return ((typeOffset == LONGVARCHAR_OFFSET) ||
				(typeOffset == LONGVARCHAR_FOR_BIT_OFFSET) || 
				(typeOffset == CLOB_OFFSET) ||
				(typeOffset == BLOB_OFFSET));
	}

	public static boolean isCharacterType(int typeOffset)
	{
		return ((typeOffset == CHAR_OFFSET) || 
				(typeOffset == VARCHAR_OFFSET) || 
				(typeOffset == LONGVARCHAR_OFFSET) || 
				(typeOffset == CLOB_OFFSET));
	}

	public static boolean isBinaryType(int typeOffset)
	{
		return ((typeOffset == CHAR_FOR_BIT_OFFSET) || 
				(typeOffset == VARCHAR_FOR_BIT_OFFSET) || 
				(typeOffset == LONGVARCHAR_FOR_BIT_OFFSET) || 
				(typeOffset == BLOB_OFFSET));
	}

	public static boolean isDateTimeTimestamp(int typeOffset)
	{
		return ( (typeOffset == DATE_OFFSET) ||
				 (typeOffset == TIME_OFFSET) ||
				 (typeOffset == TIMESTAMP_OFFSET));

	}

	public static boolean isClob(int typeOffset)
	{
		return (typeOffset == CLOB_OFFSET);
	}
		
	public static boolean isLob(int typeOffset)
	{
		return ((typeOffset == CLOB_OFFSET) || 
				(typeOffset == BLOB_OFFSET));

	}

	public static String getCompatibleString(int sourceType, int targetType,
											  int dataOffset)
	{
		String convertString = null;
		if ((isCharacterType(sourceType) || isBinaryType(sourceType)) &&
			!isLob(sourceType))
			convertString = formatString(SQLData[targetType][dataOffset]);
		else
			convertString = SQLData[sourceType][dataOffset];

		return convertString;
	}
	
	// Data is already a  string (starts with X, or a character string,
	// just return, otherwise bracket with ''s
	public static String formatString(String str)
	{
		if ((str != null) && (
							  str.startsWith("X") ||
							  str.startsWith("'") ||
							  (str == NULL_VALUE)))
			return str;
		else
			return "'" + str + "'";

	}

	public static boolean setValidValue(PreparedStatement ps, int param, int jdbcType)
		throws SQLException {

		switch (jdbcType) {
		case Types.SMALLINT:
			ps.setShort(param, (short) 32);
			return true;
		case Types.INTEGER:
			ps.setInt(param, 32);
			return true;
		case Types.BIGINT:
			ps.setLong(param, 32L);
			return true;
		case Types.REAL:
			ps.setFloat(param, 32.0f);
			return true;
		case Types.FLOAT:
		case Types.DOUBLE:
			ps.setDouble(param, 32.0);
			return true;
		case Types.DECIMAL:
			ps.setBigDecimal(param, new BigDecimal(32.0));
			return true;
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			ps.setString(param, "32");
			return true;
		case Types.BINARY:
		case Types.VARBINARY:
			{
				byte[] data = {(byte) 0x04, (byte) 0x03, (byte) 0xfd, (byte) 0xc3, (byte) 0x73};
				ps.setBytes(param, data);
				return true;
			}
		//Types.LONGVARBINARY:
		case Types.DATE:
			ps.setDate(param, java.sql.Date.valueOf("2004-02-14"));
			return true;
		case Types.TIME:
			ps.setTime(param, java.sql.Time.valueOf("13:26:42"));
			return true;
		case Types.TIMESTAMP:
			ps.setTimestamp(param, java.sql.Timestamp.valueOf("2004-02-23 17:14:24.097625551"));
			return true;
		case Types.CLOB:
			// JDBC 3.0 spec section 16.3.2 explictly states setCharacterStream is OK for setting a CLOB
			ps.setCharacterStream(param, new java.io.StringReader("67"), 2);
			return true;
		case Types.BLOB:
			// JDBC 3.0 spec section 16.3.2 explictly states setBinaryStream is OK for setting a BLOB
			{
				byte[] data = new byte[6];
				data[0] = (byte) 0x82;
				data[1] = (byte) 0x43;
				data[2] = (byte) 0xca;
				data[3] = (byte) 0xfe;
				data[4] = (byte) 0x00;
				data[5] = (byte) 0x32;

			ps.setBinaryStream(param, new java.io.ByteArrayInputStream(data), 6);
			return true;
			}
		default:
			return false;
		}
	}

	/**
	 * Truncates (*) from typename
	 * @param type - Type offset
	 *
	 * @returns  short name of type (e.g DECIMAL instead of DECIMAL(10,5)
	 */

	public static  String getShortTypeName(int type)
	{
		String typeName = SQLTypes[type];
		String shortName = typeName;
		int parenIndex = typeName.indexOf('(');
		if (parenIndex >= 0)
		{
			shortName = typeName.substring(0,parenIndex);
			int endParenIndex =  typeName.indexOf(')');
			shortName = shortName + typeName.substring(endParenIndex+1,typeName.length());
		}
		return shortName;

	}


	/**
	 * Build a unique table name from the type
	 * @param -  table offset
	 * @returns  Table name in format <TYPE>_TAB. Replaces ' ' _;
	 */
	public static  String getTableName(int type)
	{
		return getShortTypeName(type).replace(' ', '_') + "_TAB";
			
	}

	public static void checkSupportedCast(int sourceType, int targetType)
	{
		String description = " Cast from " +
			SQLTypes[sourceType] +
			" to " + SQLTypes[targetType];
		
		if(!isSupportedCast(sourceType,targetType))
			printShouldNotSucceedMessage(description);
	}

	public static void printShouldNotSucceedMessage(String description)
	{
		if (isDB2)
		{
			System.out.println("WARNING:" + description + " which is not supported in Derby works in DB2");
		}
		else
			System.out.println("FAIL:" +  description +
							   " should not be supported");
		
	}

	public static void checkSupportedAssignment(int sourceType, int targetType)
	{
		String description = " Assignment from " +
			SQLTypes[sourceType] +
			" to " + SQLTypes[targetType];
		
		if (!isSupportedAssignment(sourceType,targetType))
			printShouldNotSucceedMessage(description);


	}

	public static void checkSupportedComparison(int sourceType,int targetType)
	{
		String description = " Comparison of " +
			SQLTypes[sourceType] +
			" to " + SQLTypes[targetType];
		
		if (!isSupportedComparison(sourceType,targetType))
			printShouldNotSucceedMessage(description);
	}

  
	//	-- HTML Table generation

	public static void printHTMLTables()
	{
		// For headers.  First four letters of each word
		String [] shortTypes = new String[SQLTypes.length];

		for (int i = 0; i < SQLTypes.length; i++)
			shortTypes[i] = getShortTypeName(i);

		TestUtil.startHTMLPage("Datatype Casting, Assignment, and Comparison", 
					  "person@a.company.com");
		
		TestUtil.printBoolArrayHTMLTable("Source Types","Target Types",
										 shortTypes,
										 shortTypes,
										 T_146,
										 "Table 146 - Explicit Casts Allowed by Derby");
		
		TestUtil.printBoolArrayHTMLTable("Source Types","Target Types",
										 shortTypes,
										 shortTypes,
										 T_147a,
										 "Table 147a - Assignments Allowed by Derby");

		TestUtil.printBoolArrayHTMLTable("Source Types","Target Types",
										 shortTypes,
										 shortTypes,
										 T_147b,
										 "Table 147b - Comparisons Allowed by Derby");

	   
		TestUtil.endHTMLPage();
	}


}

