/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.odbc_metadata

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Timestamp;
import java.sql.Time;
import java.sql.Date;
import java.math.BigDecimal;

import java.util.Properties;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 * Test of database metadata for ODBC clients.  This test does
 * everything that is done in "metadata.java" (which this class
 * extends), except that it makes the metadata calls in such a way
 * as to retrieve ODBC-compliant result sets.  Unlike metadata.java,
 * this test also does a (simple) check of the metadata result sets
 * to see if they comply with the standards--in this case, with the
 * ODBC 3.0 standard as defined at this URL:
 *
 * http://msdn.microsoft.com/library/default.asp?url=/library/
 * en-us/odbc/htm/odbcsqlprocedurecolumns.asp
 *
 * The ODBC standards verification involves checking the following
 * for each column in each of the relevant metadata result sets:
 *
 * 1. Does the column name match what the spec says?
 * 2. Does the column type match what the spec says?
 * 3. Does the column nullability match what the spec says?
 *
 * If compliance failures occur in any of these ways, an ODBC non-
 * compliance failure will be reported as part of the test output.
 *
 * Under no circumstances should a master file for this test
 * contain an ODBC non-compliance message and still be considered
 * as "passing".
 */

public class odbc_metadata extends metadata_test {

	// The following 2-D array holds the target names,
	// types, and nullability of metadata result sets
	// as defined in the ODBC 3.0 specification.  Each
	// row in this array corresponds to a SYSIBM
	// metadata procedure that is used (by both the
	// engine and the Network Server) for retrieval
	// of metadata.  Each row in turn consists of
	// 3 * n strings, where "n" is the number of columns
	// expected as part of the result set for that row's
	// corresponding SYSIBM procedure.  For a given column
	// "c" in each row, the expected name for that column
	// is at <row>[c], the expected type is at <row>[c+1],
	// and the expected nullability is at <row>[c+2].  The
	// expected values are hard-coded into this file, and
	// are loaded via the loadODBCTargets method.
	//
	// "15" here is the number of procedure ids defined in
	// metadata.java.  "25" is a safety figure for the max
	// number of columns a single metadata procedure returns
	// in its result set.  At time of writing, the most
	// any procedure had was 19, so use 25 to give us
	// some cushion.

	private static String [][] odbcComplianceTargets =
		new String [15][25];

	/**
	 * Constructor:
	 * Intializes the Connection and Statement fields
	 * to be used through the test, and then does the
	 * first-level check of ODBC compliance.
	 */
	public odbc_metadata(String[] args) {

		try {

			ij.getPropertyArg(args);
			con = ij.startJBMS();
			s = con.createStatement();

			// Run the compliance checks for column name and
			// column type.  This method will load the target
			// values that we want to match.
			verifyODBC3Compliance();

		} catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}

	}

	/**
	 * Makes a call to the "runTest" method in metadata_test.java,
	 * which will in turn call back here for implementations of
	 * the abstract methods.
	 */
	public static void main(String[] args) {

		new odbc_metadata(args).runTest();

	}

	/**
	 * This method is responsible for executing a metadata query and returning
	 * a result set that complies with the ODBC 3.0 specification.
	 */
	protected ResultSet getMetaDataRS(DatabaseMetaData dmd, int procId,
		String [] sArgs, String [] argArray, int [] iArgs, boolean [] bArgs)
		throws SQLException
	{

		switch (procId) {

			case GET_PROCEDURES:

				s.execute("CALL SYSIBM.SQLPROCEDURES (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", 'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_PROCEDURE_COLUMNS:

				s.execute("CALL SYSIBM.SQLPROCEDURECOLS (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", " + addQuotes(sArgs[3]) +
					", 'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_TABLES:

				int count = (argArray == null) ? 0 : argArray.length;
				StringBuffer tableTypes = new StringBuffer();
				for (int i = 0; i < count; i++) {
					if (i > 0)
						tableTypes.append(",");
					tableTypes.append(argArray[i]);
				}

				s.execute("CALL SYSIBM.SQLTABLES (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", " +
					((argArray == null) ? "null" : addQuotes(tableTypes.toString())) +
					", 'DATATYPE=''ODBC''')");

				return s.getResultSet();

			case GET_COLUMNS:

				s.execute("CALL SYSIBM.SQLCOLUMNS (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", " + addQuotes(sArgs[3]) +
					", 'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_COLUMN_PRIVILEGES:

				s.execute("CALL SYSIBM.SQLCOLPRIVILEGES (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", " + addQuotes(sArgs[3]) +
					", 'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_TABLE_PRIVILEGES:

				s.execute("CALL SYSIBM.SQLTABLEPRIVILEGES (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", 'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_BEST_ROW_IDENTIFIER:

				s.execute("CALL SYSIBM.SQLSPECIALCOLUMNS (1, " +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", " + iArgs[0] + ", " +
					(bArgs[0] ? "1, " : "0, ") + "'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_VERSION_COLUMNS:

				s.execute("CALL SYSIBM.SQLSPECIALCOLUMNS (2, " +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", 1, 1, 'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_PRIMARY_KEYS:

				s.execute("CALL SYSIBM.SQLPRIMARYKEYS (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", 'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_IMPORTED_KEYS:

				s.execute("CALL SYSIBM.SQLFOREIGNKEYS (null, null, null, " +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", 'IMPORTEDKEY=1;DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_EXPORTED_KEYS:

				s.execute("CALL SYSIBM.SQLFOREIGNKEYS (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", null, null, null, " +
					"'EXPORTEDKEY=1;DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_CROSS_REFERENCE:

				s.execute("CALL SYSIBM.SQLFOREIGNKEYS (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + ", " + addQuotes(sArgs[3]) +
					", " + addQuotes(sArgs[4]) + ", " + addQuotes(sArgs[5]) +
					", 'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_TYPE_INFO:

				s.execute("CALL SYSIBM.SQLGETTYPEINFO (0, 'DATATYPE=''ODBC''')");
				return s.getResultSet();

			case GET_INDEX_INFO:

				s.execute("CALL SYSIBM.SQLSTATISTICS (" +
					addQuotes(sArgs[0]) + ", " + addQuotes(sArgs[1]) +
					", " + addQuotes(sArgs[2]) + (bArgs[0] ? ", 0, " : ", 1, ") +
					(bArgs[1] ? "1, " : "0, ") + "'DATATYPE=''ODBC''')");
				return s.getResultSet();

			default:
			// shouldn't get here.

				System.out.println("*** UNEXPECTED PROCEDURE ID ENCOUNTERED: " + procId + ".");
				return null;

		}

	}

	/**
	 * Dumps a result to output and, if procId is not -1, checks
	 * to see if the nullability of the result set values conforms
	 * to the ODBC 3.0 specification.
	 */
	protected void dumpRS(int procId, ResultSet s) throws SQLException {

		ResultSetMetaData rsmd = s.getMetaData ();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount ();
		String[] headers = new String[numCols];
		if (numCols <= 0) {
			System.out.println("(no columns!)");
			return;
		}
		
		// Display column headings, and include column types
		// as part of those headings.
		for (int i=1; i<=numCols; i++) {
			if (i > 1) System.out.print(",");
			headers[i-1] = rsmd.getColumnLabel(i);
			System.out.print(headers[i-1]);
			System.out.print("[" + rsmd.getColumnTypeName(i) + "]");

		}
		System.out.println();
	
		// Display data, fetching until end of the result set
		StringBuffer errorColumns;
		while (s.next()) {
			// Loop through each column, getting the
			// column data and displaying
			errorColumns = new StringBuffer();
			String value;
			for (int i=1; i<=numCols; i++) {
				if (i > 1) System.out.print(",");
				value = s.getString(i);
				if (headers[i-1].equals("DATA_TYPE"))
				{
					if (((TestUtil.getJDBCMajorVersion(s.getStatement().getConnection()) >= 3) &&
						 (Integer.valueOf(value).intValue() == 16)) ||
						(Integer.valueOf(value).intValue() == -7))
						System.out.print("**BOOLEAN_TYPE for VM**");
					else
						System.out.print(value);
				}
				else
					System.out.print(value);

				// Check ODBC nullability, if required.
				if ((procId != IGNORE_PROC_ID) &&
					badNullability(procId, headers[i-1], i, s.wasNull()))
				{
					errorColumns.append(headers[i-1]);
				}

			}

			if (errorColumns.length() > 0) {
				System.out.println(
					"\n--> ODBC COMPLIANCE FAILED: Column was NULL in " +
					"the preceding row when it is specified as NOT NULL: " +
					errorColumns.toString() + ".");
			}

			System.out.println();
		}
		s.close();
	}

	/**
	 * This method tests to see if the result sets returned for
	 * ODBC clients do in fact comply with the ODBC 3.0 spec.
	 * That specification can be found here:
	 *
	 * http://msdn.microsoft.com/library/default.asp?url=/library/
	 * en-us/odbc/htm/odbcsqlprocedurecolumns.asp
	 *
	 * NOTE: This method only verifies the names and types of the
	 * columns.  The nullability of the values is checked as part
	 * of the dumpRS() method.
	 *
	 */
	protected void verifyODBC3Compliance()
		throws Exception
	{

		System.out.println(
			"\n=============== Begin ODBC 3.0 Compliance Tests =================\n");

		// Load the "target" values, which are the values that
		// specified by the ODBC specification.
		loadODBCTargets();

		System.out.println("SQLProcedures:");
		s.execute(
			"call sysibm.sqlprocedures (null, '%', 'GETPCTEST%', 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_PROCEDURES);

		System.out.println("SQLProcedureColumns:");
		s.execute(
				"call sysibm.sqlprocedurecols(null, '%', 'GETPCTEST%', '%', 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_PROCEDURE_COLUMNS);

		System.out.println("SQLTables:");
		s.execute(
			"call sysibm.sqltables (null, null, null, 'SYSTEM TABLE', 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_TABLES);

		System.out.println("SQLColumns:");
		s.execute(
			"call sysibm.sqlcolumns ('', null, '', '', 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_COLUMNS);

		System.out.println("SQLColumnPrivileges:");
		s.execute(
			"call sysibm.sqlcolprivileges ('Huey', 'Dewey', 'Louie', 'Frooey', 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_COLUMN_PRIVILEGES);

		System.out.println("SQLTablePrivileges:");
		s.execute(
			"call sysibm.sqltableprivileges ('Huey', 'Dewey', 'Louie', 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_TABLE_PRIVILEGES);

		System.out.println("SQLSpecialColumns: getBestRowIdentifier");
		s.execute(
			"call sysibm.sqlspecialcolumns (1, '', null, 'LOUIE', 1, 1, 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_BEST_ROW_IDENTIFIER);

		System.out.println("SQLSpecialColumns: getVersionColumns");
		s.execute(
			"call sysibm.sqlspecialcolumns (2, 'Huey', 'Dewey', 'Louie', 1, 1, 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_VERSION_COLUMNS);

		System.out.println("SQLPrimaryKeys:");
		s.execute(
			"call sysibm.sqlprimarykeys ('', '%', 'LOUIE', 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_PRIMARY_KEYS);

		System.out.println("SQLForeignKeys: getImportedKeys");
		s.execute(
			"call sysibm.sqlforeignkeys (null, null, null, null, null, null, " +
					"'IMPORTEDKEY=1;DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_IMPORTED_KEYS);

		System.out.println("SQLForeignKeys: getExportedKeys");
		s.execute(
			"call sysibm.sqlforeignkeys (null, null, null, null, null, null, " +
				"'EXPORTEDKEY=1;DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_EXPORTED_KEYS);

		System.out.println("SQLForeignKeys: getCrossReference");
		s.execute(
			"call sysibm.sqlforeignkeys ('', null, 'LOUIE', '', null, 'REFTAB', 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_CROSS_REFERENCE);

		System.out.println("SQLGetTypeInfo");
		s.execute(
			"call sysibm.sqlgettypeinfo (0, 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_TYPE_INFO);

		System.out.println("SQLStatistics:");
		s.execute(
			"call sysibm.sqlstatistics ('', 'SYS', 'SYSCOLUMNS', 1, 0, 'DATATYPE=''ODBC''')");
		checkODBCNamesAndTypes(s.getResultSet(), GET_INDEX_INFO);

		System.out.println(
			"\n=============== End ODBC 3.0 Compliance Tests =================\n");

	}

	/**
	 * This is where we load the metadata result set schema
	 * that is specified in the ODBC 3.0 spec.  When we do
	 * validation of the ODBC result set, we will compare	
	 * the actual results with the target values that we
	 * load here.
	 *
	 * Target lists consist of three strings for each column
	 * in the result set.  The first string is the expected
	 * (ODBC 3.0) column name. The second string is the
	 * expected column type.  The third string is the
	 * expected column nullability (null means that the
	 * column is nullable; any non-null String means that
	 * the column is NOT NULLABLE).
	 *
	 * The target values in this method come from the following
	 * URL:
	 *
	 * http://msdn.microsoft.com/library/default.asp?url=/library/
	 * en-us/odbc/htm/odbcsqlprocedurecolumns.asp
	 *
	 */
	protected void loadODBCTargets() {

		odbcComplianceTargets[GET_PROCEDURES] = new String [] {

			"PROCEDURE_CAT", "VARCHAR", null,
			"PROCEDURE_SCHEM", "VARCHAR", null,
			"PROCEDURE_NAME", "VARCHAR", "NOT NULL",
			"NUM_INPUT_PARAMS", "INTEGER", null,
			"NUM_OUTPUT_PARAMS", "INTEGER", null,
			"NUM_RESULT_SETS", "INTEGER", null,
			"REMARKS", "VARCHAR", null,
			"PROCEDURE_TYPE", "SMALLINT", null

		};

		odbcComplianceTargets[GET_PROCEDURE_COLUMNS] = new String [] {

			"PROCEDURE_CAT", "VARCHAR", null,
			"PROCEDURE_SCHEM", "VARCHAR", null,
			"PROCEDURE_NAME", "VARCHAR", "NOT NULL",
			"COLUMN_NAME", "VARCHAR", "NOT NULL",
			"COLUMN_TYPE", "SMALLINT", "NOT NULL",
			"DATA_TYPE", "SMALLINT", "NOT NULL",
			"TYPE_NAME", "VARCHAR", "NOT NULL",
			"COLUMN_SIZE", "INTEGER", null,
			"BUFFER_LENGTH", "INTEGER", null,
			"DECIMAL_DIGITS", "SMALLINT", null,
			"NUM_PREC_RADIX", "SMALLINT", null,
			"NULLABLE", "SMALLINT", "NOT NULL",
			"REMARKS", "VARCHAR", null,
			"COLUMN_DEF", "VARCHAR", null,
			"SQL_DATA_TYPE", "SMALLINT", "NOT NULL",
			"SQL_DATETIME_SUB", "SMALLINT", null,
			"CHAR_OCTET_LENGTH", "INTEGER", null,
			"ORDINAL_POSITION", "INTEGER", "NOT NULL",
			"IS_NULLABLE", "VARCHAR", null

		};

		odbcComplianceTargets[GET_TABLES] = new String [] {

			"TABLE_CAT", "VARCHAR", null,
			"TABLE_SCHEM", "VARCHAR", null,
			"TABLE_NAME", "VARCHAR", null,
			"TABLE_TYPE", "VARCHAR", null,
			"REMARKS", "VARCHAR", null

		};

		odbcComplianceTargets[GET_COLUMNS] = new String [] {

			"TABLE_CAT", "VARCHAR", null,
			"TABLE_SCHEM", "VARCHAR", null,
			"TABLE_NAME", "VARCHAR", "NOT NULL",
			"COLUMN_NAME", "VARCHAR", "NOT NULL",
			"DATA_TYPE", "SMALLINT", "NOT NULL",
			"TYPE_NAME", "VARCHAR", "NOT NULL",
			"COLUMN_SIZE", "INTEGER", null,
			"BUFFER_LENGTH", "INTEGER", null,
			"DECIMAL_DIGITS", "SMALLINT", null,
			"NUM_PREC_RADIX", "SMALLINT", null,
			"NULLABLE", "SMALLINT", "NOT NULL",
			"REMARKS", "VARCHAR", null,
			"COLUMN_DEF", "VARCHAR", null,
			"SQL_DATA_TYPE", "SMALLINT", "NOT NULL",
			"SQL_DATETIME_SUB", "SMALLINT", null,
			"CHAR_OCTET_LENGTH", "INTEGER", null,
			"ORDINAL_POSITION", "INTEGER", "NOT NULL",
			"IS_NULLABLE", "VARCHAR", null

		};

		odbcComplianceTargets[GET_COLUMN_PRIVILEGES] = new String [] {

			"TABLE_CAT", "VARCHAR", null,
			"TABLE_SCHEM", "VARCHAR", null,
			"TABLE_NAME", "VARCHAR", "NOT NULL",
			"COLUMN_NAME", "VARCHAR", "NOT NULL",
			"GRANTOR", "VARCHAR", null,
			"GRANTEE", "VARCHAR", "NOT NULL",
			"PRIVILEGE", "VARCHAR", "NOT NULL",
			"IS_GRANTABLE", "VARCHAR", null

		};

		odbcComplianceTargets[GET_TABLE_PRIVILEGES] = new String [] {

			"TABLE_CAT", "VARCHAR", null,
			"TABLE_SCHEM", "VARCHAR", null,
			"TABLE_NAME", "VARCHAR", "NOT NULL",
			"GRANTOR", "VARCHAR", null,
			"GRANTEE", "VARCHAR", "NOT NULL",
			"PRIVILEGE", "VARCHAR", "NOT NULL",
			"IS_GRANTABLE", "VARCHAR", null

		};

		// Next two corresond to ODBC's "SQLSpecialColumns".

		odbcComplianceTargets[GET_BEST_ROW_IDENTIFIER] = new String [] {

			"SCOPE", "SMALLINT", null,
			"COLUMN_NAME", "VARCHAR", "NOT NULL",
			"DATA_TYPE", "SMALLINT", "NOT NULL",
			"TYPE_NAME", "VARCHAR", "NOT NULL",
			"COLUMN_SIZE", "INTEGER", null,
			"BUFFER_LENGTH", "INTEGER", null,
			"DECIMAL_DIGITS", "SMALLINT", null,
			"PSEUDO_COLUMN", "SMALLINT", null

		};

		odbcComplianceTargets[GET_VERSION_COLUMNS] = 
			odbcComplianceTargets[GET_BEST_ROW_IDENTIFIER];

		odbcComplianceTargets[GET_PRIMARY_KEYS] = new String [] {

			"TABLE_CAT", "VARCHAR", null,
			"TABLE_SCHEM", "VARCHAR", null,
			"TABLE_NAME", "VARCHAR", "NOT NULL",
			"COLUMN_NAME", "VARCHAR", "NOT NULL",
			"KEY_SEQ", "SMALLINT", "NOT NULL",
			"PK_NAME", "VARCHAR", null

		};

		// Next three correspond to ODBC's "SQLForeignKeys".

		odbcComplianceTargets[GET_IMPORTED_KEYS] = new String [] {

			"PKTABLE_CAT", "VARCHAR", null,
			"PKTABLE_SCHEM", "VARCHAR", null,
			"PKTABLE_NAME", "VARCHAR", "NOT NULL",
			"PKCOLUMN_NAME", "VARCHAR", "NOT NULL",
			"FKTABLE_CAT", "VARCHAR", null,
			"FKTABLE_SCHEM", "VARCHAR", null,
			"FKTABLE_NAME", "VARCHAR", "NOT NULL",
			"FKCOLUMN_NAME", "VARCHAR", "NOT NULL",
			"KEY_SEQ", "SMALLINT", "NOT NULL",
			"UPDATE_RULE", "SMALLINT", null,
			"DELETE_RULE", "SMALLINT", null,
			"FK_NAME", "VARCHAR", null,
			"PK_NAME", "VARCHAR", null,
			"DEFERRABILITY", "SMALLINT", null

		};

		odbcComplianceTargets[GET_EXPORTED_KEYS] =
			odbcComplianceTargets[GET_IMPORTED_KEYS];

		odbcComplianceTargets[GET_CROSS_REFERENCE] =
			odbcComplianceTargets[GET_IMPORTED_KEYS];

		odbcComplianceTargets[GET_TYPE_INFO] = new String [] {

			"TYPE_NAME", "VARCHAR", "NOT NULL",
			"DATA_TYPE", "SMALLINT", "NOT NULL",
			"COLUMN_SIZE", "INTEGER", null,
			"LITERAL_PREFIX", "VARCHAR", null,
			"LITERAL_SUFFIX", "VARCHAR", null,
			"CREATE_PARAMS", "VARCHAR", null,
			"NULLABLE", "SMALLINT", "NOT NULL",
			"CASE_SENSITIVE", "SMALLINT", "NOT NULL",
			"SEARCHABLE", "SMALLINT", "NOT NULL",
			"UNSIGNED_ATTRIBUTE", "SMALLINT", null,
			"FIXED_PREC_SCALE", "SMALLINT", "NOT NULL",
			"AUTO_UNIQUE_VAL", "SMALLINT", null,
			"LOCAL_TYPE_NAME", "VARCHAR", null,
			"MINIMUM_SCALE", "SMALLINT", null,
			"MAXIMUM_SCALE", "SMALLINT", null,
			"SQL_DATA_TYPE", "SMALLINT", "NOT NULL",
			"SQL_DATETIME_SUB", "SMALLINT", null,
			"NUM_PREC_RADIX", "INTEGER", null,
			"INTERVAL_PRECISION", "SMALLINT", null

		};

		// Next one corresponds to ODBC's "SQLStatistics".
		odbcComplianceTargets[GET_INDEX_INFO] = new String [] {

			"TABLE_CAT", "VARCHAR", null,
			"TABLE_SCHEM", "VARCHAR", null,
			"TABLE_NAME", "VARCHAR", "NOT NULL",
			"NON_UNIQUE", "SMALLINT", null,
			"INDEX_QUALIFIER", "VARCHAR", null,
			"INDEX_NAME", "VARCHAR", null,
			"TYPE", "SMALLINT", "NOT NULL",
			"ORDINAL_POSITION", "SMALLINT", null,
			"COLUMN_NAME", "VARCHAR", null,
			"ASC_OR_DESC", "CHAR", null,
			"CARDINALITY", "INTEGER", null,
			"PAGES", "INTEGER", null,
			"FILTER_CONDITION", "VARCHAR", null

		};

	}

	/**
	 * Takes result set metadata and sees if the names
	 * and types of its columns match what ODBC 3.0
	 * dictates.
	 */
	protected void checkODBCNamesAndTypes(ResultSet rs, int procId)
		throws SQLException
	{

		ResultSetMetaData rsmd = rs.getMetaData();

		int numCols = rsmd.getColumnCount();
		int targetCols = odbcComplianceTargets[procId].length / 3;
		if (numCols < targetCols) {
		// result set is missing columns, so we already know
		// something is wrong.
			System.out.println(
				" --> ODBC COMPLIANCE FAILED: Result set was missing columns.");
			return;
		}

		// Check type and name of each column in the result set.
		for (int i = 1; i <= targetCols; i++) {

			int offset = 3 * (i - 1);
			if (!rsmd.getColumnLabel(i).equals(odbcComplianceTargets[procId][offset])) {
				System.out.println(
					"--> ODBC COMPLIANCE FAILED: Column name '" + rsmd.getColumnLabel(i) +
					"' does not match expected name '" +
					odbcComplianceTargets[procId][offset] + "'.");
			}
			if (!rsmd.getColumnTypeName(i).equals(odbcComplianceTargets[procId][offset + 1])) {
				System.out.println(
					"--> ODBC COMPLIANCE FAILED: Column type '" + rsmd.getColumnTypeName(i) +
					"' does not match expected type '" +
					odbcComplianceTargets[procId][offset + 1] + "' for column '" +
					rsmd.getColumnLabel(i) + "'.");
			}

		}

		System.out.println("==> ODBC type/name checking done.");

	}

	/**
	 * Takes result set metadata and sees if the
	 * nullability of its columns match what ODBC 3.0
	 * dictates.
	 */
	protected boolean badNullability(int odbcProc, String colName,
		int colNum, boolean wasNull)
	{

		return (wasNull &&
			odbcComplianceTargets[odbcProc][3 * (colNum-1) + 2] != null);

	}

	private static String addQuotes(String str) {

		if (str == null)
			return "null";

		if (str.length() == 0)
			return "''";

		if ((str.charAt(0) == '\'') && (str.charAt(str.length()-1) == '\''))
		// already have quotes.
			return str;

		return "'" + str + "'";

	}

}

