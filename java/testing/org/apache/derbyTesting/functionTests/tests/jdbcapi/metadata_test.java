/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata_test

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

/**
 * Test of database meta-data.  This program simply calls each of the meta-data
 * methods, one by one, and prints the results.  The test passes if the printed
 * results match a previously stored "master".  Thus this test cannot actually
 * discern whether it passes or not.
 *
 */

public abstract class metadata_test {

	// Ids for the Derby internal procedures that are used to fetch
	// some of the metadata.

	protected static final int GET_PROCEDURES = 0;
 	protected static final int GET_PROCEDURE_COLUMNS = 1;
 	protected static final int GET_TABLES = 2;
 	protected static final int GET_COLUMNS = 3;
 	protected static final int GET_COLUMN_PRIVILEGES = 5;
 	protected static final int GET_TABLE_PRIVILEGES = 6;
 	protected static final int GET_BEST_ROW_IDENTIFIER = 7;
 	protected static final int GET_VERSION_COLUMNS = 8;
 	protected static final int GET_PRIMARY_KEYS = 9;
 	protected static final int GET_IMPORTED_KEYS = 10;
 	protected static final int GET_EXPORTED_KEYS = 11;
 	protected static final int GET_CROSS_REFERENCE = 12;
 	protected static final int GET_TYPE_INFO = 13;
 	protected static final int GET_INDEX_INFO = 14;

	protected static final int IGNORE_PROC_ID = -1;

	// We leave it up to the classes which extend this one to
	// initialize the following fields at contruct time.
	protected Connection con;
	protected static Statement s;

	protected void runTest() {

		DatabaseMetaData met;
		ResultSet rs;
		ResultSetMetaData rsmet;

		System.out.println("Test metadata starting");

		try
		{

			// test decimal type and other numeric types precision, scale,
			// and display width after operations, beetle 3875, 3906
			s.execute("create table t (i int, s smallint, r real, "+
				"d double precision, dt date, t time, ts timestamp, "+
				"c char(10), v varchar(40) not null, dc dec(10,2))");
			s.execute("insert into t values(1,2,3.3,4.4,date('1990-05-05'),"+
						 "time('12:06:06'),timestamp('1990-07-07 07:07:07.07'),"+
						 "'eight','nine', 11.1)");

			// test decimal type and other numeric types precision, scale,
			// and display width after operations, beetle 3875, 3906
			//rs = s.executeQuery("select dc from t where tn = 10 union select dc from t where i = 1");
			rs = s.executeQuery("select dc from t where dc = 11.1 union select dc from t where i = 1");
			rsmet = rs.getMetaData();
			System.out.println("Column display size of the union result is: " + rsmet.getColumnDisplaySize(1));
			rs.close();

			rs = s.executeQuery("select dc, dc, r+dc, d-dc, dc-d from t");
			rsmet = rs.getMetaData();
			System.out.println("dec(10,2) -- precision: " + rsmet.getPrecision(1) + " scale: " + rsmet.getScale(1) + " display size: " + rsmet.getColumnDisplaySize(1) + " type name: " + rsmet.getColumnTypeName(1));
			System.out.println("dec(10,2) -- precision: " + rsmet.getPrecision(2) + " scale: " + rsmet.getScale(2) + " display size: " + rsmet.getColumnDisplaySize(2) + " type name: " + rsmet.getColumnTypeName(2));
			System.out.println("real + dec(10,2) -- precision: " + rsmet.getPrecision(3) + " scale: " + rsmet.getScale(3) + " display size: " + rsmet.getColumnDisplaySize(3) + " type name: " + rsmet.getColumnTypeName(3));
			System.out.println("double precision - dec(10,2) -- precision: " + rsmet.getPrecision(4) + " scale: " + rsmet.getScale(4) + " display size: " + rsmet.getColumnDisplaySize(4) + " type name: " + rsmet.getColumnTypeName(4));
			// result is double, precision/scale don't make sense
			System.out.println("dec(10,2) - double precision -- precision: " + rsmet.getPrecision(5) + " scale: " + rsmet.getScale(5) + " display size: " + rsmet.getColumnDisplaySize(5) + " type name: " + rsmet.getColumnTypeName(5));
			while (rs.next())
				System.out.println("result row: " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3) + " " + rs.getString(4) + " " + rs.getString(5));
			rs.close();

			s.execute("insert into t values(1,2,3.3,4.4,date('1990-05-05'),"+
						 "time('12:06:06'),timestamp('1990-07-07 07:07:07.07'),"+
						 "'eight','nine', 11.11)");

			// test decimal/integer static column result scale consistent
			// with result set metadata after division, beetle 3901
			rs = s.executeQuery("select dc / 2 from t");
			rsmet = rs.getMetaData();
			System.out.println("Column result scale after division is: " + rsmet.getScale(1));
			while (rs.next())
				System.out.println("dc / 2 = " + rs.getString(1));
			rs.close();


			s.execute("create table louie (i int not null default 10, s smallint not null, " +
				      "c30 char(30) not null, " +
					  "vc10 varchar(10) not null default 'asdf', " +
					  "constraint PRIMKEY primary key(vc10, i), " +
					  "constraint UNIQUEKEY unique(c30, s), " + 
					  "ai bigint generated always as identity (start with -10, increment by 2001))");

			// Create another unique index on louie
			s.execute("create unique index u1 on louie(s, i)");
			// Create a non-unique index on louie
			s.execute("create index u2 on louie(s)");
			// Create a view on louie
			s.execute("create view screwie as select * from louie");

			// Create a foreign key
			s.execute("create table reftab (vc10 varchar(10), i int, " +
					  "s smallint, c30 char(30), " +
					  "s2 smallint, c302 char(30), " +
					  "dprim decimal(5,1) not null, dfor decimal(5,1) not null, "+
					  "constraint PKEY_REFTAB	primary key (dprim), " + 
					  "constraint FKEYSELF 		foreign key (dfor) references reftab, "+
					  "constraint FKEY1 		foreign key(vc10, i) references louie, " + 
				  	  "constraint FKEY2 		foreign key(c30, s2) references louie (c30, s), "+
				  	  "constraint FKEY3 		foreign key(c30, s) references louie (c30, s))");

			s.execute("create table reftab2 (t2_vc10 varchar(10), t2_i int, " +
					  "constraint T2_FKEY1 		foreign key(t2_vc10, t2_i) references louie)");

			// Create a table with all types
			s.execute("create table alltypes ( "+
							//"bitcol16_______ bit(16), "+
							//"bitvaryingcol32 bit varying(32), "+ 
							//"tinyintcol tinyint, "+
							"smallintcol smallint, "+
							"intcol int default 20, "+
							"bigintcol bigint, "+
							"realcol real, "+
							"doublepreccol double precision default 10, "+
							"decimalcol10p4s decimal(10,4), "+
							"numericcol20p2s numeric(20,2), "+
							"char8col___ char(8), "+
							"varchar9col varchar(9), "+
							"longvarcharcol long varchar,"+
							//"longvarbinarycol long bit varying,"+
							//"nchar10col nchar(10)"
					  //+ ", nvarchar8col nvarchar(8)"
					  //+ ", longnvarchar long nvarchar"
					  //+ ", 
						"blobcol blob(3K)"
					  + ")" );
			// test for beetle 4620
			s.execute("CREATE TABLE INFLIGHT(FLT_NUM CHAR(20) NOT NULL," + 
						"FLT_ORIGIN CHAR(6), " +
						"FLT_DEST CHAR(6),  " +
						"FLT_AIRCRAFT CHAR(20), " +
						"FLT_FLYING_TIME VARCHAR(22), "+
						"FLT_DEPT_TIME CHAR(8),  "+
						"FLT_ARR_TIME CHAR(8),  "+
						"FLT_NOTES VARCHAR(510), "+ 
						"FLT_DAYS_OF_WK CHAR(14), "+ 
						"FLT_CRAFT_PIC VARCHAR(32672), "+
						"PRIMARY KEY(FLT_NUM))");

			// Create procedures so we can test 
			// getProcedureColumns()
                        s.execute("create procedure GETPCTEST1 (" +
				// for creating, the procedure's params do not need to exactly match the method's
				"out outb VARCHAR(3), a VARCHAR(3), b NUMERIC, c SMALLINT, " +
				"e SMALLINT, f INTEGER, g BIGINT, h FLOAT, i DOUBLE PRECISION, " +
				"k DATE, l TIME, T TIMESTAMP )"+
				"language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata.getpc'" +
							" parameter style java"); 
                        s.execute("create procedure GETPCTEST2 (pa INTEGER, pb BIGINT)"+
				"language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata.getpc'" +
				" parameter style java"); 
                        s.execute("create procedure GETPCTEST3A (STRING1 VARCHAR(5), out STRING2 VARCHAR(5))"+
				"language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata.getpc'" +
				" parameter style java"); 
                        s.execute("create procedure GETPCTEST3B (in STRING3 VARCHAR(5), inout STRING4 VARCHAR(5))"+
				"language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata.getpc'" +
				" parameter style java"); 
                        s.execute("create procedure GETPCTEST4A()  "+
				"language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata.getpc4a'"+
				" parameter style java"); 
                        s.execute("create procedure GETPCTEST4B() "+
				"language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata.getpc4b'" +
				" parameter style java"); 
                        s.execute("create procedure GETPCTEST4Bx(out retparam INTEGER) "+
				"language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata.getpc4b'" +
				" parameter style java"); 

			met = con.getMetaData();

			System.out.println("JDBC Driver '" + met.getDriverName() +
							   "', version " + met.getDriverMajorVersion() +
							   "." + met.getDriverMinorVersion() +
							   " (" + met.getDriverVersion() + ")");

			try {
			    System.out.println("The URL is: " + met.getURL());
			} catch (Throwable err) {
			    System.out.println("%%getURL() gave the exception: " + err);
			}

			System.out.println("allTablesAreSelectable(): " +
							   met.allTablesAreSelectable());
			
			System.out.println("maxColumnNameLength(): " + met.getMaxColumnNameLength());

			System.out.println();
			System.out.println("getSchemas():");
			dumpRS(met.getSchemas());

			System.out.println();
			System.out.println("getCatalogs():");
			dumpRS(met.getCatalogs());

			System.out.println("getSearchStringEscape(): " +
							   met.getSearchStringEscape());

			System.out.println("getSQLKeywords(): " +
							   met.getSQLKeywords());

			System.out.println("getDefaultTransactionIsolation(): " +
							   met.getDefaultTransactionIsolation());

			System.out.println("getProcedures():");
			dumpRS(GET_PROCEDURES, getMetaDataRS(met, GET_PROCEDURES,
				new String [] {null, "%", "GETPCTEST%"},
				null, null, null));


			/*
			 * any methods that were not tested above using code written
			 * specifically for it will now be tested in a generic way.
			 */


			System.out.println("allProceduresAreCallable(): " +
							   met.allProceduresAreCallable());
			System.out.println("getUserName(): " +
							   met.getUserName());
			System.out.println("isReadOnly(): " +
							   met.isReadOnly());
			System.out.println("nullsAreSortedHigh(): " +
							   met.nullsAreSortedHigh());
			System.out.println("nullsAreSortedLow(): " +
							   met.nullsAreSortedLow());
			System.out.println("nullsAreSortedAtStart(): " +
							   met.nullsAreSortedAtStart());
			System.out.println("nullsAreSortedAtEnd(): " +
							   met.nullsAreSortedAtEnd());


			System.out.println("getDatabaseProductName(): " + met.getDatabaseProductName());

			String v = met.getDatabaseProductVersion();
			int l = v.indexOf('(');
			if (l<0) l = v.length();
			v = v.substring(0,l);
			System.out.println("getDatabaseProductVersion(): " + v);
			System.out.println("getDriverVersion(): " +
							   met.getDriverVersion());
			System.out.println("usesLocalFiles(): " +
							   met.usesLocalFiles());
			System.out.println("usesLocalFilePerTable(): " +
							   met.usesLocalFilePerTable());
			System.out.println("supportsMixedCaseIdentifiers(): " +
							   met.supportsMixedCaseIdentifiers());
			System.out.println("storesUpperCaseIdentifiers(): " +
							   met.storesUpperCaseIdentifiers());
			System.out.println("storesLowerCaseIdentifiers(): " +
							   met.storesLowerCaseIdentifiers());
			System.out.println("storesMixedCaseIdentifiers(): " +
							   met.storesMixedCaseIdentifiers());
			System.out.println("supportsMixedCaseQuotedIdentifiers(): " +
							   met.supportsMixedCaseQuotedIdentifiers());
			System.out.println("storesUpperCaseQuotedIdentifiers(): " +
							   met.storesUpperCaseQuotedIdentifiers());
			System.out.println("storesLowerCaseQuotedIdentifiers(): " +
							   met.storesLowerCaseQuotedIdentifiers());
			System.out.println("storesMixedCaseQuotedIdentifiers(): " +
							   met.storesMixedCaseQuotedIdentifiers());
			System.out.println("getIdentifierQuoteString(): " +
							   met.getIdentifierQuoteString());
			System.out.println("getNumericFunctions(): " +
							   met.getNumericFunctions());
			System.out.println("getStringFunctions(): " +
							   met.getStringFunctions());
			System.out.println("getSystemFunctions(): " +
							   met.getSystemFunctions());
			System.out.println("getTimeDateFunctions(): " +
							   met.getTimeDateFunctions());
			System.out.println("getExtraNameCharacters(): " +
							   met.getExtraNameCharacters());
			System.out.println("supportsAlterTableWithAddColumn(): " +
							   met.supportsAlterTableWithAddColumn());
			System.out.println("supportsAlterTableWithDropColumn(): " +
							   met.supportsAlterTableWithDropColumn());
			System.out.println("supportsColumnAliasing(): " +
							   met.supportsColumnAliasing());
			System.out.println("nullPlusNonNullIsNull(): " +
							   met.nullPlusNonNullIsNull());
			System.out.println("supportsConvert(): " +
							   met.supportsConvert());
			System.out.println("supportsConvert(Types.INTEGER, Types.SMALLINT): " +
							   met.supportsConvert(Types.INTEGER, Types.SMALLINT));
			System.out.println("supportsTableCorrelationNames(): " +
							   met.supportsTableCorrelationNames());
			System.out.println("supportsDifferentTableCorrelationNames(): " +
							   met.supportsDifferentTableCorrelationNames());
			System.out.println("supportsExpressionsInOrderBy(): " +
							   met.supportsExpressionsInOrderBy());
			System.out.println("supportsOrderByUnrelated(): " +
							   met.supportsOrderByUnrelated());
			System.out.println("supportsGroupBy(): " +
							   met.supportsGroupBy());
			System.out.println("supportsGroupByUnrelated(): " +
							   met.supportsGroupByUnrelated());
			System.out.println("supportsGroupByBeyondSelect(): " +
							   met.supportsGroupByBeyondSelect());
			System.out.println("supportsLikeEscapeClause(): " +
							   met.supportsLikeEscapeClause());
			System.out.println("supportsMultipleResultSets(): " +
							   met.supportsMultipleResultSets());
			System.out.println("supportsMultipleTransactions(): " +
							   met.supportsMultipleTransactions());
			System.out.println("supportsNonNullableColumns(): " +
							   met.supportsNonNullableColumns());
			System.out.println("supportsMinimumSQLGrammar(): " +
							   met.supportsMinimumSQLGrammar());
			System.out.println("supportsCoreSQLGrammar(): " +
							   met.supportsCoreSQLGrammar());
			System.out.println("supportsExtendedSQLGrammar(): " +
							   met.supportsExtendedSQLGrammar());
			System.out.println("supportsANSI92EntryLevelSQL(): " +
							   met.supportsANSI92EntryLevelSQL());
			System.out.println("supportsANSI92IntermediateSQL(): " +
							   met.supportsANSI92IntermediateSQL());
			System.out.println("supportsANSI92FullSQL(): " +
							   met.supportsANSI92FullSQL());
			System.out.println("supportsIntegrityEnhancementFacility(): " +
							   met.supportsIntegrityEnhancementFacility());
			System.out.println("supportsOuterJoins(): " +
							   met.supportsOuterJoins());
			System.out.println("supportsFullOuterJoins(): " +
							   met.supportsFullOuterJoins());
			System.out.println("supportsLimitedOuterJoins(): " +
							   met.supportsLimitedOuterJoins());
			System.out.println("getSchemaTerm(): " +
							   met.getSchemaTerm());
			System.out.println("getProcedureTerm(): " +
							   met.getProcedureTerm());
			System.out.println("getCatalogTerm(): " +
							   met.getCatalogTerm());
			System.out.println("isCatalogAtStart(): " +
							   met.isCatalogAtStart());
			System.out.println("getCatalogSeparator(): " +
							   met.getCatalogSeparator());
			System.out.println("supportsSchemasInDataManipulation(): " +
							   met.supportsSchemasInDataManipulation());
			System.out.println("supportsSchemasInProcedureCalls(): " +
							   met.supportsSchemasInProcedureCalls());
			System.out.println("supportsSchemasInTableDefinitions(): " +
							   met.supportsSchemasInTableDefinitions());
			System.out.println("supportsSchemasInIndexDefinitions(): " +
							   met.supportsSchemasInIndexDefinitions());
			System.out.println("supportsSchemasInPrivilegeDefinitions(): " +
							   met.supportsSchemasInPrivilegeDefinitions());
			System.out.println("supportsCatalogsInDataManipulation(): " +
							   met.supportsCatalogsInDataManipulation());
			System.out.println("supportsCatalogsInProcedureCalls(): " +
							   met.supportsCatalogsInProcedureCalls());
			System.out.println("supportsCatalogsInTableDefinitions(): " +
							   met.supportsCatalogsInTableDefinitions());
			System.out.println("supportsCatalogsInIndexDefinitions(): " +
							   met.supportsCatalogsInIndexDefinitions());
			System.out.println("supportsCatalogsInPrivilegeDefinitions(): " +
							   met.supportsCatalogsInPrivilegeDefinitions());
			System.out.println("supportsPositionedDelete(): " +
							   met.supportsPositionedDelete());
			System.out.println("supportsPositionedUpdate(): " +
							   met.supportsPositionedUpdate());
			System.out.println("supportsSelectForUpdate(): " +
							   met.supportsSelectForUpdate());
			System.out.println("supportsStoredProcedures(): " +
							   met.supportsStoredProcedures());
			System.out.println("supportsSubqueriesInComparisons(): " +
							   met.supportsSubqueriesInComparisons());
			System.out.println("supportsSubqueriesInExists(): " +
							   met.supportsSubqueriesInExists());
			System.out.println("supportsSubqueriesInIns(): " +
							   met.supportsSubqueriesInIns());
			System.out.println("supportsSubqueriesInQuantifieds(): " +
							   met.supportsSubqueriesInQuantifieds());
			System.out.println("supportsCorrelatedSubqueries(): " +
							   met.supportsCorrelatedSubqueries());
			System.out.println("supportsUnion(): " +
							   met.supportsUnion());
			System.out.println("supportsUnionAll(): " +
							   met.supportsUnionAll());
			System.out.println("supportsOpenCursorsAcrossCommit(): " +
							   met.supportsOpenCursorsAcrossCommit());
			System.out.println("supportsOpenCursorsAcrossRollback(): " +
							   met.supportsOpenCursorsAcrossRollback());
			System.out.println("supportsOpenStatementsAcrossCommit(): " +
							   met.supportsOpenStatementsAcrossCommit());
			System.out.println("supportsOpenStatementsAcrossRollback(): " +
							   met.supportsOpenStatementsAcrossRollback());
			System.out.println("getMaxBinaryLiteralLength(): " +
							   met.getMaxBinaryLiteralLength());
			System.out.println("getMaxCharLiteralLength(): " +
							   met.getMaxCharLiteralLength());
			System.out.println("getMaxColumnsInGroupBy(): " +
							   met.getMaxColumnsInGroupBy());
			System.out.println("getMaxColumnsInIndex(): " +
							   met.getMaxColumnsInIndex());
			System.out.println("getMaxColumnsInOrderBy(): " +
							   met.getMaxColumnsInOrderBy());
			System.out.println("getMaxColumnsInSelect(): " +
							   met.getMaxColumnsInSelect());
			System.out.println("getMaxColumnsInTable(): " +
							   met.getMaxColumnsInTable());
			System.out.println("getMaxConnections(): " +
							   met.getMaxConnections());
			System.out.println("getMaxCursorNameLength(): " +
							   met.getMaxCursorNameLength());
			System.out.println("getMaxIndexLength(): " +
							   met.getMaxIndexLength());
			System.out.println("getMaxSchemaNameLength(): " +
							   met.getMaxSchemaNameLength());
			System.out.println("getMaxProcedureNameLength(): " +
							   met.getMaxProcedureNameLength());
			System.out.println("getMaxCatalogNameLength(): " +
							   met.getMaxCatalogNameLength());
			System.out.println("getMaxRowSize(): " +
							   met.getMaxRowSize());
			System.out.println("doesMaxRowSizeIncludeBlobs(): " +
							   met.doesMaxRowSizeIncludeBlobs());
			System.out.println("getMaxStatementLength(): " +
							   met.getMaxStatementLength());
			System.out.println("getMaxStatements(): " +
							   met.getMaxStatements());
			System.out.println("getMaxTableNameLength(): " +
							   met.getMaxTableNameLength());
			System.out.println("getMaxTablesInSelect(): " +
							   met.getMaxTablesInSelect());
			System.out.println("getMaxUserNameLength(): " +
							   met.getMaxUserNameLength());
			System.out.println("supportsTransactions(): " +
							   met.supportsTransactions());
			System.out.println("supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE): " +
							   met.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
			System.out.println("supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ): " +
							   met.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
			System.out.println("supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE): " +
							   met.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
			System.out.println("supportsDataDefinitionAndDataManipulationTransactions(): " +
							   met.supportsDataDefinitionAndDataManipulationTransactions());
			System.out.println("supportsDataManipulationTransactionsOnly(): " +
							   met.supportsDataManipulationTransactionsOnly());
			System.out.println("dataDefinitionCausesTransactionCommit(): " +
							   met.dataDefinitionCausesTransactionCommit());
			System.out.println("dataDefinitionIgnoredInTransactions(): " +
							   met.dataDefinitionIgnoredInTransactions());

			System.out.println("getConnection(): "+
					   ((met.getConnection()==con)?"same connection":"different connection") );
			System.out.println("getProcedureColumns():");
			dumpRS(GET_PROCEDURE_COLUMNS, getMetaDataRS(met, GET_PROCEDURE_COLUMNS,
				new String [] {null, "%", "GETPCTEST%", "%"},
				null, null, null));

 			System.out.println("getTables() with TABLE_TYPE in ('SYSTEM TABLE') :");
 			String[] tabTypes = new String[1];
 			tabTypes[0] = "SYSTEM TABLE";
 			dumpRS(GET_TABLES, getMetaDataRS(met, GET_TABLES,
				new String [] {null, null, null},
 				tabTypes, null, null));

			System.out.println("getTables() with no types:");
 			dumpRS(GET_TABLES, getMetaDataRS(met, GET_TABLES,
				new String [] {"", null, "%"},
				null, null, null));

 			System.out.println("getTables() with TABLE_TYPE in ('VIEW','TABLE') :");
 			tabTypes = new String[2];
 			tabTypes[0] = "VIEW";
 			tabTypes[1] = "TABLE";
 			dumpRS(GET_TABLES, getMetaDataRS(met, GET_TABLES,
				new String [] {null, null, null},
 				tabTypes, null, null));


			System.out.println("getTableTypes():");
			dumpRS(met.getTableTypes());

			System.out.println("getColumns():");
			dumpRS(GET_COLUMNS, getMetaDataRS(met, GET_COLUMNS,
				new String [] {"", null, "", ""},
				null, null, null));

			System.out.println("getColumns('SYSTABLES'):");
			dumpRS(GET_COLUMNS, getMetaDataRS(met, GET_COLUMNS,
				new String [] {"", "SYS", "SYSTABLES", null},
				null, null, null));

			System.out.println("getColumns('ALLTYPES'):");
			dumpRS(GET_COLUMNS, getMetaDataRS(met, GET_COLUMNS,
				new String [] {"", "APP", "ALLTYPES", null},
				null, null, null));

			System.out.println("getColumns('LOUIE'):");
			dumpRS(GET_COLUMNS, getMetaDataRS(met, GET_COLUMNS,
				new String [] {"", "APP", "LOUIE", null},
				null, null, null));

			// test for beetle 4620
			System.out.println("getColumns('INFLIGHT'):");
			dumpRS(GET_COLUMNS, getMetaDataRS(met, GET_COLUMNS,
				new String [] {"", "APP", "INFLIGHT", null},
				null, null, null));

			System.out.println("getColumnPrivileges():");
			dumpRS(GET_COLUMN_PRIVILEGES, getMetaDataRS(met, GET_COLUMN_PRIVILEGES,
				new String [] {"Huey", "Dewey", "Louie", "Frooey"},
				null, null, null));

			System.out.println("getTablePrivileges():");
			dumpRS(GET_TABLE_PRIVILEGES, getMetaDataRS(met, GET_TABLE_PRIVILEGES,
				new String [] {"Huey", "Dewey", "Louie"},
				null, null, null));

			System.out.println("getBestRowIdentifier(\"\",null,\"LOUIE\"):");
			dumpRS(GET_BEST_ROW_IDENTIFIER, getMetaDataRS(met, GET_BEST_ROW_IDENTIFIER,
				new String [] {"", null, "LOUIE"}, null,
				new int [] {DatabaseMetaData.bestRowTransaction},
				new boolean [] {true}));

			System.out.println("getBestRowIdentifier(\"\",\"SYS\",\"SYSTABLES\"):");
			dumpRS(GET_BEST_ROW_IDENTIFIER, getMetaDataRS(met, GET_BEST_ROW_IDENTIFIER,
				new String [] {"", "SYS", "SYSTABLES"}, null,
				new int [] {DatabaseMetaData.bestRowTransaction},
				new boolean [] {true}));

			System.out.println("getVersionColumns():");
			dumpRS(GET_VERSION_COLUMNS, getMetaDataRS(met, GET_VERSION_COLUMNS,
				new String [] {"Huey", "Dewey", "Louie"},
				null, null, null));

			System.out.println("getPrimaryKeys():");
			dumpRS(GET_PRIMARY_KEYS, getMetaDataRS(met, GET_PRIMARY_KEYS,
				new String [] {"", "%", "LOUIE"},
				null, null, null));

			//beetle 4571
			System.out.println("getPrimaryKeys(null, null, tablename):");
			dumpRS(GET_PRIMARY_KEYS, getMetaDataRS(met, GET_PRIMARY_KEYS,
				new String [] {null, null, "LOUIE"},
				null, null, null));

			System.out.println("getImportedKeys():");
			dumpRS(GET_IMPORTED_KEYS, getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {null, null, "%"},
				null, null, null));

			System.out.println("getExportedKeys():");
			dumpRS(GET_EXPORTED_KEYS, getMetaDataRS(met, GET_EXPORTED_KEYS,
				new String [] {null, null, "%"},
				null, null, null));

			System.out.println("---------------------------------------");
			System.out.println("getCrossReference('',null,'louie','',null,'reftab' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", null, "LOUIE", "", null, "REFTAB"},
				null, null, null));

			System.out.println("\ngetCrossReference('','APP','reftab','',null,'reftab' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", "APP", "REFTAB", "", null, "REFTAB"},
				null, null, null));

			System.out.println("\ngetCrossReference('',null,null,'','APP','reftab' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", null, "%", "", "APP", "REFTAB"},
				null, null, null));

			System.out.println("\ngetImportedKeys('',null,null,'','APP','reftab' ):");
			dumpRS(GET_IMPORTED_KEYS, getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {"", "APP", "REFTAB"},
				null, null, null));

			System.out.println("\ngetCrossReference('',null,'louie','','APP',null):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", null, "LOUIE", "", "APP", "%"},
				null, null, null));

			System.out.println("\ngetExportedKeys('',null,'louie,'','APP',null ):");
			dumpRS(GET_EXPORTED_KEYS, getMetaDataRS(met, GET_EXPORTED_KEYS,
				new String [] {"", null, "LOUIE"},
				null, null, null));

			System.out.println("\ngetCrossReference('','badschema','LOUIE','','APP','REFTAB' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", "BADSCHEMA", "LOUIE", "", "APP", "REFTAB"},
				null, null, null));

			System.out.println("getTypeInfo():");
			dumpRS(GET_TYPE_INFO, getMetaDataRS(met, GET_TYPE_INFO, null, null, null, null));

			/* NOTE - we call getIndexInfo() only on system tables here
 			 * so that there will be no diffs due to generated names.
			 */
			// unique indexes on SYSCOLUMNS
			System.out.println("getIndexInfo():");
			dumpRS(GET_INDEX_INFO, getMetaDataRS(met, GET_INDEX_INFO,
				new String [] {"", "SYS", "SYSCOLUMNS"},
				null, null, new boolean [] {true, false}));

			// all indexes on SYSCOLUMNS
			System.out.println("getIndexInfo():");
			dumpRS(GET_INDEX_INFO, getMetaDataRS(met, GET_INDEX_INFO,
				new String [] {"", "SYS", "SYSCOLUMNS"},
				null, null, new boolean [] {false, false}));

			System.out.println("getIndexInfo():");
			dumpRS(GET_INDEX_INFO, getMetaDataRS(met, GET_INDEX_INFO,
				new String [] {"", "SYS", "SYSTABLES"},
				null, null, new boolean [] {true, false}));

			rs = s.executeQuery("SELECT * FROM SYS.SYSTABLES");

			System.out.println("getColumns('SYSTABLES'):");
			dumpRS(GET_COLUMNS, getMetaDataRS(met, GET_COLUMNS,
				new String [] {"", "SYS", "SYSTABLES", null},
				null, null, null));
			
			try {
				if (!rs.next()) {
					System.out.println("FAIL -- user result set closed by"+
					" intervening getColumns request");
				}
			} catch (SQLException se) {
				if (this instanceof metadata) {
					System.out.println("FAIL -- user result set closed by"+
					" intervening getColumns request");
				}
				else {
					System.out.println("OK -- user result set closed by"+
					" intervening OBDC getColumns request; this was" +
					" expected because of the way the test works.");
				}
			}
			rs.close();
			//
			// Test referential actions on delete
			//
			System.out.println("---------------------------------------");
			//create tables to test that we get the delete and update 
			// referential action correct
			System.out.println("Referential action values");
			System.out.println("RESTRICT = "+ DatabaseMetaData.importedKeyRestrict);
			System.out.println("NO ACTION = "+ DatabaseMetaData.importedKeyNoAction);
			System.out.println("CASCADE = "+ DatabaseMetaData.importedKeyCascade);
			System.out.println("SETNULL = "+ DatabaseMetaData.importedKeySetNull);
			System.out.println("SETDEFAULT = "+ DatabaseMetaData.importedKeySetDefault);
			s.execute("create table refaction1(a int not null primary key)");
			s.execute("create table refactnone(a int references refaction1(a))");
			s.execute("create table refactrestrict(a int references refaction1(a) on delete restrict)");
			s.execute("create table refactnoaction(a int references refaction1(a) on delete no action)");
			s.execute("create table refactcascade(a int references refaction1(a) on delete cascade)");
			s.execute("create table refactsetnull(a int references refaction1(a) on delete set null)");
			System.out.println("getCrossReference('','APP','REFACTION1','','APP','REFACTIONNONE' ):");
			s.execute("create table refactupdrestrict(a int references refaction1(a) on update restrict)");
			s.execute("create table refactupdnoaction(a int references refaction1(a) on update no action)");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", "APP", "REFACTION1", "", "APP", "REFACTNONE"},
				null, null, null));
			System.out.println("\ngetCrossReference('','APP','REFACTION1','','APP','REFACTRESTRICT' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", "APP", "REFACTION1", "", "APP", "REFACTRESTRICT"},
				null, null, null));
			System.out.println("\ngetCrossReference('','APP','REFACTION1','','APP','REFACTNOACTION' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", "APP", "REFACTION1", "", "APP", "REFACTNOACTION"},
				null, null, null));
			System.out.println("\ngetCrossReference('','APP','REFACTION1','','APP','REFACTCASCADE' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", "APP", "REFACTION1", "", "APP", "REFACTCASCADE"},
				null, null, null));
			System.out.println("\ngetCrossReference('','APP','REFACTION1','','APP','REFACTSETNULL' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", "APP", "REFACTION1", "", "APP", "REFACTSETNULL"},
				null, null, null));
			System.out.println("\ngetCrossReference('','APP','REFACTION1','','APP','REFACTUPDRESTRICT' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", "APP", "REFACTION1", "", "APP", "REFACTUPDRESTRICT"},
				null, null, null));
			System.out.println("\ngetCrossReference('','APP','REFACTION1','','APP','REFACTUPDNOACTION' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", "APP", "REFACTION1", "", "APP", "REFACTUPDNOACTION"},
				null, null, null));

			ResultSet refrs = getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {"", "APP", "REFACTNONE"}, null, null, null);

			if (refrs.next())
			{
				//check update rule
				if (refrs.getShort(11) != DatabaseMetaData.importedKeyNoAction)
					System.out.println("\ngetImportedKeys - none update Failed - action = " + refrs.getShort(11) + " required value = " + DatabaseMetaData.importedKeyNoAction);
				else
					System.out.println("\ngetImportedKeys - none update Passed");
				//check delete rule
				if (refrs.getShort(11) != DatabaseMetaData.importedKeyNoAction)
					System.out.println("\ngetImportedKeys - none delete Failed - action = " + refrs.getShort(11) + " required value = " + DatabaseMetaData.importedKeyNoAction);
				else
					System.out.println("\ngetImportedKeys - none delete Passed");
			}
			else
					System.out.println("\ngetImportedKeys - none Failed no rows");
					
			refrs.close();
			refrs = getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {"", "APP", "REFACTRESTRICT"}, null, null, null);

			if (refrs.next())
			{
				if (refrs.getShort(11) != DatabaseMetaData.importedKeyRestrict)
					System.out.println("\ngetImportedKeys - delete Restrict Failed - action = " + refrs.getShort(11) + " required value = " + DatabaseMetaData.importedKeyRestrict);
				else
					System.out.println("\ngetImportedKeys - delete Restrict Passed");
			}
			else
					System.out.println("\ngetImportedKeys - delete Restrict Failed no rows");

			refrs.close();
			refrs = getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {"", "APP", "REFACTNOACTION"}, null, null, null);

			if (refrs.next())
			{
				if (refrs.getShort(11) != DatabaseMetaData.importedKeyNoAction)
					System.out.println("\ngetImportedKeys - delete NO ACTION Failed - action = " + refrs.getShort(11) + " required value = " + DatabaseMetaData.importedKeyNoAction);
				else
					System.out.println("\ngetImportedKeys - delete NO ACTION Passed");
			}
			else
					System.out.println("\ngetImportedKeys - delete NO ACTION Failed no rows");

			refrs.close();
			refrs = getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {"", "APP", "REFACTCASCADE"}, null, null, null);

			if (refrs.next())
			{
				if (refrs.getShort(11) != DatabaseMetaData.importedKeyCascade)
					System.out.println("\ngetImportedKeys - delete CASCADE Failed - action = " + refrs.getShort(11) + " required value = " + DatabaseMetaData.importedKeyCascade);
				else
					System.out.println("\ngetImportedKeys - delete CASCADE Passed");
			}
			else
					System.out.println("\ngetImportedKeys - delete CASCADE Failed no rows");

			refrs.close();
			refrs = getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {"", "APP", "REFACTSETNULL"}, null, null, null);

			if (refrs.next())
			{
				if (refrs.getShort(11) != DatabaseMetaData.importedKeySetNull)
					System.out.println("\ngetImportedKeys - delete SET NULL Failed - action = " + refrs.getShort(11) + " required value = " + DatabaseMetaData.importedKeySetNull);
				else
					System.out.println("\ngetImportedKeys - delete SET NULL Passed");
			}
			else
					System.out.println("\ngetImportedKeys - SET NULL Failed no rows");

			refrs.close();
			refrs = getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {"", "APP", "REFACTRESTRICT"}, null, null, null);

			if (refrs.next())
			{
				// test update rule
				if (refrs.getShort(11) != DatabaseMetaData.importedKeyRestrict)
					System.out.println("\ngetImportedKeys - update Restrict Failed - action = " + refrs.getShort(11) + " required value = " + DatabaseMetaData.importedKeyRestrict);
				else
					System.out.println("\ngetImportedKeys - update Restrict Passed");
			}
			else
					System.out.println("\ngetImportedKeys - update Restrict Failed no rows");

			refrs.close();
			refrs = getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {"", "APP", "REFACTNOACTION"}, null, null, null);

			if (refrs.next())
			{
				if (refrs.getShort(11) != DatabaseMetaData.importedKeyNoAction)
					System.out.println("\ngetImportedKeys - update NO ACTION Failed - action = " + refrs.getShort(11) + " required value = " + DatabaseMetaData.importedKeyNoAction);
				else
					System.out.println("\ngetImportedKeys - update NO ACTION Passed");
			}
			else
					System.out.println("\ngetImportedKeys - update NO ACTION Failed no rows");
			refrs.close();

			System.out.println("\ngetExportedKeys('',null,null,'','APP','REFACTION1' ):");
			dumpRS(GET_EXPORTED_KEYS, getMetaDataRS(met, GET_EXPORTED_KEYS,
				new String [] {"", "APP", "REFACTION1"},
				null, null, null));

			System.out.println("---------------------------------------");

			// drop referential action test tables
			s.execute("drop table refactnone");
			s.execute("drop table refactupdrestrict");
			s.execute("drop table refactupdnoaction");
			s.execute("drop table refactrestrict");
			s.execute("drop table refactnoaction");
			s.execute("drop table refactcascade");
			s.execute("drop table refactsetnull");
			s.execute("drop table refaction1");

			// test beetle 5195
			s.execute("create table t1 (c1 int not null, c2 int, c3 int default null, c4 char(10) not null, c5 char(10) default null, c6 char(10) default 'NULL', c7 int default 88)");

			String schema = "APP";
			String tableName = "T1";
			DatabaseMetaData dmd = con.getMetaData();

			System.out.println("getColumns for '" + tableName + "'");

			rs = getMetaDataRS(dmd, GET_COLUMNS,
				new String [] {null, schema, tableName, null},
				null, null, null);

			try
			{
				while (rs.next())
				{
					String col = rs.getString(4);
					String type = rs.getString(6);
					String defval = rs.getString(13);
					if (defval == null)
						System.out.println("  Next line is real null.");
					System.out.println("defval for col " + col + 
						" type " + type + " DEFAULT '" + defval + "' wasnull " + rs.wasNull());
				}
		
			}
			finally
			{
				if (rs != null)
					rs.close();
			}
			s.execute("drop table t1");

			// tiny test moved over from no longer used metadata2.sql
			// This checks for a bug where you get incorrect behavior on a nested connection.
			// if you do not get an error, the bug does not occur.			
                        s.execute("create procedure isReadO() "+
				"language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata.isro'" +
				" parameter style java"); 
			s.execute("call isReadO()");
			
			s.close();
			if (con.getAutoCommit() == false)
				con.commit();

			con.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}

		System.out.println("Test metadata finished");
    }

	static protected void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se.printStackTrace(System.out);
			se = se.getNextException();
		}
	}

	/**
	 * This method is responsible for executing a metadata query and returning
	 * the result set.  We do it like this so that the metadata.java and
	 * odbc_metadata.java classes can implement this method in their
	 * own ways (which is needed because we have to extra work to
	 * get the ODBC versions of the metadata).
	 */
	abstract protected ResultSet getMetaDataRS(DatabaseMetaData dmd, int procId,
		String [] sArgs, String [] argArray, int [] iArgs, boolean [] bArgs)
		throws SQLException;

	/**
	 * Dump the values in the received result set to output.
	 */
	protected void dumpRS(ResultSet rs) throws SQLException {
		dumpRS(IGNORE_PROC_ID, rs);
	}

	/**
	 * Dump the values in the received result set to output.
	 */
	abstract protected void dumpRS(int procId, ResultSet s) throws SQLException;

	/**
	 * Create a connect based on the test arguments passed in.
	 */
	protected Connection createConnection(String[] args) throws Exception {

		Connection con;

		// use the ij utility to read the property file and
		// make the initial connection.
		ij.getPropertyArg(args);
		con = ij.startJBMS();
		//con.setAutoCommit(true); // make sure it is true
		con.setAutoCommit(false);

		return con;

	}
}

