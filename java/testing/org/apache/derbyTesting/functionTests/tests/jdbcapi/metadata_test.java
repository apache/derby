/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata_test

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
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
import java.util.StringTokenizer;

import java.lang.reflect.Method;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

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
	
	//Used for JSR169
	private static boolean HAVE_DRIVER_CLASS;
	static{
		try{
			Class.forName("java.sql.Driver");
			HAVE_DRIVER_CLASS = true;
		}
		catch(ClassNotFoundException e){
			//Used for JSR169
			HAVE_DRIVER_CLASS = false;
		}
	}

	// We leave it up to the classes which extend this one to
	// initialize the following fields at construct time.
	public Connection con;
	public static Statement s;
	

	public void runTest() {

		DatabaseMetaData met;
		ResultSet rs;
		ResultSetMetaData rsmet;

		System.out.println("Test metadata starting");

		try
		{
			//Cleanup any leftover database objects from previous test run
			cleanUp(s);

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
			metadata_test.showNumericMetaData("Union Result", rsmet, 1);
			rs.close();

			rs = s.executeQuery("select dc, r, d, r+dc, d-dc, dc-d from t");
			rsmet = rs.getMetaData();
			metadata_test.showNumericMetaData("dec(10,2)", rsmet, 1);
			metadata_test.showNumericMetaData("real", rsmet, 2);
			metadata_test.showNumericMetaData("double", rsmet, 3);
			metadata_test.showNumericMetaData("real + dec(10,2)", rsmet, 4);
			metadata_test.showNumericMetaData("double precision - dec(10,2)", rsmet, 5);
			metadata_test.showNumericMetaData("dec(10,2) - double precision", rsmet, 6);

			while (rs.next())
				System.out.println("result row: " + rs.getString(1) + " "
						+ rs.getString(2) + " " + rs.getString(3)
						+ " " + rs.getString(4) + " " + rs.getString(5)
						+ " " + rs.getString(6)
						);
			rs.close();

			rsmet = s.executeQuery("VALUES CAST (0.0 AS DECIMAL(10,0))").getMetaData();
			metadata_test.showNumericMetaData("DECIMAL(10,0)", rsmet, 1);
			
			rsmet = s.executeQuery("VALUES CAST (0.0 AS DECIMAL(10,10))").getMetaData();
			metadata_test.showNumericMetaData("DECIMAL(10,10)", rsmet, 1);
			
			rsmet = s.executeQuery("VALUES CAST (0.0 AS DECIMAL(10,2))").getMetaData();
			metadata_test.showNumericMetaData("DECIMAL(10,2)", rsmet, 1);

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
							"floatcol float default 8.8, "+
							"decimalcol10p4s decimal(10,4), "+
							"numericcol20p2s numeric(20,2), "+
							"char8col___ char(8), "+
							"char8forbitcol___ char(8) for bit data, "+
							"varchar9col varchar(9), "+
							"varchar9bitcol varchar(9) for bit data, "+
							"longvarcharcol long varchar,"+
							"longvarbinarycol long varchar for bit data, "+
							//"nchar10col nchar(10)"
					  //+ ", nvarchar8col nvarchar(8)"
					  //+ ", longnvarchar long nvarchar"
					  //+ ", 
							"blobcol blob(3K), "+
							"clobcol clob(3K), "+
							"datecol date, "+
							"timecol time, "+
							"tscol timestamp"
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

						// Create functions so we can test
						// getFunctions()
						s.execute("CREATE FUNCTION DUMMY1 ( X SMALLINT ) "+
								  "RETURNS SMALLINT PARAMETER STYLE JAVA "+
								  "NO SQL LANGUAGE JAVA EXTERNAL "+
								  "NAME 'java.some.func'");
						s.execute("CREATE FUNCTION DUMMY2 ( X INTEGER, Y "+
								  "SMALLINT ) RETURNS INTEGER PARAMETER STYLE"+
								  " JAVA NO SQL LANGUAGE JAVA "+
								  "EXTERNAL NAME 'java.some.func'");
						s.execute("CREATE FUNCTION DUMMY3 ( X VARCHAR(16), "+
								  "Y INTEGER ) RETURNS VARCHAR(16) PARAMETER"+
								  " STYLE JAVA NO SQL LANGUAGE"+
								  " JAVA EXTERNAL NAME 'java.some.func'");
						s.execute("CREATE FUNCTION DUMMY4 ( X VARCHAR(128), "+
								  "Y INTEGER ) RETURNS INTEGER PARAMETER "+
								  "STYLE JAVA NO SQL LANGUAGE "+
								  "JAVA EXTERNAL NAME 'java.some.func'");
						
			met = con.getMetaData();

			System.out.println("JDBC Driver '" + met.getDriverName() +
							   "', version " + met.getDriverMajorVersion() +
							   "." + met.getDriverMinorVersion() +
							   " (" + met.getDriverVersion() + ")");


			System.out.println("getProcedures():");
			dumpRS(GET_PROCEDURES, getMetaDataRS(met, GET_PROCEDURES,
				new String [] {null, "%", "GETPCTEST%"},
				null, null, null));

			System.out.println("getUDTs() with user-named types null :");
 			dumpRS(met.getUDTs(null, null, null, null));

			System.out.println("getUDTs() with user-named types in ('JAVA_OBJECT') :");
 			int[] userNamedTypes = new int[1];
 			userNamedTypes[0] = java.sql.Types.JAVA_OBJECT;
 			dumpRS(met.getUDTs("a", null, null, userNamedTypes));      

 			System.out.println("getUDTs() with user-named types in ('STRUCT') :");
 			userNamedTypes[0] = java.sql.Types.STRUCT;
 			dumpRS(met.getUDTs("b", null, null, userNamedTypes));

 			System.out.println("getUDTs() with user-named types in ('DISTINCT') :");
 			userNamedTypes[0] = java.sql.Types.DISTINCT;
 			dumpRS(met.getUDTs("c", null, null, userNamedTypes));

			System.out.println("getUDTs() with user-named types in ('JAVA_OBJECT', 'STRUCT') :");
 			userNamedTypes = new int[2];
 			userNamedTypes[0] = java.sql.Types.JAVA_OBJECT;
 			userNamedTypes[1] = java.sql.Types.STRUCT;
 			dumpRS(met.getUDTs("a", null, null, userNamedTypes));

			testGetClientInfoProperties(met);

			/*
			 * any methods that were not tested above using code written
			 * specifically for it will now be tested in a generic way.
			 */


			System.out.println("supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE): " +
							   met.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));

			System.out.println("Test the metadata calls related to visibility of changes made by others for different resultset types");
			System.out.println("Since Derby materializes a forward only ResultSet incrementally, it is possible to see changes");
			System.out.println("made by others and hence following 3 metadata calls will return true for forward only ResultSets.");
			System.out.println("othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + met.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + met.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + met.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("Scroll insensitive ResultSet by their definition do not see changes made by others and hence following metadata calls return false");
			System.out.println("othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
			System.out.println("othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
			System.out.println("othersInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
			System.out.println("Derby does not yet implement scroll sensitive resultsets and hence following metadata calls return false");
			System.out.println("othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
			System.out.println("othersDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
			System.out.println("othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));

 			System.out.println("Test the metadata calls related to visibility of *own* changes for different resultset types");
 			System.out.println("ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + met.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
 			System.out.println("ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + met.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
 			System.out.println("ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + met.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
 			System.out.println("Scroll insensitive ResultSet see updates and deletes, but not inserts");
 			System.out.println("ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
 			System.out.println("ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
 			System.out.println("ownInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
 			System.out.println("Derby does not yet implement scroll sensitive resultsets and hence following metadata calls return false");
 			System.out.println("ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
 			System.out.println("ownDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
 			System.out.println("ownInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
 
 			System.out.println("Test the metadata calls related to detectability of visible changes for different resultset types");
 			System.out.println("Expect true for updates and deletes of TYPE_SCROLL_INSENSITIVE, all others should be false");
 			System.out.println("updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY)? " + met.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
 			System.out.println("deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY)? " + met.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
 			System.out.println("insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY)? " + met.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
 			System.out.println("updatesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.updatesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
 			System.out.println("deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
 			System.out.println("insertsAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.insertsAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
 			System.out.println("updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
 			System.out.println("deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
 			System.out.println("insertsAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.insertsAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
            
			if (!TestUtil.isJCCFramework()) { // gives false on all.. bug
				int[] types = {ResultSet.TYPE_FORWARD_ONLY, 
							   ResultSet.TYPE_SCROLL_INSENSITIVE,
							   ResultSet.TYPE_SCROLL_SENSITIVE};
	 
				int[] conc  = {ResultSet.CONCUR_READ_ONLY, 
							   ResultSet.CONCUR_UPDATABLE};

				String[] typesStr = {"TYPE_FORWARD_ONLY", 
									 "TYPE_SCROLL_INSENSITIVE",
									 "TYPE_SCROLL_SENSITIVE"};
			
				String[] concStr  = {"CONCUR_READ_ONLY", 
									 "CONCUR_UPDATABLE"};
	 
				for (int i = 0; i < types.length ; i++) {
					for (int j = 0; j < conc.length; j++) {
						System.out.println
							("SupportsResultSetConcurrency: " +
							 typesStr[i] + "," + concStr[j] + ": " +
							 met.supportsResultSetConcurrency(types[i], 
															  conc[j]));
					}
				}
			}

			System.out.println("getProcedureColumns():");
			dumpRS(GET_PROCEDURE_COLUMNS, getMetaDataRS(met, GET_PROCEDURE_COLUMNS,
				new String [] {null, "%", "GETPCTEST%", "%"},
				null, null, null));

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

			// DERBY-2610 - wildcards no longer accepted in getImportedKeys
			//      Prior to 2610, this query showed imported keys in
			//      all tables, but only REFTAB and REFTAB2 had any. 

			System.out.println("getImportedKeys('reftab'):");
			dumpRS(GET_IMPORTED_KEYS, getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {null, null, "REFTAB"},
				//PRE 2610: new String [] {null, null, "%"},
				null, null, null));

			System.out.println("getImportedKeys('reftab2'):");
			dumpRS(GET_IMPORTED_KEYS, getMetaDataRS(met, GET_IMPORTED_KEYS,
				new String [] {null, null, "REFTAB2"},
				//PRE 2610: new String [] {null, null, "%"},
				null, null, null));

			// DERBY-2610 - wildcards no longer accepted in getExportedKeys
			//      See getImportedKeys for change details. References in
			//      REFTAB are to REFTAB and LOUIE
			System.out.println("getExportedKeys(LOUIE):");
			dumpRS(GET_EXPORTED_KEYS, getMetaDataRS(met, GET_EXPORTED_KEYS,
				new String [] {null, null, "LOUIE"},
				//PRE 2610: new String [] {null, null, "%"},
				null, null, null));
			System.out.println("getExportedKeys(REFTAB):");
			dumpRS(GET_EXPORTED_KEYS, getMetaDataRS(met, GET_EXPORTED_KEYS,
				new String [] {null, null, "REFTAB"},
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

			// DERBY-2610 - wildcards no longer accepted in getImportedKeys
			//      Prior to 2610, this query showed crossrefs between all
			//      tables and reftab. Effectively, this was louie-reftab
			//      and reftab-reftab.
			System.out.println(
						"\ngetCrossReference('',null,'louie','','APP','reftab' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", null, "LOUIE", "", "APP", "REFTAB"},
				//PRE 2610: new String [] {"", null, "%", "", "APP", "REFTAB"},
				null, null, null));
			System.out.println(
						"\ngetCrossReference('',null,'reftab','','APP','reftab' ):");
			dumpRS(GET_CROSS_REFERENCE, getMetaDataRS(met, GET_CROSS_REFERENCE,
				new String [] {"", null, "REFTAB", "", "APP", "REFTAB"},
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
			s.execute("drop table inflight");
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

			// test DERBY-655, DERBY-1343
			// If a table has duplicate backing index, then it will share the 
			// physical conglomerate with the existing index, but the duplicate
			// indexes should have their own unique logical congomerates 
			// associated with them. That way, it will be possible to 
			// distinguish the 2 indexes in SYSCONGLOMERATES from each other.
			s.execute("CREATE TABLE Derby655t1(c11_ID BIGINT NOT NULL)");
			s.execute("CREATE TABLE Derby655t2 (c21_ID BIGINT NOT NULL primary key)");
			s.execute("ALTER TABLE Derby655t1 ADD CONSTRAINT F_12 Foreign Key (c11_ID) REFERENCES Derby655t2 (c21_ID) ON DELETE CASCADE ON UPDATE NO ACTION");
			s.execute("CREATE TABLE Derby655t3(c31_ID BIGINT NOT NULL primary key)");
			s.execute("ALTER TABLE Derby655t2 ADD CONSTRAINT F_443 Foreign Key (c21_ID) REFERENCES Derby655t3(c31_ID) ON DELETE CASCADE ON UPDATE NO ACTION");
			dmd = con.getMetaData();
			System.out.println("\ngetImportedKeys('',null,null,'','APP','Derby655t1' ):");
			dumpRS(met.getImportedKeys("", "APP", "DERBY655T1"));
			s.execute("drop table Derby655t1");
			s.execute("drop table Derby655t2");
			s.execute("drop table Derby655t3");

			// tiny test moved over from no longer used metadata2.sql
			// This checks for a bug where you get incorrect behavior on a nested connection.
			// if you do not get an error, the bug does not occur.			
            if(HAVE_DRIVER_CLASS){
            	s.execute("create procedure isReadO() language java external name " +
				"'org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata.isro'" +
				" parameter style java"); 
            	s.execute("call isReadO()");
            }
			cleanUp(s);
	
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

    /**
     * Run tests for <code>getClientInfoProperties()</code> introduced
     * by JDBC 4.0.
     *
     * @param dmd a <code>DatabaseMetaData</code> object
     */
    private void testGetClientInfoProperties(DatabaseMetaData dmd) {
        // not implemented in JCC
        if (TestUtil.isJCCFramework()) return;

        Method method = null;
        try {
            method = dmd.getClass().getMethod("getClientInfoProperties", null);
        } catch (NoSuchMethodException nsme) {}

        if (method == null || Modifier.isAbstract(method.getModifiers())) {
            System.out.println("DatabaseMetaData.getClientInfoProperties() " +
                               "is not available.");
            return;
        }

        System.out.println();
        System.out.println("getClientInfoProperties():");

        try {
            dumpRS((ResultSet) method.invoke(dmd, null));
        } catch (Exception e) {
            dumpAllExceptions(e);
        }
    }

	static private void showSQLExceptions (SQLException se) {
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): " + se.getMessage());
			se = se.getNextException();
		}
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
     * Print the entire exception chain.
     *
     * @param t a <code>Throwable</code>
     */
    private static void dumpAllExceptions(Throwable t) {
        System.out.println("FAIL -- unexpected exception");
        do {
            t.printStackTrace(System.out);
            if (t instanceof SQLException) {
                t = ((SQLException) t).getNextException();
            } else if (t instanceof InvocationTargetException) {
                t = ((InvocationTargetException) t).getTargetException();
            } else {
                break;
            }
        } while (t != null);
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

	protected void cleanUp(Statement stmt) throws SQLException {
		con.setAutoCommit(true);
		String[] testObjects = {"table t", "table t1", "view screwie", 
			"table reftab", "table reftab2", "table inflight" , "table alltypes", 
			"table louie",
			"procedure getpctest1", "procedure getpctest2",
			"procedure getpctest3a", "procedure getpctest3b",
			"procedure getpctest4a", "procedure getpctest4b", "procedure getpctest4bx",
			"procedure isreadO", "FUNCTION DUMMY1", "FUNCTION DUMMY2", 
			"FUNCTION DUMMY3", "FUNCTION DUMMY4" };
		TestUtil.cleanUpTest(stmt, testObjects);
	}
	
	/**
	 * Display the numeric JDBC metadata for a column
	 * @param expression Description of the expression
	 * @param rsmd thje meta data
	 * @param col which column
	 * @throws SQLException
	 */
	private static void showNumericMetaData(String expression,
			ResultSetMetaData rsmd, int col)
	   throws SQLException
   {
		System.out.print(expression);
		System.out.print(" --");
		
		System.out.print(" precision: ");
		System.out.print(rsmd.getPrecision(col));
		
		System.out.print(" scale: ");
		System.out.print(rsmd.getScale(col));
		
		System.out.print(" display size: ");
		System.out.print(rsmd.getColumnDisplaySize(col));

		System.out.print(" type name: ");
		System.out.print(rsmd.getColumnTypeName(col));
		
		System.out.println("");
		
   }
}

