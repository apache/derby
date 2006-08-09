/*

   Derby - Class org.apache.derbyTesting.functionTests.lang.updatableResultSet

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derbyTesting.functionTests.util.BigDecimalHandler;
import org.apache.derbyTesting.functionTests.util.TestUtil;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
  This tests JDBC 2.0 updateable resutlset - deleteRow, updateRow api
 */
public class updatableResultSet { 
	
	private static boolean HAVE_BIG_DECIMAL;
	
	static{
		if(BigDecimalHandler.representation != BigDecimalHandler.BIGDECIMAL_REPRESENTATION)
			HAVE_BIG_DECIMAL = false;
		else
			HAVE_BIG_DECIMAL = true;
	}

	private static Connection conn;
	private static DatabaseMetaData dbmt;
	private static Statement stmt, stmt1;
	private static ResultSet rs, rs1;
	private static PreparedStatement pStmt = null;
	private static CallableStatement callStmt = null;
	static SQLWarning warnings;

	//test all the supported SQL datatypes using updateXXX methods
	private static String[] allSQLTypes =
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
		"CHAR(2) FOR BIT DATA",
		"VARCHAR(2) FOR BIT DATA",
		"LONG VARCHAR FOR BIT DATA",
		"CLOB(1k)",
		"DATE",
		"TIME",
		"TIMESTAMP",
		"BLOB(1k)",

	};

	//names for column names to test all the supported SQL datatypes using updateXXX methods
	private static String[] ColumnNames =
	{
		"SMALLINTCOL",
		"INTEGERCOL",
		"BIGINTCOL",
		"DECIMALCOL",
		"REALCOL",
		"DOUBLECOL",
		"CHARCOL",
		"VARCHARCOL",
		"LONGVARCHARCOL",
		"CHARFORBITCOL",
		"VARCHARFORBITCOL",
		"LVARCHARFORBITCOL",
		"CLOBCOL",
		"DATECOL",
		"TIMECOL",
		"TIMESTAMPCOL",
		"BLOBCOL",

	};

	//data to test all the supported SQL datatypes using updateXXX methods
	private static String[][]SQLData =
	{
		{"11","22"},       // SMALLINT
		{"111","1111"},       // INTEGER
		{"22","222"},       // BIGINT
		{"3.3","3.33"},      // DECIMAL(10,5)
		{"4.4","4.44"},      // REAL,
		{"5.5","5.55"},      // DOUBLE
		{"'1992-01-06'","'1992'"},      // CHAR(60)
		{"'1992-01-07'","'1992'"},      //VARCHAR(60)",
		{"'1992-01-08'","'1992'"},      // LONG VARCHAR
		{"X'10'","X'10aa'"},  // CHAR(2)  FOR BIT DATA
		{"X'10'","X'10bb'"},  // VARCHAR(2) FOR BIT DATA
		{"X'10'","X'10cc'"},  //LONG VARCHAR FOR BIT DATA
		{"'13'","'14'"},     //CLOB(1k)
		{"'2000-01-01'","'2000-01-01'"},        // DATE
		{"'15:30:20'","'15:30:20'"},        // TIME
		{"'2000-01-01 15:30:20'","'2000-01-01 15:30:20'"},   // TIMESTAMP
		{"X'1020'","X'10203040'"}                 // BLOB
	};

	//used for printing useful messages about the test
	private static String[] allUpdateXXXNames =
	{
		"updateShort",
		"updateInt",
		"updateLong",
		"updateBigDecimal",
		"updateFloat",
		"updateDouble",
		"updateString",
		"updateAsciiStream",
		"updateCharacterStream",
		"updateByte",
		"updateBytes",
		"updateBinaryStream",
		"updateClob",
		"updateDate",
		"updateTime",
		"updateTimestamp",
		"updateBlob",
		"updateBoolean",
		"updateNull",
		"updateArray",
		"updateRef"

	};


	//I have constructed following table based on if combination of datatype and updateXXX method would work or not.
	public static final String[][]  updateXXXRulesTableForEmbedded = {

  // Types.             u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u
	//                    p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p
	//                    d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d
  //                    a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a
  //                    t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t
  //                    e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e
  //                    S  I  L  B  F  D  S  A  C  B  B  B  C  D  T  T  B  B  N  A  R
	//                    h  n  o  i  l  o  t  s  h  y  y  i  l  a  i  i  l  o  u  r  e
	//                    o  t  n  g  o  u  r  c  a  t  t  n  o  t  m  m  o  o  l  r  f
	//                    r     g  D  a  b  i  i  r  e  e  a  b  e  e  e  b  l  l  a
	//                    t        e  t  l  n  i  c     s  r           s     e     y
	//                             c     e  g  S  t        y           t     a
	//                             i           t  e        S           a     n
	//                             m           r  r        t           m
	//                             a           e  S        r           p
	//                             l           a  t        e
	//                                         m  r        a
	//                                            e        m
	//                                            a
	//                                            m
/* 0 SMALLINT */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 1 INTEGER  */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 2 BIGINT   */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 3 DECIMAL  */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 4 REAL     */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 5 DOUBLE   */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 6 CHAR     */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "PASS", "PASS", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 7 VARCHAR  */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "PASS", "PASS", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 8 LONGVARCHAR */     { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "PASS", "PASS", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 9 CHAR FOR BIT */    { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 10 VARCH. BIT   */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 11 LONGVAR. BIT */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 12 CLOB         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 13 DATE         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 14 TIME         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 15 TIMESTAMP    */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 16 BLOB         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "PASS", "ERROR", "ERROR" },

	};

	//I have constructed following table for network server based on if combination of datatype and updateXXX method would work or not.
	public static final String[][]  updateXXXRulesTableForNetworkServer = {

  // Types.             u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u  u
	//                    p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p  p
	//                    d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d  d
  //                    a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a  a
  //                    t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t  t
  //                    e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e  e
  //                    S  I  L  B  F  D  S  A  C  B  B  B  C  D  T  T  B  B  N  A  R
	//                    h  n  o  i  l  o  t  s  h  y  y  i  l  a  i  i  l  o  u  r  e
	//                    o  t  n  g  o  u  r  c  a  t  t  n  o  t  m  m  o  o  l  r  f
	//                    r     g  D  a  b  i  i  r  e  e  a  b  e  e  e  b  l  l  a
	//                    t        e  t  l  n  i  c     s  r           s     e     y
	//                             c     e  g  S  t        y           t     a
	//                             i           t  e        S           a     n
	//                             m           r  r        t           m
	//                             a           e  S        r           p
	//                             l           a  t        e
	//                                         m  r        a
	//                                            e        m
	//                                            a
	//                                            m
/* 0 SMALLINT */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 1 INTEGER  */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 2 BIGINT   */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 3 DECIMAL  */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 4 REAL     */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 5 DOUBLE   */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 6 CHAR     */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "PASS", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 7 VARCHAR  */        { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "PASS", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 8 LONGVARCHAR */     { "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "PASS", "ERROR", "PASS", "PASS", "ERROR", "ERROR" },
/* 9 CHAR FOR BIT */    { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 10 VARCH. BIT   */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 11 LONGVAR. BIT */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 12 CLOB         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 13 DATE         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 14 TIME         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 15 TIMESTAMP    */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "PASS", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },
/* 16 BLOB         */   { "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "PASS", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "PASS", "ERROR", "ERROR" },

	};

	public static void main(String[] args) {
		System.out.println("Start testing delete and update using JDBC2.0 updateable resultset apis");

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			setup(true);

			System.out.println("Negative Testl - request for scroll insensitive updatable resultset will give a read only scroll insensitive resultset");
			System.out.println("This test has been removed because scrollable " +
												 "insensitive updatable result sets have been " + 
												 "implemented.");

			System.out.println("Negative Test2 - request for scroll sensitive " + 
												 "updatable resultset will give an updatable " + 
												 "scroll insensitive resultset");
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			JDBCDisplayUtil.ShowWarnings(System.out, conn);
      System.out.println("requested TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE but that is not supported");
			System.out.println("Jira issue Derby-154 : When client connects to Network Server using JCC, it incorrectly shows support for scroll sensitive updatable resultsets");
      System.out.println("Make sure that we got TYPE_SCROLL_INSENSITIVE? " +  (stmt.getResultSetType() == ResultSet.TYPE_SCROLL_INSENSITIVE));
			System.out.println("Make sure that we got CONCUR_UPDATABLE? " +  
												 (stmt.getResultSetConcurrency() == 
													ResultSet.CONCUR_UPDATABLE));
			System.out.println("Rest of the test removed because scrollable " + 
												 "insensitive updatable result sets have been " + 
												 "implemented.");

			System.out.println("Negative Test3 - request a read only resultset and attempt deleteRow and updateRow on it");
			stmt = conn.createStatement();//the default is a read only forward only resultset
			rs = stmt.executeQuery("select * from t1");
			System.out.println("Make sure that we got CONCUR_READ_ONLY? " + (rs.getConcurrency() == ResultSet.CONCUR_READ_ONLY));
			rs.next();
      System.out.println("Now attempting to send a deleteRow on a read only resultset.");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because this is a read only resultset");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
      System.out.println("Now attempting to send an updateRow on a read only resultset.");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed because this is a read only resultset");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Negative Test4 - request a read only resultset and send a sql with FOR UPDATE clause and attempt deleteRow/updateRow on it");
			stmt = conn.createStatement();//the default is a read only forward only resultset
			rs = stmt.executeQuery("select * from t1 FOR UPDATE");
			System.out.println("Make sure that we got CONCUR_READ_ONLY? " + (rs.getConcurrency() == ResultSet.CONCUR_READ_ONLY));
			rs.next();
      System.out.println("Now attempting to send a deleteRow on a read only resultset with FOR UPDATE clause in the SELECT sql.");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because this is a read only resultset");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Now attempting to send a updateRow on a read only resultset with FOR UPDATE clause in the SELECT sql.");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed because this is a read only resultset");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Negative Test5 - request resultset with no FOR UPDATE clause and CONCUR_READ_ONLY");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery("select * from t1");//notice that we forgot to give mandatory FOR UPDATE clause for updatable resultset
			System.out.println("Make sure that we got CONCUR_READ_ONLY? " + (rs.getConcurrency() == ResultSet.CONCUR_READ_ONLY));
			
			System.out.println("Now attempting to send a delete on a sql with no FOR UPDATE clause and CONCUR_READ_ONLY.");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed on sql with no FOR UPDATE clause and CONCUR_READ_ONLY.");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Now attempting to send a updateRow on a sql with no FOR UPDATE clause and CONCUR_READ_ONLY.");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed on sql with no FOR UPDATE clause and CONCUR_READ_ONLY.");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Negative Test6 - request updatable resultset for sql with FOR READ ONLY clause");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("select * from t1 FOR READ ONLY");
			System.out.println("Make sure that we got CONCUR_READ_ONLY? " + (rs.getConcurrency() == ResultSet.CONCUR_READ_ONLY));
			System.out.println("Jira issue Derby-159 : Warnings raised by Derby are not getting passed to the Client in Network Server Mode");
			JDBCDisplayUtil.ShowWarnings(System.out, rs);
			rs.next();
      System.out.println("Now attempting to send a delete on a sql with FOR READ ONLY clause.");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed on sql with FOR READ ONLY clause");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Now attempting to send a updateRow on a sql with FOR READ ONLY clause.");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed on sql with FOR READ ONLY clause");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Negative Test7 - attempt to deleteRow & updateRow on updatable resultset when the resultset is not positioned on a row");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT * FROM t1");
			System.out.println("Make sure that we got CONCUR_UPDATABLE? " + (rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE));
      System.out.println("Now attempt a deleteRow without first doing next on the resultset.");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because resultset is not on a row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
      System.out.println("Now attempt a updateRow without first doing next on the resultset.");
			System.out.println("updateRow will check if it is on a row or not even " +
				"though no changes have been made to the row using updateXXX");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed because " +
						"resultset is not on a row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			while (rs.next());//read all the rows from the resultset and position after the last row
      System.out.println("ResultSet is positioned after the last row. attempt to deleteRow at this point should fail!");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because resultset is after the last row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("ResultSet is positioned after the last row. attempt to updateRow at this point should fail!");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed because resultset is after the last row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			rs.close();

			System.out.println("Negative Test8 - attempt deleteRow & updateRow on updatable resultset after closing the resultset");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			System.out.println("Make sure that we got CONCUR_UPDATABLE? " + (rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE));
			rs.next();
			rs.close();
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because resultset is closed");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed because resultset is closed");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}

			System.out.println("Negative Test9 - try updatable resultset on system table");
			try {
				rs = stmt.executeQuery("SELECT * FROM sys.systables FOR UPDATE");
				System.out.println("FAIL!!! trying to open an updatable resultset on a system table should have failed because system tables can't be updated by a user");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}

			System.out.println("Negative Test10 - try updatable resultset on a view");
			try {
				rs = stmt.executeQuery("SELECT * FROM v1 FOR UPDATE");
				System.out.println("FAIL!!! trying to open an updatable resultset on a view should have failed because Derby doesnot support updates to views yet");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			stmt.executeUpdate("drop view v1");

			System.out.println("Negative Test11 - attempt to open updatable resultset when there is join in the select query should fail");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			try {
				rs = stmt.executeQuery("SELECT c1 FROM t1,t2 where t1.c1 = t2.c21 FOR UPDATE");
				System.out.println("FAIL!!! trying to open an updatable resultset should have failed because updatable resultset donot support join in the select query");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}

			System.out.println("Negative Test12 - With autocommit on, attempt to drop a table when there is an open updatable resultset on it");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT c1 FROM t1 FOR UPDATE");
			rs.next();
			rs.updateInt(1,123);
			System.out.println("Opened an updatable resultset. Now trying to drop that table through another Statement");
			stmt1 = conn.createStatement();
			try {
				stmt1.executeUpdate("drop table t1");
				System.out.println("FAIL!!! drop table should have failed because the updatable resultset is still open");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Since autocommit is on, the drop table exception resulted in a runtime rollback causing updatable resultset object to close");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! resultset should have been closed at this point and updateRow should have failed");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! resultset should have been closed at this point and deleteRow should have failed");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}

			System.out.println("Negative Test13 - foreign key constraint failure will cause deleteRow to fail");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT * FROM tableWithPrimaryKey FOR UPDATE");
			rs.next();
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because it will cause foreign key constraint failure");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Since autocommit is on, the constraint exception resulted in a runtime rollback causing updatable resultset object to close");
			try {
				rs.next();
				if (TestUtil.isNetFramework())
					System.out.println("Jira entry Derby-160 : for Network Server because next should have failed");
				System.out.println("FAIL!!! next should have failed because foreign key constraint failure resulted in a runtime rollback");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}

			System.out.println("Negative Test14 - foreign key constraint failure will cause updateRow to fail");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT c1, c2 FROM tableWithPrimaryKey FOR UPDATE");
			rs.next();
			rs.updateInt(1,11);
			rs.updateInt(2,22);
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed because it will cause foreign key constraint failure");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Since autocommit is on, the constraint exception resulted in a runtime rollback causing updatable resultset object to close");
			try {
				rs.next();
				if (TestUtil.isNetFramework())
					System.out.println("Jira entry Derby-160 : for Network Server because next should have failed");
				System.out.println("FAIL!!! next should have failed because foreign key constraint failure resulted in a runtime rollback");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}

			System.out.println("Negative Test15 - Can't call updateXXX methods on columns that do not correspond to a column in the table");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
			rs.next();
			try {
				rs.updateInt(1,22);
				System.out.println("FAIL!!! updateInt should have failed because it is trying to update a column that does not correspond to column in base table");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}

			System.out.println("Negative Test16 - Call updateXXX method on out of the range column");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT c1, c2 FROM t1 FOR UPDATE");
			rs.next();
			System.out.println("There are only 2 columns in the select list and we are trying to send updateXXX on column position 3");
			try {
				rs.updateInt(3,22);
				System.out.println("FAIL!!! updateInt should have failed because there are only 2 columns in the select list");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}

			reloadData();

			System.out.println("Positive Test1a - request updatable resultset for forward only type resultset");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			JDBCDisplayUtil.ShowWarnings(System.out, conn);
      System.out.println("requested TYPE_FORWARD_ONLY, CONCUR_UPDATABLE");
      System.out.println("got TYPE_FORWARD_ONLY? " +  (stmt.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY));
      System.out.println("got CONCUR_UPDATABLE? " +  (stmt.getResultSetConcurrency() == ResultSet.CONCUR_UPDATABLE));
      rs = stmt.executeQuery("SELECT * FROM t1");
			System.out.println("JDBC 2.0 updatable resultset apis on this ResultSet object will pass because this is an updatable resultset");
			rs.next();
      System.out.println("column 1 on this row before deleteRow is " + rs.getInt(1));
      System.out.println("column 2 on this row before deleteRow is " + rs.getString(2));
			rs.deleteRow();
      System.out.println("Since after deleteRow(), in embedded mode and Network "+
				"Server mode using Derby Net Client, ResultSet is positioned before " +
				"the next row, getXXX will fail");
			try {
				System.out.println("column 1 on this deleted row is " + rs.getInt(1));
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
      System.out.println("calling deleteRow again w/o first positioning the ResultSet on the next row will fail");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because ResultSet is not positioned on a row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
      System.out.println("Position the ResultSet with next()");
			rs.next();
      System.out.println("Should be able to deletRow() on the current row now");
			rs.deleteRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test1b - request updatable resultset for forward only type resultset");
			reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			JDBCDisplayUtil.ShowWarnings(System.out, conn);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			System.out.println("column 1 on this row before updateInt is " + rs.getInt(1));
			rs.updateInt(1,234);
			System.out.println("column 1 on this row after updateInt is " + rs.getInt(1));
			System.out.println("column 2 on this row before updateString is " + rs.getString(2));
			System.out.println("now updateRow on the row");
			rs.updateRow();
      System.out.println("Since after updateRow(), in embedded mode and Network "+
				"Server mode using Derby Net Client, ResultSet is positioned before " +
				"the next row, getXXX will fail");
			try {
				System.out.println("column 1 on this updateRow row is " + rs.getInt(1));
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("calling updateRow again w/o first positioning the ResultSet on the next row will fail");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed because ResultSet is not positioned on a row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Position the ResultSet with next()");
			rs.next();
			System.out.println("Should be able to updateRow() on the current row now");
			rs.updateString(2,"234");
			rs.updateRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test1c - use updatable resultset to do postitioned delete");
			conn.setAutoCommit(false);
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			JDBCDisplayUtil.ShowWarnings(System.out, conn);
			
			System.out.println("requested TYPE_FORWARD_ONLY, CONCUR_UPDATABLE");
			System.out.println("got TYPE_FORWARD_ONLY? " +  (stmt.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("got CONCUR_UPDATABLE? " +  (stmt.getResultSetConcurrency() == ResultSet.CONCUR_UPDATABLE));
			
			rs = stmt.executeQuery("SELECT * FROM t1");
			rs.next();
			
			System.out.println("column 1 on this row before positioned delete " + rs.getInt(1));
			System.out.println("column 2 on this row before positioned delete " + rs.getString(2));
			
			pStmt = conn.prepareStatement("DELETE FROM T1 WHERE CURRENT OF " + rs.getCursorName());
			pStmt.executeUpdate();
			try {
			    System.out.println("column 1 on this deleted row is " + rs.getInt(1));
			    System.out.println("column 2 on this deleted row is " + rs.getString(2));
			} catch (SQLException e) {
			    System.out.println("SQL State : " + e.getSQLState());
			    System.out.println("Got expected exception " + e.getMessage());
			}
			
			System.out.println("doing positioned delete again w/o first positioning the ResultSet on the next row will fail");
			
			try {
			    pStmt.executeUpdate();
			    System.out.println("FAIL!!! positioned delete should have failed because ResultSet is not positioned on a row");
			} catch (SQLException e) {
			    System.out.println("SQL State : " + e.getSQLState());
			    System.out.println("Got expected exception " + e.getMessage());
			}
			
			System.out.println("Position the ResultSet with next()");
			
			rs.next();
			
			System.out.println("Should be able to do positioned delete on the current row now");
			
			pStmt.executeUpdate();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
			conn.rollback();
			
			System.out.println("Positive Test1d - updatable resultset to do positioned update");
			reloadData();
			
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			JDBCDisplayUtil.ShowWarnings(System.out, conn);
			
			rs = stmt.executeQuery("SELECT * FROM t1");
			rs.next();
			
			System.out.println("column 1 on this row before positioned update is " + rs.getInt(1));
			
			pStmt = conn.prepareStatement("UPDATE T1 SET C1=?,C2=? WHERE CURRENT OF " + rs.getCursorName());
			final int c1 = 2345;
			final String c2 = "UUU";
			
			pStmt.setInt(1, c1);
			pStmt.setString(2, c2); // current value
			System.out.println("now dow positioned update on the row");
			
			pStmt.executeUpdate();
			
			System.out.println("column 1 on the updated  row is " + rs.getInt(1));
			System.out.println("column 1 on the updated  row is " + rs.getString(2));
			
			try {
			    System.out.println("Refreshing the row");
			    rs.refreshRow(); // MAY FAIL HERE
			    
			    // If not, verify that it worked..
			    if (c1!=rs.getInt(1)) {
				System.out.println("FAIL!!! Expected column 1 to be update to " + c1);
			    }
			    if (!c2.equals(rs.getString(2))) {
				System.out.println("FAIL!!! Expected column 1 to be update to " + c2);
			    }
			} catch (SQLException e) {
			    System.out.println("SQL State : " + e.getSQLState());
			    System.out.println("Got expected exception " + e.getMessage());
			}
			
			System.out.println("doing positioned update again w/o positioning the RS will succeed");
			System.out.println("because the cursor is still positioned");
			
			pStmt.setInt(1, c1);
			pStmt.setString(2, c2); // current value
			pStmt.executeUpdate();
			
			System.out.println("Position the ResultSet with next()");
			
			rs.next();
			
			System.out.println("Should still be able to do positioned update");
			
			pStmt.setInt(1, rs.getInt(1)); // current value
			pStmt.setString(2, "abc");
			pStmt.executeUpdate();
			
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
			conn.rollback();
			
			conn.setAutoCommit(true);
			System.out.println("Positive Test2 - even if no columns from table " +
				"specified in the column list, we should be able to get updatable " +
				"resultset");
      reloadData();
			System.out.println("Will work in embedded mode because target table is "+
				"not derived from the columns in the select list");
			System.out.println("Will not work in network server mode because it " +
				"derives the target table from the columns in the select list");
      System.out.println("total number of rows in T1 ");
      dumpRS(stmt.executeQuery("select count(*) from t1"));
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
			rs.next();
      System.out.println("column 1 on this row is " + rs.getInt(1));
			try {
				rs.deleteRow();
				if (TestUtil.isNetFramework())
					System.out.println("FAIL!!! should have failed in network server");
				else
					System.out.println("PASS!!! passed in embedded mode");
			}
			catch (SQLException e) {
				if (TestUtil.isNetFramework()) {
					System.out.println("SQL State : " + e.getSQLState());
					System.out.println("Got expected exception " + e.getMessage());
				} else
					System.out.println("Got unexpected exception " + e.getMessage());
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
      System.out.println("total number of rows in T1 after one deleteRow is ");
      dumpRS(stmt.executeQuery("select count(*) from t1"));

			System.out.println("Positive Test3a - use prepared statement with concur updatable status to test deleteRow");
      reloadData();
			pStmt = conn.prepareStatement("select * from t1 where c1>?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      System.out.println("requested TYPE_FORWARD_ONLY, CONCUR_UPDATABLE");
      System.out.println("got TYPE_FORWARD_ONLY? " +  (pStmt.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY));
      System.out.println("got CONCUR_UPDATABLE? " +  (pStmt.getResultSetConcurrency() == ResultSet.CONCUR_UPDATABLE));
			pStmt.setInt(1,0);
      rs = pStmt.executeQuery();
			rs.next();
      System.out.println("column 1 on this row is " + rs.getInt(1));
			rs.deleteRow();
      System.out.println("Since after deleteRow(), ResultSet is positioned before the next row, getXXX will fail");
			try {
				System.out.println("column 1 on this deleted row is " + rs.getInt(1));
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
      System.out.println("calling deleteRow again w/o first positioning the ResultSet on the next row will fail");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because it can't be called more than once on the same row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
      System.out.println("Position the ResultSet with next()");
			rs.next();
                        //Derby-718 check that column values are not null after next()
                        if (rs.getInt(1) == 0) {
                            System.out.println("First column should not be 0");
                        }
                        // Derby-718
      System.out.println("Should be able to deletRow() on the current row now");
			rs.deleteRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test3b - use prepared statement with concur updatable status to test updateXXX");
			reloadData();
			pStmt = conn.prepareStatement("select * from t1 where c1>? for update", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("requested TYPE_FORWARD_ONLY, CONCUR_UPDATABLE");
			System.out.println("got TYPE_FORWARD_ONLY? " +  (pStmt.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("got CONCUR_UPDATABLE? " +  (pStmt.getResultSetConcurrency() == ResultSet.CONCUR_UPDATABLE));
			pStmt.setInt(1,0);
			rs = pStmt.executeQuery();
			rs.next();
			System.out.println("column 1 on this row is " + rs.getInt(1));
			rs.updateInt(1,5);
			System.out.println("column 1 on this row after updateInt is " + rs.getInt(1));
			rs.updateRow();
			System.out.println("Since after updateRow(), ResultSet is positioned before the next row, getXXX will fail");
			try {
				System.out.println("column 1 on this updated row is " + rs.getInt(1));
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("calling updateRow/updateXXX again w/o first positioning the ResultSet on the next row will fail");
			try {
				rs.updateInt(1,0);
				System.out.println("FAIL!!! updateXXX should have failed because resultset is not positioned on a row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed because resultset is not positioned on a row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			try {
				rs.cancelRowUpdates();
				System.out.println("FAIL!!! cancelRowUpdates should have failed because resultset is not positioned on a row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Position the ResultSet with next()");
			rs.next();
			System.out.println("Should be able to cancelRowUpdates() on the current row now");
			rs.cancelRowUpdates();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test4 - use callable statement with concur updatable status");
      reloadData();
			callStmt = conn.prepareCall("select * from t1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = callStmt.executeQuery();
      System.out.println("requested TYPE_FORWARD_ONLY, CONCUR_UPDATABLE");
      System.out.println("got TYPE_FORWARD_ONLY? " +  (callStmt.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY));
      System.out.println("got CONCUR_UPDATABLE? " +  (callStmt.getResultSetConcurrency() == ResultSet.CONCUR_UPDATABLE));
			rs.next();
      System.out.println("column 1 on this row is " + rs.getInt(1));
			rs.deleteRow();
      System.out.println("Since after deleteRow(), ResultSet is positioned before the next row, getXXX will fail");
			try {
				System.out.println("column 1 on this deleted row is " + rs.getInt(1));
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
      System.out.println("calling deleteRow again w/o first positioning the ResultSet on the next row will fail");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because it can't be called more than once on the same row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
      System.out.println("Position the ResultSet with next()");
			rs.next();
      System.out.println("Should be able to deletRow() on the current row now");
			rs.deleteRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test5 - donot have to select primary key to get an updatable resultset");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT c32 FROM t3");
			rs.next();
      System.out.println("column 1 on this row is " + rs.getInt(1));
      System.out.println("now try to delete row when primary key is not selected for that row");
			rs.deleteRow();
			rs.next();
			rs.updateLong(1,123);
			rs.updateRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test6a - For Forward Only resultsets, DatabaseMetaData will return false for ownDeletesAreVisible and deletesAreDetected");
			System.out.println("This is because, after deleteRow, we position the ResultSet before the next row. We don't make a hole for the deleted row and then stay on that deleted hole");
			dbmt = conn.getMetaData();
      System.out.println("ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + dbmt.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
      System.out.println("othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + dbmt.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
      System.out.println("deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY)? " + dbmt.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE of c1");
			rs.next();
      System.out.println("The JDBC program should look at rowDeleted only if deletesAreDetected returns true");
      System.out.println("Since Derby returns false for detlesAreDetected for FORWARD_ONLY updatable resultset,the program should not rely on rs.rowDeleted() for FORWARD_ONLY updatable resultsets");
      System.out.println("Have this call to rs.rowDeleted() just to make sure the method does always return false? " + rs.rowDeleted());
			rs.deleteRow();
			rs.close();

			System.out.println("Positive Test6b - For Forward Only resultsets, DatabaseMetaData will return false for ownUpdatesAreVisible and updatesAreDetected");
			System.out.println("This is because, after updateRow, we position the ResultSet before the next row");
			dbmt = conn.getMetaData();
			System.out.println("ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + dbmt.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + dbmt.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY)? " + dbmt.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
			reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE of c1");
			rs.next();
			System.out.println("The JDBC program should look at rowUpdated only if updatesAreDetected returns true");
			System.out.println("Since Derby returns false for updatesAreDetected for FORWARD_ONLY updatable resultset,the program should not rely on rs.rowUpdated() for FORWARD_ONLY updatable resultsets");
			System.out.println("Have this call to rs.rowUpdated() just to make sure the method does always return false? " + rs.rowUpdated());
			rs.updateLong(1,123);
			rs.updateRow();
			rs.close();

			System.out.println("Positive Test7a - delete using updatable resultset api from a temporary table");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(c21 int, c22 int) on commit preserve rows not logged");
			stmt.executeUpdate("insert into SESSION.t2 values(21, 1)");
			stmt.executeUpdate("insert into SESSION.t2 values(22, 1)");
			System.out.println("following rows in temp table before deleteRow");
			dumpRS(stmt.executeQuery("select * from SESSION.t2"));
			rs = stmt.executeQuery("select c21 from session.t2 for update");
			rs.next();
			rs.deleteRow();
			rs.next();
                        //Derby-718 check that column values are not null after next()
                        if (rs.getInt(1) == 0) {
                            System.out.println("Column c21 should not be 0");
                        }
                        // Derby-718
			rs.deleteRow();
			System.out.println("As expected, no rows in temp table after deleteRow");
			dumpRS(stmt.executeQuery("select * from SESSION.t2"));
			rs.close();
			stmt.executeUpdate("DROP TABLE SESSION.t2");

			System.out.println("Positive Test7b - update using updatable resultset api from a temporary table");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(c31 int, c32 int) on commit preserve rows not logged");
			stmt.executeUpdate("insert into SESSION.t3 values(21, 1)");
			stmt.executeUpdate("insert into SESSION.t3 values(22, 1)");
			System.out.println("following rows in temp table before deleteRow");
			dumpRS(stmt.executeQuery("select * from SESSION.t3"));
			rs = stmt.executeQuery("select c31 from session.t3");
			rs.next();
			rs.updateLong(1,123);
			rs.updateRow();
			rs.next();
			rs.updateLong(1,123);
			rs.updateRow();
			System.out.println("As expected, updated rows in temp table after updateRow");
			dumpRS(stmt.executeQuery("select * from SESSION.t3"));
			rs.close();
			stmt.executeUpdate("DROP TABLE SESSION.t3");

			System.out.println("Positive Test8a - change the name of the statement " +
				"when the resultset is open and see if deleteRow still works");
			System.out.println("This test works in embedded mode since Derby can " +
				"handle the change in the name of the statement with an open resultset");
			System.out.println("But it fails under Network Server mode because JCC " +
				"and Derby Net Client do not allow statement name change when there " +
				"an open resultset against it");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      System.out.println("change the cursor name(case sensitive name) with setCursorName and then try to deleteRow");
			stmt.setCursorName("CURSORNOUPDATe");//notice this name is case sensitive
      rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE of c1");
			rs.next();
			rs.deleteRow();
      System.out.println("change the cursor name one more time with setCursorName and then try to deleteRow");
			try {
				stmt.setCursorName("CURSORNOUPDATE1");
				rs.next();
				rs.deleteRow();
				if (TestUtil.isNetFramework())
					System.out.println("FAIL!!! should have failed in network server");
				else
					System.out.println("PASS!!! passed in embedded mode");
			}
			catch (SQLException e) {
				if (TestUtil.isNetFramework()) {
					System.out.println("SQL State : " + e.getSQLState());
					System.out.println("Got expected exception " + e.getMessage());
				} else
					System.out.println("Got unexpected exception " + e.getMessage());
			}
			rs.close();

			System.out.println("Positive Test8b - change the name of the statement " +
				"when the resultset is open and see if updateRow still works");
			System.out.println("This test works in embedded mode since Derby can " +
				"handle the change in the name of the statement with an open resultset");
			System.out.println("But it fails under Network Server mode because JCC " +
				"and Derby Net Client do not allow statement name change when there " +
				"an open resultset against it");
      reloadData();
			System.out.println("change the cursor name one more time with setCursorName and then try to updateRow");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("change the cursor name(case sensitive name) with setCursorName and then try to updateRow");
			stmt.setCursorName("CURSORNOUPDATe");//notice this name is case sensitive
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE of c1");
			rs.next();
			rs.updateLong(1,123);
			try {
				stmt.setCursorName("CURSORNOUPDATE1");
				rs.updateRow();
				if (TestUtil.isNetFramework())
					System.out.println("FAIL!!! should have failed in network server");
				else
					System.out.println("PASS!!! passed in embedded mode");
			}
			catch (SQLException e) {
				if (TestUtil.isNetFramework()) {
					System.out.println("SQL State : " + e.getSQLState());
					System.out.println("Got expected exception " + e.getMessage());
				} else
					System.out.println("Got unexpected exception " + e.getMessage());
			}
			rs.close();

			System.out.println("Positive Test9a - using correlation name for the " +
				"table in the select sql works in embedded mode and Network Server " +
				"using Derby Net Client driver");
			System.out.println("Correlation name for table does not work in Network "+
				"Server mode (using JCC) because the drivers construct the delete sql "+
				"with the correlation name rather than the base table name");
			reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT * FROM t1 abcde FOR UPDATE of c1");
			rs.next();
      System.out.println("column 1 on this row is " + rs.getInt(1));
      System.out.println("now try to deleteRow");
			try {
				rs.deleteRow();
				if (TestUtil.isJCCFramework())
					System.out.println("FAIL!!! should have failed in network server");
				else
					System.out.println("PASS!!! passed in embedded mode");
			}
			catch (SQLException e) {
				if (TestUtil.isJCCFramework()) {
					System.out.println("SQL State : " + e.getSQLState());
					System.out.println("Got expected exception " + e.getMessage());
				} else
					System.out.println("Got unexpected exception " + e.getMessage());
			}
			rs.close();

			System.out.println("Positive Test9b - using correlation name for " +
				"updatable columns is not allowed.");
			reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("Table t1 has following rows");
			dumpRS(stmt.executeQuery("select * from t1"));
			try {
				System.out.println("attempt to get an updatable resultset using correlation name for an updatable column");
				System.out.println("The sql is SELECT c1 as col1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
				rs = stmt.executeQuery("SELECT c1 as col1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
				System.out.println("FAIL!!! executeQuery should have failed");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("attempt to get an updatable resultset using correlation name for an readonly column. It should work");
			System.out.println("The sql is SELECT c1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
			rs = stmt.executeQuery("SELECT c1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
			rs.next();
			rs.updateInt(1,11);
			rs.updateRow();
			rs.close();
			System.out.println("Table t1 after updateRow has following rows");
			dumpRS(stmt.executeQuery("select * from t1"));

			System.out.println("Positive Test9c - try to updateXXX on a readonly column. Should get error");
			reloadData();
			rs = stmt.executeQuery("SELECT c1, c2 FROM t1 abcde FOR UPDATE of c1");
			rs.next();
			try {
				rs.updateString(2,"bbbb");
				System.out.println("FAIL!!! updateString on readonly column should have failed");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("attempt to get an updatable resultset using correlation name for an readonly column. It should work");
			System.out.println("The sql is SELECT c1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
			rs = stmt.executeQuery("SELECT c1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
			rs.next();
			rs.updateInt(1,11);
			rs.updateRow();
			rs.close();
			System.out.println("Table t1 after updateRow has following rows");
			dumpRS(stmt.executeQuery("select * from t1"));

			System.out.println("Positive Test9d - try to updateXXX on a readonly column with correlation name. Should get error");
			reloadData();
			rs = stmt.executeQuery("SELECT c1, c2 as col2 FROM t1 abcde FOR UPDATE of c1");
			rs.next();
			try {
				rs.updateString(2,"bbbb");
				System.out.println("FAIL!!! updateString on readonly column should have failed");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			rs.close();
			System.out.println("Table t1 has following rows");
			dumpRS(stmt.executeQuery("select * from t1"));

			System.out.println("Positive Test10 - 2 updatable resultsets going against the same table, will they conflict?");
			conn.setAutoCommit(false);
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt1 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1");
			rs.next();
			rs1 = stmt1.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs1.next();
			System.out.println("delete using first resultset");
			rs.deleteRow();
			try {
				System.out.println("attempt to send deleteRow on the same row through a different resultset should throw an exception");
				rs1.deleteRow();
				System.out.println("FAIL!!! delete using second resultset succedded? ");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Move to next row in the 2nd resultset and then delete using the second resultset");
			rs1.next();
			rs1.deleteRow();
			rs.close();
			rs1.close();
			conn.setAutoCommit(true);

			System.out.println("Positive Test11 - setting the fetch size to > 1 will be ignored by updatable resultset. Same as updatable cursors");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
			stmt.setFetchSize(200);
      rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE of c1");
			System.out.println("Notice the Fetch Size in run time statistics output.");
      showScanStatistics(rs, conn);
			System.out.println("statement's fetch size is " + stmt.getFetchSize());
			rs.close();
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");

			System.out.println("Positive Test12a - make sure delete trigger gets fired when deleteRow is issued");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("Verify that before delete trigger got fired, row count is 0 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			rs = stmt.executeQuery("SELECT * FROM table0WithTriggers FOR UPDATE");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getInt(1));
			System.out.println("now try to delete row and make sure that trigger got fired");
			rs.deleteRow();
			rs.close();
			System.out.println("Verify that delete trigger got fired by verifying the row count to be 1 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test12b - make sure update trigger gets fired when updateRow is issued");
			reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("Verify that before update trigger got fired, row count is 0 in updateTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from updateTriggerInsertIntoThisTable"));
			rs = stmt.executeQuery("SELECT * FROM table0WithTriggers");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getInt(1));
			System.out.println("now try to update row and make sure that trigger got fired");
			rs.updateLong(1,123);
			rs.updateRow();
			rs.close();
			System.out.println("Verify that update trigger got fired by verifying the row count to be 1 in updateTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from updateTriggerInsertIntoThisTable"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test13a - Another test case for delete trigger");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM table1WithTriggers FOR UPDATE");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getInt(1));
			System.out.println("this delete row will fire the delete trigger which will delete all the rows from the table and from the resultset");
			rs.deleteRow();
			rs.next();
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! there should have be no more rows in the resultset at this point because delete trigger deleted all the rows");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			rs.close();
			System.out.println("Verify that delete trigger got fired by verifying the row count to be 0 in table1WithTriggers");
			dumpRS(stmt.executeQuery("select count(*) from table1WithTriggers"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test13b - Another test case for update trigger");
			System.out.println("Look at the current contents of table2WithTriggers");
			dumpRS(stmt.executeQuery("select * from table2WithTriggers"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM table2WithTriggers where c1>1 FOR UPDATE");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getInt(1));
			System.out.println("this update row will fire the update trigger which will update all the rows in the table to have c1=1 and hence no more rows will qualify for the resultset");
			rs.updateLong(1,123);
			rs.updateRow();
			rs.next();
			try {
				rs.updateRow();
				System.out.println("FAIL!!! there should have be no more rows in the resultset at this point because update trigger made all the rows not qualify for the resultset");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			rs.close();
			System.out.println("Verify that update trigger got fired by verifying that all column c1s have value 1 in table2WithTriggers");
			dumpRS(stmt.executeQuery("select * from table2WithTriggers"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
			conn.rollback();

			System.out.println("Positive Test14a - make sure self referential delete cascade works when deleteRow is issued");
			dumpRS(stmt.executeQuery("select * from selfReferencingT1"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM selfReferencingT1");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getString(1));
			System.out.println("this delete row will cause the delete cascade constraint to delete all the rows from the table and from the resultset");
			rs.deleteRow();
			rs.next();
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! there should have be no more rows in the resultset at this point because delete cascade deleted all the rows");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			rs.close();
			System.out.println("Verify that delete trigger got fired by verifying the row count to be 0 in selfReferencingT1");
			dumpRS(stmt.executeQuery("select count(*) from selfReferencingT1"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test14b - make sure self referential update restrict works when updateRow is issued");
			dumpRS(stmt.executeQuery("select * from selfReferencingT2"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM selfReferencingT2 FOR UPDATE");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getString(1));
			System.out.println("update row should fail because cascade constraint is update restrict");
			rs.updateString(1,"e2");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! this update should have caused violation of foreign key constraint");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test15 - With autocommit off, attempt to drop a table when there is an open updatable resultset on it");
      reloadData();
      conn.setAutoCommit(false);
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1");
			rs.next();
			System.out.println("Opened an updatable resultset. Now trying to drop that table through another Statement");
			stmt1 = conn.createStatement();
			try {
				stmt1.executeUpdate("drop table t1");
				System.out.println("FAIL!!! drop table should have failed because the updatable resultset is still open");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Since autocommit is off, the drop table exception will NOT result in a runtime rollback and hence updatable resultset object is still open");
      rs.deleteRow();
			rs.close();
      conn.setAutoCommit(true);

			System.out.println("Positive Test16a - Do deleteRow within a transaction and then rollback the transaction");
      reloadData();
      conn.setAutoCommit(false);
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("Verify that before delete trigger got fired, row count is 0 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			System.out.println("Verify that before deleteRow, row count is 4 in table0WithTriggers");
			dumpRS(stmt.executeQuery("select count(*) from table0WithTriggers"));
			rs = stmt.executeQuery("SELECT * FROM table0WithTriggers FOR UPDATE");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getInt(1));
			System.out.println("now try to delete row and make sure that trigger got fired");
			rs.deleteRow();
			rs.close();
			System.out.println("Verify that delete trigger got fired by verifying the row count to be 1 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			System.out.println("Verify that deleteRow in transaction, row count is 3 in table0WithTriggers");
			dumpRS(stmt.executeQuery("select count(*) from table0WithTriggers"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
      conn.rollback();
			System.out.println("Verify that after rollback, row count is back to 0 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			System.out.println("Verify that after rollback, row count is back to 4 in table0WithTriggers");
			dumpRS(stmt.executeQuery("select count(*) from table0WithTriggers"));
      conn.setAutoCommit(true);

			System.out.println("Positive Test16b - Do updateRow within a transaction and then rollback the transaction");
			reloadData();
			conn.setAutoCommit(false);
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("Verify that before update trigger got fired, row count is 0 in updateTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from updateTriggerInsertIntoThisTable"));
			System.out.println("Look at the data in table0WithTriggers before trigger gets fired");
			dumpRS(stmt.executeQuery("select * from table0WithTriggers"));
			rs = stmt.executeQuery("SELECT * FROM table0WithTriggers");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getInt(1));
			System.out.println("now try to update row and make sure that trigger got fired");
			rs.updateLong(1,123);
			rs.updateRow();
			rs.close();
			System.out.println("Verify that update trigger got fired by verifying the row count to be 1 in updateTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from updateTriggerInsertIntoThisTable"));
			System.out.println("Verify that new data in table0WithTriggers");
			dumpRS(stmt.executeQuery("select * from table0WithTriggers"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
			conn.rollback();
			System.out.println("Verify that after rollback, row count is back to 0 in updateTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from updateTriggerInsertIntoThisTable"));
			System.out.println("Verify that after rollback, table0WithTriggers is back to its original contents");
			dumpRS(stmt.executeQuery("select * from table0WithTriggers"));
			conn.setAutoCommit(true);

			System.out.println("Positive Test17 - After deleteRow, resultset is positioned before the next row");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			rs.deleteRow();
      System.out.println("getXXX right after deleteRow will fail because resultset is not positioned on a row, instead it is right before the next row");
			try {
				System.out.println("column 1 (which is not nullable) after deleteRow is " + rs.getString(1));
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			rs.close();

			System.out.println("Positive Test18 - Test cancelRowUpdates method as the first updatable ResultSet api on a read-only resultset");
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT * FROM AllDataTypesForTestingTable");
			try {
				rs.cancelRowUpdates();
				System.out.println("Test failed - should not have reached here because cancelRowUpdates is being called on a read-only resultset");
			} catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			rs.close();

			System.out.println("Positive Test19 - Test updateRow method as the first updatable ResultSet api on a read-only resultset");
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT * FROM AllDataTypesForTestingTable");
			rs.next();
			try {
				rs.updateRow();
				System.out.println("Test failed - should not have reached here because updateRow is being called on a read-only resultset");
				return;
			} catch (Throwable e) {
				System.out.println("  Got expected exception : " + e.getMessage());
			}
			rs.close();

			System.out.println("Positive Test20 - Test updateXXX methods as the first updatable ResultSet api on a read-only resultset");
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			for (int updateXXXName = 1;  updateXXXName <= allUpdateXXXNames.length; updateXXXName++) {
				System.out.println("  Test " + allUpdateXXXNames[updateXXXName-1] + " on a readonly resultset");
				for (int indexOrName = 1; indexOrName <= 2; indexOrName++) {
					rs = stmt.executeQuery("SELECT * FROM AllDataTypesForTestingTable");
					rs.next();
					rs1 = stmt1.executeQuery("SELECT * FROM AllDataTypesNewValuesData");
					rs1.next();
					if (indexOrName == 1) //test by passing column position
						System.out.println("  Using column position as first parameter to " + allUpdateXXXNames[updateXXXName-1]);
					else
						System.out.println("  Using column name as first parameter to " + allUpdateXXXNames[updateXXXName-1]);
					try {
						if (updateXXXName == 1) {//update column with updateShort methods
								if (indexOrName == 1) //test by passing column position
									rs.updateShort(1, rs1.getShort(updateXXXName));
								else //test by passing column name
									rs.updateShort(ColumnNames[0], rs1.getShort(updateXXXName));
						} else if (updateXXXName == 2){ //update column with updateInt methods
								if (indexOrName == 1) //test by passing column position
									rs.updateInt(1, rs1.getInt(updateXXXName));
								else //test by passing column name
									rs.updateInt(ColumnNames[0], rs1.getInt(updateXXXName));
						} else if (updateXXXName ==  3){ //update column with updateLong methods
								if (indexOrName == 1) //test by passing column position
									rs.updateLong(1, rs1.getLong(updateXXXName));
								else //test by passing column name
									rs.updateLong(ColumnNames[0], rs1.getLong(updateXXXName));
						} else if (updateXXXName == 4){ //update column with updateBigDecimal methods
								if (indexOrName == 1) //test by passing column position
									BigDecimalHandler.updateBigDecimalString(rs, 1, 
											BigDecimalHandler.getBigDecimalString(rs1, updateXXXName));
								else //test by passing column name
									BigDecimalHandler.updateBigDecimalString(rs, ColumnNames[0], 
											BigDecimalHandler.getBigDecimalString(rs1, updateXXXName));
						} else if (updateXXXName == 5){ //update column with updateFloat methods
								if (indexOrName == 1) //test by passing column position
									rs.updateFloat(1, rs1.getFloat(updateXXXName));
								else //test by passing column name
									rs.updateFloat(ColumnNames[0], rs1.getFloat(updateXXXName));
						} else if (updateXXXName == 6){ //update column with updateDouble methods
								if (indexOrName == 1) //test by passing column position
									rs.updateDouble(1, rs1.getDouble(updateXXXName));
								else //test by passing column name
									rs.updateDouble(ColumnNames[0], rs1.getDouble(updateXXXName));
						} else if (updateXXXName == 7){ //update column with updateString methods
								if (indexOrName == 1) //test by passing column position
									rs.updateString(1, rs1.getString(updateXXXName));
								else //test by passing column name
									rs.updateString(ColumnNames[0], rs1.getString(updateXXXName));
						} else if (updateXXXName == 8){ //update column with updateAsciiStream methods
								if (indexOrName == 1) //test by passing column position
									rs.updateAsciiStream(1,rs1.getAsciiStream(updateXXXName), 4);
								else //test by passing column name
									rs.updateAsciiStream(ColumnNames[0],rs1.getAsciiStream(updateXXXName), 4);
						} else if (updateXXXName == 9){ //update column with updateCharacterStream methods
								if (indexOrName == 1) //test by passing column position
									rs.updateCharacterStream(1,rs1.getCharacterStream(updateXXXName), 4);
								else //test by passing column name
									rs.updateCharacterStream(ColumnNames[0],rs1.getCharacterStream(updateXXXName), 4);
						} else if (updateXXXName == 10){ //update column with updateByte methods
								if (indexOrName == 1) //test by passing column position
									rs.updateByte(1,rs1.getByte(1));
								else //test by passing column name
									rs.updateByte(ColumnNames[0],rs1.getByte(1));
						} else if (updateXXXName == 11){ //update column with updateBytes methods
								if (indexOrName == 1) //test by passing column position
									rs.updateBytes(1,rs1.getBytes(updateXXXName));
								else //test by passing column name
									rs.updateBytes(ColumnNames[0],rs1.getBytes(updateXXXName));
						} else if (updateXXXName == 12){ //update column with updateBinaryStream methods
								if (indexOrName == 1) //test by passing column position
									rs.updateBinaryStream(1,rs1.getBinaryStream(updateXXXName), 2);
								else //test by passing column name
									rs.updateBinaryStream(ColumnNames[0],rs1.getBinaryStream(updateXXXName), 2);
						} else if (updateXXXName == 13){ //update column with updateClob methods
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateClob(1,rs1.getClob(updateXXXName));
								else //test by passing column name
									rs.updateClob(ColumnNames[0],rs1.getClob(updateXXXName));
						} else if (updateXXXName == 14){ //update column with updateDate methods
								if (indexOrName == 1) //test by passing column position
									rs.updateDate(1,rs1.getDate(updateXXXName));
								else //test by passing column name
									rs.updateDate(ColumnNames[0],rs1.getDate(updateXXXName));
						} else if (updateXXXName == 15){ //update column with updateTime methods
								if (indexOrName == 1) //test by passing column position
									rs.updateTime(1,rs1.getTime(updateXXXName));
								else //test by passing column name
									rs.updateTime(ColumnNames[0],rs1.getTime(updateXXXName));
						} else if (updateXXXName == 16){ //update column with updateTimestamp methods
								if (indexOrName == 1) //test by passing column position
									rs.updateTimestamp(1,rs1.getTimestamp(updateXXXName));
								else //test by passing column name
									rs.updateTimestamp(ColumnNames[0],rs1.getTimestamp(updateXXXName));
						} else if (updateXXXName == 17){ //update column with updateBlob methods
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateBlob(1,rs1.getBlob(updateXXXName));
								else //test by passing column name
									rs.updateBlob(ColumnNames[0],rs1.getBlob(updateXXXName));
						} else if (updateXXXName == 18){ //update column with getBoolean methods
									//use SHORT sql type column's value for testing boolean since Derby don't support boolean datatype
									//Since Derby does not support Boolean datatype, this method is going to fail with the syntax error
								if (indexOrName == 1) //test by passing column position
									rs.updateBoolean(1, rs1.getBoolean(1));
								else //test by passing column name
									rs.updateBoolean(ColumnNames[0], rs1.getBoolean(1));
						} else if (updateXXXName == 19){ //update column with updateNull methods
								if (indexOrName == 1) //test by passing column position
									rs.updateNull(1);
								else //test by passing column name
									rs.updateNull(ColumnNames[0]);
						} else if (updateXXXName == 20){ //update column with updateArray methods - should get not implemented exception
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateArray(1, null);
								else //test by passing column name
									rs.updateArray(ColumnNames[0], null);
						} else if (updateXXXName == 21){ //update column with updateRef methods - should get not implemented exception
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateRef(1, null);
								else //test by passing column name
									rs.updateRef(ColumnNames[0], null);
						}
						System.out.println("Test failed - should not have reached here because updateXXX is being called on a read-only resultset");
						return;
					} catch (Throwable e) {
							System.out.println("  Got expected exception : " + e.getMessage());
					}
				}
			}
			conn.rollback();
			conn.setAutoCommit(true);

			System.out.println("Positive Test21 - Test all updateXXX(excluding updateObject) methods on all the supported sql datatypes");
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM AllDataTypesForTestingTable FOR UPDATE", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			PreparedStatement pstmt1 = conn.prepareStatement("SELECT * FROM AllDataTypesNewValuesData");
			for (int sqlType = 1, checkAgainstColumn = 1; sqlType <= allSQLTypes.length; sqlType++ ) {
				conn.rollback();
				System.out.println("Next datatype to test is " + allSQLTypes[sqlType-1]);
				for (int updateXXXName = 1;  updateXXXName <= allUpdateXXXNames.length; updateXXXName++) {
					checkAgainstColumn = updateXXXName;
					if(!HAVE_BIG_DECIMAL && (updateXXXName == 4))
						continue;
					System.out.println("  Testing " + allUpdateXXXNames[updateXXXName-1] + " on SQL type " + allSQLTypes[sqlType-1]);
					for (int indexOrName = 1; indexOrName <= 2; indexOrName++) {
						if (indexOrName == 1) //test by passing column position
							System.out.println("    Using column position as first parameter to " + allUpdateXXXNames[updateXXXName-1]);
						else
							System.out.println("    Using column name as first parameter to " + allUpdateXXXNames[updateXXXName-1]);
						rs = pstmt.executeQuery();
						rs.next();
						rs1 = pstmt1.executeQuery();
						rs1.next();
						try {
							if (updateXXXName == 1) {//update column with updateShort methods
								if (indexOrName == 1) //test by passing column position
									rs.updateShort(sqlType, rs1.getShort(updateXXXName));
								else //test by passing column name
									rs.updateShort(ColumnNames[sqlType-1], rs1.getShort(updateXXXName));
							} else if (updateXXXName == 2){ //update column with updateInt methods
								if (indexOrName == 1) //test by passing column position
									rs.updateInt(sqlType, rs1.getInt(updateXXXName));
								else //test by passing column name
									rs.updateInt(ColumnNames[sqlType-1], rs1.getInt(updateXXXName));
							} else if (updateXXXName ==  3){ //update column with updateLong methods
								if (indexOrName == 1) //test by passing column position
									rs.updateLong(sqlType, rs1.getLong(updateXXXName));
								else //test by passing column name
									rs.updateLong(ColumnNames[sqlType-1], rs1.getLong(updateXXXName));
							} else if (updateXXXName == 4){ //update column with updateBigDecimal methods
								if(HAVE_BIG_DECIMAL) {
									if (indexOrName == 1) //test by passing column position
										rs.updateBigDecimal(sqlType, rs1.getBigDecimal(updateXXXName));
									else //test by passing column name
										rs.updateBigDecimal(ColumnNames[sqlType-1], rs1.getBigDecimal(updateXXXName));
								}
							} else if (updateXXXName == 5){ //update column with updateFloat methods
								if (indexOrName == 1) //test by passing column position
									rs.updateFloat(sqlType, rs1.getFloat(updateXXXName));
								else //test by passing column name
									rs.updateFloat(ColumnNames[sqlType-1], rs1.getFloat(updateXXXName));
							} else if (updateXXXName == 6){ //update column with updateDouble methods
								if (indexOrName == 1) //test by passing column position
									rs.updateDouble(sqlType, rs1.getDouble(updateXXXName));
								else //test by passing column name
									rs.updateDouble(ColumnNames[sqlType-1], rs1.getDouble(updateXXXName));
							} else if (updateXXXName == 7){ //update column with updateString methods
								if (indexOrName == 1) //test by passing column position
									rs.updateString(sqlType, rs1.getString(updateXXXName));
								else //test by passing column name
									rs.updateString(ColumnNames[sqlType-1], rs1.getString(updateXXXName));
							} else if (updateXXXName == 8){ //update column with updateAsciiStream methods
								if (indexOrName == 1) //test by passing column position
									rs.updateAsciiStream(sqlType,rs1.getAsciiStream(updateXXXName), 4);
								else //test by passing column name
									rs.updateAsciiStream(ColumnNames[sqlType-1],rs1.getAsciiStream(updateXXXName), 4);
							} else if (updateXXXName == 9){ //update column with updateCharacterStream methods
								if (indexOrName == 1) //test by passing column position
									rs.updateCharacterStream(sqlType,rs1.getCharacterStream(updateXXXName), 4);
								else //test by passing column name
									rs.updateCharacterStream(ColumnNames[sqlType-1],rs1.getCharacterStream(updateXXXName), 4);
							} else if (updateXXXName == 10){ //update column with updateByte methods
									checkAgainstColumn = 1;
								if (indexOrName == 1) //test by passing column position
									rs.updateByte(sqlType,rs1.getByte(checkAgainstColumn));
								else //test by passing column name
									rs.updateByte(ColumnNames[sqlType-1],rs1.getByte(checkAgainstColumn));
							} else if (updateXXXName == 11){ //update column with updateBytes methods
								if (indexOrName == 1) //test by passing column position
									rs.updateBytes(sqlType,rs1.getBytes(updateXXXName));
								else //test by passing column name
									rs.updateBytes(ColumnNames[sqlType-1],rs1.getBytes(updateXXXName));
							} else if (updateXXXName == 12){ //update column with updateBinaryStream methods
								if (indexOrName == 1) //test by passing column position
									rs.updateBinaryStream(sqlType,rs1.getBinaryStream(updateXXXName), 2);
								else //test by passing column name
									rs.updateBinaryStream(ColumnNames[sqlType-1],rs1.getBinaryStream(updateXXXName), 2);
							} else if (updateXXXName == 13){ //update column with updateClob methods
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateClob(sqlType,rs1.getClob(updateXXXName));
								else //test by passing column name
									rs.updateClob(ColumnNames[sqlType-1],rs1.getClob(updateXXXName));
							} else if (updateXXXName == 14){ //update column with updateDate methods
								if (indexOrName == 1) //test by passing column position
									rs.updateDate(sqlType,rs1.getDate(updateXXXName));
								else //test by passing column name
									rs.updateDate(ColumnNames[sqlType-1],rs1.getDate(updateXXXName));
							} else if (updateXXXName == 15){ //update column with updateTime methods
								if (indexOrName == 1) //test by passing column position
									rs.updateTime(sqlType,rs1.getTime(updateXXXName));
								else //test by passing column name
									rs.updateTime(ColumnNames[sqlType-1],rs1.getTime(updateXXXName));
							} else if (updateXXXName == 16){ //update column with updateTimestamp methods
								if (indexOrName == 1) //test by passing column position
									rs.updateTimestamp(sqlType,rs1.getTimestamp(updateXXXName));
								else //test by passing column name
									rs.updateTimestamp(ColumnNames[sqlType-1],rs1.getTimestamp(updateXXXName));
							} else if (updateXXXName == 17){ //update column with updateBlob methods
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateBlob(sqlType,rs1.getBlob(updateXXXName));
								else //test by passing column name
									rs.updateBlob(ColumnNames[sqlType-1],rs1.getBlob(updateXXXName));
							} else if (updateXXXName == 18){ //update column with getBoolean methods
									//use SHORT sql type column's value for testing boolean since Derby don't support boolean datatype
									//Since Derby does not support Boolean datatype, this method is going to fail with the syntax error
								if (indexOrName == 1) //test by passing column position
									rs.updateBoolean(sqlType, rs1.getBoolean(1));
								else //test by passing column name
									rs.updateBoolean(ColumnNames[sqlType-1], rs1.getBoolean(1));
							} else if (updateXXXName == 19){ //update column with updateNull methods
								if (indexOrName == 1) //test by passing column position
									rs.updateNull(sqlType);
								else //test by passing column name
									rs.updateNull(ColumnNames[sqlType-1]);
							} else if (updateXXXName == 20){ //update column with updateArray methods - should get not implemented exception
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateArray(sqlType, null);
								else //test by passing column name
									rs.updateArray(ColumnNames[sqlType-1], null);
							} else if (updateXXXName == 21){ //update column with updateRef methods - should get not implemented exception
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateRef(sqlType, null);
								else //test by passing column name
									rs.updateRef(ColumnNames[sqlType-1], null);
              }
							rs.updateRow();
							if ((TestUtil.isNetFramework() && updateXXXRulesTableForNetworkServer[sqlType-1][updateXXXName-1].equals("ERROR")) ||
								(TestUtil.isEmbeddedFramework() && updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1].equals("ERROR"))) {
								System.out.println("FAILURE : We shouldn't reach here. The test should have failed earlier on updateXXX or updateRow call");
								return;
							}
							if (verifyData(sqlType,checkAgainstColumn, "AllDataTypesNewValuesData") == false)
							{
								System.out.println("Test failed");
								return;
							}
							resetData();
						} catch (Throwable e) {
							if ((TestUtil.isNetFramework() && updateXXXRulesTableForNetworkServer[sqlType-1][updateXXXName-1].equals("ERROR")) ||
								(TestUtil.isEmbeddedFramework() && updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1].equals("ERROR")))
								System.out.println("      Got expected exception : " + e.getMessage());
							else {
								if ((sqlType == 14 || sqlType == 15 || sqlType == 16) && //we are dealing with DATE/TIME/TIMESTAMP column types
									checkAgainstColumn == 7) //we are dealing with updateString. The failure is because string does not represent a valid datetime value
									System.out.println("      Got expected exception : " + e.getMessage());
								else {
									System.out.println("      Got UNexpected exception : " + e.getMessage());
									return;
								}
							}
						}
					}
					rs.close();
					rs1.close();
				}
			}
			conn.rollback();
			conn.setAutoCommit(true);

			System.out.println("Positive Test22 - Test updateObject method");
			conn.setAutoCommit(false);
			String displayString;
			for (int sqlType = 1; sqlType <= allSQLTypes.length; sqlType++ ) {
				conn.rollback();
				System.out.println("Next datatype to test is " + allSQLTypes[sqlType-1]);
				for (int updateXXXName = 1;  updateXXXName <= allUpdateXXXNames.length; updateXXXName++) {
					if(!HAVE_BIG_DECIMAL && (updateXXXName == 4))
						continue;
					for (int indexOrName = 1; indexOrName <= 2; indexOrName++) {
						if (indexOrName == 1) //test by passing column position
							displayString = "  updateObject with column position &";
						else
							displayString = "  updateObject with column name &";
						rs = pstmt.executeQuery();
						rs.next();
						rs1 = pstmt1.executeQuery();
						rs1.next();
						try {
							if (updateXXXName == 1){ //updateObject using Short object
								System.out.println(displayString + " Short object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, new Short(rs1.getShort(updateXXXName)));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], new Short(rs1.getShort(updateXXXName)));
							} else if (updateXXXName == 2){ //updateObject using Integer object
								System.out.println(displayString + " Integer object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, new Integer(rs1.getInt(updateXXXName)));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], new Integer(rs1.getInt(updateXXXName)));
							} else if (updateXXXName ==  3){ //updateObject using Long object
								System.out.println(displayString + " Long object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, new Long(rs1.getLong(updateXXXName)));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], new Long(rs1.getLong(updateXXXName)));
							} else if (updateXXXName == 4){ //updateObject using BigDecimal object
								if(HAVE_BIG_DECIMAL) {
									System.out.println(displayString + " BigDecimal object as parameters");
									if (indexOrName == 1) //test by passing column position
										rs.updateObject(sqlType, rs1.getBigDecimal(updateXXXName));
									else //test by passing column name
										rs.updateObject(ColumnNames[sqlType-1],rs1.getBigDecimal(updateXXXName));
								}
							} else if (updateXXXName == 5){ //updateObject using Float object
								System.out.println(displayString + " Float object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, new Float(rs1.getFloat(updateXXXName)));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], new Float(rs1.getFloat(updateXXXName)));
							} else if (updateXXXName == 6){ //updateObject using Double object
								System.out.println(displayString + " Double object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, new Double(rs1.getDouble(updateXXXName)));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], new Double(rs1.getDouble(updateXXXName)));
							} else if (updateXXXName == 7){ //updateObject using String object
								System.out.println(displayString + " String object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType,rs1.getString(updateXXXName));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1],rs1.getString(updateXXXName));
							} else if (updateXXXName == 8 || updateXXXName == 12) //updateObject does not accept InputStream and hence this is a no-op
									continue;
							else if (updateXXXName == 9) //updateObject does not accept Reader and hence this is a no-op
									continue;
							else if (updateXXXName == 10) //update column with updateByte methods
									//non-Object parameter(which is byte in this cas) can't be passed to updateObject mthod
									continue;
							else if (updateXXXName == 11){ //update column with updateBytes methods
								System.out.println(displayString + " bytes[] array as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType,rs1.getBytes(updateXXXName));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], rs1.getBytes(updateXXXName));
							} else if (updateXXXName == 13){ //update column with updateClob methods
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								System.out.println(displayString + " Clob object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, rs1.getClob(updateXXXName));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], rs1.getClob(updateXXXName));
							} else if (updateXXXName == 14){ //update column with updateDate methods
								System.out.println(displayString + " Date object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, rs1.getDate(updateXXXName));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], rs1.getDate(updateXXXName));
							} else if (updateXXXName == 15){ //update column with updateTime methods
								System.out.println(displayString + " Time object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, rs1.getTime(updateXXXName));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], rs1.getTime(updateXXXName));
							} else if (updateXXXName == 16){ //update column with updateTimestamp methods
								System.out.println(displayString + " TimeStamp object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, rs1.getTimestamp(updateXXXName));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], rs1.getTimestamp(updateXXXName));
							} else if (updateXXXName == 17){ //update column with updateBlob methods
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								System.out.println(displayString + " Blob object as parameters");
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, rs1.getBlob(updateXXXName));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], rs1.getBlob(updateXXXName));
							} else if (updateXXXName == 18) {//update column with getBoolean methods
								System.out.println(displayString + " Boolean object as parameters");
									//use SHORT sql type column's value for testing boolean since Derby don't support boolean datatype
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, new Boolean(rs1.getBoolean(1)));
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], new Boolean(rs1.getBoolean(1)));
							} else if (updateXXXName == 19){ //update column with updateNull methods
								System.out.println(displayString + " null as parameters");
								try {
								if (indexOrName == 1) //test by passing column position
									rs.updateObject(sqlType, null);
								else //test by passing column name
									rs.updateObject(ColumnNames[sqlType-1], null);
								} catch (Throwable e) {							
									System.out.println("   Got UNexpected exception:" + e.getMessage());
									return;
								}
							} else if (updateXXXName == 20 || updateXXXName == 21) //since Derby does not support Array, Ref datatype, this is a no-op
									continue;

								rs.updateRow();
								if (TestUtil.isNetFramework() && updateXXXName == 13 &&
									(sqlType==7 || sqlType==8 || sqlType==9 || sqlType==13)) 
								//updateObject with clob allowed on char, varchar, longvarchar & clob
									System.out.print("");
								else if (TestUtil.isNetFramework() && updateXXXName == 17 &&
									(sqlType==12 || sqlType==17)) 
								//updateObject with blob allowed on longvarchar for bit & blob
									System.out.print("");
								else if ((TestUtil.isNetFramework() && updateXXXRulesTableForNetworkServer[sqlType-1][updateXXXName-1].equals("ERROR")) ||
									(TestUtil.isEmbeddedFramework() && updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1].equals("ERROR"))) {
									System.out.println("FAILURE : We shouldn't reach here. The test should have failed earlier on updateXXX or updateRow call");
									return;
								}
								if(!HAVE_BIG_DECIMAL && (updateXXXName == 4))
									continue;
								if (verifyData(sqlType,updateXXXName, "AllDataTypesNewValuesData") == false)
								{
									System.out.println("Test failed");
									return;
								}
								resetData();
						} catch (Throwable e) {
							if ((TestUtil.isNetFramework() && updateXXXRulesTableForNetworkServer[sqlType-1][updateXXXName-1].equals("ERROR")) ||
								(TestUtil.isEmbeddedFramework() && updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1].equals("ERROR")))
									System.out.println("    Got expected exception : " + e.getMessage());
								else {
									if ((sqlType == 14 || sqlType == 15 || sqlType == 16) && //we are dealing with DATE/TIME/TIMESTAMP column types
									updateXXXName == 7) //we are dealing with updateString. The failure is because string does not represent a valid datetime value
									System.out.println("    Got expected exception : " + e.getMessage());
									else {
									System.out.println("    Got UNexpected exception : " + e.getMessage());
									return;}
								}
						}
						rs.close();
						rs1.close();
					}
				}
			}
			conn.rollback();
			conn.setAutoCommit(true);

			System.out.println("Positive Test23 - Test cancelRowUpdates after updateXXX methods on all the supported sql datatypes");
			conn.setAutoCommit(false);
			reloadAllDataTypesForTestingTableData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt1 = conn.createStatement();
			rs = stmt.executeQuery("SELECT * FROM AllDataTypesForTestingTable FOR UPDATE");
			rs.next();
			rs1 = stmt1.executeQuery("SELECT * FROM AllDataTypesNewValuesData");
			rs1.next();

			System.out.println("  updateShort and then cancelRowUpdates");
			short s = rs.getShort(1);
			rs.updateShort(1, rs1.getShort(1));
			if(rs.getShort(1) != rs1.getShort(1))
				return;
			rs.cancelRowUpdates();
			if(rs.getShort(1) != s)
				return;

			System.out.println("  updateInt and then cancelRowUpdates");
			int i = rs.getInt(2);
			rs.updateInt(2, rs1.getInt(2));
			if(rs.getInt(2) != rs1.getInt(2))
				return;
			rs.cancelRowUpdates();
			if(rs.getInt(2) != i)
				return;

			System.out.println("  updateLong and then cancelRowUpdates");
			long l = rs.getLong(3);
			rs.updateLong(3, rs1.getLong(3));
			if(rs.getLong(3) != rs1.getLong(3))
				return;
			rs.cancelRowUpdates();
			if(rs.getLong(3) != l)
				return;

			System.out.println("  updateBigDecimal and then cancelRowUpdates");
			String bdString = BigDecimalHandler.getBigDecimalString(rs, 4);
			BigDecimalHandler.updateBigDecimalString(rs, 4, 
					BigDecimalHandler.getBigDecimalString(rs1, 4));
			if(!BigDecimalHandler.getBigDecimalString(rs,4)
					.equals(BigDecimalHandler.getBigDecimalString(rs1, 4)))
				return;
			rs.cancelRowUpdates();
			if(!BigDecimalHandler.getBigDecimalString(rs, 4).equals(bdString))
				return;

			System.out.println("  updateFloat and then cancelRowUpdates");
			float f = rs.getFloat(5);
			rs.updateFloat(5, rs1.getFloat(5));
			if(rs.getFloat(5) != rs1.getFloat(5))
				return;
			rs.cancelRowUpdates();
			if(rs.getFloat(5) != f)
				return;

			System.out.println("  updateDouble and then cancelRowUpdates");
			double db = rs.getDouble(6);
			rs.updateDouble(6, rs1.getDouble(6));
			if(rs.getDouble(6) != rs1.getDouble(6))
				return;
			rs.cancelRowUpdates();
			if(rs.getDouble(6) != db)
				return;

			System.out.println("  updateString and then cancelRowUpdates");
			String str = rs.getString(7);
			rs.updateString(7, rs1.getString(7));
			if(!rs.getString(7).equals(rs1.getString(7)))
				return;
			rs.cancelRowUpdates();
			if(!rs.getString(7).equals(str))
				return;

			System.out.println("  updateAsciiStream and then cancelRowUpdates");
			str = rs.getString(8);
			rs.updateAsciiStream(8,rs1.getAsciiStream(8), 4);
			if(!rs.getString(8).equals(rs1.getString(8)))
				return;
			rs.cancelRowUpdates();
			if(!rs.getString(8).equals(str))
				return;

			System.out.println("  updateCharacterStream and then cancelRowUpdates");
			str = rs.getString(9);
			rs.updateCharacterStream(9,rs1.getCharacterStream(9), 4);
			if(!rs.getString(9).equals(rs1.getString(9)))
				return;
			rs.cancelRowUpdates();
			if(!rs.getString(9).equals(str))
				return;

			System.out.println("  updateByte and then cancelRowUpdates");
			s = rs.getShort(1);
			rs.updateByte(1,rs1.getByte(1));
			if(rs.getShort(1) != rs1.getShort(1))
				return;
			rs.cancelRowUpdates();
			if(rs.getShort(1) != s)
				return;

			System.out.println("  updateBytes and then cancelRowUpdates");
			byte[] bts = rs.getBytes(11);
			rs.updateBytes(11,rs1.getBytes(11));
			if (!(java.util.Arrays.equals(rs.getBytes(11),rs1.getBytes(11))))
				return;
			rs.cancelRowUpdates();
			if (!(java.util.Arrays.equals(rs.getBytes(11),bts)))
				return;

			System.out.println("  updateBinaryStream and then cancelRowUpdates");
			bts = rs.getBytes(12);
			rs.updateBinaryStream(12,rs1.getBinaryStream(12), 2);
			if (!(java.util.Arrays.equals(rs.getBytes(12),rs1.getBytes(12))))
				return;
			rs.cancelRowUpdates();
			if (!(java.util.Arrays.equals(rs.getBytes(12),bts)))
				return;

			System.out.println("  updateDate and then cancelRowUpdates");
			Date date = rs.getDate(14);
			rs.updateDate(14,rs1.getDate(14));
			if(rs.getDate(14).compareTo(rs1.getDate(14)) != 0)
				return;
			rs.cancelRowUpdates();
			if(rs.getDate(14).compareTo(date) != 0)
				return;

			System.out.println("  updateTime and then cancelRowUpdates");
			Time time = rs.getTime(15);
			rs.updateTime(15,rs1.getTime(15));
			if(rs.getTime(15).compareTo(rs1.getTime(15)) != 0)
				return;
			rs.cancelRowUpdates();
			if(rs.getTime(15).compareTo(time) != 0)
				return;

			System.out.println("  updateTimestamp and then cancelRowUpdates");
			Timestamp timeStamp = rs.getTimestamp(16);
			rs.updateTimestamp(16,rs1.getTimestamp(16));
			if(!rs.getTimestamp(16).toString().equals(rs1.getTimestamp(16).toString()))
				return;
			rs.cancelRowUpdates();
			if(!rs.getTimestamp(16).toString().equals(timeStamp.toString()))
				return;

			//Don't test this when running JDK1.3/in Network Server because they both
			//do not support updateClob and updateBlob
			if (JVMInfo.JDK_ID != 2 && TestUtil.isEmbeddedFramework()){ 
				System.out.println("  updateClob and then cancelRowUpdates");
				String clb1 = rs.getString(13);
				rs.updateClob(13,rs1.getClob(13));
				if(!rs.getString(13).equals(rs1.getString(13)))
					return;
				rs.cancelRowUpdates();
				if(!rs.getString(13).equals(clb1))
					return;
				System.out.println("  updateBlob and then cancelRowUpdates");
				bts = rs.getBytes(17);
				rs.updateBlob(17,rs1.getBlob(17));
				if (!(java.util.Arrays.equals(rs.getBytes(17),rs1.getBytes(17))))
					return;
				rs.cancelRowUpdates();
				if (!(java.util.Arrays.equals(rs.getBytes(17),bts)))
					return;
			}

			rs.close();
			rs1.close();
			conn.setAutoCommit(true);

			System.out.println("Positive Test24a - after updateXXX, try cancelRowUpdates and then deleteRow");
			reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			System.out.println("column 1 on this row before updateInt is " + rs.getInt(1));
			rs.updateInt(1,234);
			System.out.println("column 1 on this row after updateInt is " + rs.getInt(1));
			System.out.println("now cancelRowUpdates on the row");
			rs.cancelRowUpdates();
			System.out.println("Since after cancelRowUpdates(), ResultSet is positioned on the same row, getXXX will pass");
			System.out.println("column 1 on this row after cancelRowUpdates is " + rs.getInt(1));
			System.out.println("Since after cancelRowUpdates(), ResultSet is positioned on the same row, a deleteRow at this point will pass");
			try {
				rs.deleteRow();
				System.out.println("PASS : deleteRow passed as expected");
			}
			catch (SQLException e) {
				dumpSQLExceptions(e);
			}
			System.out.println("calling updateRow after deleteRow w/o first positioning the ResultSet on the next row will fail");
			try {
				rs.updateRow();
				System.out.println("FAIL!!! updateRow should have failed because ResultSet is not positioned on a row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			System.out.println("Position the ResultSet with next()");
			rs.next();
			System.out.println("Should be able to updateRow() on the current row now");
			rs.updateString(2,"234");
			rs.updateRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test25 - issue cancelRowUpdates without any updateXXX");
			reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1");
			rs.next();
			rs.cancelRowUpdates();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test26 - issue updateRow without any updateXXX will not move the resultset position");
			reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			rs.updateRow(); //this will not move the resultset to right before the next row because there were no updateXXX issued before updateRow
			rs.updateRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test27 - issue updateXXX and then deleteRow");
			reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1");
			rs.next();
			rs.updateInt(1,1234);
			rs.updateString(2,"aaaaa");
			rs.deleteRow();
			try {
				rs.updateRow();
				System.out.println("FAIL!!! deleteRow should have moved the ResultSet to right before the next row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			try {
				rs.updateInt(1,2345);
				System.out.println("FAIL!!! deleteRow should have moved the ResultSet to right before the next row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			try {
				rs.getInt(1);
				System.out.println("FAIL!!! deleteRow should have moved the ResultSet to right before the next row");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("Positive Test28 - issue updateXXXs and then move off the row, the changes should be ignored");
			reloadData();
			dumpRS(stmt.executeQuery("select * from t1"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			System.out.println("  column 1 on this row before updateInt is " + rs.getInt(1));
			System.out.println("  Issue updateInt to change the column's value to 2345");
			rs.updateInt(1,2345);
			System.out.println("  Move to next row w/o issuing updateRow");
			rs.next(); //the changes made on the earlier row should have be ignored because we moved off that row without issuing updateRow
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
			System.out.println("  Make sure that changes didn't make it to the database");
			dumpRS(stmt.executeQuery("select * from t1"));

			System.out.println("Positive Test29 - issue multiple updateXXXs and then a updateRow");
			reloadData();
			dumpRS(stmt.executeQuery("select * from t1"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			System.out.println("  column 1 on this row before updateInt is " + rs.getInt(1));
			System.out.println("  Issue updateInt to change the column's value to 2345");
			rs.updateInt(1,2345);
			System.out.println("  Issue another updateInt on the same row and column to change the column's value to 9999");
			rs.updateInt(1,9999);
			System.out.println("  Issue updateString to change the column's value to 'xxxxxxx'");
			rs.updateString(2,"xxxxxxx");
			System.out.println("  Now issue updateRow");
			rs.updateRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
			System.out.println("  Make sure that changes made it to the database correctly");
			dumpRS(stmt.executeQuery("select * from t1"));

			System.out.println("Positive Test30 - call updateXXX methods on only columns that correspond to a column in the table");
			dumpRS(stmt.executeQuery("select * from t1"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT 1, 2, c1, c2 FROM t1");
			rs.next();
			rs.updateInt(3,22);
			rs.updateRow();
			rs.close();
			System.out.println("  Make sure that changes made it to the database correctly");
			dumpRS(stmt.executeQuery("select * from t1"));

			System.out.println("Positive Test31a - case sensitive table and column names");
			stmt.executeUpdate("create table \"t1\" (\"c11\" int, c12 int)");
			stmt.executeUpdate("insert into \"t1\" values(1, 2), (2,3)");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT \"c11\", \"C12\" FROM \"t1\" FOR UPDATE");
			rs.next();
			rs.updateInt(1,11);
			rs.updateInt(2,22);
			rs.updateRow();
			rs.next();
			rs.deleteRow();
			rs.close();
			System.out.println("  Make sure that changes made it to the database correctly");
			dumpRS(stmt.executeQuery("select * from \"t1\""));

			System.out.println("Positive Test31b - table and column names with spaces in middle and end");
			stmt.executeUpdate("create table \" t 11 \" (\" c 111 \" int, c112 int)");
			stmt.executeUpdate("insert into \" t 11 \" values(1, 2), (2,3)");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT \" c 111 \", \"C112\" FROM \" t 11 \" ");
			rs.next();
			rs.updateInt(1,11);
			rs.updateInt(2,22);
			rs.updateRow();
			rs.next();
			rs.deleteRow();
			rs.close();
			System.out.println("  Make sure for table \" t 11 \" that changes made it to the database correctly");
			dumpRS(stmt.executeQuery("select * from \" t 11 \""));

			System.out.println("Positive Test32 - call updateXXX methods on column that is not in for update columns list");
			dumpRS(stmt.executeQuery("select * from t1"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT c1, c2 FROM t1 FOR UPDATE of c1");
			rs.next();
			try {
				rs.updateInt(2,22);
				if (TestUtil.isEmbeddedFramework())
				System.out.println("PASS!!! Embedded throws exception for updateRow");
				else
				System.out.println("FAIL!!! Network Server should throw exception for updateXXX");
			}
			catch (SQLException e) {
				System.out.println("SQL State : " + e.getSQLState());
				System.out.println("Got expected exception " + e.getMessage());
			}
			try {
				rs.updateRow();
				System.out.println("updateRow passed");
			}
			catch (SQLException e) {
				if (TestUtil.isNetFramework())
				System.out.println("FAIL!!! updateRow w/o updateXXX is no-op in Network Server");
				else
				System.out.println("FAIL!!! exception is " + e.getMessage());
			}
			rs.close();
			System.out.println("  Make sure the contents of table are unchanged");
			dumpRS(stmt.executeQuery("select * from t1"));

			System.out.println("Positive Test33 - try to update a table from another schema");
			System.out.println("  contents of table t1 from current schema");
			dumpRS(stmt.executeQuery("select * from t1"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt.executeUpdate("create schema s2");
			stmt.executeUpdate("create table s2.t1 (c1s2t1 int, c2s2t1 smallint, c3s2t2 double)");
			stmt.executeUpdate("insert into s2.t1 values(1,2,2.2),(1,3,3.3)");
			System.out.println("  contents of table t1 from schema s2");
			dumpRS(stmt.executeQuery("select * from s2.t1"));
			System.out.println("  Try to change contents of 2nd column of s2.t1 using updateRow");
			rs = stmt.executeQuery("SELECT * FROM s2.t1 FOR UPDATE");
			rs.next();
			rs.updateInt(2,1);
			rs.updateRow();
			rs.next();
			rs.updateInt(2,1);
			rs.updateRow();
			rs.close();
			System.out.println("  Make sure that changes made to the right table t1");
			System.out.println("  contents of table t1 from current schema should have remained unchanged");
			dumpRS(stmt.executeQuery("select * from t1"));
			System.out.println("  contents of table t1 from schema s2 should have changed");
			dumpRS(stmt.executeQuery("select * from s2.t1"));

			System.out.println("Positive Test34 - in autocommit mode, check that updateRow and deleteRow does not commit");
			conn.setAutoCommit(true);

			// First try deleteRow and updateRow on *first* row of result set
			reloadData();
			System.out.println("  Contents before changes to first row in RS:");
			dumpRS(stmt.executeQuery("select * from t1"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			rs.deleteRow(); 
			conn.rollback();
			rs.close();
			System.out.println("  Make sure the contents of table are unchanged:");
			dumpRS(stmt.executeQuery("select * from t1"));			
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			rs.updateInt(1,-rs.getInt(1));
			rs.updateRow();
			conn.rollback();
			rs.close();
			System.out.println("  Make sure the contents of table are unchanged:");
			dumpRS(stmt.executeQuery("select * from t1"));			

			// Now try the same on the *last* row in the result set
			reloadData();
			stmt = conn.createStatement();
		        rs = stmt.executeQuery("SELECT COUNT(*) FROM t1");
			rs.next();
			int count = rs.getInt(1);
			rs.close();
			
			System.out.println("  Contents before changes to last row in RS:");
			dumpRS(stmt.executeQuery("select * from t1"));
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			for (int j = 0; j < count; j++) {
			   rs.next();
			}
			rs.deleteRow(); 
			conn.rollback();
			rs.close();
			System.out.println("  Make sure the contents of table are unchanged:");
			dumpRS(stmt.executeQuery("select * from t1"));			
			
			stmt = conn.createStatement();
		        rs = stmt.executeQuery("SELECT COUNT(*) FROM t1");
			rs.next();
			count = rs.getInt(1);
			rs.close();

			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			for (int j = 0; j < count; j++) {
			   rs.next();
			}
			rs.updateInt(1,-rs.getInt(1));
			rs.updateRow();
			conn.rollback();
			rs.close();
			System.out.println("  Make sure the contents of table are unchanged:");
			dumpRS(stmt.executeQuery("select * from t1"));	

			stmt.close();
			reloadData();
            
			// Tests for insert Row
			int c41, c42, c41old, c42old;
			conn.setAutoCommit(false);
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			
			System.out.println("Positive Test 35 - moveToInsertRow, insertRow," +
					"getXXX and moveToCurrentRow");
			rs = stmt.executeQuery("SELECT * FROM t4");
			rs.next();
			// Get values before insertRow for test36
			c41old = rs.getInt(1);
			c42old = rs.getInt(2);
			System.out.println("Positive Test 35.a - moveToInsertRow");
			rs.moveToInsertRow();
			rs.updateInt(1, 4);
			rs.updateInt(2, 4);
			System.out.println("Positive Test 35.b - insertRow");
			try {
				rs.insertRow();
			} catch (Throwable t) {
				System.out.println("Error " + t.getMessage());
			}
			System.out.println("Positive Test 35.c - check that getXXX gets the " +
					"correct values after insertRow");
			c41 = rs.getInt(1);
			c42 = rs.getInt(2);
			if ((c41 != 4) || (c42 != 4)) {
				System.out.println("getXXX failed after insertRow");
			}
			System.out.println("Positive Test 35.d - moveToCurrentRow");
			rs.moveToCurrentRow();
			System.out.println("Positive Test 35.e - check that getXXX gets the " +
					"correct values after moveToCurrentRow");
			if (c41old != rs.getInt(1) || c42old != rs.getInt(2)) {
				System.out.println("rs positioned on wrong row after moveToCurrentRow");
			}

			System.out.println("Positive test 36 - call moveToCurrentRow from current row");
			rs.moveToCurrentRow();
			if (c41old != rs.getInt(1) || c42old != rs.getInt(2)) {
				System.out.println("rs positioned on wrong row after second moveToCurrentRow");
			}			

			System.out.println("Positive test 37 - several moveToInsertRow");
			System.out.println("Positive test 37.a - check that getXXX gets the " +
					"correct values after moveToInsertRow");
			rs.moveToInsertRow();
			rs.updateInt(1, 5);
			rs.updateInt(2, 4);
			c41 = rs.getInt(1);
			c42 = rs.getInt(2);
			if (c41 != 5 || c42 != 4) {
				System.out.println("Got wrong value for columns");
			}			
			System.out.println("Positive test 37.b - moveToinsertRow from " +
					"insertRow");
			rs.moveToInsertRow();
			System.out.println("Positive test 37.c - check that getXXX gets " +
					"undefined values when updateXXX has not been called yet " +
					"on insertRow");
			c41 = rs.getInt(1);
			if (!rs.wasNull() || c41 != 0) {
				System.out.println("c41 should have been set to NULL after second " +
					"moveToInsertRow");
			}
			c42 = rs.getInt(2);
			if (!rs.wasNull() || c42 != 0) {
				System.out.println("c42 should have been set to NULL after second " +
					"moveToInsertRow");
			}

			System.out.println("Negative Test 38 - insertRow: do not set a value " +
				"to all not nullable columns");
			rs.moveToInsertRow();
			// Do not update column1
			rs.updateInt(2, 5);
			try {
				rs.insertRow();
				System.out.println("Should not have gotten here");
			} catch (SQLException se) {
				dumpExpectedSQLException(se);
			}

			System.out.println("Negative Test 39 - run updateRow and deleterow " +
				"when positioned at insertRow");
			rs.moveToInsertRow();
			rs.updateInt(1, 6);
			rs.updateInt(2, 6);
			try {
				System.out.println("Negative Test 39.a - run updateRow on " +
						"insertRow");
				rs.updateRow();
				System.out.println("Never get here, updateRow not allowed from insertRow");
			} catch (SQLException se) {
				dumpExpectedSQLException(se);
			}
			try {
				System.out.println("Negative Test 39.a - run deleteRow on " +
						"insertRow");
				rs.deleteRow();
				System.out.println("Never get here, deleteRow not allowed from insertRow");
			} catch (SQLException se) {
				dumpExpectedSQLException(se);
			}
			
			System.out.println("Negative test 40 - Try to insert row from currentRow");
			rs.moveToCurrentRow();
			try {
				rs.insertRow();
				System.out.println("Should not get here, insertRow should fail " +
						"when cursor is not positioned on InsertRow.");
			} catch (SQLException se) {
				dumpExpectedSQLException(se);
			}
			
			System.out.println("Positive test 41 - try to insertRow from all " +
					"posible positions");
			rs = stmt.executeQuery("SELECT * FROM t4 WHERE c41 <= 5");
			rs.moveToInsertRow();
			rs.updateInt(1, 1000);
			rs.updateInt(2, 1000);
			rs.insertRow();	
			while (rs.next()) {
				c41 = rs.getInt(1);
				c42 = rs.getInt(2);
				rs.moveToInsertRow();
				rs.updateInt(1, c41 + 100);
				rs.updateInt(2, c42 + 100);
				rs.insertRow();
			}
			rs.moveToInsertRow();
			rs.updateInt(1, 2000);
			rs.updateInt(2, 2000);
			rs.insertRow();
			
			System.out.println("Positive test 42 - InsertRow leaving a nullable " +
					"columns = NULL");
			rs.moveToInsertRow();
			rs.updateInt(1, 7);
			rs.insertRow();
			
			rs.close();
			stmt.close();
			
			System.out.println("Positive and negative tests 43 - Commit while on insertRow");
			try {
				conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			} catch (Throwable e) {
				if (JVMInfo.JDK_ID <= 2) {
					System.out.println("This exception is expected with jdk 1.3: " +
							"holdability not supported with jdk131 /ibm131 based jvms");

				} else {
					System.out.println("Got unexpected exception: " + e.getMessage());
				}
			}
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("Positive test 43 - Commit while on insertRow " +
					"with holdable cursor");
			rs = stmt.executeQuery("SELECT * FROM t4");
			rs.next();
			rs.moveToInsertRow();
			rs.updateInt(1, 8);
			rs.updateInt(2, 8);
			conn.commit();
			try {
				rs.insertRow();
			} catch (SQLException se){ 
				dumpSQLExceptions(se);
			}
			rs.close();
			stmt.close();
			try {
				conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
			} catch (Throwable e) {
				if (JVMInfo.JDK_ID <= 2) {
					System.out.println("This exception is expected with jdk 1.3: " +
							"holdability not supported with jdk131 /ibm131 based jvms");
				} else {
					System.out.println("Got unexpected exception: " + e.getMessage());
				}
			}
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("Negative test 43 - Commit while on insertRow " +
					"with not holdable cursor");
			rs = stmt.executeQuery("SELECT * FROM t4");
			rs.next();
			rs.moveToInsertRow();
			rs.updateInt(1, 82);
			rs.updateInt(2, 82);
			conn.commit();
			try {
				rs.insertRow();
			} catch (SQLException se){ 
				dumpExpectedSQLException(se);
			}
			rs.close();
			
			System.out.println("Negative test 44 - Closed RS");
			rs = stmt.executeQuery("SELECT * FROM t4");
			rs.next();
			rs.moveToInsertRow();
			rs.updateInt(1, 9);
			rs.updateInt(2, 9);
			rs.close();
			System.out.println("Negative test 44.a - try insertRow on closed RS");
			try {
				rs.insertRow();
				System.out.println("FAIL: insertRow can not be called on " +
						"closed RS");
			} catch (SQLException se){ 
				dumpExpectedSQLException(se);
			}
			System.out.println("Negative test 44.b - try moveToCurrentRow on " +
					"closed RS");
			try {
				rs.moveToCurrentRow();
				System.out.println("FAIL: moveToCurrentRow can not be called on " +
						"closed RS");
			} catch (SQLException se){ 
				dumpExpectedSQLException(se);
			}
			System.out.println("Negative test 44.c - try moveToInsertRow on " +
					"closed RS");
			try {
				rs.moveToInsertRow();
				System.out.println("FAIL: moveToInsertRow can not be called on " +
						"closed RS");
			} catch (SQLException se){ 
				dumpExpectedSQLException(se);
			}
			
			System.out.println("Positive test 45 - try to insert without " +
					"updating all columns. All columns allow nulls or have a " +
					"default value");
			rs = stmt.executeQuery("SELECT * FROM t5");
			rs.next();
			rs.moveToInsertRow();
			try {
				// Should insert a row with NULLS and DEFAULT VALUES
				rs.insertRow();
			} catch (SQLException se){ 
				dumpSQLExceptions(se);
			}
			
			rs.close();
			conn.commit();
			
			System.out.println("Positive test 46 - Rollback with AutoCommit on");
			conn.setAutoCommit(true);
			rs = stmt.executeQuery("SELECT * FROM t4");
			rs.next();
			rs.moveToInsertRow();
			rs.updateInt(1, 4000);
			rs.updateInt(2, 4000);
			rs.insertRow();
			conn.rollback();
			stmt.close();

			System.out.println("Negative test 47 - insertRow and read-only RS");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
					ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery("SELECT * FROM t4");
			System.out.println("Negative test 47.a - try moveToInsertRow on " +
					"read-only RS");
			try {
				rs.moveToInsertRow();
				System.out.println("FAIL: moveToInsertRow can not be called on " +
						"read-only RS");
			} catch (SQLException se){ 
				dumpExpectedSQLException(se);
			}
			System.out.println("Negative test 47.b - try updateXXX on " +
					"read-only RS");
			try {
				rs.updateInt(1, 5000);
				System.out.println("FAIL: updateXXX not allowed on read-only RS");
				rs.updateInt(2, 5000);
				System.out.println("FAIL: updateXXX not allowed on read-only RS");
			} catch (SQLException se){ 
				dumpExpectedSQLException(se);
			}
			System.out.println("Negative test 47.c - try insertRow on " +
					"read-only RS");
			try {
				rs.insertRow();
				System.out.println("FAIL: insertRow not allowed on read-only RS");
			} catch (SQLException se){ 
				dumpExpectedSQLException(se);
			}
			System.out.println("Negative test 47.d - try moveToCurrentRow on " +
					"read-only RS");
			try {
				rs.moveToCurrentRow();
				System.out.println("FAIL: moveToCurrentRow can not be called on " +
						"read-only RS");
			} catch (SQLException se){ 
				dumpExpectedSQLException(se);
			}
			rs.close();
			conn.commit();
			stmt.close();
			
			System.out.println("Positive test 48 - Test all updateXXX methods on " +
					"all the supported sql datatypes");
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			stmt.executeUpdate("DELETE FROM AllDataTypesForTestingTable");
			conn.commit();
			PreparedStatement pstmti = conn.prepareStatement("SELECT * FROM AllDataTypesForTestingTable FOR UPDATE", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			PreparedStatement pstmt1i = conn.prepareStatement("SELECT * FROM AllDataTypesNewValuesData");
			for (int sqlType = 1, checkAgainstColumn = 1; sqlType <= allSQLTypes.length; sqlType++ ) {
				System.out.println("Next datatype to test is " + allSQLTypes[sqlType-1]);
				for (int updateXXXName = 1;  updateXXXName <= allUpdateXXXNames.length; updateXXXName++) {
					checkAgainstColumn = updateXXXName;
					System.out.println("  Testing " + allUpdateXXXNames[updateXXXName-1] + " on SQL type " + allSQLTypes[sqlType-1]);
					for (int indexOrName = 1; indexOrName <= 2; indexOrName++) {
						if (indexOrName == 1) //test by passing column position
							System.out.println("    Using column position as first parameter to " + allUpdateXXXNames[updateXXXName-1]);
						else
							System.out.println("    Using column name as first parameter to " + allUpdateXXXNames[updateXXXName-1]);
						rs = pstmti.executeQuery();
						rs.moveToInsertRow();
						rs1 = pstmt1i.executeQuery();
						rs1.next();
						try {
							if (updateXXXName == 1) {//update column with updateShort methods
								if (indexOrName == 1) //test by passing column position
									rs.updateShort(sqlType, rs1.getShort(updateXXXName));
								else //test by passing column name
									rs.updateShort(ColumnNames[sqlType-1], rs1.getShort(updateXXXName));
							} else if (updateXXXName == 2){ //update column with updateInt methods
								if (indexOrName == 1) //test by passing column position
									rs.updateInt(sqlType, rs1.getInt(updateXXXName));
								else //test by passing column name
									rs.updateInt(ColumnNames[sqlType-1], rs1.getInt(updateXXXName));
							} else if (updateXXXName ==  3){ //update column with updateLong methods
								if (indexOrName == 1) //test by passing column position
									rs.updateLong(sqlType, rs1.getLong(updateXXXName));
								else //test by passing column name
									rs.updateLong(ColumnNames[sqlType-1], rs1.getLong(updateXXXName));
							} else if (updateXXXName == 4){ //update column with updateBigDecimal methods
								if (indexOrName == 1) //test by passing column position
									rs.updateBigDecimal(sqlType, rs1.getBigDecimal(updateXXXName));
								else //test by passing column name
									rs.updateBigDecimal(ColumnNames[sqlType-1], rs1.getBigDecimal(updateXXXName));
							} else if (updateXXXName == 5){ //update column with updateFloat methods
								if (indexOrName == 1) //test by passing column position
									rs.updateFloat(sqlType, rs1.getFloat(updateXXXName));
								else //test by passing column name
									rs.updateFloat(ColumnNames[sqlType-1], rs1.getFloat(updateXXXName));
							} else if (updateXXXName == 6){ //update column with updateDouble methods
								if (indexOrName == 1) //test by passing column position
									rs.updateDouble(sqlType, rs1.getDouble(updateXXXName));
								else //test by passing column name
									rs.updateDouble(ColumnNames[sqlType-1], rs1.getDouble(updateXXXName));
							} else if (updateXXXName == 7){ //update column with updateString methods
								if (indexOrName == 1) //test by passing column position
									rs.updateString(sqlType, rs1.getString(updateXXXName));
								else //test by passing column name
									rs.updateString(ColumnNames[sqlType-1], rs1.getString(updateXXXName));
							} else if (updateXXXName == 8){ //update column with updateAsciiStream methods
								if (indexOrName == 1) //test by passing column position
									rs.updateAsciiStream(sqlType,rs1.getAsciiStream(updateXXXName), 4);
								else //test by passing column name
									rs.updateAsciiStream(ColumnNames[sqlType-1],rs1.getAsciiStream(updateXXXName), 4);
							} else if (updateXXXName == 9){ //update column with updateCharacterStream methods
								if (indexOrName == 1) //test by passing column position
									rs.updateCharacterStream(sqlType,rs1.getCharacterStream(updateXXXName), 4);
								else //test by passing column name
									rs.updateCharacterStream(ColumnNames[sqlType-1],rs1.getCharacterStream(updateXXXName), 4);
							} else if (updateXXXName == 10){ //update column with updateByte methods
									checkAgainstColumn = 1;
								if (indexOrName == 1) //test by passing column position
									rs.updateByte(sqlType,rs1.getByte(checkAgainstColumn));
								else //test by passing column name
									rs.updateByte(ColumnNames[sqlType-1],rs1.getByte(checkAgainstColumn));
							} else if (updateXXXName == 11){ //update column with updateBytes methods
								if (indexOrName == 1) //test by passing column position
									rs.updateBytes(sqlType,rs1.getBytes(updateXXXName));
								else //test by passing column name
									rs.updateBytes(ColumnNames[sqlType-1],rs1.getBytes(updateXXXName));
							} else if (updateXXXName == 12){ //update column with updateBinaryStream methods
								if (indexOrName == 1) //test by passing column position
									rs.updateBinaryStream(sqlType,rs1.getBinaryStream(updateXXXName), 2);
								else //test by passing column name
									rs.updateBinaryStream(ColumnNames[sqlType-1],rs1.getBinaryStream(updateXXXName), 2);
							} else if (updateXXXName == 13){ //update column with updateClob methods
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateClob(sqlType,rs1.getClob(updateXXXName));
								else //test by passing column name
									rs.updateClob(ColumnNames[sqlType-1],rs1.getClob(updateXXXName));
							} else if (updateXXXName == 14){ //update column with updateDate methods
								if (indexOrName == 1) //test by passing column position
									rs.updateDate(sqlType,rs1.getDate(updateXXXName));
								else //test by passing column name
									rs.updateDate(ColumnNames[sqlType-1],rs1.getDate(updateXXXName));
							} else if (updateXXXName == 15){ //update column with updateTime methods
								if (indexOrName == 1) //test by passing column position
									rs.updateTime(sqlType,rs1.getTime(updateXXXName));
								else //test by passing column name
									rs.updateTime(ColumnNames[sqlType-1],rs1.getTime(updateXXXName));
							} else if (updateXXXName == 16){ //update column with updateTimestamp methods
								if (indexOrName == 1) //test by passing column position
									rs.updateTimestamp(sqlType,rs1.getTimestamp(updateXXXName));
								else //test by passing column name
									rs.updateTimestamp(ColumnNames[sqlType-1],rs1.getTimestamp(updateXXXName));
							} else if (updateXXXName == 17){ //update column with updateBlob methods
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateBlob(sqlType,rs1.getBlob(updateXXXName));
								else //test by passing column name
									rs.updateBlob(ColumnNames[sqlType-1],rs1.getBlob(updateXXXName));
							} else if (updateXXXName == 18){ //update column with getBoolean methods
									//use SHORT sql type column's value for testing boolean since Derby don't support boolean datatype
									//Since Derby does not support Boolean datatype, this method is going to fail with the syntax error
								if (indexOrName == 1) //test by passing column position
									rs.updateBoolean(sqlType, rs1.getBoolean(1));
								else //test by passing column name
									rs.updateBoolean(ColumnNames[sqlType-1], rs1.getBoolean(1));
							} else if (updateXXXName == 19){ //update column with updateNull methods
								if (indexOrName == 1) //test by passing column position
									rs.updateNull(sqlType);
								else //test by passing column name
									rs.updateNull(ColumnNames[sqlType-1]);
							} else if (updateXXXName == 20){ //update column with updateArray methods - should get not implemented exception
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateArray(sqlType, null);
								else //test by passing column name
									rs.updateArray(ColumnNames[sqlType-1], null);
							} else if (updateXXXName == 21){ //update column with updateRef methods - should get not implemented exception
								if (JVMInfo.JDK_ID == 2) //Don't test this method because running JDK1.3 and this jvm does not support the method
									continue;
								if (indexOrName == 1) //test by passing column position
									rs.updateRef(sqlType, null);
								else //test by passing column name
									rs.updateRef(ColumnNames[sqlType-1], null);
							}
							rs.insertRow();
							if ((TestUtil.isNetFramework() && updateXXXRulesTableForNetworkServer[sqlType-1][updateXXXName-1].equals("ERROR")) ||
								(TestUtil.isEmbeddedFramework() && updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1].equals("ERROR"))) {
								System.out.println("FAILURE : We shouldn't reach here. The test should have failed earlier on updateXXX or updateRow call");
								return;
							}
							if (!verifyData(sqlType,checkAgainstColumn, "AllDataTypesNewValuesData"))
							{
								System.out.println("Verify data failed\nTest failed");
								return;
							}
							stmt.executeUpdate("DELETE FROM AllDataTypesForTestingTable");
						} catch (Throwable e) {
							if ((TestUtil.isNetFramework() && updateXXXRulesTableForNetworkServer[sqlType-1][updateXXXName-1].equals("ERROR")) ||
								(TestUtil.isEmbeddedFramework() && updateXXXRulesTableForEmbedded[sqlType-1][updateXXXName-1].equals("ERROR")))
								System.out.println("      Got expected exception : " + e.getMessage());
							else {
								if ((sqlType == 14 || sqlType == 15 || sqlType == 16) && //we are dealing with DATE/TIME/TIMESTAMP column types
									checkAgainstColumn == 7) //we are dealing with updateString. The failure is because string does not represent a valid datetime value
									System.out.println("      Got expected exception : " + e.getMessage());
								else {
									System.out.println("      Got UNexpected exception : " + e.getMessage());
									return;
								}
							}
						}
					}
					rs.close();
					rs1.close();
				}
			}
			conn.rollback();
			conn.setAutoCommit(true);
			
			// Verify positive tests
			dumpRS(stmt.executeQuery("select * from t4"));
			dumpRS(stmt.executeQuery("select * from t5"));
			
			stmt.close();			

			teardown();

			conn.close();


		} catch (Throwable e) {
			System.out.println("FAIL: exception thrown:");
			JDBCDisplayUtil.ShowException(System.out,e);
		}

		System.out.println("Finished testing updateable resultsets");
	}

	static boolean verifyData(int sqlType, int updateXXXName, String checkAgainstTheTable) throws SQLException {
		PreparedStatement pstmt1 = conn.prepareStatement("select * from " + checkAgainstTheTable);
		ResultSet rs1 = pstmt1.executeQuery();
		rs1.next();
		PreparedStatement pstmt = conn.prepareStatement("select * from AllDataTypesForTestingTable");
		ResultSet rs = pstmt.executeQuery();
		rs.next();

		if (updateXXXName == 18){ //verifying updateBoolean
			if(rs.getBoolean(sqlType) != rs1.getBoolean(1))
				return(false);
			else
				return(true);
		}

		if (updateXXXName == 19){ //verifying updateNull
			if(rs.getObject(sqlType) == null && rs.wasNull())
				return(true);
			else
				return(false);
		}

		if (sqlType == 1) {//verify update made to SMALLINT column with updateXXX methods
			if(rs.getShort(sqlType) != rs1.getShort(updateXXXName)) {
				return(false); }
		} else if (sqlType == 2) {  //verify update made to INTEGER column with updateXXX methods
			if(rs.getInt(sqlType) != rs1.getInt(updateXXXName)) {
				return(false); }
		} else if (sqlType ==  3)  //verify update made to BIGINT column with updateXXX methods
			if(rs.getLong(sqlType) != rs1.getLong(updateXXXName)) {
				return(false); }
		else if (sqlType == 4)  //verify update made to DECIMAL column with updateXXX methods
			if(BigDecimalHandler.getBigDecimalString(rs, sqlType) != 
				BigDecimalHandler.getBigDecimalString(rs1, updateXXXName)) {
				return(false); }
		else if (sqlType == 5)  //verify update made to REAL column with updateXXX methods
			if(rs.getFloat(sqlType) != rs1.getFloat(updateXXXName)) {
				return(false); }
		else if (sqlType == 6)  //verify update made to DOUBLE column with updateXXX methods
			if(rs.getDouble(sqlType) != rs1.getDouble(updateXXXName)) {
				return(false); }
		else if (sqlType == 7 || sqlType == 8 || sqlType == 9)  //verify update made to CHAR/VARCHAR/LONG VARCHAR column with updateXXX methods
			if(!rs.getString(sqlType).equals(rs1.getString(updateXXXName))) {
				return(false); }
		else if (sqlType == 10 || sqlType == 11 || sqlType == 12)  //verify update made to CHAR/VARCHAR/LONG VARCHAR FOR BIT DATA column with updateXXX methods
			if(rs.getBytes(sqlType) != rs1.getBytes(updateXXXName)) {
				return(false); }
		else if (sqlType == 13 && JVMInfo.JDK_ID != 2)  //verify update made to CLOB column with updateXXX methods
			if(!rs.getClob(sqlType).getSubString(1,4).equals(rs1.getClob(updateXXXName).getSubString(1,4))) {
				return(false); }
		else if (sqlType == 14)  //verify update made to DATE column with updateXXX methods
			if(rs.getDate(sqlType) != rs1.getDate(updateXXXName)) {
				return(false); }
		else if (sqlType == 15) { //verify update made to TIME column with updateXXX methods
			if(rs.getTime(sqlType) != rs1.getTime(updateXXXName)) {
				return(false); }
		} else if (sqlType == 16) { //verify update made to TIMESTAMP column with updateXXX methods
//			if(rs.getTimestamp(sqlType) != rs1.getTimestamp(updateXXXName)) {
			if(!rs.getTimestamp(sqlType).equals(rs1.getTimestamp(updateXXXName))) {
				return(false); }
		} else if (sqlType == 17 && JVMInfo.JDK_ID != 2)  //verify update made to BLOB column with updateXXX methods
			if(rs.getBlob(sqlType).getBytes(1,4) != rs1.getBlob(updateXXXName).getBytes(1,4)) {
				return(false); }

		rs.close();
		rs1.close();
		pstmt.close();
		pstmt1.close();
		return(true);
	}

	static void resetData() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("delete from AllDataTypesForTestingTable");
		StringBuffer insertSQL = new StringBuffer("insert into AllDataTypesForTestingTable values(");
		for (int type = 0; type < allSQLTypes.length - 1; type++)
		{
			insertSQL.append(SQLData[type][0] + ",");
		}
		insertSQL.append("cast("+SQLData[allSQLTypes.length - 1][0]+" as BLOB(1K)))");
		stmt.executeUpdate(insertSQL.toString());
	}

	// lifted from the autoGeneratedJdbc30 test
	public static void dumpRS(ResultSet s) throws SQLException
	{
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		ResultSetMetaData rsmd = s.getMetaData();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount();

		if (numCols <= 0)
		{
			System.out.println("(no columns!)");
			return;
		}

		StringBuffer heading = new StringBuffer("\t ");
		StringBuffer underline = new StringBuffer("\t ");

		int len;
		// Display column headings
		for (int i=1; i<=numCols; i++)
		{
			if (i > 1)
			{
				heading.append(",");
				underline.append(" ");
			}
			len = heading.length();
			heading.append(rsmd.getColumnLabel(i));
			len = heading.length() - len;
			for (int j = len; j > 0; j--)
			{
				underline.append("-");
			}
		}
		System.out.println(heading.toString());
		System.out.println(underline.toString());


		StringBuffer row = new StringBuffer();
		// Display data, fetching until end of the result set
		while (s.next())
		{
			row.append("\t{");
			// Loop through each column, getting the
			// column data and displaying
			for (int i=1; i<=numCols; i++)
			{
				if (i > 1) row.append(",");
				row.append(s.getString(i));
			}
			row.append("}\n");
		}
		System.out.println(row.toString());
		s.close();
	}

	static void reloadAllDataTypesForTestingTableData() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("delete from t1");
		stmt.executeUpdate("delete from AllDataTypesForTestingTable");
		StringBuffer insertSQL = new StringBuffer("insert into AllDataTypesForTestingTable values(");
		for (int type = 0; type < allSQLTypes.length - 1; type++)
			insertSQL.append(SQLData[type][0] + ",");
		insertSQL.append("cast("+SQLData[allSQLTypes.length - 1][0]+" as BLOB(1K)))");
		stmt.executeUpdate(insertSQL.toString());
	}

	static void reloadData() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("delete from t1");
		stmt.executeUpdate("insert into t1 values (1,'aa'), (2,'bb'), (3,'cc')");
		stmt.executeUpdate("delete from t3");
		stmt.executeUpdate("insert into t3 values (1,1), (2,2)");
		stmt.executeUpdate("delete from t4");
		stmt.executeUpdate("insert into t4 values (1,1), (2,2), (3,3)");
		stmt.executeUpdate("delete from t5");
		stmt.executeUpdate("insert into t5 values (1,1), (2,2), (3,3)");        
		stmt.executeUpdate("delete from table0WithTriggers");
		stmt.executeUpdate("insert into table0WithTriggers values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("delete from table1WithTriggers");
		stmt.executeUpdate("insert into table1WithTriggers values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("delete from table2WithTriggers");
		stmt.executeUpdate("insert into table2WithTriggers values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("delete from deleteTriggerInsertIntoThisTable");
		stmt.executeUpdate("delete from updateTriggerInsertIntoThisTable");
	}

	static void setup(boolean first) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("create table t1 (c1 int, c2 char(20))");
		stmt.executeUpdate("create view v1 as select * from t1");
		stmt.executeUpdate("create table t2 (c21 int, c22 int)");
		stmt.executeUpdate("create table t3 (c31 int not null primary key, c32 smallint)");
		stmt.executeUpdate("create table t4 (c41 int not null primary key, c42 int)");
		stmt.executeUpdate("create table t5 (c51 int not null default 0, c52 int)");
		stmt.executeUpdate("create table tableWithPrimaryKey (c1 int not null, c2 int not null, constraint pk primary key(c1,c2))");
		stmt.executeUpdate("create table tableWithConstraint (c1 int, c2 int, constraint fk foreign key(c1,c2) references tableWithPrimaryKey)");
		stmt.executeUpdate("create table table0WithTriggers (c1 int, c2 bigint)");
		stmt.executeUpdate("create table deleteTriggerInsertIntoThisTable (c1 int)");
		stmt.executeUpdate("create table updateTriggerInsertIntoThisTable (c1 int)");
		stmt.executeUpdate("create trigger tr1 after delete on table0WithTriggers for each statement mode db2sql insert into deleteTriggerInsertIntoThisTable values (1)");
		stmt.executeUpdate("create trigger tr2 after update on table0WithTriggers for each statement mode db2sql insert into updateTriggerInsertIntoThisTable values (1)");
		stmt.executeUpdate("create table table1WithTriggers (c1 int, c2 bigint)");
		stmt.executeUpdate("create trigger tr3 after delete on table1WithTriggers for each statement mode db2sql delete from table1WithTriggers");
		stmt.executeUpdate("create table table2WithTriggers (c1 int, c2 bigint)");
		stmt.executeUpdate("create trigger tr4 after update on table2WithTriggers for each statement mode db2sql update table2WithTriggers set c1=1");
		stmt.executeUpdate("create table selfReferencingT1 (c1 char(2) not null, c2 char(2), constraint selfReferencingT1 primary key(c1), constraint manages1 foreign key(c2) references selfReferencingT1(c1) on delete cascade)");
		stmt.executeUpdate("create table selfReferencingT2 (c1 char(2) not null, c2 char(2), constraint selfReferencingT2 primary key(c1), constraint manages2 foreign key(c2) references selfReferencingT2(c1) on update restrict)");

		stmt.executeUpdate("insert into t1 values (1,'aa')");
		stmt.executeUpdate("insert into t1 values (2,'bb')");
		stmt.executeUpdate("insert into t1 values (3,'cc')");
		stmt.executeUpdate("insert into t2 values (1,1)");
		stmt.executeUpdate("insert into t3 values (1,1)");
		stmt.executeUpdate("insert into t3 values (2,2)");
		stmt.executeUpdate("insert into t4 values (1,1), (2,2), (3,3)");
		stmt.executeUpdate("insert into t5 values (1,1), (2,2), (3,3)");        
		stmt.executeUpdate("insert into tableWithPrimaryKey values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("insert into tableWithConstraint values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("insert into table0WithTriggers values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("insert into table1WithTriggers values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("insert into table2WithTriggers values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("insert into selfReferencingT1 values ('e1', null), ('e2', 'e1'), ('e3', 'e2'), ('e4', 'e3')");
		stmt.executeUpdate("insert into selfReferencingT2 values ('e1', null), ('e2', 'e1'), ('e3', 'e2'), ('e4', 'e3')");

		StringBuffer createSQL = new StringBuffer("create table AllDataTypesForTestingTable (");
		StringBuffer createTestDataSQL = new StringBuffer("create table AllDataTypesNewValuesData (");
		for (int type = 0; type < allSQLTypes.length - 1; type++)
		{
			createSQL.append(ColumnNames[type] + " " + allSQLTypes[type] + ",");
			createTestDataSQL.append(ColumnNames[type] + " " + allSQLTypes[type] + ",");
		}
		createSQL.append(ColumnNames[allSQLTypes.length - 1] + " " + allSQLTypes[allSQLTypes.length - 1] + ")");
		createTestDataSQL.append(ColumnNames[allSQLTypes.length - 1] + " " + allSQLTypes[allSQLTypes.length - 1] + ")");
		stmt.executeUpdate(createSQL.toString());
		stmt.executeUpdate(createTestDataSQL.toString());

		createSQL = new StringBuffer("insert into AllDataTypesForTestingTable values(");
		createTestDataSQL = new StringBuffer("insert into AllDataTypesNewValuesData values(");
		for (int type = 0; type < allSQLTypes.length - 1; type++)
		{
			createSQL.append(SQLData[type][0] + ",");
			createTestDataSQL.append(SQLData[type][1] + ",");
		}
		createSQL.append("cast("+SQLData[allSQLTypes.length - 1][0]+" as BLOB(1K)))");
		createTestDataSQL.append("cast("+SQLData[allSQLTypes.length - 1][1]+" as BLOB(1K)))");
		stmt.executeUpdate(createSQL.toString());
		stmt.executeUpdate(createTestDataSQL.toString());

		stmt.close();
	}


	static void teardown() throws SQLException {
		Statement stmt = conn.createStatement();
		String[] testObjects={"table \" t 11 \"", "table \"t1\"",
			"trigger tr1", "trigger tr2", "trigger tr3", "trigger tr4",
			"view v1", "table s2.t1", "schema s2 restrict", "table t2", 
			"table t1", "table t3",	"table tableWithConstraint",
			"table tableWithPrimaryKey", "table deleteTriggerInsertIntoThisTable",
			"table updateTriggerInsertIntoThisTable", "table table0WithTriggers",
			"table table1WithTriggers", "table table2WithTriggers",
			"table selfReferencingT1", "table selfReferencingT2",
			"table AllDataTypesForTestingTable", "table AllDataTypesNewValuesData",
			"table t4", "table t5"};
		TestUtil.cleanUpTest(stmt, testObjects);	
		conn.commit();
		stmt.close();
	}

	public static void showScanStatistics(ResultSet rs, Connection conn)
	{
		Statement s = null;
		ResultSet infors = null;


		try {
			rs.close(); // need to close to get statistics
			s =conn.createStatement();
			infors = s.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
			JDBCDisplayUtil.setMaxDisplayWidth(2000);
			JDBCDisplayUtil.DisplayResults(System.out,infors,conn);
			infors.close();
		}
		catch (SQLException se)
		{
			System.out.print("FAIL:");
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}			
	}

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se = se.getNextException();
		}
	}
	
	static private void dumpExpectedSQLException (SQLException se) {
		while (se != null) {
			System.out.println("SQL State: " + se.getSQLState());
			System.out.println("Got expected exception: " + se.getMessage());
			se = se.getNextException();
		}		
	}

}

