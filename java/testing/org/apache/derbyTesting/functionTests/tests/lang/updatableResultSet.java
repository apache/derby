/*

   Derby - Class org.apache.derbyTesting.functionTests.lang.updatableResultSet

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

/**
  This tests JDBC 2.0 updateable resutlset - deleteRow api
 */
public class updatableResultSet { 

	private static Connection conn;
	private static DatabaseMetaData dbmt;
	private static Statement stmt, stmt1;
	private static ResultSet rs;
	private static PreparedStatement pStmt = null;
	private static CallableStatement callStmt = null;

	public static void main(String[] args) {
		System.out.println("Start testing delete using JDBC2.0 updateable resultset apis");

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			setup(true);

			System.out.println("---Negative Testl - request for scroll insensitive updatable resultset will give a read only scroll insensitive resultset");
			conn.clearWarnings();
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			SQLWarning warnings = conn.getWarnings();
			while (warnings != null)
			{
				System.out.println("warnings on connection = " + warnings);
				warnings = warnings.getNextWarning();
			}
			conn.clearWarnings();
      System.out.println("requested TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE but that is not supported");
      System.out.println("Make sure that we got TYPE_SCROLL_INSENSITIVE? " +  (stmt.getResultSetType() == ResultSet.TYPE_SCROLL_INSENSITIVE));
      System.out.println("Make sure that we got CONCUR_READ_ONLY? " +  (stmt.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY));
			dbmt = conn.getMetaData();
      System.out.println("ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + dbmt.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
      System.out.println("othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + dbmt.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
      System.out.println("deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + dbmt.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
			System.out.println("JDBC 2.0 updatable resultset api will fail on this resultset because this is not an updatable resultset");
      rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because Derby does not yet support scroll insensitive updatable resultsets");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("XJ083"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			rs.next();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Negative Test2 - request for scroll sensitive updatable resultset will give a read only scroll insensitive resultset");
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			while (warnings != null)
			{
				System.out.println("warnings on connection = " + warnings);
				warnings = warnings.getNextWarning();
			}
			conn.clearWarnings();
      System.out.println("requested TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE but that is not supported");
      System.out.println("Make sure that we got TYPE_SCROLL_INSENSITIVE? " +  (stmt.getResultSetType() == ResultSet.TYPE_SCROLL_INSENSITIVE));
      System.out.println("Make sure that we got CONCUR_READ_ONLY? " +  (stmt.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY));
			System.out.println("JDBC 2.0 updatable resultset api will fail on this resultset because this is not an updatable resultset");
      rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			rs.next();
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because Derby does not yet support scroll sensitive updatable resultsets");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("XJ083"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			rs.next();//make sure rs.next() does not fail because of earlier deleteRow
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Negative Test3 - request a read only resultset and attempt deleteRow on it");
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
				if (e.getSQLState().equals("XJ083"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Negative Test4 - request a read only resultset and send a sql with FOR UPDATE clause and attempt deleteRow on it");
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
				if (e.getSQLState().equals("XJ083"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Negative Test5 - request updatable resultset for sql with no FOR UPDATE clause");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("select * from t1");//notice that we forgot to give mandatory FOR UPDATE clause for updatable resultset
			System.out.println("Make sure that we got CONCUR_READ_ONLY? " + (rs.getConcurrency() == ResultSet.CONCUR_READ_ONLY));
			warnings = rs.getWarnings();
			while (warnings != null)
			{
				System.out.println("Expected warnings on resultset = " + warnings);
				warnings = warnings.getNextWarning();
			}
			rs.clearWarnings();
			rs.next();
      System.out.println("Now attempting to send a delete on a sql with no FOR UPDATE clause.");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed on sql with no FOR UPDATE clause");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("XJ083"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Negative Test6 - request updatable resultset for sql with FOR READ ONLY clause");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("select * from t1 FOR READ ONLY");
			System.out.println("Make sure that we got CONCUR_READ_ONLY? " + (rs.getConcurrency() == ResultSet.CONCUR_READ_ONLY));
			warnings = rs.getWarnings();
			while (warnings != null)
			{
				System.out.println("Expected warnings on resultset = " + warnings);
				warnings = warnings.getNextWarning();
			}
			rs.clearWarnings();
			rs.next();
      System.out.println("Now attempting to send a delete on a sql with FOR READ ONLY clause.");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed on sql with FOR READ ONLY clause");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("XJ083"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Negative Test7 - attempt to deleteRow on updatable resultset when the resultset is not positioned on a row");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
			System.out.println("Make sure that we got CONCUR_UPDATABLE? " + (rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE));
      System.out.println("Now attempt a deleteRow without first doing next on the resultset.");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because resultset is not on a row");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("24000"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			while (rs.next());//read all the rows from the resultset and position after the last row
      System.out.println("ResultSet is positioned after the last row. attempt to deleteRow at this point should fail!");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because resultset is after the last row");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("24000"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			rs.close();

			System.out.println("---Negative Test8 - attempt deleteRow on updatable resultset after closing the resultset");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
			System.out.println("Make sure that we got CONCUR_UPDATABLE? " + (rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE));
			rs.next();
			rs.close();
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because resultset is closed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("XCL16"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("---Negative Test9 - try updatable resultset on system table");
			try {
				rs = stmt.executeQuery("SELECT * FROM sys.systables FOR UPDATE");
				System.out.println("FAIL!!! trying to open an updatable resultset on a system table should have failed because system tables can't be updated by a user");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42Y90"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("---Negative Test10 - try updatable resultset on a view");
			try {
				rs = stmt.executeQuery("SELECT * FROM v1 FOR UPDATE");
				System.out.println("FAIL!!! trying to open an updatable resultset on a view should have failed because Derby doesnot support updates to views yet");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42Y90"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			stmt.executeUpdate("drop view v1");

			System.out.println("---Negative Test11 - attempt to open updatable resultset when there is join in the select query should fail");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			try {
				rs = stmt.executeQuery("SELECT c1 FROM t1,t2 where t1.c1 = t2.c21 FOR UPDATE");
				System.out.println("FAIL!!! trying to open an updatable resultset should have failed because updatable resultset donot support join in the select query");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42Y90"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("---Negative Test12 - With autocommit on, attempt to drop a table when there is an open updatable resultset on it");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
			rs.next();
			System.out.println("Opened an updatable resultset. Now trying to drop that table through another Statement");
			stmt1 = conn.createStatement();
			try {
				stmt1.executeUpdate("drop table t1");
				System.out.println("FAIL!!! drop table should have failed because the updatable resultset is still open");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("X0X95")) {
					System.out.println("expected exception " + e.getMessage());
				} else
					dumpSQLExceptions(e);
			}
			System.out.println("Since autocommit is on, the drop table exception resulted in a runtime rollback causing updatable resultset object to close");
			try {
				rs.deleteRow();
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("XCL16"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("---Negative Test13 - foreign key constraint failure will cause deleteRow to fail");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT 1, 2 FROM tableWithPrimaryKey FOR UPDATE");
			rs.next();
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because it will cause foreign key constraint failure");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("23503"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			System.out.println("Since autocommit is on, the constraint exception resulted in a runtime rollback causing updatable resultset object to close");
			try {
				rs.next();
				System.out.println("FAIL!!! next should have failed because foreign key constraint failure resulted in a runtime rollback");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("XCL16"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("---Positive Test1 - request updatable resultset for forward only type resultset");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			warnings = conn.getWarnings();
			while (warnings != null)
			{
				System.out.println("warnings = " + warnings);
				warnings = warnings.getNextWarning();
			}
      System.out.println("requested TYPE_FORWARD_ONLY, CONCUR_UPDATABLE");
      System.out.println("got TYPE_FORWARD_ONLY? " +  (stmt.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY));
      System.out.println("got CONCUR_UPDATABLE? " +  (stmt.getResultSetConcurrency() == ResultSet.CONCUR_UPDATABLE));
      rs = stmt.executeQuery("SELECT * FROM t1 FOR UPDATE");
			System.out.println("JDBC 2.0 updatable resultset apis on this ResultSet object will pass because this is an updatable resultset");
			rs.next();
      System.out.println("column 1 on this row before deleteRow is " + rs.getInt(1));
      System.out.println("column 2 on this row before deleteRow is " + rs.getString(2));
			rs.deleteRow();
      System.out.println("Since after deleteRow(), ResultSet is positioned before the next row, getXXX will fail");
			try {
				System.out.println("column 1 on this deleted row is " + rs.getInt(1));
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("24000"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
      System.out.println("calling deleteRow again w/o first positioning the ResultSet on the next row will fail");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because it can't be called more than once on the same row");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("24000"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
      System.out.println("Position the ResultSet with next()");
			rs.next();
      System.out.println("Should be able to deletRow() on the current row now");
			rs.deleteRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Positive Test2 - even if no columns from table specified in the column list, we should be able to get updatable resultset");
      reloadData();
      System.out.println("total number of rows in T1 ");
      dumpRS(stmt.executeQuery("select count(*) from t1"));
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
			rs.next();
      System.out.println("column 1 on this row is " + rs.getInt(1));
			rs.deleteRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
      System.out.println("total number of rows in T1 after one deleteRow is ");
      dumpRS(stmt.executeQuery("select count(*) from t1"));

			System.out.println("---Positive Test3 - use prepared statement with concur updatable status");
      reloadData();
			pStmt = conn.prepareStatement("select * from t1 where c1>? for update", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
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
				if (e.getSQLState().equals("24000"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
      System.out.println("calling deleteRow again w/o first positioning the ResultSet on the next row will fail");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because it can't be called more than once on the same row");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("24000"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
      System.out.println("Position the ResultSet with next()");
			rs.next();
      System.out.println("Should be able to deletRow() on the current row now");
			rs.deleteRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Positive Test4 - use callable statement with concur updatable status");
      reloadData();
			callStmt = conn.prepareCall("select * from t1 for update", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = callStmt.executeQuery();
      System.out.println("requested TYPE_FORWARD_ONLY, CONCUR_UPDATABLE");
      System.out.println("got TYPE_FORWARD_ONLY? " +  (callStmt.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY));
      System.out.println("got CONCUR_UPDATABLE? " +  (callStmt.getResultSetConcurrency() == ResultSet.CONCUR_UPDATABLE));
			rs.next();
      System.out.println("row not deleted yet. Confirm with rs.rowDeleted()? " + rs.rowDeleted());
      System.out.println("column 1 on this row is " + rs.getInt(1));
			rs.deleteRow();
      System.out.println("Since after deleteRow(), ResultSet is positioned before the next row, getXXX will fail");
			try {
				System.out.println("column 1 on this deleted row is " + rs.getInt(1));
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("24000"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
      System.out.println("calling deleteRow again w/o first positioning the ResultSet on the next row will fail");
			try {
				rs.deleteRow();
				System.out.println("FAIL!!! deleteRow should have failed because it can't be called more than once on the same row");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("24000"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
      System.out.println("Position the ResultSet with next()");
			rs.next();
      System.out.println("Should be able to deletRow() on the current row now");
			rs.deleteRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Positive Test5 - donot have to select primary key to get an updatable resultset");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT c32 FROM t3 FOR UPDATE");
			rs.next();
      System.out.println("column 1 on this row is " + rs.getInt(1));
      System.out.println("now try to delete row when primary key is not selected for that row");
			rs.deleteRow();
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Positive Test6 - For Forward Only resultsets, DatabaseMetaData will return false for ownDeletesAreVisible and deletesAreDetected");
			System.out.println("---This is because, after deleteRow, we position the ResultSet before the next row. We don't make a hole for the deleted row and then stay on that deleted hole");
			dbmt = conn.getMetaData();
      System.out.println("ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + dbmt.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
      System.out.println("othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + dbmt.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
      System.out.println("deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY)? " + dbmt.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE of c1");
			rs.next();
      System.out.println("The JDBC program should look at rowDeleted only if deletesAreDetected returns true");
      System.out.println("Since Derby returns false for detlesAreDetected for FORWARD_ONLY updatable resultset,the program should not rely on rs.rowDeleted() for FORWARD_ONLY updatable resultsets");
      System.out.println("Have this call to rs.rowDeleted() just to make sure the method does always return false? " + rs.rowDeleted());
			rs.deleteRow();
      System.out.println("Have this call to rs.rowDeleted() just to make sure the method does always return false? " + rs.rowDeleted());
			rs.close();

			System.out.println("---Positive Test7 - delete using updatable resultset api from a temporary table");
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
			rs.deleteRow();
			System.out.println("As expected, no rows in temp table after deleteRow");
			dumpRS(stmt.executeQuery("select * from SESSION.t2"));
			rs.close();
			stmt.executeUpdate("DROP TABLE SESSION.t2");

			System.out.println("---Positive Test8 - change the name of the resultset and see if deleteRow still works");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      System.out.println("change the cursor name(case sensitive name) with setCursorName and then try to deleteRow");
			stmt.setCursorName("CURSORNOUPDATe");//notice this name is case sensitive
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE of c1");
			rs.next();
			rs.deleteRow();
      System.out.println("change the cursor name one more time with setCursorName and then try to deleteRow");
			stmt.setCursorName("CURSORNOUPDATE1");
			rs.next();
			rs.deleteRow();
			rs.close();

			System.out.println("---Positive Test9 - using correlation name for the table in the select sql is not a problem");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 abcde FOR UPDATE of c1");
			rs.next();
      System.out.println("column 1 on this row is " + rs.getInt(1));
      System.out.println("now try to deleteRow");
			rs.deleteRow();
			rs.close();

			System.out.println("---Positive Test10 - 2 updatable resultsets going against the same table, will they conflict?");
			conn.setAutoCommit(false);
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt1 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
			rs.next();
      ResultSet rs1 = stmt1.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
			rs1.next();
			System.out.println("delete using first resultset");
			rs.deleteRow();
			try {
				System.out.println("attempt to send deleteRow on the same row through a different resultset should throw an exception");
				rs1.deleteRow();
				System.out.println("FAIL!!! delete using second resultset succedded? " + rs1.rowDeleted());
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("XCL08"))
					System.out.println("Got expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			System.out.println("Move to next row in the 2nd resultset and then delete using the second resultset");
			rs1.next();
			rs1.deleteRow();
			rs.close();
			rs1.close();
			conn.setAutoCommit(true);

			System.out.println("---Positive Test11 - setting the fetch size to > 1 will be ignored by updatable resultset. Same as updatable cursors");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
			stmt.setFetchSize(200);
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE of c1");
			System.out.println("Notice the Fetch Size in run time statistics output.");
      showScanStatistics(rs, conn);
			System.out.println("statement's fetch size is " + stmt.getFetchSize());
			rs.close();
			stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");

			System.out.println("---Positive Test12 - make sure delete trigger gets fired when deleteRow is issued");
      reloadData();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("Verify that before delete trigger got fired, row count is 0 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			rs = stmt.executeQuery("SELECT * FROM tableWithDeleteTriggers FOR UPDATE");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getInt(1));
			System.out.println("now try to delete row and make sure that trigger got fired");
			rs.deleteRow();
			rs.close();
			System.out.println("Verify that delete trigger got fired by verifying the row count to be 1 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Positive Test13 - Another test case for delete trigger");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM anotherTableWithDeleteTriggers FOR UPDATE");
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
				if (e.getSQLState().equals("24000")) {
					System.out.println("expected exception " + e.getMessage());
				} else
					dumpSQLExceptions(e);
			}
			rs.close();
			System.out.println("Verify that delete trigger got fired by verifying the row count to be 0 in anotherTableWithDeleteTriggers");
			dumpRS(stmt.executeQuery("select count(*) from anotherTableWithDeleteTriggers"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Positive Test14 - make sure self referential delete cascade works when deleteRow is issued");
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery("SELECT * FROM selfReferencingT1 FOR UPDATE");
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
				if (e.getSQLState().equals("24000")) {
					System.out.println("expected exception " + e.getMessage());
				} else
					dumpSQLExceptions(e);
			}
			rs.close();
			System.out.println("Verify that delete trigger got fired by verifying the row count to be 0 in anotherTableWithDeleteTriggers");
			dumpRS(stmt.executeQuery("select count(*) from selfReferencingT1"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();

			System.out.println("---Positive Test15 - With autocommit off, attempt to drop a table when there is an open updatable resultset on it");
      reloadData();
      conn.setAutoCommit(false);
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
      rs = stmt.executeQuery("SELECT 1, 2 FROM t1 FOR UPDATE");
			rs.next();
			System.out.println("Opened an updatable resultset. Now trying to drop that table through another Statement");
			stmt1 = conn.createStatement();
			try {
				stmt1.executeUpdate("drop table t1");
				System.out.println("FAIL!!! drop table should have failed because the updatable resultset is still open");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("X0X95")) {
					System.out.println("expected exception " + e.getMessage());
				} else
					dumpSQLExceptions(e);
			}
			System.out.println("Since autocommit is off, the drop table exception will NOT result in a runtime rollback and hence updatable resultset object is still open");
      rs.deleteRow();
			rs.close();
      conn.setAutoCommit(true);

			System.out.println("---Positive Test16 - Do deleteRow within a transaction and then rollback the transaction");
      reloadData();
      conn.setAutoCommit(false);
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			System.out.println("Verify that before delete trigger got fired, row count is 0 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			System.out.println("Verify that before deleteRow, row count is 4 in tableWithDeleteTriggers");
			dumpRS(stmt.executeQuery("select count(*) from tableWithDeleteTriggers"));
			rs = stmt.executeQuery("SELECT * FROM tableWithDeleteTriggers FOR UPDATE");
			rs.next();
			System.out.println("column 1 on this row is " + rs.getInt(1));
			System.out.println("now try to delete row and make sure that trigger got fired");
			rs.deleteRow();
			rs.close();
			System.out.println("Verify that delete trigger got fired by verifying the row count to be 1 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			System.out.println("Verify that deleteRow in transaction, row count is 3 in tableWithDeleteTriggers");
			dumpRS(stmt.executeQuery("select count(*) from tableWithDeleteTriggers"));
			//have to close the resultset because by default, resultsets are held open over commit
			rs.close();
      conn.rollback();
			System.out.println("Verify that after rollback, row count is back to 0 in deleteTriggerInsertIntoThisTable");
			dumpRS(stmt.executeQuery("select count(*) from deleteTriggerInsertIntoThisTable"));
			System.out.println("Verify that after rollback, row count is back to 4 in tableWithDeleteTriggers");
			dumpRS(stmt.executeQuery("select count(*) from tableWithDeleteTriggers"));
      conn.setAutoCommit(true);

			System.out.println("---Positive Test17 - After deleteRow, resultset is positioned before the next row");
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
				if (e.getSQLState().equals("24000")) {
					System.out.println("expected exception " + e.getMessage());
				} else
					dumpSQLExceptions(e);
			}
			rs.close();

			teardown();

			conn.close();

		} catch (Throwable e) {
			System.out.println("FAIL: exception thrown:");
			JDBCDisplayUtil.ShowException(System.out,e);
		}

		System.out.println("Finished testing updateable resultsets");
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

	static void reloadData() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("delete from t1");
		stmt.executeUpdate("insert into t1 values (1,'aa'), (2,'bb'), (3,'cc')");
		stmt.executeUpdate("delete from t3");
		stmt.executeUpdate("insert into t3 values (1,1), (2,2)");
		stmt.executeUpdate("delete from tableWithDeleteTriggers");
		stmt.executeUpdate("insert into tableWithDeleteTriggers values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("delete from deleteTriggerInsertIntoThisTable");
	}

	static void setup(boolean first) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("create table t1 (c1 int, c2 char(20))");
		stmt.executeUpdate("create view v1 as select * from t1");
		stmt.executeUpdate("create table t2 (c21 int, c22 int)");
		stmt.executeUpdate("create table t3 (c31 int not null primary key, c32 smallint)");
		stmt.executeUpdate("create table tableWithPrimaryKey (c1 int not null, c2 int not null, constraint pk primary key(c1,c2))");
		stmt.executeUpdate("create table tableWithConstraint (c1 int, c2 int, constraint fk foreign key(c1,c2) references tableWithPrimaryKey)");
		stmt.executeUpdate("create table tableWithDeleteTriggers (c1 int, c2 bigint)");
		stmt.executeUpdate("create table deleteTriggerInsertIntoThisTable (c1 int)");
		stmt.executeUpdate("create trigger tr1 after delete on tableWithDeleteTriggers for each statement mode db2sql insert into deleteTriggerInsertIntoThisTable values (1)");
		stmt.executeUpdate("create table anotherTableWithDeleteTriggers (c1 int, c2 bigint)");
		stmt.executeUpdate("create trigger tr2 after delete on anotherTableWithDeleteTriggers for each statement mode db2sql delete from anotherTableWithDeleteTriggers");
		stmt.executeUpdate("create table selfReferencingT1 (c1 char(2) not null, c2 char(2), constraint selfReferencingT1 primary key(c1), constraint manages foreign key(c2) references selfReferencingT1(c1) on delete cascade)");

		stmt.executeUpdate("insert into t1 values (1,'aa')");
		stmt.executeUpdate("insert into t1 values (2,'bb')");
		stmt.executeUpdate("insert into t1 values (3,'cc')");
		stmt.executeUpdate("insert into t2 values (1,1)");
		stmt.executeUpdate("insert into t3 values (1,1)");
		stmt.executeUpdate("insert into t3 values (2,2)");
		stmt.executeUpdate("insert into tableWithPrimaryKey values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("insert into tableWithConstraint values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("insert into tableWithDeleteTriggers values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("insert into anotherTableWithDeleteTriggers values (1, 1), (2, 2), (3, 3), (4, 4)");
		stmt.executeUpdate("insert into selfReferencingT1 values ('e1', null), ('e2', 'e1'), ('e3', 'e2'), ('e4', 'e3')");
		stmt.close();
	}


	static void teardown() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("drop table t1");
		stmt.executeUpdate("drop table t2");
		stmt.executeUpdate("drop table t3");
		stmt.executeUpdate("drop table tableWithConstraint");
		stmt.executeUpdate("drop table tableWithPrimaryKey");
		stmt.executeUpdate("drop table deleteTriggerInsertIntoThisTable");
		stmt.executeUpdate("drop table tableWithDeleteTriggers");
		stmt.executeUpdate("drop table anotherTableWithDeleteTriggers");
		stmt.executeUpdate("drop table selfReferencingT1");
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

}
