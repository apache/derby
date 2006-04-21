/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.checkDataSource

   Copyright 2002, 2005 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.derby.jdbc.EmbeddedSimpleDataSource;
import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;
import org.apache.derby.jdbc.EmbeddedXADataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;

import javax.sql.DataSource;
import javax.sql.XADataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.ConnectionPoolDataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionEvent;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;

import java.io.*;
import java.util.Hashtable;
import java.util.Iterator;

import javax.naming.*;
import javax.naming.directory.*;

public class checkDataSource
{ 
    protected static Hashtable conns = new Hashtable();

	public static void main(String[] args) throws Exception {

        try
        {
			new checkDataSource().runTest(args);
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw e;
        }
		System.out.println("Completed checkDataSource");

	}


	public checkDataSource() throws Exception {
	}

	protected void runTest(String[] args) throws Exception {

		// Check the returned type of the JDBC Connections.
		ij.getPropertyArg(args);
		Connection dmc = ij.startJBMS();

		dmc.createStatement().executeUpdate("create table y(i int)");

		dmc.createStatement().executeUpdate("create procedure checkConn2(in dsname varchar(20)) parameter style java language java modifies SQL Data external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.checkDataSource.checkNesConn'");
		CallableStatement cs = dmc.prepareCall("call checkConn2(?)");
		cs.setString(1,"Nested");
		cs.execute();
		

		checkConnection("DriverManager ", dmc);
		checkJBMSToString();


		EmbeddedDataSource dscs = new EmbeddedDataSource();
		dscs.setDatabaseName("wombat");
		checkToString(dscs);

		DataSource ds = dscs;

		checkConnection("EmbeddedDataSource", ds.getConnection());
		
		EmbeddedSimpleDataSource dssimple = new EmbeddedSimpleDataSource();
		dssimple.setDatabaseName("wombat");
		ds = dssimple;
		checkConnection("EmbeddedSimpleDataSource", ds.getConnection());		

		EmbeddedConnectionPoolDataSource dscsp = new EmbeddedConnectionPoolDataSource();
		dscsp.setDatabaseName("wombat");
		//dscsp.setConnectionAttributes("unicode=true");
		ConnectionPoolDataSource dsp = dscsp;
		checkToString(dsp);

		PooledConnection pc = dsp.getPooledConnection();
		pc.addConnectionEventListener(new EventCatcher(1));

		checkConnection("EmbeddedConnectionPoolDataSource", pc.getConnection());
		checkConnection("EmbeddedConnectionPoolDataSource", pc.getConnection());

		// BUG 4471 - check outstanding updates are rolled back.
		Connection c1 = pc.getConnection();

		Statement s = c1.createStatement();

		s.executeUpdate("create table t (i int)");

		s.executeUpdate("insert into t values(1)");

		c1.setAutoCommit(false);

		// this update should be rolled back
		s.executeUpdate("insert into t values(2)");

		c1 = pc.getConnection();

		ResultSet rs = c1.createStatement().executeQuery("select count(*) from t");
		rs.next();
		int count = rs.getInt(1);

		System.out.println(count == 1 ? "Changes rolled back OK in auto closed pooled connection" :
				("FAIL changes committed in in auto closed pooled connection - " + count));

		c1.close();

		// check connection objects are closed once connection is closed
		try {
			rs.next();
			System.out.println("FAIL - ResultSet is open for a closed connection obtained from PooledConnection");
		} catch (SQLException sqle) {
			System.out.println("expected " + sqle.toString());
		}

		try {
			s.executeUpdate("update t set i = 1");
			System.out.println("FAIL - Statement is open for a closed connection obtained from PooledConnection");
		} catch (SQLException sqle) {
			System.out.println("expected " + sqle.toString());
		}

		pc.close();
		pc = null;

		testPoolReset("EmbeddedConnectionPoolDataSource", dsp.getPooledConnection());


		EmbeddedXADataSource dscsx = new EmbeddedXADataSource();
		dscsx.setDatabaseName("wombat");
		//dscsx.setConnectionAttributes("unicode=true");

		XADataSource dsx = dscsx;
		checkToString(dsx);

		XAConnection xac = dsx.getXAConnection();
		xac.addConnectionEventListener(new EventCatcher(3));

		checkConnection("EmbeddedXADataSource", xac.getConnection());

		// BUG 4471 - check outstanding updates are rolled back wi XAConnection.
		c1 = xac.getConnection();

		s = c1.createStatement();

		s.executeUpdate("insert into t values(1)");

		c1.setAutoCommit(false);

		// this update should be rolled back
		s.executeUpdate("insert into t values(2)");

		c1 = xac.getConnection();

		rs = c1.createStatement().executeQuery("select count(*) from t");
		rs.next();
		count = rs.getInt(1);
		rs.close();

		System.out.println(count == 2 ? "Changes rolled back OK in auto closed local XAConnection" :
				("FAIL changes committed in in auto closed pooled connection - " + count));

		c1.close();
		xac.close();
		xac = null;

		testPoolReset("EmbeddedXADataSource", dsx.getXAConnection());



		try {
			DriverManager.getConnection("jdbc:derby:;shutdown=true");
		} catch (SQLException sqle) {
			JDBCDisplayUtil.ShowSQLException(System.out, sqle);
		}

		dmc = ij.startJBMS();

		cs = dmc.prepareCall("call checkConn2(?)");
		cs.setString(1,"Nested");
		cs.execute();
		

		checkConnection("DriverManager ", dmc);

		// reset ds back to the EmbeddedDataSource
		ds = dscs;
		checkConnection("EmbeddedDataSource", ds.getConnection());
		
		// and back to EmbeddedSimpleDataSource
		ds = dssimple;
		checkConnection("EmbeddedSimpleDataSource", dssimple.getConnection());
		
		pc = dsp.getPooledConnection();
		pc.addConnectionEventListener(new EventCatcher(2));
		checkConnection("EmbeddedConnectionPoolDataSource", pc.getConnection());
		checkConnection("EmbeddedConnectionPoolDataSource", pc.getConnection());

		// test "local" XAConnections
		xac = dsx.getXAConnection();
		xac.addConnectionEventListener(new EventCatcher(4));
		checkConnection("EmbeddedXADataSource", xac.getConnection());
		checkConnection("EmbeddedXADataSource", xac.getConnection());
		xac.close();

		// test "global" XAConnections
		xac = dsx.getXAConnection();
		xac.addConnectionEventListener(new EventCatcher(5));
		XAResource xar = xac.getXAResource();
		Xid xid = new cdsXid(1, (byte) 35, (byte) 47);
		xar.start(xid, XAResource.TMNOFLAGS);
		Connection xacc = xac.getConnection();
		xacc.close();
		checkConnection("Global EmbeddedXADataSource", xac.getConnection());
		checkConnection("Global EmbeddedXADataSource", xac.getConnection());

		xar.end(xid, XAResource.TMSUCCESS);

		checkConnection("Switch to local EmbeddedXADataSource", xac.getConnection());
		checkConnection("Switch to local EmbeddedXADataSource", xac.getConnection());

		Connection backtoGlobal = xac.getConnection();

		xar.start(xid, XAResource.TMJOIN);
		checkConnection("Switch to global EmbeddedXADataSource", backtoGlobal);
		checkConnection("Switch to global EmbeddedXADataSource", xac.getConnection());
		xar.end(xid, XAResource.TMSUCCESS);
		xar.commit(xid, true);

		xac.close();

		// now some explicit tests for how connection state behaves
		// when switching between global transactions and local
		// and setting connection state.
		// some of this is already tested in simpleDataSource and checkDataSource
		// but I want to make sure I cover all situations. (djd)
		xac = dsx.getXAConnection();
		xac.addConnectionEventListener(new EventCatcher(6));
		xar = xac.getXAResource();
		xid = new cdsXid(1, (byte) 93, (byte) 103);

		// series 1 - Single connection object
		Connection cs1 = xac.getConnection();
		printState("initial local", cs1);
		xar.start(xid, XAResource.TMNOFLAGS);
		printState("initial  X1", cs1);
		cs1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		cs1.setReadOnly(true);
		setHoldability(cs1, false);
		printState("modified X1", cs1);
		xar.end(xid, XAResource.TMSUCCESS);
		// the underlying local transaction/connection must pick up the
		// state of the Connection handle cs1
		printState("modified local", cs1);
		cs1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		cs1.setReadOnly(false);
		setHoldability(cs1, false);

		printState("reset local", cs1);

		// now re-join the transaction, should pick up the read-only
		// and isolation level from the transaction,
		// holdability remains that of this handle.
		xar.start(xid, XAResource.TMJOIN);
		printState("re-join X1", cs1);
		xar.end(xid, XAResource.TMSUCCESS);

		// should be the same as the reset local
		printState("back to local (same as reset)", cs1);

		cs1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		cs1.setReadOnly(true);
		setHoldability(cs1, true);
		cs1.close();

		cs1 = xac.getConnection();
		printState("new handle - local ", cs1);
		cs1.close();

		xar.start(xid, XAResource.TMJOIN);
		cs1 = xac.getConnection();
		printState("re-join with new handle X1", cs1);
		cs1.close();
		xar.end(xid, XAResource.TMSUCCESS);

		// now get a connection (attached to a local)
		// attach to the global and commit it.
		// state should be that of the local after the commit.
		cs1 = xac.getConnection();
		cs1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		printState("pre-X1 commit - local", cs1);
		xar.start(xid, XAResource.TMJOIN);
		printState("pre-X1 commit - X1", cs1);
		xar.end(xid, XAResource.TMSUCCESS);
		printState("post-X1 end - local", cs1);
		xar.commit(xid, true);
		printState("post-X1 commit - local", cs1);
		cs1.close();

		//Derby-421 Setting isolation level with SQL was not getting handled correctly 
		System.out.println("Some more isolation testing using SQL and JDBC api");
		cs1 = xac.getConnection();
		s = cs1.createStatement();
		printState("initial local", cs1);

    System.out.println("Issue setTransactionIsolation in local transaction");
		cs1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		printState("setTransactionIsolation in local", cs1);

    System.out.println("Issue SQL to change isolation in local transaction");
		s.executeUpdate("set current isolation = RR");
		printState("SQL to change isolation in local", cs1);

		xid = new cdsXid(1, (byte) 35, (byte) 47);
		xar.start(xid, XAResource.TMNOFLAGS);
		printState("1st global(new)", cs1);
		xar.end(xid, XAResource.TMSUCCESS);

		printState("local", cs1);
    System.out.println("Issue SQL to change isolation in local transaction");
		s.executeUpdate("set current isolation = RS");
		printState("SQL to change isolation in local", cs1);

		Xid xid2 = new cdsXid(1, (byte) 93, (byte) 103);
		xar.start(xid2, XAResource.TMNOFLAGS);
		printState("2nd global(new)", cs1);
		xar.end(xid2, XAResource.TMSUCCESS);

		xar.start(xid, XAResource.TMJOIN);
		printState("1st global(existing)", cs1);
		xar.end(xid, XAResource.TMSUCCESS);

		printState("local", cs1);

		xar.start(xid, XAResource.TMJOIN);
		printState("1st global(existing)", cs1);
    System.out.println("Issue SQL to change isolation in 1st global transaction");
		s.executeUpdate("set current isolation = UR");
		printState("change isolation of existing 1st global transaction", cs1);
		xar.end(xid, XAResource.TMSUCCESS);

		printState("local", cs1);

		xar.start(xid2, XAResource.TMJOIN);
		printState("2nd global(existing)", cs1);
		xar.end(xid2, XAResource.TMSUCCESS);

		xar.rollback(xid2);
		printState("(After 2nd global rollback) local", cs1);

		xar.rollback(xid);
		printState("(After 1st global rollback) local", cs1);

		// now check re-use of *Statement objects across local/global connections.
		System.out.println("TESTING RE_USE OF STATEMENT OBJECTS");
		cs1 = xac.getConnection();

    
		// ensure read locsk stay around until end-of transaction
		cs1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		cs1.setAutoCommit(false);

		checkLocks(cs1);
		Statement sru1 = cs1.createStatement();
		sru1.setCursorName("SN1");
		sru1.executeUpdate("create table ru(i int)");
		sru1.executeUpdate("insert into ru values 1,2,3");
		Statement sruBatch = cs1.createStatement();
		Statement sruState = createFloatStatementForStateChecking(cs1);
		PreparedStatement psruState = createFloatStatementForStateChecking(cs1, "select i from ru where i = ?");
		CallableStatement csruState = createFloatCallForStateChecking(cs1, "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?,?)");
		PreparedStatement psParams = cs1.prepareStatement("select * from ru where i > ?");
		psParams.setCursorName("params");
		psParams.setInt(1, 2);
		resultSetQuery("Params-local-1", psParams.executeQuery());

		sruBatch.addBatch("insert into ru values 4");
		queryOnStatement("sru1-local-1", cs1, sru1);
		cs1.commit(); // need to commit to switch to an global connection;
		xid = new cdsXid(1, (byte) 103, (byte) 119);
		xar.start(xid, XAResource.TMNOFLAGS); // simple case - underlying connection is re-used for global.
		System.out.println("Expecting downgrade because global transaction sru1-global-2 is using a statement with holdability true");
		queryOnStatement("sru1-global-2", cs1, sru1);
		sruBatch.addBatch("insert into ru values 5");
		Statement sru2 = cs1.createStatement();
		sru2.setCursorName("OAK2");
		queryOnStatement("sru2-global-3", cs1, sru2);
		System.out.println("Expecting downgrade because global transaction sru1-global-4 is using a statement with holdability true");
		queryOnStatement("sru1-global-4", cs1, sru1);
		showStatementState("GLOBAL ", sruState);
		showStatementState("PS GLOBAL ", psruState);
		showStatementState("CS GLOBAL ", csruState);
		resultSetQuery("Params-global-1", psParams.executeQuery());

		xar.end(xid, XAResource.TMSUCCESS);
		// now a new underlying connection is created
		queryOnStatement("sru1-local-5", cs1, sru1);
		queryOnStatement("sru2-local-6", cs1, sru2);
		sruBatch.addBatch("insert into ru values 6,7");
		Statement sru3 = cs1.createStatement();
		sru3.setCursorName("SF3");
		queryOnStatement("sru3-local-7", cs1, sru3);
		// Two transactions should hold locks (global and the current XA);
		showStatementState("LOCAL ", sruState);
		showStatementState("PS LOCAL ", psruState);
		showStatementState("CS LOCAL ", csruState);
		resultSetQuery("Params-local-2", psParams.executeQuery());
		checkLocks(cs1);
		cs1.commit();

		// attach the XA transaction to another connection and see what happens
		XAConnection xac2 = dsx.getXAConnection();
		xac2.addConnectionEventListener(new EventCatcher(5));
		XAResource xar2 = xac2.getXAResource();

		xar2.start(xid, XAResource.TMJOIN);
		Connection cs2 = xac2.getConnection();

		// these statements were generated by cs1 and thus are still
		// in a local connection.
		queryOnStatement("sru1-local-8", cs1, sru1);
		queryOnStatement("sru2-local-9", cs1, sru2);
		queryOnStatement("sru3-local-10", cs1, sru3);
		sruBatch.addBatch("insert into ru values 8");
		showStatementState("LOCAL 2 ", sruState);
		showStatementState("PS LOCAL 2 ", psruState);
		showStatementState("CS LOCAL 2", csruState);

		checkLocks(cs1);

		int[] updateCounts = sruBatch.executeBatch();
		System.out.print("sruBatch update counts :");
		for (int i = 0; i < updateCounts.length; i++) {
			System.out.print(" " + updateCounts[i] + " ");
		}
		System.out.println(":");
		queryOnStatement("sruBatch", cs1, sruBatch);


		xar2.end(xid, XAResource.TMSUCCESS);

		xac2.close();

		// allow close on already closed XAConnection
		xac2.close();
		xac2.addConnectionEventListener(null);
		xac2.removeConnectionEventListener(null);

		// test methods against a closed XAConnection and its resource
		try {
			xac2.getXAResource();
		} catch (SQLException sqle) {
			System.out.println("XAConnection.getXAResource : " + sqle.getMessage());
		}
		try {
			xac2.getConnection();
		} catch (SQLException sqle) {
			System.out.println("XAConnection.getConnection : " + sqle.getMessage());
		}
		try {
			xar2.start(xid, XAResource.TMJOIN);
		} catch (XAException xae) {
			showXAException("XAResource.start", xae);
		}

		try {
			xar2.end(xid, XAResource.TMJOIN);
		} catch (XAException xae) {
			showXAException("XAResource.end", xae);
		}
		try {
			xar2.commit(xid, true);
		} catch (XAException xae) {
			showXAException("XAResource.commit", xae);
		}
		try {
			xar2.prepare(xid);
		} catch (XAException xae) {
			showXAException("XAResource.prepare", xae);
		}
		try {
			xar2.recover(0);
		} catch (XAException xae) {
			showXAException("XAResource.recover", xae);
		}
		try {
			xar2.prepare(xid);
		} catch (XAException xae) {
			showXAException("XAResource.prepare", xae);
		}
		try {
			xar2.isSameRM(xar2);
		} catch (XAException xae) {
			showXAException("XAResource.isSameRM", xae);
		}

		// Patricio (on the forum) one was having an issue with set schema not working in an XA connection.
		dmc = ij.startJBMS();
		dmc.createStatement().executeUpdate("create schema SCHEMA_Patricio");
		dmc.createStatement().executeUpdate("create table SCHEMA_Patricio.Patricio (id VARCHAR(255), value INTEGER)");
		dmc.commit();

		dmc.close();

		XAConnection xac3 = dsx.getXAConnection();
		Connection conn3 = xac3.getConnection();
		Statement st3 = conn3.createStatement();
		st3.execute("SET SCHEMA SCHEMA_Patricio");
		st3.close();

		PreparedStatement ps3 = conn3.prepareStatement("INSERT INTO Patricio VALUES (? , ?)");
		ps3.setString(1, "Patricio");
		ps3.setInt(2, 3);
		ps3.executeUpdate();

		System.out.println("Patricio update count " + ps3.getUpdateCount());
		ps3.close();
		conn3.close();
		xac3.close();

		// test that an xastart in auto commit mode commits the existing work.(beetle 5178)
		XAConnection xac4 = dsx.getXAConnection();
		Xid xid4a = new cdsXid(4, (byte) 23, (byte) 76);
		Connection conn4 = xac4.getConnection();
		System.out.println("conn4 autcommit " + conn4.getAutoCommit());

		Statement s4 = conn4.createStatement();
		s4.executeUpdate("create table autocommitxastart(i int)");
		s4.executeUpdate("insert into autocommitxastart values 1,2,3,4,5");

		ResultSet rs4 = s4.executeQuery("select i from autocommitxastart");
		rs4.next(); System.out.println("acxs " + rs4.getInt(1));
		rs4.next(); System.out.println("acxs " + rs4.getInt(1));

		xac4.getXAResource().start(xid4a, XAResource.TMNOFLAGS);
		xac4.getXAResource().end(xid4a, XAResource.TMSUCCESS);

		try {
			rs4.next(); System.out.println("acxs " + rs.getInt(1));
		} catch (SQLException sqle) {
			System.out.println("autocommitxastart expected " + sqle.getMessage());
		}

		conn4.setAutoCommit(false);

		rs4 = s4.executeQuery("select i from autocommitxastart");
		rs4.next(); System.out.println("acxs " + rs4.getInt(1));
		rs4.next(); System.out.println("acxs " + rs4.getInt(1));

		try {
			xac4.getXAResource().start(xid4a, XAResource.TMNOFLAGS);
		} catch (XAException xae) {
			showXAException("autocommitxastart expected ", xae);
		}
		rs4.next(); System.out.println("acxs " + rs4.getInt(1));
		rs4.close();

		conn4.rollback();
		conn4.close();
		xac4.close();
		


		// test jira-derby 95 - a NullPointerException was returned when passing
		// an incorrect database name (a url in this case) - should now give error XCY00
		Connection dmc95 = ij.startJBMS();
		String sqls; 
		try {
			testJira95ds( dmc95, "jdbc:derby:mydb" );
		} catch (SQLException sqle) {
			sqls = sqle.getSQLState();
			if (sqls.equals("XCY00"))
				System.out.println("; ok - expected exception: " + sqls);
			else 
				System.out.println("; wrong, unexpected exception: " + sqls + " - " + sqle.toString());
		} catch (Exception e) {
				System.out.println("; wrong, unexpected exception: " + e.toString());
		}
			
		try {
			testJira95xads( dmc95, "jdbc:derby:wombat" );
		} catch (SQLException sqle) {
			sqls = sqle.getSQLState();
			if (sqls.equals("XCY00"))
				System.out.println("; ok - expected exception: " + sqls + "\n");
			else 
				System.out.println("; wrong - unexpected exception: " + sqls + " - " + sqle.toString());
		} catch (Exception e) {
				System.out.println("; wrong, unexpected exception: " + e.toString());
		}

		testDSRequestAuthentication();
		
	}

	protected void showXAException(String tag, XAException xae) {

		System.out.println(tag + " : XAException - " + xae.getMessage());
	}

	/**
		Create a statement with modified State.
	*/
	protected Statement createFloatStatementForStateChecking(Connection conn) throws SQLException {
		Statement s = internalCreateFloatStatementForStateChecking(conn);
		s.setCursorName("StokeNewington");
		s.setFetchDirection(ResultSet.FETCH_REVERSE);
		s.setFetchSize(444);
		s.setMaxFieldSize(713);
		s.setMaxRows(19);

		showStatementState("Create ", s);
		return s;
	}

	protected Statement internalCreateFloatStatementForStateChecking(Connection conn) throws SQLException {
		return conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}

	protected void showStatementState(String when, Statement s) throws SQLException {
		System.out.println("Statement State @ " + when);
		System.out.println("  getResultSetType() " + rsType(s.getResultSetType()));
		System.out.println("  getResultSetConcurrency() " + rsConcurrency(s.getResultSetConcurrency()));
		System.out.println("  getFetchDirection() " + rsFetchDirection(s.getFetchDirection()));
		System.out.println("  getFetchSize() " + s.getFetchSize());
		System.out.println("  getMaxFieldSize() " + s.getMaxFieldSize());
		System.out.println("  getMaxRows() " + s.getMaxRows());
	}
	protected PreparedStatement createFloatStatementForStateChecking(Connection conn, String sql) throws SQLException {
		PreparedStatement s = internalCreateFloatStatementForStateChecking(conn, sql);
		s.setCursorName("StokeNewington");
		s.setFetchDirection(ResultSet.FETCH_REVERSE);
		s.setFetchSize(888);
		s.setMaxFieldSize(317);
		s.setMaxRows(91);

		showStatementState("PS Create ", s);
		return s;
	}
	protected CallableStatement createFloatCallForStateChecking(Connection conn, String sql) throws SQLException {
		CallableStatement s = internalCreateFloatCallForStateChecking(conn, sql);
		s.setCursorName("StokeNewington");
		s.setFetchDirection(ResultSet.FETCH_REVERSE);
		s.setFetchSize(999);
		s.setMaxFieldSize(137);
		s.setMaxRows(85);

		showStatementState("CS Create ", s);
		return s;
	}
	protected PreparedStatement internalCreateFloatStatementForStateChecking(Connection conn, String sql) throws SQLException {
		return conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}
	protected CallableStatement internalCreateFloatCallForStateChecking(Connection conn, String sql) throws SQLException {
		return conn.prepareCall(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	}

	static String rsType(int type) {
		switch (type) {
		case ResultSet.TYPE_FORWARD_ONLY:
			return "FORWARD_ONLY";
		case ResultSet.TYPE_SCROLL_SENSITIVE:
			return "SCROLL_SENSITIVE";
		case ResultSet.TYPE_SCROLL_INSENSITIVE:
			return "SCROLL_INSENSITIVE";
		default:
			return "?? TYPE UNKNOWN ??";

		}
	}

	static String rsConcurrency(int type) {
		switch (type) {
		case ResultSet.CONCUR_READ_ONLY:
			return "READ_ONLY";
		case ResultSet.CONCUR_UPDATABLE:
			return "UPDATEABLE";
		default:
			return "?? CONCURRENCY UNKNOWN ??";

		}
	}
	static String rsFetchDirection(int type) {
		switch (type) {
		case ResultSet.FETCH_FORWARD:
			return "FORWARD";
		case ResultSet.FETCH_REVERSE:
			return "REVERSE";
		case ResultSet.FETCH_UNKNOWN:
			return "UNKNOWN";
		default:
			return "?? FETCH DIRECTION REALLY UNKNOWN ??";

		}
	}
	private static void checkLocks(Connection conn) throws SQLException {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("SELECT XID, sum(cast (LOCKCOUNT AS INT)) FROM new org.apache.derby.diag.LockTable() AS L GROUP BY XID");
		System.out.println("LOCK TABLE");

        // Don't output actual XID's as they tend for every catalog change
        // to the system.
        int xact_index = 0;
		while (rs.next()) {
			// System.out.println("  xid " + rs.getString(1) + " lock count " + rs.getInt(2));
			System.out.println("  xid row " + xact_index + " lock count " + rs.getInt(2));
            xact_index++;
		}
		s.close();
		System.out.println("END LOCK TABLE");
	}

	private static void queryOnStatement(String tag, Connection conn, Statement s) throws SQLException {

		try {
			if (s.getConnection() != conn)
				System.out.println(tag + ": mismatched Statement connection");
			resultSetQuery(tag, s.executeQuery("select * from ru"));
		} catch (SQLException sqle) {
			System.out.println(tag + ": " + sqle.toString());
		}
	}

	private static void resultSetQuery(String tag, ResultSet rs) throws SQLException {
		System.out.print(tag + ": ru(" + rs.getCursorName() + ") contents");
		while (rs.next()) {
			System.out.print(" {" + rs.getInt(1) + "}");
		}
		System.out.println("");
		rs.close();
	}

	private void printState(String header, Connection conn) throws SQLException {
		System.out.println(header);
		getHoldability(conn);
		System.out.println("  isolation level " + translateIso(conn.getTransactionIsolation()));
		System.out.println("  auto commit     " + conn.getAutoCommit());
		System.out.println("  read only       " + conn.isReadOnly());
	}

	protected void setHoldability(Connection conn, boolean hold) throws SQLException {
	}

	protected void getHoldability(Connection conn) throws SQLException {
	}

	//calling checkConnection - for use in a procedure to get a nested connection.
	public static void checkNesConn (String dsName) throws SQLException {
		checkConnectionS(dsName, DriverManager.getConnection("jdbc:default:connection"));
			
	}

	public static void checkConnectionS(String dsName, Connection conn) throws SQLException {

		System.out.println("Running connection checks on " + dsName);

		//System.out.println("  url             " + conn.getMetaData().getURL());
		System.out.println("  isolation level " + conn.getTransactionIsolation());
		System.out.println("  auto commit     " + conn.getAutoCommit());
		System.out.println("  read only       " + conn.isReadOnly());

		// when 4729 is fixed, remove the startsWith() clause
		if (dsName.endsWith("DataSource") && !dsName.startsWith("Global"))
			System.out.println("  has warnings    " + (conn.getWarnings() != null));

		checkStatementS(conn, conn.createStatement());
		checkStatementS(conn, conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));

		Connection c1 = conn.getMetaData().getConnection();
		if (c1 != conn)
			System.out.println("FAIL incorrect connection object returned for DatabaseMetaData.getConnection()");

		checkConnectionPreCloseS(dsName, conn);
		conn.close();
	}

	public void checkConnection(String dsName, Connection conn) throws SQLException {

		System.out.println("Running connection checks on " + dsName);

		//System.out.println("  url             " + conn.getMetaData().getURL());
		System.out.println("  isolation level " + conn.getTransactionIsolation());
		System.out.println("  auto commit     " + conn.getAutoCommit());
		System.out.println("  read only       " + conn.isReadOnly());

		// when 4729 is fixed, remove the startsWith() clause
		if (dsName.endsWith("DataSource") && !dsName.startsWith("Global"))
			System.out.println("  has warnings    " + (conn.getWarnings() != null));

		Statement s1 = conn.createStatement();
		checkStatement(conn, s1);
		checkStatement(conn, conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));

		Connection c1 = conn.getMetaData().getConnection();
		if (c1 != conn)
			System.out.println("FAIL incorrect connection object returned for DatabaseMetaData.getConnection()");

		// Derby-33 - setTypeMap on connection
		try {
			conn.setTypeMap(java.util.Collections.EMPTY_MAP);
			System.out.println("setTypeMap(EMPTY_MAP) - ok");
		} catch (SQLException sqle) {
			System.out.println("setTypeMap(EMPTY_MAP) - FAIL " + sqle.getSQLState() + " - " + sqle.getMessage());
		}
		try {
			conn.setTypeMap(null);
			System.out.println("setTypeMap(null) - FAIL  - should throw exception");
		} catch (SQLException sqle) {
			System.out.println("setTypeMap(null) - ok " + sqle.getSQLState() + " - " + sqle.getMessage());
		}
		try {
			// a populated map, not implemented
			java.util.Map map = new java.util.HashMap();
			map.put("name", "class");

			conn.setTypeMap(map);
			System.out.println("setTypeMap(map) - FAIL  - should throw exception");
		} catch (SQLException sqle) {
			System.out.println("setTypeMap(map) - ok " + sqle.getSQLState() + " - " + sqle.getMessage());
		}

		checkConnectionPreClose(dsName, conn);
		conn.close();

		System.out.println("method calls on a closed connection");

		try {
			conn.close();
			System.out.println(dsName + " <closedconn>.close() no error");
		} catch (SQLException sqle) {
			System.out.println(dsName + " <closedconn>.close() " + sqle.getSQLState() + " - " + sqle.getMessage());
		}
		try {
			conn.createStatement();
			System.out.println(dsName + " <closedconn>.createStatement() no error");
		} catch (SQLException sqle) {
			System.out.println(dsName + " <closedconn>.createStatement() " + sqle.getSQLState() + " - " + sqle.getMessage());
		}
		try {
			s1.execute("values 1");
			System.out.println(dsName + " <closedstmt>.execute() no error");
		} catch (SQLException sqle) {
			System.out.println(dsName + " <closedstmt>.execute() " + sqle.getSQLState() + " - " + sqle.getMessage());
		}
	}
        
    /**
     * Make sure this connection's string is unique (DERBY-243)
     */
    protected void checkToString(Connection conn) throws Exception
    {
        String str = conn.toString();

        if ( conns.containsKey(str))
        {
            throw new Exception("ERROR: Connection toString() is not unique: " 
              + str);
        }
        conns.put(str, conn);
    }
    
    /**
     * Clear out and close connections in the connections
     * hashtable. 
     */
    protected void clearConnections() throws SQLException
    {
        java.util.Iterator it = conns.values().iterator();
        while ( it.hasNext() )
        {
            Connection conn = (Connection)it.next();
            conn.close();
        }
        conns.clear();
    }
    
    /**
     * Get connections  using ij.startJBMS() and make sure
     * they're unique
     */
    protected void checkJBMSToString() throws Exception
    {
        clearConnections();
        // Open ten connections rather than just two to
        // try and catch any odd uniqueness bugs.  Still
        // no guarantee but is better than just two.
        int numConnections = 10;
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            Connection conn = ij.startJBMS();
            checkToString(conn);
        }
        
        // Now close the connections
        clearConnections();
    }
    
    /**
     * Check uniqueness of connection strings coming from a
     * DataSouce
     */
    protected void checkToString(DataSource ds) throws Exception
    {
        clearConnections();
        
        int numConnections = 10;
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            Connection conn = ds.getConnection();
            checkToString(conn);
        }
        
        clearConnections();
    }
    
    /**
     * Check uniqueness of strings with a pooled data source.
     * We want to check the PooledConnection as well as the
     * underlying physical connection. 
     */
    protected void checkToString(ConnectionPoolDataSource pds)
        throws Exception
    {
        int numConnections = 10;
        
        //  First get a bunch of pooled connections
        //  and make sure they're all unique
        Hashtable pooledConns = new Hashtable();
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            PooledConnection pc = pds.getPooledConnection();
            String str = pc.toString();
            if ( pooledConns.get(str) != null )
            {
                throw new Exception("Pooled connection toString " +
                  "value " + str + " is not unique");
            }
            pooledConns.put(str, pc);
        }

        // Now check that connections from each of these
        // pooled connections have different string values
        Iterator it = pooledConns.values().iterator();
        clearConnections();
        while ( it.hasNext() )
        {
            PooledConnection pc = (PooledConnection)it.next();
            Connection conn = pc.getConnection();
            checkToString(conn);
        }
        clearConnections();
        
        // Now clear out the pooled connections
        it = pooledConns.values().iterator();
        while ( it.hasNext() )
        {
            PooledConnection pc = (PooledConnection)it.next();
            pc.close();
        }
        pooledConns.clear();
        
        // Now check that two connections from the same 
        // PooledConnection have the same string value
        PooledConnection pc = pds.getPooledConnection();
        Connection conn = pc.getConnection();
        String str = conn.toString();
        conn = pc.getConnection();
        if ( ! conn.toString().equals(str) )
        {
            throw new Exception("Two connections from the " +
              "same pooled connection have different string " +
              "values: " + str + ", " + conn.toString());
        }
        pc.close();
    }
    
    /**
     * Check uniqueness of strings for an XA data source
     */
    protected void checkToString(XADataSource xds) throws Exception
    {
        int numConnections = 10;
        
        //  First get a bunch of pooled connections
        //  and make sure they're all unique
        Hashtable xaConns = new Hashtable();
        for ( int i = 0 ; i < numConnections ; i++ )
        {
            XAConnection xc = xds.getXAConnection();
            String str = xc.toString();
            if ( xaConns.get(str) != null )
            {
                throw new Exception("XA connection toString " +
                  "value " + str + " is not unique");
            }
            xaConns.put(str, xc);
        }

        // Now check that connections from each of these
        // pooled connections have different string values
        Iterator it = xaConns.values().iterator();
        clearConnections();
        while ( it.hasNext() )
        {
            XAConnection xc = (XAConnection)it.next();
            Connection conn = xc.getConnection();
            checkToString(conn);
        }
        clearConnections();
        
        // Now clear out the pooled connections
        it = xaConns.values().iterator();
        while ( it.hasNext() )
        {
            XAConnection xc = (XAConnection)it.next();
            xc.close();
        }
        xaConns.clear();
        
        // Now check that two connections from the same 
        // XAConnection have the same string value
        XAConnection xc = xds.getXAConnection();
        Connection conn = xc.getConnection();
        String str = conn.toString();
        conn = xc.getConnection();
        if ( ! conn.toString().equals(str) )
        {
            throw new Exception("Two connections from the " +
              "same pooled connection have different string " +
              "values: " + str + ", " + conn.toString());
        }
        xc.close();
    }
    
	protected static void checkConnectionPreCloseS(String dsName, Connection conn) throws SQLException {
		if (dsName.endsWith("DataSource")) {

			// see if setting the state is carried over to any future connection from the
			// data source object.
			try {
				conn.setReadOnly(true);
			} catch (SQLException sqle) {
				// cannot set read-only in an active transaction, & sometimes
				// connections are active at this point.
			}
		}
	}

	protected void checkConnectionPreClose(String dsName, Connection conn) throws SQLException {
		if (dsName.endsWith("DataSource")) {

			// see if setting the state is carried over to any future connection from the
			// data source object.
			try {
				conn.setReadOnly(true);
			} catch (SQLException sqle) {
				// cannot set read-only in an active transaction, & sometimes
				// connections are active at this point.
			}
		}
	}

	protected static void checkStatementS(Connection conn, Statement s) throws SQLException {

		Connection c1 = s.getConnection();
		if (c1 != conn)
			System.out.println("FAIL incorrect connection object returned for Statement.getConnection()");

		s.addBatch("insert into y values 1");
		s.addBatch("insert into y values 2,3");
		int[] states = s.executeBatch();
		if (states[0] != 1)
			System.out.println("FAIL invalid update count for first batch statement");
		if (states[1] != 2)
			System.out.println("FAIL invalid update count for second batch statement");

		s.close();
	}
	protected void checkStatement(Connection conn, Statement s) throws SQLException {

		Connection c1 = s.getConnection();
		if (c1 != conn)
			System.out.println("FAIL incorrect connection object returned for Statement.getConnection()");

		s.addBatch("insert into y values 1");
		s.addBatch("insert into y values 2,3");
		int[] states = s.executeBatch();
		if (states[0] != 1)
			System.out.println("FAIL invalid update count for first batch statement");
		if (states[1] != 2)
			System.out.println("FAIL invalid update count for second batch statement");

		s.close();
	}

	private static void testDSRequestAuthentication() throws SQLException {

		EmbeddedDataSource ds = new EmbeddedDataSource();

		System.out.println("DataSource - EMPTY");
		dsConnectionRequests(ds);

		System.out.println("DataSource - connectionAttributes=databaseName=wombat");
		ds.setConnectionAttributes("databaseName=wombat");
		dsConnectionRequests(ds);
		ds.setConnectionAttributes(null);

		System.out.println("DataSource - attributesAsPassword=true");
		ds.setAttributesAsPassword(true);
		dsConnectionRequests(ds);
		ds.setAttributesAsPassword(false);

		System.out.println("DataSource - attributesAsPassword=true, connectionAttributes=databaseName=kangaroo");
		ds.setAttributesAsPassword(true);
		ds.setConnectionAttributes("databaseName=kangaroo");
		dsConnectionRequests(ds);
		ds.setAttributesAsPassword(false);
		ds.setConnectionAttributes(null);

		System.out.println("Enable Authentication");
		ds.setDatabaseName("wombat");
		Connection cadmin = ds.getConnection();
		CallableStatement cs = cadmin.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
		cs.setString(1, "derby.user.fred");
		cs.setString(2, "wilma");
		cs.execute();

		cs.setString(1, "derby.authentication.provider");
		cs.setString(2, "BUILTIN");
		cs.execute();

		cs.setString(1, "derby.connection.requireAuthentication");
		cs.setString(2, "true");
		cs.execute();

		cs.close();

		cadmin.close();

		ds.setShutdownDatabase("shutdown");
		try {
			ds.getConnection();
		} catch (SQLException sqle) {
			System.out.println(sqle.getSQLState() + ":" + sqle.getMessage() );
		}

		ds.setDatabaseName(null);
		ds.setShutdownDatabase(null);

		System.out.println("AUTHENTICATION NOW ENABLED");

		System.out.println("DataSource - attributesAsPassword=true");
		ds.setAttributesAsPassword(true);
		dsConnectionRequests(ds);
		ds.setAttributesAsPassword(false);

		// ensure the DS property password is not treated as a set of attributes.
		System.out.println("DataSource - attributesAsPassword=true, user=fred, password=databaseName=wombat;password=wilma");
		ds.setAttributesAsPassword(true);
		ds.setUser("fred");
		ds.setPassword("databaseName=wombat;password=wilma");
		dsConnectionRequests(ds);
		ds.setAttributesAsPassword(false);
		ds.setUser(null);
		ds.setPassword(null);
		ds = null;

		// now with ConnectionPoolDataSource
		EmbeddedConnectionPoolDataSource cpds = new EmbeddedConnectionPoolDataSource();
		System.out.println("ConnectionPoolDataSource - EMPTY");
		dsConnectionRequests((ConnectionPoolDataSource)cpds);

		System.out.println("ConnectionPoolDataSource - connectionAttributes=databaseName=wombat");
		cpds.setConnectionAttributes("databaseName=wombat");
		dsConnectionRequests((ConnectionPoolDataSource)cpds);
		cpds.setConnectionAttributes(null);

		System.out.println("ConnectionPoolDataSource - attributesAsPassword=true");
		cpds.setAttributesAsPassword(true);
		dsConnectionRequests((ConnectionPoolDataSource)cpds);
		cpds.setAttributesAsPassword(false);
		
		// ensure the DS property password is not treated as a set of attributes.
		System.out.println("ConnectionPoolDataSource - attributesAsPassword=true, user=fred, password=databaseName=wombat;password=wilma");
		cpds.setAttributesAsPassword(true);
		cpds.setUser("fred");
		cpds.setPassword("databaseName=wombat;password=wilma");
		dsConnectionRequests((ConnectionPoolDataSource)cpds);
		cpds.setAttributesAsPassword(false);
		cpds.setUser(null);
		cpds.setPassword(null);
		cpds = null;

		// now with XADataSource
		EmbeddedXADataSource xads = new EmbeddedXADataSource();
		System.out.println("XADataSource - EMPTY");
		dsConnectionRequests((XADataSource) xads);

		System.out.println("XADataSource - databaseName=wombat");
		xads.setDatabaseName("wombat");
		dsConnectionRequests((XADataSource) xads);
		xads.setDatabaseName(null);

		System.out.println("XADataSource - connectionAttributes=databaseName=wombat");
		xads.setConnectionAttributes("databaseName=wombat");
		dsConnectionRequests((XADataSource) xads);
		xads.setConnectionAttributes(null);

		System.out.println("XADataSource - attributesAsPassword=true");
		xads.setAttributesAsPassword(true);
		dsConnectionRequests((XADataSource) xads);
		xads.setAttributesAsPassword(false);

		System.out.println("XADataSource - databaseName=wombat, attributesAsPassword=true");
		xads.setDatabaseName("wombat");
		xads.setAttributesAsPassword(true);
		dsConnectionRequests((XADataSource) xads);
		xads.setAttributesAsPassword(false);
		xads.setDatabaseName(null);
	}

	private static void dsConnectionRequests(DataSource ds) {
		try {
			Connection c1 = ds.getConnection();
			System.out.println("  getConnection() - OK");
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getConnection() - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}

		try {
			Connection c1 = ds.getConnection(null, null);
			System.out.println("  getConnection(null, null) - OK");
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getConnection(null, null) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			Connection c1 = ds.getConnection("fred", null);
			System.out.println("  getConnection(fred, null) - OK");
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getConnection(fred, null) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			Connection c1 = ds.getConnection("fred", "wilma");
			System.out.println("  getConnection(fred, wilma) - OK");
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getConnection(fred, wilma) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			Connection c1 = ds.getConnection(null, "wilma");
			System.out.println("  getConnection(null, wilma) - OK");
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getConnection(null, wilma) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			Connection c1 = ds.getConnection(null, "databaseName=wombat");
			System.out.println("  getConnection(null, databaseName=wombat) - OK");
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getConnection(null, databaseName=wombat) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			Connection c1 = ds.getConnection("fred", "databaseName=wombat");
			System.out.println("  getConnection(fred, databaseName=wombat) - OK");
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getConnection(fred, databaseName=wombat) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			Connection c1 = ds.getConnection("fred", "databaseName=wombat;password=wilma");
			System.out.println("  getConnection(fred, databaseName=wombat;password=wilma) - OK");
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getConnection(fred, databaseName=wombat;password=wilma) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			Connection c1 = ds.getConnection("fred", "databaseName=wombat;password=betty");
			System.out.println("  getConnection(fred, databaseName=wombat;password=betty) - OK");
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getConnection(fred, databaseName=wombat;password=betty) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
	}
	private static void dsConnectionRequests(ConnectionPoolDataSource ds) {
		try {
			PooledConnection pc = ds.getPooledConnection();
			System.out.println("  getPooledConnection() - OK");
			Connection c1 = pc.getConnection();
			c1.close();
			pc.close();
		} catch (SQLException sqle) {
			System.out.println("  getPooledConnection() - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}

		try {
			PooledConnection pc = ds.getPooledConnection(null, null);
			System.out.println("  getPooledConnection(null, null) - OK");
			Connection c1 = pc.getConnection();
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getPooledConnection(null, null) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			PooledConnection pc = ds.getPooledConnection("fred", null);
			System.out.println("  getPooledConnection(fred, null) - OK");
			Connection c1 = pc.getConnection();
			c1.close();
			pc.close();
		} catch (SQLException sqle) {
			System.out.println("  getPooledConnection(fred, null) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			PooledConnection pc = ds.getPooledConnection("fred", "wilma");
			System.out.println("  getPooledConnection(fred, wilma) - OK");
			Connection c1 = pc.getConnection();
			c1.close();
			pc.close();
		} catch (SQLException sqle) {
			System.out.println("  getPooledConnection(fred, wilma) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			PooledConnection pc = ds.getPooledConnection(null, "wilma");
			System.out.println("  getPooledConnection(null, wilma) - OK");
			Connection c1 = pc.getConnection();
			c1.close();
			pc.close();
		} catch (SQLException sqle) {
			System.out.println("  getPooledConnection(null, wilma) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			PooledConnection pc = ds.getPooledConnection(null, "databaseName=wombat");
			System.out.println("  getPooledConnection(null, databaseName=wombat) - OK");
			Connection c1 = pc.getConnection();
			c1.close();
			pc.close();
		} catch (SQLException sqle) {
			System.out.println("  getPooledConnection(null, databaseName=wombat) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			PooledConnection pc = ds.getPooledConnection("fred", "databaseName=wombat");
			System.out.println("  getPooledConnection(fred, databaseName=wombat) - OK");
			Connection c1 = pc.getConnection();
			c1.close();
			pc.close();
		} catch (SQLException sqle) {
			System.out.println("  getPooledConnection(fred, databaseName=wombat) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			PooledConnection pc = ds.getPooledConnection("fred", "databaseName=wombat;password=wilma");
			System.out.println("  getPooledConnection(fred, databaseName=wombat;password=wilma) - OK");
			Connection c1 = pc.getConnection();
			c1.close();
			pc.close();
		} catch (SQLException sqle) {
			System.out.println("  getPooledConnection(fred, databaseName=wombat;password=wilma) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			PooledConnection pc = ds.getPooledConnection("fred", "databaseName=wombat;password=betty");
			System.out.println("  getPooledConnection(fred, databaseName=wombat;password=betty) - OK");
			Connection c1 = pc.getConnection();
			c1.close();
			pc.close();
		} catch (SQLException sqle) {
			System.out.println("  getPooledConnection(fred, databaseName=wombat;password=betty) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
	}
	private static void dsConnectionRequests(XADataSource ds) {
		try {
			XAConnection xc = ds.getXAConnection();
			System.out.println("  getXAConnection() - OK");
			Connection c1 = xc.getConnection();
			c1.close();
			xc.close();
		} catch (SQLException sqle) {
			System.out.println("  getXAConnection() - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}

		try {
			XAConnection xc = ds.getXAConnection(null, null);
			System.out.println("  getXAConnection(null, null) - OK");
			Connection c1 = xc.getConnection();
			c1.close();
		} catch (SQLException sqle) {
			System.out.println("  getXAConnection(null, null) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			XAConnection xc = ds.getXAConnection("fred", null);
			System.out.println("  getXAConnection(fred, null) - OK");
			Connection c1 = xc.getConnection();
			c1.close();
			xc.close();
		} catch (SQLException sqle) {
			System.out.println("  getXAConnection(fred, null) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			XAConnection xc = ds.getXAConnection("fred", "wilma");
			System.out.println("  getXAConnection(fred, wilma) - OK");
			Connection c1 = xc.getConnection();
			c1.close();
			xc.close();
		} catch (SQLException sqle) {
			System.out.println("  getXAConnection(fred, wilma) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			XAConnection xc = ds.getXAConnection(null, "wilma");
			System.out.println("  getXAConnection(null, wilma) - OK");
			Connection c1 = xc.getConnection();
			c1.close();
			xc.close();
		} catch (SQLException sqle) {
			System.out.println("  getXAConnection(null, wilma) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			XAConnection xc = ds.getXAConnection(null, "databaseName=wombat");
			System.out.println("  getXAConnection(null, databaseName=wombat) - OK");
			Connection c1 = xc.getConnection();
			c1.close();
			xc.close();
		} catch (SQLException sqle) {
			System.out.println("  getXAConnection(null, databaseName=wombat) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			XAConnection xc = ds.getXAConnection("fred", "databaseName=wombat");
			System.out.println("  getXAConnection(fred, databaseName=wombat) - OK");
			Connection c1 = xc.getConnection();
			c1.close();
			xc.close();
		} catch (SQLException sqle) {
			System.out.println("  getXAConnection(fred, databaseName=wombat) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			XAConnection xc = ds.getXAConnection("fred", "databaseName=wombat;password=wilma");
			System.out.println("  getXAConnection(fred, databaseName=wombat;password=wilma) - OK");
			Connection c1 = xc.getConnection();
			c1.close();
			xc.close();
		} catch (SQLException sqle) {
			System.out.println("  getXAConnection(fred, databaseName=wombat;password=wilma) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
		try {
			XAConnection xc = ds.getXAConnection("fred", "databaseName=wombat;password=betty");
			System.out.println("  getXAConnection(fred, databaseName=wombat;password=betty) - OK");
			Connection c1 = xc.getConnection();
			c1.close();
			xc.close();
		} catch (SQLException sqle) {
			System.out.println("  getXAConnection(fred, databaseName=wombat;password=betty) - " + sqle.getSQLState() + ":" + sqle.getMessage() );
		}
	}

	protected Xid getXid(int xid, byte b1, byte b2) {
		return new cdsXid(xid, b1, b2);
	}

	public static String translateIso(int iso)
	{
		switch(iso)
		{
		case Connection.TRANSACTION_READ_COMMITTED: return "READ_COMMITTED";
		case Connection.TRANSACTION_SERIALIZABLE: return "SERIALIZABLE";
		case Connection.TRANSACTION_REPEATABLE_READ: return "REPEATABLE_READ";
		case Connection.TRANSACTION_READ_UNCOMMITTED: return "READ_UNCOMMITTED";
		}
		return "unknown";
	}

	/**
		When a connection is being pooled, the underlying JDBC embedded
		connection object is re-used. As each application gets a new
		Connection object, that is really a wrapper around the old connection
		it should reset any connection spoecific state on the embedded connection
		object.
	*/
	private static void testPoolReset(String type, PooledConnection pc) throws SQLException
	{
		System.out.println("Start testPoolReset " + type);
		testPoolResetWork("C", pc.getConnection());
		testPoolResetWork("", pc.getConnection());
		testPoolResetWork("D", pc.getConnection());

		pc.close();
		System.out.println("End testPoolReset " + type);
	}

	private static void testPoolResetWork(String tableAction, Connection conn) throws SQLException
	{
		Statement s = conn.createStatement();
		if (tableAction.equals("C"))
		{
			s.execute("CREATE TABLE testPoolResetWork (id int generated always as identity, name varchar(25))");
		}

		ResultSet rs = s.executeQuery("VALUES IDENTITY_VAL_LOCAL()");
		rs.next();
		String val = rs.getString(1);
		if (!rs.wasNull() || (val != null))
			System.out.println("FAIL - initial call to IDENTITY_VAL_LOCAL is not NULL!" + val);
		rs.close();

		s.executeUpdate("INSERT INTO testPoolResetWork(name) values ('derby-222')");

		rs = s.executeQuery("VALUES IDENTITY_VAL_LOCAL()");
		rs.next();
		val = rs.getString(1);
		System.out.println("IDENTITY_VAL_LOCAL=" + val);
		rs.close();

		if (tableAction.equals("D"))
		{
			s.execute("DROP TABLE testPoolResetWork");
		}


		s.close();
		conn.close();

	}

	public void testJira95ds(Connection conn, String dbName) throws SQLException
	{
		System.out.print("\ntesting jira 95 for DataSource");
		EmbeddedDataSource ds = new EmbeddedDataSource();
		ds.setDatabaseName(dbName);
		Connection conn1 = ds.getConnection();
		conn1.close();
	}
	
	public void testJira95xads(Connection conn, String dbName) throws SQLException
	{
		System.out.print("testing jira 95 for XADataSource");
		EmbeddedXADataSource dxs = new EmbeddedXADataSource();
		dxs.setDatabaseName(dbName);
		Connection conn2 = dxs.getXAConnection().getConnection();
		conn2.close();
	}


}
class cdsXid implements Xid, Serializable
{
  private static final long serialVersionUID = 64467338100036L;

	private final int format_id;
	private byte[] global_id;
	private byte[] branch_id;


	cdsXid(int xid, byte b1, byte b2)
	{
		format_id = xid;
		global_id = new byte[Xid.MAXGTRIDSIZE];
		branch_id = new byte[Xid.MAXBQUALSIZE];

		for (int i = 0; i < global_id.length; i++) {
			global_id[i] = b1;
		}

		for (int i = 0; i < branch_id.length; i++) {
			branch_id[i] = b2;
		}
	}

    /**
     * Obtain the format id part of the Xid.
     * <p>
     *
     * @return Format identifier. O means the OSI CCR format.
     **/
    public int getFormatId()
    {
        return(format_id);
    }

    /**
     * Obtain the global transaction identifier part of XID as an array of 
     * bytes.
     * <p>
     *
	 * @return A byte array containing the global transaction identifier.
     **/
    public byte[] getGlobalTransactionId()
    {
        return(global_id);
    }

    /**
     * Obtain the transaction branch qualifier part of the Xid in a byte array.
     * <p>
     *
	 * @return A byte array containing the branch qualifier of the transaction.
     **/
    public byte[] getBranchQualifier()
    {
        return(branch_id);
    }


}

class EventCatcher implements ConnectionEventListener
{
	private final int catcher;

	EventCatcher(int which) {
		catcher=which;
	}

	// ConnectionEventListener methods
	public void connectionClosed(ConnectionEvent event)
	{
		System.out.print("EVENT("+catcher+"):connectionClosed");
		SQLException sqle = event.getSQLException();
		if (sqle != null)
			System.out.print(" SQLSTATE=" + sqle.getSQLState());
		System.out.println("");
	}

	public void connectionErrorOccurred(ConnectionEvent event)
	{
		System.out.print("EVENT("+catcher+"):connectionErrorOccurred");
		SQLException sqle = event.getSQLException();
		if (sqle != null)
			System.out.print(" SQLSTATE=" + sqle.getSQLState());
		System.out.println("");

	}

}
