/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.checkDataSource30

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.functionTests.util.SecurityCheck;
import org.apache.derbyTesting.functionTests.util.TestUtil;


/**
 * Extends checkDataSource to provide testing of JDBC 3.0 specific
 * methods for the embedded DataSource implementations.
 * @author djd
 *
 */
public class checkDataSource30 extends checkDataSource
{ 

    // DERBY-1370 - Embedded and client driver differ in the holdability 
	// reported inside a global transaction for statements that were created
	// with HOLD_CURSORS_OVER_COMMIT outside the transaction
	// It looks like embedded behaviour is correct.
	private static boolean stmtHoldabilityError = 
							TestUtil.isDerbyNetClientFramework();
	
	public static void main(String[] args) throws Exception {

		checkDataSource30 tester = new checkDataSource30();
		tester.runTest(args);
		tester.checkXAHoldability();
		
		testDerby1144();
		// Print a report on System.out of the issues
		// found with the security checks.
		SecurityCheck.report();
		
		
		System.out.println("Completed checkDataSource30");

	}


	public checkDataSource30() {
	}

	public void checkConnection(String dsName, Connection conn) throws SQLException {

		System.out.println("Running JDBC 3.0 connection checks on " + dsName);

		
		System.out.println("  holdability     " + (conn.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));

		// check it's a 3.0 connection object
		try {
			conn.releaseSavepoint(conn.setSavepoint());
			System.out.println("JDBC 3.0 savepoint OK");
		} catch (SQLException sqle) {
			// we expect savepoints exceptions because either
			// it's a global transaction, or it's in auto commit mode.
			System.out.println("JDBC 3.0 savepoint SQL Exception: (" +
                              sqle.getSQLState() + ") " + sqle.getMessage());
		}


		super.checkConnection(dsName, conn);
	}

	protected void checkConnectionPreClose(String dsName, Connection conn) throws SQLException {

		conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

		super.checkConnectionPreClose(dsName, conn);

	}
	protected void setHoldability(Connection conn, boolean hold) throws SQLException {

		conn.setHoldability(hold ? ResultSet.HOLD_CURSORS_OVER_COMMIT : ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}
	protected void getHoldability(Connection conn) throws SQLException {

		System.out.println("  holdability     " + (conn.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
	}

	protected Statement internalCreateFloatStatementForStateChecking(Connection conn) throws SQLException {
		return conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}
	protected PreparedStatement internalCreateFloatStatementForStateChecking(Connection conn, String sql) throws SQLException {
		return conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}
	protected CallableStatement internalCreateFloatCallForStateChecking(Connection conn, String sql) throws SQLException {
		return conn.prepareCall(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}


	protected void showStatementState(String when, Statement s) throws SQLException {
		super.showStatementState(when, s);
		System.out.println("  getResultSetHoldability() " + rsHoldability(s.getResultSetHoldability()));
		if (s instanceof PreparedStatement) {
			PreparedStatement ps = (PreparedStatement) s;
			ParameterMetaData psmd = ps.getParameterMetaData();
			System.out.println("  Parameter Count " + psmd.getParameterCount());
			for (int i = 1; i <= psmd.getParameterCount(); i++) {
				System.out.println("    " + i + " type " + psmd.getParameterType(i));
			}
		}
	}

	protected void showXAException(String tag, XAException xae) {

		super.showXAException(tag, xae);
		Throwable t = xae.getCause();
		if (t instanceof SQLException)
			JDBCDisplayUtil.ShowSQLException(System.out, (SQLException) t);
	}


	static String rsHoldability(int type) {
		switch (type) {
		case ResultSet.HOLD_CURSORS_OVER_COMMIT :
			return "HOLD_CURSORS_OVER_COMMIT ";
		case ResultSet.CLOSE_CURSORS_AT_COMMIT :
			return "CLOSE_CURSORS_AT_COMMIT ";
		default:
			return "?? HOLDABILITY UNKNOWN ??";

		}
	}

	private void checkXAHoldability() {
		System.out.println("START XA HOLDABILITY TEST");
		try {
			Properties attrs = new Properties();
			attrs.setProperty("databaseName", "wombat");
			attrs.setProperty("connectionAttributes", "create=true");
			XADataSource dscsx =  TestUtil.getXADataSource(attrs);
		
			XAConnection xac = dscsx.getXAConnection("fred", "wilma");
			XAResource xr = xac.getXAResource();
			Xid xid = getXid(25, (byte) 21, (byte) 01);
			Connection conn1 = xac.getConnection();
			System.out.println("By default, autocommit is " + conn1.getAutoCommit() + " for a connection");
			System.out.println("Default holdability for a connection is HOLD_CURSORS_OVER_COMMIT");
			System.out.println("CONNECTION(not in xa transaction yet) HOLDABILITY " + (conn1.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			//start a global transaction and default holdability and autocommit will be switched to match Derby XA restrictions
			xr.start(xid, XAResource.TMNOFLAGS);
			System.out.println("Notice that autocommit now is " + conn1.getAutoCommit() + " for connection because it is part of the global transaction");
			System.out.println("Notice that connection's holdability at this point is CLOSE_CURSORS_AT_COMMIT because it is part of the global transaction");
			System.out.println("CONNECTION(in xa transaction) HOLDABILITY " + (conn1.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			xr.end(xid, XAResource.TMSUCCESS);
			conn1.commit();
			conn1.close();

			xid = getXid(27, (byte) 21, (byte) 01);
			xr.start(xid, XAResource.TMNOFLAGS);
			conn1 = xac.getConnection();
			System.out.println("CONNECTION(in xa transaction) HOLDABILITY " + (conn1.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("Autocommit on Connection inside global transaction has been set correctly to " + conn1.getAutoCommit());
			xr.end(xid, XAResource.TMSUCCESS);
			conn1.rollback();

			Connection conn = xac.getConnection();
			conn.setAutoCommit(false);
			conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
			System.out.println("CONNECTION(non-xa) HOLDABILITY " + (conn.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));

			Statement s = conn.createStatement();
			System.out.println("STATEMENT HOLDABILITY " + (s.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));


			s.executeUpdate("create table hold_30 (id int not null primary key, b char(30))");
			s.executeUpdate("insert into hold_30 values (1,'init2'), (2, 'init3'), (3,'init3')");
			s.executeUpdate("insert into hold_30 values (4,'init4'), (5, 'init5'), (6,'init6')");
			s.executeUpdate("insert into hold_30 values (7,'init7'), (8, 'init8'), (9,'init9')");

			System.out.println("STATEMENT HOLDABILITY " + (s.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));

			Statement sh = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			PreparedStatement psh = conn.prepareStatement("select id from hold_30 for update",
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
			CallableStatement csh = conn.prepareCall("select id from hold_30 for update",
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);

			System.out.println("STATEMENT HOLDABILITY " + (sh.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("PREPARED STATEMENT HOLDABILITY " + (psh.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("CALLABLE STATEMENT HOLDABILITY " + (csh.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));

			ResultSet rsh = sh.executeQuery("select id from hold_30 for update");
			rsh.next(); System.out.println("H@1 id " + rsh.getInt(1));
			rsh.next(); System.out.println("H@2 id " + rsh.getInt(1));
			conn.commit();
			rsh.next(); System.out.println("H@3 id " + rsh.getInt(1));
			conn.commit();


			xid = getXid(23, (byte) 21, (byte) 01);
			xr.start(xid, XAResource.TMNOFLAGS);
			Statement stmtInsideGlobalTransaction = conn.createStatement();
			PreparedStatement prepstmtInsideGlobalTransaction = conn.prepareStatement("select id from hold_30");
			CallableStatement callablestmtInsideGlobalTransaction = conn.prepareCall("select id from hold_30");

			System.out.println("CONNECTION(xa) HOLDABILITY " + (conn.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("STATEMENT(this one was created with holdability false, outside the global transaction. Check it's holdability inside global transaction) HOLDABILITY " + (s.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("STATEMENT(this one was created with holdability true, outside the global transaction. Check it's holdability inside global transaction) HOLDABILITY " + (sh.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("STATEMENT(this one was created with default holdability inside this global transaction. Check it's holdability) HOLDABILITY " + (stmtInsideGlobalTransaction.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("PREPAREDSTATEMENT(this one was created with default holdability inside this global transaction. Check it's holdability) HOLDABILITY " + (prepstmtInsideGlobalTransaction.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("CALLABLESTATEMENT(this one was created with default holdability inside this global transaction. Check it's holdability) HOLDABILITY " + (callablestmtInsideGlobalTransaction.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));

			ResultSet rsx = s.executeQuery("select id from hold_30 for update");
			 
			rsx.next(); System.out.println("X@1 id " + rsx.getInt(1));
			rsx.next(); System.out.println("X@2 id " + rsx.getInt(1));
			xr.end(xid, XAResource.TMSUCCESS);

			// result set should not be useable, since it is part of a detached
			// XAConnection
			try {
				rsx.next(); System.out.println("FAIL - rsx's connection not active id " + rsx.getInt(1));
			} catch (SQLException sqle) {
				System.out.println("Expected SQLException " + sqle.getMessage());
			}

			// result set should not be useable, it should have been closed by the xa start.
			try {
				rsh.next(); System.out.println("FAIL - rsh's should be closed " + rsx.getInt(1));
			} catch (SQLException sqle) {
				System.out.println("Expected SQLException " + sqle.getMessage());
			}

			System.out.println("resume XA transaction and keep using rs");
			xr.start(xid, XAResource.TMJOIN);
			Statement stmtAfterGlobalTransactionResume = conn.createStatement();
			PreparedStatement prepstmtAfterGlobalTransactionResume = conn.prepareStatement("select id from hold_30");
			CallableStatement callablestmtAfterGlobalTransactionResume = conn.prepareCall("select id from hold_30");

			System.out.println("Check holdability of various jdbc objects after resuming XA transaction");
			System.out.println("CONNECTION(xa) HOLDABILITY " + (conn.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("STATEMENT(this one was created with holdability false, outside the global transaction. Check it's holdability inside global transaction) HOLDABILITY " + (s.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("STATEMENT(this one was created with holdability true, outside the global transaction. Check it's holdability inside global transaction) HOLDABILITY " + (sh.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("STATEMENT(this one was created with default holdability inside the global transaction when it was first started. Check it's holdability) HOLDABILITY " + (stmtInsideGlobalTransaction.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("PREPAREDSTATEMENT(this one was created with default holdability inside the global transaction when it was first started. Check it's holdability) HOLDABILITY " + (prepstmtInsideGlobalTransaction.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("CALLABLESTATEMENT(this one was created with default holdability inside the global transaction when it was first started. Check it's holdability) HOLDABILITY " + (callablestmtInsideGlobalTransaction.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("STATEMENT(this one was created with default holdability after the global transaction was resumed. Check it's holdability) HOLDABILITY " + (stmtAfterGlobalTransactionResume.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("PREPAREDSTATEMENT(this one was created with default holdability after the global transaction was resumed. Check it's holdability) HOLDABILITY " + (prepstmtAfterGlobalTransactionResume.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			System.out.println("CALLABLESTATEMENT(this one was created with default holdability after the global transaction was resumed. Check it's holdability) HOLDABILITY " + (callablestmtAfterGlobalTransactionResume.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
		    // DERBY-1370			
			if (! stmtHoldabilityError)
			{
				// Network XA BUG gives result set closed
				rsx.next(); System.out.println("X@3 id " + rsx.getInt(1));
			}
			xr.end(xid, XAResource.TMSUCCESS);


			if (xr.prepare(xid) != XAResource.XA_RDONLY)
				xr.commit(xid, false);

			// try again once the xa transaction has been committed.			
			try {
				rsx.next(); System.out.println("FAIL - rsx's connection not active id (B)" + rsx.getInt(1));
			} catch (SQLException sqle) {
				System.out.println("Expected SQLException " + sqle.getMessage());
			}
			try {
				rsh.next(); System.out.println("FAIL - rsh's should be closed (B) " + rsx.getInt(1));
			} catch (SQLException sqle) {
				System.out.println("Expected SQLException " + sqle.getMessage());
			}

			System.out.println("Set connection to hold ");
			conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			System.out.println("CONNECTION(held) HOLDABILITY " + (conn.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));

			xid = getXid(24, (byte) 21, (byte) 01);
			xr.start(xid, XAResource.TMNOFLAGS);
			System.out.println("CONNECTION(xa) HOLDABILITY " + (conn.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
			try {
				conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
				System.out.println("FAIL allowed to set hold mode in xa transaction");
			} catch (SQLException sqle) {
				System.out.println("Expected SQLException(setHoldability) " + sqle.getMessage());
			}
			
            // JDBC 4.0 (proposed final draft) section 16.1.3.1 allows Statements to
            // be created with a different holdability if the driver cannot support it.
            // In this case the driver does not support holdability in a global transaction
            // so a valid statement is returned with close cursors on commit,
			
		    Statement shxa = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
            System.out.println("HOLDABLE Statement in global xact " +
                    (s.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT) +
                    " connection warning " + conn.getWarnings().getMessage());
            shxa.close();
            
		
           shxa = conn.prepareStatement("select id from hold_30",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
           System.out.println("HOLDABLE PreparedStatement in global xact " +
                   (s.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT) +
                   " connection warning " + conn.getWarnings().getMessage());
           shxa.close();
           
           shxa = conn.prepareCall("CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
           System.out.println("HOLDABLE CallableStatement in global xact " +
                   (s.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT) +
                   " connection warning " + conn.getWarnings().getMessage());
           shxa.close();
           
           
		   // check we can use a holdable statement set up in local mode.
           // holdability is downgraded, tested in XATest.java
  	       // DERBY-1370           
           if(! stmtHoldabilityError) {
           		System.out.println("STATEMENT HOLDABILITY " + (sh.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
				sh.executeQuery("select id from hold_30").close();
				sh.execute("select id from hold_30");
	            sh.getResultSet().close();
	
	            System.out.println("PREPARED STATEMENT HOLDABILITY " + (psh.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
				psh.executeQuery().close();
				psh.execute();
	            psh.getResultSet().close();
	
				System.out.println("CALLABLE STATEMENT HOLDABILITY " + (csh.getResultSetHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));
				csh.executeQuery().close();
				csh.execute();
	            csh.getResultSet().close();
           }    	
	            
			// but an update works
			sh.executeUpdate("insert into hold_30 values(10, 'init10')");

			xr.end(xid, XAResource.TMSUCCESS);
	
			System.out.println("CONNECTION(held) HOLDABILITY " + (conn.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT));

			conn.close();
			System.out.println("PASS XA HOLDABILITY TEST");

		} catch (XAException xae) {
			System.out.println("XAException error code " + xae.errorCode);
			xae.printStackTrace(System.out);
			Throwable t = xae.getCause();
			if (t instanceof SQLException)
				JDBCDisplayUtil.ShowSQLException(System.out, (SQLException) t);

		} catch (SQLException sqle) {
			JDBCDisplayUtil.ShowSQLException(System.out, sqle);
		} catch (Throwable t) {
			t.printStackTrace(System.out);
		}
		System.out.flush();
	}
    
    /**
     * Perform connection checks on the default connection
     * using checkDataSourc30.
     */
    public static void checkNesConn30(String dsName) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        new checkDataSource30().checkConnection(dsName, conn);            
    }

    
    /**
     * USe checkNesConn30 for the procedure, will
     * cause the 30 checks to be invoked as well. 
     */
    protected String getNestedMethodName()
    {
        return "checkDataSource30.checkNesConn30";
    }
    
    
    /**
     * Tests for DERBY-1144
     * 
     * This test tests that holdability, autocomit, and transactionIsolation are
     * reset  on getConnection for PooledConnections obtaind from connectionPoolDataSources 
     * 
     * DERBY-1134 has been filed for more comprehensive testing of client connection state. 
     * 
     * @throws SQLException
     */
    public static void testDerby1144() throws SQLException
    {
    	Connection conn = null;
    	PooledConnection pc1 = null;
		Properties p = new Properties();
		
		p.put("databaseName","sample");
		p.put("connectionAttributes","create=true");
		p.put("user","APP");
		p.put("password","pw");
		
        // Test holdability   
        ConnectionPoolDataSource ds = TestUtil.getConnectionPoolDataSource(p);
        pc1 = ds.getPooledConnection();
        testPooledConnHoldability("PooledConnection", pc1);
        pc1.close();
        
        // Test autocommit
        pc1 = ds.getPooledConnection();
        testPooledConnAutoCommit("PooledConnection", pc1);
        pc1.close();
        
        // Test pooled connection isolation
		ds = TestUtil.getConnectionPoolDataSource(p);
		pc1 = ds.getPooledConnection();
		testPooledConnIso("PooledConnection" , pc1);   
        pc1.close();
       
        // Test xa connection isolation
		XADataSource xds = TestUtil.getXADataSource(p);
    	XAConnection xpc1 = xds.getXAConnection();        
        testPooledConnIso("XAConnection", xpc1);                 
        xpc1.close();
      }
  
    
	/**
	 * Make sure autocommit gets reset on PooledConnection.getConnection()
	 * @param desc		description of connection
	 * @param pc1		pooled connection to test
	 * @throws SQLException
	 */
	private static void testPooledConnAutoCommit(String desc, PooledConnection pc1) throws SQLException {
		System.out.println("\n** Test autoCommit state for: " + desc + "**");
		Connection conn  = pc1.getConnection();
		conn.setAutoCommit(true);
		// reset the connection and see if the autocommit 
		conn = pc1.getConnection();
		boolean autocommit  = conn.getAutoCommit();
		if (autocommit != true)
		{
			new Exception("FAIL: autoCommit not reset on getConnection").printStackTrace(System.out);			
		}
		else 
		{
			System.out.println("PASS: autoCommit reset on getConnection");
		}
		conn.close();
	}


	/**
	 * Test Holdability gets reset on PooledConnection.getConnection()
	 * @param desc
	 * @param pc1
	 * @throws SQLException
	 */
	private static void testPooledConnHoldability(String desc, PooledConnection pc1) 
	throws SQLException { 
		System.out.println("\n**Test holdability state for: " + desc + " **");
		Connection conn  = pc1.getConnection();
		conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		// reset the connection and see if the holdability gets reset
		// to HOLD_CURSORS_OVER_COMMIT
		conn = pc1.getConnection();
		checkConnHoldability(conn, ResultSet.HOLD_CURSORS_OVER_COMMIT);
		conn.close();
	}
	


	/**
	 * Verify connection holdablity is expected holdability
	 * @param conn
	 * @param expectedHoldability 
	 * 	 * @throws SQLException
	 */
	private static void checkConnHoldability(Connection conn, 
				int expectedHoldability) throws SQLException {
		int holdability = conn.getHoldability();
		if (holdability != expectedHoldability)
		{
			new Exception("FAIL: Holdability:" + translateHoldability(holdability) +
					" does not match expected holdability:" +
						translateHoldability(expectedHoldability)).printStackTrace(System.out);			
		}
		else 
		{
			System.out.println("PASS: Holdability matches expected holdability:" +
					translateHoldability(expectedHoldability));
		}
	}


	/**
	 * Test that isolation is reset on PooledConnection.getConnection()
	 * @param pooledConnType   Descripiton of the type of pooled connection
	 * @param pc
	 * @throws SQLException
	 */
	private static void testPooledConnIso(String pooledConnType, PooledConnection pc)
	 throws SQLException {
			 Connection conn = pc.getConnection();
             
			 setupDerby1144Table(conn);
			 System.out.println("*** Test isolation level reset on " + pooledConnType+ ".getConnection()***");;			 
			 System.out.println("\nsetTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED");
			 conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			 checkIsoLocks(conn,Connection.TRANSACTION_READ_UNCOMMITTED);
			 
			 conn.close();
			 System.out.println("\nGet a new connection with " + pooledConnType+ ".getConnection()");
			 System.out.println("Isolation level should be reset to READ_COMMITTED");
			 Connection newconn = pc.getConnection();
			 checkIsoLocks(newconn,Connection.TRANSACTION_READ_COMMITTED);
			 
      
	}


	/**
	 * Make a simple table for DERBY-1144 tests
	 * @param conn
	 * @throws SQLException
	 */
	private static void  setupDerby1144Table(Connection conn) throws SQLException
	{
		Statement stmt = conn.createStatement();
		try {
			stmt.executeUpdate("DROP TABLE TAB1");
		}
		catch (SQLException e)
		{
			// ignore drop error
		}
		stmt.executeUpdate("CREATE TABLE TAB1(COL1 INT NOT NULL)");
		stmt.executeUpdate("INSERT INTO TAB1 VALUES(1)");
		stmt.executeUpdate("INSERT INTO TAB1 VALUES(2)");

        System.out.println("done creating  table");
             conn.commit ();
    }


	
	
	/* 
	 * Checks locks for designated isolation level on the connection.
	 * Currently only supports TRANSACTION_READ_COMMITTED and 
	 * TRANSACTION_READ_UNCOMMITTED
	 * @param conn   Connection to test
	 * @param isoLevel expected isolation level
	 *
	 */
	private static void checkIsoLocks(Connection conn, int expectedIsoLevel)
	{
		try {
			int conniso = conn.getTransactionIsolation();
			if (conniso !=  expectedIsoLevel)
			{
				new  Exception("FAIL: Connection isolation level " + 
						translateIso(conniso) + 
						" does not match expected level " +
						translateIso(expectedIsoLevel));
			}			

			boolean selectTimedOut  = selectTimesoutDuringUpdate(conn);
			switch (conniso) {
				case Connection.TRANSACTION_READ_UNCOMMITTED:
					if (selectTimedOut)
						new Exception("FAIL: Unexpected lock timeout for READ_UNCOMMITTED").printStackTrace(System.out);
					else
						System.out.println("PASS: No lock timeout occurs for READ_UNCOMMITTED");
					case Connection.TRANSACTION_READ_COMMITTED:
					if (selectTimedOut)	
						System.out.println("PASS: Expected lock timeout for READ_COMMITTED");
					else
						new Exception("FAIL: Did not get lock timeout for READ_COMMITTED");
					default:
						new Exception("No test support for isolation level" + 
									translateIso(conniso));
			}
		} catch (SQLException se) {
			se.printStackTrace();	
		}
	}
	
	/**
	 * Determine if a select on this connection during update will timeout.
	 * Used to establish isolation level.  If the connection isolation level
	 * is <code> Connection.TRANSACTION_READ_UNCOMMITTED </code> it will not
	 * timeout.  Otherwise it should.  
	 * 
	 * @param conn   Connection to test.
	 * @return  true if the select got a lock timeout, false otherwise.
	 */
	private static boolean selectTimesoutDuringUpdate(Connection conn) 	{
	 Connection updateConn = null;
	
		try {
		
		conn.setAutoCommit(false);
		// create another connection and do an update but don't commit
		updateConn = TestUtil.getConnection("sample","create=true");
		updateConn.setAutoCommit(false);
		
		
		// First update the rows on the update connection
		Statement upStmt = updateConn.createStatement();
		upStmt.executeUpdate("update tab1 set col1 = 3");

		// now see if we can select them

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("Select * from tab1");
		while (rs.next()){};
		rs.close();

		}
		catch (SQLException e)
		{
			if (e.getSQLState().equals("40XL1"))
			{
				// If we got a lock timeout this is not read uncommitted
				return true;
			}	

		}
		finally {

			try {
				conn.rollback();
				updateConn.rollback();
			}catch (SQLException  se) {
				se.printStackTrace();
			}
		}
		return false;
		
	}
	
	/**
	 * Translate holdability int readable format.
	 * 
	 * @param holdability   holdability to translate.
	 * @return  "HOLD_CURSORS_OVER_COMMIT" or "CLOSE_CURSORS_AT_COMMIT"
	 */
	public static String translateHoldability(int holdability)
	{
		switch (holdability)
		{
			case ResultSet.HOLD_CURSORS_OVER_COMMIT : return  "HOLD_CURSORS_OVER_COMMIT";
			case ResultSet.CLOSE_CURSORS_AT_COMMIT : return  "CLOSE_CURSORS_AT_COMMIT";
		}
		return "UNKNOWN_HOLDABILTY";
	}    

}
