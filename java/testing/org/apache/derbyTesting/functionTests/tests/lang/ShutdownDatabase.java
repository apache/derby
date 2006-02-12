/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ShutdownDatabase

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

package org.apache.derbyTesting.functionTests.tests.lang;

/**
   This test confirm 
   that no trouble happens when database , of which active connection exists with , was shut down.


   Tested combinations for transaction of connection to a database that is shutted down is as next :

   With committed/rollbacked transatction :
   <ul>
   <li>
   <ul>
   <li> The only transaction was committed.
   <li> The transaction was committed, and next transaction was committed.
   <li> The transaction was rollbacked, and next transaction was commited.
   </ul>
   </li>

   <li>
   <ul>
   <li> The only transaction was rollbacked.
   <li> The transaction was commited, and next transaction was rollbacked.
   <li> The transaction was rollbacked, and next transaction was rollbacked.
   </ul>
   </li>
   </ul>

   With not yet committed/rollbacked transaction :
   <ul>
   <li>
   <ul>
   <li> The only transaction was not committed/rollbacked.
   <li> The transaction was committed, and next transaction was not committed/rollbacked yet.
   <li>  The transaction was rollbacked, and next transaction was not committed/rollbacked yet.
   </ul>
   </li>
   </ul>
   
   @author Tomohito Nakayama
*/

import java.util.Properties;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.impl.tools.ij.util;

import org.apache.derby.iapi.error.StandardException;



public class ShutdownDatabase{
	
	
	public static void main(String[] args) {
		try{
			util.getPropertyArg(args);

			testShutDownWithCommitedTransaction();
			testShutDownWithRollbackedTransaction();
			testShutDownWithLeftTransaction();
			
		}catch(IOException e){
			e.printStackTrace();
			
		}catch(SQLException e){
			e.printStackTrace();
			
		}catch(ClassNotFoundException e){
			e.printStackTrace();
			
		}catch(InstantiationException e){
			e.printStackTrace();

		}catch(IllegalAccessException e){
			e.printStackTrace();
			
		}catch(Throwable t){
			t.printStackTrace();

		}
		
	}
	
	
	private static void testShutDownWithCommitedTransaction()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		testOnlyTransactionWasCommited();
		testTwiceCommited();
		testOnceRollbackedAndCommited();

	}
	
	
	private static void testShutDownWithRollbackedTransaction()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {

		testOnlyTransactionWasRollbacked();
		testOnceCommitedAndRollbacked();
		testTwiceRollbacked();

	}
	
	
	private static void testShutDownWithLeftTransaction()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {

		testOnlyTransactionWasLeft();
		testOnceCommitedAndLeft();
		testOnceRollbackedAndLeft();

	}


	private static void testOnlyTransactionWasCommited()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		final String dbname = "testOnlyTransactionWasCommitedDB";
		Connection conn = null;
		
		
		try{
			conn = openConnectionToNewDatabase(dbname);
			createTestTable(conn);

			conn.setAutoCommit(false);
			insertIntoTestTable(conn,
					    1,
					    1000);
			conn.commit();
			
			shutdownDatabase(dbname);
			
		}catch(SQLException e){
			verifyShutdownError(e);
		}
		
		
		conn = null;

		try{
			conn = reopenConnectionToDatabase(dbname);
			countRowInTestTable(conn);
			
		}finally{
			if(conn != null){
				conn.close();
				conn = null;
			}
		}
		
	}


	private static void testTwiceCommited()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		final String dbname = "testTwiceCommitedDB";
		Connection conn = null;
		

		try{
			conn = openConnectionToNewDatabase(dbname);
			createTestTable(conn);

			conn.setAutoCommit(false);
			insertIntoTestTable(conn,
					    1,
					    1000);
			conn.commit();
			insertIntoTestTable(conn,
					    1001,
					    999);
			conn.commit();

			shutdownDatabase(dbname);

		}catch(SQLException e){
			verifyShutdownError(e);
		}
		
		
		conn = null;
		
		try{
			conn = reopenConnectionToDatabase(dbname);
			countRowInTestTable(conn);
			
		}finally{
			if(conn != null){
				conn.close();
				conn = null;
			}
		}
		
	}


	private static void testOnceRollbackedAndCommited()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
	
		final String dbname = "testOnceRollbackedAndCommitedDB";
		Connection conn = null;
		

		try{
			conn = openConnectionToNewDatabase(dbname);
			createTestTable(conn);

			conn.setAutoCommit(false);
			insertIntoTestTable(conn,
					    1,
					    1000);
			conn.rollback();
			insertIntoTestTable(conn,
					    1001,
					    999);
			conn.commit();

			shutdownDatabase(dbname);
			
		}catch(SQLException e){
			verifyShutdownError(e);
		}
			

		conn = null;

		try{
			conn = reopenConnectionToDatabase(dbname);
			countRowInTestTable(conn);
			
		}finally{
			if(conn != null){
				conn.close();
				conn = null;
			}
		}
		
	}

	
	private static void testOnlyTransactionWasRollbacked()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		final String dbname = "testOnlyTransactionWasRollbackedDB";
		Connection conn = null;
		
		
		try{
			conn = openConnectionToNewDatabase(dbname);
			createTestTable(conn);

			conn.setAutoCommit(false);
			insertIntoTestTable(conn,
					    1,
					    1000);
			conn.rollback();
			
			shutdownDatabase(dbname);
			
		}catch(SQLException e){
			verifyShutdownError(e);
		}
		
		
		conn = null;
		
		try{
			conn = reopenConnectionToDatabase(dbname);
			countRowInTestTable(conn);
			
		}finally{
			if(conn != null){
				conn.close();
				conn = null;
			}
		}
		
	}


	private static void testOnceCommitedAndRollbacked()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		final String dbname = "testOnceCommitedAndRollbackedDB";
		Connection conn = null;
		
		
		try{
			conn = openConnectionToNewDatabase(dbname);
			createTestTable(conn);
			
			conn.setAutoCommit(false);
			insertIntoTestTable(conn,
					    1,
					    1000);
			conn.commit();
			insertIntoTestTable(conn,
					    1001,
					    999);
			conn.rollback();
			
			shutdownDatabase(dbname);

		}catch(SQLException e){
			verifyShutdownError(e);
		}
		
		
		conn = null;

		try{
			conn = reopenConnectionToDatabase(dbname);
			countRowInTestTable(conn);
			
		}finally{
			if(conn != null){
				conn.close();
				conn = null;
			}
		}
		
	}
	
	
	private static void testTwiceRollbacked()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		final String dbname = "testTwiceRollbackedDB";
		Connection conn = null;

		
		try{
			conn = openConnectionToNewDatabase(dbname);
			createTestTable(conn);

			conn.setAutoCommit(false);
			insertIntoTestTable(conn,
					    1,
					    1000);
			conn.rollback();
			insertIntoTestTable(conn,
					    1001,
					    999);
			conn.rollback();
			
			shutdownDatabase(dbname);
			
		}catch(SQLException e){
			verifyShutdownError(e);
		}
		
		
		conn = null;

		try{
			conn = reopenConnectionToDatabase(dbname);
			countRowInTestTable(conn);
			
		}finally{
			if(conn != null){
				conn.close();
				conn = null;
			}
		}
		
	}


	private static void testOnlyTransactionWasLeft()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		final String dbname = "testOnlyTransactionWasLeftDB";
		Connection conn = null;
		
		
		try{
			conn = openConnectionToNewDatabase(dbname);
			createTestTable(conn);

			conn.setAutoCommit(false);
			insertIntoTestTable(conn,
					    1,
					    1000);
			
			shutdownDatabase(dbname);

		}catch(SQLException e){
			verifyShutdownError(e);
		}

		
		conn = null;
		
		try{
			conn = reopenConnectionToDatabase(dbname);
			countRowInTestTable(conn);
			
		}finally{
			if(conn != null){
				conn.close();
				conn = null;
			}
		}
		
	}

	
	private static void testOnceCommitedAndLeft()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		final String dbname = "testOnceCommitedAndLeftDB";
		Connection conn = null;
		
		
		try{
			conn = openConnectionToNewDatabase(dbname);
			createTestTable(conn);

			conn.setAutoCommit(false);
			insertIntoTestTable(conn,
					    1,
					    1000);
			conn.commit();
			insertIntoTestTable(conn,
					    1001,
					    999);
			
			shutdownDatabase(dbname);

		}catch(SQLException e){
			verifyShutdownError(e);
		}

		
		conn = null;
		
		try{
			conn  = reopenConnectionToDatabase(dbname);
			countRowInTestTable(conn);
			
		}finally{
			if(conn != null){
				conn.close();
				conn = null;
			}
		}
		
	}


	private static void testOnceRollbackedAndLeft()
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		final String dbname = "testOnceRollbackedAndLeftDB";
		Connection conn = null;
		
		
		try{
			conn = openConnectionToNewDatabase(dbname);
			createTestTable(conn);

			conn.setAutoCommit(false);
			insertIntoTestTable(conn,
					    1,
					    1000);
			conn.rollback();
			insertIntoTestTable(conn,
					    1001,
					    999);
			
			shutdownDatabase(dbname);
			
		}catch(SQLException e){
			verifyShutdownError(e);
		}
		

		conn = null;

		try{
			conn = reopenConnectionToDatabase(dbname);
			countRowInTestTable(conn);

		}finally{
			if(conn != null){
				conn.close();
				conn = null;
			}
		}
		
	}
	

	private static Connection openConnectionToNewDatabase(String databaseName)
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		System.setProperty("database",
				   databaseName + ";create=true");
		
		Connection conn = util.startJBMS();
		
		System.out.println("A connection to " + databaseName + " was opened.");

		return conn;
		
	}


	private static Connection reopenConnectionToDatabase(String databaseName)
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		
		System.setProperty("database",
				   databaseName);

		return util.startJBMS();
		
	}
	
	
	private static void shutdownDatabase(String databaseName)
		throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {

		System.setProperty("database",
				   databaseName + ";shutdown=true");
		
		util.startJBMS();
		
		System.out.println(databaseName + " was shutted down.");
		
	}
	
	
	private static void createTestTable(Connection conn)
		throws SQLException{

		Statement st = null;
		
		try{
			st = conn.createStatement();
			st.execute( "create table " + 
				    "TEST_TABLE " + 
				    "( TEST_COL integer )" );
			
		}finally{
			if(st != null){
				st.close();
				st = null;
			}
		}
	}
	

	private static void insertIntoTestTable(Connection conn, 
						int val) 
		throws SQLException {
		
		PreparedStatement st = null;

		try{
			st = conn.prepareStatement( "insert into " + 
						    "TEST_TABLE " + 
						    "( TEST_COL ) " + 
						    "values( ? )" );
			st.setInt(1,val);
			st.execute();
			
		}finally{
			if(st != null){
				st.close();
				st = null;
			}
		}
	}
	
	
	private static void insertIntoTestTable(Connection conn, 
						int initialval, 
						int count)
		throws SQLException {

		for( int i = initialval ;
		     i < initialval + count ;
		     i ++ ){
			
			insertIntoTestTable(conn, i );
			
		}

	}
	
	
	private static void countRowInTestTable(Connection conn)
		throws SQLException {
		
		Statement st = null;
		ResultSet rs = null;

		try{
			st = conn.createStatement();
			rs = st.executeQuery( "select " + 
					      "count(*) " + 
					      "from " +
					      "TEST_TABLE " );
			
			rs.next();
			System.out.println(rs.getInt(1));

		}finally{

			if(rs != null){
				rs.close();
				rs = null;
			}
			
			if(st != null){
				st.close();
				st = null;
			}
			
		}
	}
	

	private static void verifyShutdownError(SQLException e)
		throws SQLException{
		
		if(!isShutdownError(e))
			throw e;
		
		System.out.println("SQLException of shutting down was found.");
		
	}


	private static boolean isShutdownError(SQLException e){
		return e.getSQLState().equals(StandardException.getSQLStateFromIdentifier(SQLState.SHUTDOWN_DATABASE));
	}
	
	
}
