/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.batchUpdate

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.functionTests.util.BigDecimalHandler;

public class batchUpdate { 
	
	private static boolean HAVE_BIG_DECIMAL;
	private static String CLASS_NAME;
	
	//Get the class name to be used for the procedures
	//outparams - J2ME; outparams30 - non-J2ME
	static{
		if(BigDecimalHandler.representation != BigDecimalHandler.BIGDECIMAL_REPRESENTATION)
			HAVE_BIG_DECIMAL = false;
		else
			HAVE_BIG_DECIMAL = true;
		if(HAVE_BIG_DECIMAL)
			CLASS_NAME = "org.apache.derbyTesting.functionTests.tests.lang.outparams30.";
		else
			CLASS_NAME = "org.apache.derbyTesting.functionTests.tests.lang.outparams.";
	}

	public static void main(String[] args) {
		boolean		passed = true;
		Connection	conn = null;
    Connection  conn2 = null;
		try {
			System.out.println("Test batchUpdate starting");

			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			conn2 = ij.startJBMS();
            passed = runTests( conn, conn2);
		} catch (SQLException se) {
			passed = false;
			dumpSQLExceptions(se);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception caught in main():\n");
			System.out.println(e.getMessage());
			e.printStackTrace();
			passed = false;
		}

		if (passed)
			System.out.println("PASS");

		System.out.println("Test batchUpdate finished");
  }

    // the runTests method is also used by the wascache/wsc_batchUpdate.java test.
    public static boolean runTests( Connection conn, Connection conn2)
    {
        boolean passed = true;
		Statement	stmt = null;
		Statement	stmt2 = null;

        try
        {
			conn.setAutoCommit(false);
            stmt = conn.createStatement();
			conn2.setAutoCommit(false);
            stmt2 = conn2.createStatement();

			/* Create the table and do any other set-up */
			passed = passed && setUpTest(conn, stmt);

			// Positive tests for statement batch update
			passed = passed && statementBatchUpdatePositive(conn, stmt);

			// Negative tests for statement batch update
			passed = passed && statementBatchUpdateNegative(conn, stmt, conn2, stmt2);

			// Positive tests for callable statement batch update
			passed = passed && callableStatementBatchUpdate(conn, stmt);

			// Positive tests for prepared statement batch update
			passed = passed && preparedStatementBatchUpdatePositive(conn, stmt);

			// Negative tests for prepared statement batch update
			passed = passed && preparedStatementBatchUpdateNegative(conn, stmt, conn2, stmt2);
        }            
        catch (SQLException se)
        {
			passed = false;
			dumpSQLExceptions(se);
		}
        catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception caught in main():\n");
			System.out.println(e.getMessage());
			e.printStackTrace();
			passed = false;
		}
        finally
        {
			/* Test is finished - clean up after ourselves */
			passed = passed && cleanUp(conn, stmt);
		}
        return passed;
    } // end of runTests
    
	static boolean callableStatementBatchUpdate( Connection conn, Statement stmt)
		throws SQLException
	{
		boolean 	passed = true;

    //try callable statements
    passed = passed && runCallableStatementBatch(conn);

    //try callable statement with output parameters
    passed = passed && runCallableStatementWithOutputParamBatch(conn);

    return passed;
  }


	/**
 	 * Positive tests for statement batch update.
	 *
	 * @param conn	The connection to use.
	 *
	 * @return	Whether or not we were successful.
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
	static boolean statementBatchUpdatePositive( Connection conn, Statement stmt)
		throws SQLException
	{
		boolean 	passed = true;

    //try executing a batch which nothing in it.
    passed = passed && runEmptyStatementBatch(conn, stmt);

    //try executing a batch which one statement in it.
    passed = passed && runSingleStatementBatch(conn, stmt);

    //try executing a batch with 3 different statements in it.
    passed = passed && runMultipleStatementsBatch(conn, stmt);

    //try executing a batch with 1000 statements in it.
    passed = passed && run1000StatementsBatch(conn, stmt);

    //try batch with autocommit true
    passed = passed && runAutoCommitTrueBatch(conn, stmt);

    //try clear batch
    passed = passed && runCombinationsOfClearBatch(conn, stmt);

	// confirm associated parameters run ok with batches
    passed = passed && checkAssociatedParams(conn, stmt);
	
    conn.commit();

    return passed;
  }

	/**
 	 * Negative tests for statement batch update.
	 *
	 * @param conn	The connection to use.
	 *
	 * @return	Whether or not we were successful.
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
	static boolean statementBatchUpdateNegative( Connection conn, Statement stmt,
    Connection conn2, Statement stmt2) throws SQLException 
	{
		boolean 	passed = true;

    //statements which will return a resultset are not allowed in batch update
    //the following case should throw an exception for select. Below trying
    //various placements of select statement in the batch, ie as 1st stmt,
    //nth stat and last stmt
    passed = passed && runStatementWithResultSetBatch(conn, stmt);

    //try executing a batch with regular statement intermingled.
    passed = passed && runStatementNonBatchStuffInBatch(conn, stmt);

    //Below trying various placements of overflow update statement in the batch, ie
    //as 1st stmt, nth stat and last stmt
    passed = passed && runStatementWithErrorsBatch(conn, stmt);

    //try transaction error, in this particular case time out while getting the lock
    passed = passed && runTransactionErrorBatch(conn, stmt, conn2, stmt2);

    return passed;
  }

	/**
 	 * Positive tests for prepared statement batch update.
	 *
	 * @param conn	The connection to use.
	 *
	 * @return	Whether or not we were successful.
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
	static boolean preparedStatementBatchUpdatePositive(Connection conn, Statement stmt)
		throws SQLException 
	{
		boolean 	passed = true;

    //try executing a batch which nothing in it.
    passed = passed && runEmptyValueSetPreparedBatch(conn, stmt);

    //try executing a batch with no parameters.
    passed = passed && runNoParametersPreparedBatch(conn, stmt);

    //try executing a batch which one parameter set in it.
    passed = passed && runSingleValueSetPreparedBatch(conn, stmt);

    //try executing a batch with 3 parameter sets in it.
    passed = passed && runMultipleValueSetPreparedBatch(conn, stmt);

    //try executing a batch with 2 parameter sets in it and they are set to null.
    passed = passed && runMultipleValueSetNullPreparedBatch(conn, stmt);

    //try executing a batch with 1000 statements in it.
    passed = passed && run1000ValueSetPreparedBatch(conn, stmt);

    //try executing batches with various rollback and commit combinations.
    passed = passed && runPreparedStatRollbackAndCommitCombinations(conn, stmt);

    //try prepared statement batch with autocommit true
    passed = passed && runAutoCommitTruePreparedStatBatch(conn, stmt);

    //try clear batch
    passed = passed && runCombinationsOfClearPreparedStatBatch(conn, stmt);

    return passed;
  }

	/**
 	 * Negative tests for prepared statement batch update.
	 *
	 * @param conn	The connection to use.
	 *
	 * @return	Whether or not we were successful.
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */
	static boolean preparedStatementBatchUpdateNegative(Connection conn, Statement stmt,
    Connection conn2, Statement stmt2) throws SQLException 
	{
		boolean 	passed = true;

    //statements which will return a resultset are not allowed in batch update
    //the following case should throw an exception for select.
    passed = passed && runPreparedStmtWithResultSetBatch(conn, stmt);

    //try executing a batch with regular statement intermingled.
    passed = passed && runPreparedStmtNonBatchStuffInBatch(conn, stmt);

    //Below trying various placements of overflow update statement in the batch
    passed = passed && runPreparedStmtWithErrorsBatch(conn, stmt);

    //try transaction error, in this particular case time out while getting the lock
    passed = passed && runTransactionErrorPreparedStmtBatch(conn, stmt, conn2, stmt2);

    return passed;
  }


	static public void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se.printStackTrace();
			se = se.getNextException();
		}
	}

	/**
	 * Check to make sure that the given SQLException is an exception
	 * with the expected sqlstate.
	 *
	 * @param e		The SQLException to check
	 * @param SQLState	The sqlstate to look for
	 *
	 * @return	true means the exception is the expected one
	 */

	private static boolean checkException(SQLException e,
											String SQLState)
	{
		String				state;
		String				nextState;
		SQLException		next;
		boolean				passed = true;

		state = e.getSQLState();


		if (! SQLState.equals(state)) {
				System.out.println("FAIL -- unexpected exception " + e +
					"sqlstate: " + state + SQLState);
				passed = false;
			}

		return passed;
	}

	/**
	 * Clean up after ourselves when testing is done.
	 *
	 * @param conn	The Connection
	 * @param s		A Statement on the Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean cleanUp(Connection conn, Statement s) {
    boolean passed = true;
		try {
			/* Drop the table we created */
			if (s != null)
			{
				s.execute("drop table t1");
				s.execute("drop table datetab");
				s.execute("drop table timetab");
				s.execute("drop table timestamptab");
				s.execute("drop table usertypetab");
				s.execute("drop procedure Integ");
			}

			/* Close the connection */
			if (conn != null) {
				conn.rollback();
				conn.close();
			}
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception caught in cleanup()");
			JDBCDisplayUtil.ShowException(System.out, e);
			passed = false;
		}

		return passed;
	}

  //Below trying placements of overflow update statement in the batch
  static boolean runPreparedStmtWithErrorsBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;
    PreparedStatement pStmt = null;

    stmt.executeUpdate("insert into t1 values(1)");

		try
		{
      System.out.println("Negative Prepared Stat: testing overflow as first set of values");
      pStmt = conn.prepareStatement("update t1 set c1=(? + 1)");
      pStmt.setInt(1, java.lang.Integer.MAX_VALUE);
      pStmt.addBatch();
      updateCount = pStmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the one we expect */
			passed = passed && checkException(sqle, "22003");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount != null) {
          if (updateCount.length != 0) {
            System.out.println("ERROR: Overflow is first statement in the batch, so there shouldn't have been any update count");
            passed = false;
          }
        } 
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 1) {
      System.out.println("ERROR: There should been 1 row in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      System.out.println("Negative Prepared Stat: testing overflow as nth set of values");
      pStmt = conn.prepareStatement("update t1 set c1=(? + 1)");
      pStmt.setInt(1, 1);
      pStmt.addBatch();
      pStmt.setInt(1, java.lang.Integer.MAX_VALUE);
      pStmt.addBatch();
      pStmt.setInt(1, 1);
      pStmt.addBatch();
      updateCount = pStmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the one we expect */
			passed = passed && checkException(sqle, "22003");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount.length != 1) {
          System.out.println("ERROR: Overflow is second statement in the batch, so there should have been only 1 update count");
          passed = false;
        }
        for (int i=0; i<updateCount.length; i++) {
          if (updateCount[i] != 1) {
            System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
            passed = false;
          }
        }
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 1) {
      System.out.println("There should been 1 row in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      //trying select as the last statement
      System.out.println("Negative Prepared Stat: testing overflow as last set of values");
      pStmt = conn.prepareStatement("update t1 set c1=(? + 1)");
      pStmt.setInt(1, 1);
      pStmt.addBatch();
      pStmt.setInt(1, 1);
      pStmt.addBatch();
      pStmt.setInt(1, java.lang.Integer.MAX_VALUE);
      pStmt.addBatch();
      updateCount = pStmt.executeBatch();
			passed = false;
		}
		catch (SQLException sqle) {
			/* Check to be sure the exception is the one we expect */
			passed = passed && checkException(sqle, "22003");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount.length != 2) {
          System.out.println("ERROR: Overflow is last statement in the batch, so there should have been only 2 update count");
          passed = false;
        }
        for (int i=0; i<updateCount.length; i++) {
          if (updateCount[i] != 1) {
            System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
            passed = false;
          }
        }
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 1) {
      System.out.println("There should been 1 row in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();
    pStmt.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return(passed);
  }

  //Below trying various placements of overflow update statement in the batch, ie
  //as 1st stmt, nth stat and last stmt
  static boolean runStatementWithErrorsBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;

    stmt.executeUpdate("insert into t1 values(1)");

		try
		{
      //trying select as the first statement
      System.out.println("Negative Statement: statement testing overflow error as first stat in the batch");
      stmt.addBatch("update t1 set c1=2147483647 + 1");
      stmt.addBatch("insert into t1 values(1)");
      updateCount = stmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the one we expect */
			passed = passed && checkException(sqle, "22003");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount != null) {
          if (updateCount.length != 0) {
            System.out.println("ERROR: Overflow is first statement in the batch, so there shouldn't have been any update count");
            passed = false;
          }
        } 
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 1) {
      System.out.println("ERROR: There should been 1 row in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      //trying select as the nth statement
      System.out.println("Negative Statement: statement testing overflow error as nth stat in the batch");
      stmt.addBatch("insert into t1 values(1)");
      stmt.addBatch("update t1 set c1=2147483647 + 1");
      stmt.addBatch("insert into t1 values(1)");
      updateCount = stmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the one we expect */
			passed = passed && checkException(sqle, "22003");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount.length != 1) {
          System.out.println("ERROR: Update is second statement in the batch, so there should have been only 1 update count");
          passed = false;
        }
        for (int i=0; i<updateCount.length; i++) {
          if (updateCount[i] != 1) {
            System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
            passed = false;
          }
        }
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 2) {
      System.out.println("There should been 2 row in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      //trying select as the last statement
      System.out.println("Negative Statement: statement testing overflow error as last stat in the batch");
      stmt.addBatch("insert into t1 values(1)");
      stmt.addBatch("insert into t1 values(1)");
      stmt.addBatch("update t1 set c1=2147483647 + 1");
      updateCount = stmt.executeBatch();
			passed = false;
		}
		catch (SQLException sqle) {
			/* Check to be sure the exception is the one we expect */
			passed = passed && checkException(sqle, "22003");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount.length != 2) {
          System.out.println("ERROR: Update is last statement in the batch, so there should have been only 2 update count");
          passed = false;
        }
        for (int i=0; i<updateCount.length; i++) {
          if (updateCount[i] != 1) {
            System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
            passed = false;
          }
        }
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 4) {
      System.out.println("There should been 4 rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return(passed);
  }

  //try transaction error, in this particular case time out while getting the lock
  static boolean runTransactionErrorPreparedStmtBatch(Connection conn, Statement stmt,
    Connection conn2, Statement stmt2) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;

		try
		{
      System.out.println("Negative Prepared Stat: testing transaction error, time out while getting the lock");

      stmt.execute("insert into t1 values(1)");
      stmt2.execute("insert into t1 values(2)");

      PreparedStatement pStmt1 = conn.prepareStatement("update t1 set c1=3 where c1=?");
      pStmt1.setInt(1, 2);
      pStmt1.addBatch();

      PreparedStatement pStmt2 = conn.prepareStatement("update t1 set c1=4 where c1=?");
      pStmt2.setInt(1, 1);
      pStmt2.addBatch();

      pStmt1.executeBatch();
      pStmt2.executeBatch();
		} catch (SQLException sqle) {
			/* Check to be sure the exception is time out while getting the lock related */
			passed = passed && checkException(sqle, "40XL1");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount != null) {
          if (updateCount.length != 0) {
            System.out.println("ERROR: first statement in the batch caused time out while getting the lock, so there shouldn't have been any update count");
            passed = false;
          }
        }
      }
		}

    conn.rollback();
    conn2.rollback();
    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try transaction error, in this particular case time out while getting the lock
  static boolean runTransactionErrorBatch(Connection conn, Statement stmt,
    Connection conn2, Statement stmt2) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;

		try
		{
      System.out.println("Negative Statement: statement testing time out while getting the lock in the batch");

      stmt.execute("insert into t1 values(1)");
      stmt2.execute("insert into t1 values(2)");

      stmt.addBatch("update t1 set c1=3 where c1=2");
      stmt2.addBatch("update t1 set c1=4 where c1=1");

      stmt.executeBatch();
      updateCount = stmt2.executeBatch();
		} catch (SQLException sqle) {
			/* Check to be sure the exception is time out while getting the lock related */
			passed = passed && checkException(sqle, "40XL1");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount != null) {
          if (updateCount.length != 0) {
            System.out.println("ERROR: first statement in the batch caused time out while getting the lock, so there shouldn't have been any update count");
            passed = false;
          }
        }
      }
		}

    conn.rollback();
    conn2.rollback();
    stmt.clearBatch();
    stmt2.clearBatch();
    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //statements which will return a resultset are not allowed in batch update
  //the following case should throw an exception for select.
  static boolean runPreparedStmtWithResultSetBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;

		try
		{
      //trying select as the first statement
      System.out.println("Negative Prepared Stat: testing select in the batch");
      PreparedStatement pStmt = conn.prepareStatement("select * from t1 where c1=?");
      pStmt.setInt(1, 1);
      pStmt.addBatch();
      updateCount = pStmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the ResultSetReturnNotAllowed */
			passed = passed && checkException(sqle, "X0Y79");
        if (sqle instanceof BatchUpdateException) {
          updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
          if (updateCount != null) {
            if (updateCount.length != 0) {
              System.out.println("ERROR: Select is first statement in the batch, so there shouldn't have been any update count");
              passed = false;
            }
          }
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing a batch with regular statement intermingled.
  static boolean runPreparedStmtNonBatchStuffInBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;

		try
		{
      //trying execute in the middle of batch
      System.out.println("Negative Prepared Stat: testing execute in the middle of batch");
      PreparedStatement pStmt = conn.prepareStatement("select * from t1 where c1=?");
      pStmt.setInt(1, 1);
      pStmt.addBatch();
      pStmt.execute();
      updateCount = pStmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the MIDDLE_OF_BATCH */
			passed = passed && checkException(sqle, "XJ068");
			// do clearBatch so we can proceed
			if (checkException(sqle, "XJ068"))
        stmt.clearBatch();
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      //trying executeQuery in the middle of batch
      System.out.println("Negative Prepared Stat: testing executeQuery in the middle of batch");
      PreparedStatement pStmt = conn.prepareStatement("select * from t1 where c1=?");
      pStmt.setInt(1, 1);
      pStmt.addBatch();
      pStmt.executeQuery();
      updateCount = pStmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the MIDDLE_OF_BATCH */
			passed = passed && checkException(sqle, "XJ068");
			// do clearBatch so we can proceed
			if (checkException(sqle, "XJ068"))
        stmt.clearBatch();
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      //trying executeUpdate in the middle of batch
      System.out.println("Negative Prepared Stat: testing executeUpdate in the middle of batch");
      PreparedStatement pStmt = conn.prepareStatement("select * from t1 where c1=?");
      pStmt.setInt(1, 1);
      pStmt.addBatch();
      pStmt.executeUpdate();
      updateCount = pStmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the MIDDLE_OF_BATCH */
			passed = passed && checkException(sqle, "XJ068");
			// do clearBatch so we can proceed
			if (checkException(sqle, "XJ068"))
        stmt.clearBatch();
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //statements which will return a resultset are not allowed in batch update
  //the following case should throw an exception for select. Below trying
  //various placements of select statement in the batch, ie as 1st stmt,
  //nth stat and last stmt
  static boolean runStatementWithResultSetBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;

		try
		{
      //trying select as the first statement
      System.out.println("Negative Statement: statement testing select as first stat in the batch");
      stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
      stmt.addBatch("insert into t1 values(1)");
      updateCount = stmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the ResultSetReturnNotAllowed */
			passed = passed && checkException(sqle, "X0Y79");
        if (sqle instanceof BatchUpdateException) {
          updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
          if (updateCount != null) {
            if (updateCount.length != 0) {
              System.out.println("ERROR: Select is first statement in the batch, so there shouldn't have been any update count");
              passed = false;
            }
          } 
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      //trying select as the nth statement
      System.out.println("Negative Statement: statement testing select as nth stat in the batch");
      stmt.addBatch("insert into t1 values(1)");
      stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
      stmt.addBatch("insert into t1 values(1)");
      updateCount = stmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the ResultSetReturnNotAllowed */
			passed = passed && checkException(sqle, "X0Y79");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount.length != 1) {
          System.out.println("ERROR: Select is second statement in the batch, so there should have been only 1 update count");
          passed = false;
        }
        for (int i=0; i<updateCount.length; i++) {
          if (updateCount[i] != 1) {
            System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
            passed = false;
          }
        }
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 1) {
      System.out.println("There should been 1 row in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      //trying select as the last statement
      System.out.println("Negative Statement: statement testing select as last stat in the batch");
      stmt.addBatch("insert into t1 values(1)");
      stmt.addBatch("insert into t1 values(1)");
      stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
      updateCount = stmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the ResultSetReturnNotAllowed */
			passed = passed && checkException(sqle, "X0Y79");
      if (sqle instanceof BatchUpdateException) {
        updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
        if (updateCount.length != 2) {
          System.out.println("ERROR: Select is last statement in the batch, so there should have been only 2 update count");
          passed = false;
        }
        for (int i=0; i<updateCount.length; i++) {
          if (updateCount[i] != 1) {
            System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
            passed = false;
          }
        }
      }
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 3) {
      System.out.println("There should been 3 row in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    conn.rollback();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing a batch with regular statement intermingled.
  static boolean runStatementNonBatchStuffInBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;

		try
		{
      //trying execute after addBatch
      System.out.println("Negative Statement: statement testing execute in the middle of batch");
      stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
      stmt.execute("insert into t1 values(1)");
      stmt.addBatch("insert into t1 values(1)");
      updateCount = stmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the MIDDLE_OF_BATCH */
			passed = passed && checkException(sqle, "XJ068");
			// do clearBatch so we can proceed
			if (checkException(sqle, "XJ068"))
        stmt.clearBatch();
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      //trying executeQuery after addBatch
      System.out.println("Negative Statement: statement testing executeQuery in the middle of batch");
      stmt.addBatch("insert into t1 values(1)");
      stmt.executeQuery("SELECT * FROM SYS.SYSTABLES");
      updateCount = stmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the MIDDLE_OF_BATCH */
			passed = passed && checkException(sqle, "XJ068");
			// do clearBatch so we can proceed
			if (checkException(sqle, "XJ068"))
        stmt.clearBatch();
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

		try
		{
      //trying executeUpdate after addBatch
      System.out.println("Negative Statement: statement testing executeUpdate in the middle of batch");
      stmt.addBatch("insert into t1 values(1)");
      stmt.executeUpdate("insert into t1 values(1)");
      stmt.addBatch("insert into t1 values(1)");
      stmt.addBatch("SELECT * FROM SYS.SYSCOLUMNS");
      updateCount = stmt.executeBatch();
			passed = false;
		} catch (SQLException sqle) {
			/* Check to be sure the exception is the MIDDLE_OF_BATCH */
			passed = passed && checkException(sqle, "XJ068");
			// do clearBatch so we can proceed
			if (checkException(sqle, "XJ068"))
        stmt.clearBatch();
		}

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    conn.rollback();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing batches with various rollback and commit combinations.
  static boolean runPreparedStatRollbackAndCommitCombinations(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    System.out.println("Positive Prepared Stat: batch, rollback, batch and commit combinations");
    PreparedStatement pStmt = conn.prepareStatement("insert into t1 values(?)");
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.rollback();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should have been 0 rows");
      passed = false;
    }
    rs.close();

    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.commit();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 2) {
      System.out.println("ERROR: There should have been 2 rows");
      passed = false;
    }
    rs.close();

    //try batch and commit
    System.out.println("Positive Prepared Stat: batch and commit combinations");
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.commit();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 4) {
      System.out.println("ERROR: There should have been 4 rows");
      passed = false;
    }
    rs.close();

    //try batch, batch and rollback
    System.out.println("Positive Prepared Stat: batch, batch and rollback combinations");
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.rollback();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 4) {
      System.out.println("ERROR: There should have been 4 rows");
      passed = false;
    }
    rs.close();

    //try batch, batch and commit
    System.out.println("Positive Prepared Stat: batch, batch and commit combinations");
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.commit();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 8) {
      System.out.println("ERROR: There should have been 8 rows");
      passed = false;
    }
    rs.close();

    pStmt.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing batches with various rollback and commit combinations.
  static boolean runRollbackAndCommitCombinations(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    System.out.println("Positive Statement: batch, rollback, batch and commit combinations");
    stmt.addBatch("insert into t1 values(1)");
    stmt.addBatch("insert into t1 values(1)");
    updateCount = stmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.rollback();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should have been 0 rows");
      passed = false;
    }
    rs.close();

    stmt.addBatch("insert into t1 values(1)");
    stmt.addBatch("insert into t1 values(1)");
    updateCount = stmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.commit();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 2) {
      System.out.println("ERROR: There should have been 2 rows");
      passed = false;
    }
    rs.close();

    //try batch and commit
    System.out.println("Positive Statement: batch and commit combinations");
    stmt.addBatch("insert into t1 values(1)");
    stmt.addBatch("insert into t1 values(1)");
    updateCount = stmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.commit();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 4) {
      System.out.println("ERROR: There should have been 4 rows");
      passed = false;
    }
    rs.close();

    //try batch, batch and rollback
    System.out.println("Positive Statement: batch, batch and rollback combinations");
    stmt.addBatch("insert into t1 values(1)");
    stmt.addBatch("insert into t1 values(1)");
    updateCount = stmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    stmt.addBatch("insert into t1 values(1)");
    stmt.addBatch("insert into t1 values(1)");
    updateCount = stmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.rollback();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 4) {
      System.out.println("ERROR: There should have been 4 rows");
      passed = false;
    }
    rs.close();

    //try batch, batch and commit
    System.out.println("Positive Statement: batch, batch and rollback combinations");
    stmt.addBatch("insert into t1 values(1)");
    stmt.addBatch("insert into t1 values(1)");
    updateCount = stmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    stmt.addBatch("insert into t1 values(1)");
    stmt.addBatch("insert into t1 values(1)");
    updateCount = stmt.executeBatch();

    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    conn.commit();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 8) {
      System.out.println("ERROR: There should have been 8 rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try prepared statement batch with autocommit true
  static boolean runAutoCommitTruePreparedStatBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

		conn.setAutoCommit(true);
   //prepared statement batch with autocommit true
    System.out.println("Positive Prepared Stat: testing batch with autocommit true");
    PreparedStatement pStmt = conn.prepareStatement("insert into t1 values(?)");
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 3) {
      System.out.println("ERROR: there were 3 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 3) {
      System.out.println("ERROR: There should been 3 rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();
    pStmt.close();

    //turn it true again after the above negative test
		conn.setAutoCommit(false);

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

    //try batch with autocommit true
  static boolean runAutoCommitTrueBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

		conn.setAutoCommit(true);
     //try batch with autocommit true
    System.out.println("Positive Statement: statement testing batch with autocommit true");
    stmt.addBatch("insert into t1 values(1)");
    stmt.addBatch("insert into t1 values(1)");
    stmt.addBatch("delete from t1");
    updateCount = stmt.executeBatch();

    if (updateCount.length != 3) {
      System.out.println("ERROR: there were 3 statements in the batch");
      passed = false;
    }

    for (int i=0; i<(updateCount.length-1); i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }
    if (updateCount[2] != 2) {
      System.out.println("ERROR: update count for stat 2 should have been 2 but it is " + updateCount[2]);
      passed = false;
    }

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should been no rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    //turn it true again after the above negative test
		conn.setAutoCommit(false);

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try callable statements with output parameters
  static boolean runCallableStatementWithOutputParamBatch(Connection conn) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;

    System.out.println("Negative Callable Statement: callable statement with output parameters in the batch");
		Statement s = conn.createStatement();

		s.execute("CREATE PROCEDURE takesString(OUT P1 VARCHAR(40), IN P2 INT) " +
				"EXTERNAL NAME '" + CLASS_NAME + "takesString'" +
				" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");

  	CallableStatement cs = conn.prepareCall("call takesString(?,?)");
		try
		{

		cs.registerOutParameter(1, Types.CHAR);
		cs.setInt(2, Types.INTEGER);
    cs.addBatch();
		System.out.println("FAIL - addBatch() allowed with registered out parameter");
		passed = false;
		}
		catch (SQLException sqle)
		{
			// Check to be sure the exception is callback related
			passed = passed && checkException(sqle, "XJ04C");
		}

    cs.close();
				s.execute("drop procedure takesString");
				s.close();
    conn.rollback();
    conn.commit();
    return passed;
  }

  //try callable statements
  static boolean runCallableStatementBatch(Connection conn) throws SQLException {
    boolean passed = true;
    int updateCount[] = null;
    ResultSet rs;

    System.out.println("Positive Callable Statement: statement testing callable statement batch");
  	CallableStatement cs = conn.prepareCall("insert into t1 values(?)");

	cs.setInt(1, 1);
    cs.addBatch();
	cs.setInt(1,2);
    cs.addBatch();
	try
	{
		passed = passed && executeBatchCallableStatement(cs);
	}
	catch (SQLException sqle)
	{
		/* Check to be sure the exception is callback related */
		passed = passed && checkException(sqle, "XJ04C");
        if (sqle instanceof BatchUpdateException) 
		{
			updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
			if (updateCount != null) 
			{
				if (updateCount.length != 0) 
				{
					System.out.println("ERROR: callable statement has output parameter, so there shouldn't have been any update count");
              		passed = false;
            	}
          	} 
      	}
	}

	cleanUpCallableStatement(conn, cs, "t1");

	/* Bug 2813 - verify setXXXX() works with
	 * Date, Time and Timestamp on CallableStatement.
	 */
	cs = conn.prepareCall("insert into datetab values(?)");

	cs.setDate(1, Date.valueOf("1990-05-05"));
    cs.addBatch();
	cs.setDate(1,Date.valueOf("1990-06-06"));
    cs.addBatch();
	try
	{
		passed = passed && executeBatchCallableStatement(cs);
	}
	catch (SQLException sqle)
	{
		/* Check to be sure the exception is callback related */
		passed = passed && checkException(sqle, "XJ04C");
        if (sqle instanceof BatchUpdateException) 
		{
			updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
			if (updateCount != null) 
			{
				if (updateCount.length != 0) 
				{
					System.out.println("ERROR: callable statement has output parameter, so there shouldn't have been any update count");
              		passed = false;
            	}
          	} 
      	}
	}

	cleanUpCallableStatement(conn, cs, "datetab");
 
	cs = conn.prepareCall("insert into timetab values(?)");

	cs.setTime(1, Time.valueOf("11:11:11"));
    cs.addBatch();
	cs.setTime(1, Time.valueOf("12:12:12"));
    cs.addBatch();
	try
	{
		passed = passed && executeBatchCallableStatement(cs);
	}
	catch (SQLException sqle)
	{
		/* Check to be sure the exception is callback related */
		passed = passed && checkException(sqle, "XJ04C");
        if (sqle instanceof BatchUpdateException) 
		{
			updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
			if (updateCount != null) 
			{
				if (updateCount.length != 0) 
				{
					System.out.println("ERROR: callable statement has output parameter, so there shouldn't have been any update count");
              		passed = false;
            	}
          	} 
      	}
	}

	cleanUpCallableStatement(conn, cs, "timestamptab");
 
	cs = conn.prepareCall("insert into timestamptab values(?)");

	cs.setTimestamp(1, Timestamp.valueOf("1990-05-05 11:11:11.1"));
    cs.addBatch();
	cs.setTimestamp(1, Timestamp.valueOf("1992-07-07 12:12:12.2"));
    cs.addBatch();
	try
	{
		passed = passed && executeBatchCallableStatement(cs);
	}
	catch (SQLException sqle)
	{
		/* Check to be sure the exception is callback related */
		passed = passed && checkException(sqle, "XJ04C");
        if (sqle instanceof BatchUpdateException) 
		{
			updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
			if (updateCount != null) 
			{
				if (updateCount.length != 0) 
				{
					System.out.println("ERROR: callable statement has output parameter, so there shouldn't have been any update count");
              		passed = false;
            	}
          	} 
      	}
	}

	cleanUpCallableStatement(conn, cs, "timestamptab");

	// Try with a user type
	cs = conn.prepareCall("insert into usertypetab values(?)");

	cs.setObject(1, Date.valueOf("1990-05-05"));
    cs.addBatch();
	cs.setObject(1,Date.valueOf("1990-06-06"));
    cs.addBatch();
	try
	{
		passed = passed && executeBatchCallableStatement(cs);
	}
	catch (SQLException sqle)
	{
		/* Check to be sure the exception is callback related */
		passed = passed && checkException(sqle, "XJ04C");
        if (sqle instanceof BatchUpdateException) 
		{
			updateCount = ((BatchUpdateException)sqle).getUpdateCounts();
			if (updateCount != null) 
			{
				if (updateCount.length != 0) 
				{
					System.out.println("ERROR: callable statement has output parameter, so there shouldn't have been any update count");
              		passed = false;
            	}
          	} 
      	}
	}

	cleanUpCallableStatement(conn, cs, "usertypetab");
 
	return passed;
	}

	private static boolean executeBatchCallableStatement(CallableStatement cs)
		throws SQLException
	{
		boolean passed = true;
		int updateCount[];

		updateCount = cs.executeBatch();
		if (updateCount.length != 2)
		{
			System.out.println("ERROR: there were 2 statements in the batch");
			passed = false;
		}
		for (int i=0; i<updateCount.length; i++) 
		{
			if (updateCount[i] != 1) 
			{
				System.out.println("ERROR: update count should have been 1 but it's " + updateCount[i]);
				passed = false;
			}
		}

		return passed;
	}

	private static void cleanUpCallableStatement(Connection conn, CallableStatement cs, String tableName)
		throws SQLException
	{
		cs.close();
		conn.rollback();
		cs = conn.prepareCall("delete from " + tableName);
		cs.executeUpdate();
		cs.close();
		conn.commit();
	}

  //try combinations of clear batch.
  static boolean runCombinationsOfClearPreparedStatBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    System.out.println("Positive Prepared Stat: add 3 statements, clear batch and execute batch");
    PreparedStatement pStmt = conn.prepareStatement("insert into t1 values(?)");
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 2);
    pStmt.addBatch();
    pStmt.setInt(1, 3);
    pStmt.addBatch();
    pStmt.clearBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 0) {
      System.out.println("ERROR: there were 0 statements in the batch");
      passed = false;
    }
    
    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should been no rows in the table");
      passed = false;
    }
    rs.close();

    System.out.println("Positive Prepared Stat: add 3 statements, clear batch, add 3 and execute batch");
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 2);
    pStmt.addBatch();
    pStmt.setInt(1, 3);
    pStmt.addBatch();
    pStmt.clearBatch();
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 2);
    pStmt.addBatch();
    pStmt.setInt(1, 3);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 3) {
      System.out.println("ERROR: there were 3 statements in the batch");
      passed = false;
    }
    
    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 3) {
      System.out.println("ERROR: There should been 3 rows in the table");
      passed = false;
    }
    rs.close();
    pStmt.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try combinations of clear batch.
  static boolean runCombinationsOfClearBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    System.out.println("Positive Statement: add 3 statements, clear batch and execute batch");
    stmt.addBatch("insert into t1 values(2)");
    stmt.addBatch("insert into t1 values(2)");
    stmt.addBatch("insert into t1 values(2)");
    stmt.clearBatch();
    updateCount = stmt.executeBatch();

    if (updateCount.length != 0) {
      System.out.println("ERROR: there were 0 statements in the batch");
      passed = false;
    }
    
    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should been no rows in the table");
      passed = false;
    }
    rs.close();

    System.out.println("Positive Statement: add 3 statements, clear batch, add 3 and execute batch");
    stmt.addBatch("insert into t1 values(2)");
    stmt.addBatch("insert into t1 values(2)");
    stmt.addBatch("insert into t1 values(2)");
    stmt.clearBatch();
    stmt.addBatch("insert into t1 values(2)");
    stmt.addBatch("insert into t1 values(2)");
    stmt.addBatch("insert into t1 values(2)");
    updateCount = stmt.executeBatch();

    if (updateCount.length != 3) {
      System.out.println("ERROR: there were 3 statements in the batch");
      passed = false;
    }
    
    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 3) {
      System.out.println("ERROR: There should been 3 rows in the table");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing a batch with 1000 statements in it.
  static boolean run1000ValueSetPreparedBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    System.out.println("Positive Prepared Stat: 1000 parameter set batch");
    PreparedStatement pStmt = conn.prepareStatement("insert into t1 values(?)");
    for (int i=0; i<1000; i++){
      pStmt.setInt(1, 1);
      pStmt.addBatch();
    }
    updateCount = pStmt.executeBatch();
    
    if (updateCount.length != 1000) {
      System.out.println("ERROR: there were 1000 parameter sets in the batch");
      passed = false;
    }

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 1000) {
      System.out.println("There should been 1000 rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    pStmt.close();
    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing a batch with 1000 statements in it.
  static boolean run1000StatementsBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    System.out.println("Positive Statement: 1000 statements batch");
    for (int i=0; i<1000; i++){
      stmt.addBatch("insert into t1 values(1)");
    }
    updateCount = stmt.executeBatch();
    
    if (updateCount.length != 1000) {
      System.out.println("ERROR: there were 1000 statements in the batch");
      passed = false;
    }

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 1000) {
      System.out.println("There should been 1000 rows in the table, but found " + rs.getInt(1) + " rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing a batch with 3 different parameter sets in it.
  static boolean runMultipleValueSetPreparedBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    //try prepared statement batch with just one set of values
    System.out.println("Positive Prepared Stat: set 3 set of parameter values and run the batch");
    PreparedStatement pStmt = conn.prepareStatement("insert into t1 values(?)");
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    pStmt.setInt(1, 2);
    pStmt.addBatch();
    pStmt.setInt(1, 3);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();
    if (updateCount.length != 3) {
      System.out.println("ERROR: there were 3 parameter sets in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    pStmt.close();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 3) {
      System.out.println("ERROR: There should have been 3 rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing a batch with 3 different statements in it.
  static boolean runMultipleStatementsBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    System.out.println("Positive Statement: testing 2 inserts and 1 update batch");
    stmt.addBatch("insert into t1 values(2)");
    stmt.addBatch("update t1 set c1=4");
    stmt.addBatch("insert into t1 values(3)");

    updateCount = stmt.executeBatch();

    if (updateCount.length != 3) {
      System.out.println("ERROR: there were 3 statements in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    rs = stmt.executeQuery("select count(*) from t1 where c1=2");
    rs.next();
    if(rs.getInt(1) != 0) {
      System.out.println("ERROR: There should have been 0 rows with c1 = 2");
      passed = false;
    }
    rs.close();

    rs = stmt.executeQuery("select count(*) from t1 where c1=4");
    rs.next();
    if(rs.getInt(1) != 1) {
      System.out.println("ERROR: There should have been 1 row with c1 = 4");
      passed = false;
    }
    rs.close();

    rs = stmt.executeQuery("select count(*) from t1 where c1=3");
    rs.next();
    if(rs.getInt(1) != 1) {
      System.out.println("ERROR: There should have been 1 row with c1 = 3");
      passed = false;
    }
    rs.close();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 2) {
      System.out.println("ERROR: There should have been 2 rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try prepared statement batch with just one set of values.
  static boolean runSingleValueSetPreparedBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    //try prepared statement batch with just one set of values
    System.out.println("Positive Prepared Stat: set one set of parameter values and run the batch");
    PreparedStatement pStmt = conn.prepareStatement("insert into t1 values(?)");
    pStmt.setInt(1, 1);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();
    if (updateCount.length != 1) {
      System.out.println("ERROR: there was 1 parameter set in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    pStmt.close();
    rs = stmt.executeQuery("select count(*) from t1 where c1=1");
    rs.next();
    if(rs.getInt(1) != 1) {
      System.out.println("ERROR: There should have been one rows with c1 = 1");
      passed = false;
    }
    rs.close();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 1) {
      System.out.println("ERROR: There should have been 1 row");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try prepared statement batch with just no settable parameters.
  static boolean runNoParametersPreparedBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    System.out.println("Positive Prepared Stat: no settable parameters");
    PreparedStatement pStmt = conn.prepareStatement("insert into t1 values(5)");
    pStmt.addBatch();
    pStmt.addBatch();
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();
    if (updateCount.length != 3) {
      System.out.println("ERROR: there was 3 parameter set in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    pStmt.close();
    rs = stmt.executeQuery("select count(*) from t1 where c1=5");
    rs.next();
    if(rs.getInt(1) != 3) {
      System.out.println("ERROR: There should have been three rows with c1 = 5");
      passed = false;
    }
    rs.close();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 3) {
      System.out.println("ERROR: There should have been 3 rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }
  
  //try prepared statement batch with just 2 set of values and there value is null. Bug 4002
  static boolean runMultipleValueSetNullPreparedBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];
    ResultSet rs;

    //try prepared statement batch with just one set of values
    System.out.println("Positive Prepared Stat: set one set of parameter values to null and run the batch");
    PreparedStatement pStmt = conn.prepareStatement("insert into t1 values(?)");
    pStmt.setNull(1, Types.INTEGER);
    pStmt.addBatch();
    pStmt.setNull(1, Types.INTEGER);
    pStmt.addBatch();
    updateCount = pStmt.executeBatch();
    if (updateCount.length != 2) {
      System.out.println("ERROR: there were 2 parameter set to null in the batch");
      passed = false;
    }

    for (int i=0; i<updateCount.length; i++) {
      if (updateCount[i] != 1) {
        System.out.println("ERROR: update count for stat " + i + "should have been 1 but it is " + updateCount[i]);
        passed = false;
      }
    }

    pStmt.close();
    rs = stmt.executeQuery("select count(*) from t1 where c1 is null");
    rs.next();
    if(rs.getInt(1) != 2) {
      System.out.println("ERROR: There should have been two rows with c1 is null");
      passed = false;
    }
    rs.close();

    rs = stmt.executeQuery("select count(*) from t1");
    rs.next();
    if(rs.getInt(1) != 2) {
      System.out.println("ERROR: There should have been 2 rows");
      passed = false;
    }
    rs.close();

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing a batch which single statement in it. Should work.
  static boolean runSingleStatementBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];

    System.out.println("Positive Statement: testing 1 statement batch");
    stmt.addBatch("insert into t1 values(2)");
    updateCount = stmt.executeBatch();

    if (updateCount.length>1) {
      System.out.println("ERROR: Since this is a single statement, there should have been only one update count");
      passed = false;
    }

    if (updateCount[0] != 1) {
      System.out.println("ERROR: update count should have been 1, instead it is " + updateCount[0]);
      passed = false;
    }

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing a batch which nothing in it. Should work.
  static boolean runEmptyValueSetPreparedBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];

    //try executing a batch which nothing in it. Should work.
    System.out.println("Positive Prepared Stat: set no parameter values and run the batch");
    PreparedStatement pStmt = conn.prepareStatement("insert into t1 values(?)");
    updateCount = pStmt.executeBatch();

    if (updateCount.length != 0) {
      System.out.println("ERROR: update count should have been zero");
      passed = false;
    }

    pStmt.close();
    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

  //try executing a batch which nothing in it. Should work.
  static boolean runEmptyStatementBatch(Connection conn, Statement stmt) throws SQLException {
    boolean passed = true;
    int updateCount[];

    //try executing a batch which nothing in it. Should work.
    System.out.println("Positive Statement: clear the batch and run the empty batch");
    stmt.clearBatch();
    updateCount = stmt.executeBatch();

    if (updateCount.length != 0) {
      System.out.println("ERROR: Since this is an empty statement, there shouldn't have been any update count");
      passed = false;
    }

    stmt.executeUpdate("delete from t1");
    conn.commit();
    return passed;
  }

	/**
	 * Set up the test.
	 *
	 * This method creates the table used by the rest of the test.
	 *
	 * @param conn	The Connection
	 *
	 * @return	true if it succeeds, false if it doesn't
	 *
	 * @exception SQLException	Thrown if some unexpected error happens
	 */

	static boolean setUpTest(Connection conn, Statement stmt)
					throws SQLException 
	{
		boolean	passed = true;
		int		rows;

		/* Create a table  */
    stmt.addBatch("create table t1(c1 int)");
    // stmt.addBatch("create class alias for java.lang.Integer");
    stmt.addBatch("create procedure Integ() language java parameter style java external name 'java.lang.Integer'");
    stmt.addBatch("create table datetab(c1 date)");
    stmt.addBatch("create table timetab(c1 time)");
    stmt.addBatch("create table timestamptab(c1 timestamp)");
    stmt.addBatch("create table usertypetab(c1 DATE)");
    stmt.executeBatch();


    conn.commit();
		return passed;
	}

	/*
	** Associated parameters are extra parameters that are created
	** and associated with the root parameter (the user one) to
	** improve the performance of like.	  For something like
	** where c1 like ?, we generate extra 'associated' parameters 
	** that we use for predicates that we give to the access
	** manager. 
	*/
	static boolean checkAssociatedParams(Connection conn, Statement stmt) throws SQLException 
	{
		int i;
		conn.setAutoCommit(false);
		System.out.println("Positive Statement: testing associated parameters");
		stmt.executeUpdate("create table assoc(x char(10) not null primary key, y char(100))");
		stmt.executeUpdate("create table assocout(x char(10))");
		PreparedStatement ps = conn.prepareStatement("insert into assoc values (?, 'hello')");
		for (i = 10; i < 60; i++)
		{
			ps.setString(1, new Integer(i).toString());
			ps.executeUpdate();	
		}

		ps = conn.prepareStatement("insert into assocout select x from assoc where x like ?");
		ps.setString(1, "33%");
		ps.addBatch();
		ps.setString(1, "21%");
		ps.addBatch();
		ps.setString(1, "49%");
		ps.addBatch();
		int[] updateCount = ps.executeBatch();
		if (updateCount.length != 3)
		{
			System.out.println("ERROR: unexpected updateCount length "+updateCount.length);
			conn.rollback();
			return false;
		}

    	for (i = 0; i < 3; i++)
		{
			if (updateCount[i] != 1)
			{
				System.out.println("ERROR: unexpected updateCount["+i+"] value = "+updateCount[i]+".  Expected 1");
				conn.rollback();
				return false;
			}
		}
		stmt.execute("select cast(x as int) as myint from assocout order by myint");
		ResultSet rs = stmt.getResultSet();
		for (i = 0; rs.next(); i++)
		{
			int expect = 0;
			switch (i)
			{
				case 0:
					expect = 21;
					break;
				case 1:
					expect = 33;
					break;
				case 2:
					expect = 49;
					break;
			}
			if (rs.getInt(1) != expect)
			{
				System.out.println("ERROR: didn't find value "+expect+" in assocout table.  It would appear that associated parameters aren't working correctly");
				conn.rollback();
				return false;
			}
		}
		stmt.executeUpdate("delete from assocout");

		ps = conn.prepareStatement("insert into assocout select x from assoc where x like ?");
		ps.setString(1, "3%");
		ps.addBatch();
		ps.setString(1, "2%");
		ps.addBatch();
		ps.setString(1, "1%");
		ps.addBatch();
		updateCount = ps.executeBatch();
		if (updateCount.length != 3)
		{
			System.out.println("ERROR: unexpected updateCount2 length "+updateCount.length);
			conn.rollback();
			return false;
		}

    	for (i = 0; i < 3; i++)
		{
			if (updateCount[i] != 10)
			{
				System.out.println("ERROR: unexpected updateCount2["+i+"] value = "+updateCount[i]+".  Expected 10");
				conn.rollback();
				return false;
			}
		}

		stmt.execute("select cast(x as int) as myint from assocout order by myint");
		rs = stmt.getResultSet();
		for (i = 10; rs.next(); i++)
		{
			if (rs.getInt(1) != i)
			{
				System.out.println("ERROR: didn't find value "+i+" in assocout table.  It would appear that associated parameters aren't working correctly");
				stmt.execute("select x from assocout order by x");
				dumpRS(stmt.getResultSet());	
				conn.rollback();
				return false;
			}
		}
		if (i != 40)
		{
			System.out.println("ERROR: expected to get 30 values from assocout, but got "+(i-10)+" instead.  It would appear that associated parameters aren't working correctly");
			stmt.execute("select x from assocout order by x");
			dumpRS(stmt.getResultSet());	
			conn.rollback();
			return false;
		}
		
		stmt.executeUpdate("delete from assocout");

		ps = conn.prepareStatement("insert into assocout select x from assoc where x like ?");
		ps.setString(1, "%");
		ps.addBatch();
		ps.setString(1, "666666");
		ps.addBatch();
		ps.setString(1, "%");
		ps.addBatch();
		updateCount = ps.executeBatch();
		if (updateCount.length != 3)
		{
			System.out.println("ERROR: unexpected updateCount2 length "+updateCount.length);
			conn.rollback();
			return false;
		}

		stmt.execute("select count(x) from assocout");
		rs = stmt.getResultSet();
		rs.next();
		if (rs.getInt(1) != 100)
		{
			System.out.println("ERROR: count from assocout is not 100 as expected, it is "+rs.getString(1)+".  This is after executing like queries using '%'");
			stmt.execute("select x from assocout order by x");
			dumpRS(stmt.getResultSet());	
			conn.rollback();
			return false;
		}


		return true;
	}

	// lifted from the metadata test	
	private static void dumpRS(ResultSet s) throws SQLException 
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
}
