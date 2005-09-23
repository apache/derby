/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.StmtCloseFunTest

   Copyright 2000, 2005 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.tools.ij;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Types;
import java.util.GregorianCalendar;
import org.apache.derby.iapi.reference.JDBC20Translation;
import org.apache.derbyTesting.functionTests.util.BigDecimalHandler;
import org.apache.derbyTesting.functionTests.util.TestUtil;

public class StmtCloseFunTest {
    
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
	
	static private boolean isDerbyNet = false;

    public static void main(String[] args) {

		System.out.println("Statement Close Fun Test starting ");
		isDerbyNet= TestUtil.isNetFramework();


		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Connection conn = ij.startJBMS();
			test1(conn);
			test2(conn);
			test3(conn);

			conn.close();		
				
		} catch (SQLException e) {
			dumpSQLExceptions(e);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			e.printStackTrace(System.out);
		}

		System.out.println("Statement Close Fun Test finished");
	}

    private static void test1(Connection conn) {
		Statement s;
        try {
			System.out.println("Statement test begin");
				s = conn.createStatement();
				/*
				  We create a table, add a row, and close the statement.
				*/
				s.execute("create table tab1(num int, addr varchar(40))");
				s.execute("insert into tab1 values (1910,'Union St.')");
				s.close();
		  				
			try {
				s.executeQuery("create table tab2(num int)");
				System.out.println("Statement Test failed (1)");
			}
			catch(SQLException e) { }

			try {
				s.executeUpdate("update tab1 set num=180, addr='Grand Ave.' where num=1910");
				System.out.println("Statement Test failed");
			}
			catch(SQLException e) { }

			/* close() is the only method that can be called
			 * after a Statement has been closed, as per
			 * Jon Ellis.
			 */
			try {
				s.close();
			}
			catch(SQLException e) { 
				System.out.println("Statement Test failed (2) " + e.toString());
			}

			try {
				s.execute("insert into tab1 values (300,'Lakeside Dr.')");
				System.out.println("Statement Test failed (3)");
			}
			catch(SQLException e) { }

			try {
				s.getMaxFieldSize();
				System.out.println("Statement Test failed (4)");
			}
			catch(SQLException e) { }

			try {
				s.setMaxFieldSize(100);
				System.out.println("Statement Test failed (5)");
			}
			catch(SQLException e) { }

			try {
				s.getMaxRows();
				System.out.println("Statement Test failed (6)");
			}
			catch(SQLException e) { }

			try {
				s.setMaxRows(1000);
				System.out.println("Statement Test failed (7)");
			}
			catch(SQLException e) { }

			try {
				s.setEscapeProcessing(true);
				System.out.println("Statement Test failed (8)");
			}
			catch(SQLException e) { }

			if (! isDerbyNet)
			{
				try {
					// currently derby only supports returning 0 in this case.
					
					int qry_timeout = s.getQueryTimeout();
					
					if (qry_timeout != 0)
						System.out.println("Statement Test failed, must return 0.");
				}
				catch(SQLException e)
				{
					System.out.println("Statement Test failed (9) " + e.toString());
				}
			}
			try {
				s.setQueryTimeout(20);
				System.out.println("Statement Test failed (10)");
			}
			catch(SQLException e) { }

			try {
				s.cancel();
				System.out.println("Statement Test failed (11)");
			}
			catch(SQLException e) { }

			if (isDerbyNet)
				System.out.println("beetle 5524");
			try {
				s.getWarnings();
				System.out.println("Statement Test failed (12)");
			}
			catch(SQLException e) { }
		
			if (isDerbyNet)
				System.out.println("beetle 5524");			
			try {
				s.clearWarnings();
				System.out.println("Statement Test failed (13)");
			}
			catch(SQLException e) { }

			try {
				s.setCursorName("ABC");
				System.out.println("Statement Test failed (14)");
			}
			catch(SQLException e) { }

			try {
				s.execute("create table tab3(num int)");
				System.out.println("Statement Test failed (15)");
			}
			catch(SQLException e) { }

			try {
				s.getResultSet();
				System.out.println("Statement Test failed (16)");
			}
			catch(SQLException e) { }

			try {
				s.getUpdateCount();
				System.out.println("Statement Test failed (17)");
			}
			catch(SQLException e) { }

			try {
				s.getMoreResults();
				System.out.println("Statement Test failed (18) ");
			}
			catch(SQLException e) { }

			try {
				s.getResultSetType();
				System.out.println("Statement Test failed (19)");
			}
			catch(SQLException e) { }

			try {
				s.setFetchDirection(JDBC20Translation.FETCH_FORWARD);
				System.out.println("Statement Test failed (20)");
			}
			catch(SQLException e) { }

			try {
				s.setFetchSize(100);
				System.out.println("Statement Test failed (21)");
			}
			catch(SQLException e) { }

			try {
				s.getFetchSize();
				System.out.println("Statement Test failed (22)");
			}
			catch(SQLException e) { }

			try {
				s.getResultSetConcurrency();
				System.out.println("Statement Test failed (23)");
			}
			catch(SQLException e) { }

			try {
				s.addBatch("create table tab3(age int)");
				System.out.println("Statement Test failed (24)");
			}
			catch(SQLException e) { }

			try {
				s.clearBatch();
				System.out.println("Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				s.executeBatch();
				System.out.println("Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				s.getConnection();
				System.out.println("Statement Test failed");
			}
			catch(SQLException e) { }
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			e.printStackTrace(System.out);
			return;
		}
		System.out.println("Statement test end");
}

     private static void test2(Connection conn) {
		PreparedStatement ps;
        try {
			System.out.println("Prepared Statement test begin");
			ps = conn.prepareStatement("create table tab2(a int, b float, c date, d varchar(100))");
			ps.execute();
		  	ps.close();

			//we test execute() and executeQuery() here
			ps =  conn.prepareStatement("select a from tab2");
			ps.execute();
			ps.close();
			try {
				ps.execute();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

   			try {
				ps.executeQuery();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }
		
			ps = conn.prepareStatement("insert into tab2 values(?, ?, ?, ?)");
		   	/*
		   	  We create a table, add a row, and close the statement.
		   	*/
		   	ps.setInt(1, 420);
		   	ps.setFloat(2, (float)12.21);
		   	ps.setDate(3, new Date(870505200000L));
		   	ps.setString(4, "China");
		   	ps.executeUpdate();
		   	ps.close();
		  				
			//now, we begin the test
			try {
				ps.setInt(1, 530);
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.setFloat(2, (float)3.14);
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.setDate(3, new Date(870505200000L));
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.setDate(3, new Date(870505200000L), new GregorianCalendar());
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.setString(4, "HongKong");
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.executeUpdate();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			/* close() is the only method that can be called
			 * after a Statement has been closed, as per
			 * Jon Ellis.
			 */
			try {
				ps.close();
			}
			catch(SQLException e) { 
				System.out.println("Prepared Statement Test failed");
			}

			try {
				ps.clearParameters();
				System.out.println("Prepared Statement Test failed"); 
			}
			catch(SQLException e) { }

			try {
				ps.getMetaData();
				System.out.println("Prepared Statement Test failed"); 
			}
			catch(SQLException e) { }

			try {
				ps.getMaxFieldSize();
				System.out.println("Prepared Statement Test failed"); 
			}
			catch(SQLException e) { }

			try {
				ps.setMaxFieldSize(100);
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.getMaxRows();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.setMaxRows(1000);
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.setEscapeProcessing(true);
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			if (! isDerbyNet)
			{
				try {
					// currently derby only supports returning 0 in this case.
					
					int qry_timeout = ps.getQueryTimeout();
					
					if (qry_timeout != 0)
						System.out.println("Statement Test failed, must return 0.");
				}
				catch(SQLException e)
				{ 
					System.out.println("Statement Test failed");
				}
			}

			try {
				ps.setQueryTimeout(20);
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.cancel();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }
			
			if (isDerbyNet)
				System.out.println("beetle 5524");	
			try {
				ps.getWarnings();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			if (isDerbyNet)
				System.out.println("beetle 5524");	
			try {
				ps.clearWarnings();
			    System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.setCursorName("ABC");
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.execute("create table tab3(num int)");
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.getResultSet();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.getUpdateCount();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.getMoreResults();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.getResultSetType();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.setFetchDirection(JDBC20Translation.FETCH_FORWARD);
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.setFetchSize(100);
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.getFetchSize();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.getResultSetConcurrency();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.addBatch();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.clearBatch();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.executeBatch();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				ps.getConnection();
				System.out.println("Prepared Statement Test failed");
			}
			catch(SQLException e) { }
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			e.printStackTrace(System.out);
			return;
		}
		System.out.println("Prepared Statement test end");
}

     private static void test3(Connection conn) {
		CallableStatement cs;
        try {
			System.out.println("Callable Statement test begin");
			try {

				Statement s = conn.createStatement();

				s.execute("CREATE PROCEDURE takesString(OUT P1 VARCHAR(40), IN P2 INT) " +
						"EXTERNAL NAME '" + CLASS_NAME + "takesString'" +
						" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");


				cs = conn.prepareCall("call takesString(?,?)");
			
				cs.registerOutParameter(1, Types.CHAR);
				cs.setInt(2, Types.INTEGER);

				cs.execute();
				System.out.println("The result is " + cs.getString(1));

				cs.close();

				try {
					cs.setString(1, "ABC");
					System.out.println("Callable Statement Test failed");
				}
				catch(SQLException e) { }

				try {
					cs.registerOutParameter(1, Types.CHAR);
					System.out.println("Callable Statement Test failed");
				}
				catch(SQLException e) { }

				s.execute("drop procedure takesString");
				s.close();
			}
		
			catch(SQLException e) { 
				dumpSQLExceptions(e);
			}
		
			//now, testing all the inherited functions
			cs = conn.prepareCall("create table tab3(a int, b float, c varchar(100))");
			cs.execute();
		  	cs.close();

			//we test execute() and executeQuery() here
			cs =  conn.prepareCall("select a from tab3");
			ResultSet rs = cs.executeQuery();
			cs.close();
			try {
				cs.execute();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

   			try {
				cs.executeQuery();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }
		
			cs = conn.prepareCall("insert into tab3 values(?, ?, ?)");
		   	/*
		   	  We create a table, add a row, and close the statement.
		   	*/
		   	cs.setInt(1, 420);
		   	cs.setFloat(2, (float)12.21);
		   	cs.setString(3, "China");

			cs.executeUpdate();
		   	cs.close();
		  				
			//now, we begin the test
			try {
				cs.setInt(1, 530);
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.setFloat(2, (float)3.14);
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.setString(3, "HongKong");
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.executeUpdate();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			/* close() is the only method that can be called
			 * after a Statement has been closed, as per
			 * Jon Ellis.
			 */
			try {
				cs.close();
			}
			catch(SQLException e) { 
				System.out.println("Callable Statement Test failed");
			}

			try {
				cs.clearParameters();
				System.out.println("Callable Statement Test failed"); 
			}
			catch(SQLException e) { }

			try {
				cs.getMetaData();
				System.out.println("Callable Statement Test failed"); 
			}
			catch(SQLException e) { }

			try {
				cs.getMaxFieldSize();
				System.out.println("Callable Statement Test failed"); 
			}
			catch(SQLException e) { }

			try {
				cs.setMaxFieldSize(100);
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.getMaxRows();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.setMaxRows(1000);
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.setEscapeProcessing(true);
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			if (! isDerbyNet)
			{
				try {
					// currently derby only supports returning 0 in this case.
					
					int qry_timeout = cs.getQueryTimeout();
					
					if (qry_timeout != 0)
						System.out.println("Statement Test failed, must return 0.");
				}
				catch(SQLException e)
				{ 
					System.out.println("Statement Test failed");
				}
			}
			
			try {
				cs.setQueryTimeout(20);
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.cancel();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			if (isDerbyNet)
				System.out.println("beetle 5524");	
			try {
				cs.getWarnings();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			if (isDerbyNet)
				System.out.println("beetle 5524");	
			try {
				cs.clearWarnings();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.setCursorName("ABC");
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.execute("create table tab3(num int)");
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.getResultSet();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.getUpdateCount();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.getMoreResults();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.getResultSetType();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.setFetchDirection(JDBC20Translation.FETCH_FORWARD);
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.setFetchSize(100);
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.getFetchSize();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.getResultSetConcurrency();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.addBatch();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.clearBatch();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.executeBatch();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }

			try {
				cs.getConnection();
				System.out.println("Callable Statement Test failed");
			}
			catch(SQLException e) { }
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			e.printStackTrace(System.out);
			return;
		}
		System.out.println("Callable Statement test end");
}


	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se = se.getNextException();
		}
	}

}






