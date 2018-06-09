/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.maxfieldsize

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
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.PreparedStatement;
import java.io.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 *This Program Test SetMaxFieldsize()/getMaxFieldsize().
 *and the getXXX calls that are affected.
 */

public class maxfieldsize { 

	static final int START_SECOND_HALF = 5;
	static final int NUM_EXECUTIONS = 2 * START_SECOND_HALF;

    static final String c1_value="C1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    static final String c2_value="C2XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    static final String c3_value="C3XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    static final String c4_value="C4XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    static final String c5_value="C5XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    static final String c6_value="C6XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
	
	static private boolean isDerbyNet = false;
   
	public static void main(String[] args) {
		Connection conn;
		Statement stmt;

		// start by cleaning up, just in case
		String[] testObjects = {"table tab1", "table tab2"};
		

		System.out.println("Test MaxFieldSize  starting");

		isDerbyNet = TestUtil.isNetFramework();
		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			 ij.getPropertyArg(args);
			 conn = ij.startJBMS();
			 stmt = conn.createStatement();

            //create a table, insert a row, do a select from the table,
			 stmt.execute("create table tab1("+
                                           "c1 char(100) for bit data,"+
                                           "c2 varchar(100) for bit data," +
                                           "c3 long varchar for bit data,"+
                                           "c4 char(100),"+ 
                                           "c5 varchar(100),"+
                                           "c6 long varchar)");
            // load some data into this table ..
            load_data(conn);

            // read the data   of each type with all the possible functions
            int loop = 0;
            while (loop < NUM_EXECUTIONS )
            {
				if (loop == START_SECOND_HALF)
				{
					stmt.setMaxFieldSize(24);
				}

				System.out.println("Iteration #: " + loop);
	            System.out.println("Max Field Size = "  + stmt.getMaxFieldSize());
				ResultSet rs = stmt.executeQuery("select * from tab1");
                while (rs.next())
                {
                    for(int i=1 ; i < 7 ; i++)
                    {
						System.out.println("Column #: " + i);
						switch (loop % START_SECOND_HALF)
						{
							case 0:
		                        connectionJdbc20.get_using_object(rs, i);
								break;

							case 1:
								if (isDerbyNet)
									System.out.println("beetle 5350 - JCC returns incorrect result for maxfieldsize()");
								connectionJdbc20.get_using_string(rs, i);
								break;

							case 2:
								connectionJdbc20.get_using_ascii_stream(rs, i);
								break;

							case 3:
		                        if(i < 4 ) // only c1 , c2, c3
				                {
						            connectionJdbc20.get_using_binary_stream(rs, i);
								}
								else
								{
									System.out.println("SKIPPING");
								}
								break;

							case 4:
		                        if(i < 4 ) // only c1 , c2, c3
				                {
								    connectionJdbc20.get_using_bytes(rs, i);
								}
								else
								{
									System.out.println("SKIPPING");
								}
								break;
						}
                    } 
                }
                rs.close();
                loop++;
            }
            // make sure that we throw exception for invalid values
            try{
                // negative value should throw an exception
                stmt.setMaxFieldSize(-200);
            } catch (SQLException e) {
				if ((e.getMessage() != null &&
					 e.getMessage().indexOf("Invalid maxFieldSize value") >= 0)
					|| (e.getSQLState() != null &&
						(e.getSQLState().equals("XJ066"))))
					System.out.println("Expected Exception - Invalid maxFieldsize");
				else System.out.println("Unexpected FAILURE at " +e);
            }
            // zero is valid value -- which means unlimited
            stmt.setMaxFieldSize(0);

			// Do an external sort (forced via properties file),
			// verifying that streams work correctly
			System.out.println("Doing external sort");

			testSort(conn, stmt);

			stmt.close();
			conn.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
			e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception: "+e);
			e.printStackTrace();
		}

		System.out.println("Test maxfieldsize  finished");
    }


    static private void load_data(Connection conn) throws Exception{
        PreparedStatement pstmt = null;

        try{
            pstmt = conn.prepareStatement(
                     "insert into tab1 values(?,?,?,?,?,?)");

            pstmt.setBytes(1, c1_value.getBytes("US-ASCII"));
            pstmt.setBytes(2, c2_value.getBytes("US-ASCII"));
            pstmt.setBytes(3, c3_value.getBytes("US-ASCII"));
            pstmt.setString(4, c4_value);
            pstmt.setString(5, c5_value);
            pstmt.setString(6, c6_value);
            pstmt.execute();
        }
		catch(SQLException e)
		{
			dumpSQLExceptions(e);
			e.printStackTrace();
        }
		catch(Throwable e )
		{
			System.out.println("Fail -- unexpected exception ");
			e.printStackTrace();
        }
		finally
		{
             pstmt.close();
        }
    }

	private static void testSort(Connection conn, Statement stmt)
		throws SQLException, java.io.UnsupportedEncodingException
	{
		PreparedStatement insertPStmt;

		// Load up a 2nd table using streams where appropriate
		stmt.execute("create table tab2("+
									   "c0 int, " +
                                          "c1 char(100) for bit data,"+
                                          "c2 varchar(100) for bit data," +
                                          "c3 long varchar for bit data,"+
                                          "c4 char(100),"+ 
                                          "c5 varchar(100),"+
                                          "c6 long varchar)");

		// Populate the table
		insertPStmt = conn.prepareStatement(
						"insert into tab2 values (?, ?, ?, ?, ?, ?, ?)");
		for (int index = 0; index < 5000; index++)
		{
			insertPStmt.setInt(1, index);
            insertPStmt.setBytes(2, c1_value.getBytes("US-ASCII"));
            insertPStmt.setBytes(3, c2_value.getBytes("US-ASCII"));
            insertPStmt.setBytes(4, c3_value.getBytes("US-ASCII"));
            insertPStmt.setString(5, c4_value);
            insertPStmt.setString(6, c5_value);
            insertPStmt.setString(7, c6_value);
			insertPStmt.executeUpdate();
		}

		insertPStmt.close();

		// Do sort with maxFieldSize = 0
		doSort(stmt);

		// Set maxFieldSize to 24 and do another sort
		stmt.setMaxFieldSize(24);
		doSort(stmt);
	}

	private static void doSort(Statement stmt)
		throws SQLException
	{
		System.out.println("Max Field Size = "  + stmt.getMaxFieldSize());

		try
		{
			/* Do a descending sort on 1st column, but only select
			 * out 1st and last 5 rows.  This should test streaming to/from
			 * a work table.
			 */
			ResultSet rs = stmt.executeQuery("select * from tab2 order by c0 desc");
			for (int index = 0; index < 5000; index++)
			{
				rs.next();
				if (index < 5 || index >= 4995)
				{
					System.out.println("Iteration #: " + index);
					System.out.println("Column #1: " + rs.getInt(1));
					System.out.println("Column #2:");
					connectionJdbc20.get_using_binary_stream(rs, 2);
					System.out.println("Column #3:");
					connectionJdbc20.get_using_binary_stream(rs, 3);
					System.out.println("Column #4:");
					connectionJdbc20.get_using_binary_stream(rs, 4);
					System.out.println("Column #5:");
					connectionJdbc20.get_using_ascii_stream(rs, 5);
					System.out.println("Column #6:");
					connectionJdbc20.get_using_ascii_stream(rs, 6);
					System.out.println("Column #7:");
					connectionJdbc20.get_using_ascii_stream(rs, 7);
				}
			}
		}
		catch (SQLException e) 
		{
			throw e;
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception: "+e);
			e.printStackTrace();
		}
	}

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null)
		{
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
		}
	}

	static private void dumpExpectedSQLExceptions (SQLException se) {
		System.out.println("PASS -- expected exception");
		while (se != null)
		{
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
        }
    }

	static private void cleanUp(Connection conn) throws SQLException
	{
		
	}


}
