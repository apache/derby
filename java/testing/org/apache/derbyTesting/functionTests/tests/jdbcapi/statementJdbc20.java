/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.statementJdbc20

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Test of additional methods in JDBC2.0  methods in statement and 
 * resultset classes.
 *
 */

public class statementJdbc20 { 
    
	public static void main(String[] args) {
		Connection con;
		ResultSet rs;
		Statement stmt;

		System.out.println("Test statementJdbc20 starting");

		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();


			stmt = con.createStatement();

            //create a table, insert a row, do a select from the table,
			stmt.execute("create table tab1 (i int, s smallint, r real)");
            stmt.executeUpdate("insert into tab1 values(1, 2, 3.1)");

            // set all the peformance hint parameters
            stmt.setFetchSize(25);
            stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
            stmt.setEscapeProcessing(true);

           //Error  testing  : set wrong values ..
           try{
              stmt.setFetchSize(-1000);
           } 
           catch(SQLException e){
              dumpExpectedSQLExceptions(e);
           }

           try{
              stmt.setFetchDirection(-1000);
           } 
           catch(SQLException e){
              dumpExpectedSQLExceptions(e);
           }
            

            System.out.println("Fetch Size "  + stmt.getFetchSize());
            System.out.println("Fetch Direction " + stmt.getFetchDirection());

            // read the data just for the heck of it 
			rs = stmt.executeQuery("select * from tab1");
            while (rs.next())
            {
               System.out.println(rs.getInt(1) + " " + rs.getShort(2) + 
                                   " " + rs.getFloat(3));
            }

            // Get the constatnts for a result set            
            System.out.println("Result Set Fetch Size "  + rs.getFetchSize());
            System.out.println("Result Set Fetch Direction " + rs.getFetchDirection());

           // change values local to result set and get them back
            rs.setFetchSize(250);
            try{
               rs.setFetchDirection(ResultSet.FETCH_FORWARD);
            }catch(SQLException e){
              dumpExpectedSQLExceptions(e);
            }

            System.out.println("Result Set Fetch Size "  + rs.getFetchSize());
            System.out.println("Result Set Fetch Direction " + rs.getFetchDirection());

          // exception conditions 
            stmt.setMaxRows(10);
           try{
              rs.setFetchSize(100);
           } 
           catch(SQLException e){
              dumpExpectedSQLExceptions(e);
           }

           //Error  testing  : set wrong values ..
           try{
              rs.setFetchSize(-2000);
           } 
           catch(SQLException e){
              dumpExpectedSQLExceptions(e);
           }

           try{
              rs.setFetchDirection(-2000);
           } 
           catch(SQLException e){
              dumpExpectedSQLExceptions(e);
           }

           // set the fetch size values to zero .. to ensure 
           // error condtions are correct !

            rs.setFetchSize(0);
            stmt.setFetchSize(0);
       
			rs.close();

			//RESOLVE - uncomment tests in 3.5
			// executeQuery() not allowed on statements
			// that return a row count
			try
			{
				stmt.executeQuery("create table trash(c1 int)");
			}
			catch (SQLException e)
			{
              dumpExpectedSQLExceptions(e);
			}

			// verify that table was not created
			try
			{
				rs = stmt.executeQuery("select * from trash");
				System.out.println("select from trash expected to fail");
			}
			catch (SQLException e)
			{
              dumpExpectedSQLExceptions(e);
			}

			// executeUpdate() not allowed on statements
			// that return a ResultSet
			try
			{
				stmt.executeUpdate("values 1");
			}
			catch (SQLException e)
			{
              dumpExpectedSQLExceptions(e);
			}

			stmt.close();
			con.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
			e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception: "+e);
			e.printStackTrace();
		}

		System.out.println("Test statementJdbc20 finished");
    }

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
		}
	}

	static private void dumpExpectedSQLExceptions (SQLException se) {
		System.out.println("PASS -- expected exception");
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
        }
    }

}
