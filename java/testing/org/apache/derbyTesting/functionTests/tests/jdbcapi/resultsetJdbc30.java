/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.resultsetJdbc30

   Copyright 2001, 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;

/**
 * Test of additional methods in JDBC3.0 result set
 *
 */

public class resultsetJdbc30 { 
	public static void main(String[] args) {
		Connection con;
		ResultSet rs;
		Statement stmt;
		String[]  columnNames = {"i", "s", "r", "d", "dt", "t", "ts", "c", "v", "tn", "dc"};

		System.out.println("Test resultsetJdbc30 starting");

		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();

			stmt = con.createStatement();

      //create a table, insert a row, do a select from the table,
			stmt.execute("create table t (i int, s smallint, r real, "+
				"d double precision, dt date, t time, ts timestamp, "+
				"c char(10), v varchar(40) not null, dc dec(10,2))");
			stmt.execute("insert into t values(1,2,3.3,4.4,date('1990-05-05'),"+
						 "time('12:06:06'),timestamp('1990-07-07 07:07:07.07'),"+
						 "'eight','nine', 11.1)");

			rs = stmt.executeQuery("select * from t");
			rs.next();

			//following will give not implemented exceptions.
			try {
			  System.out.println();
			  System.out.println("trying rs.getURL(int) :");
			  rs.getURL(8);
			  System.out.println("Shouldn't reach here. Method not implemented yet.");
 			} catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			try {
			  System.out.println();
			  System.out.println("trying rs.getURL(String) :");
			  rs.getURL("c");
			  System.out.println("Shouldn't reach here. Method not implemented yet.");
 			} catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			try {
			  System.out.println();
			  System.out.println("trying rs.updateRef(int, Ref) :");
			  rs.updateRef(8,null);
			  System.out.println("Shouldn't reach here. Method not implemented yet.");
			} catch (NoSuchMethodError nsme) {
				System.out.println("ResultSet.updateRef not present - correct for JSR169");
			}
			catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			try {
			  System.out.println();
			  System.out.println("trying rs.updateRef(String, Ref) :");
			  rs.updateRef("c",null);
			  System.out.println("Shouldn't reach here. Method not implemented yet.");
 			} catch (NoSuchMethodError nsme) {
				System.out.println("ResultSet.updateRef not present - correct for JSR169");
			} catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			try {
			  System.out.println();
			  System.out.println("trying rs.updateBlob(int, Blob) :");
			  rs.updateBlob(8,null);
			  System.out.println("Shouldn't reach here because method is being invoked on a read only resultset");
 			} catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			try {
			  System.out.println();
			  System.out.println("trying rs.updateBlob(String, Blob) :");
			  rs.updateBlob("c",null);
			  System.out.println("Shouldn't reach here because method is being invoked on a read only resultset");
 			} catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			try {
			  System.out.println();
			  System.out.println("trying rs.updateClob(int, Clob) :");
			  rs.updateClob(8,null);
			  System.out.println("Shouldn't reach here because method is being invoked on a read only resultset");
 			} catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			try {
			  System.out.println();
			  System.out.println("trying rs.updateClob(String, Clob) :");
			  rs.updateClob("c",null);
			  System.out.println("Shouldn't reach here because method is being invoked on a read only resultset");
 			} catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			try {
			  System.out.println();
			  System.out.println("trying rs.updateArray(int, Array) :");
			  rs.updateArray(8,null);
			  System.out.println("Shouldn't reach here. Method not implemented yet.");
 			} catch (NoSuchMethodError nsme) {
				System.out.println("ResultSet.updateArray not present - correct for JSR169");
			} catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			try {
			  System.out.println();
			  System.out.println("trying rs.updateArray(String, Array) :");
			  rs.updateArray("c",null);
			  System.out.println("Shouldn't reach here. Method not implemented yet.");
 			} catch (NoSuchMethodError nsme) {
				System.out.println("ResultSet.updateArray not present - correct for JSR169");
			}catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}

			rs.close();
			stmt.close();
            
            //
            // Check our behavior around closing result sets when auto-commit
            // is true.  Test with both holdable and non-holdable result sets
            //
            con.setAutoCommit(true);
            
            // Create a non-updatable holdable result set, and then try to 
            // update it
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
            
			rs = stmt.executeQuery("select * from t");
			rs.next();
            
            checkForCloseOnException(rs, true);
            
            rs.close();
            stmt.close();

            // Create a non-updatable non-holdable result set, and then try to 
            // update it
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            
			rs = stmt.executeQuery("select * from t");
			rs.next();
            
            checkForCloseOnException(rs, false);
                
            rs.close();
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

		System.out.println("Test resultsetJdbc30 finished");
    }
    
    static private void checkForCloseOnException(ResultSet rs, boolean holdable) 
            throws Exception {
        try {
            rs.updateBlob("c",null);
            throw new Exception("rs.updateBlob() on a read-only result set" +
                "should not have succeeded");
        } catch (SQLException ex) {
        }

        try {
            rs.beforeFirst();
            String holdableStr = holdable ? "holdable" : "non-holdable";
            System.out.println(holdableStr + " result set was not closed on exception");
        }
        catch ( SQLException ex) {
            String state = ex.getSQLState();
            if ( state.equals("XCL16"))
            {
                System.out.println("Holdable result set was closed on exception");
            }
            else
            {
                throw ex;
            }
        }

    }

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
		}
	}

}
