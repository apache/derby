/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.bug5052rts

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;


/**
 * testing gathering of runtime statistics for 
 * for the resultsets/statements not closed by the usee,
 * but get closed when garbage collector collects such
 * objects and closes them by calling the finalize.
 * See bug : 5052 for details.
 *
 */

public class bug5052rts {
 
	public static void main(String[] args) {
		Connection conn;
		Statement stmt;
		PreparedStatement pstmt;

		System.out.println("Test RunTime Statistics starting");
		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();
			stmt = conn.createStatement();
            try{
               stmt.execute("drop table tab1");
            }catch(Throwable e ) {}

            //create a table, insert a row, do a select from the table,
			stmt.execute("create table tab1 (COL1 int, COL2 smallint, COL3 real)");
            stmt.executeUpdate("insert into tab1 values(1, 2, 3.1)");
			stmt.executeUpdate("insert into tab1 values(2, 2, 3.1)");
			conn.setAutoCommit( false );

			//case1: Setting runtime statistics on just before result set close.
			if(true)
			{
				Statement stmt0 = conn.createStatement();
				ResultSet rs = stmt0.executeQuery("select * from tab1");  // opens the result set
			
				while ( rs.next() ) {
					System.out.println(rs.getString(1));
				}
				//set the runtime statistics on now.
                CallableStatement cs = 
                    conn.prepareCall(
                        "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(?)");
                cs.setInt(1, 1);
                cs.execute();
                cs.close();

				rs.close();
				stmt0.close();
			}
			
            CallableStatement cs = 
                conn.prepareCall(
                    "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(?)");
            cs.setInt(1, 0);
            cs.execute();
            cs.close();
			
			//case2: Stament/Resutset getting closed by the Garbage collector.
			if(true)
			{
				Statement stmt1 = conn.createStatement();
				ResultSet rs = stmt1.executeQuery("select * from tab1");  // opens the result set
			
				while ( rs.next() ) {
					System.out.println(rs.getString(1));
				}
				//set the runtime statistics on now.
                cs = conn.prepareCall(
                        "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(?)");
                cs.setInt(1, 1);
                cs.execute();
                cs.close();
			}

			
			if(true)
			{
				Statement stmt2 = conn.createStatement();
				ResultSet rs1 = stmt2.executeQuery("select count(*) from tab1");  // opens the result set
			
				while ( rs1.next() ) {
					System.out.println(rs1.getString(1));
				}
			}

			
			for(int i = 0 ; i < 3 ; i++){
				System.gc();
				System.runFinalization();
				//sleep for sometime to make sure garbage collector kicked in 
				//and collected the result set object.
				Thread.sleep(3000);
			}

			conn.commit(); //This should have failed before we fix 5052
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
		System.out.println("Test RunTimeStatistics finished successfully");
    }

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
		}
	}

}







