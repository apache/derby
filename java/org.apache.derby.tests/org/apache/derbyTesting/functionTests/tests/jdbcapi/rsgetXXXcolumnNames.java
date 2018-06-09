/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.rsgetXXXcolumnNames

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


import java.sql.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

import org.apache.derbyTesting.functionTests.util.TestUtil;

public class rsgetXXXcolumnNames {

    public static void main(String[] args) {
        test1(args);
    }
    
        public static void test1(String []args) {   
                Connection con;
                ResultSet rs;
                Statement stmt = null;
                PreparedStatement stmt1 = null;

                System.out.println("Test rsgetXXXcolumnNames starting");

                try
                {
                        // use the ij utility to read the property file and
                        // make the initial connection.
                        ij.getPropertyArg(args);
                        con = ij.startJBMS();
					

                        stmt = con.createStatement(); 

                        // first cleanup in case we're using useprocess false
                        String[] testObjects = {"table caseiscol"};
                        TestUtil.cleanUpTest(stmt, testObjects);

			con.setAutoCommit(false);                        			              

			// create a table with two columns, their names differ in they being in different cases.
                        stmt.executeUpdate("create table caseiscol(COL1 int ,\"col1\" int)");

   			con.commit();
   			
			stmt.executeUpdate("insert into caseiscol values (1,346)");

			con.commit();

                        // select data from this table for updating
			stmt1 = con.prepareStatement("select COL1, \"col1\" from caseiscol FOR UPDATE",ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		        rs = stmt1.executeQuery();

			// Get the data and disply it before updating.
                        System.out.println("Before updation...");
			while(rs.next()) {
			   System.out.println("ResultSet is: "+rs.getObject(1));
			   System.out.println("ResultSet is: "+rs.getObject(2));
			}
                        rs.close();
			rs = stmt1.executeQuery();
			while(rs.next()) {
			   // Update the two columns with different data.
			   // Since update is case insensitive only the first column should get updated in both cases.
			   rs.updateInt("col1",100);
			   rs.updateInt("COL1",900);
			   rs.updateRow();
			}
			rs.close();

			System.out.println("After update...");
			rs = stmt1.executeQuery();

			// Display the data after updating. Only the first column should have the updated value.
			while(rs.next()) {
			   System.out.println("Column Number 1: "+rs.getInt(1));
			   System.out.println("Column Number 2: "+rs.getInt(2));
			}
			rs.close();
			rs = stmt1.executeQuery();
			while(rs.next()) {
			   // Again checking for case insensitive behaviour here, should display the data in the first column.
			   System.out.println("Col COL1: "+rs.getInt("COL1"));
			   System.out.println("Col col1: "+rs.getInt("col1"));
			}
			rs.close();
            stmt1.close();
            stmt.close();
            con.commit();
            con.close();
 		} catch(SQLException sqle) {
 		   dumpSQLExceptions(sqle);
 		   sqle.printStackTrace();
 		} catch(Throwable e) {
 		   System.out.println("FAIL -- unexpected exception: "+e.getMessage());
                   e.printStackTrace();

 		}
     }
     
     static private void dumpSQLExceptions (SQLException se) {
                System.out.println("FAIL -- unexpected exception");
                while (se != null) {
                        System.out.println("SQLSTATE("+se.getSQLState()+"): "+se.getMessage());
                        se = se.getNextException();
                }
        }
}
