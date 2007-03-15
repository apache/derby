/*
 
 Derby - Class org.apache.derbyTesting.system.langtest.utils.DataUtils
 
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
package org.apache.derbyTesting.system.optimizer.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.derbyTesting.system.optimizer.StaticValues;
/**
 * 
 * Class DataUtils: Utility class to drop/create database objects and populate data
 *
 */


public class DataUtils {
	public static void dropObjects(Connection conn, boolean verbose) throws SQLException {
		Statement stmt = null;
		if (verbose)
			System.out.println("Dropping existing Tables and Views...");
		for (int i=0;i<TestViews.dropViews.size();i++){
			try{
				stmt = conn.createStatement();
				stmt.executeUpdate((String)TestViews.dropViews.get(i));
			}catch(SQLException sqe){
				if(!sqe.getSQLState().equalsIgnoreCase("X0X05")){
					throw sqe;
				}
			}
		}
		for (int i = 1; i <= StaticValues.NUM_OF_TABLES; i++) {
			try {
				String tableName = StaticValues.TABLE_NAME + i;
				stmt = conn.createStatement();
				stmt.execute(StaticValues.DROP_TABLE+ tableName);
				stmt.close();
			} catch (SQLException sqe) {
				if (!sqe.getSQLState().equalsIgnoreCase("42Y55")) {
					throw sqe;
				} 
			}
		}// end for
	}
	public static void createObjects(Connection conn,boolean verbose) throws SQLException {
		Statement stmt = null;
		if (verbose)
			System.out.println("Creating Tables...");
		for (int i = 1; i <= StaticValues.NUM_OF_TABLES; i++) {
			try {
				String tableName = StaticValues.TABLE_NAME + i;
				if (verbose)
					System.out.println(" Creating Table - "+tableName);
				stmt = conn.createStatement();
				stmt.execute(StaticValues.CREATE_TABLE+ tableName+ StaticValues.TABLE_COLS);
				
				stmt.close();
			} catch (SQLException sqe) {
				if (!sqe.getSQLState().equalsIgnoreCase("X0Y32")) {
					throw sqe;
				} else {
							System.out.println("Table " + StaticValues.TABLE_NAME + i
							+ " exists");
				}

			}
		}// end for
		if (verbose)
			System.out.println("Creating Views...");
		for (int i=0;i<TestViews.createViews.size();i++){
			try{
				stmt = conn.createStatement();
				stmt.executeUpdate((String)TestViews.createViews.get(i));
			}catch(SQLException sqe){
				System.out.println("SQLState = "+sqe.getSQLState()+", "+sqe);
				System.out.println("View statement ==> "+(String)TestViews.createViews.get(i)+" failed");
			}
		}
	}

	public static void insertData(Connection conn,boolean verbose){
		try{
			String commonString = "String value for the ";
			String valueForString = commonString + "varchar column ";
			String valueForBitData = commonString + "bit data column ";
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			ResultSet rs = null;
			int totalRows = 0;
			for (int i = 1; i <= StaticValues.NUM_OF_TABLES; i++) {
				String tableName = StaticValues.TABLE_NAME + i;
				rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
				while (rs.next()) {
					totalRows = rs.getInt(1);
				}
				if (totalRows >= StaticValues.NUM_OF_ROWS) {
					if (verbose)
						System.out.println(" InsertData.insert_data() => "
							+ totalRows + " exists in table " + tableName
							+ "...");

				}else{
					if(totalRows>0){
						if (verbose)
							System.out.println("Dropping existing indexes from table: "
								+ tableName);
						try {
							stmt.executeUpdate("DROP INDEX " + tableName
									+ "_col4_idx");
						} catch (SQLException sqe) {
							if (!sqe.getSQLState().equalsIgnoreCase("42X65")) {
								throw sqe;
							}
						}
						try {
							stmt.executeUpdate("DROP INDEX " + tableName
									+ "_col7_idx");
						} catch (SQLException sqe) {
							if (!sqe.getSQLState().equalsIgnoreCase("42X65")) {
								throw sqe;
							}
						}
						if (verbose)
							System.out.println("Rows deleted from " + tableName + "= "
								+ stmt.executeUpdate("DELETE FROM " + tableName));
					}
					PreparedStatement ps = conn
							.prepareStatement(StaticValues.INSERT_TABLE
									+ tableName + StaticValues.INSERT_VALUES);
					long start = System.currentTimeMillis();
					int k = 1;
					while (k <= StaticValues.NUM_OF_ROWS) {

						ps.setInt(1, k);
						ps.setString(2, valueForString + "in Table "
								+ StaticValues.TABLE_NAME + i + ": " + k);
						ps.setString(3, valueForBitData + "in Table "
								+ StaticValues.TABLE_NAME + i + ": " + k);
						ps.setString(4, StaticValues.TABLE_NAME + i + "_COL4:"
								+ k);
						ps.setString(5, StaticValues.TABLE_NAME + i + "_COL5:"
								+ k);
						ps.setString(6, StaticValues.TABLE_NAME + i + "_COL6:"
								+ k);
						ps.setString(7, StaticValues.TABLE_NAME + i + "_COL7:"
								+ k);
						ps.setInt(8, k);
						/*
						 * ps.setString(8, StaticValues.TABLE_NAME + i +
						 * "_COL8:" + k);
						 */
						ps.setTimestamp(9, new Timestamp(System
								.currentTimeMillis()));
						ps.executeUpdate();
						if ((k % 10000) == 0) {
							conn.commit();
						}
						k++;
					}
					ps.close();
					conn.commit();
					if (verbose)
						System.out.println("Inserted " + (k - 1) + " rows into "
							+ tableName + " in "
							+ (System.currentTimeMillis() - start)
							+ " milliseconds");
					conn.setAutoCommit(true);

					if (verbose)
						System.out.println("Creating indexes for table: "
							+ tableName);

					stmt.executeUpdate("CREATE INDEX " + tableName
							+ "_col4_idx on " + tableName + "(col4)");
					stmt.executeUpdate("CREATE INDEX " + tableName
							+ "_col7_idx on " + tableName + "(col7)");
				}//end else
			}// end for
		}catch (Exception se){
			System.out.println(" EXCEPTION:" + se.getMessage());
			System.out.println("Stack Trace :  \n" );
			se.printStackTrace();
			return;
		}
	}		
}
			


