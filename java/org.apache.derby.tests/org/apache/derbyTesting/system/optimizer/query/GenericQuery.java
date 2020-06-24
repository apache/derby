/*
 
 Derby - Class org.apache.derbyTesting.system.langtest.query.GenericQuery;
 
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
package org.apache.derbyTesting.system.optimizer.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.derbyTesting.system.optimizer.StaticValues;
import org.apache.derbyTesting.system.optimizer.utils.TestUtils;
/**
 * 
 * Class GenericQuery: The generic class that is extended by the Query classes or instantiated
 * when the 'query.list' of custom queries is provided
 *
 */



public  class GenericQuery {
	protected String description="Custom Test Query";
	protected Connection conn=null;
	protected ArrayList<String> queries = new ArrayList<String>();
	protected ArrayList<String[]> prepStmtRunResults = new ArrayList<String[]>(); //times using PreparedStatement
	protected ArrayList<String[]> stmtRunResults = new ArrayList<String[]>(); //times using Statement
	protected int[] rowsExpected=null; //add rows expected
	
	public void setConnection(Connection con){
		conn=con;
	}
	public  void generateQueries(){
		
	}
	public void generateQueries(Properties prop){
		Enumeration qenum=prop.keys();
		while(qenum.hasMoreElements()){
			String queryName=(String)qenum.nextElement();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
			queries.add(prop.getProperty(queryName));
		}
	}
		
	public String getDescription(){
		return description;
	}
	public void  executeQueries(boolean prepare,boolean verbose) throws SQLException{
		rowsExpected=new int[queries.size()]; //initialize the array with correct size
		String query="";
		if(prepare){	
//IC see: https://issues.apache.org/jira/browse/DERBY-2392
			if (verbose)
				System.out.println("=====================> Using java.sql.PreparedStatement <====================");					
		}else{
			if (verbose)
				System.out.println("=====================> Using java.sql.Statement <====================");
			
		}
		try{
			for(int k=0;k<queries.size();k++){
				
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
				query = queries.get(k);
				String [] times=new String [StaticValues.ITER];
				int rowsReturned=0;
				for (int i=0;i<StaticValues.ITER;i++){ 
					
					Statement stmt=null;
					ResultSet rs=null;
					PreparedStatement pstmt=null;
					if(prepare){	
						pstmt=conn.prepareStatement(query);					
					}else{
						stmt=conn.createStatement();
						
					}
					long start=System.currentTimeMillis();
					if(prepare)
						rs=pstmt.executeQuery();
					else
						rs=stmt.executeQuery(query);
					ResultSetMetaData rsmd=rs.getMetaData();
					int totalCols=rsmd.getColumnCount();
					
					while(rs.next()){
						String row="";
						for(int j=1;j<=totalCols;j++){
							row+=rs.getString(j)+" | ";
						}
						rowsReturned++;
					}
					long time_taken=(System.currentTimeMillis() - start);
//IC see: https://issues.apache.org/jira/browse/DERBY-2392
					if (verbose){
						System.out.println("Time required to execute:");
						System.out.println(query);
						System.out.println("Total Rows returned = "+rowsReturned);
					
						System.out.println("==> "+time_taken+" milliseconds "+" OR "+TestUtils.getTime(time_taken));
					}
//IC see: https://issues.apache.org/jira/browse/DERBY-3845
					times[i]=TestUtils.getTime(time_taken);
					rs.close();
					if(prepare){
						pstmt.close();
					}else{
						stmt.close();
					}
					rowsExpected[k]=rowsReturned;//add expected rows for respective queries
					rowsReturned=0;
				}//end for loop to run StaticValues.ITER times
				
				if(prepare){	
					prepStmtRunResults.add(times);
				}else{
					stmtRunResults.add(times);
				}
				
			}
		}catch(SQLException sqe){
			throw new SQLException("Failed query:\n "+query+"\n SQLState= "+sqe.getSQLState()+"\n ErrorCode= "+sqe.getErrorCode()+"\n Message= "+sqe.getMessage());
		}
	}
	public ArrayList<String[]> getPrepStmtRunResults() {
		return prepStmtRunResults;
	}
	public ArrayList<String[]> getStmtRunResults() {
		return stmtRunResults;
	}
	public int getRowsExpected(int index) {
		return rowsExpected[index];
	}
	public ArrayList getQueries() {
		return queries;
	}
	
}
