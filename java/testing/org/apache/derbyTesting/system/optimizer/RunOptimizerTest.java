/*
 
 Derby - Class org.apache.derbyTesting.system.optimizer.RunOptimizerTest
 
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
package org.apache.derbyTesting.system.optimizer;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.derbyTesting.system.optimizer.query.GenericQuery;
import org.apache.derbyTesting.system.optimizer.query.QueryList;
import org.apache.derbyTesting.system.optimizer.utils.DataUtils;
import org.apache.derbyTesting.system.optimizer.utils.TestViews;
/**
 * 
 * Class RunOptimizerTest: The main class that runs this test. 
 * 
 * To run the test use:
 * 
 * java org.apache.derbyTesting.system.optimizer.RunOptimizerTest -reset|-qlist
 * -reset = Reset the database and begin run
 * -qlist = Run only test queries from the 'query.list' file provided
 * No arguments will run all the default test queries, provided via classes
 * Query1 - Query6 in this test case.
 * 
 * Set the 'derby.langtest.mode' to 'client' to run this test using the 
 * DerbyClient against a Derby Network Server running on port 1527
 */

public class RunOptimizerTest {

	public static void main(String[] args) {
		Connection conn = null;
		String driverClass=StaticValues.embedClass;
		String jdbcurl=StaticValues.embedURL;
		boolean reset=false;
		boolean verbose=false;
		try {
			String mode=System.getProperty("derby.optimizertest.mode");
			if(mode!=null){
				if(mode.equalsIgnoreCase("client")){
					driverClass=StaticValues.clientClass;
					jdbcurl=StaticValues.clientURL;
				}else{
					driverClass=StaticValues.embedClass;
					jdbcurl=StaticValues.embedURL;
				}
			}

			File dir = new File("testdb");
			if((!dir.exists())){
				reset=true; // If nonexisting must always .init and .createObjects
			}

			System.out.println("Running test with url "+jdbcurl);
			if(args.length>0){
				for(int i=0;i<args.length;i++){
					if(args[i].equalsIgnoreCase("-reset"))
						reset=true;
						else if(args[i].equalsIgnoreCase("-qlist"))
							QueryList.queryListOnly=true;
						else if(args[i].equalsIgnoreCase("-verbose"))
							verbose=true;
						else{
							printUsage();
							return;
						}
				}
				}
			
			Class.forName(driverClass);
			if (reset) { // Must also be done if db nonexisting
                System.out.println("Initializing db ...");
				conn = DriverManager.getConnection(jdbcurl);
				TestViews.init();
				DataUtils.dropObjects(conn,verbose);
				DataUtils.createObjects(conn,verbose);
			}else{
                System.out.println("Use existing db ...");
				conn = DriverManager.getConnection(jdbcurl);
			}
			DataUtils.insertData(conn,verbose);
			QueryList.init(conn);
			if (verbose)
				System.out.println(" List of query scenarios to run: "+QueryList.getQList().size());
            System.out.println("Starting tests ...");
			for(int i=0;i<QueryList.getQList().size();i++){
				if (verbose)
					System.out.println("\n______________________________________________________________________\n");
				GenericQuery gq=(GenericQuery)QueryList.getQList().get(i);
				if (verbose)
					System.out.println("*** Running query: "+gq.getDescription()+" ***");
				conn=null; //conn.close() throws "Invalid transaction state" exception
				conn = DriverManager.getConnection(jdbcurl);
				gq.setConnection(conn);
				gq.executeQueries(false,verbose); //using regular STATEMENTS
				conn.close();
				conn=null; //conn.close() throws "Invalid transaction state" exception
				conn = DriverManager.getConnection(jdbcurl);
				gq.setConnection(conn);
				gq.executeQueries(true,verbose); //using prepared STATEMENTS
				
			}
		} catch (ClassNotFoundException cne) {
			System.out.println("Class not found Exception: " + cne.getMessage());
		} catch (SQLException sqe) {
			System.out.println("SQL Exception :" + sqe);

			sqe.printStackTrace();
		}catch (Exception e){
			System.out.println("Unexpected Exception "+e);
			e.printStackTrace();
		}
		printResults();
	}
	private static void printUsage(){
		System.out.println("Usage:");
		System.out.println("\njava org.apache.derbyTesting.system.optimizer.RunOptimizerTest -reset|-qlist\n");
		System.out.println("-reset = Reset the database and begin run");
		System.out.println("-qlist = Run only test queries from the 'query.list' file provided");
		System.out.println("\nNo arguments will run all the default test queries available in this test case.\n");
	}
	private static void printResults(){
		System.out.println("\n\n========================= R E S U L T S =========================\n");
			for(int i=0;i<QueryList.getQList().size();i++){
			System.out.println("\n________________________________________________________________________________________________");
			GenericQuery gq=(GenericQuery)QueryList.getQList().get(i);
			if (gq.getPrepStmtRunResults().size()==0){
				System.out.println("Queries didn't run");
				System.exit(0);
			}
			else{
			System.out.println("Timings for Query type: "+gq.getDescription()+"\n");
			System.out.println("QueryName\tUsing PreparedStatment\tUsing Statement\tRows Expected");
			System.out.println("------------------------------------------------------------------------------");
			System.out.println("Query size: " + gq.getQueries().size());
			for(int k=0;k<gq.getQueries().size();k++){
				String queryName="QUERY # "+(k+1);
				String[] prepStmtTimes=(String [])gq.getPrepStmtRunResults().get(k);
				String [] stmtTimes=(String [])gq.getStmtRunResults().get(k);
				for(int j=0; j<StaticValues.ITER;j++){
					System.out.println(queryName+"\t"+prepStmtTimes[j]+"\t\t"+stmtTimes[j]+"\t"+gq.getRowsExpected(k));
				}
			
				System.out.println("*************************************************************************");
				
			}
			System.out.println("\n________________________________________________________________________________________________");
			}
		}
			
	}
}
