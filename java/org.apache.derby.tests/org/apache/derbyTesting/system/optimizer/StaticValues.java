/*
 
 Derby - Class org.apache.derbyTesting.system.langtest.StaticValues
 
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
/**
 * 
 * Class StaticValues: A location to store all the common static values used in 
 * this test
 *
 */
public class StaticValues {
	public static String clientURL="jdbc:derby://localhost:1527/testdb;create=true";
	public static String clientClass="org.apache.derby.jdbc.ClientDriver";
	
	public static String embedURL="jdbc:derby:testdb;create=true";
	public static String embedClass="org.apache.derby.jdbc.EmbeddedDriver";
	
	public static int NUM_OF_ROWS=1000; //Total number of rows expected in each table
	public static int NUM_OF_TABLES=64; //Total number of tables to be created
	public static int ITER=2; 			//Number of iterations of each query
	    
	public static String queryFile="query.list"; //File name that contains the custom queries 
	//SCHEMA OBJECTS
	public static String DROP_TABLE="DROP TABLE ";
	public static String CREATE_TABLE="CREATE TABLE ";
	public static String TABLE_NAME="MYTABLE";
	public static String TABLE_COLS="(col1 INT primary key, col2 VARCHAR(100),col3 VARCHAR(100),col4 VARCHAR(30),col5 VARCHAR(30),col6 varchar(30),col7 VARCHAR(40), col8 INT, col9 timestamp)";
	public static String CREATE_VIEW="CREATE VIEW ";
	public static String VIEW1_COLS="col1, col2, col3, col4, col5, col6, col7 from ";
	public static String VIEW2_COLS="col1, col2, col3, col4, col5, col6, col7, col8, col9 from ";
	
	//INSERT
	public static String INSERT_TABLE="INSERT INTO ";
	public static String INSERT_VALUES=" VALUES(?,?,?,?,?,?,?, ?, ?) ";
	
	public static void init(){
		//TODO Load from property file
	}
}
