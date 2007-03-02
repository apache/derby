/*
 
 Derby - Class org.apache.derbyTesting.system.langtest.query.QueryList
 
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
/**
 * Class QueryList: Returns the list of queries to be run as a part of the test. If the
 *                  'query.list' file is provided, this class will read the file and
 *                  return a GenericQuery object 
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.derbyTesting.system.optimizer.StaticValues;

public class QueryList {
	private static ArrayList qList=new ArrayList();
	public static boolean queryListOnly=false;
	public static void init(Connection conn){
		GenericQuery q=new GenericQuery();
		File queryFile = new File(StaticValues.queryFile);
		if(queryFile.exists()){
			System.out.println("External query list found, adding to the run...");
			Properties p=new Properties();
			queryListOnly=true;
			try{
				p.load(new FileInputStream(queryFile));
				q.generateQueries(p);
				getQList().add(q);
				if(queryListOnly){
					return;
				}
			}catch(IOException ioe){
				ioe.printStackTrace();
			}
		}
		
		q=new Query1();
		getQList().add(q);
		q=new Query2();
		getQList().add(q);
		q=new Query3();
		getQList().add(q);
		q=new Query4();
		getQList().add(q);
		q=new Query5();
		getQList().add(q);
		q=new Query6();
		getQList().add(q);
		
	}
	public static ArrayList getQList() {
		return qList;
	}
}
