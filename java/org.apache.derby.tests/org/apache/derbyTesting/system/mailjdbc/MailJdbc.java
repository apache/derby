/*
 * 
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.MailJdbc
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *  
 */
package org.apache.derbyTesting.system.mailjdbc;

import org.apache.derbyTesting.system.mailjdbc.utils.DbTasks;
import org.apache.derbyTesting.system.mailjdbc.utils.ThreadUtils;
import org.apache.derbyTesting.system.mailjdbc.utils.LogFile;
/**
 * This has the main method with arguements for embedded and NWserver.
 */
public class MailJdbc {
	//Prints out the activities/transactions done by the test
	public static LogFile logAct = new LogFile("Activity.out");

	public static void main(String[] args) throws Exception {
		boolean useexistingdb = false;
		String type = args[0];
//IC see: https://issues.apache.org/jira/browse/DERBY-4203
		if (args.length > 1 && args[1].equals("samedb"))
			useexistingdb = true;
		System.out.println("Test started with " + type + " driver");
		//Loads the driver
		DbTasks.jdbcLoad(type, useexistingdb);
		//Starts all 4 threads
		ThreadUtils.startThreads();
	}
}
