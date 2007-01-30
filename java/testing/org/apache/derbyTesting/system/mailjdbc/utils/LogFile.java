/*
 * 
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.utils.LogFile
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
package org.apache.derbyTesting.system.mailjdbc.utils;

import java.io.FileOutputStream;
import java.io.PrintWriter;

//	utility class that logs messages to the given log file
public class LogFile {
	public static String ERROR = " : ERROR :";

	public static String WARN = " : WARNING :";

	public static String INFO = " : INFO :";

	public PrintWriter log;

	//Constructor that will initialize the output log file
	public LogFile(String logFileName) {
		try { //auto-flush printwriter
			log = new PrintWriter(new FileOutputStream(logFileName), true);
		} catch (Exception e) {
			System.out.println("Exception in LogFile.java: " + e.getMessage());
		}
	}

	//closing the log file
	public void closeLog() {
		try {
			log.close();
		} catch (Exception e) {
			System.out.println("Exception closing the log file: "
					+ e.getMessage());
		}
	}

	//logging the supplied message to the logfile
	public synchronized void logMsg(String msg) {
		log.println(msg);
	}
}

