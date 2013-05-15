/*
 * 
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.utils.ThreadUtils
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
/**
 * This class is used to keep the threads in an ArrayList and starts them 
 * also checks whether the thread is alive or sleeping
 */
import java.util.ArrayList;

import org.apache.derbyTesting.system.mailjdbc.MailJdbc;
import org.apache.derbyTesting.system.mailjdbc.tasks.Backup;
import org.apache.derbyTesting.system.mailjdbc.tasks.Browse;
import org.apache.derbyTesting.system.mailjdbc.tasks.Purge;
import org.apache.derbyTesting.system.mailjdbc.tasks.Refresh;

public class ThreadUtils {
	private static ArrayList<Thread> userThreads = new ArrayList<Thread>();

	public static ThreadUtils threadutil = new ThreadUtils();

	//constructor which will start the threads
	public static void startThreads() {
		threadutil.run();
	}

	public void run() {
		Thread t = null;
		try {
			//Starting Refresh Thread
			t = new Refresh("Refresh Thread");
			t.start();
			MailJdbc.logAct.logMsg(LogFile.INFO + "Started: " + t.getName());
			userThreads.add(t);
			//Starting browsing thread
			t = new Browse("Browsing Thread");
			t.start();
			MailJdbc.logAct.logMsg(LogFile.INFO + "Started: " + t.getName());
			userThreads.add(t);
			//Starting Purge Thread
			t = new Purge("Purging Thread");
			int sleep_time = (int) 150000; //Due the cascade constriant
									      // This is the number that
										  // make sure insert attachment has been finished
			Thread.sleep(sleep_time);
			t.start();
			MailJdbc.logAct.logMsg(LogFile.INFO + "Started: " + t.getName() + " with 150000 sleep time");
			userThreads.add(t);
			//Starting Backup Thread
			t = new Backup("Backup Thread");
			t.start();
			MailJdbc.logAct.logMsg(LogFile.INFO + "Started: " + t.getName());
			userThreads.add(t);
			sleep_time = (int) (Math.random() * 15000);
			Thread.sleep(sleep_time);
		} catch (Exception e) {
			MailJdbc.logAct
					.logMsg(LogFile.ERROR
							+ "Exception while starting the threads: "
							+ e.getMessage());
		}
	}

	public static synchronized boolean isThreadRunning(String name) {
		//checks and returns true is the Refresh thread is active
		if (name.equalsIgnoreCase("Refresh Thread")) {
			Refresh rt = (Refresh) userThreads.get(0);
			return rt.isRunning();
		} else
			return false;
	}

	public static synchronized Thread getThread(String name) {
		if (name.equalsIgnoreCase("Refresh Thread")) {
			return (Refresh) userThreads.get(0);
		} if (name.equalsIgnoreCase("Purging Thread")) {
			return (Purge) userThreads.get(2);
		} else {
			return null;
		}
	}

	public ThreadUtils getInstance() {
		return threadutil;
	}
}
