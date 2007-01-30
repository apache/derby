/*
 * 
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.tasks.Browse
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
package org.apache.derbyTesting.system.mailjdbc.tasks;
/**
 * This class is used to do read, and update activity on the inbox and attach table
 */
import java.sql.Connection;

import org.apache.derbyTesting.system.mailjdbc.MailJdbc;
import org.apache.derbyTesting.system.mailjdbc.utils.DbTasks;
import org.apache.derbyTesting.system.mailjdbc.utils.ThreadUtils;
import org.apache.derbyTesting.system.mailjdbc.utils.LogFile;

public class Browse extends Thread {
	private DbTasks dbtasks = new DbTasks();

	private Connection conn = DbTasks.getConnection("BROWSE", "Browse");

	public Browse(String name) {
		//sets the name of the thread
		setName(name);
	}

	public void run() {
		try {
			while (true) {
				//*Does the functions like browsing the inbox, delete mail by
				// the user,
				//*moving mails to different folders
				readInbox(conn, this.getName());
				deleteMailByUser(conn, this.getName());
				moveToFolders(conn, this.getName());
				Thread.sleep(30000);
				//Checking whther Refresh thread is running after doing the
				// work
				// If not interrupt the thread to do a refresh
				if (ThreadUtils.isThreadRunning("Refresh Thread")) {
					MailJdbc.logAct.logMsg("******** Refresh is running");
				} else {
					Refresh th = (Refresh) ThreadUtils
							.getThread("Refresh Thread");
					th.interrupt();
				}
			}
		} catch (Exception e) {
			MailJdbc.logAct.logMsg(LogFile.ERROR
					+ "Error while sleeping the thread in Browse: "
					+ e.getMessage());
			e.printStackTrace();
		}

	}

	public void readInbox(Connection conn, String thread_name) {
		dbtasks.readMail(conn, thread_name);
		dbtasks.totals();
	}

	public void deleteMailByUser(Connection conn, String thread_name) {
		dbtasks.deleteMailByUser(conn, thread_name);
	}

	public void moveToFolders(Connection conn, String thread_name) {
		dbtasks.moveToFolders(conn, thread_name);
	}
}
