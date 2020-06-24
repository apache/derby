/*
 * 
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.tasks.Refresh
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
 * This class is used to insert, delete and update the rows
 */

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.derbyTesting.system.mailjdbc.MailJdbc;
import org.apache.derbyTesting.system.mailjdbc.utils.DbTasks;
import org.apache.derbyTesting.system.mailjdbc.utils.LogFile;

public class Refresh extends Thread {
	//This is the thread which does most of the work.
	private boolean isRunning = false;

	private DbTasks dbtasks = new DbTasks();

	private Connection conn = null;

	public Refresh(String name) throws Exception{
		//sets the thread name
		setName(name);
//IC see: https://issues.apache.org/jira/browse/DERBY-3448
		conn = DbTasks.getConnection("REFRESH", "Refresh");
	}

	public void run() {
		try {
			//Applying permission to other threads/users
			grantRevoke(conn, this.getName());
			while (true) {
				doWork();
				try {
//IC see: https://issues.apache.org/jira/browse/DERBY-4166
					Thread.sleep(150000);
				} catch (InterruptedException ie) {
					MailJdbc.logAct.logMsg("#### " + getName()
							+ "...Interrupted");
					conn.commit();
					MailJdbc.logAct.logMsg("#### " + getName()
							+ "...commit connection...");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			MailJdbc.logAct.logMsg(LogFile.ERROR
					+ "Error while sleeping the thread in refresh: "
					+ e.getMessage());
		}

	}

	/**
	 * @throws InterruptedException
	 */
	private void doWork() {
		isRunning = true;
		//Thread is running and does inserting the mails and deleting the mails
		try {
			insertMail(conn, this.getName());
			//Try to avoid deadlock situation by Purge thread
//IC see: https://issues.apache.org/jira/browse/DERBY-4166
            Thread.sleep(60000);
		} catch (Exception e) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + "insertMail() failed "
					+ e.getMessage());
		}
		try {
			MailJdbc.logAct.logMsg(LogFile.INFO + "Deleting mail by Refresh Thread : ");
			deleteMailByRefresh(conn, this.getName());
		} catch (Exception e) {
			MailJdbc.logAct.logMsg(LogFile.ERROR
					+ "deleteMailByRefresh() failed " + e.getMessage());
			try {
				conn.rollback();
			} catch (SQLException se) {
				MailJdbc.logAct.logMsg(LogFile.ERROR
						+ "rollback connection on Refresh failed..."
						+ se.getMessage());
			}  			
			MailJdbc.logAct.logMsg("#### " + getName()
					+ "...rollback connection after deleteMailByRefresh...");
		}
		MailJdbc.logAct.logMsg(LogFile.INFO + "Refresh doWork() completed");
		isRunning = false;
	}


	public void grantRevoke(Connection conn, String thread_name)
			throws Exception {
		dbtasks.grantRevoke(conn, thread_name);
	}

	public void insertMail(Connection conn, String thread_name)
			throws Exception {
		dbtasks.insertMail(conn, thread_name);
	}

	public void deleteMailByRefresh(Connection conn, String thread_name)
			throws Exception {

		dbtasks.deleteMailByThread(conn, thread_name);
	}

	/**
	 * @return Returns the isRunning.
	 */
	public boolean isRunning() {
		return isRunning;
	}
}
