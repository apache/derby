/*
 * 
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.tasks.Backup
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
 * This class is used to do the back up activity and inline compression
 */
import java.sql.Connection;

import org.apache.derbyTesting.system.mailjdbc.MailJdbc;
import org.apache.derbyTesting.system.mailjdbc.utils.DbTasks;
import org.apache.derbyTesting.system.mailjdbc.utils.LogFile;

public class Backup extends Thread {
	private boolean isRunning = false;

	private DbTasks dbtasks = new DbTasks();

	private Connection conn = DbTasks.getConnection("BACKUP", "Backup");

	public Backup(String name) {
		setName(name);
	}

	public void run() {
		try {
			while (true) {
				doWork();
				try {
					Thread.sleep(150000);
				} catch (InterruptedException ie) {
					MailJdbc.logAct.logMsg("#### " + getName()
							+ "...Interrupted");
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			MailJdbc.logAct.logMsg(LogFile.ERROR
					+ "Error while sleeping the thread in Backup: "
					+ e.getMessage());
		}

	}

	//Notifies backup thread is active and then does compress and takes Backup
	private void doWork() {
		isRunning = true;
		try {
			DoCompress();
			DoBackup();
		} catch (Exception e) {
			MailJdbc.logAct.logMsg(LogFile.ERROR
					+ "Error while doing work with the thread in Backup: "
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	public void DoCompress() {
		dbtasks.compressTable(conn, "INBOX", this.getName());
		dbtasks.compressTable(conn, "ATTACH", this.getName());
	}

	public void DoBackup() {
		dbtasks.Backup(conn, this.getName());
	}

	public boolean isRunning() {
		return isRunning;
	}
}
