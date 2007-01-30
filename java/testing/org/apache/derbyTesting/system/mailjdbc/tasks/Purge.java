/*
 * 
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.tasks.Purge
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
 * This class is used to delete and mails which are older than a day
 */

import java.sql.Connection;

import org.apache.derbyTesting.system.mailjdbc.MailJdbc;
import org.apache.derbyTesting.system.mailjdbc.utils.DbTasks;
import org.apache.derbyTesting.system.mailjdbc.utils.LogFile;

public class Purge extends Thread {
	//This thread behaves as a backend service which will mionitor the expiry
	// date and size of the database
	//and deletes them.
	private DbTasks dbtasks = new DbTasks();

	private Connection conn = DbTasks.getConnection("PURGE", "Purge");

	public Purge(String name) {
		//Sets the thread name
		setName(name);
	}

	public void run() {
		try {
			while (true) {
				//Wait for some activity to happen before deleting the mails,
				// so sleep for sometime
				Thread.sleep(120000);
				//Deleting mails
				purgeFromInbox(conn);
				//Gets the size of the database
				DoDbSizeCheck();
				int sleep_time = (int) (Math.random() * 15000);
				Thread.sleep(sleep_time);
			}
		} catch (Exception e) {
			MailJdbc.logAct.logMsg(LogFile.ERROR
					+ "Error while sleeping the thread in Purge: "
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	public void purgeFromInbox(Connection conn) {
		dbtasks.deleteMailByExp(conn, this.getName());
	}

	public void DoDbSizeCheck() {
		dbtasks.checkDbSize(conn, this.getName());
	}
}
