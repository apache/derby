/*
 *
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.utils.DbTasks
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
 * This class is used all other classes for various tasks like insert, delete,
 * backup etc
 */

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.system.mailjdbc.MailJdbc;

public class DbTasks extends Thread {

	public static LogFile log = new LogFile("performance.out");

	static boolean saveAutoCommit;

	private static int id_count = 0;

	public static int insert_count = 0;

	public static int delete_count = 0;

	public static int clob_count = 0;

	public static int blob_count = 0;

	public static Random Rn = new Random();

	public static Properties prop = new Properties();

	public static void jdbcLoad(String driverType) {
		setSystemProperty("derby.database.sqlAuthorization", "true");
		if (driverType.equalsIgnoreCase("embedded")) {
			setSystemProperty("driver", "org.apache.derby.jdbc.EmbeddedDriver");
			MailJdbc.logAct
					.logMsg(" \n*****************************************************");
			MailJdbc.logAct.logMsg("\n\n\tStarting the test in Embedded mode");
			MailJdbc.logAct
					.logMsg("\n\n*****************************************************");
			// setting the properties like user, password etc for both the
			// database and the backup datatbase
			setSystemProperty("database", "jdbc:derby:mailsdb;create=true");
			setSystemProperty("ij.user", "REFRESH");
			setSystemProperty("ij.password", "Refresh");
		} else {
			setSystemProperty("driver", "org.apache.derby.jdbc.ClientDriver");
			setSystemProperty("database",
					"jdbc:derby://localhost:1527/mailsdb;create=true;user=REFRESH;password=Refresh");
			MailJdbc.logAct
					.logMsg(" \n*****************************************************");
			MailJdbc.logAct
					.logMsg("\n\n\tStarting the test in NetworkServer mode");
			MailJdbc.logAct
					.logMsg("\n\n*****************************************************");
		}
		try {
			// Create the schema (tables)
			long s_schema = System.currentTimeMillis();
			org.apache.derby.tools.ij
					.main(new String[] { "-fr",
							"/org/apache/derbyTesting/system/mailjdbc/schema/schema.sql" });
			long e_schema = System.currentTimeMillis();
			log
					.logMsg(" \n*****************************************************");
			log.logMsg("\n\n\tPerformance Info for the Test on" + s_schema);
			log
					.logMsg("\n\n*****************************************************");
			log.logMsg(LogFile.INFO + "Schema Creation :"
					+ PerfTime.readableTime(e_schema - s_schema));
			System.out.println("created the schema");
		} catch (Exception e) {
			log.logMsg(LogFile.ERROR
					+ "Exception while running loading and creating tables: "
					+ e.getMessage());
			e.printStackTrace();
			errorPrint(e);
		}

	}

	public static Connection getConnection(String usr, String passwd) {
		try {
			// Returns the Connection object
			Class.forName(System.getProperty("driver")).newInstance();
			prop.setProperty("user", usr);
			prop.setProperty("password", passwd);
			Connection con = DriverManager.getConnection(System
					.getProperty("database"), prop);
			return con;
		} catch (Exception e) {
			log.logMsg(LogFile.ERROR
					+ "Error while getting connection for threads:"
					+ e.getMessage());
			e.printStackTrace();
			errorPrint(e);
			return null;
		}
	}

	public void readMail(Connection conn, String thread_name) {
		// This function will be reading mails from the inbox.
		// Getiing the number of rows in the table and getting the
		// size of the attachment (Blob) for a randomly selected row
		Statement stmt = null;
		Statement stmt1 = null;
		int count = 0;
		int count1 = 0;
		long size = 0;
		try {
			saveAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			conn
					.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			long s_select = System.currentTimeMillis();
			stmt = conn.createStatement();
			stmt1 = conn.createStatement();
			ResultSet rs = stmt.executeQuery(Statements.getRowCount);
			ResultSet rs1 = stmt1.executeQuery(Statements.getRowCountAtach);
			while (rs.next()) {
				count = rs.getInt(1);
			}
			while (rs1.next()) {
				count1 = rs1.getInt(1);
			}
			if (count == 0)
				MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
						+ "Inbox is empty");
			long e_select = System.currentTimeMillis();
			MailJdbc.logAct
					.logMsg(LogFile.INFO + thread_name + " : "
							+ "The number of mails in the REFRESH.INBOX are : "
							+ count);
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
					+ "The number of mails in the attachment table are : "
					+ count1);
			log.logMsg(LogFile.INFO + thread_name + " : "
					+ "Time taken to scan the entire REFRESH.INBOX for count :"
					+ PerfTime.readableTime(e_select - s_select));
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt1.close();
			if (rs1 != null)
				rs1.close();
		} catch (SQLException sqe) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
					+ "SQL Exception while reading : " + sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
			try {
				conn.rollback();
			} catch (SQLException sq) {
				MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
						+ "Exception while rolling back: " + sq);
				errorPrint(sq);
				sq.printStackTrace();
			}
		}
		try {
			int attach_id = Rn.nextInt(count - 1);
			ResultSet rs = stmt
					.executeQuery("select attachment from REFRESH.attach where id  = "
							+ attach_id);
			long start = System.currentTimeMillis();
			if (rs.next()) {
				size = rs.getBlob(1).length();
				MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
						+ "size of the attachment for id " + attach_id
						+ " is : " + size);
			} else
				MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
						+ "no attachment");
			if (rs != null)
				rs.close();

			long end = System.currentTimeMillis();
			log.logMsg(LogFile.INFO + thread_name + " : "
					+ "Time taken to get the blob :"
					+ PerfTime.readableTime(end - start));
			rs = stmt
					.executeQuery("select message from REFRESH.INBOX where id  = "
							+ attach_id);
			long start_t = System.currentTimeMillis();
			if (rs.next()) {
				size = rs.getClob(1).length();
				MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
						+ "size of the message for id " + attach_id + " is : "
						+ size);
			} else
				MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
						+ "mail with the id " + attach_id + " does not exist");
			long end_t = System.currentTimeMillis();
			log.logMsg(LogFile.INFO + thread_name + " : "
					+ "Time taken to get the clob :"
					+ PerfTime.readableTime(end_t - start_t));
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			conn.commit();
			conn.setAutoCommit(saveAutoCommit);
		} catch (SQLException sqe) {
			MailJdbc.logAct
					.logMsg(LogFile.ERROR
							+ thread_name
							+ " : "
							+ "SQL Exception while getting the message and attach size : "
							+ sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
			try {
				conn.rollback();
			} catch (SQLException sq) {
				MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
						+ "Exception while rolling back: " + sq);
				sq.printStackTrace();
				errorPrint(sq);
			}
		}

	}

	public synchronized void deleteMailByUser(Connection conn,
			String thread_name) {
		// Delete done by the user. Thre user will mark the mails to be deleted
		// and then
		int id_count = 0;
		try {
			saveAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			PreparedStatement updateUser = conn
					.prepareStatement(Statements.updateStr);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt
					.executeQuery("select max(id)from REFRESH.INBOX ");
			if (rs.next())
				id_count = rs.getInt(1);
			short to_delete = 1;
			int id = Rn.nextInt(id_count - 1);
			long s_update = System.currentTimeMillis();
			int delete_count = 0;
			for (int i = 0; i < id; i++) {
				updateUser.setShort(1, to_delete);
				int for_id = Rn.nextInt(id_count - 1);
				updateUser.setInt(2, for_id);
				int del = updateUser.executeUpdate();
				delete_count = delete_count + del;
			}
			long e_update = System.currentTimeMillis();
			log.logMsg(LogFile.INFO + thread_name + " : "
					+ " Time taken to mark the mails to be deleted :"
					+ PerfTime.readableTime(e_update - s_update));
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
					+ "The number of mails marked to be deleted  by user:"
					+ delete_count);
			if (rs != null)
				rs.close();
			if (updateUser != null)
				updateUser.close();
			if (stmt != null)
				stmt.close();
			conn.commit();
			conn.setAutoCommit(saveAutoCommit);
		} catch (SQLException sqe) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
					+ "Exception while deleting mail by user: "
					+ sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
			try {
				conn.rollback();
			} catch (SQLException sq) {
				MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
						+ "Exception while rolling back: " + sq);
				sq.printStackTrace();
				errorPrint(sq);
			}
		}
	}

	public void deleteMailByThread(Connection conn, String thread_name)
			throws Exception {
		// Deleting mails which are marked to be deleted
		try {
			saveAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			PreparedStatement deleteThread = conn
					.prepareStatement(Statements.deleteStr);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt
					.executeQuery("select count(*) from REFRESH.INBOX where to_delete=1");
			long s_delete = System.currentTimeMillis();
			int count = 0;
			count = deleteThread.executeUpdate();
			long e_delete = System.currentTimeMillis();
			log.logMsg(LogFile.INFO + thread_name + " : "
					+ "Time taken to delete mails by thread :"
					+ PerfTime.readableTime(e_delete - s_delete));
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : " + count
					+ " rows deleted");
			delete_count = delete_count + count;
			if (deleteThread != null)
				deleteThread.close();
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			conn.commit();
			conn.setAutoCommit(saveAutoCommit);
		} catch (SQLException sqe) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
					+ "Exception while deleting mail by Thread: "
					+ sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
			try {
				conn.rollback();
			} catch (SQLException sq) {
				MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
						+ "Exception while rolling back: " + sq);
				sq.printStackTrace();
				errorPrint(sq);
				throw sqe;
			}
		}

	}

	public void moveToFolders(Connection conn, String thread_name) {
		// Changing the folder id of randomly selected rows
		try {
			saveAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			PreparedStatement moveToFolder = conn
					.prepareStatement(Statements.movefolder);
			ResultSet rs = stmt.executeQuery(Statements.getRowCount);
			if (!(rs.next()))
				MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
						+ "no message in the REFRESH.INBOX to move");
			else {
				int count = rs.getInt(1);
				int folder_id = Rn.nextInt(5 - 1);
				int message_id = Rn.nextInt(count - 1);
				moveToFolder.setInt(1, folder_id);
				moveToFolder.setInt(2, message_id);
				long s_folder = System.currentTimeMillis();
				moveToFolder.executeUpdate();
				long e_folder = System.currentTimeMillis();
				log.logMsg(LogFile.INFO + thread_name + " : "
						+ "Time taken to move a mail to the folder :"
						+ PerfTime.readableTime(e_folder - s_folder));
				MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
						+ "Mail with id : " + message_id
						+ " is moved to folder with id : " + folder_id);
			}
			if (stmt != null)
				stmt.close();
			if (moveToFolder != null)
				moveToFolder.close();
			if (rs != null)
				rs.close();
			conn.commit();
			conn.setAutoCommit(saveAutoCommit);
		} catch (SQLException sqe) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
					+ "Exception while moving mail to folders: "
					+ sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
			try {
				conn.rollback();
			} catch (SQLException sq) {
				MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
						+ "Exception while rolling back: " + sq);
				sq.printStackTrace();
				errorPrint(sq);
			}
		}

	}

	public void insertMail(Connection conn, String thread_name)
			throws Exception {
		// Inserting rows to the inbox table. Making attach_id of randomly
		// selected rows to be one
		// and for those rows inserting blobs in the attach table
		Statement stmt = conn.createStatement();
		int num = Rn.nextInt(10 - 1);
		System.out.println("num: " + num);
		InputStream streamIn = null;
		Reader streamReader = null;
		try {
			saveAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			PreparedStatement insertFirst = conn.prepareStatement(
					Statements.insertStr, Statement.RETURN_GENERATED_KEYS);
			String name = new String("ABCD");
			String l_name = new String("WXYZ");
			long s_insert = System.currentTimeMillis();
			for (int i = 0; i < num; i++) {
				String new_name = new String(increment(name, 60));
				String new_lname = new String(decrement(l_name, 60));
				insertFirst.setString(1, new_name);
				insertFirst.setString(2, new_lname);
				insertFirst.setTimestamp(3, new Timestamp(System
						.currentTimeMillis()));
				name = new_name;
				l_name = new_lname;
				try {
					// to create a stream of random length between 200 bytes and 3MB
					int clobLength = Rn.nextInt(3078000 - 200 + 1) + 200;
					streamReader = new LoopingAlphabetReader(clobLength,
							CharAlphabet.modernLatinLowercase());
					insertFirst.setCharacterStream(4, streamReader, clobLength);
				} catch (Exception e) {
					MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
							+ "File not found Exception : " + e.getMessage());
					errorPrint(e);
					throw e;
				}
				int rand_num = Rn.nextInt(10 - 1);
				if (i == rand_num) {
					ResultSet rs = stmt
							.executeQuery("select count(*) from REFRESH.INBOX where attach_id>0");
					while (rs.next()) {
						id_count = rs.getInt(1);
						insertFirst.setInt(5, rs.getInt(1) + 1);
					}

					if (rs != null)
						rs.close();
					conn
							.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				} else
					insertFirst.setInt(5, 0);
				insertFirst
						.setString(
								6,
								"This column is used only to by pass the space problem. If the problem still exists, then we are going to "
										+ "have a serious issue here.*****************************************************************************************************");
				int result = insertFirst.executeUpdate();
				if (result != 0) {
					insert_count = insert_count + 1;
				}
			}
			if (insertFirst != null)
				insertFirst.close();
			conn.commit();
			streamReader.close();

			long e_insert = System.currentTimeMillis();
			log.logMsg(LogFile.INFO + thread_name + " : "
					+ "Time taken to insert " + num + "rows :"
					+ PerfTime.readableTime(e_insert - s_insert));
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
					+ "number of mails inserted : " + num);

		} catch (SQLException sqe) {
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
					+ "Error while inserting into REFRESH.INBOX:"
					+ sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
			try {
				conn.rollback();
			} catch (SQLException sq) {
				MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
						+ "Exception while rolling back: " + sq);
				sq.printStackTrace();
				errorPrint(sq);
				throw sqe;
			}
		}
		try {
			PreparedStatement insertAttach = conn
					.prepareStatement(Statements.insertStrAttach);
			Statement stmt1 = conn.createStatement();
			ResultSet rs = stmt1
					.executeQuery("select id,attach_id from REFRESH.INBOX where attach_id >"
							+ id_count);
			int row_count = 0;
			long a_start = System.currentTimeMillis();
			while (rs.next()) {
				insertAttach.setInt(1, rs.getInt(1));
				insertAttach.setInt(2, rs.getInt(2));
				try {
					// to create a stream of random length between 0 and 5M
					int blobLength = Rn.nextInt(5130000 - 0 + 1) + 0;
					streamIn = new LoopingAlphabetStream(blobLength);
					insertAttach.setBinaryStream(3, streamIn, blobLength);
				} catch (Exception e) {
					MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
							+ "Exception : " + e.getMessage());
					errorPrint(e);
					throw e;
				}
				int result_attach = insertAttach.executeUpdate();
				streamIn.close();
				if (result_attach != 0) {
					blob_count = blob_count + 1;
					row_count++;
				}

			}
			long a_end = System.currentTimeMillis();
			log.logMsg(LogFile.INFO + thread_name + " : "
					+ "Time taken to insert " + row_count + "attachments :"
					+ PerfTime.readableTime(a_end - a_start));
			id_count++;
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if (stmt1 != null)
				stmt1.close();
			if (insertAttach != null)
				insertAttach.close();
			conn.commit();
			conn.setAutoCommit(saveAutoCommit);
		} catch (SQLException sqe) {
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
					+ "Error while inserting attachments:" + sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
			try {
				conn.rollback();
			} catch (SQLException sq) {
				MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
						+ "Exception while rolling back: " + sq);
				sq.printStackTrace();
				errorPrint(sq);
				throw sqe;
			}
		}
	}

	public synchronized void deleteMailByExp(Connection conn, String thread_name) {
		try {
			// Deleting mails which are older than 1 day
			saveAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			long s_delExp = System.currentTimeMillis();
			Statement selExp = conn.createStatement();
			PreparedStatement deleteExp = conn
					.prepareStatement(Statements.delExp);
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
					+ "delete mails which are older than 1 day");
			int count = 0;
			count = selExp.executeUpdate(Statements.del_jdbc_exp);
			long e_delExp = System.currentTimeMillis();
			log.logMsg(LogFile.INFO + thread_name + " : "
					+ "Time taken to delete " + count + "mails :"
					+ PerfTime.readableTime(e_delExp - s_delExp));
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
					+ " number of mails deleted : " + count);
			delete_count = delete_count + count;
			if (deleteExp != null)
				deleteExp.close();
			if (selExp != null)
				selExp.close();
			conn.commit();
			conn.setAutoCommit(saveAutoCommit);
		} catch (SQLException sqe) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
					+ "Error while deleting mails by expiry manager: "
					+ sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
			try {
				conn.rollback();
			} catch (SQLException sq) {
				MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
						+ "Exception while rolling back: " + sq);
				sq.printStackTrace();
				errorPrint(sq);
			}
		}
	}

	public void Backup(Connection conn, String thread_name) {
		// when the backup thread kicks in, it will use this function to
		// take the periodic backups
		long s_backup = System.currentTimeMillis();
		try {
			saveAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(true);
			CallableStatement cs = conn
					.prepareCall("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT(?, ?)");
			cs.setString(1, System.getProperty("user.dir") + File.separator
					+ "mailbackup");
			cs.setInt(2, 1);
			cs.execute();
			cs.close();
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
					+ "Finished backing up the Database");
			conn.commit();
			conn.setAutoCommit(saveAutoCommit);
		} catch (Throwable sqe) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
					+ "Error while doing the backup system procedure: "
					+ sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
		}
		long e_backup = System.currentTimeMillis();
		log.logMsg(LogFile.INFO + thread_name + " : "
				+ "Time taken to do backup :"
				+ PerfTime.readableTime(e_backup - s_backup));

	}

	public void compressTable(Connection conn, String tabname,
			String thread_name)
	// preiodically compresses the table to get back the free spaces available
	// after
	// the deletion of some rows
	{
		long s_compress = System.currentTimeMillis();
		long dbsize = databaseSize("mailsdb/seg0");
		MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
				+ "dbsize before compress : " + dbsize);
		try {
			boolean saveAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(true);
			CallableStatement cs = conn
					.prepareCall("CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, ?, ?, ?)");
			cs.setString(1, "REFRESH");
			cs.setString(2, tabname);
			cs.setShort(3, (short) 1);
			cs.setShort(4, (short) 1);
			cs.setShort(5, (short) 1);
			cs.execute();
			conn.setAutoCommit(saveAutoCommit);
			cs.close();
		} catch (Throwable sqe) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
					+ "Error while doing the Compress procedure: "
					+ sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);
		}
		long e_compress = System.currentTimeMillis();
		MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
				+ "Finished Compressing the table: " + tabname);
		log.logMsg(LogFile.INFO + thread_name + " : "
				+ "Time taken to compress the table : " + tabname
				+ PerfTime.readableTime(e_compress - s_compress));
		dbsize = databaseSize("mailsdb/seg0");
		MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
				+ "dbsize after compress : " + dbsize);
	}

	public synchronized void checkDbSize(Connection conn, String thread_name)
	// Will give the information about the size of the database in regular
	// intervals
	{
		try {
			int del_count = 0;
			int count = 0;
			int diff = 0;
			ArrayList idArray = new ArrayList();
			Integer id_element = new Integer(0);
			Statement stmt = conn.createStatement();
			Statement stmt1 = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			ResultSet rs = stmt
					.executeQuery("select count(*) from REFRESH.INBOX ");
			while (rs.next())
				count = rs.getInt(1);
			if (count > 12) {
				diff = count - 12;
				ResultSet rs1 = stmt1
						.executeQuery("select id from REFRESH.INBOX");
				while (rs1.next()) {
					id_element = new Integer(rs1.getInt(1));
					idArray.add(id_element);
				}
				for (int i = 0; i <= diff; i++) {
					del_count = del_count
							+ stmt3
									.executeUpdate("delete from REFRESH.INBOX where id ="
											+ idArray.get(i));

				}
				if (rs1 != null)
					rs1.close();
			}
			delete_count = delete_count + del_count;
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if (stmt1 != null)
				stmt1.close();
			if (stmt2 != null)
				stmt2.close();
			if (stmt3 != null)
				stmt3.close();
		} catch (Exception fe) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " :  "
					+ fe.getMessage());
			errorPrint(fe);
		}

	}

	public void grantRevoke(Connection conn, String thread_name) {
		try {
			// Giving appropriate permission to eahc threads
			saveAutoCommit = conn.getAutoCommit();
			Statement stmt = conn.createStatement();
			stmt.execute(Statements.grantSel1);
			stmt.execute(Statements.grantSel2);
			stmt.execute(Statements.grantSel3);
			stmt.execute(Statements.grantSel4);
			stmt.execute(Statements.grantSel5);
			stmt.execute(Statements.grantSel6);
			stmt.execute(Statements.grantSel7);
			stmt.execute(Statements.grantIns1);
			stmt.execute(Statements.grantIns2);
			stmt.execute(Statements.grantIns3);
			stmt.execute(Statements.grantUp1);
			stmt.execute(Statements.grantUp2);
			stmt.execute(Statements.grantUp3);
			stmt.execute(Statements.grantDel1);
			stmt.execute(Statements.grantDel2);
			stmt.execute(Statements.grantDel3);
			stmt.execute(Statements.grantExe1);
			stmt.execute(Statements.grantExe2);
			stmt.execute(Statements.grantExe3);
			stmt.execute(Statements.grantExe4);
			stmt.execute(Statements.grantExe5);
			conn.commit();
			conn.setAutoCommit(saveAutoCommit);
			if (stmt != null)
				stmt.close();
			MailJdbc.logAct.logMsg(LogFile.INFO + thread_name + " : "
					+ "Finished Granting permissions");
		} catch (Throwable sqe) {
			MailJdbc.logAct.logMsg(LogFile.ERROR + thread_name + " : "
					+ "Error while doing Grant Revoke: " + sqe.getMessage());
			sqe.printStackTrace();
			errorPrint(sqe);

		}

	}

	public static long databaseSize(String dbname) {
		File dir = new File(dbname);
		File[] files = dir.listFiles();
		long length = 0;
		int count = 0;
		for (int i = 0; i < files.length; i++) {
			length = length + files[i].length();
			count++;
		}
		return length;
	}

	public static void setSystemProperty(String key, String value) {
		String svalue = System.getProperty(key);
		if (svalue == null)
			System.setProperty(key, value);
		else
			value = svalue;
		MailJdbc.logAct.logMsg(LogFile.INFO + key + "=" + value);
	}

	public void totals() {
		MailJdbc.logAct.logMsg(LogFile.INFO + " total number of inserts : "
				+ insert_count);
		MailJdbc.logAct.logMsg(LogFile.INFO + " total number of deletes : "
				+ delete_count);
		MailJdbc.logAct.logMsg(LogFile.INFO
				+ " total number of clobs inserted : " + insert_count);
		MailJdbc.logAct.logMsg(LogFile.INFO
				+ " total number of blobs inserted : " + blob_count);
	}

	public static String decrement(String name, int maxLength) {
		StringBuffer buff = new StringBuffer(name);

		// if the String is '0', return the maximum String
		StringBuffer tempBuff = new StringBuffer();
		if (name.length() == 1 && name.charAt(0) == firstChar()) {
			for (int i = 0; i < maxLength; i++) {
				tempBuff.append(lastChar());
			}
			return tempBuff.toString();
		}

		// if String is all '000...0', eliminate one '0' and set the rest to 'z'
		else {
			boolean isAll0 = true;
			for (int i = 0; i < buff.length(); i++) {
				if (buff.charAt(i) != firstChar()) {
					isAll0 = false;
					break;
				}
			}
			if (isAll0 == true) {
				buff.deleteCharAt(0);
				for (int i = 0; i < buff.length(); i++) {
					buff.setCharAt(i, lastChar());
				}
			}
			// if the String is not all '000...0', loop starting with the last
			// char
			else {
				for (int i = buff.length() - 1; i >= 0; i--) {
					// if this char is not '0'
					if (buff.charAt(i) > firstChar()) {
						// decrement this char
						buff.setCharAt(i, previousChar(buff.charAt(i)));
						break;
					}
					// Resetting the counter 000 -> zzz
					// if this char is '0' and if the char before is not '0',
					// set this char to 'z' and decrement the char before
					else
						buff.setCharAt(i, lastChar());
					if (buff.charAt(i - 1) < firstChar()) {
						buff.setCharAt(i - 1, previousChar(buff.charAt(i - 1)));
						break;
					}
				}
			}
		}
		return buff.toString();
	}

	private static char firstChar() {
		return (char) 48;
	}

	private static char lastChar() {
		return (char) 122;
	}

	private static char previousChar(char c) {
		if (c <= 65 && c >= 59)
			return (char) 57;
		else if (c <= 97 && c >= 92)
			return (char) 90;
		else
			return (char) (c - 1);
	}

	public static String increment(String name, int maxLength) {

		// if (name.length() > maxLength) {
		// //String greater than maxLength, so set it to '0'
		// return "0";
		// }
		StringBuffer buff = new StringBuffer(name);
		// check if the String is all 'zzz...z'
		boolean isAllZ = true;
		for (int i = 0; i < name.length(); i++) {
			if (name.charAt(i) != lastChar()) {
				isAllZ = false;
				break;
			}
		}
		// if the String is all 'zzz...z', check if it's the maximum length
		if (isAllZ == true) {
			if (name.length() >= maxLength) {
				// String is all 'zzz...z' to maxLength, so set it to '0'
				return "0";
			} else {
				// String is all 'zzz...z' but not maxLength, so set all to 0
				// and append '0'
				for (int i = 0; i < buff.length(); i++) {
					buff.setCharAt(i, firstChar());
				}
				buff.append('0');
			}
		}
		// if the String is not all 'zzz...z', loop starting with the last char
		else {
			for (int i = buff.length() - 1; i >= 0; i--) {
				// if this char is not 'z'
				if (buff.charAt(i) < lastChar()) {
					// increment this char
					buff.setCharAt(i, nextChar(buff.charAt(i)));
					break;
				}
				// if this char is 'z' and if the char before is not 'z', set
				// this char to '0' and increment the char before
				else
					buff.setCharAt(i, firstChar());
				if (buff.charAt(i - 1) < lastChar()) {
					buff.setCharAt(i - 1, nextChar(buff.charAt(i - 1)));
					break;
				}
			}
		}
		return buff.toString();
	}

	private static char nextChar(char c) {
		if (c <= 63 && c >= 57)
			return (char) 65;
		else if (c <= 95 && c >= 90)
			return (char) 97;
		else
			return (char) (c + 1);
	}

	static void errorPrint(Throwable e) {
		if (e instanceof SQLException)
			SQLExceptionPrint((SQLException) e);
		else {
			System.out.println("A non SQL error occured.");
			e.printStackTrace();
		}
	} // END errorPrint

	// Iterates through a stack of SQLExceptions
	static void SQLExceptionPrint(SQLException sqle) {
		while (sqle != null) {
			System.out.println("\n---SQLException Caught---\n");
			System.out.println("SQLState:   " + (sqle).getSQLState());
			System.out.println("Severity: " + (sqle).getErrorCode());
			System.out.println("Message:  " + (sqle).getMessage());
			sqle.printStackTrace();
			sqle = sqle.getNextException();
		}
	}
}
