/*
 *
 * Derby - Class org.apache.derbyTesting.system.sttest.Sttest
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

package org.apache.derbyTesting.system.sttest;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Random;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.system.sttest.tools.MemCheck;
import org.apache.derbyTesting.system.sttest.utils.Datatypes;
import org.apache.derbyTesting.system.sttest.utils.Setup;
import org.apache.derbyTesting.system.sttest.utils.StStatus;

/*
 * * Sttest.java * 'Sttest' is short for 'single table test.' Sttest.java
 * supplies * the main entry point and the top level code for controlling the *
 * actions of the test, including the ddl for the table and indexes. * The
 * purpose of the test is to exercise the store code by running * updates on the
 * single table for an indefinitely long time, with * an indefinitely large
 * number of user connections, each randomly * executing one of a small number
 * of update procedures with random * data. The test sets a minimum and maximum
 * number of rows, builds * the table up to the minimum number of rows, and from
 * that point * either gradually grows the table to the max size, or gradually *
 * shrinks it to the min size. Periodically memory use is reported, * and the
 * table is compressed, to keep performance from deteriorating.
 */
public class Sttest extends Thread {

	static int loops = 200;

	static int rowcount = 0;

	static int testcount = 0;

	static int connections_to_make = 250;

	static Random rand;

	static boolean increase = true;

	static boolean not_finished = true;

	static int targetmax = 100000; // build up to 1GB database

	static int targetmin = 90000;

	static int insertsize = 7;

	static int updatesize = 1;

	static int deletesize = 1;

	static boolean fatal = false;

	static int rows = 0;

	static boolean countlock = false;

	static int delete_freq = 1;

	static int locker_id = -1;

	static final int INIT = 0;

	static final int GROW = 1;

	static final int SHRINK = 2;

	static int mode = INIT;

	static int count_timer = 0;

	static int inserts_to_try = 0;

	static final int INITIAL_CONNECTIONS = 2; // initial connections should be

	// low, otherwise deadlock will
	// happen.

	static boolean startByIJ = false;

	static String dbURL = "jdbc:derby:testDB";

	static String driver = "org.apache.derby.jdbc.EmbeddedDriver";

	static StStatus status = null;

	int thread_id;

	int ind = 0;

	public static void main(String[] args) throws SQLException, IOException,
	InterruptedException, Exception, Throwable {
		System.getProperties().put("derby.locks.deadlockTimeout", "60");
		System.getProperties().put("derby.locks.waitTimeout", "200");
		System.out.println("Test Sttest starting");
		System.getProperties().put("derby.infolog.append", "true");
		System.getProperties().put("derby.stream.error.logSeverityLevel", "0");
		// get any properties user may have set in Sttest.properties file
		// these will override any of those set above
		userProperties();
		Class.forName(driver).newInstance();
		if (Setup.doit(dbURL) == false)
			System.exit(1);
		status = new StStatus();
		sttTop();
	}

	static void userProperties() throws Throwable {
		FileInputStream fileIn = null;
		try {
			fileIn = new FileInputStream("Sttest.properties");
		} catch (Exception e) {
			System.out
			.println("user control file 'Sttest.properties' not found; using defaults");
		}
		if (fileIn != null) {
			Properties props = new Properties();
			props.load(fileIn);
			fileIn.close();
			String prop = null;
			prop = props.getProperty("connections");
			if (prop != null)
				connections_to_make = Integer.parseInt(prop);
			prop = props.getProperty("dbURL");
			if (prop != null)
				dbURL = prop;
			prop = props.getProperty("driver");
			if (prop != null)
				driver = prop;
			// allows us to get any other properties into the system
			Properties sysprops = System.getProperties();
			Enumeration list = props.propertyNames();
			String s = null;
			while (list.hasMoreElements()) {
				s = (String) list.nextElement();
				sysprops.put(s, props.getProperty(s));
			}
		}
		System.out.println("driver = " + driver);
		System.out.println("dbURL = " + dbURL);
		System.out.println("connections = " + connections_to_make);
	}

	public static void sttTop() throws SQLException, IOException,
	InterruptedException, Exception, Throwable {
		rand = new Random();

		Datatypes.Rn = rand;
		// harder to actually delete rows when there are
		// more connections, so repeat operation more often
		delete_freq = 1 + connections_to_make % 5;
		initial_data();
		Date d = new Date();
		status.firstMessage(connections_to_make, d);
		// check memory in separate thread-- allows us to monitor
		// usage during database calls
		// 200,000 msec = 3min, 20 sec delay between checks
		MemCheck mc = new MemCheck(200000);
		mc.start();
		Sttest testsessions[] = new Sttest[connections_to_make];
		for (int i = 0; i < connections_to_make; i++) {
			testsessions[i] = new Sttest(i);
			testsessions[i].start();
			sleep(3000);
		}
		for (int i = 0; i < connections_to_make; i++) {
			testsessions[i].join();
		}
		try {
			mc.stopNow = true;
			mc.join();
		} catch (Throwable t) {
			throw (t);
		}
	}

	Sttest(int num) throws SQLException {
		this.thread_id = num;
	}

	static synchronized void reset_loops(int myloopcount) {
		if (myloopcount == loops)
			loops--;
		// otherwise some other thread got there first and reset it
	}

	// available to force synchronization of get_countlock(), ...
	static synchronized void locksync() {
		return;
	}

	static synchronized boolean get_countlock() {
		locksync();
		return (countlock);
	}

	static synchronized boolean set_countlock(boolean state) {
		locksync();
		if (state == true && countlock == true)
			return (false);
		countlock = state;
		return (true);
	}

	static synchronized void changerowcount(int in) {
		rowcount += in;
	}

	static synchronized void changerowcount2zero() {
		rowcount = 0;
	}

	static void initial_data() throws Exception, Throwable {
		Connection conn = null;
		int rows = 0;
		try {
			conn = mystartJBMS();
		} catch (Throwable t) {
			throw (t);
		}
		// our goal is to get up to minimum table size
		int x = Datatypes.get_table_count(conn);
		if (x != -1) {
			rows = x;
		}
		if (conn != null) {
			conn.commit();
			conn.close();
		}
		rowcount = rows;
		if (rows >= targetmin) {
			mode = GROW;
			System.out.println("initial data not needed");
			return;
		}
		inserts_to_try = targetmin - rows;
		Sttest testthreads[] = new Sttest[INITIAL_CONNECTIONS];
		for (int i = 0; i < INITIAL_CONNECTIONS; i++) {
			testthreads[i] = new Sttest(i);
			testthreads[i].start();
		}
		for (int i = 0; i < INITIAL_CONNECTIONS; i++) {
			testthreads[i].join();
		}
		mode = GROW;
		System.out.println("complete initial data");
		return;
	}

	public void run() {
		Connection conn = null;
		Date d = null;
		try {
			conn = mystartJBMS();
		} catch (Throwable t) {
			return;
		}
		int ind2 = 0;
		int myloops = loops;
		while (not_finished) {
			if (loops <= 0)
				break;// done
			// thread-private copy to be checked against global copy
			// before attempting to update
			myloops = loops;
			if (fatal == true)
				break;
			if (mode == INIT && inserts_to_try <= 0) {
				break;
			}
			// test rowcount
			if (mode == GROW && rowcount > targetmax) {
				System.out.println("hit targetmax with " + rowcount + " " + d);
				d = new Date();
				mode = SHRINK;
				reset_loops(myloops);
				insertsize = 1;
				deletesize = 12;
				if (set_countlock(true) == true) {
					try {
						checkrowcount(conn);
						MemCheck.showmem();
						status.updateStatus();
					} catch (Exception e) {
						System.out.println("unexpected exception in rowcount");
						set_countlock(false);
						System.exit(1);
					}
					MemCheck.showmem();
				}
				set_countlock(false);
				yield();
			} else if (mode == GROW && rowcount >= targetmax) {
				d = new Date();
				System.out.println("hit targetmax with " + rowcount + " " + d);
				mode = SHRINK;
				insertsize = 1;
				deletesize = 12;
			} else if (mode == SHRINK && rowcount <= targetmin) {
				d = new Date();
				System.out.println("hit targetmin with " + rowcount + " " + d);
				mode = GROW;
				reset_loops(myloops);
				insertsize = 8;
				deletesize = 1;
				if (set_countlock(true) == true) {
					try {
						checkrowcount(conn);
						MemCheck.showmem();
						status.updateStatus();
					} catch (Exception e) {
						System.out.println("unexpected exception in rowcount");
						set_countlock(false);
						System.exit(1);
					}
					MemCheck.showmem();
					// compress 
					try {
						compress(conn);
					} catch  (Exception e) {
						System.out.println("unexpected exception during compress");
					}
				}
				set_countlock(false);
				yield();
			}
			// don't interfere with count query
			while (get_countlock() == true) {
				try {
					sleep(1000);
				} catch (java.lang.InterruptedException ex) {
					System.out.println("unexpected sleep interruption");
					break;
				}
			}
			try {
				if (mode == INIT)
					ind = 0;
				else
					ind = Math.abs(rand.nextInt() % 3);
				switch (ind) {
				case 0:
					ind2 = Math.abs(rand.nextInt() % insertsize);
					int addrows = 0;
					for (int i = 0; i <= ind2; i++) {
						Datatypes.add_one_row(conn, thread_id);
						addrows++;
						conn.commit();
						if (mode == INIT) {
							inserts_to_try--;
						}
						yield();
						changerowcount(1);
					}
					System.out.println(addrows + "  Rows inserted");
					break;
				case 1:
					ind2 = Math.abs(rand.nextInt() % updatesize);
					int updaterow = 0;
					for (int i = 0; i <= ind2; i++) {
						Datatypes.update_one_row(conn, thread_id);
						updaterow++;
						conn.commit();
						yield();
					}
					System.out.println(updaterow + "  rows updated");
					break;
				case 2:
					ind2 = Math.abs(rand.nextInt() % deletesize);
					int del_rows = 0;
					del_rows = Datatypes.delete_some(conn, thread_id, ind2 + 1);
					yield();
					changerowcount(-1 * del_rows);
					// commits are done inside delete_some()
					System.out.println(del_rows + " rows deleted");
					break;
				} // end switch

			} catch (SQLException se) {
				if (se.getSQLState() == null)
					se.printStackTrace();
				if (se.getSQLState().equals("40001")) {
					System.out.println("t" + thread_id + " deadlock op = "
							+ ind);
					continue;
				}
				if (se.getSQLState().equals("40XL1")) {
					System.out
					.println("t" + thread_id + " timeout op = " + ind);
					continue;
				}
				if (se.getSQLState().equals("23500")) {
					System.out.println("t" + thread_id
							+ " duplicate key violation\n");
					continue;
				}
				if (se.getSQLState().equals("23505")) {
					System.out.println("t" + thread_id
							+ " duplicate key violation\n");
					continue;
				}
				System.out.println("t" + thread_id
						+ " FAIL -- unexpected exception:");
				JDBCDisplayUtil.ShowException(System.out, se);
				fatal = true;
				break;
			} catch (Throwable e) {
				e.printStackTrace();
				if (e.getMessage().equals("java.lang.ThreadDeath")) {
					System.out.println("caught threaddeath and continuing\n");
				} else {
					fatal = true;
					e.printStackTrace();
				}
			}
		}// end while
		try {
			conn.close();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.out.println("Thread finished: " + thread_id);
	}

	static synchronized void checkrowcount(Connection conn)
	throws java.lang.Exception {
		int x = Datatypes.get_table_count(conn);
		if (x == -1) { // count timed itself out
			System.out.println("table count timed out");
		} else {
			System.out.println("rowcount by select: " + x
					+ " client rowcount = " + rowcount);
			changerowcount(x - rowcount);
		}
		conn.commit();
	}

	static public Connection mystartJBMS() throws Throwable {
		Connection conn = null;
		if (startByIJ == true) {
			conn = ij.startJBMS();
		} else
			try {
				conn = DriverManager.getConnection(dbURL + ";create=false");
				conn.setAutoCommit(false);
				conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
			} catch (SQLException se) {
				System.out.println("connect failed  for " + dbURL);
				JDBCDisplayUtil.ShowException(System.out, se);
			}
			return (conn);
	}

	static synchronized void compress(Connection conn)
	throws java.lang.Exception {
		System.out.println("compressing table");
		boolean autocom = conn.getAutoCommit();
		try {
			conn.setAutoCommit(true);
			CallableStatement cs = conn.prepareCall(
				"CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, ?, ?, ?)");
			cs.setString(1, "APP");
			cs.setString(2, "DATATYPES");
			cs.setShort(3, (short) 1);
			cs.setShort(4, (short) 1);
			cs.setShort(5, (short) 1);
			cs.execute();
			cs.close();
		} catch (SQLException se) {
			System.out.println("compress table: FAIL -- unexpected exception:");
			JDBCDisplayUtil.ShowException(System.out, se);
			se.printStackTrace();
		}
		conn.setAutoCommit(autocom);
	}
}
