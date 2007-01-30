/*
 
 Derby - Class org.apache.derbyTesting.system.sttest.Datatypes
 
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

package org.apache.derbyTesting.system.sttest.utils;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;
import java.util.Vector;
import org.apache.derby.tools.JDBCDisplayUtil;


/**
 * This class is used to insert, delete and updated the rows
 */
public class Datatypes {
	
	public static final int TCHAR = 0;
	
	public static final int TBLOB = 1;
	
	public static final int TCLOB = 2;
	
	public static final int TDATE = 3;
	
	public static final int TDECIMAL = 4;
	
	public static final int TDECIMALNN = 5;
	
	public static final int TDOUBLE = 6;
	
	public static final int TFLOAT = 7;
	
	public static final int TINT = 8;
	
	public static final int TLONGINT = 9;
	
	public static final int TNUMERICLARGE = 10;
	
	public static final int TREAL = 11;
	
	public static final int TSMALLINT = 12;
	
	public static final int TTIME = 13;
	
	public static final int TTIMESTAMP = 14;
	
	public static final int TVARCHAR = 15;
	
	public static final int NUMTYPES = 15;
	
	static String[] fileName = new String[35];
	
	static String[] fileName1 = new String[35];
	
	static String[] fileName2 = new String[35];
	
	static long[] fileLength = { 16644, 65949, 65675, 65675, 9894, 26651,
			37370, 9676, 32191, 26883, 52817, 34590, 59510, 52063, 19723, 9894,
			10937, 12522, 10962, 29815, 13605, 1620, 13800, 14606, 10126,
			14430, 21757, 7765, 11431, 14047, 10992, 20141, 11357, 8609, 23259 };
	
	static long[] fileLength1 = { 6285, 6944, 58937, 28225, 89773, 31076,
			11577, 43729, 29501, 14044, 3258, 10590, 0, 2588, 3270, 3433, 4149,
			2778, 1529, 6672, 1918, 1308, 1899, 6143, 6554, 15456, 16617, 571,
			13657, 5000, 10000, 401, 401, 401, 401 };
	
	public static String[] colnames = { "t_char", "t_blob", "t_clob", "t_date",
			"t_decimal", "t_decimal_nn", "t_double", "t_float", "t_int",
			"t_longint", "t_numeric_large", "t_real", "t_smallint", "t_time",
			"t_timestamp", "t_varchar"
			
	};
	
	public static Random rand;
	
	public static synchronized void add_one_row(Connection conn, int thread_id)
	throws Exception {
		try {
			//initialize();
			PreparedStatement ps = conn.prepareStatement(
					" insert into Datatypes (id,t_char,t_blob," + "t_clob,"
					+ " t_date, t_decimal, t_decimal_nn, t_double, "
					+ " t_float, t_int, t_longint, t_numeric_large,"
					+ " t_real, t_smallint, t_time, t_timestamp,"
					+ " t_varchar) values ("
					+ " ?,?, ?,?, ?, ?,?, ?, ?, ?,?, ?, ?, ?, ?, ?,?)" //autoincrement
					// feature
					// added,
					// so
					// we
					// need
					// to
					// specify
					// the
					// column
					// name
					// for prepared statement, otherwise auto increment
					// column will think
					// it is trying to update/insert a null value to the column.
					, Statement.RETURN_GENERATED_KEYS);
			int ind = rand.nextInt();
			int id_ind = ind;
			double x;
			Date dt = new Date(1);
			Time tt = new Time(1);
			Timestamp ts = new Timestamp(1);
			String cs = "asdf qwerqwer 12341234 ZXCVZXCVZXCV !@#$!@#$ asdfasdf 1 q a z asdf ASDF qwerasdfzxcvasdfqwer1234asd#";
			boolean bo = false;
			Vector vec = null;
			Integer ji = null;
			ps.setInt(1, ind);
			// scramble the string
			int i1 = Math.abs(ind % 100);
			String cs2 = cs.substring(i1, 99) + cs.substring(0, i1);
			int i2 = i1 < 89 ? i1 + 10 : i1;
			ps.setString(2, cs2.substring(0, i2));
			//"t_blob"
			int j = (int) (Math.random() * 35);
			fileName[j] = "p" + j + ".jpg";
			InputStream fileIn = Datatypes.class
			.getResourceAsStream(fileName[j]);
			ps.setBinaryStream(3, fileIn, (int) fileLength[j]);
			//"t_clob
			int k = (int) (Math.random() * 35);
			fileName1[k] = "t" + k + ".txt";
			InputStream fileIn1 = Datatypes.class
			.getResourceAsStream(fileName1[k]);
			ps.setAsciiStream(4, fileIn1, (int) fileLength1[k]);
			//"t_ndate"
			dt.setTime(Math.abs(rand.nextLong() / 150000));
			ps.setDate(5, dt);
			//"t_decimal"
			x = Math.abs(rand.nextInt() % 18);
			if (x > 5)
				x = 5;
			ps.setDouble(6, Math.abs(rand.nextDouble() * Math.pow(10, x)));
			//"t_decimal_nn"
			ps.setDouble(7, rand.nextDouble());
			//"t_double"
			ps.setDouble(8, rand.nextDouble()
					* Math.pow(10, Math.abs(rand.nextInt() % 300)));
			//"t_float"
			ps.setFloat(9, rand.nextFloat()
					* (float) Math.pow(10, Math.abs(rand.nextInt() % 30)));
			//"t_int"
			ps.setInt(10, rand.nextInt());
			//"t_longint"
			ps.setLong(11, rand.nextLong());
			//"t_numeric_large"
			x = Math.abs(rand.nextInt() % 30);
			if (x > 30)
				x = 31;
			ps.setDouble(12, Math.abs(rand.nextDouble() * Math.pow(10, x)));
			//"t_real"
			ps.setFloat(13, rand.nextFloat()
					* (float) Math.pow(10, Math.abs(rand.nextInt() % 7)));
			//"t_smallint"
			ps.setInt(14, rand.nextInt() % (256 * 128));
			//"t_time"
			tt.setTime(Math.abs(rand.nextInt()));
			ps.setTime(15, tt);
			//"t_timestamp"
			ts.setTime(Math.abs(rand.nextLong() / 50000));
			ps.setTimestamp(16, ts);
			//"t_varchar"
			ps.setString(17, cs.substring(Math.abs(rand.nextInt() % 100)));
			int rows = ps.executeUpdate();
			if (rows == 1) {
				
				System.out.println("t" + thread_id + " inserted row with id "
						+ id_ind);
				ResultSet rs = ps.getGeneratedKeys();
				
				while (rs.next()) {
					ResultSetMetaData rsmd = rs.getMetaData();
					int numCols = rsmd.getColumnCount();
				}
			} else
				System.out.println("t" + thread_id + " insert failed");
			
		} catch (SQLException se) {
			if (se.getNextException() == null)
				throw se;
			String m = se.getNextException().getSQLState();
			System.out.println(se.getNextException().getMessage()
					+ " SQLSTATE: " + m);
		}
	}
	
	//pick quantity number of rows randomly and delete them
	public static int delete_some(Connection conn, int thread_id, int quantity)
	throws Exception {
		PreparedStatement ps = null;
		int list[] = pick_some(conn, thread_id, quantity);
		//delete them
		int rows = 0;
		try {
			ps = conn.prepareStatement(" delete from  Datatypes where id = ?");
			for (int i = 0; i < quantity; i++) {
				ps.setInt(1, list[i]);
				rows += ps.executeUpdate();
				System.out.println("t" + thread_id + " deleted "
						+ " row with id " + list[i]);
			}
			if (ps != null)
				ps.close();
		} catch (SQLException se) {
			if (se.getNextException() == null)
				throw se;
			String m = se.getNextException().getSQLState();
			System.out.println(se.getNextException().getMessage()
					+ " SQLSTATE: " + m);
			return (rows);
		}
		// all deletes in a single transaction, to force some overlap of
		// transactions
		// by different threads
		conn.commit();
		return (rows);
	}
	
	//get a random set of row ids
	public static int[] pick_some(Connection conn, int thread_id, int quantity)
	throws Exception {
		System.out.println("quantity in pick_some is: " + quantity);
		int ind = 0;
		PreparedStatement ps = null;
		ResultSet rs = null;
		//pick the rows
		try {
			ps = conn
			.prepareStatement(" select id from  Datatypes where id = ?");
		} catch (SQLException se) {
			if (se.getNextException() == null)
				throw se;
			String m = se.getNextException().getSQLState();
			System.out.println(se.getNextException().getMessage()
					+ " SQLSTATE: " + m);
			return (null);
		}
		int list[] = new int[quantity];
		int j = 0;
		for (int i = 0; i < quantity; i++) {
			ind = rand.nextInt();
			try {
				ps.setInt(1, ind);
				rs = ps.executeQuery();
				if (rs.next()) {
					//keep trying till we get a good one
					j = rs.getInt(1);
					if (rs.wasNull()) {
						System.out.println("rs.wasNull() ind is " + ind);
						i--;
						continue;
					}
					list[i] = j;
					
					System.out.println("list[1] in picksome=:" + list[i]);
				} else {
					System.out.println("thread " + thread_id
							+ ", select for delete: no row found");
				}
				//don't worry about consistency; if row with this id
				//gets changed by another thread we will just forge ahead;
				//otherwise we gets lots of deadlocks
				conn.commit();
			} catch (SQLException se) {
				if (se.getNextException() == null)
					throw se;
				String m = se.getNextException().getSQLState();
				System.out.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
				return (null);
			}
		}
		if (rs != null)
			rs.close();
		if (ps != null)
			ps.close();
		return (list);
	}
	
	public static void delete_one_row(Connection conn, int thread_id)
	throws Exception {
		
		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		String column = null;
		int ind = 0;
		ResultSet rs = null;
		ind = Math.abs(rand.nextInt());
		while (ind % NUMTYPES == TDECIMAL || ind % NUMTYPES == TVARCHAR
				|| ind % NUMTYPES == TCHAR)
			ind = Math.abs(rand.nextInt());
		column = colnames[ind % NUMTYPES];
		try {
			ps = conn.prepareStatement(" select cast (max (" + column + ") as "
					+ " char(120)) from Datatypes where " + column + " <= ? ");
			ps2 = conn.prepareStatement(" delete from  Datatypes where "
					+ column + " = ?");
		} catch (SQLException se) {
			if (se.getNextException() == null)
				throw se;
			String m = se.getNextException().getSQLState();
			System.out.println(se.getNextException().getMessage()
					+ " SQLSTATE: " + m);
			return;
		}
		String ds = null;
		String cs = "asdf qwerqwer 12341234 ZXCVZXCVZXCV !@#$!@#$ asdfasdf 1 q a z asdf ASDF qwerasdfzxcvasdfqwer1234asd#";
		double d = 0.0;
		float f = 0;
		BigDecimal bdec = null;
		switch (ind % NUMTYPES) {
		case TCHAR:
			ds = cs.substring(Math.abs(rand.nextInt() % 100));
			ps.setString(1, ds);
			break;
		case TDATE:
			Date dt = new Date(1);
			dt.setTime(Math.abs(rand.nextLong() / 150000));
			ps.setString(1, dt.toString());
			ds = dt.toString();
			break;
		case TDECIMAL:
			d = rand.nextDouble() * Math.pow(10, rand.nextInt() % 18);
			bdec = new BigDecimal(d);
			ps.setString(1, String.valueOf(bdec));
			ds = String.valueOf(d);
			break;
		case TDECIMALNN:
			d = rand.nextDouble();
			bdec = new BigDecimal(d);
			ps.setString(1, String.valueOf(bdec));
			ds = String.valueOf(d);
			break;
		case TDOUBLE:
			d = rand.nextDouble() * Math.pow(10, rand.nextInt() % 300);
			ps.setString(1, String.valueOf(d));
			ds = String.valueOf(d);
			break;
		case TFLOAT:
			f = rand.nextFloat() * (float) Math.pow(10, rand.nextInt() % 30);
			ps.setString(1, String.valueOf(f));
			ds = String.valueOf(f);
			break;
		case TINT:
			ps.setString(1, String.valueOf(rand.nextInt()));
			ds = String.valueOf(rand.nextInt());
			break;
		case TLONGINT:
			ps.setString(1, String.valueOf(rand.nextLong()));
			ds = String.valueOf(rand.nextLong());
			break;
		case TNUMERICLARGE:
			d = rand.nextDouble() * Math.pow(10, rand.nextInt() % 50);
			bdec = new BigDecimal(d);
			ps.setString(1, String.valueOf(bdec));
			ds = String.valueOf(d);
			break;
		case TREAL:
			f = rand.nextFloat() * (float) Math.pow(10, rand.nextInt() % 7);
			ps.setString(1, String.valueOf(f));
			ds = String.valueOf(f);
			break;
		case TSMALLINT:
			int i = rand.nextInt() % (256 * 128);
			ps.setString(1, String.valueOf(i));
			ds = String.valueOf(i);
			break;
		case TTIME:
			Time tt = new Time(1);
			tt.setTime(Math.abs(rand.nextInt()));
			ps.setString(1, "time'" + tt.toString() + "'");
			ds = "time'" + tt.toString() + "'";
			break;
		case TTIMESTAMP:
			Timestamp ts = new Timestamp(1);
			ts.setTime(Math.abs(rand.nextLong() / 50000));
			ps.setString(1, "timestamp'" + ts.toString() + "'");
			ds = "timestamp'" + ts.toString() + "'";
			break;
		case TVARCHAR:
			ds = cs.substring(Math.abs(rand.nextInt() % 100));
			ps.setString(1, ds);
			break;
		}
		System.out.println("t" + thread_id + " delete " + column
				+ " select seed " + ds);
		String ds3 = null;
		String ds4 = null;
		int rows = 0;
		boolean cleanuponly = false;
		try {
			rs = ps.executeQuery();
			if (rs.next()) {
				ds3 = rs.getString(1);
				if (rs.wasNull()) {
					cleanuponly = true;
				} else {
					ds4 = ds3.trim();
					ds3 = ds4;
				}
			}
		} catch (SQLException se) {
			if (se.getNextException() == null)
				throw se;
			String m = se.getNextException().getSQLState();
			System.out.println(se.getNextException().getMessage()
					+ " SQLSTATE: " + m);
		}
		if (ps != null)
			try {
				ps.close();
			} catch (SQLException se) {
				if (se.getNextException() == null)
					throw se;
				String m = se.getNextException().getSQLState();
				System.out.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
			}
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException se) {
					if (se.getNextException() == null)
						throw se;
					String m = se.getNextException().getSQLState();
					System.out.println(se.getNextException().getMessage()
							+ " SQLSTATE: " + m);
				}
				if (cleanuponly == false) {
					System.out.println("t" + thread_id + " delete where " + column
							+ " = " + ds3);
					try {
						ps2.setString(1, ds3);
						rows = ps2.executeUpdate();
						
					} catch (SQLException se) {
						if (se.getNextException() == null)
							throw se;
						String m = se.getNextException().getSQLState();
						System.out.println(se.getNextException().getMessage()
								+ " SQLSTATE: " + m);
					}
					
					if (rows > 0)
						System.out.println("t" + thread_id + " deleted " + rows
								+ " row(s)");
					else
						System.out.println("t" + thread_id + " delete failed.");
				}
				if (ps2 != null)
					try {
						ps2.close();
					} catch (SQLException se) {
						if (se.getNextException() == null)
							throw se;
						String m = se.getNextException().getSQLState();
						System.out.println(se.getNextException().getMessage()
								+ " SQLSTATE: " + m);
					}
	}
	
	public static synchronized void update_one_row(Connection conn,
			int thread_id) throws Exception {
		PreparedStatement ps2 = null;
		Statement stmt = conn.createStatement();
		ResultSet rs;
		String column = null;
		int ind = 0;
		long max = 0;
		long min = 0;
		double x;
		long id_to_update;
		rs = stmt.executeQuery("select max(serialkey) from Datatypes");
		while (rs.next())
			max = rs.getLong(1);
		rs = stmt.executeQuery("select min(serialkey) from Datatypes");
		while (rs.next())
			min = rs.getLong(1);
		id_to_update = (min + 1) + (Math.abs(rand.nextLong()) % (max - min));
		if (id_to_update == 0)
			id_to_update = 1;
		ind = Math.abs(rand.nextInt());
		column = colnames[ind % NUMTYPES];
		try {
			conn
			.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			ps2 = conn.prepareStatement(" update Datatypes set " + column
					+ " = ? " + " where serialkey = " + id_to_update);
		} catch (SQLException se) {
			if (se.getNextException() == null)
				throw se;
			String m = se.getNextException().getSQLState();
			return;
		}
		String ds = null;
		String ds2 = null;
		String cs = "asdf qwerqwer 12341234 ZXCVZXCVZXCV !@#$!@#$ asdfasdf 1 q a z asdf ASDF qwerasdfzxcvasdfqwer1234asd#";
		double d = 0.0;
		float f = 0;
		BigDecimal bdec = null;
		int type = (ind % NUMTYPES);
		int ji = 0;
		boolean bo = false;
		switch (type) {
		case TCHAR:
			ds2 = cs.substring(Math.abs(rand.nextInt() % 100));
			ps2.setString(1, ds2);
			break;
		case TDATE:
			Date dt = new Date(1);
			dt.setTime(Math.abs(rand.nextLong() / 150000));
			dt.setTime(Math.abs(rand.nextLong() / 150000));
			ps2.setDate(1, dt);
			ds2 = dt.toString();
			break;
		case TDECIMAL:
			x = Math.abs(rand.nextInt() % 18);
			if (x > 5)
				x = 5;
			d = rand.nextDouble() * Math.pow(10, x);
			bdec = new BigDecimal(d);
			ps2.setBigDecimal(1, bdec);
			ds2 = String.valueOf(d);
			break;
		case TDECIMALNN:
			ds = String.valueOf(d);
			d = rand.nextDouble();
			bdec = new BigDecimal(d);
			ps2.setBigDecimal(1, bdec);
			ds2 = String.valueOf(d);
			break;
			
		case TDOUBLE:
			d = rand.nextDouble() * Math.pow(10, rand.nextInt() % 300);
			ps2.setDouble(1, d);
			ds2 = String.valueOf(d);
			break;
		case TFLOAT:
			ds = String.valueOf(f);
			f = rand.nextFloat() * (float) Math.pow(10, rand.nextInt() % 30);
			ps2.setFloat(1, f);
			ds2 = String.valueOf(f);
			break;
		case TINT:
			int i = rand.nextInt();
			ds2 = String.valueOf(i);
			ps2.setInt(1, i);
			break;
		case TLONGINT:
			long l = rand.nextLong();
			ds2 = String.valueOf(l);
			ps2.setLong(1, l);
			break;
		case TNUMERICLARGE:
			ds = String.valueOf(d);
			x = Math.abs(rand.nextInt() % 30);
			if (x > 30)
				x = 31;
			d = rand.nextDouble() * Math.pow(10, x);
			bdec = new BigDecimal(d);
			ps2.setBigDecimal(1, bdec);
			ds2 = String.valueOf(d);
			break;
		case TREAL:
			ds = String.valueOf(f);
			f = rand.nextFloat() * (float) Math.pow(10, rand.nextInt() % 7);
			ps2.setFloat(1, f);
			ds2 = String.valueOf(f);
			break;
		case TSMALLINT:
			i = rand.nextInt() % (256 * 128);
			ds = String.valueOf(i);
			short si = (short) i;
			ps2.setShort(1, si);
			ds2 = String.valueOf(si);
			break;
		case TTIME:
			Time tt = new Time(1);
			tt.setTime(Math.abs(rand.nextInt()));
			ps2.setTime(1, tt);
			ds2 = tt.toString();
			break;
		case TTIMESTAMP:
			Timestamp ts = new Timestamp(1);
			ts.setTime(Math.abs(rand.nextLong() / 50000));
			ps2.setTimestamp(1, ts);
			ds2 = ts.toString();
			break;
		case TVARCHAR:
			ds2 = cs.substring(Math.abs(rand.nextInt() % 100));
			ps2.setString(1, ds2);
			break;
		case TBLOB:
			int j = (int) (Math.random() * 35);
			fileName[j] = "p" + j + ".jpg";
			InputStream fileIn = Datatypes.class
			.getResourceAsStream(fileName[j]);
			ps2.setBinaryStream(1, fileIn, (int) fileLength[j]);
			break;
		case TCLOB:
			int k = (int) (Math.random() * 35);
			fileName1[k] = "t" + k + ".txt";
			InputStream fileIn1 = Datatypes.class
			.getResourceAsStream(fileName1[k]);
			ps2.setAsciiStream(1, fileIn1, (int) fileLength1[k]);
			break;
		}
		int rows = 0;
		boolean cleanuponly = false;
		if (cleanuponly == false) {
			System.out.println("t" + thread_id + " update " + column);
			try {
				rows = ps2.executeUpdate();
				
			} catch (SQLException se) {
				if (se.getNextException() == null)
					throw se;
				String m = se.getNextException().getSQLState();
				System.out.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
			}
			if (rows > 0)
				System.out.println("t" + thread_id + " updated 1 row with id "
						+ id_to_update);
			else
				System.out.println("t" + thread_id + " update failed.");
		}
		if (ps2 != null)
			try {
				ps2.close();
				rs.close();
			} catch (SQLException se) {
				if (se.getNextException() == null)
					throw se;
				String m = se.getNextException().getSQLState();
				System.out.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
			}
	}
	
	public static synchronized int get_table_count(Connection conn)
	throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;
		int rows = 0;
		boolean locked = false;
		int tick = 1;
		while (locked == false) {
			try {
				Statement s = conn.createStatement();
				s.execute("lock table Datatypes in exclusive mode");
				s.close();
				locked = true;
			} catch (SQLException se) {
				// not now lockable
				if (se.getSQLState().equals("X0X02")) {
					Thread.sleep(20000);
					if (tick++ < 60) {
						System.out
						.println("count: cannot lock table, retrying "
								+ tick + "\n");
						continue;
					} else {
						System.out.println("count timed out\n");
						return (-1);
					}
				} else
					JDBCDisplayUtil.ShowException(System.out, se);
			}
		}
		try {
			ps = conn.prepareStatement(" select count (*) from Datatypes ");
			rs = ps.executeQuery();
			if (rs.next())
				rows = rs.getInt(1);
			if (ps != null)
				ps.close();
		} catch (SQLException se) {
			if (se.getNextException() == null)
				throw se;
			String m = se.getNextException().getSQLState();
			System.out.println(se.getNextException().getMessage()
					+ " SQLSTATE: " + m);
		}
		System.out.println("rows in calling initial_data()" + rows);
		locked = true;
		return (rows);
	}
}