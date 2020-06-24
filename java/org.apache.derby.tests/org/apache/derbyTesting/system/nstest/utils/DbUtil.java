/*
 
 Derby - Class org.apache.derbyTesting.system.nstest.utils.DbUtil
 
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

package org.apache.derbyTesting.system.nstest.utils;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.derbyTesting.system.nstest.NsTest;

/**
 * DbUtil - a database utility class for all IUD and Select operations
 */
public class DbUtil {
	private String threadName = "";
	
	public static final int TCHAR = 0;
	
	public static final int TDATE = 1;
	
	public static final int TDECIMAL = 2;
	
	public static final int TDECIMALNN = 3;
	
	public static final int TDOUBLE = 4;
	
	public static final int TFLOAT = 5;
	
	public static final int TINT = 6;
	
	public static final int TLONGINT = 7;
	
	public static final int TNUMERICLARGE = 8;
	
	public static final int TREAL = 9;
	
	public static final int TSMALLINT = 10;
	
	public static final int TTIME = 11;
	
	public static final int TTIMESTAMP = 12;
	
	public static final int TVARCHAR = 13;
	
	public static final int NUMTYPES = 14;
	
	public static String[] colnames = { "t_char", "t_date", "t_decimal",
		"t_decimal_nn", "t_double", "t_float", "t_int", "t_longint",
		"t_numeric_large", "t_real", "t_smallint", "t_time", "t_timestamp",
	"t_varchar" };
	
	public DbUtil(String thName) {
		threadName = thName;
	}
	
	/*
	 * Add a row for each iteration
	 */
	
	public int add_one_row(Connection conn, String thread_id) throws Exception {
		
		PreparedStatement ps = null;
		int rowsAdded = 0;
		
		try {
			// autoincrement feature added, so we need to specify the column
			// name for prepared statement, otherwise auto increment column
			// will think it is trying to update/insert a null value to the
			// column.
			
			ps = conn
			.prepareStatement(" insert into nstesttab (id, t_char,"
					+ " t_date, t_decimal, t_decimal_nn, t_double, "
					+ " t_float, t_int, t_longint, t_numeric_large,"
					+ " t_real, t_smallint, t_time, t_timestamp,"
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
					+ " t_varchar,t_clob,t_blob,sequenceColumn) values ("
					+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,cast('00000000000000000000000000000000031' as clob(1K)),cast(X'000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000031' as blob(10K)), next value for nstesttab_seq)");
			
			Random rand = new Random();
			
			int ind = rand.nextInt();
			int id_ind = ind;
			
			Date dt = new Date(1);
			Time tt = new Time(1);
			Timestamp ts = new Timestamp(1);
			String cs = "asdf qwerqwer 12341234 ZXCVZXCVZXCV !@#$!@#$ asdfasdf 1 q a z asdf ASDF qwerasdfzxcvasdfqwer1234asd#";
			
			// Integer ji = null;
			
			// Set value of column "id"
			ps.setInt(1, ind);
			// NsTest.logger.println("set int col 1 to " + ind);
			
			// Set value of column "t_char"
			// scramble the string
			int i1 = Math.abs(ind % 100);
			String cs2 = cs.substring(i1, 99) + cs.substring(0, i1);
			int i2 = i1 < 89 ? i1 + 10 : i1;
			ps.setString(2, cs2.substring(0, i2));
			// NsTest.logger.println("set t_Char to " + cs2.substring(0,i2));
			
			// NsTest.logger.println("now setting date");
			// Set value of column "t_date"
			dt.setTime(Math.abs(rand.nextLong() / 150000));
			ps.setDate(3, dt);
			// NsTest.logger.println("set t_date to " + dt.toString());
			
			// Set value of column "t_decimal"
			double t_dec = rand.nextDouble()
			* Math.pow(10, Math.abs(rand.nextInt() % 6));
			ps.setDouble(4, t_dec);
			// NsTest.logger.println("set t_decimal to "+ t_dec);
			
			// Set value of column "t_decimal_nn"
			double t_dec_nn = rand.nextDouble();
			ps.setDouble(5, t_dec_nn);
			// NsTest.logger.println("set t_decimal_nn " + t_dec_nn);
			
			// Set value of column "t_double"
			double t_doub = rand.nextDouble()
			* Math.pow(10, Math.abs(rand.nextInt() % 300));
			ps.setDouble(6, t_doub);
			// NsTest.logger.println("set t_double to "+ t_doub);
			
			// Set value of column "t_float"
			float t_flt = rand.nextFloat()
			* (float) Math.pow(10, Math.abs(rand.nextInt() % 30));
			ps.setFloat(7, t_flt);
			// NsTest.logger.println("set t_float to " + t_flt);
			
			// Set value of column "t_int"
			int t_intval = rand.nextInt();
			ps.setInt(8, t_intval);
			// NsTest.logger.println("set t_int to " + t_intval);
			
			// Set value of column "t_longint"
			long t_longval = rand.nextLong();
			ps.setLong(9, t_longval);
			// NsTest.logger.println("set t_longint " + t_longval);
			
			// Set value of column "t_numeric_large"
			double t_num_lrg = rand.nextDouble()
			* Math.pow(10, Math.abs(rand.nextInt() % 20));
			ps.setDouble(10, t_num_lrg);
			// NsTest.logger.println("set t_numeric large to " + t_num_lrg);
			
			// Set value of column "t_real"
			float t_fltval = rand.nextFloat()
			* (float) Math.pow(10, Math.abs(rand.nextInt() % 7));
			ps.setFloat(11, t_fltval);
			// NsTest.logger.println("set t_real to " + t_fltval);
			
			// Set value of column "t_smallint"
			int t_smlint = rand.nextInt() % (256 * 128);
			ps.setInt(12, t_smlint);
			// NsTest.logger.println("set t_smallint to " + t_smlint);
			
			// Set value of column "t_time"
			tt.setTime(Math.abs(rand.nextInt()));
			ps.setTime(13, tt);
			// NsTest.logger.println("set t_time to " + tt.toString());
			
			// Set value of column "t_timestamp"
			ts.setTime(Math.abs(rand.nextLong() / 50000));
			ps.setTimestamp(14, ts);
			// NsTest.logger.println("set t_timestamp to " + ts.toString());
			
			// Set value of column "t_varchar"
			ps.setString(15, cs.substring(Math.abs(rand.nextInt() % 100)));
			// NsTest.logger.println("set t_varchar, now executing update stmt");
			try {
				rowsAdded = ps.executeUpdate();
			} catch (SQLException sqe) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
                if ( NsTest.justCountErrors() )
                {
                    NsTest.addError( sqe );
                }
                else
                {
                    if (sqe.getSQLState().equalsIgnoreCase("40XL1")) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
                        NsTest.logger
//IC see: https://issues.apache.org/jira/browse/DERBY-5454
                            .println("LOCK TIMEOUT obtained during insert - add_one_row() "
                                     + sqe.getSQLState());
                    }
                    else if (sqe.getSQLState().equalsIgnoreCase("23505")) {
                        NsTest.logger
                            .println("prevented duplicate row - add_one_row(): "
                                     + sqe.getSQLState() + "; " + sqe.getMessage());

                    } else {
                        throw sqe;
                    }
                }
				
			}
			if (rowsAdded == 1) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
				NsTest.logger.println(thread_id + " inserted 1 row with id "
						//+ id_ind + NsTest.SUCCESS);
                        + id_ind);
			} else
				NsTest.logger.println("FAIL: " + thread_id + " inserted " + rowsAdded + "rows");
			
		} catch (Exception e) {
			NsTest.logger
			.println("Exception when preparing or executing insert prepared stmt");
			printException("executing/preparing insert stmt in dbUtil", e);
			e.printStackTrace( NsTest.logger );
			// ps.close();
		}
		
	
		return rowsAdded;
	}
	
	/*
	 * Update a random row. This method is common to all the worker threads
	 */
	
	public int update_one_row(Connection conn, String thread_id)
	throws Exception {
		
		PreparedStatement ps2 = null;
		String column = null;
		int ind = 0;
		Random rand = new Random();
		int rowsUpdated = 0;
		conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		long skey = pick_one(conn, thread_id);
		if (skey == 0) { // means we did not find a row
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
			NsTest.logger.println(thread_id
					+ " could not find a row to update or there was an error.");
			return rowsUpdated;
		}
		
		ind = Math.abs(rand.nextInt());
		
		column = colnames[ind % NUMTYPES]; // randomly gets one of the columns
		// of the table
		
		try {
			
			ps2 = conn.prepareStatement(" update nstesttab set " + column
					+ " = ? " + " where serialkey = " + skey);
			
		} catch (Exception e) {
			printException(
					"closing update prepared stmt in dbUtil.update_one_row() ",
					e);
			return rowsUpdated;
		}
		
		String ds2 = null;
		String cs = "asdf qwerqwer 12341234 ZXCVZXCVZXCV !@#$!@#$ asdfasdf 1 q a z asdf ASDF qwerasdfzxcvasdfqwer1234asd#";
		double d = 0.0;
		float f = 0;
		int type = (ind % NUMTYPES);
		
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5649
			d = rand.nextDouble() * Math.pow(10, Math.abs(rand.nextInt() % 6));
			ps2.setDouble(1, d);
			ds2 = String.valueOf(d);
			break;
			
		case TDECIMALNN:
			d = rand.nextDouble();
			ps2.setDouble(1, d);
			ds2 = String.valueOf(d);
			break;
			
		case TDOUBLE:
			d = rand.nextDouble() * Math.pow(10, rand.nextInt() % 300);
			ps2.setDouble(1, d);
			ds2 = String.valueOf(d);
			break;
			
		case TFLOAT:
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
			d = rand.nextDouble() * Math.pow(10, rand.nextInt() % 20);
			ps2.setDouble(1, d);
			ds2 = String.valueOf(d);
			break;
			
		case TREAL:
			f = rand.nextFloat() * (float) Math.pow(10, rand.nextInt() % 7);
			ps2.setFloat(1, f);
			ds2 = String.valueOf(f);
			break;
			
		case TSMALLINT:
			i = rand.nextInt() % (256 * 128);
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
			
		} // end of switch(type)
		
		//NsTest.logger.println(thread_id + " attempting  to update col " + column
		//		+ " to " + ds2);
		try {
			rowsUpdated = ps2.executeUpdate();
		} catch (SQLException sqe) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
			NsTest.logger.println(sqe.getSQLState() + " " + sqe.getErrorCode()
					+ " " + sqe.getMessage());
			if ( NsTest.justCountErrors() ) { NsTest.printException( DbUtil.class.getName(), sqe ); }
			else { sqe.printStackTrace( NsTest.logger ); }
		} catch (Exception e) {
			printException("Error in update_one_row()", e);
			e.printStackTrace( NsTest.logger );
		} finally {
			conn
			.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		}
		
		if (rowsUpdated > 0)
			NsTest.logger.println(thread_id + " updated " + rowsUpdated
					+ " row with serialkey " + skey + NsTest.SUCCESS);
		else
			NsTest.logger
			.println(thread_id + " update failed, no such row exists");
		
	
		return rowsUpdated;
	}
	
	//
	// Delete one row from the table. The row to be deleted is chosen randomly
	// using the
	// pick_one method which randomly returns a number between the max of
	// serialkey and
	// the minimum serialkey value that is untouched (nstest.NUM_UNTOUCHED_ROWS)
	//
	public int delete_one_row(Connection conn, String thread_id)
	throws Exception {
		
		PreparedStatement ps = null;
		int rowsDeleted = 0;
		conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		try {
			
			ps = conn
			.prepareStatement(" delete from nstesttab where serialkey = ?");
		} catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
			NsTest.logger
			.println("Unexpected error preparing the statement in delete_one()");
			printException("delete_one_row prepare ", e);
			return rowsDeleted;
		}
		
		long skey = pick_one(conn, thread_id);
		//NsTest.logger.println(thread_id
		//		+ " attempting  to delete a row with serialkey = " + skey);
		if (skey == 0) { // means we did not find a row
			NsTest.logger.println(thread_id
					+ " could not find a row to delete or there was an error.");
			return rowsDeleted;
		}
		
		try {
			ps.setLong(1, skey);
			rowsDeleted = ps.executeUpdate();
		} catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
			NsTest.logger
			.println("Error in delete_one(): either with setLong() or executeUpdate");
			printException("failure to execute delete stmt", e);
		} finally {
			conn
			.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			// set it back to read uncommitted
		}
		
		if (rowsDeleted > 0)
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
			NsTest.logger.println(thread_id + " deleted row with serialkey "
					+ skey + NsTest.SUCCESS);
		else
			NsTest.logger.println(thread_id + " delete for serialkey " + skey
					+ " failed, no such row exists.");
		
		return rowsDeleted;
	}// end of method delete_one()
	
	//
	// get a random serialkey value that matches the criteria:
	// - should not be one of the "protected" rows (set by
	// nstest.NUM_UNTOUCHED_ROWS)
	// - should be less than the current value of the max(serialkey)
	//
	public long pick_one(Connection conn, String thread_id) throws Exception {
				
		Random rand = new Random();
		
//IC see: https://issues.apache.org/jira/browse/DERBY-5454
		long minVal = NsTest.NUM_UNTOUCHED_ROWS + 1;//the max we start with
		long maxVal = NsTest.numInserts;// this is an almost accurate count of
		// the max serialkey since it keeps a count of the num of inserts made
		// so far
		
		// Now choose a random value between minVal and maxVal. We use this
		// value even if the row does not exist (i.e. in a situation where some
		// other thread has deleted this row).
		// The test should just complain and exit with a row not found exception
		// now get a value between the original max, and the current max 
		long rowToReturn = minVal + (long)(rand.nextDouble() * (maxVal - minVal));
		return rowToReturn;
		
//IC see: https://issues.apache.org/jira/browse/DERBY-5421
	}//of method pick_one(...)
	
	// ** This method abstracts exception message printing for all exception
	// messages. You may want to change
	// ****it if more detailed exception messages are desired.
	// ***Method is synchronized so that the output file will contain sensible
	// stack traces that are not
	// ****mixed but rather one exception printed at a time
	public synchronized void printException(String where, Exception e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6533
        if ( NsTest.justCountErrors() )
        {
            NsTest.addError( e );
            return;
        }

		NsTest.logger.println(e.toString());
		if (e instanceof SQLException) {
			SQLException se = (SQLException) e;
			
			if (se.getSQLState().equals("40001"))
				NsTest.logger.println(getThreadName()
						+ " dbUtil --> deadlocked detected");
			if (se.getSQLState().equals("40XL1"))
				NsTest.logger.println(getThreadName()
						+ " dbUtil --> lock timeout exception");
			if (se.getSQLState().equals("23500"))
				NsTest.logger.println(getThreadName()
						+ " dbUtil --> duplicate key violation");
			if (se.getNextException() != null) {
				String m = se.getNextException().getSQLState();
				NsTest.logger.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
				NsTest.logger.println(getThreadName()
						+ " dbUtil ---> Details of exception: " + se.toString()
						+ " " + se.getErrorCode());
			}
		}
//IC see: https://issues.apache.org/jira/browse/DERBY-5465
		if (e.getMessage() == null) {
			NsTest.logger.println(getThreadName()
					+ " dbUtil --> NULL error message detected");
			NsTest.logger
			.println(getThreadName()
					+ " dbUtil --> Here is the NULL exection - "
					+ e.toString());
			NsTest.logger.println(getThreadName()
					+ " dbUtil --> Stack trace of the NULL exception - ");
			if ( NsTest.justCountErrors() ) { NsTest.printException( DbUtil.class.getName(), e ); }
			else { e.printStackTrace( NsTest.logger ); }
		}
		NsTest.logger.println(getThreadName() + " dbUtil ----> During " + where
				+ ", exception thrown was : " + e.toString());
	}
	
	public String getThreadName() {
		return threadName;
	}
}
