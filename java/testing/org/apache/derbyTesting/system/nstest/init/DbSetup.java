/*

 Derby - Class org.apache.derbyTesting.system.nstest.init.DbSetup

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

package org.apache.derbyTesting.system.nstest.init;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derbyTesting.system.nstest.NsTest;

/**
 * DbSetup: Creates database and builds single user table with indexes
 */
public class DbSetup {

	/**
	 * The main database setup method
	 */
	public static boolean doIt(Connection conn) throws Throwable {
		Statement s = null;
		ResultSet rs = null;
		boolean finished = false;

		System.out.println("dbSetup.doIt() starting...");

		try {
			conn.setAutoCommit(false);
		} catch (Exception e) {
			System.out.println("FAIL - setAutoCommit() failed:");
			printException("setting autocommit in dbSetup", e);
			return (false);
		}

		try {
			s = conn.createStatement();
			rs = s.executeQuery("select tablename from sys.systables "
					+ " where tablename = 'NSTESTTAB'");
			if (rs.next()) {
				rs.close();
				System.out.println("table 'NSTESTTAB' already exists");
				finished = true;
				NsTest.schemaCreated = true; // indicates to other classes
				// that the schema already
				// exists
			}
		} catch (Exception e) {
			System.out
			.println("dbSetup.doIt() check existance of table: FAIL -- unexpected exception:");
			printException(
					"executing query or processing resultSet to check for table existence",
					e);
			return (false);
		}

		// if we reach here then the table does not exist, so we create it
		if (finished == false) {
			try {
				System.out
				.println("creating table 'NSTESTTAB' and corresponding indices");
				s.execute("create table nstesttab (" + "id int,"
						+ "t_char char(100)," + "t_date date,"
						+ "t_decimal decimal," + "t_decimal_nn decimal(10,10),"
						+ "t_double double precision," + "t_float float,"
						+ "t_int int," + "t_longint bigint,"
						+ "t_numeric_large numeric(30,10)," + "t_real real,"
						+ "t_smallint smallint," + "t_time time,"
						+ "t_timestamp timestamp," + "t_varchar varchar(100),"
						+ "t_clob clob(1K)," + "t_blob blob(10K),"
						+ "serialkey bigint generated always as identity, "
						+ "unique (serialkey)) ");

				s.execute("create index t_char_ind on nstesttab ( t_char)");
				s.execute("create index t_date_ind on nstesttab ( t_date)");
				s
				.execute("create index t_decimal_ind on nstesttab ( t_decimal)");
				s
				.execute("create index t_decimal_nn_ind on nstesttab ( t_decimal_nn)");
				s.execute("create index t_double_ind on nstesttab ( t_double)");
				s.execute("create index t_float_ind on nstesttab ( t_float)");
				s.execute("create index t_int_ind on nstesttab ( t_int)");
				s
				.execute("create index t_longint_ind on nstesttab ( t_longint)");
				s
				.execute("create index t_num_lrg_ind on nstesttab ( t_numeric_large)");
				s.execute("create index t_real_ind on nstesttab ( t_real)");
				s
				.execute("create index t_smallint_ind on nstesttab ( t_smallint)");
				s.execute("create index t_time_ind on nstesttab ( t_time)");
				s
				.execute("create index t_timestamp_ind on nstesttab ( t_timestamp)");
				s
				.execute("create index t_varchar_ind on nstesttab ( t_varchar)");
				s
				.execute("create index t_serialkey_ind on nstesttab (serialkey)");

				System.out
				.println("creating table 'NSTRIGTAB' and corresponding indices");
				s.execute("create table NSTRIGTAB (" + "id int,"
						+ "t_char char(100)," + "t_date date,"
						+ "t_decimal decimal," + "t_decimal_nn decimal(10,10),"
						+ "t_double double precision," + "t_float float,"
						+ "t_int int," + "t_longint bigint,"
						+ "t_numeric_large numeric(30,10)," + "t_real real,"
						+ "t_smallint smallint," + "t_time time,"
						+ "t_timestamp timestamp," + "t_varchar varchar(100),"
						+ "t_clob clob(1K)," + "t_blob blob(10K),"
						+ "serialkey bigint )");
				// create trigger
				s.execute("CREATE TRIGGER NSTEST_TRIG AFTER DELETE ON nstesttab "
						+ "REFERENCING OLD AS OLDROW FOR EACH ROW MODE DB2SQL "
						+ "INSERT INTO NSTRIGTAB values("
						+ "OLDROW.ID, OLDROW.T_CHAR,OLDROW.T_DATE,"
						+ "OLDROW.T_DECIMAL,OLDROW.T_DECIMAL_NN,OLDROW.T_DOUBLE,"
						+ "OLDROW.T_FLOAT, OLDROW.T_INT,OLDROW.T_LONGINT, OLDROW.T_numeric_large,"
						+ "OLDROW.T_real,OLDROW.T_smallint,OLDROW.T_time,OLDROW.T_timestamp,OLDROW.T_varchar,"
						+ "OLDROW.T_clob,OLDROW.T_blob, "
						+ "OLDROW.serialkey)");
			} catch (Exception e) {
				e.printStackTrace();
				System.out
				.println("FAIL - unexpected exception in dbSetup.doIt() while creating schema:");
				printException("executing statements to create schema", e);
				return (false);
			}
		}// end of if(finished==false)

		conn.commit();
		return (true);

	}// end of method doIt()

	// ** This method abstracts exception message printing for all exception
	// messages. You may want to change
	// ****it if more detailed exception messages are desired.
	// ***Method is synchronized so that the output file will contain sensible
	// stack traces that are not
	// ****mixed but rather one exception printed at a time
	public static synchronized void printException(String where, Exception e) {
		if (e instanceof SQLException) {
			SQLException se = (SQLException) e;

			if (se.getSQLState().equals("40001"))
				System.out.println("deadlocked detected");
			if (se.getSQLState().equals("40XL1"))
				System.out.println(" lock timeout exception");
			if (se.getSQLState().equals("23500"))
				System.out.println(" duplicate key violation");
			if (se.getNextException() != null) {
				String m = se.getNextException().getSQLState();
				System.out.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
			}
		}
		if (e.getMessage().equals(null)) {
			System.out.println("NULL error message detected");
			System.out.println("Here is the NULL exection - " + e.toString());
			System.out.println("Stack trace of the NULL exception - ");
			e.printStackTrace(System.out);
		}
		System.out.println("During " + where + ", exception thrown was : "
				+ e.getMessage());
	}

}//end of class definition

