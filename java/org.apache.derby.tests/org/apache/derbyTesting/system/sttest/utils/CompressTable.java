/*
 * 
 * Derby - Class org.apache.derbyTesting.system.sttest.CompressTable
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
package org.apache.derbyTesting.system.sttest.utils;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.system.sttest.tools.MemCheck;

/**
 * This class is used to compress the table to retrieve the space after deletion
 */
public class CompressTable {
	
	static boolean startByIJ = false;
	
	static String dbURL = "jdbc:derby:testDB";
	
	static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	
	public static void main(String[] args) throws SQLException, IOException,
	InterruptedException, Exception, Throwable {
		Connection conn = null;
		Date d = null;

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
        Class<?> clazz = Class.forName(driver);
        clazz.getConstructor().newInstance();
		
		try {
			conn = mystartJBMS();
		} catch (Throwable t) {
			return;
		}
		MemCheck mc = new MemCheck(200000);
		mc.start();
		compress(conn);
		System.exit(0);
	}
	
	static public Connection mystartJBMS() throws Throwable {
		Connection conn = null;
		if (startByIJ == true)
			conn = ij.startJBMS();
		else
			try {
				conn = DriverManager.getConnection(dbURL + ";create=false");
				conn.setAutoCommit(false);
			} catch (SQLException se) {
				System.out.println("connect failed  for " + dbURL);
				JDBCDisplayUtil.ShowException(System.out, se);
			}
			return (conn);
	}
	
	static synchronized void compress(Connection conn)
	throws java.lang.Exception {
		System.out.println("compressing table");
		try {
//IC see: https://issues.apache.org/jira/browse/DERBY-4213
			conn.setAutoCommit(true);
			CallableStatement cs = conn
				.prepareCall("CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, ?, ?, ?)");
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
		}
	}
}
