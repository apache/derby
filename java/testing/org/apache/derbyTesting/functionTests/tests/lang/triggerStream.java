/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.triggerStream

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.StreamUtil;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/*
Small trigger stream test.  Make sure we can read streams ok from the context
of a row or statement trigger.
*/
public class triggerStream {

	public static void main(String[] args) {
		try{
			ij.getPropertyArg(args); 
	        Connection conn = ij.startJBMS();
	        
	        //create tables and functions to be used in triggers
	        createTablesAndFunctions(conn);
	        
	        //create triggers for Ascii test
	        createTriggersWithAsciiStream(conn);
	        //test triggers for Ascii stream
	        testTriggersWithAsciiStream(conn);
	        
	        //create triggers for binary test
	        createTriggersWithBinaryStream(conn);
	        //test triggers for binary stream
	        testTriggersWithBinaryStream(conn);
	        
	        conn.close();
		} catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}
	}
	
	private static void createTablesAndFunctions(Connection conn) throws SQLException{
		
		System.out.println("Start creating tables and functions to be used in triggers ...");
		
		Statement stmt = conn.createStatement();
		
		stmt.execute("create table x1 (x int, c1 long varchar, y int, slen int)");
		stmt.execute("create table x2 (x int, c1 long varchar for bit data, y int, slen int)");
		
		// getAsciiColumn() method reads in the stream and verifies each byte and prints 
		// out the length of the column
		stmt.execute("create function getAsciiColumn(whichRS int,colNumber int,value varchar(128))"
				+ " returns int PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME "
				+"'org.apache.derbyTesting.functionTests.util.StreamUtil.getAsciiColumn'");
		
		// getBinaryColumn() method reads in the stream and verifies each byte and prints
		// out the length of the column
		stmt.execute("create function getBinaryColumn( whichRS int, colNumber int, value varchar(128))"
				+ " returns int PARAMETER STYLE JAVA LANGUAGE JAVA NO SQL EXTERNAL NAME "
				+ "'org.apache.derbyTesting.functionTests.util.StreamUtil.getBinaryColumn'");
		
		stmt.close();
		
		System.out.println("... done creating tables and functions to be used in triggers.");
	}
	
	private static void createTriggersWithAsciiStream(Connection conn) throws SQLException{
		
		System.out.println("Start creating triggers for Ascii stream tests ...");
		
		Statement stmt = conn.createStatement();
		
		stmt.execute("create trigger t11 NO CASCADE before update of x,y on x1 "
				+ "for each statement values getAsciiColumn( 0, 2, 'a')");
		stmt.execute("create trigger t12 after update of x,y on x1 for each row"
				+ " values getAsciiColumn( 1, 2, 'a')");
		stmt.execute("create trigger t13 after insert on x1 for each statement" 
				+ " values getAsciiColumn( 1, 2, 'a')");
		stmt.execute("create trigger t14 NO CASCADE before insert on x1 for each row" 
				+ " values getAsciiColumn( 1, 2, 'a')");
		stmt.execute("create trigger t15 NO CASCADE before delete on x1 "
				+ "for each statement values getAsciiColumn( 0, 2, 'a')");
		stmt.execute("create trigger t16 after delete on x1 for each row "
				+ " values getAsciiColumn( 0, 2, 'a')");
		
		stmt.close();
		
		System.out.println("... done creating triggers for Ascii stream tests.");
	}
	
	private static void testTriggersWithAsciiStream(Connection conn) throws Throwable{
		
		int count = 1;
		insertAsciiColumn(count++,conn,"insert into x1 values (1, ?, 1, ?)", 1, "a", 1);
		insertAsciiColumn(count++,conn,"insert into x1 values (2, ?, 2, ?)", 1, "a", 10);
		insertAsciiColumn(count++,conn,"insert into x1 values (3, ?, 3, ?)", 1, "a", 100);
		insertAsciiColumn(count++,conn,"insert into x1 values (4, ?, 4, ?)", 1, "a", 1000);
		insertAsciiColumn(count++,conn,"insert into x1 values (5, ?, 5, ?)", 1, "a", 5000);
		insertAsciiColumn(count++,conn,"insert into x1 values (6, ?, 6, ?)", 1, "a", 10000);
		insertAsciiColumn(count++,conn,"insert into x1 values (7, ?, 7, ?)", 1, "a", 16500);
		insertAsciiColumn(count++,conn,"insert into x1 values (8, ?, 8, ?)", 1, "a", 32500);
		insertAsciiColumn(count++,conn,"insert into x1 values (9, ?, 9, ?)", 1, "a", 0);
		insertAsciiColumn(count++,conn,"insert into x1 values (10, ?, 10, ?)", 1, "a", 666);
		
		executeStatement(conn,"update x1 set x = x+1");
		executeStatement(conn,"update x1 set x = null");
		executeStatement(conn,"insert into x1 select * from x1");
		executeStatement(conn,"delete from x1");
	}
	
	private static void createTriggersWithBinaryStream(Connection conn) throws SQLException{
		
		System.out.println("Start creating triggers for binary stream tests ...");
		
		Statement stmt = conn.createStatement();
		stmt.execute("create trigger t21 NO CASCADE before update of x,y on x2 "
				+ "for each statement values getBinaryColumn( 0, 2, 'a')");
		stmt.execute("create trigger t22 after update of x,y on x2 for each row"
				+ " values getBinaryColumn( 1, 2, 'a')");
		stmt.execute("create trigger t23 after insert on x2 for each statement"
				+ " values getBinaryColumn( 1, 2, 'a')");
		stmt.execute("create trigger t24 NO CASCADE before insert on x2 for each row"
				+ " values getBinaryColumn( 1, 2, 'a')");
		stmt.execute("create trigger t25 NO CASCADE before delete on x2 for each statement"
				+ " values getBinaryColumn( 1, 2, 'a')");
		stmt.execute("create trigger t26 after delete on x2 for each row"
				+ " values getBinaryColumn( 0, 2, 'a')");
		stmt.close();
		
		System.out.println("... done creating triggers for binary stream tests.");
	}
	
	private static void testTriggersWithBinaryStream(Connection conn) throws Throwable{
		
		int count = 1;
		insertBinaryColumn(count++,conn,"insert into x2 values (1, ?, 1, ?)", 1, "a", 1);
		insertBinaryColumn(count++,conn,"insert into x2 values (2, ?, 2, ?)", 1, "a", 10);
		insertBinaryColumn(count++,conn,"insert into x2 values (3, ?, 3, ?)", 1, "a", 100);
		insertBinaryColumn(count++,conn,"insert into x2 values (4, ?, 4, ?)", 1, "a", 1000);
		insertBinaryColumn(count++,conn,"insert into x2 values (5, ?, 5, ?)", 1, "a", 10000);
		insertBinaryColumn(count++,conn,"insert into x2 values (6, ?, 6, ?)", 1, "a", 32700);
		insertBinaryColumn(count++,conn,"insert into x2 values (7, ?, 7, ?)", 1, "a", 32699);
		insertBinaryColumn(count++,conn,"insert into x2 values (8, ?, 8, ?)", 1, "a", 16384);
		insertBinaryColumn(count++,conn,"insert into x2 values (9, ?, 9, ?)", 1, "a", 16383);
		insertBinaryColumn(count++,conn,"insert into x2 values (10, ?, 10, ?)", 1, "a", 0);
		insertBinaryColumn(count++,conn,"insert into x2 values (11, ?, 11, ?)", 1, "a", 666);
		
		
		executeStatement(conn,"select x, length(c1) from x2 order by 1");
		executeStatement(conn,"update x2 set x = x+1");
		executeStatement(conn,"select x, length(c1) from x2 order by 1");
		executeStatement(conn,"update x2 set x = null");
		executeStatement(conn,"select x, length(c1) from x2 order by 2");
		executeStatement(conn,"insert into x2 select * from x2");
		executeStatement(conn,"select x, length(c1) from x2 order by 2");
		executeStatement(conn,"delete from x2");
		
	}
	
	private static void executeStatement(Connection conn, String str) throws SQLException{
		System.out.println("#### Executing \""+ str + "\"");
		Statement stmt = conn.createStatement();
		
		//Display results for select statements
		if(str.startsWith("select")) {
			ResultSet rs = stmt.executeQuery(str);
			JDBCDisplayUtil.DisplayResults(System.out,rs,conn);
			rs.close();
		} else 
			stmt.execute(str);
		
		
		stmt.close();
	}
	
	private static void insertAsciiColumn
	(
		int 			count,	
		Connection 		conn,	
		String 			stmtText, 
		int				colNumber,
		String 			value, 
		int 			length
	)
		throws Throwable 
	{
		System.out.println("Call #" + count + " to insertAsciiColumn"); 
		StreamUtil.insertAsciiColumn(conn, stmtText, colNumber, value, length);
	}
	
	private static void insertBinaryColumn
	(
		int 			count,
		Connection 		conn,
		String 			stmtText, 
		int				colNumber,
		String 			value, 
		int 			length
	)
		throws Throwable
	{
		System.out.println("Call #" + count + " to insertBinaryColumn");
		StreamUtil.insertBinaryColumn(conn, stmtText, colNumber, value, length);
	}
	
}
