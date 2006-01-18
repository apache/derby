/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.maxMemPerTab

Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.functionTests.util.TestUtil;

public class maxMemPerTab {

	public static void main(String[] args) {
		try {
			ij.getPropertyArg(args); 
		    Connection conn = ij.startJBMS();
		    conn.setAutoCommit(false);
		    
		    createTablesAndInsertData(conn);
		    getStatistics(conn);
		
		    conn.rollback();
		    conn.close();  
		} catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}
	}

	private static void createTablesAndInsertData(Connection conn) throws SQLException {
		
		System.out.println("Start creating tables and inserting data ...");
		
		//create tables
		PreparedStatement ps = conn.prepareStatement("create table tab1 (c1 int, c2 varchar(20000))");
		ps.execute();
		ps = conn.prepareStatement("create table tab2 (c1 int, c2 varchar(20000))");
		ps.execute();
		ps = conn.prepareStatement("create table tab3 (c1 int, c2 varchar(2000))");
		ps.execute();
		ps = conn.prepareStatement("create table tab4 (c1 int, c2 varchar(2000))");
		ps.execute();
		
		//insert data
		String largeStringA20000 = new String(Formatters.repeatChar("a",20000));
		String largeStringA2000 = new String(Formatters.repeatChar("a",2000));
		String largeStringB20000 = new String(Formatters.repeatChar("b",20000));
		String largeStringB2000 = new String(Formatters.repeatChar("b",2000));
		String largeStringC20000 = new String(Formatters.repeatChar("c",20000));
		String largeStringC2000 = new String(Formatters.repeatChar("c",2000));
		String largeStringD20000 = new String(Formatters.repeatChar("d",20000));
		String largeStringD2000 = new String(Formatters.repeatChar("d",2000));

		ps = conn.prepareStatement("insert into tab1 values (?, ?)");
		ps.setInt(1, 1);
		ps.setString(2, largeStringA20000);
		ps.executeUpdate();
		ps.setInt(1, 2);
		ps.setString(2, largeStringB20000);
		ps.executeUpdate();
		ps.setInt(1, 3);
		ps.setString(2, largeStringC20000);
		ps.executeUpdate();
		ps.close();
		ps = conn.prepareStatement("insert into tab2 values (?, ?)");
		ps.setInt(1, 1);
		ps.setString(2, largeStringA20000);
		ps.executeUpdate();
		ps.setInt(1, 2);
		ps.setString(2, largeStringC20000);
		ps.executeUpdate();
		ps.setInt(1, 3);
		ps.setString(2, largeStringD20000);
		ps.executeUpdate();
		ps.close();
		ps = conn.prepareStatement("insert into tab3 values (?, ?)");
		ps.setInt(1, 1);
		ps.setString(2, largeStringA2000);
		ps.executeUpdate();
		ps.setInt(1, 2);
		ps.setString(2, largeStringB2000);
		ps.executeUpdate();
		ps.setInt(1, 3);
		ps.setString(2, largeStringC2000);
		ps.executeUpdate();
		ps.close();
		ps = conn.prepareStatement("insert into tab4 values (?, ?)");
		ps.setInt(1, 1);
		ps.setString(2, largeStringA2000);
		ps.executeUpdate();
		ps.setInt(1, 2);
		ps.setString(2, largeStringC2000);
		ps.executeUpdate();
		ps.setInt(1, 3);
		ps.setString(2, largeStringD2000);
		ps.executeUpdate();
		ps.close();
		
		System.out.println("... done creating tables and inserting data.");
	}
	
	private static void getStatistics(Connection conn) throws SQLException {
		
		Statement stmt = conn.createStatement();
		stmt.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
		System.out.println("Called SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
		
		JDBCDisplayUtil.setMaxDisplayWidth(2500);
		
		//should use nested loop join due to maxMemoryPerTable property setting
		executeQuery(stmt,conn,"select * from tab1, tab2 where tab1.c2 = tab2.c2");
		executeQuery(stmt,conn,"values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
		
		//should use hash join, maxMemoryPerTable property value is big enough
		executeQuery(stmt,conn,"select * from tab3, tab4 where tab3.c2 = tab4.c2");
		executeQuery(stmt,conn,"values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
		
		stmt.close();
	}
	
	private static void executeQuery(Statement stmt, Connection conn, String query) throws SQLException{
		System.out.println("#### Executing \""+ query + "\"");
		//Display results for select statements
		ResultSet rs = stmt.executeQuery(query);
		JDBCDisplayUtil.DisplayResults(System.out,rs,conn);
		rs.close();
	}
}
