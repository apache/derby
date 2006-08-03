/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.wisconsin

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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.impl.tools.ij.utilMain;
import org.apache.derby.tools.ij;

public class wisconsin {

	public static void main(String[] args) throws Throwable{
		ij.getPropertyArg(args); 
        Connection conn = ij.startJBMS();
        
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        
        createTables(conn);
        
        BufferedInputStream inStream;
        
		// set input stream
		String filePath = "wisc_setup.sql";

		try 
		{
			inStream = new BufferedInputStream(new FileInputStream(filePath), 
							utilMain.BUFFEREDFILESIZE);		
		} catch (FileNotFoundException e)
		{
			System.out.println("unable to find input file "+filePath);
			throw e;
		}

		ij.runScript(conn, inStream, "US-ASCII",
				System.out, (String) null);
		conn.commit();
	}
	
	private static void createTables(Connection conn) throws SQLException{
		Statement stmt = conn.createStatement();
		
		stmt.execute("create table TENKTUP1 ( unique1 int not null, " +
											 "unique2 int not null, " +
											 "two int, " +
											 "four int, " +
											 "ten int, " +
											 "twenty int, " +
											 "onePercent int, " +
											 "tenPercent int, " +
											 "twentyPercent int, " +
											 "fiftyPercent int, " +
											 "unique3 int, " +
											 "evenOnePercent int, " +
											 "oddOnePercent int, " +
											 "stringu1 char(52) not null, " +
											 "stringu2 char(52) not null, " +
											 "string4 char(52) )");
		//--insert 10000 rows into TENKTUP1
		WISCInsert wi = new WISCInsert();
		wi.doWISCInsert(10000, "TENKTUP1", conn);
		
		stmt.execute("create unique index TK1UNIQUE1 on TENKTUP1(unique1)");
		stmt.execute("create unique index TK1UNIQUE2 on TENKTUP1(unique2)");
		stmt.execute("create index TK1TWO on TENKTUP1(two)");
		stmt.execute("create index TK1FOUR on TENKTUP1(four)");
		stmt.execute("create index TK1TEN on TENKTUP1(ten)");
		stmt.execute("create index TK1TWENTY on TENKTUP1(twenty)");
		stmt.execute("create index TK1ONEPERCENT on TENKTUP1(onePercent)");
		stmt.execute("create index TK1TWENTYPERCENT on TENKTUP1(twentyPercent)");
		stmt.execute("create index TK1EVENONEPERCENT on TENKTUP1(evenOnePercent)");
		stmt.execute("create index TK1ODDONEPERCENT on TENKTUP1(oddOnePercent)");
		stmt.execute("create unique index TK1STRINGU1 on TENKTUP1(stringu1)");
		stmt.execute("create unique index TK1STRINGU2 on TENKTUP1(stringu2)");
		stmt.execute("create index TK1STRING4 on TENKTUP1(string4)");
		
		stmt.execute("create table TENKTUP2 (unique1 int not null, " +
											"unique2 int not null, " +
											"two int, " +
											"four int, " +
											"ten int, " +
											"twenty int, " +
											"onePercent int, " +
											"tenPercent int, " +
											"twentyPercent int, " +
											"fiftyPercent int, " +
											"unique3 int, " +
											"evenOnePercent int, " +
											"oddOnePercent int, " +
											"stringu1 char(52), " +
											"stringu2 char(52), " +
											"string4 char(52) )");
		//-- insert 10000 rows into TENKTUP2
		wi = new WISCInsert();
		wi.doWISCInsert(10000, "TENKTUP2", conn);
		
		stmt.execute("create unique index TK2UNIQUE1 on TENKTUP2(unique1)");
		stmt.execute("create unique index TK2UNIQUE2 on TENKTUP2(unique2)");
		
		stmt.execute("create table ONEKTUP ( unique1 int not null, " +
											"unique2 int not null, " +
											"two int, " +
											"four int, " +
											"ten int, " +
											"twenty int, " +
											"onePercent int, " +
											"tenPercent int, " +
											"twentyPercent int, " +
											"fiftyPercent int, " +
											"unique3 int, " +
											"evenOnePercent int, " +
											"oddOnePercent int, " +
											"stringu1 char(52), " +
											"stringu2 char(52), " +
											"string4 char(52) )");
		
		//-- insert 1000 rows into ONEKTUP
		wi = new WISCInsert();
		wi.doWISCInsert(1000, "ONEKTUP", conn);
		
		stmt.execute("create unique index ONEKUNIQUE1 on ONEKTUP(unique1)");
		stmt.execute("create unique index ONEKUNIQUE2 on ONEKTUP(unique2)");

		stmt.execute("create table BPRIME (	 unique1 int, " +
										  	"unique2 int, " +
											"two int, " +
											"four int, " +
											"ten int, " +
											"twenty int, " +
											"onePercent int, " +
											"tenPercent int, " +
											"twentyPercent int, " +
											"fiftyPercent int, " +
											"unique3 int, " +
											"evenOnePercent int, " +
											"oddOnePercent int, " +
											"stringu1 char(52), " +
											"stringu2 char(52), " +
											"string4 char(52))");

		stmt.execute("insert into BPRIME select * from TENKTUP2 where TENKTUP2.unique2 < 1000");

		conn.commit();
		
		PreparedStatement ps2 = conn.prepareStatement
			("call SYSCS_UTIL.SYSCS_COMPRESS_TABLE(?, ?, ?)");
		ps2.setString(1, "APP");
		ps2.setString(2, "BPRIME");
		ps2.setInt(3, 0);
		ps2.executeUpdate();
		conn.commit();

		ps2.setString(1, "APP");
		ps2.setString(2, "TENKTUP1");
		ps2.setInt(3, 0);
		ps2.executeUpdate();
		conn.commit();
		
		ps2.setString(1, "APP");
		ps2.setString(2, "TENKTUP2");
		ps2.setInt(3, 0);
		ps2.executeUpdate();
		conn.commit();

		ps2.setString(1, "APP");
		ps2.setString(2, "ONEKTUP");
		ps2.setInt(3, 0);
		ps2.executeUpdate();
		conn.commit();
	}
	
}
