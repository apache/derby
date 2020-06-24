/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.wisconsin

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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1961
//IC see: https://issues.apache.org/jira/browse/DERBY-2911
        createTables(conn, true);
        
        BufferedInputStream inStream;
//IC see: https://issues.apache.org/jira/browse/DERBY-1914
        String resource = "org/apache/derbyTesting/functionTests/tests/" +
                "lang/wisc_setup.sql";  
        // set input stream
        URL sql = getTestResource(resource);
        InputStream sqlIn = openTestResource(sql);
        if (sqlIn == null ) {
            throw new Exception("SQL Resource missing:" +
                    resource);
        }

        inStream = new BufferedInputStream(sqlIn, 
                utilMain.BUFFEREDFILESIZE);		

		ij.runScript(conn, inStream, "US-ASCII",
			     System.out, (String) null );
		conn.commit();
	}
	
	public static void createTables(Connection conn, boolean compress)
			throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4363
                createTables(conn, compress, 10000);
        }
	public static void createTables(Connection conn, boolean compress, int numRows)
			throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-1961
//IC see: https://issues.apache.org/jira/browse/DERBY-2911

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
		//--insert numRows rows into TENKTUP1
		WISCInsert wi = new WISCInsert();
//IC see: https://issues.apache.org/jira/browse/DERBY-4363
		wi.doWISCInsert(numRows, "TENKTUP1", conn);
		
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
		//-- insert numRows rows into TENKTUP2
		wi = new WISCInsert();
//IC see: https://issues.apache.org/jira/browse/DERBY-4363
		wi.doWISCInsert(numRows, "TENKTUP2", conn);
		
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

//IC see: https://issues.apache.org/jira/browse/DERBY-1961
//IC see: https://issues.apache.org/jira/browse/DERBY-2911
		if (!compress) {
			return;
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-937
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
	
	
    /**
     * Open the URL for a a test resource, e.g. a policy
     * file or a SQL script.
     * @param url URL obtained from getTestResource
     * @return An open stream
    */
    protected static InputStream openTestResource(final URL url)
//IC see: https://issues.apache.org/jira/browse/DERBY-1914
        throws PrivilegedActionException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return AccessController.doPrivileged
        (new java.security.PrivilegedExceptionAction<InputStream>(){

            public InputStream run() throws IOException {
            return url.openStream();

            }

        }
         );     
    }
    
    /**
     * Obtain the URL for a test resource, e.g. a policy
     * file or a SQL script.
     * @param name Resource name, typically - org.apache.derbyTesing.something
     * @return URL to the resource, null if it does not exist.
     */
    protected static URL getTestResource(final String name)
    {

//IC see: https://issues.apache.org/jira/browse/DERBY-5840
    return AccessController.doPrivileged
        (new java.security.PrivilegedAction<URL>(){

            public URL run(){
            return this.getClass().getClassLoader().
                getResource(name);

            }

        }
         );
    }  
}
