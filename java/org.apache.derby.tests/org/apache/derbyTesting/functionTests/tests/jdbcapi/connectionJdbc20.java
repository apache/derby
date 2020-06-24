/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.connectionJdbc20

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.io.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 *This Program Test getConnection()/getStatement().
 */

public class connectionJdbc20{ 

	public static void main(String[] args) {
		Connection conn, connreturn;
		Statement stmt, stmtreturn;


		System.out.println("Test connection20 starting");
		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			 ij.getPropertyArg(args);
			 conn = ij.startJBMS();

			 stmt = conn.createStatement();

            //create a table, insert a row, do a select from the table,
			 stmt.execute("create table tab1("+
                                           "c1 char(100) for bit data,"+
                                           "c2 varchar(100) for bit data," +
                                           "c3 long varchar for bit data,"+
                                           "c4 char(100),"+ 
                                           "c5 varchar(100),"+
                                           "c6 long varchar)");

            connreturn = stmt.getConnection();
            if (conn.equals(connreturn))
                System.out.println("Got Same Connection Object");
            else
                System.out.println("Got Different Connection Object");
           
            // load some data into this table ..
            load_data(connreturn);
            
			// read the data   of each type with all the possible functions
//IC see: https://issues.apache.org/jira/browse/DERBY-721
			ResultSet rs = stmt.executeQuery("select " + 
							 "c1," + 
							 "c2," + 
							 "c3," + 
							 "c4," + 
							 "c5," + 
							 "c6," + 
							 "c1 as c1_spare," + 
							 "c2 as c2_spare,"  +
							 "c3 as c3_spare "  +
							 "from tab1");
            int loop = 0;
            while(loop < 2 )
            {
                while (rs.next())
                {
                    for(int i=1 ; i < 7 ; i++)
                    {
                        get_using_object(rs, i);
                        get_using_string(rs, i);
			
			get_using_ascii_stream(rs, i);
//IC see: https://issues.apache.org/jira/browse/DERBY-721

                        if(i < 4 ) // only c1 , c2, c3
                        {
                            get_using_binary_stream(rs, i + 6);
                            get_using_bytes(rs, i + 6);
                        }
                    } 
                }
                // get the statment back from the result set
                stmtreturn = rs.getStatement();
                if (stmt.equals(stmtreturn))
                    System.out.println("Got Same Statement Object");
                else
                    System.out.println("Got Different Statement Object");
                
                rs.close();
//IC see: https://issues.apache.org/jira/browse/DERBY-721
		rs = stmt.executeQuery("select " + 
				       "c1," + 
				       "c2," + 
				       "c3," + 
				       "c4," + 
				       "c5," + 
				       "c6," + 
				       "c1 as c1_spare," + 
				       "c2 as c2_spare,"  +
				       "c3 as c3_spare "  +
				       "from tab1");
		loop++;
            }

	    stmt.close();

            // Try to get the connection object thro database meta data
            DatabaseMetaData dbmeta = conn.getMetaData();

			rs = dbmeta.getTypeInfo();
            while (rs.next())
            {
               System.out.println(rs.getString(1)); 
            }
            // try to get a statemet from a meta data result set
            stmt = rs.getStatement(); 
            rs.close();

            // Try to get the Connection back from a Metadata
            System.out.println("Try to Get the connection back from metadata");
            connreturn = dbmeta.getConnection();
            if (conn.equals(connreturn))
                System.out.println("Got Same Connection Object");
            else
                System.out.println("Got Different Connection Object");

             
            // Try to get the connection thru callable statement
            CallableStatement  cs = conn.prepareCall("select * from tab1");
            System.out.println(" Try to get the connection back from a callable stmt");
            connreturn = cs.getConnection();
            if (conn.equals(connreturn))
                System.out.println("Got Same Connection Object");
            else
                System.out.println("Got Different Connection Object");

            cs.close();
			conn.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
			e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception: "+e);
			e.printStackTrace();
		}

		System.out.println("Test getConnection  finished");
    }


    static private void load_data(Connection conn) throws Exception{
        PreparedStatement pstmt = null;

        try{
            pstmt = conn.prepareStatement(
                     "insert into tab1 values(?,?,?,?,?,?)");
            String c1_value="C1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
            String c2_value="C2XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
            String c3_value="C3XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
            String c4_value="C4XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
            String c5_value="C5XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
            String c6_value="C6XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

            pstmt.setBytes(1, c1_value.getBytes("US-ASCII"));
            pstmt.setBytes(2, c2_value.getBytes("US-ASCII"));
            pstmt.setBytes(3, c3_value.getBytes("US-ASCII"));
            pstmt.setString(4, c4_value);
            pstmt.setString(5, c5_value);
            pstmt.setString(6, c6_value);
            pstmt.execute();

            // get the connection back thru preapred statement
            System.out.println("Try to get connection using preaparedstatement");
            Connection connreturn = pstmt.getConnection();
            if (conn.equals(connreturn))
                System.out.println("Got Same Connection Object");
            else
                System.out.println("Got Different Connection Object");

        }catch(SQLException e){
             dumpSQLExceptions(e);
             e.printStackTrace();
        }catch(Throwable e ){
             System.out.println("Fail -- unexpected exception ");
             e.printStackTrace();
        }finally{
             pstmt.close();
        }
    }

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
		}
	}

	static private void dumpExpectedSQLExceptions (SQLException se) {
		System.out.println("PASS -- expected exception");
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
			se = se.getNextException();
        }
    }

    static private int printbytearray(byte [] a , int len, int count){

		for (int i =0 ; i < len; i++, count++) {

			System.out.print("x" + Integer.toHexString(a[i]));

			if ((i > 0) && ((i % 20) == 0))
				System.out.println("");
		}
		return count;
    }

    static void get_using_object(ResultSet rs, int col_no) throws Exception{
		System.out.println("getObject(" + col_no + ")");
        Object cobj = rs.getObject(col_no);
        if (cobj instanceof byte[]){
            byte[] bytearray = (byte[]) cobj;
			System.out.println("  as byte[] length " + bytearray.length);
            printbytearray(bytearray, bytearray.length, 0);
			System.out.println("");
        }else {
			System.out.println("  as String");
            System.out.println(cobj.toString());
		}
    }

    static void get_using_bytes(ResultSet rs, int col_no) throws Exception{
		System.out.println("getBytes(" + col_no + ")");
       byte[] bytearray = (byte[]) rs.getBytes(col_no);
       printbytearray(bytearray, bytearray.length, 0);
		System.out.println("");
    }

    static void get_using_string(ResultSet rs, int col_no) throws Exception{
		String s = rs.getString(col_no);
		System.out.println("getString(" + col_no + ") length " + s.length());
        System.out.println(s);
    }

    static void get_using_ascii_stream(ResultSet rs, int col_no) throws Exception{
 		System.out.println("getAsciiStream(" + col_no + ")");
		int no_bytes_read = 0;
        InputStream rsbin = rs.getAsciiStream(col_no);
        byte [] bytearray = new byte[200];
		int count = 0;
        while((no_bytes_read=rsbin.read(bytearray)) != -1)
        {
			count = printbytearray(bytearray, no_bytes_read, count);
        }
		System.out.println("");

    }

    static void get_using_binary_stream(ResultSet rs, int col_no) throws Exception{
 		System.out.println("getBinaryStream(" + col_no + ")");
        int no_bytes_read = 0;
        InputStream rsbin = rs.getBinaryStream(col_no);
        byte [] bytearray = new byte[200];
		int count = 0;
        while((no_bytes_read=rsbin.read(bytearray)) != -1)
        {
			count = printbytearray(bytearray, no_bytes_read, count);
        }
		System.out.println("");
    }

}
