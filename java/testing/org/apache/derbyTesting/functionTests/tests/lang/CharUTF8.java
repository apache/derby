/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CharUTF8

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import java.io.*;
import java.sql.PreparedStatement;

/**
 * Test all characters written through the UTF8 format.
 */

public class CharUTF8 {

	public static PreparedStatement psSet;
	public static PreparedStatement psGet;    

    public static void main(String[] args) {

		System.out.println("Test CharUTF8 starting");

		StringBuffer buff = new StringBuffer();

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Connection conn = ij.startJBMS();

			Statement st2 = conn.createStatement();
			st2.execute("CREATE TABLE TEST(id int not null primary key, body varchar(60))");
			psSet = conn.prepareStatement("insert into test values(?,?)");
			psGet = conn.prepareStatement("select body from test where id=?");  
			
			int off = 0;
			for (int i = Character.MIN_VALUE; i <= Character.MAX_VALUE; i++) {

				buff.append((char) i);

				if ((buff.length() == 60) || (i == Character.MAX_VALUE)) {

					String text = buff.toString();
					System.out.println("Testing with last char value " + i + " length=" + text.length());

					// set the text
					setBody(i, text);
    
				// now read the text
					String res = getBody(i);
					if (!res.equals(text)) {
						System.out.println("FAIL -- string fetched is incorrect, length is "
							+ buff.length() + ", expecting string: " + text
							+ ", instead got the following: " + res);
						break;
					}

					buff.setLength(0);
				}
			}

			// quick test of an empty string aswell
			setBody(-1, "");
			if (!getBody(-1).equals("")) {
				System.out.println("FAIL: empty string returned as " + getBody(-1));
			}


			conn.close();

		} catch (SQLException e) {
			dumpSQLExceptions(e);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
		}

		System.out.println("Test CharUTF8 finished");

    }

	private static void setBody(int key, String body) {
        
		try {
			psSet.setInt(1, key);
			psSet.setString(2, body);
			psSet.executeUpdate();

		} catch (SQLException ex) {
			ex.printStackTrace();

			System.out.println("FAIL -- unexpected exception");
			System.exit(-1);
		}        
	}
        
    private static String getBody(int key) {
        
        String result="NO RESULT";
        
        try {
			psGet.setInt(1, key);
			ResultSet rs = psGet.executeQuery();

			if (rs.next())
				result = rs.getString(1);    

		} catch (SQLException ex) {
              ex.printStackTrace();
        }        
        
        return result;
    }

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se = se.getNextException();
		}
	}

	/**
		Utility method for dynamicLikeOptimization test. Return a single character
		string with the highest defined Unicode character. See java.lang.Character.isDefined.
	*/
	public static String getMaxDefinedCharAsString() {

		return "\uFA2D";
	}

}
