/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.tests.lang
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.*;
import java.io.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * Demonstrate subselect behavior with prepared statement. 
 */
public class logStream {

  public static void main (String args[]) 
  { 
    try {
		System.out.println("Start logStream");
		/* Load the JDBC Driver class */
		// use the ij utility to read the property file and
		// make the initial connection.
		ij.getPropertyArg(args);
		Connection conn = ij.startJBMS();

		conn.close();

		String systemHome = System.getProperty("derby.system.home");

		File derbyLog = new File(systemHome, "derby.log");

		System.out.println("derby.log exists ?       " + derbyLog.exists());
		System.out.println("derby.log is directory ? " + derbyLog.isDirectory());
		System.out.println("derby.log has content ?  " + (derbyLog.length() > 0));

		System.out.println("SHUTDOWN Cloudscape");
		try {
			DriverManager.getConnection("jdbc:derby:;shutdown=true");
			System.out.println("FAIL - shutdown returned connection");
		} catch (SQLException sqle) {
			System.out.println("SHUTDOWN :" + sqle.getMessage());
		}


		System.out.println("derby.log exists ?       " + derbyLog.exists());
		System.out.println("derby.log is directory ? " + derbyLog.isDirectory());
		System.out.println("derby.log has content ?  " + (derbyLog.length() > 0));

		boolean deleted = derbyLog.delete();
		System.out.println("deleted derby.log ?     " + deleted);

		System.out.println("End logStream");
    } catch (Exception e) {
		System.out.println("FAIL -- unexpected exception "+e);
		JDBCDisplayUtil.ShowException(System.out, e);
      	e.printStackTrace();
    }
  } 

}
