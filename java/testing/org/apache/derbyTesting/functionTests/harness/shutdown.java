/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.*;
import java.io.*;

import org.apache.derby.tools.JDBCDisplayUtil;

/*
 **
 ** shutdown
 **
 **	force a shutdown after a test complete to guarantee shutdown
 **	which doesn't always seem to happen with useprocess=false
 **
 */
public class shutdown
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
 
	static String shutdownurl;
	static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	static String systemHome;

	public static void main(String[] args) throws SQLException,
		InterruptedException, Exception 
    {
		systemHome = args[0];
		shutdownurl = args[1];
		try
		{
		    doit();
		}
		catch(Exception e)
		{
		    System.out.println("Exception in shutdown: " + e);
		}
	}

	public static void doit() throws SQLException,
		InterruptedException, Exception 
	{
		Connection conn = null;
		boolean finished = false;	
		Date d = new Date();

        Properties sp = System.getProperties();
        if (systemHome == null)
        {
		    systemHome = sp.getProperty("user.dir") + File.separatorChar +
			"testCSHome";
        	sp.put("derby.system.home", systemHome);
        	System.setProperties(sp);
        }
		boolean useprocess = true;
		String up = sp.getProperty("useprocess");
		if (up != null && up.equals("false"))
			useprocess = false;		

        PrintStream stdout = System.out;
    	PrintStream stderr = System.err;

		Class.forName(driver).newInstance();

		try 
		{
			conn = DriverManager.getConnection(shutdownurl);
		} 
		catch (SQLException  se) 
		{
		    if (se.getSQLState().equals("08006"))
		    {
		        // It was already shutdown
		        //System.out.println("Shutdown with: " + shutdownurl);
		    }
		    else 
			{
				System.out.println("shutdown failed for " + shutdownurl);
				JDBCDisplayUtil.ShowException(System.out, se);
				System.exit(1);
	        }
		}
    }
}
