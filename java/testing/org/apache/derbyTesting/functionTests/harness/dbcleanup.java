/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.dbcleanup

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.harness;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.io.*;
import java.util.*;
import java.lang.Long;
import java.util.Vector;

import org.apache.derby.tools.JDBCDisplayUtil;

/*
 **
 ** dbcleanup
 **
 ** Preliminary version:
 **	gets rid of all the items in a database except those that
 **	are present when a fresh database is created.  There are
 **	some gaps still-- sync objects, and I have not done SYSFILES.
 **	I have probably missed other things as well.  At present this
 **	is hardwired for jdbc:derby:wombat, the focus of our
 **	attention in the embedded tests.
 **
 */
public class dbcleanup {

	static String dbURL = "jdbc:derby:wombat";
	static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	static boolean dbIsDirty = false;

	int thread_id;
	int ind = 0;

	public static void main(String[] args) throws SQLException, IOException,
		InterruptedException, Exception {
		doit(true);
	}

	public static void doit(boolean dbIsNew) throws SQLException, IOException,
		InterruptedException, Exception {

		Connection conn = null;
		Statement s = null;
		ResultSet rs = null;
		boolean finished = false;	
		Date d = new Date();

        	Properties dbclProps = System.getProperties();
		String systemHome = dbclProps.getProperty("user.dir") + File.separatorChar +
			"testCSHome";
        	dbclProps.put("derby.system.home", systemHome);
        	System.setProperties(dbclProps);

		boolean useprocess = true;
		String up = dbclProps.getProperty("useprocess");
		if (up != null && up.equals("false"))
			useprocess = false;		
		
    		PrintStream stdout = System.out;
    		PrintStream stderr = System.err;

		Class.forName(driver).newInstance();

		if (dbIsNew) {
		try {
			conn = DriverManager.getConnection(dbURL +
				";create=true");
			conn.setAutoCommit(false);
			System.out.println("created " + dbURL + " " + d);
//FIX: temporarily we will always cleanup, so skip the shutdown
//			conn.close();
			// shutdown required only if 2 processes access database
//			if (useprocess) doshutdown();
		//	return;
		} catch (SQLException  se) {
			System.out.println("connect failed for " + dbURL);
			JDBCDisplayUtil.ShowException(System.out, se);
			System.exit(1);
		}
		}
		else {
		try {
			conn = DriverManager.getConnection(dbURL);
			conn.setAutoCommit(false);
			System.out.println("connected to " + dbURL + " " + d);
		} catch (SQLException  se) {
			System.out.println("connect failed for " + dbURL);
			JDBCDisplayUtil.ShowException(System.out, se);
			System.exit(1);
		}
		}

		d = new Date();
		System.out.println("dbcleanup starting: " + d);

		Enumeration schemalist = null;
		Enumeration list = null;
		Vector schemavec = new Vector();
		Vector tablevec = null;
		// get a list of the user schemas
		try {
			s = conn.createStatement();
			rs = s.executeQuery( " select schemaname from sys.sysschemas " +
				" where schemaname <> 'SYS'"); 
			while (rs.next()) { 
				schemavec.addElement(new String(rs.getString(1)));
			}
			rs.close();
			if (schemavec.size() > 1) {
				// there is at least one schema to clean up
				dbIsDirty = true;
			}
		} catch (SQLException  se) {
			System.out.println("select schemas: FAIL -- unexpected exception:");
			JDBCDisplayUtil.ShowException(System.out, se);
			System.exit(1);
		}

		// for each user schema, drop the objects
		String schema = null;
		String n = null;
		boolean viewdependencyFound = false;
		boolean tabledependencyFound = false;
		Vector viewvec = null;
		int count = 0;
		for (schemalist = schemavec.elements(); schemalist.hasMoreElements();) {
			schema = (String)schemalist.nextElement();
			for (viewdependencyFound = true; viewdependencyFound;){
				viewdependencyFound = false;
				viewvec = findTables(conn, s, 'V', schema);
				//for (list = viewvec.elements(); list.hasMoreElements();)
				//	System.out.println("\t" + list.nextElement());
				if (viewvec.size() > 0) {
					System.out.println("schema " + schema);
					viewdependencyFound = dropTables(conn, s, viewvec, "view");
				}
			}

			for (tabledependencyFound = true; tabledependencyFound;){
				tabledependencyFound = false;
				tablevec = findTables(conn, s, 'T', schema);
				if (tablevec.size() > 0) {
					System.out.println("schema " + schema);
					tabledependencyFound = 
						dropTables(conn, s, tablevec, "table");
				}
			}

			Vector stmtvec = new Vector();
			try {
				rs = s.executeQuery( " select stmtname " +
					" from sys.sysstatements t, sys.sysschemas  s " +
					" where t.schemaid = s.schemaid " +
					" and s.schemaname = '" + schema + "'");
				for (count = 0; rs.next(); count++) { 
					dbIsDirty = true;
					stmtvec.addElement(new String(rs.getString(1)));
				}
				rs.close();
			} catch (SQLException  se) {
				System.out.println("select statements: FAIL -- unexpected exception:");
				JDBCDisplayUtil.ShowException(System.out, se);
				System.exit(1);
			}

			if (count > 1) {
			try {
				System.out.println("schema " + schema);
				System.out.println("dropping leftover statements: ");
				for (list = stmtvec.elements(); list.hasMoreElements();) {
					n = (String)list.nextElement();
					s.execute("drop statement " + n);
					conn.commit();
					System.out.println("\t" + n);
				}
			} catch (SQLException  se) {
				System.out.println("drop statement: FAIL -- unexpected exception:");
				JDBCDisplayUtil.ShowException(System.out, se);
				System.exit(1);
			}
			}
		}
		// drop every user schema except APP
		if (schemavec.size() > 1) {
		System.out.println("dropping extra user schemas: ");
		schemalist = null;
		for (schemalist = schemavec.elements(); schemalist.hasMoreElements();) {
			schema = (String)schemalist.nextElement();
			if (schema.equals("APP")) continue;
			if (schema == null) {
				System.out.println("null schema in schemalist");
				continue;
			}
			try {
				System.out.println("\t" + schema);
				s.execute("drop schema \"" + schema + "\"");
			} catch (SQLException  se) {
				System.out.println("drop schema: FAIL -- unexpected exception:");
				JDBCDisplayUtil.ShowException(System.out, se);
				System.exit(1);
			}
		}
		}
		// drop all method aliases
		dropAliases(conn, 'M');
		dropAliases(conn, 'C');

		// DEBUG: help figure out what's going on with extra entries in sysdepends
		try {
			rs = s.executeQuery("select count (*) from sys.sysdepends");
			if (rs.next()) {
				int i = rs.getInt(1);
				if (i > 0)
					System.out.println("found " + i + " leftover dependencies");
			}
		} catch (SQLException  se) {
			System.out.println("drop schema: FAIL -- unexpected exception:");
			JDBCDisplayUtil.ShowException(System.out, se);
			System.exit(1);
		}

		// shutdown required only if 2 processes access database
		if (useprocess) doshutdown();
		//conn.close();
		d = new Date();
		System.out.println("dbcleanup finished: " + d);
	}

	static void doshutdown() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(dbURL +
				";shutdown=true");
		} catch (SQLException  se) {
			if (se.getSQLState().equals("08006")){
				System.out.println("shutting down " + dbURL);
			}
			else {
				System.out.println("shutdown failed for " + dbURL);
				JDBCDisplayUtil.ShowException(System.out, se);
				System.exit(1);
			}
		}
	}

	static boolean dropTables(Connection conn, Statement s, Vector tablevec,
		String tabletype) throws Exception {

		boolean dependencyFound = false;
		String n = null;

		String objtype = null;
		System.out.println("dropping " + tabletype + "(s)");

		for (Enumeration list = tablevec.elements(); list.hasMoreElements();) {
			n = (String)list.nextElement();
			try {
				s.execute("drop " + tabletype + " " + n);
				conn.commit();
				System.out.println("\t" + n);
			} catch (SQLException  se) {
				if (se.getSQLState().equals("X0Y25")){
					dependencyFound=true;
					//System.out.println("error X0Y25: " + se.getMessage());
					System.out.println(n + " not droped due to dependency, will retry a bit later");
				}
				else if (se.getSQLState().equals("X0Y23")){
					dependencyFound=true;
					//System.out.println("error X0Y23: " + se.getMessage());
					System.out.println(n + " not droped due to dependency, will retry a bit later");
				}
				else {
					System.out.println("drop table: FAIL -- unexpected exception:");
					JDBCDisplayUtil.ShowException(System.out, se);
					System.exit(1);
		//FIX exits
				}
			}
		}
		return(dependencyFound);
	}

	static  Vector findTables(Connection conn, Statement s, char c, String schema) throws Exception {

		ResultSet rs = null;
		Vector tableviewvec = new Vector();

		try {
			rs = s.executeQuery( " select t.tablename " +
				" from sys.systables t, sys.sysschemas  s " +
				" where t.schemaid = s.schemaid " +
				" and t.tabletype = '" + c + "'" +
				" and s.schemaname = '" + schema + "'" );
			while (rs.next()) { 
				dbIsDirty = true;
				tableviewvec.addElement(new String(rs.getString(1)));
			}
			rs.close();
		} catch (SQLException  se) {
			System.out.println("select tables: FAIL -- unexpected exception:");
			JDBCDisplayUtil.ShowException(System.out, se);
			System.exit(1);
		//FIX exits
		}
		return(tableviewvec);
	}

	static void dropAliases (Connection conn, char aliastype) throws Exception {
		
		ResultSet rs = null;
		Statement s = null;
		String typestring = null;
		Vector aliasvec = new Vector();
		String n = null;
		int count = 0;

		if (aliastype == 'M') typestring = "method";
		else if (aliastype == 'C') typestring = "class";

		try {
			s = conn.createStatement();
			rs = s.executeQuery("select alias, aliastype from sys.sysaliases " +
				" where systemalias = false " + 
				" and aliastype = '" + aliastype + "'");
			for (count = 0; rs.next(); count++) {
				dbIsDirty = true;
				aliasvec.addElement(new String(rs.getString(1)));
			}
			rs.close();
			conn.commit();
		} catch (SQLException  se) {
			System.out.println("drop alias: FAIL -- unexpected exception:");
			JDBCDisplayUtil.ShowException(System.out, se);
			System.exit(1);
		}

		if (count > 1) {
		System.out.println("dropping user aliases, type " + typestring + ": ");
		for (Enumeration list = aliasvec.elements(); list.hasMoreElements();) {
			n = (String)list.nextElement();
			try {
				s.execute("drop " + typestring + " alias " + n);
			} catch (SQLException  se) {
				System.out.println("drop alias: FAIL -- unexpected exception:");
				JDBCDisplayUtil.ShowException(System.out, se);
				System.exit(1);
			}
			conn.commit();
			System.out.println("\t" + n);
		}
		}
	}
}
