/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.JitTest

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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * DERBY-1327
 * Identity column can be created with wrong and very large start with 
 * value with "J2RE 1.5.0 IBM Windows 32 build pwi32dev-20060412 (SR2)" 
 * with JIT on
 */
public class JitTest {

  public static void main (String args[]) 
  { 
    try {
    	/* Load the JDBC Driver class */
    	// use the ij utility to read the property file and
    	// make the initial connection.
    	ij.getPropertyArg(args);
    	Connection conn = ij.startJBMS();

    	System.out.println("Start JitTest");
    	//add tests specific to a jit issue
    	testDerby1327BadStartWithForAutoIncColumn(conn);
    	conn.close();
    } catch (Exception e) {
    	System.out.println("FAIL -- unexpected exception "+e);
    	JDBCDisplayUtil.ShowException(System.out, e);
      	e.printStackTrace(System.out);
    }
  }
  
  /**
   * After some number of table creations with JIT turned on, the START WITH  
   * value for the table being created and all the ones already created gets 
   * mysteriously changed with pwi32dev-20060412 (SR2)
   * 
   * @throws Exception
   */
  public static void testDerby1327BadStartWithForAutoIncColumn(Connection conn) 
  	throws Exception
  {
	conn.setAutoCommit(false);
		Statement stmt = null;		

		dropAllAppTables(conn);
		System.out.println("Create tables until we get a wrong Start with value");
		stmt = conn.createStatement();

		// numBadStartWith will be changed if any columns get a bad start with value.
		int numBadStartWith = 0; 
		String createTableSQL = null;
		try {
			// create 200 tables.  Break out if we get a table that has a bad
			// start with value.
			for (int i = 0; (i < 200) && (numBadStartWith == 0); i++)
			{
				String tableName = "APP.MYTABLE" + i;
			    createTableSQL = "CREATE TABLE " + tableName + 
				"  (ROLEID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY ("+
				"START WITH 2, INCREMENT BY 1), INSTANCEID INTEGER, STATUS"+
				" INTEGER, LOGICAL_STATE INTEGER, LSTATE_TSTAMP  TIMESTAMP,"+
				" UPDT_TSTAMP TIMESTAMP, TSTAMP TIMESTAMP,"+
				" CLALEVEL1_CLALEVEL2_CLALEVEL2ID VARCHAR(255),  "+
				"CLALEVEL1_CLALEVEL2_CLALEVEL3_CLALEVEL3ID VARCHAR(255))";
				
				stmt.executeUpdate(createTableSQL);
				conn.commit();
                numBadStartWith = checkBadStartWithCols(conn,2);
				if (numBadStartWith > 0)
					break;
			}
		} catch (SQLException se)
		{
			System.out.println("Failed on " + createTableSQL);
			JDBCDisplayUtil.ShowSQLException(System.out,se);

		}
		
		if (numBadStartWith == 0)
		{
			System.out.println("PASS: All 200 tables created without problems");
			dropAllAppTables(conn);
		}
		stmt.close();
		conn.rollback();
  } 


  /**
   * Check that all tables in App do not have a an autoincrementstart value
   * greater tan maxautoincrementstart
   * @param conn
   * @param maxautoincrementstart  Maximum expected autoincrementstart value
   * @return number of columns with bad autoincrementstart value
   */
  	private static int checkBadStartWithCols(Connection conn, int
  	  		maxautoincrementstart) throws Exception
  	{
  		Statement stmt = conn.createStatement();
  		ResultSet rs =stmt.executeQuery("select count(autoincrementstart) from"+
  				" sys.syscolumns c, sys.systables t, sys.sysschemas s WHERE"+
				" t.schemaid =  s.schemaid and CAST(s.schemaname AS VARCHAR(128))= 'APP' and"+
				" autoincrementstart > " +  maxautoincrementstart);

  		rs.next();
  		int numBadStartWith = rs.getInt(1);
  		if (numBadStartWith > 0)
  			System.out.println(numBadStartWith + " columns have bad START WITH VALUE");
  		rs.close();
  		
  		if (numBadStartWith > 0)
  		{
  			rs =stmt.executeQuery("select tablename, columnname,"+
  					" autoincrementstart from sys.syscolumns c, sys.systables t,"+
					" CAST(sys.sysschemas AS VARCHAR(128)) s WHERE t.schemaid = s.schemaid and"+
					" CAST(s.schemaname AS VARCHAR(128)) = 'APP' and autoincrementstart > 2 ORDER"+
					" BY tablename");
  			while (rs.next())
  			{
  				System.out.println("Unexpected start value: " +
  								   rs.getLong(3) + 
  								   " on column " + rs.getString(1) +
  								   "(" + rs.getString(2) + ")");
  				
  				
  			}
  		}
         return numBadStartWith;
  	}

  	/**
       * Drop all tables in schema APP
  	 * @param conn
  	 * @throws SQLException
  	 */
  	private  static void dropAllAppTables(Connection conn) throws SQLException
  	{
  		Statement stmt1 = conn.createStatement();
  		Statement stmt2 = conn.createStatement();
  		System.out.println("Drop all tables in APP schema");
  		ResultSet rs = stmt1.executeQuery("SELECT tablename from sys.systables"+
  				" t, sys.sysschemas s where t.schemaid = s.schemaid and"+
				" CAST(s.schemaname AS VARCHAR(128)) = 'APP'");

  		while (rs.next())
  		{
  			String tableName = rs.getString(1);
  			
  			try {
  				stmt2.executeUpdate("DROP TABLE " + tableName);
  			}
  			catch (SQLException se)
  			{
  				System.out.println("Error dropping table:" + tableName);
  				se.printStackTrace();
  				continue;
  			}
  		}
  	}
}
