/*

   Derby - Class org.apache.derbyTesting.functionTests.util.Triggers

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

package org.apache.derbyTesting.functionTests.util;

import org.apache.derby.iapi.db.*;
import java.sql.*;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Methods for testing triggers
 */
public class Triggers
{

	// used for threading test
	static TriggerThread triggerThread;

	public Triggers()
	{
	}

	public static String triggerFiresMinimal(String string) throws Throwable
	{
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		System.out.println("TRIGGER: " + "<"+string+">");
		return "";
	}

	public static String triggerFires(String string) throws Throwable
	{
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		System.out.println("TRIGGER: " + "<"+string+"> on statement "+tec.getEventStatementText());
		printTriggerChanges();
		return "";
	}

	public static void triggerFiresRep(String string) throws Throwable
	{	
		String preface = isReplicationTarget() ? 
						"TARGET: " : isReplicationSource() ?
								"SOURCE: " : "<not in rep context>" ;
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		System.out.println(preface + "<"+string+"> on statement "+tec.getEventStatementText());
		printTriggerChanges();
	}

	public static boolean isReplicationTarget() throws Throwable
	{
		String repTargetProp = PropertyInfo.getDatabaseProperty("derby.engineType");
		return (repTargetProp != null && repTargetProp.equals("8"));
	}
	public static boolean isReplicationSource() throws Throwable
	{
		String repSourceProp = PropertyInfo.getDatabaseProperty("derby.engineType");
		return (repSourceProp != null && repSourceProp.equals("4"));
	}

	public static int doNothingInt() throws Throwable
	{
		return 1;
	}

	public static void doNothing() throws Throwable
	{}

	public static int doConnCommitInt() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.commit();
		return 1;
	}

	public static void doConnCommit() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.commit();
	}
			
	public static void doConnRollback() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.rollback();
	}

	public static void doConnectionSetIsolation() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.setTransactionIsolation(conn.TRANSACTION_SERIALIZABLE);
	}
			
	public static int doConnStmtIntNoRS(String text) throws Throwable
	{
		doConnStmtNoRS(text);
		return 1;
	}
	public static void doConnStmtNoRS(String text) throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = conn.createStatement();
		stmt.execute(text);
	}

	public static int doConnStmtInt(String text) throws Throwable
	{
		doConnStmt(text);
		return 1;
	}
	public static void doConnStmt(String text) throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = conn.createStatement();
		if (stmt.execute(text))
		{
			ResultSet rs = stmt.getResultSet();
			while (rs.next())
			{}
			rs.close();
		}
		stmt.close();
		conn.close();
	}

	public static void doConnStmt(String text, boolean targetOnly) throws Throwable
	{
		if (isReplicationTarget() == targetOnly)
		{
			System.out.println("Trigger Executing: "+text);
			doConnStmt(text);
		}
	}

	public static void workUnitFires() throws Throwable
	{
		System.out.println((isReplicationTarget() ? 
						"TARGET: workUnitFires" : isReplicationSource() ?
								"SOURCE: workUnitFires" : 
								"<error, not in rep context?!?>"));
	}

	public static String printTriggerInfo() throws Throwable
	{
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		int eventType = tec.getEventType();
		String eventName;

		switch (eventType)
		{
			case TriggerExecutionContext.UPDATE_EVENT: 
					eventName = "UPDATE";
					break;

			case TriggerExecutionContext.DELETE_EVENT: 
					eventName = "DELETE";
					break;

			case TriggerExecutionContext.INSERT_EVENT: 
					eventName = "INSERT";
					break;
			default:
				throw new Throwable("unexpected event type: "+eventType);
		}			

		StringBuffer strbuf = new StringBuffer("TriggerInformation\n------------------\n");
		strbuf.append("getEventStatetmentText(): "+tec.getEventStatementText());
		strbuf.append("\ngetEventType(): "+eventName);
		strbuf.append("\ngetModifiedColumns(): {");

		String[] modifiedCols = tec.getModifiedColumns();
		if (modifiedCols == null)
		{
			strbuf.append(" <all> }");
		}
		else
		{
			for (int i = 0; i < modifiedCols.length; i++)
			{
				if (i > 0)
					strbuf.append(", ");
				strbuf.append(modifiedCols[i]);
			}
			strbuf.append("}");
		}
		strbuf.append("\n");
		strbuf.append(getModifiedColumnsCheckInfo(tec));
		System.out.println(strbuf.toString());
		printTriggerChanges();
		return "";
	}

	private static String getModifiedColumnsCheckInfo(TriggerExecutionContext tec) throws Throwable
	{
		ResultSet rs = (tec.getNewRowSet() == null) ?
					tec.getOldRowSet() :
					tec.getNewRowSet();

		ResultSetMetaData rsmd = rs.getMetaData();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount();

		StringBuffer msg = new StringBuffer("wasColumnModified() on each column\n\t");
		for (int i=1; i<=numCols; i++) 
		{
			if (i > 1) 
			{
				msg.append("\n\t");
			}
			msg.append(rsmd.getColumnLabel(i));
			msg.append(":\t");
			boolean wasModified = tec.wasColumnModified(rsmd.getColumnLabel(i));
			if (wasModified != tec.wasColumnModified(i))
			{
				msg.append("ERROR: tec.wasColumnModifed(String) = "+wasModified);
				msg.append("and tec.wasColumnModifed(int) = "+(!wasModified));
			}
			else
			{
				msg.append(wasModified);
			}
		}
		msg.append("\n");
		return msg.toString();
	}

	// used for performance numbers
	public static void zipThroughTriggerResultSets() throws SQLException
	{
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		zipThroughRs(tec.getOldRowSet());
		zipThroughRs(tec.getNewRowSet());
	}

	// used for performance numbers
	static void zipThroughRs(ResultSet s) throws SQLException
	{
		if (s == null)
			return;
		
		while (s.next()) ;
	}

	public static void barfOnInsertEquals(int value) throws Throwable
	{
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		ResultSet rs = tec.getNewRowSet();
		while (rs.next())
		{
			if (rs.getInt(1) == value)
			{
				throw new SQLException("barfing because of "+value+" in BeforeResultSet","38001");
			}
		}
	}

	private static void printTriggerChanges() throws Throwable
	{
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		System.out.println("BEFORE RESULT SET");
		dumpRS(tec.getOldRowSet());
		System.out.println("\nAFTER RESULT SET");
		dumpRS(tec.getNewRowSet());
	}

	// lifted from the metadata test	
	private static void dumpRS(ResultSet s) throws SQLException 
	{
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		ResultSetMetaData rsmd = s.getMetaData();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount();

		if (numCols <= 0) 
		{
			System.out.println("(no columns!)");
			return;
		}

		StringBuffer heading = new StringBuffer("\t ");
		StringBuffer underline = new StringBuffer("\t ");

		int len;
		// Display column headings
		for (int i=1; i<=numCols; i++) 
		{
			if (i > 1) 
			{
				heading.append(",");
				underline.append(" ");
			}
			len = heading.length();
			heading.append(rsmd.getColumnLabel(i));
			len = heading.length() - len;
			for (int j = len; j > 0; j--)
			{
				underline.append("-");
			}
		}
		System.out.println(heading.toString());
		System.out.println(underline.toString());
		
	
		StringBuffer row = new StringBuffer();
		// Display data, fetching until end of the result set
		while (s.next()) 
		{
			row.append("\t{");
			// Loop through each column, getting the
			// column data and displaying
			for (int i=1; i<=numCols; i++) 
			{
				if (i > 1) row.append(",");
				row.append(s.getString(i));
			}
			row.append("}\n");
		}
		System.out.println(row.toString());
		s.close();
	}

	// WARNING: will deadlock unless on a separate
	// connection
	public static void notifyDMLDone() throws Throwable
	{
		if (triggerThread == null)
		{
			System.out.println("ERROR: no triggerThread object, has beginInvalidRefToTECTest() been executed?");
		}
		else
		{
			triggerThread.goForIt();
			while (!triggerThread.done())
			{
				try {Thread.sleep(1000L); } catch (InterruptedException e) {}
			}
			triggerThread = null;
		}
	}

	public static void simpleTrigger() throws Throwable
	{
		Long ai;
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		
		PreparedStatement lastai = conn.prepareStatement("values org.apache.derby.iapi.db.ConnectionInfo::lastAutoincrementValue('APP', 'TRIGTEST', 'S1') ");
		
		ResultSet rs = lastai.executeQuery();
		rs.next();
		ai = (Long)rs.getObject(1);
		rs.close();
		System.out.println("autoincrement value is " + ai);
	}
	/**
	 */
	public static void insertIntoTab1(Integer level)
	      throws Throwable	
	{
		Long entry, exit;
		int levelValue = level.intValue();
		long entryValue, exitValue;

		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		
		PreparedStatement lastai = conn.prepareStatement("values (org.apache.derby.iapi.db.ConnectionInfo::lastAutoincrementValue('APP','TAB1','S1'))");
		ResultSet rs = lastai.executeQuery();
		rs.next();
		entry = (Long)rs.getObject(1);
		rs.close();

		System.out.println("coming in with level = " + levelValue + 
						   " and autoincrement = " + entry.longValue());
		
		if (levelValue == 3)
			return;

		
		PreparedStatement ps = 
			conn.prepareStatement("insert into tab1 (lvl) values (?)");
		// insert one higher value into tab1.
		ps.setInt(1, levelValue + 1);
		ps.execute();
		
		rs = lastai.executeQuery();
		rs.next();
		exit = (Long)rs.getObject(1);
		rs.close();

		// When we get out of this trigger, subsequent firings of the trigger by the
		// insert statement in this routine shouldn't be returned by
		// lastautoincrement value.

		System.out.println("going out with level = " + levelValue + 
						   " and autoincrement = " + exit.longValue());

	}


	public static void aiWorkUnit(String s)
	       	      throws Throwable	
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		String query = "insert into watab (s) values (?)";
		PreparedStatement ps = conn.prepareStatement(query);
		System.out.println("in work unit with s=" + s);
		ps.setString(1, s);
		ps.execute();
	}

	public static String beginInvalidRefToTECTest() throws Throwable
	{
		triggerThread = new TriggerThread();
		triggerThread.start();
		return "";
	}

	public static long returnPrimLong(long  x)
	{
		return x;
	}

	public static Long returnLong(Long x)
	{
		return x;
	}

	// used to test binding
	public static void allTypesMethodPrimitive
	(
		int 		i,
		byte		by,	
		short		s,
		long		l,
		String		s1,
		String		s2,
		String		s3,
		boolean		bl,
		double		db,
		float		f1,
		double		db2,
		Date		dt,
		Time		t,
		Timestamp	ts,
		byte[]		b1,
		byte[]		b2,
		byte[]		b3,
		double		bd1,
		double		bd2,
		BigInteger	bi
	)
	{
	}

	public static void allTypesMethodObject
	(
		Integer		i,
		Integer		by,	
		Integer		s,
		Long		l,
		String		s1,
		String		s2,
		String		s3,
		Boolean		bl,
		Double		db,
		Float		f1,
		Double		db2,
		Date		dt,
		Time		t,
		Timestamp	ts,
		byte[]		b1,
		byte[]		b2,
		byte[]		b3,
		BigDecimal	bd1,
		BigDecimal	bd2,
		BigInteger	bi
	)
	{
	}

}

// class for testing valid tec accesses	
class TriggerThread extends Thread
{	
	private TriggerExecutionContext tec;
	private ResultSet rs;
	private boolean start; 
	private boolean done; 

	public TriggerThread() throws Throwable
	{
		this.tec = Factory.getTriggerExecutionContext();
		if (tec == null)
		{
			System.out.println("ERROR: no tec found, no trigger appears to be active");
			return;
		}
	
		this.rs = (tec.getNewRowSet() == null) ?
			tec.getOldRowSet() :
			tec.getNewRowSet();
	}

	public void goForIt()
	{
		start = true;
	}

	public boolean done()
	{
		return done;
	}

	public void run() 
	{
		boolean gotException = false;
	
		int i;	
		for (i = 0; !start && i < 1000; i++)
		{
			try {Thread.sleep(50L); } catch (InterruptedException e) {}
		} 
		if (i == 1000)
		{
			System.out.println("ERROR: start never received");
			return;
		}
		// let the other thread get to its pause point
		try {Thread.sleep(5000L); } catch (InterruptedException e) {}

		System.out.println("...nested thread running using expired tec");
		try
		{
			System.out.println("...trying to loop through stale result set");
			Triggers.zipThroughRs(rs);
		} catch (SQLException e)
		{
			gotException = true;	
			System.out.println("Got expected exception: "+e);
		}
		if (!gotException)
		{
			System.out.println("ERROR: no exception when trying to do next on stale ResultSet");
		}
		gotException = false;

		try
		{	
			tec.getNewRowSet();
		} catch (SQLException e)
		{
			gotException = true;	
			System.out.println("Got expected exception: "+e);
		}

		if (!gotException)
		{
			System.out.println("ERROR: getNewRowSet() didn't throw an exception on stale tec");
		}

		gotException = false;

		try
		{	
			tec.getOldRowSet();
		} catch (SQLException e)
		{
			gotException = true;	
			System.out.println("Got expected exception: "+e);
		}

		if (!gotException)
		{
			System.out.println("ERROR: getOldRowSet() didn't throw an exception on stale tec");
		}

		// signal that we are done
		done = true;
	}
}
