/*

   Derby - Class org.apache.derby.tools.JDBCDisplayUtil

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

package org.apache.derby.tools;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.security.AccessController;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import java.util.Properties;
import java.util.Enumeration;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.derby.iapi.tools.i18n.LocalizedResource;

import org.apache.derby.impl.tools.ij.ijException;

/**
	
	This class contains utility methods for displaying JDBC objects and results.
	
	<p>
	All of the methods are static. The output stream
	to write to is always passed in, along with the
	JDBC objects to display.

 */
public class JDBCDisplayUtil {

	// used to control display
	static final private int MINWIDTH = 4;
	static private int maxWidth = 128;
    static private boolean showSelectCount = false;

    static {
        // initialize the locale support functions to default value of JVM 
        LocalizedResource.getInstance();
    }

	//-----------------------------------------------------------------
	// Accessors

    public  static  boolean getShowSelectCount() { return showSelectCount; }
    public  static  void    setShowSelectCount( boolean newValue ) { showSelectCount = newValue; }

	//-----------------------------------------------------------------
	// Methods for displaying and checking errors

	/**
		Print information about the exception to the given PrintWriter.
		For non-SQLExceptions, does a stack trace. For SQLExceptions,
		print a standard error message and walk the list, if any.

		@param out the place to write to
		@param e the exception to display
	 */
	static public void ShowException(PrintWriter out, Throwable e) {
		if (e == null) return;

		if (e instanceof SQLException)
			ShowSQLException(out, (SQLException)e);
		else
			e.printStackTrace(out);
	}

	/**
		Print information about the SQL exception to the given PrintWriter.
		Walk the list of exceptions, if any.

		@param out the place to write to
		@param e the exception to display
	 */
	static public void ShowSQLException(PrintWriter out, SQLException e) {
		String errorCode;

		if (getSystemBoolean("ij.showErrorCode")) {
			errorCode = LocalizedResource.getMessage("UT_Error0", LocalizedResource.getNumber(e.getErrorCode()));
		}
		else {
			errorCode = "";
		}

		while (e!=null) {
			String p1 = mapNull(e.getSQLState(),LocalizedResource.getMessage("UT_NoSqlst"));
			String p2 = mapNull(e.getMessage(),LocalizedResource.getMessage("UT_NoMessa"));
			out.println(LocalizedResource.getMessage("UT_Error012", p1, p2,errorCode));
			doTrace(out, e);
			e=e.getNextException();
		}
	}

	/**
		Print information about the SQL warnings for the connection
		to the given PrintWriter.
		Walks the list of exceptions, if any.

		@param out the place to write to
		@param theConnection the connection that may have warnings.
	 */
	static public void ShowWarnings(PrintWriter out, Connection theConnection) {
	    try {
		// GET CONNECTION WARNINGS
		SQLWarning warning = null;

		if (theConnection != null) {
			ShowWarnings(out, theConnection.getWarnings());
		}

		if (theConnection != null) {
			theConnection.clearWarnings();
		}
	    } catch (SQLException e) {
			ShowSQLException(out, e);
	    }
	} // ShowWarnings

	/**
		@param out the place to write to
		@param warning the SQLWarning
	*/
	static public void ShowWarnings(PrintWriter out, SQLWarning warning) {
		while (warning != null) {
			String p1 = mapNull(warning.getSQLState(),LocalizedResource.getMessage("UT_NoSqlst_7"));
			String p2 = mapNull(warning.getMessage(),LocalizedResource.getMessage("UT_NoMessa_8"));
			out.println(LocalizedResource.getMessage("UT_Warni01", p1, p2));
			warning = warning.getNextWarning();
		}
	}

	/**
		Print information about the SQL warnings for the ResultSet
		to the given PrintWriter.
		Walk the list of exceptions, if any.
	
		@param out the place to write to
		@param rs the ResultSet that may have warnings on it
	 */
	static public void ShowWarnings(PrintWriter out, ResultSet rs) {
	    try {
		// GET RESULTSET WARNINGS
		SQLWarning warning = null;

		if (rs != null) {
			ShowWarnings(out, rs.getWarnings());
		}

		if (rs != null) {
			rs.clearWarnings();
		}
	    } catch (SQLException e) {
			ShowSQLException(out, e);
	    }
	} // ShowResultSetWarnings

	/**
		Print information about the SQL warnings for the Statement
		to the given PrintWriter.
		Walks the list of exceptions, if any.

		@param out the place to write to
		@param s the Statement that may have warnings on it
	 */
	static public void ShowWarnings(PrintWriter out, Statement s)
	{
	    try {
		// GET STATEMENT WARNINGS
		SQLWarning warning = null;

		if (s != null) {
			ShowWarnings(out, s.getWarnings());
		}

		if (s != null) {
			s.clearWarnings();
		}
	    } catch (SQLException e) {
			ShowSQLException(out, e);
	    }
	} // ShowStatementWarnings

	//-----------------------------------------------------------------
	// Methods for displaying and checking results

	// REMIND: make this configurable...
	static final private int MAX_RETRIES = 0;

	/**
		Pretty-print the results of a statement that has been executed.
		If it is a select, gathers and prints the results.  Display
		partial results up to the first error.
		If it is not a SELECT, determine if rows were involved or not,
		and print the appropriate message.

		@param out the place to write to
		@param stmt the Statement to display
		@param conn the Connection against which the statement was executed

		@exception SQLException on JDBC access failure
	 */
	static public void DisplayResults(PrintWriter out, Statement stmt, Connection conn )
		throws SQLException
	{
		indent_DisplayResults( out, stmt, conn, 0, null, null);			
	}

	static private void indent_DisplayResults
	(PrintWriter out, Statement stmt, Connection conn, int indentLevel,
	 int[] displayColumns, int[] displayColumnWidths)
		throws SQLException {

		checkNotNull(stmt, "Statement");

		ResultSet rs = stmt.getResultSet();
		if (rs != null) {
			indent_DisplayResults(out, rs, conn, indentLevel, 
								  displayColumns, displayColumnWidths);
			rs.close(); // let the result set go away
		}
		else {
			DisplayUpdateCount(out,stmt.getUpdateCount(), indentLevel);
		}

		ShowWarnings(out,stmt);
	} // DisplayResults

	/**
		@param out the place to write to
		@param count the update count to display
		@param indentLevel number of tab stops to indent line
	 */
	static void DisplayUpdateCount(PrintWriter out, int count, int indentLevel ) {
		if (count == 1) {
			indentedPrintLine( out, indentLevel, LocalizedResource.getMessage("UT_1RowInserUpdatDelet"));
		}
		else if (count >= 0) {
			indentedPrintLine( out, indentLevel, LocalizedResource.getMessage("UT_0RowsInserUpdatDelet", LocalizedResource.getNumber(count)));
		}
		else {
			indentedPrintLine( out, indentLevel, LocalizedResource.getMessage("UT_StateExecu"));
		}
	}

	/**
	    Calculates column display widths from the default widths of the
	    result set.
	 */
	static private int[] getColumnDisplayWidths(ResultSetMetaData rsmd, int[] dispColumns,
												boolean localizedOutput)
		throws SQLException {
		int count = (dispColumns == null) ? rsmd.getColumnCount() : dispColumns.length;
		int[] widths = new int[count];

		for(int i=0; i<count; i++) {
			int colnum = (dispColumns == null) ? (i + 1) : dispColumns[i];
			int dispsize = localizedOutput
				? LocalizedResource.getInstance().getColumnDisplaySize(rsmd, colnum)
                : rsmd.getColumnDisplaySize(colnum);
			widths[i] = Math.min(maxWidth,
				Math.max((rsmd.isNullable(colnum) == ResultSetMetaData.columnNoNulls)?
				0 : MINWIDTH, dispsize));
		}
		return widths;
	}


    /**
       @param out the place to write to
       @param resultSets list of <code>ResultSet</code>s to display
       @param conn the connection against which the <code>ResultSet</code>s
            were retrieved
       @param displayColumns column numbers to display, <code>null</code> if all
       @param displayColumnWidths column widths, in characters, if
            <code>displayColumns</code> is specified

       @exception SQLException on JDBC access failure
    */
    static public void DisplayMultipleResults(PrintWriter out, List<ResultSet> resultSets,
                                              Connection conn,
                                              int[] displayColumns,
                                              int[] displayColumnWidths)
        throws SQLException
    {
        indent_DisplayResults( out, resultSets, conn, 0, displayColumns,
                               displayColumnWidths);
    }

    /**
       @param out the place to write to
       @param rs the <code>ResultSet</code> to display
       @param conn the connection against which the <code>ResultSet</code>
            was retrieved
       @param displayColumns column numbers to display, <code>null</code> if all
       @param displayColumnWidths column widths, in characters, if
            <code>displayColumns</code> is specified

       @exception SQLException on JDBC access failure
    */
	static public void DisplayResults(PrintWriter out, ResultSet rs, Connection conn,
									  int[] displayColumns, int[] displayColumnWidths)
		throws SQLException
	{
		indent_DisplayResults( out, rs, conn, 0, displayColumns, 
							   displayColumnWidths);
	}

    static private void indent_DisplayResults
        (PrintWriter out, ResultSet rs, Connection conn, int indentLevel,
         int[] displayColumns, int[] displayColumnWidths)
        throws SQLException {
        List<ResultSet> resultSets = new ArrayList<ResultSet>();
        resultSets.add(rs);
        indent_DisplayResults( out, resultSets, conn, 0, displayColumns, 
                               displayColumnWidths);
    }

    static private void indent_DisplayResults
        (PrintWriter out, List<ResultSet> resultSets, Connection conn, int indentLevel,
         int[] displayColumns, int[] displayColumnWidths)
        throws SQLException {

        ResultSetMetaData rsmd = null;

        //get metadata from the first ResultSet
        if (resultSets != null && resultSets.size() > 0)
            rsmd = (resultSets.get(0)).getMetaData();

        checkNotNull(rsmd, "ResultSetMetaData");
        Vector<ResultSet> nestedResults;
        int numberOfRowsSelected = 0;

        // autocommit must be off or the nested cursors
        // are closed when the outer statement completes.
        if (!conn.getAutoCommit())
            nestedResults = new Vector<ResultSet>();
        else
            nestedResults = null;

        if(displayColumnWidths == null)
            displayColumnWidths = getColumnDisplayWidths(rsmd,
                                                         displayColumns,true);

        int len = indent_DisplayBanner(out,rsmd, indentLevel, displayColumns,
                                       displayColumnWidths);

        // When displaying rows, keep going past errors
        // unless/until the maximum # of errors is reached.
        int retry = 0;

        ResultSet rs = null;
        boolean doNext = true;
        for (int i = 0; i< resultSets.size(); i++) {
            rs = resultSets.get(i);
            doNext = true;
            while (doNext){
                try {
                    doNext = rs.next();
                    if (doNext) {

                        DisplayRow(out, rs, rsmd, len, nestedResults, conn,
                                   indentLevel, displayColumns,
                                   displayColumnWidths);
                        ShowWarnings(out, rs);
                        numberOfRowsSelected++;
                    }
                } catch (SQLException e) {
                    // REVISIT: might want to check the exception
                    // and for some, not bother with the retry.
                    if (++retry > MAX_RETRIES)
                        throw e;
                    else
                        ShowSQLException(out, e);
                }
            }
        }
        if (showSelectCount == true) {
            if (numberOfRowsSelected == 1) {
                out.println();
                indentedPrintLine(out, indentLevel,
                                  LocalizedResource.getMessage("UT_1RowSelec"));
            } else if (numberOfRowsSelected >= 0) {
                out.println();
                indentedPrintLine(out, indentLevel,
                        LocalizedResource.getMessage("UT_0RowsSelec",
                            LocalizedResource.getNumber(numberOfRowsSelected)));
            }
        }

        DisplayNestedResults(out, nestedResults, conn, indentLevel );
        nestedResults = null;
    }


	/**
		@param out the place to write to
		@param nr the vector of results
		@param conn the Connection against which the ResultSet was retrieved
		@param indentLevel number of tab stops to indent line

		@exception SQLException thrown on access error
	 */
	static private void DisplayNestedResults(PrintWriter out, Vector<ResultSet> nr, Connection conn, int indentLevel )
		throws SQLException {

		if (nr == null) return;

		String b=LocalizedResource.getMessage("UT_JDBCDisplayUtil_16");
		String oldString="0";

		for (int i=0; i < nr.size(); i++) {
			LocalizedResource.OutputWriter().println();

			//just too clever to get the extra +s
			String t = Integer.toString(i);
			if (t.length() > oldString.length()) {
				oldString = t;
				b=b+LocalizedResource.getMessage("UT_JDBCDisplayUtil_17");
			}

			LocalizedResource.OutputWriter().println(b);
			LocalizedResource.OutputWriter().println(LocalizedResource.getMessage("UT_Resul0", LocalizedResource.getNumber(i)));
			LocalizedResource.OutputWriter().println(b);
			indent_DisplayResults(out, (ResultSet) nr.elementAt(i), conn,
								  indentLevel, null, null);
		}
	}

	/**
		Fetch the next row of the result set, and if it
		exists format and display a banner and the row.

		@param out the place to write to
		@param rs the ResultSet in use
		@param conn the Connection against which the ResultSet was retrieved

		@exception SQLException on JDBC access failure
	 */
	static public void DisplayNextRow(PrintWriter out, ResultSet rs, Connection conn )
		throws SQLException
	{
		indent_DisplayNextRow( out, rs, conn, 0, null, (rs == null) ? null
							   : getColumnDisplayWidths(rs.getMetaData(), null, true));
	}

	static private void indent_DisplayNextRow(PrintWriter out, ResultSet rs, Connection conn, int indentLevel,
											  int[] displayColumns, int[] displayColumnWidths )
		throws SQLException {

		Vector<ResultSet> nestedResults;

		// autocommit must be off or the nested cursors
		// are closed when the outer statement completes.
		if (!conn.getAutoCommit())
			nestedResults = new Vector<ResultSet>();
		else
			nestedResults = null;

		checkNotNull(rs, "ResultSet");

		ResultSetMetaData rsmd = rs.getMetaData();
		checkNotNull(rsmd, "ResultSetMetaData");

		// Only print stuff out if there is a row to be had.
		if (rs.next()) {
			int rowLen = indent_DisplayBanner(out, rsmd, indentLevel, displayColumns, displayColumnWidths);
    		DisplayRow(out, rs, rsmd, rowLen, nestedResults, conn, indentLevel,
					   null, null );
		}
		else {
			indentedPrintLine( out, indentLevel, LocalizedResource.getMessage("UT_NoCurreRow"));
		}

		ShowWarnings(out, rs);

		DisplayNestedResults(out, nestedResults, conn, indentLevel );
		nestedResults = null;

	} // DisplayNextRow

	/**
		Display the current row of the result set along with
		a banner. Assume the result set is on a row.

		@param out the place to write to
		@param rs the ResultSet in use
		@param conn the Connection against which the ResultSet was retrieved

		@exception SQLException on JDBC access failure
	 */
	static public void DisplayCurrentRow(PrintWriter out, ResultSet rs, Connection conn )
		throws SQLException
	{
		indent_DisplayCurrentRow( out, rs, conn, 0, null, (rs == null) ? null
								  : getColumnDisplayWidths(rs.getMetaData(), null, true) );
	}

	static private void indent_DisplayCurrentRow(PrintWriter out, ResultSet rs, Connection conn, 
												 int indentLevel, int[] displayColumns, int[] displayColumnWidths )
		throws SQLException {

		Vector<ResultSet> nestedResults;

		if (rs == null) {
			indentedPrintLine( out, indentLevel, LocalizedResource.getMessage("UT_NoCurreRow_19"));
			return;
		}

		// autocommit must be off or the nested cursors
		// are closed when the outer statement completes.
		if (!conn.getAutoCommit())
			nestedResults = new Vector<ResultSet>();
		else
			nestedResults = null;

		ResultSetMetaData rsmd = rs.getMetaData();
		checkNotNull(rsmd, "ResultSetMetaData");

		int rowLen = indent_DisplayBanner(out, rsmd, indentLevel, displayColumns, displayColumnWidths);
   		DisplayRow(out, rs, rsmd, rowLen, nestedResults, conn, indentLevel,
				   displayColumns, displayColumnWidths );

		ShowWarnings(out, rs);

		DisplayNestedResults(out, nestedResults, conn, indentLevel );
		nestedResults = null;

	} // DisplayNextRow

	/**
		Print a banner containing the column labels separated with '|'s
		and a line of '-'s.  Each field is as wide as the display
		width reported by the metadata.

		@param out the place to write to
		@param rsmd the ResultSetMetaData to use

		@exception SQLException on JDBC access failure
	 */
	static public int DisplayBanner(PrintWriter out, ResultSetMetaData rsmd )
		throws SQLException
	{
		return indent_DisplayBanner( out, rsmd, 0, null, 
									 getColumnDisplayWidths(rsmd, null, true) );
	}

	static private int indent_DisplayBanner(PrintWriter out, ResultSetMetaData rsmd, int indentLevel,
											int[] displayColumns, int[] displayColumnWidths )
		throws SQLException	{

		StringBuffer buf = new StringBuffer();

		int numCols = displayColumnWidths.length;
		int rowLen;

		// do some precalculation so the buffer is allocated only once
		// buffer is twice as long as the display length plus one for a newline
		rowLen = (numCols - 1); // for the column separators
		for (int i=1; i <= numCols; i++)
			rowLen += displayColumnWidths[i-1];
		buf.ensureCapacity(rowLen);

		// get column header info
		// truncate it to the column display width
		// add a bar between each item.
		for (int i=1; i <= numCols; i++) {
			int colnum = displayColumns==null ? i : displayColumns[i-1];

			if (i>1)
				buf.append('|');

			String s = rsmd.getColumnLabel(colnum);

			int w = displayColumnWidths[i-1];

			if (s.length() < w) {
				
				buf.append(s);

				// try to paste on big chunks of space at a time.
				int k = w - s.length();
				for (; k >= 64; k -= 64)
					buf.append(
          "                                                                ");
				for (; k >= 16; k -= 16)
					buf.append("                ");
				for (; k >= 4; k -= 4)
					buf.append("    ");
				for (; k > 0; k--)
					buf.append(' ');
			}
			else if (s.length() > w)  {
				if (w > 1) 
					buf.append(s.substring(0,w-1));
				if (w > 0) 
					buf.append('&');
			}
			else {
				buf.append(s);
			}
		}

		buf.setLength(Math.min(rowLen, 1024));
		indentedPrintLine( out, indentLevel, buf);

		// now print a row of '-'s
		for (int i=0; i<Math.min(rowLen, 1024); i++)
			buf.setCharAt(i, '-');
		indentedPrintLine( out, indentLevel, buf);

		buf = null;

		return rowLen;
	} // DisplayBanner

	/**
		Print one row of a result set, padding each field to the
		display width and separating them with '|'s

		@param out the place to write to
		@param rs the ResultSet to use
		@param rsmd the ResultSetMetaData to use
		@param rowLen
		@param nestedResults
		@param conn
		@param indentLevel number of tab stops to indent line
	    @param displayColumns A list of column numbers to display
	    @param displayColumnWidths If displayColumns is set, the width of
								columns to display, in characters.

		@exception SQLException thrown on JDBC access failure
	 */
	static private void DisplayRow(PrintWriter out, ResultSet rs, ResultSetMetaData rsmd, int rowLen, Vector<ResultSet> nestedResults, Connection conn, int indentLevel,
								   int[] displayColumns, int[] displayColumnWidths )
		throws SQLException
	{
		StringBuffer buf = new StringBuffer();
		buf.ensureCapacity(rowLen);

		int numCols = displayColumnWidths.length;
		int i;

		// get column header info
		// truncate it to the column display width
		// add a bar between each item.
		for (i=1; i <= numCols; i++){
			int colnum = displayColumns==null ? i : displayColumns[i-1];
			if (i>1)
				buf.append('|');

			String s;
			switch (rsmd.getColumnType(colnum)) {
			default:
				s = LocalizedResource.getInstance().getLocalizedString(rs, rsmd, colnum );
				break;
			case Types.JAVA_OBJECT:
			case Types.OTHER:
			{
				Object o = rs.getObject(colnum);
				if (o == null) { s = "NULL"; }
				else if (o instanceof ResultSet && nestedResults != null)
				{
					s = LocalizedResource.getMessage("UT_Resul0_20", LocalizedResource.getNumber(nestedResults.size()));
					nestedResults.addElement( (ResultSet) o);
				}
				else
				{
					try {
						s = rs.getString(colnum);
					} catch (SQLException se) {
						// oops, they don't support refetching the column
						s = o.toString();
					}
				}
			}
			break;
			}
			if (s==null) s = "NULL";

			int w = displayColumnWidths[i-1];
			if (s.length() < w) {
				StringBuffer fullS = new StringBuffer(s);
				fullS.ensureCapacity(w);
				for (int k=s.length(); k<w; k++)
					fullS.append(' ');
				s = fullS.toString();
			}
			else if (s.length() > w)
				// add the & marker to know it got cut off
				s = s.substring(0,w-1)+"&";

			buf.append(s);
		}
		indentedPrintLine( out, indentLevel, buf);

	} // DisplayRow

	/**
		Check if an object is null, and if it is, throw an exception
		with an informative parameter about what was null.
		The exception is a run-time exception that is internal to ij.

		@param o the object to test
		@param what the information to include in the error if it is null
	 */
	public static void checkNotNull(Object o, String what) {
		if (o == null) {
			throw ijException.objectWasNull(what);
		}
	} // checkNotNull

	/**
		Map the string to the value if it is null.

		@param s the string to test for null
		@param nullValue the value to use if s is null

		@return if s is non-null, s; else nullValue.
	 */
	static public String mapNull(String s, String nullValue) {
		if (s==null) return nullValue;
		return s;
	}

	/**
		If the property ij.exceptionTrace is true, display the stack
		trace to the print stream. Otherwise, do nothing.

		@param out the output stream to write to
		@param e the exception to display
	 */
	static public void doTrace(PrintWriter out, Exception e) {
		if (getSystemBoolean("ij.exceptionTrace")) {
			e.printStackTrace(out);
		    out.flush();
		}
	}

	static public void setMaxDisplayWidth(int maxDisplayWidth) {
		maxWidth = maxDisplayWidth;
	}

	static	private	void	indentedPrintLine( PrintWriter out, int indentLevel, String text )
	{
		indent( out, indentLevel );
		out.println( text );
	}

	static	private	void	indentedPrintLine( PrintWriter out, int indentLevel, StringBuffer text )
	{
		indent( out, indentLevel );
		out.println( text );
	}

	static	private	void	indent( PrintWriter out, int indentLevel )
	{
		for ( int ictr = 0; ictr < indentLevel; ictr++ ) { out.print( "  " ); }
	}

	// ================

	static public void ShowException(PrintStream out, Throwable e) {
		if (e == null) return;

		if (e instanceof SQLException)
			ShowSQLException(out, (SQLException)e);
		else
			e.printStackTrace(out);
	}

	static public void ShowSQLException(PrintStream out, SQLException e) {
		String errorCode;

		if (getSystemBoolean("ij.showErrorCode")) {
			errorCode = " (errorCode = " + e.getErrorCode() + ")";
		}
		else {
			errorCode = "";
		}

		while (e!=null) {
			out.println("ERROR "+mapNull(e.getSQLState(),"(no SQLState)")+": "+
				 mapNull(e.getMessage(),"(no message)")+errorCode);
			doTrace(out, e);
			e=e.getNextException();
		}
	}

	static public void ShowWarnings(PrintStream out, Connection theConnection) {
	    try {
		// GET CONNECTION WARNINGS
		SQLWarning warning = null;

		if (theConnection != null) {
			ShowWarnings(out, theConnection.getWarnings());
		}

		if (theConnection != null) {
			theConnection.clearWarnings();
		}
	    } catch (SQLException e) {
			ShowSQLException(out, e);
	    }
	} // ShowWarnings

	static public void ShowWarnings(PrintStream out, SQLWarning warning) {
		while (warning != null) {
			out.println("WARNING "+
				mapNull(warning.getSQLState(),"(no SQLState)")+": "+
				mapNull(warning.getMessage(),"(no message)"));
			warning = warning.getNextWarning();
		}
	}

	static public void ShowWarnings(PrintStream out, ResultSet rs) {
	    try {
		// GET RESULTSET WARNINGS
		SQLWarning warning = null;

		if (rs != null) {
			ShowWarnings(out, rs.getWarnings());
		}

		if (rs != null) {
			rs.clearWarnings();
		}
	    } catch (SQLException e) {
			ShowSQLException(out, e);
	    }
	} // ShowResultSetWarnings

	static public void ShowWarnings(PrintStream out, Statement s)
	{
	    try {
		// GET STATEMENT WARNINGS
		SQLWarning warning = null;

		if (s != null) {
			ShowWarnings(out, s.getWarnings());
		}

		if (s != null) {
			s.clearWarnings();
		}
	    } catch (SQLException e) {
			ShowSQLException(out, e);
	    }
	} // ShowStatementWarnings

	static public void DisplayResults(PrintStream out, Statement stmt, Connection conn )
		throws SQLException
	{
		indent_DisplayResults( out, stmt, conn, 0, null, null);			
	}

	static private void indent_DisplayResults
	(PrintStream out, Statement stmt, Connection conn, int indentLevel,
	 int[] displayColumns, int[] displayColumnWidths)
		throws SQLException {

		checkNotNull(stmt, "Statement");

		ResultSet rs = stmt.getResultSet();
		if (rs != null) {
			indent_DisplayResults(out, rs, conn, indentLevel, displayColumns,
								  displayColumnWidths);
			rs.close(); // let the result set go away
		}
		else {
			DisplayUpdateCount(out,stmt.getUpdateCount(), indentLevel);
		}

		ShowWarnings(out,stmt);
	} // DisplayResults

	static void DisplayUpdateCount(PrintStream out, int count, int indentLevel ) {
		if (count == 1) {
			indentedPrintLine( out, indentLevel, "1 row inserted/updated/deleted");
		}
		else if (count >= 0) {
			indentedPrintLine( out, indentLevel, count+" rows inserted/updated/deleted");
		}
		else {
			indentedPrintLine( out, indentLevel, "Statement executed.");
		}
	}

	static public void DisplayResults(PrintStream out, ResultSet rs, Connection conn)
		throws SQLException
	{
		indent_DisplayResults( out, rs, conn, 0, null, null);
	}

	static private void indent_DisplayResults
	(PrintStream out, ResultSet rs, Connection conn, int indentLevel,
	 int[] displayColumns, int[] displayColumnWidths)
		throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		checkNotNull(rsmd, "ResultSetMetaData");
		Vector<ResultSet> nestedResults;
    int numberOfRowsSelected = 0;

		// autocommit must be off or the nested cursors
		// are closed when the outer statement completes.
		if (!conn.getAutoCommit())
			nestedResults = new Vector<ResultSet>();
		else
			nestedResults = null;

		if(displayColumnWidths == null)
			displayColumnWidths = getColumnDisplayWidths(rsmd, displayColumns, false);

		int len = indent_DisplayBanner(out,rsmd, indentLevel, displayColumns,
									   displayColumnWidths);

		// When displaying rows, keep going past errors
		// unless/until the maximum # of errors is reached.
		boolean doNext = true;
		int retry = 0;
		while (doNext) {
			try {
				doNext = rs.next();
				if (doNext) {

		    		DisplayRow(out, rs, rsmd, len, nestedResults, conn, 
							   indentLevel, displayColumns, 
							   displayColumnWidths);
					ShowWarnings(out, rs);
					numberOfRowsSelected++;
				}
				// print number of rows in the result set
				 indentedPrintLine( out, indentLevel, numberOfRowsSelected + " rows in the result set");
			} catch (SQLException e) {
				// REVISIT: might want to check the exception
				// and for some, not bother with the retry.
				if (++retry > MAX_RETRIES)
					throw e;
				else
					ShowSQLException(out, e);
			}
		}
		if (showSelectCount == true) {
		   if (numberOfRowsSelected == 1) {
			   out.println();
			   indentedPrintLine( out, indentLevel, "1 row selected");
		   } else if (numberOfRowsSelected >= 0) {
			   out.println();
		       indentedPrintLine( out, indentLevel, numberOfRowsSelected + " rows selected");
		   }
		}

		DisplayNestedResults(out, nestedResults, conn, indentLevel );
		nestedResults = null;
	}

	static private void DisplayNestedResults(PrintStream out, Vector<ResultSet> nr, Connection conn, int indentLevel )
		throws SQLException {

		if (nr == null) return;

		String s="+ ResultSet #";
		String b="++++++++++++++++";
		String oldString="0";

		for (int i=0; i < nr.size(); i++) {
			System.out.println();

			//just too clever to get the extra +s
			String t = Integer.toString(i);
			if (t.length() > oldString.length()) {
				oldString = t;
				b=b+"+";
			}

			System.out.println(b);
			System.out.println(s+i+" +");
			System.out.println(b);
			indent_DisplayResults(out, nr.elementAt(i), conn, 
								  indentLevel, null, null);
		}
	}

	static public void DisplayNextRow(PrintStream out, ResultSet rs, Connection conn )
		throws SQLException
	{
		indent_DisplayNextRow( out, rs, conn, 0, null, (rs == null) ? null
							   : getColumnDisplayWidths(rs.getMetaData(),null,false) );
	}

	static private void indent_DisplayNextRow(PrintStream out, ResultSet rs, Connection conn, int indentLevel,
											  int[] displayColumns, int[] displayColumnWidths )
		throws SQLException {

		Vector<ResultSet> nestedResults;

		// autocommit must be off or the nested cursors
		// are closed when the outer statement completes.
		if (!conn.getAutoCommit())
			nestedResults = new Vector<ResultSet>();
		else
			nestedResults = null;

		checkNotNull(rs, "ResultSet");

		ResultSetMetaData rsmd = rs.getMetaData();
		checkNotNull(rsmd, "ResultSetMetaData");

		// Only print stuff out if there is a row to be had.
		if (rs.next()) {
			int rowLen = indent_DisplayBanner(out, rsmd, indentLevel, null, null);
    		DisplayRow(out, rs, rsmd, rowLen, nestedResults, conn, indentLevel,
					   displayColumns, displayColumnWidths);
		}
		else {
			indentedPrintLine( out, indentLevel, LocalizedResource.getMessage("UT_NoCurreRow"));
		}

		ShowWarnings(out, rs);

		DisplayNestedResults(out, nestedResults, conn, indentLevel );
		nestedResults = null;

	} // DisplayNextRow

	static public void DisplayCurrentRow(PrintStream out, ResultSet rs, Connection conn )
		throws SQLException
	{
		indent_DisplayCurrentRow( out, rs, conn, 0, null, (rs == null) ? null
								  : getColumnDisplayWidths(rs.getMetaData(),null,false) );
	}

	static private void indent_DisplayCurrentRow(PrintStream out, ResultSet rs, Connection conn, 
												 int indentLevel, int[] displayColumns, int[] displayColumnWidths )
		throws SQLException {

		Vector<ResultSet> nestedResults;

		if (rs == null) {
			indentedPrintLine( out, indentLevel, LocalizedResource.getMessage("UT_NoCurreRow_19"));
			return;
		}

		// autocommit must be off or the nested cursors
		// are closed when the outer statement completes.
		if (!conn.getAutoCommit())
			nestedResults = new Vector<ResultSet>();
		else
			nestedResults = null;

		ResultSetMetaData rsmd = rs.getMetaData();
		checkNotNull(rsmd, "ResultSetMetaData");

		int rowLen = indent_DisplayBanner(out, rsmd, indentLevel, displayColumns, displayColumnWidths);
   		DisplayRow(out, rs, rsmd, rowLen, nestedResults, conn, indentLevel,
				   displayColumns, displayColumnWidths);

		ShowWarnings(out, rs);

		DisplayNestedResults(out, nestedResults, conn, indentLevel );
		nestedResults = null;

	} // DisplayNextRow

	static public int DisplayBanner(PrintStream out, ResultSetMetaData rsmd )
		throws SQLException
	{
		return indent_DisplayBanner( out, rsmd, 0, null,
									 getColumnDisplayWidths(rsmd,null,false) );
	}

	static private int indent_DisplayBanner(PrintStream out, ResultSetMetaData rsmd, int indentLevel,
											int[] displayColumns, int[] displayColumnWidths )
		throws SQLException	{

		StringBuffer buf = new StringBuffer();

		int numCols = displayColumnWidths.length;
		int rowLen;

		// do some precalculation so the buffer is allocated only once
		// buffer is twice as long as the display length plus one for a newline
		rowLen = (numCols - 1); // for the column separators
		for (int i=1; i <= numCols; i++) {
			rowLen += displayColumnWidths[i-1];
		}
		buf.ensureCapacity(rowLen);

		// get column header info
		// truncate it to the column display width
		// add a bar between each item.
		for (int i=1; i <= numCols; i++) {
			int colnum = displayColumns==null ? i : displayColumns[i-1];

			if (i>1)
				buf.append('|');

			String s = rsmd.getColumnLabel(colnum);

			int w = displayColumnWidths[i-1];

			if (s.length() < w) {
				// build a string buffer to hold the whitespace
				StringBuffer blanks = new StringBuffer(s);
				blanks.ensureCapacity(w);

				// try to paste on big chunks of space at a time.
				for (int k=blanks.length()+64; k<=w; k+=64)
					blanks.append(
          "                                                                ");
				for (int k=blanks.length()+16; k<=w; k+=16)
					blanks.append("                ");
				for (int k=blanks.length()+4; k<=w; k+=4)
					blanks.append("    ");
				for (int k=blanks.length(); k<w; k++)
					blanks.append(' ');

				buf.append(blanks);
				// REMIND: could do more cleverness, like keep around
				// past buffers to reuse...
			}
			else if (s.length() > w)  {
				if (w > 1) 
					buf.append(s.substring(0,w-1));
				if (w > 0) 
					buf.append('&');
			}
			else {
				buf.append(s);
			}
		}

		buf.setLength(Math.min(rowLen, 1024));
		indentedPrintLine( out, indentLevel, buf);

		// now print a row of '-'s
		for (int i=0; i<Math.min(rowLen, 1024); i++)
			buf.setCharAt(i, '-');
		indentedPrintLine( out, indentLevel, buf);

		buf = null;

		return rowLen;
	} // DisplayBanner

	static private void DisplayRow(PrintStream out, ResultSet rs, ResultSetMetaData rsmd, int rowLen, Vector<ResultSet> nestedResults, Connection conn, int indentLevel,
								   int[] displayColumns, int[] displayColumnWidths)
		throws SQLException
	{
		StringBuffer buf = new StringBuffer();
		buf.ensureCapacity(rowLen);

		int numCols = displayColumnWidths.length;
		int i;

		// get column header info
		// truncate it to the column display width
		// add a bar between each item.
		for (i=1; i <= numCols; i++){
			int colnum = displayColumns==null ? i : displayColumns[i-1];
			if (i>1)
				buf.append('|');

			String s;
			switch (rsmd.getColumnType(colnum)) {
			default:
				s = rs.getString(colnum);
				break;
			case Types.JAVA_OBJECT:
			case Types.OTHER:
			{
				Object o = rs.getObject(colnum);
				if (o == null) { s = "NULL"; }
				else if (o instanceof ResultSet && nestedResults != null)
				{
					s = "ResultSet #"+nestedResults.size();
					nestedResults.addElement( (ResultSet) o);
				}
				else
				{
					try {
						s = rs.getString(colnum);
					} catch (SQLException se) {
						// oops, they don't support refetching the column
						s = o.toString();
					}
				}
			}
			break;
			}

			if (s==null) s = "NULL";

			int w = displayColumnWidths[i-1];
			if (s.length() < w) {
				StringBuffer fullS = new StringBuffer(s);
				fullS.ensureCapacity(w);
				for (int k=s.length(); k<w; k++)
					fullS.append(' ');
				s = fullS.toString();
			}
			else if (s.length() > w)
				// add the & marker to know it got cut off
				s = s.substring(0,w-1)+"&";

			buf.append(s);
		}
		indentedPrintLine( out, indentLevel, buf);

	} // DisplayRow

	static public void doTrace(PrintStream out, Exception e) {
		if (getSystemBoolean("ij.exceptionTrace")) {
			e.printStackTrace(out);
		    out.flush();
		}
	}

	static	private	void	indentedPrintLine( PrintStream out, int indentLevel, String text )
	{
		indent( out, indentLevel );
		out.println( text );
	}

	static	private	void	indentedPrintLine( PrintStream out, int indentLevel, StringBuffer text )
	{
		indent( out, indentLevel );
		out.println( text );
	}

	static	private	void	indent( PrintStream out, int indentLevel )
	{
		for ( int ictr = 0; ictr < indentLevel; ictr++ ) { out.print( "  " ); }
	}
	
	// ==========================
    
    /**
     * Get an ij boolean system property.
     *
     * @param name name of the property
     */
    private static boolean getSystemBoolean(final String name) {

        return (AccessController
                .doPrivileged(new java.security.PrivilegedAction<Boolean>() {

                    public Boolean run() {
                        return Boolean.getBoolean(name) ?
                            Boolean.TRUE : Boolean.FALSE;

                    }

                })).booleanValue();
    }
}



