/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedResultSet

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Row;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.services.io.StreamStorable;

import org.apache.derby.iapi.services.io.LimitInputStream;
import org.apache.derby.iapi.services.io.NewByteArrayInputStream;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.reference.JDBC20Translation;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.util.StringUtil;

/* can't import these due to name overlap:
import java.sql.ResultSet;
*/
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.ResultSetMetaData;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import java.io.InputStream;
import java.io.IOException;

import java.util.Calendar;

/**
 * A EmbedResultSet for results from the EmbedStatement family. 
 *
 * @author ames
 */

public abstract class EmbedResultSet extends ConnectionChild 
    implements java.sql.ResultSet, Comparable {

	// cursor movement
	protected static final int FIRST = 1;
	protected static final int NEXT = 2;
	protected static final int LAST = 3;
	protected static final int PREVIOUS = 4;
	protected static final int BEFOREFIRST = 5;
	protected static final int AFTERLAST = 6;
	protected static final int ABSOLUTE = 7;
	protected static final int RELATIVE = 8;

	// mutable state
	protected ExecRow currentRow;
	private DataValueDescriptor[] rowData;
	protected boolean wasNull;
	protected boolean isClosed;
	private Object	currentStream;

	// immutable state
	protected ResultSet theResults;
	private boolean forMetaData;
	private ResultSetMetaData rMetaData;
	private SQLWarning topWarning;

	// This activation is set by EmbedStatement
	// for a single execution Activation. Ie.
	// a ResultSet from a Statement.executeQuery().
	// In this case the finalization of the ResultSet
	// will mark the Activation as unused.
	// c.f. EmbedPreparedStatement.finalize().
	Activation finalizeActivation;

	// Order of creation 
	final int order;


	private final ResultDescription resultDescription;
	/**
		An array of the JDBC column types for this
		result set, indexed by column identifier
		with the first column at index 1. Position 0
		in the array is not used.
	*/
	private final int[] jdbcColumnTypes;

    // max rows limit for this result set
    private int maxRows;
    // The Maximum field size limt set for this result set
    private int maxFieldSize;

    /*
     * Incase of forward only cursors we limit the number of rows
     * returned if the maxRows is set. The following varible is used
     * to keep the count of number of rows returned to the user.
     */
    private int NumberofFetchedRows;


	/*
		we hang on to the statement to prevent GC from
		closing it under us
	 */
	protected final EmbedStatement stmt;
	private EmbedStatement owningStmt;

	protected final boolean isAtomic;


	/**
	 * This class provides the glue between the Cloudscape
	 * resultset and the JDBC resultset, mapping calls-to-calls.
	 */
	public EmbedResultSet(EmbedConnection conn, ResultSet resultsToWrap,
		boolean forMetaData, EmbedStatement stmt, boolean isAtomic) 
        {

		super(conn);

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(resultsToWrap!=null);
		theResults = resultsToWrap;
		this.forMetaData = forMetaData;
		this.stmt = owningStmt = stmt;
		this.isAtomic = isAtomic;

		// Fill in the column types
		ResultDescription rd = resultDescription = theResults.getResultDescription();
		jdbcColumnTypes = new int[rd.getColumnCount() + 1];

		for (int column = 1; column < jdbcColumnTypes.length; column++) {
			jdbcColumnTypes[column] =
				rd.getColumnDescriptor(column).getType().getTypeId().getJDBCTypeId();
		}

        // assign the max rows and maxfiled size limit for this result set
        if (stmt != null)
        {
           // At connectivity level we handle only for forward only cursor
           if (stmt.resultSetType == JDBC20Translation.TYPE_FORWARD_ONLY) 
               maxRows = stmt.maxRows;

           maxFieldSize = stmt.MaxFieldSize;
        }

		order = conn.getResultSetOrderId();

	//	System.out.println(conn.getClass() + " create RS " + this);
	//	new Throwable("CRS").printStackTrace(System.out);
	}

	/**
		JDBC states that a ResultSet is closed when garbage collected.
		We simply mark the activation as unused. Some later use
		of the connection will clean everything up.

		@exception Throwable Allows any exception to be thrown during finalize
	*/
	protected void finalize() throws Throwable {
		super.finalize();

		if (finalizeActivation != null) {
			finalizeActivation.markUnused();
		}		
	}

	// onRow protects us from making requests of
	// resultSet that would fail with NullPointerExceptions
	// or milder problems due to not having a row.
	protected final DataValueDescriptor[] checkOnRow() throws SQLException	{

		DataValueDescriptor[] theCurrentRow = rowData;

		if (theCurrentRow == null)
			throw newSQLException(SQLState.NO_CURRENT_ROW);

		return theCurrentRow;
	}

	/**
		Check the column is in range *and* return the JDBC type of the column.

		@exception SQLException ResultSet is not on a row or columnIndex is out of range.
	*/
	protected int getColumnType(int columnIndex) throws SQLException {
		checkOnRow(); // first make sure there's a row
		
		if (columnIndex < 1 ||
		    columnIndex >= jdbcColumnTypes.length)
			throw newSQLException(SQLState.COLUMN_NOT_FOUND, 
                         new Integer(columnIndex));

		return jdbcColumnTypes[columnIndex];
	}

	/*
	 * java.sql.ResultSet interface
	 */
    /**
     * A ResultSet is initially positioned before its first row; the
     * first call to next makes the first row the current row; the
     * second call makes the second row the current row, etc.
     *
     * <P>If an input stream from the previous row is open, it is
     * implicitly closed. The ResultSet's warning chain is cleared
     * when a new row is read.
     *
     * @return true if the new current row is valid; false if there
     * are no more rows
	 * @exception SQLException thrown on failure.
     */
    public boolean next() throws SQLException 
	{
        // we seem to have some trigger paths which don't have
        // statement initialized, may not need this check in those cases
        if (maxRows !=0 )
        {
            NumberofFetchedRows++;    
            // check whether we hit the maxRows limit 
            if (NumberofFetchedRows > maxRows) 
            {
                //we return false for the next call when maxRows is hit
                closeCurrentStream();
                return false;
            }
        }

	    return movePosition(NEXT, 0, "next");
	}

	protected boolean movePosition(int position, String positionText)
		throws SQLException
	{
		return movePosition(position, 0, positionText);
	}

	protected boolean movePosition(int position, int row, String positionText)
		throws SQLException
	{
		closeCurrentStream();	// closing currentStream does not depend on the
								// underlying connection.  Do this outside of
								// the connection synchronization.

		checkExecIfClosed(positionText);	// checking result set closure does not depend
								// on the underlying connection.  Do this
								// outside of the connection synchronization.


		synchronized (getConnectionSynchronization()) {

					setupContextStack();
		    try {
				LanguageConnectionContext lcc = getEmbedConnection().getLanguageConnection();
		    try {

				/* Push and pop a StatementContext around a next call
				 * so that the ResultSet will get correctly closed down
				 * on an error.
				 * (Cache the LanguageConnectionContext)
				 */
				StatementContext statementContext = lcc.pushStatementContext(isAtomic, getSQLText(),
														getParameterValueSet(), false);

				switch (position)
				{
					case BEFOREFIRST:
						currentRow = theResults.setBeforeFirstRow();
						break;

					case FIRST:
						currentRow = theResults.getFirstRow();
						break;

					case NEXT:
						currentRow = theResults.getNextRow();
						break;

					case LAST:
						currentRow = theResults.getLastRow();
						break;

					case AFTERLAST:
						currentRow = theResults.setAfterLastRow();
						break;

					case PREVIOUS:
						currentRow = theResults.getPreviousRow();
						break;

					case ABSOLUTE:
						currentRow = theResults.getAbsoluteRow(row);
						break;

					case RELATIVE:
						currentRow = theResults.getRelativeRow(row);
						break;

					default:
						if (SanityManager.DEBUG)
						{
							SanityManager.THROWASSERT(
								"Unexpected value for position - " + position);
						}
				}

				lcc.popStatementContext(statementContext, null);
				
		    } catch (Throwable t) {
				/*
				 * Need to close the result set here because the error might
				 * cause us to lose the current connection if this is an XA
				 * connection and we won't be able to do the close later
				 */
				throw closeOnTransactionError(t);
			}
         
			SQLWarning w = theResults.getWarnings();
			if (w != null) {
				if (topWarning == null)
					topWarning = w;
				else
					topWarning.setNextWarning(w);
			}

		    boolean onRow = (currentRow!=null);

			//if (onRow && !(currentRow instanceof org.apache.derby.impl.sql.execute.ValueRow))
			//	System.out.println(currentRow.getClass());

		    /*
			    Connection.setAutoCommit says that a statement completes,
			    and will autoCommit, when it fetches the last row or is closed.
			    This means a close will get a "Cursor already closed" error.
				This rule only applies when doing a next() - if it were applied
				to scrolling actions (like FIRST or LAST) it would close
				the cursor when doing any operation on a scrolling cursor.

			    if autocommit, this will commit
		     */
		    if (!onRow && (position == NEXT)) {

		     // In case of resultset for MetaData, we will only commit
		     // if we are the only statement currently opened for this
		     // connection; otherwise we don't want to affect other
		     // resultSet's by committing the MetaData one.
		     // There is no internal xact (xact isolation) for MetaData type
		     // of resultSet; therefore committing (to release locks) would end
		     // up committing all the other resultSet for this connection.
		     //
		     // We do synchronize on the connection, therefore Activation count
		     // should be valid and protected.
		     //
			//LanguageConnectionContext lcc = getEmbedConnection().getLanguageConnection();
		     if (forMetaData && (lcc.getActivationCount() > 1)) {
		     	// we do not want to commit here as there seems to be other
		     	// statements/resultSets currently opened for this connection.
		     } else if (owningStmt != null)
				 // allow the satement to commit if required.
		     	owningStmt.resultSetClosing(this);
		    }

			rowData = onRow ? currentRow.getRowArray() : null;

			return onRow;
			} finally {
			    restoreContextStack();
			}
		}

	}




    /**
     * In some cases, it is desirable to immediately release a
     * ResultSet's database and JDBC resources instead of waiting for
     * this to happen when it is automatically closed; the close
     * method provides this immediate release.
     *
     * <P><B>Note:</B> A ResultSet is automatically closed by the
     * Statement that generated it when that Statement is closed,
     * re-executed, or is used to retrieve the next result from a
     * sequence of multiple results. A ResultSet is also automatically
     * closed when it is garbage collected.
	 * @exception SQLException thrown on failure.
     */
    public void close() throws SQLException	{

		/* if this result is already closed, don't try to close again
		 * we may have closed it earlier because of an error and trying
		 * to close again will cause a different problem if the connection
		 * has been closed as in XA error handling
		 */
		if (isClosed)
			return;

		closeCurrentStream();	// closing currentStream does not depend on the
								// underlying connection.  Do this outside of
								// the connection synchronization.
		// Would like to throw an exception if already closed, but
		// some code assumes you can close a ResultSet more than once.
		// checkIfClosed("close");

		// synchronize out here so the close and the autocommit are
		// both in the same sync block.
		synchronized (getConnectionSynchronization()) {

			try {
				setupContextStack(); // make sure there's context
			} catch (SQLException se) {
				// we may get an exception here if this is part of an XA transaction
				// and the transaction has been committed
				// just give up and return
				return;
			}

			try	{
				try	{
				    theResults.finish(); // release the result set, don't just close it
				} catch (Throwable t) {
					throw handleException(t);
				}

			    // In case of resultset for MetaData, we will only commit
		        // if we are the only statement currently opened for this
		        // connection; otherwise we don't want to affect other
		        // resultSet's by committing the MetaData one.
		        // There is no internal xact (xact isolation) for MetaData type
		        // of resultSet; therefore committing (to release locks) would end
		        // up committing all the other resultSet for this connection.
		        //
		        // We do synchronize on the connection, therefore Activation count
		        // should be valid and protected.
		        //
		        if (forMetaData) {

					LanguageConnectionContext lcc = getEmbedConnection().getLanguageConnection();
		        	if (lcc.getActivationCount() > 1) {
		     		  // we do not want to commit here as there seems to be other
					  // statements/resultSets currently opened for this connection.
					} else if (owningStmt != null)
						// allow the satement to commit if required.
		     			owningStmt.resultSetClosing(this);
		
				} else if (owningStmt != null) {
						// allow the satement to commit if required.
		     			owningStmt.resultSetClosing(this);
		     	}

			} finally {
				isClosed = true;
			    restoreContextStack();
			}

			// the idea is to release resources, so:
			currentRow = null;
			rowData = null;
			rMetaData = null; // let it go, we can make a new one

			// we hang on to theResults and messenger
			// in case more calls come in on this resultSet
		}

	}

    /**
     * A column may have the value of SQL NULL; wasNull reports whether
     * the last column read had this special value.
     * Note that you must first call getXXX on a column to try to read
     * its value and then call wasNull() to find if the value was
     * the SQL NULL.
     *
     * <p> we take the least exception approach and simply return false
     * if no column has been read yet.
     *
     * @return true if last column read was SQL NULL
	 *
	 * @exception SQLException		Thrown if this ResultSet is closed
     */
    public final boolean wasNull() throws SQLException {
		checkIfClosed("wasNull");
		return wasNull;
	}

    //======================================================================
    // Methods for accessing results by column index
    //======================================================================

    /**
     * Get the value of a column in the current row as a Java String.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final String getString(int columnIndex) throws SQLException {


			// never need to push a context stack because everything can be converted
			// into an String without a conversion error. With one exception! If
			// the type is OTHER (ie an object) then while Object.toString() cannot
			// throw an exception a user implementation of it could require the
			// current connection. In order to get the current connection the
			// context stack must be set up.
			try {

				DataValueDescriptor dvd = getColumn(columnIndex);

				if (wasNull = dvd.isNull())
					return null;

				int colType = jdbcColumnTypes[columnIndex];

				String value;

				if (colType == Types.OTHER || colType == org.apache.derby.iapi.reference.JDBC20Translation.SQL_TYPES_JAVA_OBJECT) {
					synchronized (getConnectionSynchronization()) {
						setupContextStack();
						try {
							value = dvd.getString();
						} finally {
							restoreContextStack();
						}
					}

				} else {
					value = dvd.getString();
				}

				// check for the max field size limit 
                if (maxFieldSize > 0 && isMaxFieldSizeType(colType))
                {
                    if (value.length() > maxFieldSize )
                    {
                        value = value.substring(0, maxFieldSize);
                    }
                }
     
				return value;

			} catch (Throwable t) {
				throw noStateChangeException(t);
			}
	}

    /**
     * Get the value of a column in the current row as a Java boolean.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is false
	 * @exception SQLException thrown on failure.
     */
    public final boolean getBoolean(int columnIndex) throws SQLException {


		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return false;

			return dvd.getBoolean();

		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

    /**
     * Get the value of a column in the current row as a Java byte.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final byte getByte(int columnIndex) throws SQLException {

		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return 0;

			return dvd.getByte();

		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

    /**
     * Get the value of a column in the current row as a Java short.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final short getShort(int columnIndex) throws SQLException {


		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return 0;

			return dvd.getShort();

		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

    /**
     * Get the value of a column in the current row as a Java int.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final int getInt(int columnIndex) throws SQLException	{

		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return 0;

			return dvd.getInt();

		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

    /**
     * Get the value of a column in the current row as a Java long.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final long getLong(int columnIndex) throws SQLException {

		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return 0;

			return dvd.getLong();

		} catch (StandardException t)	{
			throw noStateChangeException(t);
		}
	}

    /**
     * Get the value of a column in the current row as a Java float.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final float getFloat(int columnIndex) throws SQLException {

		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return 0.0F;

			return dvd.getFloat();

		} catch (StandardException t)	{
			throw noStateChangeException(t);
		}
	}

    /**
     * Get the value of a column in the current row as a Java double.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final double getDouble(int columnIndex) throws SQLException {

		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return 0.0;

			return dvd.getDouble();

		} catch (StandardException t)	{
			throw noStateChangeException(t);
		}
	}

    /**
     * Get the value of a column in the current row as a Java byte array.
     * The bytes represent the raw values returned by the driver.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final byte[] getBytes(int columnIndex) throws SQLException	{

		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return null;

			byte[] value = dvd.getBytes();

            // check for the max field size limit 
            if (maxFieldSize > 0 && isMaxFieldSizeType(jdbcColumnTypes[columnIndex]))
            {
                 if (value.length > maxFieldSize)
                 {
                     byte [] limited_value = new byte[maxFieldSize];
                     System.arraycopy(value, 0, limited_value, 
                                                   0 , maxFieldSize);
                     value = limited_value;
                 }
             }
			
			return value;

		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

    /**
     * Get the value of a column in the current row as a java.sql.Date object.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final Date getDate(int columnIndex) throws SQLException {
        return getDate( columnIndex, (Calendar) null);
	}

    /**
     * Get the value of a column in the current row as a java.sql.Time object.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final Time getTime(int columnIndex) throws SQLException {
        return getTime( columnIndex, (Calendar) null);
	}

    /**
     * Get the value of a column in the current row as a java.sql.Timestamp object.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp( columnIndex, (Calendar) null);
	}

    /**
     * JDBC 2.0
     *
     * Get the value of a column in the current row as a java.sql.Date 
     * object.  Use the calendar to construct an appropriate millisecond
     * value for the Date, if the underlying database doesn't store
     * timezone information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal the calendar to use in constructing the date
     * @return the column value; if the value is SQL NULL, the result is null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Date getDate(int columnIndex, Calendar cal)
        throws SQLException 
    {
		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return null;

            if( cal == null)
                cal = getCal();
            
			return dvd.getDate( cal);

		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
    }

    /**
     * JDBC 2.0
     *
     * Get the value of a column in the current row as a java.sql.Date 
     * object. Use the calendar to construct an appropriate millisecond
     * value for the Date, if the underlying database doesn't store
     * timezone information.
     *
     * @param columnName is the SQL name of the column
     * @param cal the calendar to use in constructing the date
     * @return the column value; if the value is SQL NULL, the result is null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Date getDate(String columnName, Calendar cal) 
                throws SQLException 
        {
                return getDate( findColumnName(columnName), cal);
        }

    /**
     * JDBC 2.0
     *
     * Get the value of a column in the current row as a java.sql.Time 
     * object. Use the calendar to construct an appropriate millisecond
     * value for the Time, if the underlying database doesn't store
     * timezone information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal the calendar to use in constructing the time
     * @return the column value; if the value is SQL NULL, the result is null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Time getTime(int columnIndex, Calendar cal)
        throws SQLException 
    {
		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return null;

            if( cal == null)
                cal = getCal();
			return dvd.getTime( cal);

		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
    }

    /**
     * JDBC 2.0
     *
     * Get the value of a column in the current row as a java.sql.Time 
     * object. Use the calendar to construct an appropriate millisecond
     * value for the Time, if the underlying database doesn't store
     * timezone information.
     *
     * @param columnName is the SQL name of the column
     * @param cal the calendar to use in constructing the time
     * @return the column value; if the value is SQL NULL, the result is null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Time getTime(String columnName, Calendar cal)
           throws SQLException 
        {
                return getTime( findColumnName( columnName), cal);
        }

    /**
     * JDBC 2.0
     *
     * Get the value of a column in the current row as a java.sql.Timestamp 
     * object. Use the calendar to construct an appropriate millisecond
     * value for the Timestamp, if the underlying database doesn't store
     * timezone information.
     *
     * @param columnName is the SQL name of the column
     * @param cal the calendar to use in constructing the timestamp
     * @return the column value; if the value is SQL NULL, the result is null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Timestamp getTimestamp(String columnName, Calendar cal)     
      throws SQLException 
        {
                return getTimestamp(findColumnName(columnName), cal);
        }

    /**
     * JDBC 2.0
     *
     * Get the value of a column in the current row as a java.sql.Timestamp 
     * object. Use the calendar to construct an appropriate millisecond
     * value for the Timestamp, if the underlying database doesn't store
     * timezone information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal the calendar to use in constructing the timestamp
     * @return the column value; if the value is SQL NULL, the result is null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Timestamp getTimestamp(int columnIndex, Calendar cal) 
        throws SQLException 
    {
		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return null;

            if( cal == null)
                cal = getCal();
			return dvd.getTimestamp( cal);

		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
    }

	/**
     * JDBC 2.0
     *
     * <p>Get the value of a column in the current row as a java.io.Reader.
     *
     * @exception SQLException database error.
     */
    public final java.io.Reader getCharacterStream(int columnIndex)
		throws SQLException
	{
		int lmfs;
		int colType = getColumnType(columnIndex);
		switch (colType) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			lmfs = maxFieldSize;
			break;
		case Types.CLOB: // Embedded and JCC extension - CLOB is not subject to max field size.
			lmfs = 0;
			break;

		// JDBC says to support these, but no defintion exists for the output.
		// match JCC which treats the bytes as a UTF16-BE stream
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		case Types.BLOB:
			try {
				java.io.InputStream is = getBinaryStream(columnIndex);
				if (is == null)
					return null;
				java.io.Reader r = new java.io.InputStreamReader(is, "UTF-16BE");
				currentStream = r;
				return r;
			} catch (java.io.UnsupportedEncodingException uee) {
				throw new SQLException(uee.getMessage());
			}
		default:
			throw dataTypeConversion("java.io.Reader", columnIndex);
		}

		Object syncLock = getConnectionSynchronization();

		synchronized (getConnectionSynchronization()) {

		boolean pushStack = false;
		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull()) { return null; }

			pushStack = true;
			setupContextStack();

			StreamStorable ss = (StreamStorable) dvd;

			InputStream stream = ss.returnStream();

			if (stream == null) {

				String val = dvd.getString();
				if (lmfs > 0) {
					if (val.length() > lmfs)
						val = val.substring(0, lmfs);
				}
				java.io.Reader ret = new java.io.StringReader(val);
				currentStream = ret;
				return ret;
			}

			java.io.Reader ret = new UTF8Reader(stream, lmfs, this, syncLock);
			currentStream = ret;
			return ret;

		} catch (Throwable t) {
			throw noStateChangeException(t);
		} finally {
			if (pushStack) { restoreContextStack(); }
		}
	  }
     }

    /**
		Pushes a converter on top of getCharacterStream().
	 *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value
     * as a stream of one byte ASCII characters.  If the value is SQL NULL
     * then the result is null.
	 * @exception SQLException thrown on failure.
     */
    public final InputStream getAsciiStream(int columnIndex) throws SQLException {

		int colType = getColumnType(columnIndex);
		switch (colType) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.CLOB: // Embedded and JCC extension
			break;

		// JDBC says to support these, we match JCC by returning the raw bytes.
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		case Types.BLOB:
			return getBinaryStream(columnIndex);

		default:
			throw dataTypeConversion("java.io.InputStream(ASCII)", columnIndex);
		}

		java.io.Reader reader = getCharacterStream(columnIndex);
		if (reader == null)
			return null;

		return new ReaderToAscii(reader);
	}

    /**
	 * Get the column as an InputStream. If the column is already of type
	   InputStream then just return it, otherwise convert the column to a set
	   of bytes and create a stream out of the bytes.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value
     * as a stream of uninterpreted bytes.  If the value is SQL NULL
     * then the result is null.
	 * @exception SQLException thrown on failure.
     */
    public final InputStream getBinaryStream(int columnIndex) throws SQLException {

		int lmfs;
		int colType = getColumnType(columnIndex);
		switch (colType) {
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			lmfs = maxFieldSize;
			break;
		case Types.BLOB:
			lmfs = 0;
			break;

		default:
			throw dataTypeConversion("java.io.InputStream", columnIndex);
		}

		Object syncLock = getConnectionSynchronization();

		synchronized (getConnectionSynchronization()) {

		boolean pushStack = false;
		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull()) { return null; }

			pushStack = true;
			setupContextStack();

			StreamStorable ss = (StreamStorable) dvd;

			InputStream stream = ss.returnStream();

			if (stream == null)
			{
				stream = new NewByteArrayInputStream(dvd.getBytes());
			} else
			{
				stream = new BinaryToRawStream(stream, dvd);
			}

            if (lmfs > 0)
            {
                // Just wrap the InputStream with a LimitInputStream class
                LimitInputStream  limitResultIn = new  LimitInputStream(stream);
                limitResultIn.setLimit(lmfs);
                stream = limitResultIn;
            }
			currentStream = stream;
			return stream;

		} catch (Throwable t) {
			throw noStateChangeException(t);
		} finally {
			if (pushStack) { restoreContextStack(); }
		}
	  }
	}

    //======================================================================
    // Methods for accessing results by column name
    //======================================================================


    /**
     * Get the value of a column in the current row as a Java String.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final String getString(String columnName) throws SQLException {
    	return (getString(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a Java boolean.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is false
	 * @exception SQLException thrown on failure.
     */
    public final boolean getBoolean(String columnName) throws SQLException {
    	return (getBoolean(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a Java byte.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final byte getByte(String columnName) throws SQLException	{
    	return (getByte(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a Java short.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final short getShort(String columnName) throws SQLException {
    	return (getShort(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a Java int.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final int getInt(String columnName) throws SQLException {
    	return (getInt(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a Java long.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final long getLong(String columnName) throws SQLException {
    	return (getLong(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a Java float.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final float getFloat(String columnName) throws SQLException {
    	return (getFloat(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a Java double.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is 0
	 * @exception SQLException thrown on failure.
     */
    public final double getDouble(String columnName) throws SQLException {
    	return (getDouble(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a Java byte array.
     * The bytes represent the raw values returned by the driver.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final byte[] getBytes(String columnName) throws SQLException {
    	return (getBytes(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a java.sql.Date object.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final Date getDate(String columnName) throws SQLException {
    	return (getDate(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a java.sql.Time object.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final Time getTime(String columnName) throws SQLException {
    	return (getTime(findColumnName(columnName)));
	}

    /**
     * Get the value of a column in the current row as a java.sql.Timestamp object.
     *
     * @param columnName is the SQL name of the column
     * @return the column value; if the value is SQL NULL, the result is null
	 * @exception SQLException thrown on failure.
     */
    public final Timestamp getTimestamp(String columnName) throws SQLException {
    	return (getTimestamp(findColumnName(columnName)));
	}

	/**
     * JDBC 2.0
     *
     * <p>Get the value of a column in the current row as a java.io.Reader.
         *
         * @exception SQLException Feature not implemented for now.
     */
    public final java.io.Reader getCharacterStream(String columnName)
    throws SQLException {
    	return (getCharacterStream(findColumnName(columnName)));
    }

    /**
     * A column value can be retrieved as a stream of ASCII characters
     * and then read in chunks from the stream.  This method is particularly
     * suitable for retrieving large LONGVARCHAR values.  The JDBC driver will
     * do any necessary conversion from the database format into ASCII.
     *
     * <P><B>Note:</B> All the data in the returned stream must
     * be read prior to getting the value of any other column. The
     * next call to a get method implicitly closes the stream.
     *
     * @param columnName is the SQL name of the column
     * @return a Java input stream that delivers the database column value
     * as a stream of one byte ASCII characters.  If the value is SQL NULL
     * then the result is null.
	 * @exception SQLException thrown on failure.
     */
    public final InputStream getAsciiStream(String columnName) throws SQLException {
    	return (getAsciiStream(findColumnName(columnName)));
	}

    /**
     * A column value can be retrieved as a stream of uninterpreted bytes
     * and then read in chunks from the stream.  This method is particularly
     * suitable for retrieving large LONGVARBINARY values.
     *
     * <P><B>Note:</B> All the data in the returned stream must
     * be read prior to getting the value of any other column. The
     * next call to a get method implicitly closes the stream.
     *
     * @param columnName is the SQL name of the column
     * @return a Java input stream that delivers the database column value
     * as a stream of uninterpreted bytes.  If the value is SQL NULL
     * then the result is null.
	 * @exception SQLException thrown on failure.
     */
    public final InputStream getBinaryStream(String columnName) throws SQLException {
    	return (getBinaryStream(findColumnName(columnName)));
	}


    //=====================================================================
    // Advanced features:
    //=====================================================================

    /**
     * <p>The first warning reported by calls on this ResultSet is
     * returned. Subsequent ResultSet warnings will be chained to this
     * SQLWarning.
     *
     * <P>The warning chain is automatically cleared each time a new
     * row is read.
     *
     * <P><B>Note:</B> This warning chain only covers warnings caused
     * by ResultSet methods.  Any warning caused by statement methods
     * (such as reading OUT parameters) will be chained on the
     * Statement object.
     *
     * @return the first SQLWarning or null
	 *
	 * @exception SQLException 	Thrown if this ResultSet is closed
     */
    public final SQLWarning getWarnings() throws SQLException {
		checkIfClosed("getWarnings");
		return topWarning;
	}

    /**
     * After this call getWarnings returns null until a new warning is
     * reported for this ResultSet.
	 *
	 * @exception SQLException	Thrown if this ResultSet is closed
     */
    public final void clearWarnings() throws SQLException {
		checkIfClosed("clearWarnings");
		topWarning = null;
	}

    /**
     * Get the name of the SQL cursor used by this ResultSet.
     *
     * <P>In SQL, a result table is retrieved through a cursor that is
     * named. The current row of a result can be updated or deleted
     * using a positioned update/delete statement that references the
     * cursor name.
     *
     * <P>JDBC supports this SQL feature by providing the name of the
     * SQL cursor used by a ResultSet. The current row of a ResultSet
     * is also the current row of this SQL cursor.
     *
     * <P><B>Note:</B> If positioned update is not supported a
     * SQLException is thrown
     *
     * @return the ResultSet's SQL cursor name
	 * @exception SQLException thrown on failure.
     */
    public final String getCursorName() throws SQLException {

	  checkIfClosed("getCursorName");	// checking result set closure does not depend
								// on the underlying connection.  Do this
								// outside of the connection synchronization.
	  
	  return theResults.getCursorName();
	}

    /**
     * The number, types and properties of a ResultSet's columns
     * are provided by the getMetaData method.
     *
     * @return the description of a ResultSet's columns
	 * @exception SQLException thrown on failure.
     */
    public ResultSetMetaData getMetaData() throws SQLException {

	  checkIfClosed("getMetaData");	// checking result set closure does not depend
								// on the underlying connection.  Do this
								// outside of the connection synchronization.

	  synchronized (getConnectionSynchronization()) {


		if (rMetaData == null) {
			// cache this object and keep returning it
			rMetaData = newEmbedResultSetMetaData(resultDescription);
		}
		return rMetaData;
	  }
	}

    /**
     * <p>Get the value of a column in the current row as a Java object.
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
     * Java Object type corresponding to the column's SQL type,
     * following the mapping specified in the JDBC spec.
     *
     * <p>This method may also be used to read datatabase specific abstract
     * data types.
	 *
	 * JDBC 2.0
     *
     * New behavior for getObject().
     * The behavior of method getObject() is extended to materialize  
     * data of SQL user-defined types.  When the column @columnIndex is 
     * a structured or distinct value, the behavior of this method is as 
     * if it were a call to: getObject(columnIndex, 
     * this.getStatement().getConnection().getTypeMap()).
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return A java.lang.Object holding the column value.
	 * @exception SQLException thrown on failure.
     */
    public final Object getObject(int columnIndex) throws SQLException {


		// need special handling for some types.
		int colType = getColumnType(columnIndex);
		switch (colType) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			// handles maxfield size correctly
			return getString(columnIndex);

		case Types.CLOB:
			return getClob(columnIndex);

		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			// handles maxfield size correctly
			return getBytes(columnIndex);

		case Types.BLOB:
			return getBlob(columnIndex);

		default:
			break;
		}

		try {

			DataValueDescriptor dvd = getColumn(columnIndex);
			if (wasNull = dvd.isNull())
				return null;

			return dvd.getObject();

		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

    /**
     * <p>Get the value of a column in the current row as a Java object.
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
     * Java Object type corresponding to the column's SQL type,
     * following the mapping specified in the JDBC spec.
     *
     * <p>This method may also be used to read datatabase specific abstract
     * data types.
     *
     * JDBC 2.0
     *
     * New behavior for getObject().
     * The behavior of method getObject() is extended to materialize  
     * data of SQL user-defined types.  When the column @columnName is 
     * a structured or distinct value, the behavior of this method is as 
     * if it were a call to: getObject(columnName, 
     * this.getStatement().getConnection().getTypeMap()).
     *
     * @param columnName is the SQL name of the column
     * @return A java.lang.Object holding the column value.
	 * @exception SQLException thrown on failure.
     */
    public final Object getObject(String columnName) throws SQLException {
    	return (getObject(findColumnName(columnName)));
	}


    //----------------------------------------------------------------

    /**
     * Map a Resultset column name to a ResultSet column index.
     *
     * @param columnName the name of the column
     * @return the column index
	 * @exception SQLException thrown on failure.
     */
	public final int findColumn(String columnName) throws SQLException {
		checkIfClosed("findColumn");
		return findColumnName(columnName);
	}

    /**
     * Map a Resultset column name to a ResultSet column index.
     *
     * @param columnName the name of the column
	 * @param operation		the operation the caller is trying to do
	 *						(for error reporting).  Null means don't do error
	 *						checking.
     * @return the column index
	 * @exception SQLException thrown on failure.
     */
    protected int findColumnName(String columnName)
						throws SQLException {
		// n.b. if we went through the JDBC interface,
		// there is a caching implementation in the JDBC doc
		// (appendix C).  But we go through our own info, for now.
		// REVISIT: we might want to cache our own info...
		

		if (columnName == null)
			throw newSQLException(SQLState.NULL_COLUMN_NAME);

		ResultDescription rd = resultDescription;

    	// 1 or 0 based? assume 1 (probably wrong)
    	for (int i=rd.getColumnCount(); i>=1; i--) {

    		String name = rd.getColumnDescriptor(i).getName();
    		if (StringUtil.SQLEqualsIgnoreCase(columnName, name)) {
    			return i;
    		}
    	}
    	throw newSQLException(SQLState.COLUMN_NOT_FOUND, columnName);
	}



	//
	// methods to be overridden in subimplementations
	// that want to stay within their subimplementation.
	//
	protected EmbedResultSetMetaData newEmbedResultSetMetaData(ResultDescription resultDesc) {
		return new EmbedResultSetMetaData(resultDesc.getColumnInfo());
	}

	/**
		Documented behaviour for streams is that they are implicitly closed
		on the next get*() method call.
	*/
	protected final void closeCurrentStream() {

		if (currentStream != null) {
			try {
				// 99% of the time, the stream is already closed.
				synchronized(this)
				{
					if (currentStream != null) {
						if (currentStream instanceof java.io.Reader)
							((java.io.Reader) currentStream).close();
						else
							((java.io.InputStream) currentStream).close();
					}
				}
			} catch (IOException ioe) {
				// just ignore, caller has already read the data they require
			} finally {
				currentStream = null;
			}
		}
	}

	/**
	 * Throw an exception if this ResultSet is closed.
	 *
	 * @param operation		The operation the caller is trying to perform
	 *
	 * @exception SQLException		Thrown if this ResultSet is closed.
	 */
	protected final void checkIfClosed(String operation) throws SQLException {
		if (isClosed) {
			throw newSQLException(SQLState.LANG_RESULT_SET_NOT_OPEN, operation);
		}
	}

	protected final void checkExecIfClosed(String operation) throws SQLException {
		
		checkIfClosed(operation);

		java.sql.Connection appConn = getEmbedConnection().getApplicationConnection();

		if ((appConn == null) || appConn.isClosed())
			throw Util.noCurrentConnection();
	}

	/**
	 * Try to see if we can fish the SQL Statement out of the local statement.
	 * @return null if we cannot figure out what SQL Statement is currently
	 *  executing
	 */
	protected String getSQLText()
	{
		if (stmt == null)
			return null;

		return stmt.getSQLText();
	}

	/**
	 * Try to see if we can fish the pvs out of the local statement.
	 * @return null if we cannot figure out what parameter value set is currently
	 *  using
	 */
	protected ParameterValueSet getParameterValueSet()
	{
		if (stmt == null)
			return null;

		return stmt.getParameterValueSet();
	}

    private static boolean isMaxFieldSizeType(int colType){
        return (colType == Types.BINARY || colType == Types.VARBINARY || 
            colType == Types.LONGVARBINARY || colType == Types.CHAR ||
            colType == Types.VARCHAR || colType == Types.LONGVARCHAR);
    }
	/*
	 * close result set if we have a transaction level error 
	 */
	protected final SQLException closeOnTransactionError(Throwable thrownException) throws SQLException
	{
		SQLException sqle = handleException(thrownException);
		if (thrownException instanceof StandardException)
		{
			StandardException se = (StandardException) thrownException;
			int severity = se.getSeverity();
			if (severity == ExceptionSeverity.TRANSACTION_SEVERITY)
			{
				try {
					close();
	    		} catch (Throwable t) {
		        	SQLException top = handleException(t);
					top.setNextException(sqle);
					sqle = top;
				}
			}
		}

		return sqle;
	}


	/**
		Get the column value for a getXXX() call.
		This method:
		<UL>
		<LI> Closes the current stream (as per JDBC)
		<LI> Throws a SQLException if the result set is closed
		<LI> Throws a SQLException if the ResultSet is not on a row
		<LI> Throws a SQLException if the columnIndex is out of range
		<LI> Returns the DataValueDescriptor for the column.
		</UL>
	*/
	protected final DataValueDescriptor getColumn(int columnIndex)
		throws SQLException, StandardException {

	  closeCurrentStream();

	  checkIfClosed("getXXX");

	   DataValueDescriptor[] theCurrentRow = checkOnRow(); // first make sure there's a row
		
	   //if (columnIndex < 1 ||
		//    columnIndex >= jdbcColumnTypes.length)
		//	

	   try {
		   return theCurrentRow[columnIndex - 1];
	   } catch (ArrayIndexOutOfBoundsException aoobe) {
			throw newSQLException(SQLState.COLUMN_NOT_FOUND, 
				                new Integer(columnIndex));
	   }

	   // return theCurrentRow.getColumn(columnIndex);
	}


	/**
		An exception on many method calls to JDBC objects does not change the state
		of the transaction or statement, or even the underlying object. This method
		simply wraps the excecption in a SQLException. Examples are:
		<UL>
		<LI> getXXX() calls on ResultSet - ResultSet is not closed.
		<LI> setXXX() calls on PreparedStatement - ResultSet is not closed.
		</UL>
		In addition these exceptions must not call higher level objects to
		be closed (e.g. when executing a server side Java procedure). See bug 4397

	*/
	public static final SQLException noStateChangeException(Throwable thrownException) {

		// Any exception on a setXXX/getXXX method does not close
		// the ResultSet or the Statement. So we only need
		// to convert the exception to a SQLException.

		return TransactionResourceImpl.wrapInSQLException((SQLException) null, thrownException);

	}

	/**
		A dynamic result set was created in a procedure by a nested connection.
		Once the procedure returns, there is a good chance that connection is closed,
		so we re-attach the result set to the connection of the statement the called
		the procedure, which will be still open.
	*/
	void setDynamicResultSet(EmbedStatement owningStmt) {

		this.owningStmt = owningStmt;
		this.localConn = owningStmt.getEmbedConnection();
	}

	/*
	** Comparable (for ordering dynamic result sets from procedures) 
	*/

	public final int compareTo(Object other) {

		EmbedResultSet olrs = (EmbedResultSet) other;

		return order - olrs.order;

	}

	/**
	**  Is this result set from a select for update statement?
	*/
	public final boolean isForUpdate()
	{
		if (theResults instanceof NoPutResultSet)
			return ((NoPutResultSet) theResults).isForUpdate();
		return false;
	}

	protected final SQLException dataTypeConversion(String targetType, int column) {
		return newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH, targetType,
			resultDescription.getColumnDescriptor(column).getType().getTypeId().getSQLTypeName());
	}
}

