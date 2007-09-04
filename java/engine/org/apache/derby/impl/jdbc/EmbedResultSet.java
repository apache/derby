/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedResultSet

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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

import org.apache.derby.iapi.jdbc.EngineResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecCursorTableReference;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.impl.sql.execute.ScrollInsensitiveResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorActivation;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RawToBinaryFormatStream;
import org.apache.derby.iapi.types.ReaderToUTF8Stream;
import org.apache.derby.iapi.types.UserDataValue;
import org.apache.derby.iapi.types.VariableSizeDataValue;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.services.io.StreamStorable;

import org.apache.derby.iapi.services.io.LimitInputStream;
import org.apache.derby.iapi.services.io.NewByteArrayInputStream;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.reference.JDBC20Translation;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.SQLState;

/* can't import these due to name overlap:
import java.sql.ResultSet;
*/
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.ResultSetMetaData;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import java.io.Reader;
import java.io.InputStream;
import java.io.IOException;
import java.net.URL;

import java.util.Arrays;
import java.util.Calendar;

/**
 * A EmbedResultSet for results from the EmbedStatement family. 
    <P><B>Supports</B>
   <UL>
   <LI> JSR 169
   </UL>
 */

public abstract class EmbedResultSet extends ConnectionChild 
    implements EngineResultSet, Comparable {

	// cursor movement
	protected static final int FIRST = 1;
	protected static final int NEXT = 2;
	protected static final int LAST = 3;
	protected static final int PREVIOUS = 4;
	protected static final int BEFOREFIRST = 5;
	protected static final int AFTERLAST = 6;
	protected static final int ABSOLUTE = 7;
	protected static final int RELATIVE = 8;

	/** 
	 * The currentRow contains the data of the current row of the resultset.
	 * If currentRow is null, the cursor is not postioned on a row 
	 */
	private ExecRow currentRow;	
	protected boolean wasNull;
    
    /**
     * Set if this ResultSet is definitely closed.
     * If the connection has been closed, or the database
     *  or system shutdown but the ResultSet has not been
     *  closed explictly then this may be false. Once
     *  this object detects the connection is closed
     *  isClosed will be set to true.
     */
    boolean isClosed;
    
	private boolean isOnInsertRow;
	private Object	currentStream;

	// immutable state
	private ResultSet theResults;
	private boolean forMetaData;
	private SQLWarning topWarning;

	/**
	 This activation is set by EmbedStatement
	 for a single execution Activation. Ie.
	 a ResultSet from a Statement.executeQuery().
	 In this case the closing of this ResultSet will close
	 the activation or the finalization of the parent EmbedStatement
	 without it being closed will mark the Activation as unused.
	 @see EmbedStatement#finalize()
	 @see EmbedPreparedStatement#finalize()
    */
	Activation singleUseActivation;

	// Order of creation 
	final int order;

  
	private final ResultDescription resultDescription;

    // max rows limit for this result set
    private int maxRows;
    // The Maximum field size limt set for this result set
    private final int maxFieldSize;

    /*
     * Incase of forward only cursors we limit the number of rows
     * returned if the maxRows is set. The following varible is used
     * to keep the count of number of rows returned to the user.
     */
    private int NumberofFetchedRows;


	/**
     * The statement object that originally created us.
		we hang on to the statement to prevent GC from
		closing it under us
	 */
	private final EmbedStatement stmt;
    
    /**
     * The statement that currently owns this ResultSet.
     * Statements created in procedures are passed off
     * to the Statement that called the procedure.
     * This is to avoid the ResultSet being closed
     * due to the Statement within the procedure
     * or the nested Connection being closed.
     */
	private EmbedStatement owningStmt;
    
    /**
     * Statement object the application used to
     * create this ResultSet.
     */
    private Statement applicationStmt;
    
    private final long timeoutMillis;

	private final boolean isAtomic;

	private final int concurrencyOfThisResultSet;

	/* updateRow is used to keep the values which are updated with updateXXX() 
	 * calls. It is used by both insertRow() and updateRow(). 
	 * It is initialized to null if the resultset is not updatable. 
	 */
	private final ExecRow updateRow;
	
	/* These are the columns which have been updated so far. 
	 */
	private boolean[] columnGotUpdated; 
	private boolean currentRowHasBeenUpdated; //Gets set to true after first updateXXX on a row. Gets reset to false when the cursor moves off the row

    private int fetchDirection;
    private int fetchSize;
    
    /**
     * Indicates which columns have already been fetched
     * as a stream for a row. Created on-demand by a getXXXStream call.
     */
    private boolean[] streamUsedFlags;
    
	/**
	 * This class provides the glue between the Derby
	 * resultset and the JDBC resultset, mapping calls-to-calls.
	 */
	public EmbedResultSet(EmbedConnection conn, ResultSet resultsToWrap,
		boolean forMetaData, EmbedStatement stmt, boolean isAtomic)
        throws SQLException {

		super(conn);

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(resultsToWrap!=null);
		theResults = resultsToWrap;
		
		// ResultSet's for metadata are single use, they are created
		// with a PreparedStatement internally, but that statement is
		// never returned to the application.
		if (this.forMetaData = forMetaData)
			singleUseActivation = resultsToWrap.getActivation();
        this.applicationStmt = this.stmt = owningStmt = stmt;

        this.timeoutMillis = stmt == null
            ? 0L
            : stmt.timeoutMillis;

		this.isAtomic = isAtomic;
                

		//If the Statement object has CONCUR_READ_ONLY set on it then the concurrency on the ResultSet object will be CONCUR_READ_ONLY also.
		//But, if the Statement object has CONCUR_UPDATABLE set on it, then the concurrency on the ResultSet object can be
		//CONCUR_READ_ONLY or CONCUR_UPDATABLE depending on whether the underlying language resultset is updateable or not.
		//If the underlying language resultset is not updateable, then the concurrency of the ResultSet object will be CONCUR_READ_ONLY
		//and a warning will be issued on the ResultSet object.
		if (stmt == null)
			concurrencyOfThisResultSet = JDBC20Translation.CONCUR_READ_ONLY;
		else if (stmt.resultSetConcurrency == JDBC20Translation.CONCUR_READ_ONLY)
			concurrencyOfThisResultSet = JDBC20Translation.CONCUR_READ_ONLY;
		else {
			if (!isForUpdate()) { //language resultset not updatable
				concurrencyOfThisResultSet = JDBC20Translation.CONCUR_READ_ONLY;
				SQLWarning w = StandardException.newWarning(SQLState.QUERY_NOT_QUALIFIED_FOR_UPDATABLE_RESULTSET);
				addWarning(w);
			} else
					concurrencyOfThisResultSet = JDBC20Translation.CONCUR_UPDATABLE;
		}

		// Fill in the column types
		resultDescription = theResults.getActivation().getResultDescription();
		
		// Only incur the cost of allocating and maintaining
		// updated column information if the columns can be updated.
		if (concurrencyOfThisResultSet == JDBC20Translation.CONCUR_UPDATABLE)
		{
            final int columnCount = resultDescription.getColumnCount();
            final ExecutionFactory factory = conn.getLanguageConnection().
            getLanguageConnectionFactory().getExecutionFactory();
            
			try{
				//initialize arrays related to updateRow implementation
				columnGotUpdated = new boolean[columnCount];
				updateRow = factory.getValueRow(columnCount);
				for (int i = 1; i <= columnCount; i++) {
					updateRow.setColumn(i, resultDescription.getColumnDescriptor(i).
										getType().getNull());
				}
				initializeUpdateRowModifiers();
			} catch (StandardException t) {
				throw noStateChangeException(t);
			}
		}
        else
        {
            updateRow = null;
        }

        // assign the max rows and maxfiled size limit for this result set
        if (stmt != null)
        {
           // At connectivity level we handle only for forward only cursor
           if (stmt.resultSetType == JDBC20Translation.TYPE_FORWARD_ONLY)
               maxRows = stmt.maxRows;

           maxFieldSize = stmt.MaxFieldSize;
        }
		else
			maxFieldSize = 0;

		order = conn.getResultSetOrderId();
	}

	private void checkNotOnInsertRow() throws SQLException {
		if (isOnInsertRow) {
			throw newSQLException(SQLState.NO_CURRENT_ROW);
		}
	}

	// checkOnRow protects us from making requests of
	// resultSet that would fail with NullPointerExceptions
	// or milder problems due to not having a row.
	protected final void checkOnRow() throws SQLException 
	{
		if (currentRow == null) {
			throw newSQLException(SQLState.NO_CURRENT_ROW);
		} 
	}

	/**
	 * Initializes the currentRowHasBeenUpdated and columnGotUpdated fields
	 */
	private void initializeUpdateRowModifiers() {
		currentRowHasBeenUpdated = false;
		Arrays.fill(columnGotUpdated, false);
	}

	/**
		Check the column is in range *and* return the JDBC type of the column.

		@exception SQLException ResultSet is not on a row or columnIndex is out of range.
	*/
	final int getColumnType(int columnIndex) throws SQLException {
		if (!isOnInsertRow) checkOnRow(); // first make sure there's a row
		
		if (columnIndex < 1 ||
		    columnIndex > resultDescription.getColumnCount())
			throw newSQLException(SQLState.COLUMN_NOT_FOUND, 
                         new Integer(columnIndex));

		return resultDescription.getColumnDescriptor(columnIndex).getType().getJDBCTypeId();
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

		if (isOnInsertRow) {
			moveToCurrentRow();
		}


		synchronized (getConnectionSynchronization()) {

					setupContextStack();
		    try {
				LanguageConnectionContext lcc = getEmbedConnection().getLanguageConnection();
				final ExecRow newRow;
		    try {

				/* Push and pop a StatementContext around a next call
				 * so that the ResultSet will get correctly closed down
				 * on an error.
				 * (Cache the LanguageConnectionContext)
				 */
                StatementContext statementContext =
                    lcc.pushStatementContext(isAtomic, 
					     concurrencyOfThisResultSet==JDBC20Translation.CONCUR_READ_ONLY, 
					     getSQLText(),
					     getParameterValueSet(),
                                             false, timeoutMillis);

				switch (position)
				{
					case BEFOREFIRST:
						newRow = theResults.setBeforeFirstRow();
						break;

					case FIRST:
						newRow = theResults.getFirstRow();
						break;

					case NEXT:
						newRow = theResults.getNextRow();
						break;

					case LAST:
						newRow = theResults.getLastRow();
						break;

					case AFTERLAST:
						newRow = theResults.setAfterLastRow();
						break;

					case PREVIOUS:
						newRow = theResults.getPreviousRow();
						break;

					case ABSOLUTE:
						newRow = theResults.getAbsoluteRow(row);
						break;

					case RELATIVE:
						newRow = theResults.getRelativeRow(row);
						break;

					default:
						newRow = null;
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
			
            boolean onRow = (currentRow = newRow) != null;			

			//if (onRow && !(currentRow instanceof org.apache.derby.impl.sql.execute.ValueRow))
			//	System.out.println(currentRow.getClass());

		    // The ResultSet may implicitly close when when the ResultSet type 
		    // is TYPE_FORWARD_ONLY and the next method of ResultSet returns 
		    // false. This will cause a commit if autocommit = true.
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
		     } else if (owningStmt != null && 
		    		owningStmt.getResultSetType() == TYPE_FORWARD_ONLY) {
				 // allow the satement to commit if required.
		     	owningStmt.resultSetClosing(this);
		     }
		    }

			// Clear the indication of which columns were fetched as streams.
			if (streamUsedFlags != null)
			    Arrays.fill(streamUsedFlags, false);
			if (columnGotUpdated != null && currentRowHasBeenUpdated) {
				initializeUpdateRowModifiers();
			}
			
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
				    
				    if (this.singleUseActivation != null)
				    {
				    	this.singleUseActivation.close();
				    	this.singleUseActivation = null;
				    }
				    
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
        checkIfClosed("getString");

			try {

				DataValueDescriptor dvd = getColumn(columnIndex);

				if (wasNull = dvd.isNull())
					return null;

				String value = dvd.getString();

				// check for the max field size limit 
                if (maxFieldSize > 0 && isMaxFieldSizeType(getColumnType(columnIndex)))
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
		checkIfClosed("getBoolean");

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
		checkIfClosed("getByte");
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
		checkIfClosed("getShort");

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
		checkIfClosed("getInt");
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
		checkIfClosed("getLong");
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
		checkIfClosed("getFloat");
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
		checkIfClosed("getDouble");
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
		checkIfClosed("getBytes");
		try {

			DataValueDescriptor dvd = getColumn(columnIndex);

			if (wasNull = dvd.isNull())
				return null;

			byte[] value = dvd.getBytes();

            // check for the max field size limit 
            if (maxFieldSize > 0 && isMaxFieldSizeType(getColumnType(columnIndex)))
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
		checkIfClosed("getDate");
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
                checkIfClosed("getDate");
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
		checkIfClosed("getTime");
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
                checkIfClosed("getTime");
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
                checkIfClosed("getTimestamp");
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
		checkIfClosed("getTimestamp");
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
		checkIfClosed("getCharacterStream");
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

		synchronized (syncLock) {

		boolean pushStack = false;
		try {

		    useStream(columnIndex);

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
		checkIfClosed("getAsciiStream");
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
		checkIfClosed("getBinaryStream");
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

		synchronized (syncLock) {

		boolean pushStack = false;
		try {
		    
		    useStream(columnIndex);

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
        checkIfClosed("getString");
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
        checkIfClosed("getBoolean");
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
        checkIfClosed("getByte");
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
        checkIfClosed("getShort");
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
        checkIfClosed("getInt");
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
        checkIfClosed("getLong");
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
        checkIfClosed("getFloat");
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
        checkIfClosed("getDouble");
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
        checkIfClosed("getBytes");
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
        checkIfClosed("getDate");
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
        checkIfClosed("getTime");
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
        checkIfClosed("getTimestamp");
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
        checkIfClosed("getCharacterStream");
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
        checkIfClosed("getAsciiStream");
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
        checkIfClosed("getBinaryStream");
    	return (getBinaryStream(findColumnName(columnName)));
	}

    /**
	 * JDBC 3.0
	 * 
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as a java.net.URL object in the Java programming
	 * language.
	 * 
	 * @param columnIndex -
	 *            the first column is 1, the second is 2
	 * @return the column value as a java.net.URL object, if the value is SQL
	 *         NULL, the value returned is null in the Java programming language
	 * @exception SQLException
	 *                Feature not implemented for now.
	 */
	public URL getURL(int columnIndex) throws SQLException {
		throw Util.notImplemented();
	}

	/**
	 * JDBC 3.0
	 * 
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as a java.net.URL object in the Java programming
	 * language.
	 * 
	 * @param columnName -
	 *            the SQL name of the column
	 * @return the column value as a java.net.URL object, if the value is SQL
	 *         NULL, the value returned is null in the Java programming language
	 * @exception SQLException
	 *                Feature not implemented for now.
	 */
	public URL getURL(String columnName) throws SQLException {
		throw Util.notImplemented();
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
    public final ResultSetMetaData getMetaData() throws SQLException {

	  checkIfClosed("getMetaData");	// checking result set closure does not depend
								// on the underlying connection.

      ResultSetMetaData rMetaData =
          resultDescription.getMetaData();

		if (rMetaData == null) {
			// save this object at the plan level
			rMetaData = factory.newEmbedResultSetMetaData(
                    resultDescription.getColumnInfo());
            resultDescription.setMetaData(rMetaData);
		}
		return rMetaData;
	}
    
    /**
     * JDBC 4.0
     * 
     * <p>
     * Retrieves the holdability for this <code>ResultSet</code>
     * object.
     * 
     * @return either <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code>
     *         or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @exception SQLException
     *                if a database error occurs
     */
    public final int getHoldability() throws SQLException {
        checkIfClosed("getHoldability");
        if (theResults.getActivation().getResultSetHoldability()) {
            return JDBC30Translation.HOLD_CURSORS_OVER_COMMIT;
        }
        return JDBC30Translation.CLOSE_CURSORS_AT_COMMIT;
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
        checkIfClosed("getObject");

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
        checkIfClosed("getObject");
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
	
    /////////////////////////////////////////////////////////////////////////
    //
    //      JDBC 2.0        -       New public methods
    //
    /////////////////////////////////////////////////////////////////////////


	//---------------------------------------------------------------------
	// Getter's and Setter's
	//---------------------------------------------------------------------

	/**
	 * JDBC 2.0
	 * 
	 * Return the Statement that produced the ResultSet.
	 * 
	 * @return the Statment that produced the result set, or null if the result
	 *         was produced some other way.
	 * @exception SQLException if a database error occurs or the
	 * result set is closed
	 */
	public final Statement getStatement() throws SQLException
    {
            checkIfClosed("getStatement");
            return applicationStmt;
    }
    
    /**
     * Set the application Statement object that created this ResultSet.
     * Used when the Statement objects returned to the application
     * are wrapped for XA.
     */
    public final void setApplicationStatement(Statement applicationStmt)
    {
        this.applicationStmt = applicationStmt;
    }

	//---------------------------------------------------------------------
	// Traversal/Positioning
	//---------------------------------------------------------------------

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Determine if the cursor is before the first row in the result set.
	 * 
	 * @return true if before the first row, false otherwise. Returns false when
	 *         the result set contains no rows.
	 * @exception SQLException
	 *                Thrown on error.
	 */
	public boolean isBeforeFirst() throws SQLException {
		return checkRowPosition(ResultSet.ISBEFOREFIRST, "isBeforeFirst");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Determine if the cursor is after the last row in the result set.
	 * 
	 * @return true if after the last row, false otherwise. Returns false when
	 *         the result set contains no rows.
	 * @exception SQLException
	 *                Thrown on error.
	 */
	public boolean isAfterLast() throws SQLException {
		return checkRowPosition(ResultSet.ISAFTERLAST, "isAfterLast");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Determine if the cursor is on the first row of the result set.
	 * 
	 * @return true if on the first row, false otherwise.
	 * @exception SQLException
	 *                Thrown on error.
	 */
	public boolean isFirst() throws SQLException {
		return checkRowPosition(ResultSet.ISFIRST, "isFirst");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Determine if the cursor is on the last row of the result set. Note:
	 * Calling isLast() may be expensive since the JDBC driver might need to
	 * fetch ahead one row in order to determine whether the current row is the
	 * last row in the result set.
	 * 
	 * @return true if on the last row, false otherwise.
	 * @exception SQLException
	 *                Thrown on error.
	 */
	public boolean isLast() throws SQLException {
		return checkRowPosition(ResultSet.ISLAST, "isLast");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Moves to the front of the result set, just before the first row. Has no
	 * effect if the result set contains no rows.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWARD_ONLY
	 */
	public void beforeFirst() throws SQLException {
		// beforeFirst is only allowed on scroll cursors
		checkScrollCursor("beforeFirst()");
		movePosition(BEFOREFIRST, "beforeFirst");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Moves to the end of the result set, just after the last row. Has no
	 * effect if the result set contains no rows.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWARD_ONLY.
	 */
	public void afterLast() throws SQLException {
		// afterLast is only allowed on scroll cursors
		checkScrollCursor("afterLast()");
		movePosition(AFTERLAST, "afterLast");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Moves to the first row in the result set.
	 * 
	 * @return true if on a valid row, false if no rows in the result set.
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWARD_ONLY.
	 */
	public boolean first() throws SQLException {
		// first is only allowed on scroll cursors
		checkScrollCursor("first()");
		return movePosition(FIRST, "first");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Moves to the last row in the result set.
	 * 
	 * @return true if on a valid row, false if no rows in the result set.
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWARD_ONLY.
	 */
	public boolean last() throws SQLException {
		// last is only allowed on scroll cursors
		checkScrollCursor("last()");
		return movePosition(LAST, "last");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Determine the current row number. The first row is number 1, the second
	 * number 2, etc.
	 * 
	 * @return the current row number, else return 0 if there is no current row
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public int getRow() throws SQLException {
		// getRow() is only allowed on scroll cursors
		checkScrollCursor("getRow()");

		/*
		 * * We probably needn't bother getting the text of * the underlying
		 * statement but it is better to be * consistent and we aren't
		 * particularly worried * about performance of getRow().
		 */
		return theResults.getRowNumber();
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Move to an absolute row number in the result set.
	 * 
	 * <p>
	 * If row is positive, moves to an absolute row with respect to the
	 * beginning of the result set. The first row is row 1, the second is row 2,
	 * etc.
	 * 
	 * <p>
	 * If row is negative, moves to an absolute row position with respect to the
	 * end of result set. For example, calling absolute(-1) positions the cursor
	 * on the last row, absolute(-2) indicates the next-to-last row, etc.
	 * 
	 * <p>
	 * An attempt to position the cursor beyond the first/last row in the result
	 * set, leaves the cursor before/after the first/last row, respectively.
	 * 
	 * <p>
	 * Note: Calling absolute(1) is the same as calling first(). Calling
	 * absolute(-1) is the same as calling last().
	 * 
	 * @return true if on the result set, false if off.
	 * @exception SQLException
	 *                if a database-access error occurs, or row is 0, or result
	 *                set type is TYPE_FORWARD_ONLY.
	 */
	public boolean absolute(int row) throws SQLException {
		// absolute is only allowed on scroll cursors
		checkScrollCursor("absolute()");
		return movePosition(ABSOLUTE, row, "absolute");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Moves a relative number of rows, either positive or negative. Attempting
	 * to move beyond the first/last row in the result set positions the cursor
	 * before/after the the first/last row. Calling relative(0) is valid, but
	 * does not change the cursor position.
	 * 
	 * <p>
	 * Note: Calling relative(1) is different than calling next() since is makes
	 * sense to call next() when there is no current row, for example, when the
	 * cursor is positioned before the first row or after the last row of the
	 * result set.
	 * 
	 * @return true if on a row, false otherwise.
	 * @exception SQLException
	 *                if a database-access error occurs, or there is no current
	 *                row, or result set type is TYPE_FORWARD_ONLY.
	 */
	public boolean relative(int row) throws SQLException {
		// absolute is only allowed on scroll cursors
		checkScrollCursor("relative()");
		return movePosition(RELATIVE, row, "relative");
	}

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Moves to the previous row in the result set.
	 * 
	 * <p>
	 * Note: previous() is not the same as relative(-1) since it makes sense to
	 * call previous() when there is no current row.
	 * 
	 * @return true if on a valid row, false if off the result set.
	 * @exception SQLException
	 *                if a database-access error occurs, or result set type is
	 *                TYPE_FORWAR_DONLY.
	 */
	public boolean previous() throws SQLException {
		// previous is only allowed on scroll cursors
		checkScrollCursor("previous()");
		return movePosition(PREVIOUS, "previous");
	}

	//---------------------------------------------------------------------
	// Properties
	//---------------------------------------------------------------------

	/**
	 * JDBC 2.0
	 * 
	 * Give a hint as to the direction in which the rows in this result set will
	 * be processed. The initial value is determined by the statement that
	 * produced the result set. The fetch direction may be changed at any time.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs, or the result set type
	 *                is TYPE_FORWARD_ONLY and direction is not FETCH_FORWARD.
	 */
	public void setFetchDirection(int direction) throws SQLException {
		checkScrollCursor("setFetchDirection()");
		/*
		 * FetchDirection is meaningless to us. We just save it off and return
		 * the current value if asked.
		 */
		fetchDirection = direction;
	}

	/**
	 * JDBC 2.0
	 * 
	 * Return the fetch direction for this result set.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public int getFetchDirection() throws SQLException {
		checkIfClosed("getFetchDirection");
		if (fetchDirection == 0) {
			// value is not set at the result set level
			// get it from the statement level
			return stmt.getFetchDirection();
		} else
			return fetchDirection;
	}

	/**
	 * JDBC 2.0
	 * 
	 * Give the JDBC driver a hint as to the number of rows that should be
	 * fetched from the database when more rows are needed for this result set.
	 * If the fetch size specified is zero, then the JDBC driver ignores the
	 * value, and is free to make its own best guess as to what the fetch size
	 * should be. The default value is set by the statement that creates the
	 * result set. The fetch size may be changed at any time.
	 * 
	 * @param rows
	 *            the number of rows to fetch
	 * @exception SQLException
	 *                if a database-access error occurs, or the condition 0 <=
	 *                rows <= this.getMaxRows() is not satisfied.
	 */
	public void setFetchSize(int rows) throws SQLException {
		checkIfClosed("setFetchSize");
		if (rows < 0 || (stmt.getMaxRows() != 0 && rows > stmt.getMaxRows())) {
			throw Util.generateCsSQLException(SQLState.INVALID_FETCH_SIZE,
					new Integer(rows));
		} else if (rows > 0) // if it is zero ignore the call
		{
			fetchSize = rows;
		}
	}

	/**
	 * JDBC 2.0
	 * 
	 * Return the fetch size for this result set.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public int getFetchSize() throws SQLException {
		checkIfClosed("getFetchSize");
		if (fetchSize == 0) {
			// value is not set at the result set level
			//  get the default value from the statement
			return stmt.getFetchSize();
		} else
			return fetchSize;
	}

	/**
	 * JDBC 2.0
	 * 
	 * Return the type of this result set. The type is determined based on the
	 * statement that created the result set.
	 * 
	 * @return TYPE_FORWARD_ONLY, TYPE_SCROLL_INSENSITIVE, or
	 *         TYPE_SCROLL_SENSITIVE
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public int getType() throws SQLException {
		checkIfClosed("getType");
		return stmt.getResultSetType();
	}

	/**
	 * JDBC 2.0
	 * 
	 * Return the concurrency of this result set. The concurrency is determined
	 * as follows If Statement object has CONCUR_READ_ONLY concurrency, then
	 * ResultSet object will also have the CONCUR_READ_ONLY concurrency. But if
	 * Statement object has CONCUR_UPDATABLE concurrency, then the concurrency
	 * of ResultSet object depends on whether the underlying language resultset
	 * is updatable or not. If the language resultset is updatable, then JDBC
	 * ResultSet object will also have the CONCUR_UPDATABLE concurrency. If
	 * lanugage resultset is not updatable, then JDBC ResultSet object
	 * concurrency will be set to CONCUR_READ_ONLY.
	 * 
	 * @return the concurrency type, CONCUR_READ_ONLY, etc.
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public int getConcurrency() throws SQLException {
		checkIfClosed("getConcurrency");
		return concurrencyOfThisResultSet;
	}

    //---------------------------------------------------------------------
	// Updates
	//---------------------------------------------------------------------

	/**
	 * JDBC 2.0
	 * 
	 * Determine if the current row has been updated. The value returned depends
	 * on whether or not the result set can detect updates.
	 * 
	 * @return true if the row has been visibly updated by the owner or another,
	 *         and updates are detected
	 * @exception SQLException
	 *                if a database-access error occurs
	 * 
	 * @see EmbedDatabaseMetaData#updatesAreDetected
	 */
	public boolean rowUpdated() throws SQLException {
		checkIfClosed("rowUpdated");
		checkNotOnInsertRow();
		checkOnRow();

        boolean rvalue = false;

		try {
			if (isForUpdate() && 
					getType() == java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE) {
				rvalue = ((ScrollInsensitiveResultSet)theResults).isUpdated();
			}
		} catch (Throwable t) {
				handleException(t);
		}
		return rvalue;
	}

	/**
	 * JDBC 2.0
	 * 
	 * Determine if the current row has been inserted. The value returned
	 * depends on whether or not the result set can detect visible inserts.
	 * 
	 * @return true if inserted and inserts are detected
	 * @exception SQLException
	 *                if a database-access error occurs
	 * 
	 * @see EmbedDatabaseMetaData#insertsAreDetected
	 */
	public boolean rowInserted() throws SQLException {
		checkIfClosed("rowInserted");
		checkNotOnInsertRow();
		checkOnRow();

		return false;
	}

	/**
	 * JDBC 2.0
	 *
	 * Determine if this row has been deleted. A deleted row may leave a visible
	 * "hole" in a result set. This method can be used to detect holes in a
	 * result set. The value returned depends on whether or not the result set
	 * can detect deletions.
	 *
	 * @return true if deleted and deletes are detected
	 * @exception SQLException
	 *                if a database-access error occurs
	 *
	 * @see EmbedDatabaseMetaData#deletesAreDetected
	 */
	public boolean rowDeleted() throws SQLException {
		checkIfClosed("rowUpdated");
		checkNotOnInsertRow();
		checkOnRow();

        boolean rvalue = false;

		try {
			if (isForUpdate() && 
					getType() == java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE) {
				rvalue = ((ScrollInsensitiveResultSet)theResults).isDeleted();
			}
		} catch (Throwable t) {
			handleException(t);
		}
		return rvalue;
	}

	//do following few checks before accepting updateXXX resultset api
	protected void checksBeforeUpdateXXX(String methodName, int columnIndex) throws SQLException {
      checksBeforeUpdateOrDelete(methodName, columnIndex);

      //1)Make sure for updateXXX methods, the column position is not out of range
      if (columnIndex < 1 || columnIndex > resultDescription.getColumnCount())
        throw Util.generateCsSQLException(SQLState.LANG_INVALID_COLUMN_POSITION,
					new Integer(columnIndex), String.valueOf(resultDescription.getColumnCount()));

      //2)Make sure the column corresponds to a column in the base table and it is not a derived column
      if (resultDescription.getColumnDescriptor(columnIndex).getSourceTableName() == null)
        throw Util.generateCsSQLException(SQLState.COLUMN_NOT_FROM_BASE_TABLE,
					methodName);

      //3)If column not updatable then throw an exception
      if (!getMetaData().isWritable(columnIndex))
        throw Util.generateCsSQLException(SQLState.LANG_COLUMN_NOT_UPDATABLE_IN_CURSOR,
					resultDescription.getColumnDescriptor(columnIndex).getName(),
					getCursorName());
	}

	//do following few checks before accepting updateRow or deleteRow
	//1)Make sure JDBC ResultSet is not closed
	//2)Make sure this is an updatable ResultSet
	//3)Make sure JDBC ResultSet is positioned on a row
	//4)Make sure underneath language resultset is not closed
	protected void checksBeforeUpdateOrDelete(String methodName, int columnIndex) throws SQLException {

      //1)Make sure JDBC ResultSet is not closed
      checkIfClosed(methodName);

      //2)Make sure this is an updatable ResultSet
      checkUpdatableCursor(methodName);

      //3)Make sure JDBC ResultSet is positioned on a row
      if (!isOnInsertRow) checkOnRow(); // make sure there's a current row
      //in case of autocommit on, if there was an exception which caused runtime rollback in this transaction prior to this call,
      //the rollback code will mark the language resultset closed (it doesn't mark the JDBC ResultSet closed).
      //That is why alongwith the earlier checkIfClosed call in this method, there is a check for language resultset close as well.

      //4)Make sure underneath language resultset is not closed
      if (theResults.isClosed())
        throw Util.generateCsSQLException(SQLState.LANG_RESULT_SET_NOT_OPEN, methodName);
	}

	//mark the column as updated and return DataValueDescriptor for it. It will be used by updateXXX methods to put new values
	protected DataValueDescriptor getDVDforColumnToBeUpdated(int columnIndex, String updateMethodName) throws StandardException, SQLException {
      checksBeforeUpdateXXX(updateMethodName, columnIndex);
      columnGotUpdated[columnIndex-1] = true;
      currentRowHasBeenUpdated = true;
      
      return updateRow.getColumn(columnIndex);
	}

    /* do following few checks before accepting insertRow
     * 1) Make sure JDBC ResultSet is not closed
     * 2) Make sure this is an updatable ResultSet
     * 3) Make sure JDBC ResultSet is positioned on insertRow
     * 4) Make sure underneath language resultset is not closed
     */
    protected void checksBeforeInsert() throws SQLException {
        // 1)Make sure JDBC ResultSet is not closed
        checkIfClosed("insertRow");

        // 2)Make sure this is an updatable ResultSet
        // if not updatable resultset, then throw exception
        checkUpdatableCursor("insertRow");

        // 3)Make sure JDBC ResultSet is positioned on insertRow
        if (!isOnInsertRow) {
            throw newSQLException(SQLState.CURSOR_NOT_POSITIONED_ON_INSERT_ROW);
        }

        // 4)Make sure underneath language resultset is not closed
        if (theResults.isClosed()) {
            throw Util.generateCsSQLException(SQLState.LANG_RESULT_SET_NOT_OPEN, "insertRow");
        }
    }

    /**
     * Check whether it is OK to update a column using
     * <code>updateAsciiStream()</code>.
     *
     * @param columnIndex the column index (first column is 1)
     * @exception SQLException if the column could not be updated with
     * <code>updateAsciiStream()</code>
     */
    private void checksBeforeUpdateAsciiStream(int columnIndex)
        throws SQLException
    {
        checksBeforeUpdateXXX("updateAsciiStream", columnIndex);
        int colType = getColumnType(columnIndex);
        if (!DataTypeDescriptor.isAsciiStreamAssignable(colType)) {
            throw dataTypeConversion(columnIndex, "java.io.InputStream");
        }
    }

    /**
     * Check whether it is OK to update a column using
     * <code>updateBinaryStream()</code>.
     *
     * @param columnIndex the column index (first column is 1)
     * @exception SQLException if the column could not be updated with
     * <code>updateBinaryStream()</code>
     */
    private void checksBeforeUpdateBinaryStream(int columnIndex)
        throws SQLException
    {
        checksBeforeUpdateXXX("updateBinaryStream", columnIndex);
        int colType = getColumnType(columnIndex);
        if (!DataTypeDescriptor.isBinaryStreamAssignable(colType)) {
            throw dataTypeConversion(columnIndex, "java.io.InputStream");
        }
    }

    /**
     * Check whether it is OK to update a column using
     * <code>updateCharacterStream()</code>.
     *
     * @param columnIndex the column index (first column is 1)
     * @exception SQLException if the column could not be updated with
     * <code>updateCharacterStream()</code>
     */
    private void checksBeforeUpdateCharacterStream(int columnIndex)
        throws SQLException
    {
        checksBeforeUpdateXXX("updateCharacterStream", columnIndex);
        int colType = getColumnType(columnIndex);
        if (!DataTypeDescriptor.isCharacterStreamAssignable(colType)) {
            throw dataTypeConversion(columnIndex, "java.io.Reader");
        }
    }
    
    /**
	 * JDBC 2.0
	 * 
	 * Give a nullable column a null value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateNull(int columnIndex) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateNull").setToNull();
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with a boolean value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateBoolean").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with a byte value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateByte(int columnIndex, byte x) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateByte").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with a short value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateShort(int columnIndex, short x) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateShort").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with an integer value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateInt(int columnIndex, int x) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateInt").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with a long value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateLong(int columnIndex, long x) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateLong").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with a float value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateFloat(int columnIndex, float x) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateFloat").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with a Double value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateDouble(int columnIndex, double x) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateDouble").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a String value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateString(int columnIndex, String x) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateString").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a byte array value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateBytes(int columnIndex, byte x[]) throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateBytes").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with a Date value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateDate(int columnIndex, java.sql.Date x)
			throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateDate").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with a Time value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateTime(int columnIndex, java.sql.Time x)
			throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateTime").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with a Timestamp value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateTimestamp(int columnIndex, java.sql.Timestamp x)
			throws SQLException {
		try {
			getDVDforColumnToBeUpdated(columnIndex, "updateTimestamp").setValue(x);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 *
	 * Update a column with an ascii stream value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @param length
	 *            the length of the stream
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateAsciiStream(int columnIndex, java.io.InputStream x,
			long length) throws SQLException {
		checksBeforeUpdateAsciiStream(columnIndex);

		java.io.Reader r = null;
		if (x != null)
		{
			try {
				r = new java.io.InputStreamReader(x, "ISO-8859-1");
			} catch (java.io.UnsupportedEncodingException uee) {
				throw new SQLException(uee.getMessage());
			}
		}
		updateCharacterStreamInternal(columnIndex, r, false, length,
				"updateAsciiStream");
	}

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or </code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @throws SQLException if the columnIndex is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        checksBeforeUpdateAsciiStream(columnIndex);

        java.io.Reader r = null;
        if (x != null) {
            try {
                r = new java.io.InputStreamReader(x, "ISO-8859-1");
            } catch (java.io.UnsupportedEncodingException uee) {
                throw new SQLException(uee.getMessage());
            }
        }
        updateCharacterStreamInternal(columnIndex, r, true, -1,
                                      "updateAsciiStream");
    }

	/**
	 *
	 * Update a column with a binary stream value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @param length
	 *            the length of the stream
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateBinaryStream(int columnIndex, java.io.InputStream x,
			long length) throws SQLException {
		checksBeforeUpdateBinaryStream(columnIndex);

		if (x == null)
		{
			updateNull(columnIndex);
			return;
		}

		updateBinaryStreamInternal(columnIndex, x, false, length,
                                   "updateBinaryStream");
	}

    /**
     * Updates the designated column with a binary stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @throws SQLException if the columnLabel is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        checksBeforeUpdateBinaryStream(columnIndex);
        updateBinaryStreamInternal(columnIndex, x, true, -1,
                                   "updateBinaryStream");
    }

    /**
     * Set the given binary stream for the specified parameter.
     *
     * If <code>lengthLess</code> is <code>true</code>, the following
     * conditions are either not checked or verified at the execution time
     * of <code>updateRow</code>/<code>insertRow</code>:
     * <ol><li>If the stream length is negative.
     *     <li>If the stream's actual length equals the specified length.</ol>
     * The <code>lengthLess</code> variable was added to differentiate between
     * streams with invalid lengths and streams without known lengths.
     *
     * @param columnIndex the 1-based index of the parameter to set.
     * @param x the data.
     * @param lengthLess tells whether we know the length of the data or not.
     * @param length the length of the data. Ignored if <code>lengthLess</code>
     *          is <code>true</code>.
     * @param updateMethodName the name of the method calling us. Used in
     *      error messages.
     * @throws SQLException if reading the data fails, or one of the data
     *      checks fails.
     */
    private void updateBinaryStreamInternal(int columnIndex, InputStream x,
                final boolean lengthLess, long length, String updateMethodName)
            throws SQLException {
        RawToBinaryFormatStream rawStream;
        if (!lengthLess) {
            if (length < 0)
                throw newSQLException(SQLState.NEGATIVE_STREAM_LENGTH);

            // max number of bytes that can be set to be inserted
            // in Derby is 2Gb-1 (ie Integer.MAX_VALUE).
            // (e.g into a blob column).
            if (length > Integer.MAX_VALUE ) {
                throw newSQLException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE,
                        getColumnSQLType(columnIndex));
            }
            rawStream = new RawToBinaryFormatStream(x, (int)length);
        } else {
            // Force length to -1 if stream is length less.
            length = -1;
            rawStream = new RawToBinaryFormatStream(x,
                    getMaxColumnWidth(columnIndex),
                    getColumnSQLType(columnIndex));
        }

        try {
			getDVDforColumnToBeUpdated(columnIndex, updateMethodName).setValue(
                    rawStream, (int) length);
		} catch (StandardException t) {
			throw noStateChangeException(t);
		}
	}

	/**
	 * JDBC 4.0
	 * 
	 * Update a column with a character stream value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @param length
	 *            the length of the stream
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateCharacterStream(int columnIndex, java.io.Reader x,
			long length) throws SQLException {
		checksBeforeUpdateCharacterStream(columnIndex);
		updateCharacterStreamInternal(columnIndex, x, false, length,
                                      "updateCharacterStream");
	}

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or </code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @throws SQLException if the columnIndex is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        checksBeforeUpdateCharacterStream(columnIndex);
        updateCharacterStreamInternal(columnIndex, x, true, -1,
                                      "updateCharacterStream");
    }

    /**
     * Set the given character stream for the specified parameter.
     *
     * If <code>lengthLess</code> is <code>true</code>, the following
     * conditions are either not checked or verified at the execution time
     * of the prepared statement:
     * <ol><li>If the stream length is negative.
     *     <li>If the stream's actual length equals the specified length.</ol>
     * The <code>lengthLess</code> variable was added to differentiate between
     * streams with invalid lengths and streams without known lengths.
     *
     * @param columnIndex the 1-based index of the parameter to set.
     * @param reader the data.
     * @param lengthLess tells whether we know the length of the data or not.
     * @param length the length of the data. Ignored if <code>lengthLess</code>
     *          is <code>true</code>.
     * @throws SQLException if reading the data fails, or one of the data
     *      checks fails.
     */
    private void updateCharacterStreamInternal(int columnIndex, Reader reader,
                                               final boolean lengthLess,
                                               long length,
                                               String updateMethodName)
            throws SQLException
	{
		try {

            if (reader == null)
            {
                updateNull(columnIndex);
                return;
            }
            
            ReaderToUTF8Stream utfIn;
            int usableLength = -1;
            if (!lengthLess) {
                // check for -ve length here
                if (length < 0)
                    throw newSQLException(SQLState.NEGATIVE_STREAM_LENGTH);

                // max number of characters that can be set to be inserted
                // in Derby is 2Gb-1 (ie Integer.MAX_VALUE).
                // (e.g into a CLOB column).
                if (length > Integer.MAX_VALUE ) {
                    throw newSQLException(
                            SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE,
                            getColumnSQLType(columnIndex));
                }

                // length is +ve. at this point, all checks for negative
                // length has already been done
                usableLength = (int) length;
                int truncationLength = 0;

                // Currently long varchar does not allow for truncation of
                // trailing blanks.  For char and varchar types, current
                // mechanism of materializing when using streams seems fine
                // given their max limits. This change is fix for DERBY-352:
                // Insert of clobs using streams should not materialize the
                // entire stream into memory
                // In case of clobs, the truncation of trailing blanks is
                // factored in when reading from the stream without
                // materializing the entire stream, and so the special casing
                // for clob below.
                if (getColumnType(columnIndex) == Types.CLOB) {
                    // Need column width to figure out if truncation is
                    // needed
                    int colWidth = getMaxColumnWidth(columnIndex);

                    // It is possible that the length of the stream passed in
                    // is greater than the column width, in which case the data
                    // from the stream needs to be truncated.
                    // usableLength is the length of the data from stream
                    // that can be used which is min(colWidth,length) provided
                    // length - colWidth has trailing blanks only
                    if (usableLength > colWidth) {
                        truncationLength = usableLength - colWidth;
                        usableLength = colWidth;
                    }
                }

                utfIn = new ReaderToUTF8Stream(reader, usableLength,
                        truncationLength, getColumnSQLType(columnIndex));
            } else {
                int colWidth = getMaxColumnWidth(columnIndex);
                utfIn = new ReaderToUTF8Stream(
                            reader, colWidth, getColumnSQLType(columnIndex));
            }

            // NOTE: The length argument to setValue is not used. If that
            //       changes, the value might also have to change.
            getDVDforColumnToBeUpdated(columnIndex, updateMethodName).setValue(
                    utfIn, (int) usableLength);
        } catch (StandardException t) {
            throw noStateChangeException(t);
        }
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with an Object value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @param scale
	 *            For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
	 *            this is the number of digits after the decimal. For all other
	 *            types this value will be ignored.
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateObject(int columnIndex, Object x, int scale)
			throws SQLException {
		updateObject(columnIndex, x);
		/*
		* If the parameter type is DECIMAL or NUMERIC, then
		* we need to set them to the passed scale.
		*/
		int colType = getColumnType(columnIndex);
		if ((colType == Types.DECIMAL) || (colType == Types.NUMERIC)) {
			if (scale < 0)
				throw newSQLException(SQLState.BAD_SCALE_VALUE, new Integer(scale));

			try {
				DataValueDescriptor value = updateRow.getColumn(columnIndex);

				int origvaluelen = value.getLength();
				((VariableSizeDataValue)
						value).setWidth(VariableSizeDataValue.IGNORE_PRECISION,
							scale,
							false);

			} catch (StandardException t) {
				throw EmbedResultSet.noStateChangeException(t);
			}
		}
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with an Object value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnIndex
	 *            the first column is 1, the second is 2, ...
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateObject(int columnIndex, Object x) throws SQLException {
		checksBeforeUpdateXXX("updateObject", columnIndex);
		int colType = getColumnType(columnIndex);
		if (colType == org.apache.derby.iapi.reference.JDBC20Translation.SQL_TYPES_JAVA_OBJECT) {
			try {
				((UserDataValue) getDVDforColumnToBeUpdated(columnIndex, "updateObject")).setValue(x);
				return;
			} catch (StandardException t) {
				throw noStateChangeException(t);
			}
		}

		if (x == null) {
			updateNull(columnIndex);
			return;
		}

		if (x instanceof String) {
			updateString(columnIndex, (String) x);
			return;
		}

		if (x instanceof Boolean) {
			updateBoolean(columnIndex, ((Boolean) x).booleanValue());
			return;
		}

		if (x instanceof Short) {
			updateShort(columnIndex, ((Short) x).shortValue());
			return;
		}

		if (x instanceof Integer) {
			updateInt(columnIndex, ((Integer) x).intValue());
			return;
		}

		if (x instanceof Long) {
			updateLong(columnIndex, ((Long) x).longValue());
			return;
		}

		if (x instanceof Float) {
			updateFloat(columnIndex, ((Float) x).floatValue());
			return;
		}

		if (x instanceof Double) {
			updateDouble(columnIndex, ((Double) x).doubleValue());
			return;
		}

		if (x instanceof byte[]) {
			updateBytes(columnIndex, (byte[]) x);
			return;
		}

		if (x instanceof Date) {
			updateDate(columnIndex, (Date) x);
			return;
		}

		if (x instanceof Time) {
			updateTime(columnIndex, (Time) x);
			return;
		}

		if (x instanceof Timestamp) {
			updateTimestamp(columnIndex, (Timestamp) x);
			return;
		}

		if (x instanceof Blob) {
			updateBlob(columnIndex, (Blob) x);
			return;
		}

		if (x instanceof Clob) {
			updateClob(columnIndex, (Clob) x);
			return;
		}

		throw dataTypeConversion(columnIndex, x.getClass().getName());
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a null value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateNull(String columnName) throws SQLException {
		checkIfClosed("updateNull");
		updateNull(findColumnName(columnName));
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a boolean value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateBoolean(String columnName, boolean x) throws SQLException {
		checkIfClosed("updateBoolean");
		updateBoolean(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a byte value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateByte(String columnName, byte x) throws SQLException {
		checkIfClosed("updateByte");
		updateByte(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a short value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateShort(String columnName, short x) throws SQLException {
		checkIfClosed("updateShort");
		updateShort(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with an integer value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateInt(String columnName, int x) throws SQLException {
		checkIfClosed("updateInt");
		updateInt(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a long value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateLong(String columnName, long x) throws SQLException {
		checkIfClosed("updateLong");
		updateLong(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a float value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateFloat(String columnName, float x) throws SQLException {
		checkIfClosed("updateFloat");
		updateFloat(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a double value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateDouble(String columnName, double x) throws SQLException {
		checkIfClosed("updateDouble");
		updateDouble(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a String value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateString(String columnName, String x) throws SQLException {
		checkIfClosed("updateString");
		updateString(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a byte array value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateBytes(String columnName, byte x[]) throws SQLException {
		checkIfClosed("updateBytes");
		updateBytes(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a Date value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateDate(String columnName, java.sql.Date x)
			throws SQLException {
		checkIfClosed("updateDate");
		updateDate(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a Time value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateTime(String columnName, java.sql.Time x)
			throws SQLException {
		checkIfClosed("updateTime");
		updateTime(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a Timestamp value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateTimestamp(String columnName, java.sql.Timestamp x)
			throws SQLException {
		checkIfClosed("updateTimestamp");
		updateTimestamp(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with an ascii stream value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @param length
	 *            of the stream
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateAsciiStream(String columnName, java.io.InputStream x,
			int length) throws SQLException {
		checkIfClosed("updateAsciiStream");
		updateAsciiStream(findColumnName(columnName), x, length);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a binary stream value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @param length
	 *            of the stream
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateBinaryStream(String columnName, java.io.InputStream x,
			int length) throws SQLException {
		checkIfClosed("updateBinaryStream");
		updateBinaryStream(findColumnName(columnName), x, length);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Update a column with a character stream value.
	 * 
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @param reader
	 *            the new column value
	 * @param length
	 *            length of the stream
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateCharacterStream(String columnName, java.io.Reader reader,
			int length) throws SQLException {
		checkIfClosed("updateCharacterStream");
		updateCharacterStream(findColumnName(columnName), reader, length);
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with an Object value.
	 *
	 * The updateXXX() methods are used to update column values in the
	 * current row, or the insert row.  The updateXXX() methods do not
	 * update the underlying database, instead the updateRow() or insertRow()
	 * methods are called to update the database.
	 *
	 * @param columnName the name of the column
	 * @param x the new column value
	 * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
	 *  this is the number of digits after the decimal.  For all other
	 *  types this value will be ignored.
	 * @exception SQLException if a database-access error occurs
	 */
	public void updateObject(String columnName, Object x, int scale)
      throws SQLException {
		checkIfClosed("updateObject");
		updateObject(findColumnName(columnName), x, scale);
	}

	/**
	 * JDBC 2.0
	 *
	 * Update a column with an Object value.
	 *
	 * The updateXXX() methods are used to update column values in the current
	 * row, or the insert row. The updateXXX() methods do not update the
	 * underlying database, instead the updateRow() or insertRow() methods are
	 * called to update the database.
	 *
	 * @param columnName
	 *            the name of the column
	 * @param x
	 *            the new column value
	 * @exception SQLException
	 *                if a database-access error occurs
	 */
	public void updateObject(String columnName, Object x) throws SQLException {
		checkIfClosed("updateObject");
		updateObject(findColumnName(columnName), x);
	}

	/**
	 * JDBC 2.0
	 * 
	 * Insert the contents of the insert row into the result set and the
	 * database. Must be on the insert row when this method is called.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs, if called when not on
	 *                the insert row, or if all non-nullable columns in the
	 *                insert row have not been given a value
	 */
	public void insertRow() throws SQLException {
        synchronized (getConnectionSynchronization()) {
            checksBeforeInsert();
            setupContextStack();
            LanguageConnectionContext lcc = getEmbedConnection().getLanguageConnection();
            StatementContext statementContext = null;
            try {
                /*
                 * construct the insert statement
                 *
                 * If no values have been supplied for a column, it will be set 
                 * to the column's default value, if any. 
                 * If no default value had been defined, the default value of a 
                 * nullable column is set to NULL.
                 */

                boolean foundOneColumnAlready = false;
                StringBuffer insertSQL = new StringBuffer("INSERT INTO ");
                StringBuffer valuesSQL = new StringBuffer("VALUES (");
                CursorActivation activation = lcc.lookupCursorActivation(getCursorName());

                ExecCursorTableReference targetTable = 
                        activation.getPreparedStatement().getTargetTable();
                // got the underlying (schema.)table name
                insertSQL.append(getFullBaseTableName(targetTable));

                insertSQL.append(" (");
                // in this for loop we are constructing list of column-names 
                // and values (?) ,... part of the insert sql
                for (int i=1; i<=resultDescription.getColumnCount(); i++) { 
                    if (foundOneColumnAlready) {
                        insertSQL.append(",");
                        valuesSQL.append(",");
                    }
                    // using quotes around the column name 
                    // to preserve case sensitivity
                    insertSQL.append(quoteSqlIdentifier(
                            resultDescription.getColumnDescriptor(i).getName()));
                    if (columnGotUpdated[i-1]) { 
                        valuesSQL.append("?");
                    } else {
                        valuesSQL.append("DEFAULT");
                    }
                    foundOneColumnAlready = true;
                }
                insertSQL.append(") ");
                valuesSQL.append(") ");
                insertSQL.append(valuesSQL);

                // Context used for preparing, don't set any timeout (use 0)
                statementContext = lcc.pushStatementContext(
                        isAtomic, 
                        false, 
                        insertSQL.toString(), 
                        null, 
                        false, 
                        0L);
                org.apache.derby.iapi.sql.PreparedStatement ps = 
                        lcc.prepareInternalStatement(insertSQL.toString());
                Activation act = ps.getActivation(lcc, false);

                // in this for loop we are assigning values for parameters 
                //in sql constructed earlier VALUES (?, ..)
                for (int i=1, paramPosition=0; i<=resultDescription.getColumnCount(); i++) { 
                    // if the column got updated, do following
                    if (columnGotUpdated[i-1]) {  
                        act.getParameterValueSet().
                                getParameterForSet(paramPosition++).
                                setValue(updateRow.getColumn(i));
                    }
                }
                // Don't see any timeout when inserting rows (use 0)
                //execute the insert
                org.apache.derby.iapi.sql.ResultSet rs = 
                        ps.execute(act, true, 0L); 
                rs.close();
                rs.finish();
                act.close();

                lcc.popStatementContext(statementContext, null);
            } catch (StandardException t) {
                throw closeOnTransactionError(t);
            } finally {
                if (statementContext != null)
                    lcc.popStatementContext(statementContext, null);
                restoreContextStack();
            }
        }
	}

    /**
     * JDBC 2.0
     *
     * Update the underlying database with the new contents of the
     * current row.  Cannot be called when on the insert row.
     *
     * @exception SQLException if a database-access error occurs, or
     * if called when on the insert row
     */
    public void updateRow() throws SQLException {
			synchronized (getConnectionSynchronization()) {
        checksBeforeUpdateOrDelete("updateRow", -1);
        
        // Check that the cursor is not positioned on insertRow
        checkNotOnInsertRow();
        
        setupContextStack();
        LanguageConnectionContext lcc = getEmbedConnection().getLanguageConnection();
        StatementContext statementContext = null;
        try {
            if (currentRowHasBeenUpdated == false) //nothing got updated on this row 
                return; //nothing to do since no updates were made to this row

            //now construct the update where current of sql
            boolean foundOneColumnAlready = false;
            StringBuffer updateWhereCurrentOfSQL = new StringBuffer("UPDATE ");
            CursorActivation activation = lcc.lookupCursorActivation(getCursorName());


            ExecCursorTableReference targetTable = activation.getPreparedStatement().getTargetTable();
            updateWhereCurrentOfSQL.append(getFullBaseTableName(targetTable));//got the underlying (schema.)table name
            updateWhereCurrentOfSQL.append(" SET ");
      
            for (int i=1; i<=resultDescription.getColumnCount(); i++) { //in this for loop we are constructing columnname=?,... part of the update sql
                if (columnGotUpdated[i-1]) { //if the column got updated, do following
                    if (foundOneColumnAlready)
                        updateWhereCurrentOfSQL.append(",");
                    //using quotes around the column name to preserve case sensitivity
                    updateWhereCurrentOfSQL.append(quoteSqlIdentifier(
                            resultDescription.getColumnDescriptor(i).getName()) + "=?");
                    foundOneColumnAlready = true;
                }
            }
            //using quotes around the cursor name to preserve case sensitivity
            updateWhereCurrentOfSQL.append(" WHERE CURRENT OF " + 
                    quoteSqlIdentifier(getCursorName()));

            // Context used for preparing, don't set any timeout (use 0)
            statementContext = lcc.pushStatementContext(isAtomic, false, updateWhereCurrentOfSQL.toString(), null, false, 0L);
            org.apache.derby.iapi.sql.PreparedStatement ps = lcc.prepareInternalStatement(updateWhereCurrentOfSQL.toString());
            Activation act = ps.getActivation(lcc, false);

            //in this for loop we are assigning values for parameters in sql constructed earlier with columnname=?,... 
            for (int i=1, paramPosition=0; i<=resultDescription.getColumnCount(); i++) { 
                if (columnGotUpdated[i-1])  //if the column got updated, do following
                    act.getParameterValueSet().getParameterForSet(paramPosition++).setValue(updateRow.getColumn(i));
            }
            // Don't set any timeout when updating rows (use 0)
            // Execute the update where current of sql.
            org.apache.derby.iapi.sql.ResultSet rs = ps.execute(act, true, 0L);
            SQLWarning w = act.getWarnings();
            if (w != null) {
                addWarning(w);
            }
            rs.close();
            rs.finish();
            act.close();
            //For forward only resultsets, after a update, the ResultSet will be positioned right before the next row.
            if (getType() == TYPE_FORWARD_ONLY) {
                currentRow = null;
            } else {
                movePosition(RELATIVE, 0, "relative");
            }
            lcc.popStatementContext(statementContext, null);
        } catch (StandardException t) {
            throw closeOnTransactionError(t);
        } finally {
            if (statementContext != null)
                lcc.popStatementContext(statementContext, null);
            restoreContextStack();
            initializeUpdateRowModifiers();
        }
			}
    }

    /**
     * JDBC 2.0
     *
     * Delete the current row from the result set and the underlying
     * database.  Cannot be called when on the insert row.
     *
     * @exception SQLException if a database-access error occurs, or if
     * called when on the insert row.
     */
    public void deleteRow() throws SQLException {
        synchronized (getConnectionSynchronization()) {
            checksBeforeUpdateOrDelete("deleteRow", -1);
        
            // Check that the cursor is not positioned on insertRow
            checkNotOnInsertRow();

            setupContextStack();
            
            LanguageConnectionContext lcc = getEmbedConnection().getLanguageConnection();
            StatementContext statementContext = null;
            
            //now construct the delete where current of sql
            try {
                StringBuffer deleteWhereCurrentOfSQL = new StringBuffer("DELETE FROM ");
                CursorActivation activation = lcc.lookupCursorActivation(getCursorName());
                deleteWhereCurrentOfSQL.append(getFullBaseTableName(activation.getPreparedStatement().getTargetTable()));//get the underlying (schema.)table name
                //using quotes around the cursor name to preserve case sensitivity
                deleteWhereCurrentOfSQL.append(" WHERE CURRENT OF " + 
                        quoteSqlIdentifier(getCursorName()));
                
                // Context used for preparing, don't set any timeout (use 0)
                statementContext = lcc.pushStatementContext(isAtomic, false, deleteWhereCurrentOfSQL.toString(), null, false, 0L);
                org.apache.derby.iapi.sql.PreparedStatement ps = lcc.prepareInternalStatement(deleteWhereCurrentOfSQL.toString());
                // Get activation, so that we can get the warning from it
                Activation act = ps.getActivation(lcc, false);
                // Don't set any timeout when deleting rows (use 0)
                //execute delete where current of sql
                org.apache.derby.iapi.sql.ResultSet rs = 
                        ps.execute(act, true, 0L);
                SQLWarning w = act.getWarnings();
                if (w != null) {
                    addWarning(w);
                }
                rs.close();
                rs.finish();
                act.close();
                //After a delete, the ResultSet will be positioned right before 
                //the next row.
                currentRow = null;
                lcc.popStatementContext(statementContext, null);
            } catch (StandardException t) {
                    throw closeOnTransactionError(t);
            } finally {
                if (statementContext != null)
                    lcc.popStatementContext(statementContext, null);
                restoreContextStack();
                initializeUpdateRowModifiers();
            }
        }
    }

	private String getFullBaseTableName(ExecCursorTableReference targetTable) {
		//using quotes to preserve case sensitivity
		if (targetTable.getSchemaName() != null)
			return quoteSqlIdentifier(targetTable.getSchemaName()) + "." + 
					quoteSqlIdentifier(targetTable.getBaseName());
		else
			return quoteSqlIdentifier(targetTable.getBaseName());
	}

    private String quoteSqlIdentifier(String orgValue) {
        int i = 0, start = 0;
        String retValue = "";
        while ((i = orgValue.indexOf("\"", start) + 1) > 0) {
            retValue += orgValue.substring(start, i) + "\"";
            start = i;
        }
        retValue += orgValue.substring(start, orgValue.length());
        return "\"" + retValue + "\"";
    }
    
	/**
	 * JDBC 2.0
	 * 
	 * Refresh the value of the current row with its current value in the
	 * database. Cannot be called when on the insert row.
	 * 
	 * The refreshRow() method provides a way for an application to explicitly
	 * tell the JDBC driver to refetch a row(s) from the database. An
	 * application may want to call refreshRow() when caching or prefetching is
	 * being done by the JDBC driver to fetch the latest value of a row from the
	 * database. The JDBC driver may actually refresh multiple rows at once if
	 * the fetch size is greater than one.
	 * 
	 * All values are refetched subject to the transaction isolation level and
	 * cursor sensitivity. If refreshRow() is called after calling updateXXX(),
	 * but before calling updateRow() then the updates made to the row are lost.
	 * Calling refreshRow() frequently will likely slow performance.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs, or if called when on
	 *                the insert row.
	 */
	public void refreshRow() throws SQLException {
		throw Util.notImplemented();
	}

    /**
     * JDBC 2.0
     *
     * The cancelRowUpdates() method may be called after calling an
     * updateXXX() method(s) and before calling updateRow() to rollback 
     * the updates made to a row.  If no updates have been made or 
     * updateRow() has already been called, then this method has no 
     * effect.
     *
     * @exception SQLException if a database-access error occurs, or if
     * called when on the insert row.
     *
     */
    public void cancelRowUpdates () throws SQLException {
        checksBeforeUpdateOrDelete("cancelRowUpdates", -1);
        
        checkNotOnInsertRow();

        initializeUpdateRowModifiers();        
    }

	/**
	 * JDBC 2.0
	 * 
	 * Move to the insert row. The current cursor position is remembered while
	 * the cursor is positioned on the insert row.
	 * 
	 * The insert row is a special row associated with an updatable result set.
	 * It is essentially a buffer where a new row may be constructed by calling
	 * the updateXXX() methods prior to inserting the row into the result set.
	 * 
	 * Only the updateXXX(), getXXX(), and insertRow() methods may be called
	 * when the cursor is on the insert row. All of the columns in a result set
	 * must be given a value each time this method is called before calling
	 * insertRow(). UpdateXXX()must be called before getXXX() on a column.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs, or the result set is
	 *                not updatable
	 */
	public void moveToInsertRow() throws SQLException {
		checkExecIfClosed("moveToInsertRow");

		// if not updatable resultset, then throw exception
		checkUpdatableCursor("moveToInsertRow");

		synchronized (getConnectionSynchronization()) {
			try {
				//we need to set the context because the getNull call below 
				//(if dealing with territory based database) might need to 
				//look up the current context to get the correct 
				//RuleBasedCollator. This RuleBasedCollator will be used to
				//construct a CollatorSQL... type rather than SQL...Char type 
				//when dealing with character string datatypes.
				setupContextStack();
				// initialize state corresponding to insertRow/updateRow impl.
				initializeUpdateRowModifiers();
 				isOnInsertRow = true;
				
				for (int i=1; i <= columnGotUpdated.length; i++) {
					updateRow.setColumn(i, 
						resultDescription.getColumnDescriptor(i).getType().getNull());
				}
			} catch (Throwable ex) {
				handleException(ex);
			} finally {
				restoreContextStack(); 
			}
		}
	}

	/**
	 * JDBC 2.0
	 * 
	 * Move the cursor to the remembered cursor position, usually the current
	 * row. Has no effect unless the cursor is on the insert row.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs, or the result set is
	 *                not updatable
	 */
	public void moveToCurrentRow() throws SQLException {
		checkExecIfClosed("moveToCurrentRow");

		// if not updatable resultset, then throw exception
		checkUpdatableCursor("moveToCurrentRow");

		synchronized (getConnectionSynchronization()) {
			try {

				if (isOnInsertRow) {
					// initialize state corresponding to insertRow/updateRow impl.
					initializeUpdateRowModifiers();

					isOnInsertRow = false;
				}
			} catch (Throwable ex) {
				handleException(ex);
			}
		}
	}

    /**
	 * JDBC 2.0
	 * 
	 * Get a BLOB column.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return an object representing a BLOB
	 */
	public Blob getBlob(int columnIndex) throws SQLException {

		closeCurrentStream(); // closing currentStream does not depend on the
		// underlying connection. Do this outside of
		// the connection synchronization.

		checkIfClosed("getBlob"); // checking result set closure does not depend
		// on the underlying connection. Do this
		// outside of the connection synchronization.

		synchronized (getConnectionSynchronization()) {
			int colType = getColumnType(columnIndex);

			// DB2, only allow getBlob on a BLOB column.
			if (colType != Types.BLOB)
				throw dataTypeConversion("java.sql.Blob", columnIndex);

			boolean pushStack = false;
			try {
				DataValueDescriptor dvd = getColumn(columnIndex);

				if (wasNull = dvd.isNull())
					return null;

				// should set up a context stack if we have a long column,
				// since a blob may keep a pointer to a long column in the
				// database
				if (dvd.getStream() != null)
					pushStack = true;

				if (pushStack)
					setupContextStack();

				return new EmbedBlob(dvd, getEmbedConnection());
			} catch (Throwable t) {
				throw handleException(t);
			} finally {
				if (pushStack)
					restoreContextStack();
			}
		}
	}

	/**
	 * JDBC 2.0
	 * 
	 * Get a CLOB column.
	 * 
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return an object representing a CLOB
	 */
	public final Clob getClob(int columnIndex) throws SQLException {

		closeCurrentStream(); // closing currentStream does not depend on the
		// underlying connection. Do this outside of
		// the connection synchronization.

		checkIfClosed("getClob"); // checking result set closure does not depend
		// on the underlying connection. Do this
		// outside of the connection synchronization.

		synchronized (getConnectionSynchronization()) {
			int colType = getColumnType(columnIndex);

			// DB2:, only allow getClob on a CLOB column.
			if (colType != Types.CLOB)
				throw dataTypeConversion("java.sql.Clob", columnIndex);

			boolean pushStack = false;
			try {

				DataValueDescriptor dvd = getColumn(columnIndex);

				if (wasNull = dvd.isNull())
					return null;

				// should set up a context stack if we have a long column,
				// since a blob may keep a pointer to a long column in the
				// database
				if (dvd.getStream() != null)
					pushStack = true;

				if (pushStack)
					setupContextStack();

				return new EmbedClob(getEmbedConnection(), dvd);
			} catch (Throwable t) {
				throw handleException(t);
			} finally {
				if (pushStack)
					restoreContextStack();
			}
		}
	}
	
    /**
	 * JDBC 2.0
	 * 
	 * Get a BLOB column.
	 * 
	 * @param columnName the column name
	 * @return an object representing a BLOB
	 */
	public final Blob getBlob(String columnName) throws SQLException {
		checkIfClosed("getBlob");
		return (getBlob(findColumnName(columnName)));
	}

	/**
	 * JDBC 2.0
	 * 
	 * Get a CLOB column.
	 * 
	 * @param columnName the column name
	 * @return an object representing a CLOB
	 * @exception SQLException
	 *                Feature not implemented for now.
	 */
	public final Clob getClob(String columnName) throws SQLException {
		checkIfClosed("getClob");
		return (getClob(findColumnName(columnName)));
	}	

	
    /**
	 * JDBC 3.0
	 * 
	 * Updates the designated column with a java.sql.Blob value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the updateRow or insertRow methods are called to update the database.
	 * 
	 * @param columnIndex -
	 *            the first column is 1, the second is 2
	 * @param x -
	 *            the new column value
	 * @exception SQLException
	 *                Feature not implemented for now.
	 */
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
        checksBeforeUpdateXXX("updateBlob", columnIndex);
        int colType = getColumnType(columnIndex);
        if (colType != Types.BLOB)
            throw dataTypeConversion(columnIndex, "java.sql.Blob");

        if (x == null)
            updateNull(columnIndex);
        else {
            long length = x.length();
            updateBinaryStreamInternal(columnIndex, x.getBinaryStream(), false,
                                       length, "updateBlob");
        }
	}

	/**
	 * JDBC 3.0
	 * 
	 * Updates the designated column with a java.sql.Blob value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the updateRow or insertRow methods are called to update the database.
	 * 
	 * @param columnName -
	 *            the SQL name of the column
	 * @param x -
	 *            the new column value
	 * @exception SQLException
	 *                Feature not implemented for now.
	 */
	public void updateBlob(String columnName, Blob x) throws SQLException {
		checkIfClosed("updateBlob");
		updateBlob(findColumnName(columnName), x);
	}

	/**
	 * JDBC 3.0
	 * 
	 * Updates the designated column with a java.sql.Clob value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the updateRow or insertRow methods are called to update the database.
	 * 
	 * @param columnIndex -
	 *            the first column is 1, the second is 2
	 * @param x -
	 *            the new column value
	 * @exception SQLException
	 *                Feature not implemented for now.
	 */
	public void updateClob(int columnIndex, Clob x) throws SQLException {
        checksBeforeUpdateXXX("updateClob", columnIndex);
        int colType = getColumnType(columnIndex);
        if (colType != Types.CLOB)
            throw dataTypeConversion(columnIndex, "java.sql.Clob");

        if (x == null)
        {
            updateNull(columnIndex);
        }
        else
        {
            
            long length = x.length();

            updateCharacterStreamInternal(
                columnIndex, x.getCharacterStream(), false, length,
                "updateClob");
        }
	}

	/**
	 * JDBC 3.0
	 * 
	 * Updates the designated column with a java.sql.Clob value. The updater
	 * methods are used to update column values in the current row or the insert
	 * row. The updater methods do not update the underlying database; instead
	 * the updateRow or insertRow methods are called to update the database.
	 * 
	 * @param columnName -
	 *            the SQL name of the column
	 * @param x -
	 *            the new column value
	 * @exception SQLException
	 *                Feature not implemented for now.
	 */
	public void updateClob(String columnName, Clob x) throws SQLException {
		checkIfClosed("updateClob");
		updateClob(findColumnName(columnName), x);
	}
	
	
	/*
	 * * End of JDBC public methods.
	 */

    /**
	 * Map a Resultset column name to a ResultSet column index.
	 * 
	 * @param columnName
	 *            the name of the column
	 * @return the column index
	 * @exception SQLException
	 *                thrown on failure.
	 */
    protected int findColumnName(String columnName)
						throws SQLException {
		// n.b. if we went through the JDBC interface,
		// there is a caching implementation in the JDBC doc
		// (appendix C). But we go through our own info, for now.

		if (columnName == null)
			throw newSQLException(SQLState.NULL_COLUMN_NAME);
        
        int position = resultDescription.findColumnInsenstive(columnName);
		
		if (position == -1) {
			throw newSQLException(SQLState.COLUMN_NOT_FOUND, columnName);
		} else {
			return position;
		}
	}
	/**
	 * Documented behaviour for streams is that they are implicitly closed on
	 * the next get*() method call.
	 */
	private final void closeCurrentStream() {

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
	final void checkIfClosed(String operation) throws SQLException {
		if (isClosed) {
			throw newSQLException(SQLState.LANG_RESULT_SET_NOT_OPEN, operation);
		}
	}

    /**
     * Throw an exception if this ResultSet is closed or its
     * Connection has been closed. If the ResultSet has not
     * been explictly closed but the Connection is closed,
     * then this ResultSet will be marked as closed.
     */
	final void checkExecIfClosed(String operation) throws SQLException {
		
		checkIfClosed(operation);

		java.sql.Connection appConn = getEmbedConnection().getApplicationConnection();

        // Currently disconnected, i.e. a detached gobal transaction
        if (appConn == null)
            throw Util.noCurrentConnection();
            
		if (appConn.isClosed()) {
            closeCurrentStream();
            isClosed = true;
			throw Util.noCurrentConnection();
        }
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
	final SQLException closeOnTransactionError(Throwable thrownException) throws SQLException
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
					sqle.setNextException(handleException(t));
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

	  if (columnIndex < 1 || columnIndex > resultDescription.getColumnCount()) {
		  throw newSQLException(SQLState.COLUMN_NOT_FOUND, 
								new Integer(columnIndex));
	  }
	  if (isOnInsertRow || currentRowHasBeenUpdated && columnGotUpdated[columnIndex -1]) {
		  return updateRow.getColumn(columnIndex);
	  } else {
		  checkOnRow(); // make sure there's a row
		  return currentRow.getColumn(columnIndex);
	  }
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
	static final SQLException noStateChangeException(Throwable thrownException) {

		// Any exception on a setXXX/getXXX method does not close
		// the ResultSet or the Statement. So we only need
		// to convert the exception to a SQLException.

		return TransactionResourceImpl.wrapInSQLException(thrownException);

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
     * Checks if the result set has a scrollable cursor.
     *
     * @param methodName name of the method which requests the check
     * @exception SQLException if the result set is closed or its type
     * is <code>TYPE_FORWARD_ONLY</code>
     */
    private void checkScrollCursor(String methodName) throws SQLException {
		checkIfClosed(methodName);
		if (stmt.getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY)
			throw Util
					.newEmbedSQLException(
							SQLState.NOT_ON_FORWARD_ONLY_CURSOR,
							new Object[] { methodName },
							StandardException
									.getSeverityFromIdentifier(SQLState.NOT_ON_FORWARD_ONLY_CURSOR));
	}
    
    private void checkUpdatableCursor(String operation) throws SQLException {
        if (getConcurrency() != JDBC20Translation.CONCUR_UPDATABLE) {
            throw Util.generateCsSQLException(
                    SQLState.UPDATABLE_RESULTSET_API_DISALLOWED, 
                    operation);
        }
    }

    
	private boolean checkRowPosition(int position, String positionText)
			throws SQLException {
		// beforeFirst is only allowed on scroll cursors
		checkScrollCursor(positionText);

		synchronized (getConnectionSynchronization()) {
			setupContextStack();
			try {
				try {

					/*
					 * Push and pop a StatementContext around a next call so
					 * that the ResultSet will get correctly closed down on an
					 * error. (Cache the LanguageConnectionContext)
					 */
					LanguageConnectionContext lcc = getEmbedConnection()
							.getLanguageConnection();
                    // No timeout for this operation (use 0)
					StatementContext statementContext =
                        lcc.pushStatementContext(isAtomic, 
						 concurrencyOfThisResultSet==JDBC20Translation.CONCUR_READ_ONLY,
						 getSQLText(),
                                                 getParameterValueSet(),
                                                 false, 0L);

					boolean result = theResults.checkRowPosition(position);

					lcc.popStatementContext(statementContext, null);

					return result;

				} catch (Throwable t) {
					/*
					 * Need to close the result set here because the error might
					 * cause us to lose the current connection if this is an XA
					 * connection and we won't be able to do the close later
					 */
					throw closeOnTransactionError(t);
				}

			} finally {
				restoreContextStack();
			}
		}
	}
	/**
	 * * Is this result set from a select for update statement?
	 */
	public final boolean isForUpdate()
	{
		if (theResults instanceof NoPutResultSet)
			return ((NoPutResultSet) theResults).isForUpdate();
		return false;
	}
    
    final String getColumnSQLType(int column)
    {
        return resultDescription.getColumnDescriptor(column)
                       .getType().getTypeId().getSQLTypeName();
    }

    /**
     * Return the user-defined maximum size of the column.
     *
     * Note that this may be different from the maximum column size Derby is
     * able, or allowed, to handle (called 'maximum maximum length').
     *
     * @param columnIndex the 1-based index of the column
     * @return the maximum length of the column
     */
    private final int getMaxColumnWidth(int columnIndex) {
        return resultDescription.getColumnDescriptor(columnIndex).
                    getType().getMaximumWidth();
    }

	private final SQLException dataTypeConversion(String targetType, int column) {
		return newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH, targetType,
                getColumnSQLType(column));
	}

	private final SQLException dataTypeConversion(int column, String targetType) {
		return newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
                getColumnSQLType(column), targetType);
	}
    
    /**
     * Mark a column as already having a stream accessed from it.
     * If the stream was already accessed, then throw an exception.
     * @param columnIndex
     * @throws SQLException
     */
    final void useStream(int columnIndex) throws SQLException {
    	
    	if (streamUsedFlags == null)
    		streamUsedFlags = new boolean[getMetaData().getColumnCount()];
    	
    	else if (streamUsedFlags[columnIndex - 1])
	        throw newSQLException(SQLState.LANG_STREAM_RETRIEVED_ALREADY);
    	
    	streamUsedFlags[columnIndex - 1] = true;
    }

    /**
     * JDBC 4.0
     *
     * <p>
     * Checks whether this <code>ResultSet</code> object has been
     * closed, either automatically or because <code>close()</code>
     * has been called.
     *
     * @return <code>true</code> if the <code>ResultSet</code> is
     * closed, <code>false</code> otherwise
     * @exception SQLException if a database error occurs
     */
    public final boolean isClosed() throws SQLException {
        if (isClosed) return true;
        try {
            // isClosed is not updated when EmbedConnection.close() is
            // called, so we need to check the status of the
            // connection
            checkExecIfClosed("");
            return false;
        } catch (SQLException sqle) {
            return isClosed;
        }
    }
     
     /**
      * Adds a warning to the end of the warning chain.
      *
      * @param w The warning to add to the warning chain.
      */
     private void addWarning(SQLWarning w) {
         if (topWarning == null) {
             topWarning = w;
         } else {
             topWarning.setNextWarning(w);
         }
     }

     /**
      *
      * JDBC 2.0
      *
      * Update a column with an ascii stream value.
      *
      * The updateXXX() methods are used to update column values in the current
      * row, or the insert row. The updateXXX() methods do not update the
      * underlying database, instead the updateRow() or insertRow() methods are
      * called to update the database.
      *
      * @param columnIndex
      *            the first column is 1, the second is 2, ...
      * @param x
      *            the new column value
      * @param length
      *            the length of the stream
      * @exception SQLException
      *                if a database-access error occurs
      */
     public void updateAsciiStream(int columnIndex, java.io.InputStream x,
         int length) throws SQLException {
         checkIfClosed("updateAsciiStream");
         updateAsciiStream(columnIndex,x,(long)length);
     }

     /**
      *
      * JDBC 2.0
      *
      * Update a column with a binary stream value.
      *
      * The updateXXX() methods are used to update column values in the current
      * row, or the insert row. The updateXXX() methods do not update the
      * underlying database, instead the updateRow() or insertRow() methods are
      * called to update the database.
      *
      * @param columnIndex
      *            the first column is 1, the second is 2, ...
      * @param x
      *            the new column value
      * @param length
      *            the length of the stream
      * @exception SQLException
      *                if a database-access error occurs
      */
     public void updateBinaryStream(int columnIndex, java.io.InputStream x,
         int length) throws SQLException {
         checkIfClosed("updateBinaryStream");
         updateBinaryStream(columnIndex,x,(long)length);
     }

     /**
      *
      * JDBC 2.0
      *
      * Update a column with a character stream value.
      *
      * The updateXXX() methods are used to update column values in the current
      * row, or the insert row. The updateXXX() methods do not update the
      * underlying database, instead the updateRow() or insertRow() methods are
      * called to update the database.
      *
      * @param columnIndex
      *            the first column is 1, the second is 2, ...
      * @param x
      *            the new column value
      * @param length
      *            the length of the stream
      * @exception SQLException
      *                if a database-access error occurs
      */
     public void updateCharacterStream(int columnIndex, java.io.Reader x,
         int length) throws SQLException {
         checkIfClosed("updateCharacterStream");
         updateCharacterStream(columnIndex,x,(long)length);
     }

     /**
      *
      * JDBC 4.0
      *
      * Update a column with an ascii stream value.
      *
      * The updateXXX() methods are used to update column values in the current
      * row, or the insert row. The updateXXX() methods do not update the
      * underlying database, instead the updateRow() or insertRow() methods are
      * called to update the database.
      *
      * @param columnName
      *            the name of the column
      * @param x
      *            the new column value
      * @param length
      *            of the stream
      * @exception SQLException
      *                if a database-access error occurs
      */
     public void updateAsciiStream(String columnName, java.io.InputStream x,
         long length) throws SQLException {
         checkIfClosed("updateAsciiStream");
         updateAsciiStream(findColumnName(columnName),x,length);
     }

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or </code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName the label for the column specified with the SQL AS
     *      clause. If the SQL AS clause was not specified, then the label is
     *      the name of the column
     * @param x the new column value
     * @throws SQLException if the columnIndex is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateAsciiStream(String columnName, InputStream x)
            throws SQLException {
        checkIfClosed("updateAsciiStream");
        updateAsciiStream(findColumnName(columnName), x);
    }

     /**
      *
      * JDBC 4.0
      *
      * Update a column with a binary stream value.
      *
      * The updateXXX() methods are used to update column values in the current
      * row, or the insert row. The updateXXX() methods do not update the
      * underlying database, instead the updateRow() or insertRow() methods are
      * called to update the database.
      *
      * @param columnName
      *            the name of the column
      * @param x
      *            the new column value
      * @param length
      *            of the stream
      * @exception SQLException
      *                if a database-access error occurs
      */

     public void updateBinaryStream(String columnName, java.io.InputStream x,
         long length) throws SQLException {
         checkIfClosed("updateBinaryStream");
         updateBinaryStream(findColumnName(columnName),x,length);
     }

    /**
     * Updates the designated column with a binary stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName the label for the column specified with the SQL AS
     *      clause. If the SQL AS clause was not specified, then the label is
     *      the name of the column
     * @param x the new column value
     * @throws SQLException if the columnLabel is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateBinaryStream(String columnName, InputStream x)
            throws SQLException {
        checkIfClosed("updateBinaryStream");
        updateBinaryStream(findColumnName(columnName), x);
    }

     /**
      * JDBC 4.0
      *
      * Update a column with a character stream value.
      *
      * The updateXXX() methods are used to update column values in the current
      * row, or the insert row. The updateXXX() methods do not update the
      * underlying database, instead the updateRow() or insertRow() methods are
      * called to update the database.
      *
      * @param columnName
      *            the name of the column
      * @param reader
      *            the new column value
      * @param length
      *            length of the stream
      * @exception SQLException
      *                if a database-access error occurs
      */
     public void updateCharacterStream(String columnName, java.io.Reader reader,
         long length) throws SQLException {
         checkIfClosed("updateCharacterStream");
         updateCharacterStream(findColumnName(columnName),reader,length);
     }

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream as needed until end-of-stream is
     * reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or </code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName the label for the column specified with the SQL AS
     *      clause. If the SQL AS clause was not specified, then the label is
     *      the name of the column
     * @param reader the new column value
     * @throws SQLException if the columnIndex is not valid; if a database
     *      access error occurs; the result set concurrency is
     *      <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *      result set
     */
    public void updateCharacterStream(String columnName, Reader reader)
            throws SQLException {
        checkIfClosed("updateCharacterStream");
        updateCharacterStream(findColumnName(columnName), reader);
    }

     /**
      *
      * JDBC 4.0
      *
      * Updates the designated column with a java.sql.Blob value. The updater
      * methods are used to update column values in the current row or the insert
      * row. The updater methods do not update the underlying database; instead
      * the updateRow or insertRow methods are called to update the database.
      *
      * @param columnIndex -
      *            the first column is 1, the second is 2
      * @param x -
      *            the new column value
      * @param length -
      *            the length of the Blob datatype
      * @exception SQLException
      *
      */
     public void updateBlob(int columnIndex, InputStream x, long length)
     throws SQLException {
         checksBeforeUpdateXXX("updateBlob", columnIndex);
         int colType = getColumnType(columnIndex);
         if (colType != Types.BLOB)
             throw dataTypeConversion(columnIndex, "java.sql.Blob");

         if (x == null)
             updateNull(columnIndex);
         else {
             updateBinaryStreamInternal(columnIndex, x, false, length,
                                        "updateBlob");
         }
     }

    /**
     * Updates the designated column using the given input stream.
     * The data will be read from the stream as needed until end-of-stream is reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the updateRow or insertRow methods are called to
     * update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x an object that contains the data to set the
     *     parameter value to.
     * @throws SQLException if the columnIndex is not valid; if a database
     *     access error occurs; the result set concurrency is
     *     <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *     result set
     */
    public void updateBlob(int columnIndex, InputStream x)
           throws SQLException {
       checksBeforeUpdateXXX("updateBlob", columnIndex);
       int colType = getColumnType(columnIndex);
       if (colType != Types.BLOB) {
            throw dataTypeConversion(columnIndex, "java.sql.Blob");
       }
       updateBinaryStreamInternal(columnIndex, x, true, -1, "updateBlob");
    }

     /**
      *
      * JDBC 4.0
      *
      * Updates the designated column with a java.sql.Blob value. The updater
      * methods are used to update column values in the current row or the insert
      * row. The updater methods do not update the underlying database; instead
      * the updateRow or insertRow methods are called to update the database.
      *
      * @param columnName -
      *            the name of the column to be updated
      * @param x -
      *            the new column value
      * @param length -
      *            the length of the Blob datatype
      * @exception SQLException
      *
      */

     public void updateBlob(String columnName, InputStream x, long length)
     throws SQLException {
         checkIfClosed("updateBlob");
         updateBlob(findColumnName(columnName),x,length);
     }

    /**
     * Updates the designated column using the given input stream.
     * The data will be read from the stream as needed until end-of-stream is reached.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the updateRow or insertRow methods are called to
     * update the database.
     *
     * @param columnName the label for the column specified with the SQL AS
     *     clause. If the SQL AS clause was not specified, then the label is
     *     the name of the column
     * @param x an object that contains the data to set the
     *     parameter value to.
     * @throws SQLException if the columnIndex is not valid; if a database
     *     access error occurs; the result set concurrency is
     *     <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *     result set
     */
    public void updateBlob(String columnName, InputStream x)
           throws SQLException {
       checkIfClosed("updateBlob");
       updateBlob(findColumnName(columnName), x);
    }

     /**
      *
      * JDBC 4.0
      *
      * Updates the designated column with a java.sql.Clob value. The updater
      * methods are used to update column values in the current row or the insert
      * row. The updater methods do not update the underlying database; instead
      * the updateRow or insertRow methods are called to update the database.
      *
      * @param columnIndex -
      *            the first column is 1, the second is 2
      * @param x -
      *            the new column value
      * @exception SQLException
      *                Feature not implemented for now.
      */
     public void updateClob(int columnIndex, Reader x, long length)
     throws SQLException {
         checksBeforeUpdateXXX("updateClob", columnIndex);
         int colType = getColumnType(columnIndex);
         if (colType != Types.CLOB)
             throw dataTypeConversion(columnIndex, "java.sql.Clob");

         if (x == null) {
             updateNull(columnIndex);
         } else {
             updateCharacterStreamInternal(
                 columnIndex, x, false, length, "updateClob");
         }
     }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object.
     *
     * The data will be read from the stream as needed until end-of-stream is
     * reached. The JDBC driver will do any necessary conversion from
     * <code>UNICODE</code> to the database char format.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x an object that contains the data to set the parameter
     *     value to
     * @throws SQLException if the columnIndex is not valid; if a database
     *     access error occurs; the result set concurrency is
     *     <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *     result set
     */
    public void updateClob(int columnIndex, Reader x)
           throws SQLException {
        checksBeforeUpdateXXX("updateClob", columnIndex);
        int colType = getColumnType(columnIndex);
        if (colType != Types.CLOB) {
            throw dataTypeConversion(columnIndex, "java.sql.Clob");
        }
        updateCharacterStreamInternal(columnIndex, x, true, -1, "updateClob");
    }

     /**
      *
      * JDBC 4.0
      *
      * Updates the designated column with a java.sql.Clob value. The updater
      * methods are used to update column values in the current row or the insert
      * row. The updater methods do not update the underlying database; instead
      * the updateRow or insertRow methods are called to update the database.
      *
      * @param columnName -
      *            the name of the Clob column
      * @param x -
      *            the new column value
      * @exception SQLException
      *                Feature not implemented for now.
      */

     public void updateClob(String columnName, Reader x, long length)
     throws SQLException {
         checkIfClosed("updateClob");
         updateClob(findColumnName(columnName),x,length);
     }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object.
     *
     * The data will be read from the stream as needed until end-of-stream is
     * reached. The JDBC driver will do any necessary conversion from
     * <code>UNICODE</code> to the database char format.
     *
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName the label for the column specified with the SQL AS
     *     clause. If the SQL AS clause was not specified, then the label is
     *     the name of the column
     * @param x an object that contains the data to set the parameter
     *     value to
     * @throws SQLException if the columnIndex is not valid; if a database
     *     access error occurs; the result set concurrency is
     *     <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *     result set
     */
    public void updateClob(String columnName, Reader x)
           throws SQLException {
       checkIfClosed("updateClob");
       updateClob(findColumnName(columnName), x);
    }
    
    /* 
     * @see org.apache.derby.iapi.jdbc.EngineResultSet#isNull(int)
     */
    public boolean isNull(int columnIndex) throws SQLException{
        try {
            DataValueDescriptor dvd = getColumn(columnIndex);
            return dvd.isNull();
        } catch (StandardException t) {
                throw noStateChangeException(t);
        }
    }
    
    public int getLength(int columnIndex) throws SQLException {
        try {
            DataValueDescriptor dvd = getColumn(columnIndex);
            return dvd.getLength();            
        } catch (StandardException t) {
                throw noStateChangeException(t);
        }
    }
    
}
