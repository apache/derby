/*

   Derby - Class org.apache.derby.impl.sql.execute.ScrollInsensitiveResultSet

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.Row;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.SQLInteger;

/**
 * Provide insensitive scrolling functionality for the underlying
 * result set.  We build a hash table of rows as the user scrolls
 * forward, with the position as the key.
 */

public class ScrollInsensitiveResultSet extends NoPutResultSetImpl
	implements CursorResultSet
{
	/*
    ** Set in constructor and not altered during life of object.
	*/

    public NoPutResultSet	source;



	private int							sourceRowWidth;
	private TransactionController		tc;

	private	  BackingStoreHashtable		ht;
	private	  ExecRow					resultRow;

	// Scroll tracking
	private int positionInSource;
	private int currentPosition;
	private int lastPosition;
	private	boolean seenFirst;
	private	boolean seenLast;
	private	boolean beforeFirst = true;
	private	boolean afterLast;

	public int numFromHashTable;
	public int numToHashTable;

	private int maxRows;

    private GeneratedMethod closeCleanup;

	/**
	 * Constructor for a ScrollInsensitiveResultSet
	 *
	 * @param source					The NoPutResultSet from which to get rows
	 *									to scroll through
	 * @param activation				The activation for this execution
	 * @param resultSetNumber			The resultSetNumber
	 * @param sourceRowWidth			# of columns in the source row
	 *
	 * @exception StandardException	on error
	 */

	public ScrollInsensitiveResultSet(NoPutResultSet source,
							  Activation activation, int resultSetNumber,
							  int sourceRowWidth,
							  double optimizerEstimatedRowCount,
							  double optimizerEstimatedCost,
							  GeneratedMethod c) throws StandardException
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.source = source;
		this.sourceRowWidth = sourceRowWidth;
		maxRows = activation.getMaxRows();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(maxRows != -1,
				"maxRows not expected to be -1");
		}

        closeCleanup = c;
		constructorTime += getElapsedMillis(beginTime);
	}


	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * open a scan on the source. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
 	 * @exception StandardException thrown on failure 
     */
	public void	openCore() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "ScrollInsensitiveResultSet already open");

        source.openCore();
	    isOpen = true;
		numOpens++;

		/* Create the hash table.  We pass
		 * null in as the row source as we will
		 * build the hash table on demand as
		 * the user scrolls.
		 * The 1st column, the position in the
		 * scan, will be the key column.
		 */
		int[] keyCols = new int[1];
		// keyCols[0] = 0; // not req. arrays initialized to zero

		/* We don't use the optimizer row count for this because it could be
		 * wildly pessimistic.  We only use Hash tables when the optimizer row count
		 * is within certain bounds.  We have no alternative for scrolling insensitive 
		 * cursors so we'll just trust that it will fit.
		 * We need BackingStoreHashtable to actually go to disk when it doesn't fit.
		 * This is a known limitation.
		 */
		ht = new BackingStoreHashtable(tc,
									   null,
									   keyCols,
									   false,
										-1, // don't trust optimizer row count
									   HashScanResultSet.DEFAULT_MAX_CAPACITY,
									   HashScanResultSet.DEFAULT_INITIAL_CAPACITY,
									   HashScanResultSet.DEFAULT_MAX_CAPACITY,
									   false);

		openTime += getElapsedMillis(beginTime);
		setBeforeFirstRow();
	}

	/**
     * reopen a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	reopenCore() throws StandardException 
	{
		boolean constantEval = true;

		beginTime = getCurrentTimeMillis();

		if (SanityManager.DEBUG)
		{
		    SanityManager.ASSERT(isOpen, "ScrollInsensitiveResultSet already open");
			SanityManager.THROWASSERT(
				"reopenCore() not expected to be called");
		}
		setBeforeFirstRow();
	}

	/**
	 * Returns the row at the absolute position from the query, 
	 * and returns NULL when there is no such position.
	 * (Negative position means from the end of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: An exception will be thrown on 0.
	 *
	 * @param row	The position.
	 * @return	The row at the absolute position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getAbsoluteRow(int row) throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "absolute");
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}
		}

		// 0 is an invalid parameter
		if (row == 0)
		{
			throw StandardException.newException(SQLState.LANG_ZERO_INVALID_FOR_R_S_ABSOLUTE);
		}

		if (row > 0)
		{
			// position is from the start of the result set
			if (row <= positionInSource)
			{
				// We've already seen the row before
				return getRowFromHashTable(row);
			}
			
			/* We haven't seen the row yet, scan until we find
			 * it or we get to the end.
			 */
			int diff = row - positionInSource;
			ExecRow result = null;
			while (diff > 0)
			{
				if ((result = getNextRowFromSource()) != null)
				{
					diff--;
				}
				else
				{
					break;
				}
			}
			currentRow = result;
			return result;
		}
		else if (row < 0)
		{
			// position is from the end of the result set

			// Get the last row, if we haven't already
			if (!seenLast)
			{
				getLastRow();
			}

			// Note, for negative values position is from beyond the end
			// of the result set, e.g. absolute(-1) points to the last row
			int beyondResult = lastPosition + 1;
			if (beyondResult + row > 0)
			{
				// valid row
				return getRowFromHashTable(beyondResult + row);
			}
			else
			{
				// position before the beginning of the result set
				return setBeforeFirstRow();
			}
		}
 
		currentRow = null;
		return null;
	}

	/**
	 * Returns the row at the relative position from the current
	 * cursor position, and returns NULL when there is no such position.
	 * (Negative position means toward the beginning of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: 0 is valid.
	 * NOTE: An exception is thrown if the cursor is not currently
	 * positioned on a row.
	 *
	 * @param row	The position.
	 * @return	The row at the relative position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getRelativeRow(int row) throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "relative");
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}
		}

		/* Throw exception if before first or after last */
		if (beforeFirst || afterLast)
		{
			throw StandardException.newException(SQLState.LANG_NO_CURRENT_ROW_FOR_RELATIVE);
		}

		// Return the current row for 0
		if (row == 0)
		{
			return getRowFromHashTable(currentPosition);
		}
		else if (row > 0)
		{
			return getAbsoluteRow(currentPosition + row);
		}
		else
		{
			// row < 0
			if (currentPosition + row < 0)
			{
				return setBeforeFirstRow();
			}
			return getAbsoluteRow(currentPosition + row);
		}
	}

	/**
	 * Sets the current position to before the first row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @see Row
	 */
	public ExecRow	setBeforeFirstRow() 
	{
		currentPosition = 0;
		beforeFirst = true;
		afterLast = false;
		currentRow = null;
		return null;
	}

	/**
	 * Returns the first row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The first row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getFirstRow() 
		throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "first");
		}

		/* Get the row from the hash table if
		 * we have already seen it before.
		 */
		if (seenFirst)
		{
			return getRowFromHashTable(1);
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}
		}

		return getNextRowCore();
	}

	/**
	 *
 	 * @exception StandardException thrown on failure 
	 */
	public ExecRow	getNextRowCore() throws StandardException
	{
		ExecRow result = null;

		beginTime = getCurrentTimeMillis();
		if (!isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

		/* Should we get the next row from the source or the hash table? */
		if (currentPosition == positionInSource)
		{
			/* Current position is same as position in source.
			 * Get row from the source.
			 */
			result = getNextRowFromSource();
		}
		else if (currentPosition < positionInSource)
		{
			/* Current position is before position in source.
			 * Get row from the hash table.
			 */
			result = getRowFromHashTable(currentPosition + 1);
		}
		else
		{
			result = null;
		}

		if (result != null)
		{
			rowsSeen++;
			afterLast = false;
		}

		currentRow = result;
		setCurrentRow(currentRow);
		beforeFirst = false;

		nextTime += getElapsedMillis(beginTime);

		return result;
	}

	/**
	 * Returns the previous row from the query, and returns NULL when there
	 * are no more previous rows.
	 *
	 * @return	The previous row, or NULL if no more previous rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getPreviousRow() 
		throws StandardException
	{
	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");
		}

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}
		}

		/* No row if we are positioned before the first row
		 * or the result set is empty.
		 */
		if (beforeFirst || currentPosition == 0)
		{
			currentRow = null;
			return null;
		}

		// Get the last row, if we are after it
		if (afterLast)
		{
			// Special case for empty tables
			if (lastPosition == 0)
			{
				afterLast = false;
				beforeFirst = false;
				currentRow = null;
				return null;
			}
			else
			{
				return getRowFromHashTable(lastPosition);
			}
		}

		// Move back 1
		currentPosition--;
		if (currentPosition == 0)
		{
			setBeforeFirstRow();
			return null;
		}
		return getRowFromHashTable(currentPosition);
	}

	/**
	 * Returns the last row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The last row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	getLastRow()
		throws StandardException
	{
		ExecRow result;

	    if ( ! isOpen ) 
		{
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");
		}

		/* Have we already seen the last row? */
		if (seenLast)
		{
			// Return null if the set was empty
			if (lastPosition == 0)
			{
				currentRow = null;
				return null;
			}
			else
			{
				return getRowFromHashTable(lastPosition);
			}
		}

		attachStatementContext();

		if (SanityManager.DEBUG)
		{
			if (!isTopResultSet)
			{
				SanityManager.THROWASSERT(
					this + "expected to be the top ResultSet");
			}
		}

		/* Scroll to the end, filling the hash table as
		 * we scroll, and return the last row that we find.
		 */
		while ((result = getNextRowFromSource()) != null);
		beforeFirst = false;
		afterLast = false;

		// Special case if table is empty
		if (lastPosition == 0)
		{
			currentRow = null;
			return null;
		}
		else
		{
			return getRowFromHashTable(lastPosition);
		}
	}

	/**
	 * Sets the current position to after the last row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	public ExecRow	setAfterLastRow() 
		throws StandardException
	{
		if (! seenLast)
		{
			getLastRow();
		}
		currentPosition = lastPosition + 1;
		afterLast = true;
		beforeFirst = false;
		currentRow = null;
		return null;
	}

    /**
     * Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
	 * @exception StandardException Thrown on error.
     */
   public boolean checkRowPosition(int isType) throws StandardException
	{
		switch (isType) {
		case ISBEFOREFIRST:

			if (! beforeFirst)
			{
				return false;
			}

			//  Spec says to return false if result set is empty
			if (seenFirst)
			{
				return true;
			}
			else
			{
				ExecRow firstRow = getFirstRow();
				if (firstRow == null)
				{
					// ResultSet is empty
					return false;
				}
				else
				{
					// ResultSet is not empty - reset position
					getPreviousRow();
					return true;
				}
			}
		case ISFIRST:
			return (currentPosition == 1);
		case ISLAST:
			if (beforeFirst || afterLast)
			{
				return false;
			}

			/* If we've already seen the last row
			 * then we can tell if we are on it by
			 * the current position,
			 * otherwise, we need to find the last
			 * row in order to tell if the current row
			 * is the last row.
			 */
			if (seenLast)
			{
				return (currentPosition == lastPosition && currentPosition != 0);
			}
			else
			{
				int savePosition = currentPosition;
				boolean retval = false;
				getLastRow();
				if (savePosition == lastPosition && savePosition != 0)
				{
					retval = true;
				}
				getRowFromHashTable(savePosition);
				return retval;
			}
		case ISAFTERLAST:
			return afterLast;
		default:
			return false;
		}
	}

	/**
	 * Returns the row number of the current row.  Row
	 * numbers start from 1 and go to 'n'.  Corresponds
	 * to row numbering used to position current row
	 * in the result set (as per JDBC).
	 *
	 * @return	the row number, or 0 if not on a row
	 *
	 */
	public int getRowNumber()
	{
		return currentRow == null ? 0 : currentPosition;
	}

	/* Get the next row from the source ResultSet tree and insert into the hash table */
	private ExecRow getNextRowFromSource() throws StandardException
	{
		ExecRow		sourceRow = null;
		ExecRow		result = null;

		/* Don't give back more rows than requested */
		if (maxRows > 0 && maxRows == positionInSource)
		{
			seenLast = true;
			lastPosition = positionInSource;
			afterLast = true;
			return null;
		}

		sourceRow = source.getNextRowCore();

		if (sourceRow != null)
		{
			seenFirst = true;
			beforeFirst = false;

			long beginTCTime = getCurrentTimeMillis();
			/* If this is the first row from the source then we create a new row
			 * for use when fetching from the hash table.
			 */
			if (resultRow == null)
			{
				resultRow = activation.getExecutionFactory().getValueRow(sourceRowWidth);
			}

			positionInSource++;
			currentPosition = positionInSource;
			addRowToHashTable(sourceRow);

		}
		// Remember whether or not we're past the end of the table
		else
		{
			if (! seenLast)
			{
				lastPosition = positionInSource;
			}
			seenLast = true;
			// Special case for empty table (afterLast is never true)
			if (positionInSource == 0)
			{
				afterLast = false;
			}
			else
			{
				afterLast = true;
			}
		}

		return sourceRow;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
	    if ( isOpen )
	    {
			if (closeCleanup != null) 
			{
				closeCleanup.invoke(activation); // let activation tidy up
			} 
			currentRow = null;
	        source.close();

			if (ht != null)
			{
				ht.close();
				ht = null;
			}

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of ScrollInsensitiveResultSet repeated");
		setBeforeFirstRow();

		closeTime += getElapsedMillis(beginTime);
	}

	public void	finish() throws StandardException
	{
		source.finish();
		finishAndRTS();
	}

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
		{
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		}
		else
		{
			return totTime;
		}
	}

	//
	// CursorResultSet interface
	//

	/**
	 * Gets information from its source. We might want
	 * to have this take a CursorResultSet in its constructor some day,
	 * instead of doing a cast here?
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 *
 	 * @exception StandardException thrown on failure 
	 */
	public RowLocation getRowLocation() throws StandardException 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(source instanceof CursorResultSet, "source not CursorResultSet");
		return ( (CursorResultSet)source ).getRowLocation();
	}

	/**
	 * Gets information from last getNextRow call.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() 
	{
		return currentRow;
	}

	//
	// class implementation
	//

	/**
	 * Add a row to the backing hash table,
	 * keyed on positionInSource.
	 *
	 * @param sourceRow	The row to add.
	 *
	 * @return Nothing.
	 */
	private void addRowToHashTable(ExecRow sourceRow)
		throws StandardException
	{
		DataValueDescriptor[] hashRowArray = new DataValueDescriptor[sourceRowWidth + 1];
		// 1st element is the key
		hashRowArray[0] = new SQLInteger(positionInSource);

		/* Copy rest of elements from sourceRow.
		 * NOTE: We need to clone the source row
		 * and we do our own cloning since the 1st column
		 * is not a wrapper.
		 */
		DataValueDescriptor[] sourceRowArray = sourceRow.getRowArrayClone();

		System.arraycopy(sourceRowArray, 0, hashRowArray, 1, sourceRowArray.length);

		ht.put(false, hashRowArray);

		numToHashTable++;
	}

	/**
	 * Get the row at the specified position
	 * from the hash table.
	 *
	 * @param position	The specified position.
	 *
	 * @return	The row at that position.
	 *
 	 * @exception StandardException thrown on failure 
	 */
	private ExecRow getRowFromHashTable(int position)
		throws StandardException
	{

		// Get the row from the hash table
		DataValueDescriptor[] hashRowArray = (DataValueDescriptor[]) ht.get(new SQLInteger(position));


		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(hashRowArray != null,
				"hashRowArray expected to be non-null");
		}
		// Copy out the Object[] without the position.
		DataValueDescriptor[] resultRowArray = new DataValueDescriptor[hashRowArray.length - 1];
		System.arraycopy(hashRowArray, 1, resultRowArray, 0, resultRowArray.length);

		resultRow.setRowArray(resultRowArray);

		// Reset the current position to the user position
		currentPosition = position;

		numFromHashTable++;

		if (resultRow != null)
		{
			beforeFirst = false;
			afterLast = false;
		}

		currentRow = resultRow;
		return resultRow;
	}
}
