/*

   Derby - Class org.apache.derby.impl.sql.execute.NormalizeResultSet

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

/**
 * Cast the rows from the source result set to match the format of the
 * result set for the entire statement.
 */

class NormalizeResultSet extends NoPutResultSetImpl
	implements CursorResultSet
{
	/*
    ** Set in constructor and not altered during life of object.
	*/

    public NoPutResultSet	source;
	private	ExecRow			normalizedRow;
	private	int				numCols;
	private int				startCol;

    /**
     * This array contains data value descriptors that can be used (and reused)
     * by normalizeRow() to hold the normalized column values.
     */
    private final DataValueDescriptor[] cachedDestinations;

	/* RESOLVE - We need to pass the ResultDescription for this ResultSet
	 * as a parameter to the constructor and use it instead of the one from
	 * the activation
	 */
	private ResultDescription resultDescription;

	/* info for caching DTSs */
	private DataTypeDescriptor[] desiredTypes;

	/**
	 * Constructor for a NormalizeResultSet
	 *
	 * @param source					The NoPutResultSet from which to get rows
	 *									to be normalized
	 * @param activation				The activation for this execution
	 * @param resultSetNumber			The resultSetNumber
	 * @param erdNumber					The integer for the ResultDescription
	 *
	 * @exception StandardException	on error
	 */

	public NormalizeResultSet(NoPutResultSet source,
							  Activation activation, int resultSetNumber,
							  int erdNumber,
	 					      double optimizerEstimatedRowCount,
							  double optimizerEstimatedCost,
							  boolean forUpdate) throws StandardException
	{
		super(activation, resultSetNumber, optimizerEstimatedRowCount, 
			  optimizerEstimatedCost);
		this.source = source;

		if (SanityManager.DEBUG)
		{
			if (! (activation.getPreparedStatement().getSavedObject(erdNumber)
							 instanceof ResultDescription))
			{
				SanityManager.THROWASSERT(
					"activation.getPreparedStatement().getSavedObject(erdNumber) " +
					"expected to be instanceof ResultDescription");
			}

			// source expected to be non-null, mystery stress test bug
			// - sometimes get NullPointerException in openCore().
			SanityManager.ASSERT(source != null,
				"NRS(), source expected to be non-null");
		}

		this.resultDescription = 
			(ResultDescription) activation.getPreparedStatement().getSavedObject(erdNumber);

		numCols = resultDescription.getColumnCount();
		
		startCol = computeStartColumn( forUpdate, resultDescription );
		normalizedRow = activation.getExecutionFactory().getValueRow(numCols);
        cachedDestinations = new DataValueDescriptor[numCols];
		recordConstructorTime();
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
	    	SanityManager.ASSERT( ! isOpen, "NormalizeResultSet already open");

		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"NRS().openCore(), source expected to be non-null");
		}

        source.openCore();
	    isOpen = true;
		numOpens++;

		openTime += getElapsedMillis(beginTime);
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
		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT(isOpen, "NormalizeResultSet already open");

		source.reopenCore();
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
	 *
 	 * @exception StandardException thrown on failure 
	 */
	public ExecRow	getNextRowCore() throws StandardException
	{
		if( isXplainOnlyMode() )
			return null;

		ExecRow		sourceRow = null;
		ExecRow		result = null;

		beginTime = getCurrentTimeMillis();
		if (!isOpen)
			throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN, "next");

		sourceRow = source.getNextRowCore();
		if (sourceRow != null)
		{
			result = normalizeRow(sourceRow);
			rowsSeen++;
		}

		setCurrentRow(result);

		nextTime += getElapsedMillis(beginTime);
		return result;
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
			currentRow = null;
	        source.close();

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of NormalizeResultSet repeated");

		closeTime += getElapsedMillis(beginTime);
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
			SanityManager.ASSERT(source instanceof CursorResultSet, "source is not a cursorresultset");
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

    /**
     * <p>
     * Compute the start column for an update/insert.
     *
     * @param isUpdate  True if we are executing an UPDATE statement
     * @param desc Metadata describing a result row
     * </p>
     */
    public  static  int computeStartColumn( boolean isUpdate, ResultDescription desc )
    {
		int count = desc.getColumnCount();
        
		/*
		  An update row, for an update statement which sets n columns; i.e
		     UPDATE tab set x,y,z=.... where ...;
		  has,
		  before values of x,y,z after values of x,y,z and rowlocation.
		  need only normalize after values of x,y,z.
		  i.e insead of starting at index = 1, I need to start at index = 4.
		  also I needn't normalize the last value in the row.
        */
		return (isUpdate) ? ((count - 1)/ 2) + 1 : 1;
    }

    
	/**
	 * Normalize a column.  For now, this means calling constructors through
	 * the type services to normalize a type to itself.  For example,
	 * if you're putting a char(30) value into a char(15) column, it
	 * calls a SQLChar constructor with the char(30) value, and the
	 * constructor truncates the value and makes sure that no non-blank
	 * characters are truncated.
	 *
     *
     * @param dtd Data type to coerce to
     * @param sourceRow row holding the source column
     * @param sourceColumnPosition position of column in row
     * @param resultCol where to stuff the coerced value
     * @param desc Additional metadata for error reporting if necessary
     *
 	 * @exception StandardException thrown on failure 
	 */
	public  static  DataValueDescriptor normalizeColumn
        (DataTypeDescriptor dtd, ExecRow sourceRow, int sourceColumnPosition, DataValueDescriptor resultCol, ResultDescription desc )
        throws StandardException
	{
        DataValueDescriptor sourceCol = sourceRow.getColumn( sourceColumnPosition );

        try {
            DataValueDescriptor returnValue = dtd.normalize( sourceCol, resultCol );

            return returnValue;
        } catch (StandardException se) {
            // Catch illegal null insert and add column info
            if (se.getMessageId().startsWith(SQLState.LANG_NULL_INTO_NON_NULL))
            {
                ResultColumnDescriptor columnDescriptor = desc.getColumnDescriptor( sourceColumnPosition );
                throw StandardException.newException
                    (SQLState.LANG_NULL_INTO_NON_NULL, columnDescriptor.getName());
            }
            //just rethrow if not LANG_NULL_INTO_NON_NULL
            throw se;
        }
    }
    
	//
	// class implementation
	//
    
	/**
	 * Normalize a row.
	 *
	 * @param sourceRow		The row to normalize
	 *
	 * @return	The normalized row
	 *
 	 * @exception StandardException thrown on failure 
	 */
	private ExecRow normalizeRow(ExecRow sourceRow) throws StandardException
	{
        int                     count = resultDescription.getColumnCount();

		for (int i = 1; i <= count; i++)
		{
			DataValueDescriptor sourceCol = sourceRow.getColumn( i );
			if (sourceCol != null)
			{
				DataValueDescriptor	normalizedCol;
				// skip the before values in case of update
				if (i < startCol)
                { normalizedCol = sourceCol; }
				else
                {
                    normalizedCol = normalizeColumn(
                            getDesiredType(i), sourceRow, i,
                            getCachedDestination(i), resultDescription);
                }

				normalizedRow.setColumn(i, normalizedCol);
			}
		}

		return normalizedRow;
	}

    /**
     * Get a cached data value descriptor that can receive the normalized
     * value of the specified column.
     *
     * @param col the column number (1-based)
     * @return a data value descriptor of the correct type for the column
     * @throws StandardException if a new data value descriptor cannot be
     * created
     */
    private DataValueDescriptor getCachedDestination(int col)
            throws StandardException {
        int index = col - 1;
        if (cachedDestinations[index] == null) {
            cachedDestinations[index] = getDesiredType(col).getNull();
        }
        return cachedDestinations[index];
    }

    /**
     * Get a data type descriptor that describes the desired type for the
     * specified column.
     *
     * @param col the column number (1-based)
     * @return a data type descriptor for the column
     */
    private DataTypeDescriptor getDesiredType(int col) {
        if (desiredTypes == null) {
            desiredTypes = fetchResultTypes(resultDescription);
        }
        return desiredTypes[col - 1];
    }

    /**
     * <p>
     * Fetch the result datatypes out of the activation.
     * </p>
     */
    private DataTypeDescriptor[] fetchResultTypes(ResultDescription desc)
    {
        int     count = desc.getColumnCount();

        DataTypeDescriptor[]    result = new DataTypeDescriptor[ count ];
        
        for ( int i = 1; i <= count; i++)
        {
            ResultColumnDescriptor  colDesc = desc.getColumnDescriptor(  i );
            DataTypeDescriptor dtd = colDesc.getType();

            result[i - 1] = dtd;
        }

        return result;
    }

	/**
	 * @see NoPutResultSet#updateRow
	 */
	public void updateRow (ExecRow row, RowChanger rowChanger)
			throws StandardException {
		source.updateRow(row, rowChanger);
	}

	/**
	 * @see NoPutResultSet#markRowAsDeleted
	 */
	public void markRowAsDeleted() throws StandardException {
		source.markRowAsDeleted();
	}

}
