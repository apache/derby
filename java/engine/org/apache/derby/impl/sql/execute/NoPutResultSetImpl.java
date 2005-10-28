/*

   Derby - Class org.apache.derby.impl.sql.execute.NoPutResultSetImpl

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory;
import org.apache.derby.iapi.sql.execute.TargetResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.RowSource;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.RowLocation;

import java.sql.Timestamp;


/**
 * Abstract ResultSet with built in Activation support for operations that
 * return rows but do not allow the caller to put data on output pipes. This
 * implementation of ResultSet is meant to be overridden by subtypes in the
 * execution engine. Its primary users will be DML operations that do not put
 * data on output pipes, but simply return it due to being result sets
 * themselves.
 * <p>
 * This abstract class does not define the entire ResultSet
 * interface, but leaves the 'get' half of the interface
 * for subtypes to implement. It is package-visible only,
 * with its methods being public for exposure by its subtypes.
 * <p>
 */
abstract class NoPutResultSetImpl
extends BasicNoPutResultSetImpl
{
	/* Set in constructor and not modified */
	public final int				resultSetNumber;

	/* fields used for formating run time statistics output */
	protected String indent;
	protected String subIndent;
	protected int sourceDepth;

	// fields used when being called as a RowSource
	private boolean needsRowLocation;
	protected ExecRow clonedExecRow;
	GeneratedMethod	checkGM;
	long			heapConglomerate;
	protected TargetResultSet	targetResultSet;

	/* beetle 4464. compact flags into array of key column positions that we do check/skip nulls,
	 * so that we burn less cycles for each row, column.
	 */
	protected int[] checkNullCols;
	protected int cncLen;

	/**
	 *  Constructor
	 *
	 *	@param	activation			The activation
	 *	@param	resultSetNumber		The resultSetNumber
	 *  @param	optimizerEstimatedRowCount	The optimizer's estimated number
	 *										of rows.
	 *  @param	optimizerEstimatedCost		The optimizer's estimated cost
	 */
	NoPutResultSetImpl(Activation activation,
						int resultSetNumber,
						double optimizerEstimatedRowCount,
						double optimizerEstimatedCost)
	{
		super(null,
				activation,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(activation!=null, "activation expected to be non-null");
			SanityManager.ASSERT(resultSetNumber >= 0, "resultSetNumber expected to be >= 0");
		}
		this.resultSetNumber = resultSetNumber;
	}

	// NoPutResultSet interface

	/**
     * Returns the description of the table's rows
	 */
	public ResultDescription getResultDescription() {
	    return activation.getResultDescription();
	}

	/**
		Return my cursor name for JDBC. Can be null.
	*/
	public String getCursorName() {

		String cursorName = activation.getCursorName();
		if ((cursorName == null) && isForUpdate()) {

			activation.setCursorName(activation.getLanguageConnectionContext().getUniqueCursorName());

			cursorName = activation.getCursorName();
		}

		return cursorName;
	}

	/** @see NoPutResultSet#resultSetNumber() */
	public int resultSetNumber() {
		return resultSetNumber;
	}

	/**
		Close needs to invalidate any dependent statements, if this is a cursor.
		Must be called by any subclasses that override close().
		@exception StandardException on error
	*/
	public void close() throws StandardException
	{
		if (!isOpen)
			return;

		/* If this is the top ResultSet then we must
		 * close all of the open subqueries for the
		 * entire query.
		 */
		if (isTopResultSet)
		{
			int staLength = (subqueryTrackingArray == null) ? 0 :
								subqueryTrackingArray.length;

			for (int index = 0; index < staLength; index++)
			{
				if (subqueryTrackingArray[index] == null)
				{
					continue;
				}
				if (subqueryTrackingArray[index].isClosed())
				{
					continue;
				}
				subqueryTrackingArray[index].close();
			}
		}

		/*
		** If we are the activation's top result set, make it forget about
		** us, because we're closed now.
		*/
		if (activation.getResultSet() == this)
		{
			activation.clearResultSet();
		}

		isOpen = false;

	}

	/** @see NoPutResultSet#setTargetResultSet */
	public void setTargetResultSet(TargetResultSet trs)
	{
		targetResultSet = trs;
	}

	/** @see NoPutResultSet#setNeedsRowLocation */
	public void setNeedsRowLocation(boolean needsRowLocation)
	{
		this.needsRowLocation = needsRowLocation;
	}

	// RowSource interface
	
	/** 
	 * @see RowSource#getValidColumns
	 */
	public FormatableBitSet getValidColumns()
	{
		// All columns are valid
		return null;
	}
	
	/** 
	 * @see RowSource#getNextRowFromRowSource
	 * @exception StandardException on error
	 */
	public DataValueDescriptor[] getNextRowFromRowSource()
		throws StandardException
	{
		ExecRow execRow = getNextRowCore();
 		if (execRow != null)
		{
			/* Let the target preprocess the row.  For now, this
			 * means doing an in place clone on any indexed columns
			 * to optimize cloning and so that we don't try to drain
			 * a stream multiple times.  This is where we also
			 * enforce any check constraints.
			 */
			clonedExecRow = targetResultSet.preprocessSourceRow(execRow);

			return execRow.getRowArray();
		}

		return null;
	}

	/**
	 * @see RowSource#needsToClone
	 */
	public boolean needsToClone()
	{
		return(true);
	}

	/** 
	 * @see RowSource#closeRowSource
	 */
	public void closeRowSource()
	{
		// Do nothing here - actual work will be done in close()
	}


	// RowLocationRetRowSource interface

	/**
	 * @see RowLocationRetRowSource#needsRowLocation
	 */
	public boolean needsRowLocation()
	{
		return needsRowLocation;
	}

	/**
	 * @see RowLocationRetRowSource#rowLocation
	 * @exception StandardException on error
	 */
	public void rowLocation(RowLocation rl)
		throws StandardException
	{
		targetResultSet.changedRow(clonedExecRow, rl);
	}


	// class implementation

	/**
	 * Clear the Orderable cache for each qualifier.
	 * (This should be done each time a scan/conglomerate with
	 * qualifiers is reopened.)
	 *
	 * @param qualifiers	The Qualifiers to clear
	 *
	 * @return Nothing.
	 */
	protected void clearOrderableCache(Qualifier[][] qualifiers) throws StandardException
	{
		// Clear the Qualifiers's Orderable cache 
		if (qualifiers != null)
		{
			Qualifier qual;
			for (int term = 0; term < qualifiers.length; term++)
			{
				for (int index = 0; index < qualifiers[term].length; index++)
				{
					qual = qualifiers[term][index];
					qual.clearOrderableCache();
					/* beetle 4880 performance enhancement and avoid deadlock while pushing
					 * down method call to store: pre-evaluate.
					 */
					if (((GenericQualifier) qual).variantType != Qualifier.VARIANT)
						qual.getOrderable();		// ignore return value
				}
			}
		}
	}

	/* Support methods for RowSource interface.
	 * These methods are used for enabling check constraint enforcement and
	 * replication logging for published tables when we are a RowSource.
	 */

	/**
	 * Set the GeneratedMethod for enforcing check constraints
	 * 
	 * @param checkGM	The GeneratedMethod for enforcing any check constraints.
	 *
	 * @return Nothing.
	 */
	protected void setCheckConstraints(GeneratedMethod checkGM)
	{
		this.checkGM = checkGM;
	}

	/**
	 * Set the heap conglomerate number (used in enforcing check constraints)
	 * 
	 * @param heapConglomerate	The heap conglomerate number.
	 *
	 * @return Nothing.
	 */
	protected void setHeapConglomerate(long heapConglomerate)
	{
		this.heapConglomerate = heapConglomerate;
	}

	/**
	 * Set the current row to the row passed in.
	 *
	 * @param row the new current row
	 *
	 */
	public final void setCurrentRow(ExecRow row)
	{
		activation.setCurrentRow(row, resultSetNumber);
	}

	/**
	 * Clear the current row
	 *
	 */
	public final void clearCurrentRow()
	{
		activation.clearCurrentRow(resultSetNumber);
	}

	/**
	 * Is this ResultSet or it's source result set for update
	 * This method will be overriden in the inherited Classes
	 * if it is true
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return false;
	}

	/**
	 * Return true if we should skip the scan due to nulls in the start
	 * or stop position when the predicate on the column(s) in question
	 * do not implement ordered null semantics. beetle 4464, we also compact
	 * the areNullsOrdered flags into checkNullCols here.
	 *
	 * @param startPosition	An index row for the start position
	 * @param stopPosition	An index row for the stop position
	 *
	 * @return	true means not to do the scan
	 */
	protected boolean skipScan(ExecIndexRow startPosition, ExecIndexRow stopPosition)
		throws StandardException
	{
		int nStartCols = (startPosition == null) ? 0 : startPosition.nColumns();
		int nStopCols = (stopPosition == null) ? 0 : stopPosition.nColumns();

		/* Two facts 1) for start and stop key column positions, one has to be the prefix
		 * of the other, 2) startPosition.areNullsOrdered(i) can't be different from
		 * stopPosition.areNullsOrdered(i) unless the case "c > null and c < 5", (where c is
		 * non-nullable), in which we skip the scan anyway.
		 * So we can just use the longer one to get checkNullCols.
		 */
		boolean startKeyLonger = false;
		int size = nStopCols;
		if (nStartCols > nStopCols)
		{
			startKeyLonger = true;
			size = nStartCols;
		}
		if (size == 0)
			return false;
		if ((checkNullCols == null) || (checkNullCols.length < size))
			checkNullCols = new int[size];
		cncLen = 0;

		boolean returnValue = false;
		for (int position = 0; position < nStartCols; position++)
		{
			if ( ! startPosition.areNullsOrdered(position))
			{
				if (startKeyLonger)
					checkNullCols[cncLen++] = position + 1;
				if (startPosition.getColumn(position + 1).isNull())
				{
					returnValue =  true;
					if (! startKeyLonger)
						break;
				}
			}
		}
		if (startKeyLonger && returnValue)
			return true;
		for (int position = 0; position < nStopCols; position++)
		{
			if ( ! stopPosition.areNullsOrdered(position))
			{
				if (! startKeyLonger)
					checkNullCols[cncLen++] = position + 1;
				if (returnValue)
					continue;
				if (stopPosition.getColumn(position + 1).isNull())
				{
					returnValue =  true;
					if (startKeyLonger)
						break;
				}
			}
		}

		return returnValue;
	}

	/**
	 * Return true if we should skip the scan due to nulls in the row
	 * when the start or stop positioners on the columns containing
	 * null do not implement ordered null semantics.
	 *
	 * @param row	An index row
	 *
	 * @return	true means skip the row because it has null
	 */
	protected boolean skipRow(ExecRow row)  throws StandardException
	{
		for (int i = 0; i < cncLen; i++)
		{
			if (row.getColumn(checkNullCols[i]).isNull())
				return true;
		}

		return false;
	}

	/**
	 * Return a 2-d array of Qualifiers as a String
	 */
	public static String printQualifiers(Qualifier[][] qualifiers)
	{
		String idt = "";

		String output = "";
		if (qualifiers == null)
		{
			return idt + MessageService.getTextMessage(SQLState.LANG_NONE);
		}

        for (int term = 0; term < qualifiers.length; term++)
        {
            for (int i = 0; i < qualifiers[term].length; i++)
            {
                Qualifier qual = qualifiers[term][i];

                output = idt + output +
                    MessageService.getTextMessage(
                        SQLState.LANG_COLUMN_ID_ARRAY,
                            String.valueOf(term), String.valueOf(i)) +
                        ": " + qual.getColumnId() + "\n";
                    
                int operator = qual.getOperator();
                String opString = null;
                switch (operator)
                {
                  case Orderable.ORDER_OP_EQUALS:
                    opString = "=";
                    break;

                  case Orderable.ORDER_OP_LESSOREQUALS:
                    opString = "<=";
                    break;

                  case Orderable.ORDER_OP_LESSTHAN:
                    opString = "<";
                    break;

                  default:
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.THROWASSERT("Unknown operator " + operator);
                    }

                    // NOTE: This does not have to be internationalized, because
                    // this code should never be reached.
                    opString = "unknown value (" + operator + ")";
                    break;
                }
                output = output +
                    idt + MessageService.getTextMessage(SQLState.LANG_OPERATOR) +
                            ": " + opString + "\n" +
                    idt +
                        MessageService.getTextMessage(
                            SQLState.LANG_ORDERED_NULLS) +
                        ": " + qual.getOrderedNulls() + "\n" +
                    idt +
                        MessageService.getTextMessage(
                            SQLState.LANG_UNKNOWN_RETURN_VALUE) +
                        ": " + qual.getUnknownRV() + "\n" +
                    idt +
                        MessageService.getTextMessage(
                            SQLState.LANG_NEGATE_COMPARISON_RESULT) +
                        ": " + qual.negateCompareResult() + "\n";
            }
        }

		return output;
	}
}
