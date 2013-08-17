/*

   Derby - Class org.apache.derby.impl.sql.execute.SetOpResultSet

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

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.impl.sql.compile.IntersectOrExceptNode;

/**
 * Takes the result set produced by an ordered UNION ALL of two tagged result sets and produces
 * the INTERSECT or EXCEPT of the two input result sets. This also projects out the tag, the last column
 * of the input rows.
 */
class SetOpResultSet extends NoPutResultSetImpl
    implements CursorResultSet
{
    private final NoPutResultSet leftSource;
    private final NoPutResultSet rightSource;
    private final Activation activation;
    private final int opType;
    private final boolean all;
    private final int resultSetNumber;
    private DataValueDescriptor[] prevCols; /* Used to remove duplicates in the EXCEPT DISTINCT case.
                                             * It is equal to the previously output columns.
                                             */
    private ExecRow leftInputRow;
    private ExecRow rightInputRow;

    private final int[] intermediateOrderByColumns;
    private final int[] intermediateOrderByDirection;
    private final boolean[] intermediateOrderByNullsLow;

    /* Run time statistics variables */
    private int rowsSeenLeft;
    private int rowsSeenRight;
    private int rowsReturned;

    SetOpResultSet( NoPutResultSet leftSource,
                    NoPutResultSet rightSource,
                    Activation activation, 
                    int resultSetNumber,
                    long optimizerEstimatedRowCount,
                    double optimizerEstimatedCost,
                    int opType,
                    boolean all,
                    int intermediateOrderByColumnsSavedObject,
                    int intermediateOrderByDirectionSavedObject,
                    int intermediateOrderByNullsLowSavedObject)
    {
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.activation = activation;
        this.resultSetNumber = resultSetNumber;
        this.opType = opType;
        this.all = all;

        ExecPreparedStatement eps = activation.getPreparedStatement();
        intermediateOrderByColumns = (int[]) eps.getSavedObject(intermediateOrderByColumnsSavedObject);
        intermediateOrderByDirection = (int[]) eps.getSavedObject(intermediateOrderByDirectionSavedObject);
        intermediateOrderByNullsLow = (boolean[]) eps.getSavedObject(intermediateOrderByNullsLowSavedObject);
        recordConstructorTime();
    }

	/**
     * open the first source.
 	 *	@exception StandardException thrown on failure
     */
	public void	openCore() throws StandardException 
	{
		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "SetOpResultSet already open");

        leftSource.openCore();

        try {
            rightSource.openCore();
            rightInputRow = rightSource.getNextRowCore();
        } catch (StandardException e) {
            // DERBY-4330 Result set tree must be atomically open or
            // closed for reuse to work (after DERBY-827).
            isOpen = true; // to make close work:
            try { close(); } catch (StandardException ee) {}
            throw e;
        }

        if (rightInputRow != null)
        {
            rowsSeenRight++;
        }

        isOpen = true;
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	} // end of openCore

	/**
     * @return the next row of the intersect or except, null if there is none
 	 *	@exception StandardException thrown on failure
	 */
	public ExecRow	getNextRowCore() throws StandardException
    {
		if( isXplainOnlyMode() )
			return null;

		beginTime = getCurrentTimeMillis();
	    if ( isOpen )
        {
            while( (leftInputRow = leftSource.getNextRowCore()) != null)
            {
                rowsSeenLeft++;

                DataValueDescriptor[] leftColumns = leftInputRow.getRowArray();
                if( !all)
                {
                    if( isDuplicate( leftColumns))
                        continue; // Get the next left row
                    prevCols = leftInputRow.getRowArrayClone();
                }
                int compare = 0;
                // Advance the right until there are no more right rows or leftRow <= rightRow
                while ( rightInputRow != null && (compare = compare(leftColumns, rightInputRow.getRowArray())) > 0)
                {
                    rightInputRow = rightSource.getNextRowCore();
                    if (rightInputRow != null)
                    {
                        rowsSeenRight++;
                    }
                }

                if( rightInputRow == null || compare < 0)
                {
                    // The left row is not in the right source.
                    if( opType == IntersectOrExceptNode.EXCEPT_OP)
                        // Output this row
                        break;
                }
                else
                {
                    // The left and right rows are the same
                    if( SanityManager.DEBUG)
                        SanityManager.ASSERT( rightInputRow != null && compare == 0,
                                              "Intersect/Except execution has gotten confused.");
                    if ( all)
                    {
                        // Just advance the right input by one row.
                        rightInputRow = rightSource.getNextRowCore();
                        if (rightInputRow != null)
                        {
                            rowsSeenRight++;
                        }
                    }

                    // If !all then we will skip past duplicates on the left at the top of this loop,
                    // which will then force us to skip past any right duplicates.
                    if( opType == IntersectOrExceptNode.INTERSECT_OP)
                        break; // output this row

                    // opType == IntersectOrExceptNode.EXCEPT_OP
                    // This row should not be ouput
                }
            }
        }

        setCurrentRow(leftInputRow);

        if (currentRow != null) {
           rowsReturned++;
        }

        nextTime += getElapsedMillis(beginTime);
        return currentRow;
    } // end of getNextRowCore

    private void advanceRightPastDuplicates( DataValueDescriptor[] leftColumns)
        throws StandardException
    {
        while ((rightInputRow = rightSource.getNextRowCore()) != null)
        {
            rowsSeenRight++;

            if (compare(leftColumns, rightInputRow.getRowArray()) == 0) 
                continue;
        }
    } // end of advanceRightPastDuplicates
        
    private int compare( DataValueDescriptor[] leftCols, DataValueDescriptor[] rightCols)
        throws StandardException
    {
        for( int i = 0; i < intermediateOrderByColumns.length; i++)
        {
            int colIdx = intermediateOrderByColumns[i];
            if( leftCols[colIdx].compare( Orderable.ORDER_OP_LESSTHAN,
                                          rightCols[colIdx],
                                          true, // nulls should be ordered
                                          intermediateOrderByNullsLow[i],
                                          false))
                return -1 * intermediateOrderByDirection[i];
            if( ! leftCols[colIdx].compare( Orderable.ORDER_OP_EQUALS,
                                            rightCols[colIdx],
                                            true, // nulls should be ordered
                                            intermediateOrderByNullsLow[i],
                                            false))
                return intermediateOrderByDirection[i];
        }
        return 0;
    } // end of compare
    
    private boolean isDuplicate( DataValueDescriptor[] curColumns)
        throws StandardException
    {
        if( prevCols == null)
            return false;
        /* Note that intermediateOrderByColumns.length can be less than prevCols.length if we know that a
         * subset of the columns is a unique key. In that case we only need to look at the unique key.
         */
        for( int i = 0; i < intermediateOrderByColumns.length; i++)
        {
            int colIdx = intermediateOrderByColumns[i];
            if( ! curColumns[colIdx].compare( Orderable.ORDER_OP_EQUALS, prevCols[colIdx], true, false))
                return false;
        }
        return true;
    }

	public ExecRow getCurrentRow()
    {
        return currentRow;
    }
    
	/**
	 * If the result set has been opened,
	 * close the currently open source.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
		if ( isOpen )
        {
	    	clearCurrentRow();
            prevCols = null;
            leftSource.close();
            rightSource.close();
            super.close();
        }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of SetOpResultSet repeated");

		closeTime += getElapsedMillis(beginTime);
	} // end of close

	public void	finish() throws StandardException
	{
		leftSource.finish();
		rightSource.finish();
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
			return	totTime - leftSource.getTimeSpent(ENTIRE_RESULTSET_TREE)
              - rightSource.getTimeSpent(ENTIRE_RESULTSET_TREE);
		}
		else
		{
			return totTime;
		}
	} // end of getTimeSpent

	/**
     * @see CursorResultSet
	 *
     * @return the row location of the current cursor row.
     * @exception StandardException thrown on failure
	 */
	public RowLocation getRowLocation() throws StandardException
    {
        // RESOLVE: What is the row location of an INTERSECT supposed to be: the location from the
        // left side, the right side, or null?
        return ((CursorResultSet)leftSource).getRowLocation();
    }

    /**
     * Return the set operation of this <code>SetOpResultSet</code>
     *
     * @return the set operation of this ResultSet, the value is either 
     *         <code>IntersectOrExceptNode.INTERSECT_OP</code> for 
     *         Intersect operation or <code>IntersectOrExceptNode.EXCEPT_OP
     *         </code> for Except operation
     *         
     * @see    org.apache.derby.impl.sql.compile.IntersectOrExceptNode
     */
    public int getOpType()
    {
        return opType;
    }

    /**
     * Return the result set number
     *
     * @return the result set number
     */
    public int getResultSetNumber()
    {
        return resultSetNumber;
    }

    /**
     * Return the left source input of this <code>SetOpResultSet</code>
     *
     * @return the left source input of this <code>SetOpResultSet</code>
     * @see org.apache.derby.iapi.sql.execute.NoPutResultSet
     */
    public NoPutResultSet getLeftSourceInput()
    {
        return leftSource;
    }

    /**
     * Return the right source input of this <code>SetOpResultSet</code>
     *
     * @return the right source input of this <code>SetOpResultSet</code>
     * @see org.apache.derby.iapi.sql.execute.NoPutResultSet
     */
    public NoPutResultSet getRightSourceInput()
    {
        return rightSource;
    }

    /**
     * Return the number of rows seen on the left source input
     *
     * @return the number of rows seen on the left source input
     */
    public int getRowsSeenLeft()
    {
        return rowsSeenLeft;
    }

    /**
     * Return the number of rows seen on the right source input
     *
     * @return the number of rows seen on the right source input
     */
    public int getRowsSeenRight()
    {
        return rowsSeenRight;
    }

    /**
     * Return the number of rows returned from the result set
     *
     * @return the number of rows returned from the result set
     */
    public int getRowsReturned()
    {
        return rowsReturned;
    }
}
