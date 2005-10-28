/*

   Derby - Class org.apache.derby.impl.sql.execute.UnionResultSet

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

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.types.RowLocation;


/**
 * Takes two result sets and returns their union (all).
 * (Any duplicate elimination is performed above this ResultSet.)
 *
 * @author ames
 */
public class UnionResultSet extends NoPutResultSetImpl
	implements CursorResultSet {

	/* Run time statistics variables */
	public int rowsSeenLeft;
	public int rowsSeenRight;
	public int rowsReturned;

    private int whichSource = 1; // 1 or 2, == the source we are currently on.
    private int source1FinalRowCount = -1;

	// these are set in the constructor and never altered
    public NoPutResultSet source1;
    public NoPutResultSet source2;
    protected GeneratedMethod closeCleanup;

    //
    // class interface
    //
	/*
     * implementation alternative: an array of sources,
     * using whichSource to index into the current source.
     */
    public UnionResultSet(NoPutResultSet source1, NoPutResultSet source2, 
						  Activation activation, 
						  int resultSetNumber, 
					      double optimizerEstimatedRowCount,
						  double optimizerEstimatedCost,
						  GeneratedMethod closeCleanup) 
	{
		
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.source1 = source1;
        this.source2 = source2;
		this.closeCleanup = closeCleanup;
		constructorTime += getElapsedMillis(beginTime);
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * Returns the description of the first source.
     * Assumes the compiler ensured both sources
     * had the same description.
	 */
	public ResultDescription getResultDescription() {
	    return source1.getResultDescription();
	}

	/**
     * open the first source.
 	 *	@exception StandardException thrown on failure
     */
	public void	openCore() throws StandardException 
	{
		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "UnionResultSet already open");

        isOpen = true;
        source1.openCore();
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
     * If there are rows still on the first source, return the
     * next one; otherwise, switch to the second source and
     * return a row from there.
 	 *	@exception StandardException thrown on failure
	 */
	public ExecRow	getNextRowCore() throws StandardException {
	    ExecRow result = null;

		beginTime = getCurrentTimeMillis();
	    if ( isOpen ) {
	        switch (whichSource) {
	            case 1 : result = source1.getNextRowCore();
	                     if ( result == (ExecRow) null ) {
	                        //source1FinalRowCount = source1.rowCount();
	                        source1.close();
	                        whichSource = 2;
	                        source2.openCore();
	                        result = source2.getNextRowCore();
							if (result != null)
							{
								rowsSeenRight++;
							}
	                     }
						 else
						 {
							 rowsSeenLeft++;
						 }
	                     break;
	            case 2 : result = source2.getNextRowCore();
						 if (result != null)
						 {
							rowsSeenRight++;
						 }
	                     break;
	            default: 
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT( "Bad source number in union" );
	                break;
	        }
	    }

		currentRow = result;
		setCurrentRow(result);
		if (result != null)
		{
			rowsReturned++;
		}

		nextTime += getElapsedMillis(beginTime);
	    return result;
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
		if ( isOpen ) {
			if (closeCleanup != null) {
				closeCleanup.invoke(activation); // let activation tidy up
			}

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();
			currentRow = null;
	        switch (whichSource) {
	            case 1 : source1.close();
	                     break;
	            case 2 : source2.close();
	                     source1FinalRowCount = -1;
	                     whichSource = 1;
	                     break;
	            default: 
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT( "Bad source number in union" );
	                break;
	        }

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of UnionResultSet repeated");

		closeTime += getElapsedMillis(beginTime);
	}

	public void	finish() throws StandardException
	{
		source1.finish();
		source2.finish();
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
			return	totTime - source1.getTimeSpent(ENTIRE_RESULTSET_TREE) -
							  source2.getTimeSpent(ENTIRE_RESULTSET_TREE);
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
		A union has a single underlying row at a time, although
		from one of several sources.
	
		@see CursorResultSet
	 
		@return the row location of the current cursor row.
		@exception StandardException thrown on failure
	 */
	public RowLocation getRowLocation() throws StandardException {
	    switch (whichSource) {
	        case 1 : 
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(source1 instanceof CursorResultSet, "source not CursorResultSet");
				return ((CursorResultSet)source1).getRowLocation();
	        case 2 : 
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(source2 instanceof CursorResultSet, "source2 not CursorResultSet");
				return ((CursorResultSet)source2).getRowLocation();
	        default: 
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT( "Bad source number in union" );
	            return null;
	    }
	}

	/**
		A union has a single underlying row at a time, although
		from one of several sources.
	
		@see CursorResultSet
	 
		@return the current row.
	 * @exception StandardException thrown on failure.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException{
	    ExecRow result = null;

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isOpen, "TSRS expected to be open");
			if (!(whichSource == 1 || whichSource == 2))
			{
				SanityManager.THROWASSERT("whichSource expected to be 1 or 2, not " 
					+ whichSource);
			}
		}

	    switch (whichSource) 
		{
	        case 1: 
				result = ((CursorResultSet) source1).getCurrentRow();
	            break;

	        case 2: 
				result = ((CursorResultSet) source2).getCurrentRow();
	            break;
        }


		currentRow = result;
		setCurrentRow(result);
	    return result;
	}

}
