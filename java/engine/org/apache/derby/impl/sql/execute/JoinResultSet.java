/*

   Derby - Class org.apache.derby.impl.sql.execute.JoinResultSet

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.types.RowLocation;

/**
 * Takes 2 NoPutResultSets and a join filter and returns
 * the join's rows satisfying the filter as a result set.
 *
 * @author ames
 */
public abstract class JoinResultSet extends NoPutResultSetImpl
	implements CursorResultSet
{
	/* Run time statistics variables */
	public int rowsSeenLeft;
	public int rowsSeenRight;
	public int rowsReturned;
	public long restrictionTime;

	protected boolean isRightOpen;
	protected ExecRow leftRow;
	protected ExecRow rightRow;
	protected ExecRow mergedRow;

    // set in constructor and not altered during
    // life of object.
    public	  NoPutResultSet leftResultSet;
	protected int		  leftNumCols;
	public	  NoPutResultSet rightResultSet;
	protected int		  rightNumCols;
    protected GeneratedMethod restriction;
    protected GeneratedMethod closeCleanup;
	public	  boolean oneRowRightSide;
	public	  boolean notExistsRightSide;  //right side is NOT EXISTS

    /*
     * class interface
     *
     */
    public JoinResultSet(NoPutResultSet leftResultSet,
								   int leftNumCols,
								   NoPutResultSet rightResultSet,
								   int rightNumCols,
								   Activation activation,
								   GeneratedMethod restriction,
								   int resultSetNumber,
								   boolean oneRowRightSide,
								   boolean notExistsRightSide,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
    {
		super(activation, resultSetNumber, optimizerEstimatedRowCount, 
			  optimizerEstimatedCost);
        this.leftResultSet = leftResultSet;
		this.leftNumCols = leftNumCols;
        this.rightResultSet = rightResultSet;
		this.rightNumCols = rightNumCols;
        this.restriction = restriction;
		this.oneRowRightSide = oneRowRightSide;
		this.notExistsRightSide = notExistsRightSide;
        this.closeCleanup = closeCleanup;
		constructorTime += getElapsedMillis(beginTime);
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
	 * Clear any private state that changes during scans.
	 * This includes things like the last row seen, etc.
	 * THis does not include immutable things that are
	 * typically set up in the constructor.
	 * <p>
	 * This method is called on open()/close() and reopen()
	 */
	void clearScanState()
	{
		leftRow = null;
		rightRow = null;
		mergedRow = null;
	}

	/**
     * open a scan on the join. 
	 * For a join, this means:
	 *	o  Open the left ResultSet
	 *  o  Do a getNextRow() on the left ResultSet to establish a position
	 *	   and get "parameter values" for the right ResultSet.
	 *	   NOTE: It is possible for the getNextRow() to return null, in which
	 *	   case there is no need to open the RightResultSet.  We must remember
	 *	   this condition.
	 *	o  If the getNextRow() on the left ResultSet succeeded, then open()
	 *	   the right ResultSet.
	 *
	 * scan parameters are evaluated at each open, so there is probably 
	 * some way of altering their values...
	 *
	 * @exception StandardException		Thrown on error
     */
	public void	openCore() throws StandardException
	{
		clearScanState();

		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT( ! isOpen, "JoinResultSet already open");

	    isOpen = true;
		leftResultSet.openCore();
		leftRow = leftResultSet.getNextRowCore();
		if (leftRow != null)
		{
			openRight();
			rowsSeenLeft++;
		}
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
     * reopen a a join.
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	reopenCore() throws StandardException 
	{
		clearScanState();

		// Reopen the left and get the next row
		leftResultSet.reopenCore();
		leftRow = leftResultSet.getNextRowCore();
		if (leftRow != null)
		{
			// Open the right
			openRight();
			rowsSeenLeft++;
		}
		else if (isRightOpen)
		{
			closeRight();
		}

		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}


	/**
	 * If the result set has been opened,
	 * close the open scan.
	 * <n>
	 * <B>WARNING</B> does not track close
	 * time, since it is expected to be called
	 * directly by its subclasses, and we don't
	 * want to skew the times
	 * 
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		clearScanState();

		if ( isOpen )
	    {
			if (closeCleanup != null) {
				closeCleanup.invoke(activation); // let activation tidy up
			}

	        leftResultSet.close();
			if (isRightOpen)
			{
				closeRight();
			}

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of JoinResultSet repeated");

	}

	public void finish() throws StandardException {
		leftResultSet.finish();
		rightResultSet.finish();
		super.finish();
	}

	//
	// CursorResultSet interface
	//
	/**
	 * A join is combining rows from two sources, so it has no
	 * single row location to return; just return a null.
	 *
	 * @see CursorResultSet
	 *
	 * @return the row location of the current cursor row.
	 */
	public RowLocation getRowLocation() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Join used in positioned update/delete");
		return null;
	}

	/**
	 * A join is combining rows from two sources, so it 
	 * should never be used in a positioned update or delete.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null value.
	 */
	public ExecRow getCurrentRow() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Join used in positioned update/delete");
		return null;
	}

	/* Class implementation */

	/**
	 * open the rightResultSet.  If already open,
	 * just reopen.
	 *
	 * @return Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected void openRight() throws StandardException
	{
		if (isRightOpen)
		{
			rightResultSet.reopenCore();
		}	
		else
		{
			rightResultSet.openCore();
			isRightOpen = true;
		}
	}

	/**
	 * close the rightResultSet
	 *
	 * @return Nothing
	 *
	 * @exception StandardException thrown on error
	 */
	protected void closeRight() throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isRightOpen, "isRightOpen is expected to be true");
		rightResultSet.close();
		isRightOpen = false;
	}

}
