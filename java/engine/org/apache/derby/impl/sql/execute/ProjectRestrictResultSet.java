/*

   Derby - Class org.apache.derby.impl.sql.execute.ProjectRestrictResultSet

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

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;


/**
 * Takes a table and a table filter and returns
 * the table's rows satisfying the filter as a result set.
 *
 * @author ames
 */
public class ProjectRestrictResultSet extends NoPutResultSetImpl
	implements CursorResultSet 
{
	/* Run time statistics variables */
	public long restrictionTime;
	public long projectionTime;

    // set in constructor and not altered during
    // life of object.
    public NoPutResultSet source;
	public GeneratedMethod constantRestriction;
    public GeneratedMethod restriction;
	public boolean doesProjection;
    private GeneratedMethod projection;
	private int[]			projectMapping;
    private GeneratedMethod closeCleanup;
	private boolean runTimeStatsOn;
	private ExecRow			mappedResultRow;
	public boolean reuseResult;

	private boolean shortCircuitOpen;

	private ExecRow projRow;

    //
    // class interface
    //
    public ProjectRestrictResultSet(NoPutResultSet s,
					Activation a,
					GeneratedMethod r,
					GeneratedMethod p,
					int resultSetNumber,
					GeneratedMethod cr,
					int mapRefItem,
					boolean reuseResult,
					boolean doesProjection,
				    double optimizerEstimatedRowCount,
					double optimizerEstimatedCost,
					GeneratedMethod c) 
		throws StandardException
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        source = s;
		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"PRRS(), source expected to be non-null");
		}
        restriction = r;
        projection = p;
		constantRestriction = cr;
		projectMapping = ((ReferencedColumnsDescriptorImpl) a.getPreparedStatement().getSavedObject(mapRefItem)).getReferencedColumnPositions();
		this.reuseResult = reuseResult;
		this.doesProjection = doesProjection;
        closeCleanup = c;

		// Allocate a result row if all of the columns are mapped from the source
		if (projection == null)
		{
			mappedResultRow = activation.getExecutionFactory().getValueRow(projectMapping.length);
		}

		/* Remember whether or not RunTimeStatistics is on */
		runTimeStatsOn = getLanguageConnectionContext().getRunTimeStatisticsMode();
		constructorTime += getElapsedMillis(beginTime);
    }

	//
	// NoPutResultSet interface 
	//

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException 
	{
		boolean constantEval = true;

		beginTime = getCurrentTimeMillis();

		// source expected to be non-null, mystery stress test bug
		// - sometimes get NullPointerException in openCore().
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(source != null,
				"PRRS().openCore(), source expected to be non-null");
		}

		// REVISIT: through the direct DB API, this needs to be an
		// error, not an ASSERT; users can open twice. Only through JDBC
		// is access to open controlled and ensured valid.
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "ProjectRestrictResultSet already open");

		if (constantRestriction != null)
		{
		    DataValueDescriptor restrictBoolean;
            restrictBoolean = (DataValueDescriptor) 
					constantRestriction.invoke(activation);

	            // if the result is null, we make it false --
				// so the row won't be returned.
            constantEval = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
		}

		if (constantEval)
		{
	        source.openCore();
		}
		else
		{
			shortCircuitOpen = true;
		}
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
		boolean constantEval = true;

		beginTime = getCurrentTimeMillis();

		if (SanityManager.DEBUG)
		    SanityManager.ASSERT(isOpen, "ProjectRestrictResultSet not open, cannot reopen");

		if (constantRestriction != null)
		{
		    DataValueDescriptor restrictBoolean;
            restrictBoolean = (DataValueDescriptor) 
					constantRestriction.invoke(activation);

	            // if the result is null, we make it false --
				// so the row won't be returned.
            constantEval = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
		}

		if (constantEval)
		{
	        source.reopenCore();
		}
		else
		{
			shortCircuitOpen = true;
		}
	    isOpen = true;

		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
     * Return the requested values computed
     * from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * restriction and projection parameters
     * are evaluated for each row.
	 *
	 * @exception StandardException thrown on failure.
	 * @exception StandardException ResultSetNotOpen thrown if not yet open.
	 *
	 * @return the next row in the result
	 */
	public ExecRow	getNextRowCore() throws StandardException {

	    ExecRow candidateRow = null;
	    ExecRow result = null;
	    boolean restrict = false;
	    DataValueDescriptor restrictBoolean;
		long	beginRT = 0;

		/* Return null if open was short circuited by false constant expression */
		if (shortCircuitOpen)
		{
			return result;
		}

		beginTime = getCurrentTimeMillis();
	    do 
		{
			candidateRow = source.getNextRowCore();
			if (candidateRow != null) 
			{
				beginRT = getCurrentTimeMillis();
				/* If restriction is null, then all rows qualify */
				if (restriction == null)
				{
					restrict = true;
				}
				else
				{
					setCurrentRow(candidateRow);
		            restrictBoolean = (DataValueDescriptor) 
											restriction.invoke(activation);
					restrictionTime += getElapsedMillis(beginRT);

		            // if the result is null, we make it false --
					// so the row won't be returned.
				    restrict = ((! restrictBoolean.isNull()) &&
								 restrictBoolean.getBoolean());
					if (! restrict)
					{
						rowsFiltered++;
					}
				}

				/* Update the run time statistics */
				rowsSeen++;
			}
	    } while ( (candidateRow != null) &&
	              (! restrict ) );

	    if (candidateRow != null) 
		{
			beginRT = getCurrentTimeMillis();

			result = doProjection(candidateRow);

			projectionTime += getElapsedMillis(beginRT);
        }
		/* Clear the current row, if null */
		else
		{
			clearCurrentRow();
		}


		currentRow = result;

		if (runTimeStatsOn)
		{
			if (! isTopResultSet)
			{
				/* This is simply for RunTimeStats */
				/* We first need to get the subquery tracking array via the StatementContext */
				StatementContext sc = activation.getLanguageConnectionContext().getStatementContext();
				subqueryTrackingArray = sc.getSubqueryTrackingArray();
			}
			nextTime += getElapsedMillis(beginTime);
		}
    	return result;
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

		if (type == CURRENT_RESULTSET_ONLY)
		{
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		}
		else
		{
			return totTime;
		}
	}

	// ResultSet interface

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		/* Nothing to do if open was short circuited by false constant expression */
		if (shortCircuitOpen)
		{
			isOpen = false;
			shortCircuitOpen = false;
			return;
		}

		beginTime = getCurrentTimeMillis();
	    if ( isOpen ) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();
			if (closeCleanup != null) 
			{
				closeCleanup.invoke(activation); // let activation tidy up
			}
			currentRow = null;
	        source.close();

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of ProjectRestrictResultSet repeated");

		closeTime += getElapsedMillis(beginTime);
	}

	public void	finish() throws StandardException
	{
		source.finish();
		finishAndRTS();
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
	 * @exception StandardException thrown on failure.
	 */
	public RowLocation getRowLocation() throws StandardException {
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(source instanceof CursorResultSet, "source is not CursorResultSet");
		return ( (CursorResultSet)source ).getRowLocation();
	}

	/**
	 * Gets last row returned.
	 *
	 * @see CursorResultSet
	 *
	 * @return the last row returned.
	 * @exception StandardException thrown on failure.
	 */
	/* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
	 * once there is such a method.  (currentRow is redundant)
	 */
	public ExecRow getCurrentRow() throws StandardException {
	    ExecRow candidateRow = null;
	    ExecRow result = null;
	    boolean restrict = false;
	    DataValueDescriptor restrictBoolean;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isOpen, "PRRS is expected to be open");

		/* Nothing to do if we're not currently on a row */
		if (currentRow == null)
		{
			return null;
		}

		/* Call the child result set to get it's current row.
		 * If no row exists, then return null, else requalify it
		 * before returning.
		 */
		candidateRow = ((CursorResultSet) source).getCurrentRow();
		if (candidateRow != null) {
			setCurrentRow(candidateRow);
				/* If restriction is null, then all rows qualify */
            restrictBoolean = (DataValueDescriptor) 
					((restriction == null) ? null : restriction.invoke(activation));

            // if the result is null, we make it false --
			// so the row won't be returned.
            restrict = (restrictBoolean == null) ||
						((! restrictBoolean.isNull()) &&
							restrictBoolean.getBoolean());
		}

	    if (candidateRow != null && restrict) 
		{
			result = doProjection(candidateRow);
        }

		currentRow = result;
		/* Clear the current row, if null */
		if (result == null) {
			clearCurrentRow();
		}

		return currentRow;
	}

	/**
	 * Do the projection against the source row.  Use reflection
	 * where necessary, otherwise get the source column into our
	 * result row.
	 *
	 * @param sourceRow		The source row.
	 *
	 * @return		The result of the projection
	 *
	 * @exception StandardException thrown on failure.
	 */
	private ExecRow doProjection(ExecRow sourceRow)
		throws StandardException
	{
		// No need to use reflection if reusing the result
		if (reuseResult && projRow != null)
		{
			return projRow;
		}

		ExecRow result;

		// Use reflection to do as much of projection as required
		if (projection != null)
		{
	        result = (ExecRow) projection.invoke(activation);
		}
		else
		{
			result = mappedResultRow;
		}

		// Copy any mapped columns from the source
		for (int index = 0; index < projectMapping.length; index++)
		{
			if (projectMapping[index] != -1)
			{
				result.setColumn(index + 1, sourceRow.getColumn(projectMapping[index]));
			}
		}

		/* We need to reSet the current row after doing the projection */
		setCurrentRow(result);

		/* Remember the result if reusing it */
		if (reuseResult)
		{
			projRow = result;
		}
		return result;
	}

	/**
	 * Is this ResultSet or it's source result set for update
	 * 
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return source.isForUpdate();
	}

}




