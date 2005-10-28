/*

   Derby - Class org.apache.derby.impl.sql.execute.AggregateSortObserver

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

import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.types.UserDataValue;

import org.apache.derby.iapi.types.CloneableObject;

import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.types.DataValueDescriptor;


import java.util.Vector;

/**
 * This sort observer performs aggregation.
 *
 * @author jamie
 */
public class AggregateSortObserver extends BasicSortObserver
{

	protected GenericAggregator[]	aggsToProcess;
	protected GenericAggregator[]	aggsToInitialize;

	private int firstAggregatorColumn;

	/**
	 * Simple constructor
	 *
	 * @param doClone If true, then rows that are retained
	 *		by the sorter will be cloned.  This is needed
	 *		if language is reusing row wrappers.
	 *
	 * @param aggsToProcess the array of aggregates that 
	 *		need to be accumulated/merged in the sorter.
	 *
	 * @param aggsToInitialize the array of aggregates that
	 *		need to be iniitialized as they are inserted
	 *		into the sorter.  This may be different than
	 *		aggsToProcess in the case where some distinct
	 *		aggregates are dropped in the initial pass of
	 *		a two phase aggregation for scalar or vector
	 *		distinct aggregation.  The initialization process
	 *		consists of replacing an empty UserValue with a new, 
	 *		initialized aggregate of the appropriate type.
	 *		Note that for each row, only the first aggregate
	 *		in this list is checked to see whether initialization
	 *		is needed.  If so, ALL aggregates are initialized;
	 *		otherwise, NO aggregates are initialized.
	 *
	 * @param execRow	ExecRow to use as source of clone for store.
	 */
	public AggregateSortObserver(boolean doClone, GenericAggregator[] aggsToProcess, 
								 GenericAggregator[] aggsToInitialize,
								 ExecRow execRow)
	{
		super(doClone, false, execRow, true);
		this.aggsToProcess = aggsToProcess;
		this.aggsToInitialize = aggsToInitialize;

		/*
		** We expect aggsToInitialize and aggsToProcess to
		** be non null.  However, if it is deemed ok for them
		** to be null, it shouldn't be too hard to add the
		** extra null checks herein.
		*/
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(aggsToInitialize != null, "aggsToInitialize argument to AggregateSortObserver is null");
			SanityManager.ASSERT(aggsToProcess != null, "aggsToProcess argument to AggregateSortObserver is null");
		}

		if (aggsToInitialize.length > 0)
		{
			firstAggregatorColumn = aggsToInitialize[0].aggregatorColumnId;
		} 
	}

	/**
	 * Called prior to inserting a distinct sort
	 * key.  
	 *
	 * @param insertRow the current row that the sorter
	 * 		is on the verge of retaining
	 *
	 * @return the row to be inserted by the sorter.  If null,
	 *		then nothing is inserted by the sorter.  Distinct
	 *		sorts will want to return null.
	 *
	 * @exception StandardException never thrown
	 */
	public DataValueDescriptor[] insertNonDuplicateKey(DataValueDescriptor[] insertRow)
		throws StandardException
	{
		DataValueDescriptor[] returnRow = 
            super.insertNonDuplicateKey(insertRow);

		/*
		** If we have an aggregator column that hasn't been
		** initialized, then initialize the entire row now.	
		*/
		if (aggsToInitialize.length > 0 &&
			((Storable)returnRow[firstAggregatorColumn]).isNull())
		{
			UserDataValue 		wrapper;	
			for (int i = 0; i < aggsToInitialize.length; i++)
			{
				GenericAggregator aggregator = aggsToInitialize[i];
				wrapper = ((UserDataValue)returnRow[aggregator.aggregatorColumnId]);
				if (SanityManager.DEBUG)
				{
					if (!wrapper.isNull())
					{
						SanityManager.THROWASSERT("during aggregate "+
						"initialization, all wrappers expected to be empty; "+
						"however, the wrapper for the following aggregate " +
						"was not empty:" +aggregator+".  The value stored is "+
						wrapper.getObject());
					}
				}
				wrapper.setValue(aggregator.getAggregatorInstance());
				aggregator.accumulate(returnRow, returnRow);
			}
		}

		return returnRow;
	
	}	
	/**
	 * Called prior to inserting a duplicate sort
	 * key.  We do aggregation here.
	 *
	 * @param insertRow the current row that the sorter
	 * 		is on the verge of retaining.  It is a duplicate
	 * 		of existingRow.
	 *
	 * @param existingRow the row that is already in the
	 * 		the sorter which is a duplicate of insertRow
	 *
	 * @exception StandardException never thrown
	 */
	public DataValueDescriptor[] insertDuplicateKey(DataValueDescriptor[] insertRow, DataValueDescriptor[] existingRow) 
			throws StandardException
	{
		if (aggsToProcess.length == 0)
		{
			return null;
		}

		/*
		** If the other row already has an aggregator, then
		** we need to merge with it.  Otherwise, accumulate
		** it.
		*/
		for (int i = 0; i < aggsToProcess.length; i++)
		{
			GenericAggregator aggregator = aggsToProcess[i];
			if (((Storable)insertRow[aggregator.getColumnId()]).isNull())
			{
				aggregator.accumulate(insertRow, existingRow);
			}
			else
			{
				aggregator.merge(insertRow, existingRow);
			}
		}
		return null;
	}
}
