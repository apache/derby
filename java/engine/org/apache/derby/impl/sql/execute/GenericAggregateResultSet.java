/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericAggregateResultSet

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

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.jdbc.ConnectionContext;

import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.impl.jdbc.EmbedSQLWarning;

import java.util.Properties;
import java.util.Vector;
import java.sql.SQLException;

/**
 * Generic aggregation utilities.
 *
 * @author jamie
 */
abstract class GenericAggregateResultSet extends NoPutResultSetImpl
{
	protected GenericAggregator[]		aggregates;	
	protected GeneratedMethod			rowAllocator;
	protected AggregatorInfoList	aggInfoList;	
	public NoPutResultSet source;
    protected GeneratedMethod closeCleanup;
	protected	NoPutResultSet	originalSource; // used for run time stats only

	/**
	 * Constructor
	 *
	 * @param a activation
	 * @param ra row allocator generated method
	 * @param resultSetNumber result set number
	 * @param optimizerEstimatedRowCount optimizer estimated row count
	 * @param optimizerEstimatedCost optimizer estimated cost
	 *
	 * @exception StandardException Thrown on error
	 */
	GenericAggregateResultSet
	(
		NoPutResultSet s,
		int	aggregateItem,
		Activation 	a,
		GeneratedMethod	ra,
		int 			resultSetNumber,
		double 			optimizerEstimatedRowCount,
		double 			optimizerEstimatedCost,
		GeneratedMethod c
	) 
		throws StandardException 
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		source = s;
		originalSource = s;
		closeCleanup = c;


		rowAllocator = ra;

		aggInfoList = (AggregatorInfoList) (a.getPreparedStatement().getSavedObject(aggregateItem));
		aggregates = getSortAggregators(aggInfoList, false, 
				a.getLanguageConnectionContext(), s);
	}

	/**
	 * For each AggregatorInfo in the list, generate a
	 * GenericAggregator and stick it in an array of
	 * GenericAggregators.
	 *
	 * @param list 	the list of aggregators to set up	
	 * @param eliminateDistincts	should distincts be ignored.  
	 *		Used to toss out distinct aggregates for a prelim
	 *		sort.
	 * @param lcc the lcc
	 * @param inputResultSet the incoming result set
	 *
	 * @return the array of GenericAggregators
	 * 
	 * @exception StandardException on error
	 */	
	protected final GenericAggregator[] getSortAggregators
	(
		AggregatorInfoList 			list,
		boolean 					eliminateDistincts,
		LanguageConnectionContext	lcc,
		NoPutResultSet				inputResultSet	
	) throws StandardException
	{
		GenericAggregator 	aggregators[]; 
		Vector tmpAggregators = new Vector();
		ClassFactory		cf = lcc.getLanguageConnectionFactory().getClassFactory();

		int count = list.size();
		for (int i = 0; i < count; i++)
		{
			AggregatorInfo aggInfo = (AggregatorInfo) list.elementAt(i);
			if (! (eliminateDistincts && aggInfo.isDistinct()))
			// if (eliminateDistincts == aggInfo.isDistinct())
			{
				tmpAggregators.addElement(new GenericAggregator(aggInfo, cf));
			}
		}



		aggregators = new GenericAggregator[tmpAggregators.size()];
		tmpAggregators.copyInto(aggregators);
		// System.out.println("size of sort aggregates " + tmpAggregators.size());

		return aggregators;
	}

	/**
	 * Finish the aggregation for the current row.  
	 * Basically call finish() on each aggregator on
	 * this row.  Called once per grouping on a vector
	 * aggregate or once per table on a scalar aggregate.
	 *
	 * If the input row is null, then rowAllocator is
	 * invoked to create a new row.  That row is then
	 * initialized and used for the output of the aggregation.
	 *
	 * @param 	the row to finish aggregation
	 *
	 * @return	the result row.  If the input row != null, then
	 *	the result row == input row
	 *
	 * @exception StandardException Thrown on error
	 */
	protected final ExecIndexRow finishAggregation(ExecIndexRow row)
		throws StandardException
	{
		int	size = aggregates.length;

		/*
		** If the row in which we are to place the aggregate
		** result is null, then we have an empty input set.
		** So we'll have to create our own row and set it
		** up.  Note: we needn't initialize in this case,
		** finish() will take care of it for us.
		*/ 
		if (row == null)
		{
			row = getExecutionFactory().getIndexableRow((ExecRow) rowAllocator.invoke(activation));
		}

		setCurrentRow(row);
		currentRow = row;

		boolean eliminatedNulls = false;
		for (int i = 0; i < size; i++)
		{
			GenericAggregator currAggregate = aggregates[i];
			if (currAggregate.finish(row))
				eliminatedNulls = true;
		}

		if (eliminatedNulls)
			addWarning(EmbedSQLWarning.newEmbedSQLWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION));
	
		return row;
	}

	public void finish() throws StandardException {
		source.finish();
		super.finish();
	}

}
