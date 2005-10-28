/*

   Derby - Class org.apache.derby.impl.sql.execute.DistinctGroupedAggregateResultSet

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.error.StandardException;

import java.util.Properties;
import java.util.Vector;
import java.util.Enumeration;

/**
 * This ResultSet evaluates grouped aggregates when there is 1 or more distinct aggregate.
 * It will scan the entire source result set and calculate
 * the grouped aggregates when scanning the source during the 
 * first call to next().
 *
 * RESOLVE - This subclass is essentially empty.  Someday we will need to write 
 * additional code for distinct grouped aggregates, especially when we support
 * multiple distinct aggregates.
 *
 * @author jerry (broken out from SortResultSet)
 */
public class DistinctGroupedAggregateResultSet extends GroupedAggregateResultSet
{

    /**
	 * Constructor
	 *
	 * @param	s			input result set
	 * @param	isInSortedOrder	true if the source results are in sorted order
	 * @param	aggregateItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		AggregatorInfoList used by this routine.  
	 * @param	orderingItem	indicates the number of the
	 *		SavedObject off of the PreparedStatement that holds the
	 *		ColumOrdering array used by this routine
	 * @param	a				activation
	 * @param	ra				generated method to build an empty
	 *	 	output row 
	 * @param	maxRowSize		approx row size, passed to sorter
	 * @param	resultSetNumber	The resultSetNumber for this result set
	 *
	 * @exception StandardException Thrown on error
	 */
    public DistinctGroupedAggregateResultSet(NoPutResultSet s,
					boolean isInSortedOrder,
					int	aggregateItem,
					int	orderingItem,
					Activation a,
					GeneratedMethod ra,
					int maxRowSize,
					int resultSetNumber,
				    double optimizerEstimatedRowCount,
				    double optimizerEstimatedCost,
					GeneratedMethod c) throws StandardException 
	{
		super(s, isInSortedOrder, aggregateItem, orderingItem,
			  a, ra, maxRowSize, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost, c);
    }


	///////////////////////////////////////////////////////////////////////////////
	//
	// ResultSet interface (leftover from NoPutResultSet)
	//
	///////////////////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////////////////
	//
	// CursorResultSet interface
	//
	///////////////////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////////////////
	//
	// SCAN ABSTRACTION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////////////////
	//
	// AGGREGATION UTILITIES
	//
	///////////////////////////////////////////////////////////////////////////////
}
