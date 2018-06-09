/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericAggregator

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

import java.lang.reflect.Constructor;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.UserDataValue;
/**
 * Adaptor that sits between execution layer and aggregates.
 *
 */
class GenericAggregator 
{
	private final AggregatorInfo			aggInfo;
	int						aggregatorColumnId;
	private int						inputColumnId;
	private int						resultColumnId;

	private final ClassFactory		cf;

	/*
	** We cache an aggregator to speed up
	** the instantiation of lots of aggregators.
	*/
	private ExecAggregator		cachedAggregator;

	/**
	 * Constructor:
	 *
	 * @param aggInfo 	information about the user aggregate
	 * @param cf		the class factory. 
	 */
	GenericAggregator
	(
		AggregatorInfo	aggInfo, 
		ClassFactory	cf
	)
	{
		this.aggInfo = aggInfo;
		aggregatorColumnId = aggInfo.getAggregatorColNum();
		inputColumnId = aggInfo.getInputColNum();
		resultColumnId = aggInfo.getOutputColNum();
		this.cf = cf;
	}


	/**
	 * Initialize the aggregator
	 *
	 * @param	row 	the row with the aggregator to be initialized
	 *
	 * @exception StandardException  on error
	 */
	void initialize(ExecRow row)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(row != null, "row is null");
		}

		UserDataValue aggregatorColumn = (UserDataValue) row.getColumn(aggregatorColumnId + 1);

		ExecAggregator ua = (ExecAggregator) aggregatorColumn.getObject();
		if (ua == null)
		{
			ua = getAggregatorInstance();
			aggregatorColumn.setValue(ua);
		}
	}

	/**
	 * Accumulate the aggregate results.  This is the
	 * guts of the aggregation.  We will call the user aggregate
	 * on itself to do the aggregation.
	 *
	 * @param	inputRow 	the row with the input colum
	 * @param	accumulateRow 	the row with the aggregator 
	 *
	 * @exception StandardException  on error
	 */
	void accumulate(ExecRow	inputRow, 
							ExecRow	accumulateRow)
		throws StandardException
	{
		DataValueDescriptor	inputColumn = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((inputRow != null) && (accumulateRow != null), "bad accumulate call");
		}

		DataValueDescriptor aggregatorColumn = accumulateRow.getColumn(aggregatorColumnId + 1);

		inputColumn = inputRow.getColumn(inputColumnId + 1);

		accumulate(inputColumn, aggregatorColumn);
	}

	/**
	 * Accumulate the aggregate results.  This is the
	 * guts of the aggregation.  We will call the user aggregate
	 * on itself to do the aggregation.
	 *
	 * @param	inputRow 	the row with the input colum
	 * @param	accumulateRow 	the row with the aggregator 
	 *
	 * @exception StandardException  on error
	 */
	void accumulate(Object[]	inputRow, 
							Object[]	accumulateRow)
		throws StandardException
	{
		DataValueDescriptor	inputColumn = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((inputRow != null) && (accumulateRow != null), "bad accumulate call");
		}

		DataValueDescriptor aggregatorColumn = (DataValueDescriptor) accumulateRow[aggregatorColumnId];
		inputColumn = (DataValueDescriptor) inputRow[inputColumnId];

		accumulate(inputColumn, aggregatorColumn);
	}

	/**
	 * Accumulate the aggregate results.  This is the
	 * guts of the aggregation.  We will call the user aggregate
	 * on itself to do the aggregation.
	 *
	 * @param	inputColumn 
	 * @param	aggregatorColumn
	 *
	 * @exception StandardException  on error
	 */
	void accumulate(DataValueDescriptor	inputColumn, 
							DataValueDescriptor	aggregatorColumn)
		throws StandardException
	{
		ExecAggregator		ua;

		if (SanityManager.DEBUG)
		{
			/*
			** Just to be on the safe side, confirm that we actually
			** have a Aggregator in this column.
			*/
			if (!(aggregatorColumn instanceof UserDataValue))
			{
				SanityManager.THROWASSERT("accumlator column is not a UserDataValue as "+
					"expected, it is a "+aggregatorColumn.getClass().getName());
			}
		}
		ua = (ExecAggregator) aggregatorColumn.getObject();

		/*
		** If we don't have an aggregator, then we have to
		** create one now.  This happens when the input result
		** set is null.
		*/
		if (ua == null)
		{
			ua = getAggregatorInstance();
		}
	
		ua.accumulate(inputColumn, this);
	}

	/**
	 * Merge the aggregate results.  This is the
	 * guts of the aggregation.  We will call the user aggregate
	 * on itself to do the aggregation.
	 *
	 * @param	inputRow 	the row with the input colum
	 * @param	mergeRow 	the row with the aggregator 
	 *
	 * @exception StandardException  on error
	 */
	void merge(ExecRow	inputRow, 
							ExecRow	mergeRow)
		throws StandardException
	{

		DataValueDescriptor mergeColumn = mergeRow.getColumn(aggregatorColumnId + 1);
		DataValueDescriptor inputColumn = inputRow.getColumn(aggregatorColumnId + 1);

		merge(inputColumn, mergeColumn);
	}

	/**
	 * Merge the aggregate results.  This is the
	 * guts of the aggregation.  We will call the user aggregate
	 * on itself to do the aggregation.
	 *
	 * @param	inputRow 	the row with the input colum
	 * @param	mergeRow 	the row with the aggregator 
	 *
	 * @exception StandardException  on error
	 */
	void merge(Object[]	inputRow, 
							Object[]	mergeRow)
		throws StandardException
	{
		DataValueDescriptor mergeColumn = (DataValueDescriptor) mergeRow[aggregatorColumnId];
		DataValueDescriptor inputColumn = (DataValueDescriptor) inputRow[aggregatorColumnId];

		merge(inputColumn, mergeColumn);
	}

	/**
	 * Get the results of the aggregation and put it
	 * in the result column.
	 *
	 * @param	row	the row with the result and the aggregator
	 *
	 * @exception StandardException on error
	 */
	boolean finish(ExecRow row)
		throws StandardException
	{
		DataValueDescriptor outputColumn = row.getColumn(resultColumnId + 1);
		DataValueDescriptor aggregatorColumn = row.getColumn(aggregatorColumnId + 1);
		/*
		** Just to be on the safe side, confirm that we actually
		** have a Aggregator in aggregatorColumn.
		*/
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(aggregatorColumn != null, "aggregatorColumn is null");
			SanityManager.ASSERT(outputColumn != null, "otuputColumn is null");
			SanityManager.ASSERT(aggregatorColumn instanceof UserDataValue,
				"accumlator column is not a UserDataValue as expected");
		}

		ExecAggregator ua = (ExecAggregator) aggregatorColumn.getObject();

		/*
		** If we don't have an aggregator, then we have to
		** create one now.  This happens when the input result
		** set is null.
		*/
		if (ua == null)
		{
			ua = getAggregatorInstance();
		}	

		/*
		**
		** We are going to copy
		** then entire DataValueDescriptor into the result column.
		** We could call setValue(result.setObject()), but we
		** might loose state (e.g. SQLBit.getObject() returns a
		** byte[] which looses the precision of the bit.  
		**
		*/
		
		DataValueDescriptor result = ua.getResult();
		if (result == null)
			outputColumn.setToNull();
		else
			outputColumn.setValue(result);

		return ua.didEliminateNulls();
	}

	/**
	 * Get a new instance of the aggregator and initialize it.
	 *
	 * @return an exec aggregator
	 *
	 * @exception StandardException on error
	 */
	ExecAggregator getAggregatorInstance()
		throws StandardException
	{
		ExecAggregator aggregatorInstance;
		if (cachedAggregator == null)
		{
			try
			{
				Class<?> aggregatorClass = cf.loadApplicationClass(aggInfo.getAggregatorClassName());
                Constructor<?> constructor = aggregatorClass.getConstructor();
				Object agg = constructor.newInstance();
				aggregatorInstance = (ExecAggregator)agg;
				cachedAggregator = aggregatorInstance;

				aggregatorInstance.setup
                    (
                     cf,
                     aggInfo.getAggregateName(),
                     aggInfo.getResultDescription().getColumnInfo( 0 ).getType()
                     );

			} catch (Exception e)
			{
				throw StandardException.unexpectedUserException(e);
			}
		}
		else
		{
			aggregatorInstance = cachedAggregator.newAggregator();
		}


		return aggregatorInstance;
	}
			
	/////////////////////////////////////////////////////////////
	//
	/////////////////////////////////////////////////////////////

	/**
	 * Return the column id that is being aggregated
	 */
	int getColumnId()
	{
		// Every sort has to have at least one column.
		return aggregatorColumnId;
	}

	DataValueDescriptor getInputColumnValue(ExecRow row)
	    throws StandardException
	{
	    return row.getColumn(inputColumnId + 1);
	}

	/**
	 * Merge two partial aggregations.  This is how the
	 * sorter merges partial aggregates.
	 *
	 * @exception StandardException on error
	 */
	void merge(Storable aggregatorColumnIn,
						Storable aggregatorColumnOut)
		throws StandardException
	{
		ExecAggregator	uaIn;
		ExecAggregator	uaOut;

		if (SanityManager.DEBUG)
		{
			/*
			** Just to be on the safe side, confirm that we actually
			** have a Aggregator in this column.
			*/
			if (!(aggregatorColumnIn instanceof UserDataValue))
			{
				SanityManager.THROWASSERT("aggregatorColumnOut column is not "+
					"a UserAggreator as expected, "+
					"it is a "+aggregatorColumnIn.getClass().getName());
			}
			if (!(aggregatorColumnOut instanceof UserDataValue))
			{
				SanityManager.THROWASSERT("aggregatorColumnIn column is not"+
					" a UserAggreator as expected, "+
					"it is a "+aggregatorColumnOut.getClass().getName());
			}
		}
		uaIn = (ExecAggregator)(((UserDataValue) aggregatorColumnIn).getObject());
		uaOut = (ExecAggregator)(((UserDataValue) aggregatorColumnOut).getObject());

		uaOut.merge(uaIn);
	}

	//////////////////////////////////////////////////////
	//
	// MISC
	//
	//////////////////////////////////////////////////////
	AggregatorInfo getAggregatorInfo()
	{
		return aggInfo;
	}


}
