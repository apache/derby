/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericAggregateResultSet

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

import java.util.Vector;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRowBuilder;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

/**
 * Generic aggregation utilities.
 *
 */
abstract class GenericAggregateResultSet extends NoPutResultSetImpl
{
	protected GenericAggregator[]		aggregates;	
	protected AggregatorInfoList	aggInfoList;	
	public NoPutResultSet source;
	protected	NoPutResultSet	originalSource; // used for run time stats only
    private final ExecIndexRow rowTemplate;

	/**
	 * Constructor
	 *
	 * @param a activation
	 * @param ra reference to a saved row allocator instance
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
		int ra,
		int 			resultSetNumber,
		double 			optimizerEstimatedRowCount,
		double 			optimizerEstimatedCost
	) 
		throws StandardException 
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		source = s;
		originalSource = s;

        ExecPreparedStatement ps = a.getPreparedStatement();
        ExecutionFactory ef = a.getExecutionFactory();

        rowTemplate = ef.getIndexableRow(
                ((ExecRowBuilder) ps.getSavedObject(ra)).build(ef));

		aggInfoList = (AggregatorInfoList) ps.getSavedObject(aggregateItem);
		aggregates = getSortAggregators(aggInfoList, false, 
				a.getLanguageConnectionContext(), s);
	}

    /**
     * Get a template row of the right shape for sorting or returning results.
     * The template is cached, so it may need to be cloned if callers use it
     * for multiple purposes at the same time.
     *
     * @return a row template of the right shape for this result set
     */
    ExecIndexRow getRowTemplate() {
        return rowTemplate;
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
    @SuppressWarnings("UseOfObsoleteCollectionType")
	protected final GenericAggregator[] getSortAggregators
	(
		AggregatorInfoList 			list,
		boolean 					eliminateDistincts,
		LanguageConnectionContext	lcc,
		NoPutResultSet				inputResultSet	
	) throws StandardException
	{
		GenericAggregator 	aggregators[]; 
        Vector<GenericAggregator>
                tmpAggregators = new Vector<GenericAggregator>();
		ClassFactory		cf = lcc.getLanguageConnectionFactory().getClassFactory();

        for (AggregatorInfo aggInfo : list)
		{
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
	 * @param 	row	the row to finish aggregation
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
			row = getRowTemplate();
		}

		setCurrentRow(row);

		boolean eliminatedNulls = false;
		for (int i = 0; i < size; i++)
		{
			GenericAggregator currAggregate = aggregates[i];
			if (currAggregate.finish(row))
				eliminatedNulls = true;
		}

		if (eliminatedNulls)
			addWarning(SQLWarningFactory.newSQLWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION));
	
		return row;
	}

    @Override
	public void finish() throws StandardException {
		source.finish();
		super.finish();
	}

    public Element toXML( Element parentNode, String tag ) throws Exception
    {
        // don't report the redundant originalSource node
        
        Element result = super.toXML( parentNode, tag );
        NodeList    children = result.getChildNodes();
        for ( int i = 0; i < children.getLength(); i++ )
        {
            Node child = children.item( 0 );
            if ( "originalSource".equals( child.getNodeName() ) ) { result.removeChild( child ); }
        }

        return result;
    }
}
