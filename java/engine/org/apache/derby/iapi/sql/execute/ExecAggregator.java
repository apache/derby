/*

   Derby - Class org.apache.derby.iapi.sql.execute.ExecAggregator

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

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.Formatable;

/**
 * An ExecAggregator is the interface that execution uses
 * to an aggregate.  System defined aggregates will implement
 * this directly. 
 
   <P>
   The life time of an ExecAggregator is as follows.

	<OL>
	<LI> An ExecAggregator instance is created using the defined class name.
	<LI> Its setup() method is called to define its role (COUNT(*), SUM, etc.).
	<LI> Its newAggregator() method may be called any number of times to create
	new working aggregators as required. These aggregators have the same role
	and must be created in an initialized state.
	<LI> accumlate and merge will be called across these set of aggregators
	<LI> One of these aggregators will be used as the final one for obtaining the result
	</OL>


 * <P>
 */
public interface ExecAggregator extends Formatable
{
	/**
	    Set's up the aggregate for processing.
	 */
	public void setup(String aggregateName);

	/**
	 * Iteratively accumulates the addend into the aggregator.
	 * Called on each member of the set of values that is being
	 * aggregated.
	 *
	 * @param addend	the DataValueDescriptor addend (current input to 
	 * 					the aggregation)
	 * @param ga		a result set getter
	 *
	 * @exception StandardException on error
	 */
	public void accumulate
	(
		DataValueDescriptor addend, 
		Object				ga 	
	) throws StandardException;

	/**
	 * Merges one aggregator into a another aggregator.
	 * Merges two partial aggregates results into a single result.
	 * Needed for: <UL>
	 *	<LI> parallel aggregation </LI>
	 *	<LI> vector aggregation (GROUP BY) </LI>
	 *  <LI> distinct aggregates (e.g. MAX(DISTINCT Col)) </LI></UL><p>
	 *
	 * An example of a merge would be: given two COUNT() 
	 * aggregators, C1 and C2, a merge of C1 into C2 would
	 * set C1.count += C2.count.  So, given a <i>CountAggregator</i>
	 * with a <i>getCount()</i> method that returns its counts, its 
	 * merge method might look like this: <pre>

		public void merge(ExecAggregator inputAggregator) throws StandardException
		{
		&nbsp;&nbsp;&nbsp;count += ((CountAccgregator)inputAggregator).getCount();
		} </pre>
	 * 
	 *
	 * @param inputAggregator	the other Aggregator 
	 *							(input partial aggregate)
	 *
	 * @exception StandardException on error
	 */
	public void merge(ExecAggregator inputAggregator) throws StandardException;

	/**
	 * Produces the result to be returned by the query.
	 * The last processing of the aggregate.
	 *
	 */
	public Object getResult();

	/**
 	   Return a new initialized copy of this aggregator, any state
	   set by the setup() method of the original Aggregator must be
	   copied into the new aggregator.
	 *
	 * @return ExecAggregator the new aggregator
	 */
	public ExecAggregator newAggregator();
	
	/**
		Return true if the aggregation eliminated at least one
		null from the input data set.
	*/
	public boolean didEliminateNulls();
}
