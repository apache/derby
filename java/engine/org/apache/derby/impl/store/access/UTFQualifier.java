/*

   Derby - Class org.apache.derby.impl.store.access.UTFQualifier

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

package org.apache.derby.impl.store.access;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.store.access.Qualifier;

/**
*/
public class UTFQualifier implements Qualifier
{
    private UTF      value;
	private int		 columnId;

	public UTFQualifier(int columnId, String value) {

		this.columnId = columnId;
		this.value = new UTF(value);
	}

	/*
	** Qualifier interface
	*/

	/** Get the id of the column to be qualified. **/
	public int getColumnId() {
		return columnId;
	}

	/**
	 * Get the value that the column is to be compared to.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getOrderable() {
		return value;
	}

	/** Get the operator to use in the comparison. 
     *
     *  @see DataValueDescriptor#compare
     **/
	public int getOperator() {
		return DataValueDescriptor.ORDER_OP_EQUALS;

	}

	/** Should the result from the compare operation be negated?  If true
     *  then only rows which fail the compare operation will qualify.
     *
     *  @see DataValueDescriptor#compare
     **/
	public boolean negateCompareResult() {
		return false;
	}

	/** 
     *  
     *  @see Qualifier#getOrderedNulls
     **/
    public boolean getOrderedNulls() {
		return false;
	}

	/** Get the getOrderedNulls argument to use in the comparison.
     *  
     *  @see DataValueDescriptor#compare
     **/
    public boolean getUnknownRV() {
		return false;
	}

	/** Clear the DataValueDescriptor cache, if one exists.
	 *  (The DataValueDescriptor can be 1 of 3 types:
	 *		o  VARIANT		  - cannot be cached as its value can 
	 *							vary within a scan
	 *		o  SCAN_INVARIANT - can be cached within a scan as its
	 *							value will not change within a scan
	 *		o  QUERY_INVARIANT- can be cached across the life of the query
	 *							as its value will never change
	 *		o  CONSTANT	      - can be cached across executions
     *  
     *  @see Qualifier#getUnknownRV
	 */
	public void clearOrderableCache()
	{
	}

	/** 
	 * This method reinitializes all the state of
	 * the Qualifier.  It is used to distinguish between
	 * resetting something that is query invariant
	 * and something that is constant over every
	 * execution of a query.  Basically, clearOrderableCache()
	 * will only clear out its cache if it is a VARIANT
	 * or SCAN_INVARIANT value.  However, each time a
	 * query is executed, the QUERY_INVARIANT qualifiers need
	 * to be reset.
	 */
	public void reinitialize()
	{
	}
}
