/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.QualifierUtil

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

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.types.DataValueDescriptor;

public class QualifierUtil implements Qualifier 
{
    private int                 column_id;
    private DataValueDescriptor key_val;
    private int                 operator;
    private boolean             negateCompareResult;
    private boolean             orderedNulls;
    private boolean             unknownRV;

    /**
     * Constuctor
     */
    public QualifierUtil(
    int                 column_id,
    DataValueDescriptor key_val,
    int                 operator,
    boolean             negateCompareResult,
    boolean             orderedNulls,
    boolean             unknownRV)
    {
        this.column_id              = column_id;
        this.key_val                = key_val;
        this.operator               = operator;
        this.negateCompareResult    = negateCompareResult;
        this.orderedNulls           = orderedNulls;
        this.unknownRV              = unknownRV;
    }

    /** Qualifier interface: **/

    /** Get the id of the column to be qualified. **/
    public int getColumnId()
    {
        return(this.column_id);
    }

    /** Get the value that the column is to be compared to. **/
    public DataValueDescriptor getOrderable()
    {
        return(this.key_val);
    }

    /** Get the operator to use in the comparison. 
     *
     *  @see DataValueDescriptor#compare
     **/
    public int getOperator()
    {
        return(this.operator);
    }

    /** Should the result of the compare be negated?
     *
     *  @see DataValueDescriptor#compare
     **/
    public boolean negateCompareResult()
    {
        return(this.negateCompareResult);
    }

    /** Get the getOrderedNulls argument to use in the comparison. 
     *  
     *  @see DataValueDescriptor#compare
     **/
    public boolean getOrderedNulls()
    {
        return(this.orderedNulls);
    }

    /** Get the getOrderedNulls argument to use in the comparison.
     *  
     *  @see DataValueDescriptor#compare
     **/
    public boolean getUnknownRV()
    {
        return(this.unknownRV);
    }

	/** Clear the DataValueDescriptor cache, if one exists.
	 *  (The DataValueDescriptor can be 1 of 3 types:
	 *		o  VARIANT		  - cannot be cached as its value can 
	 *							vary within a scan
	 *		o  SCAN_INVARIANT - can be cached within a scan as its
	 *							value will not change within a scan
	 *		o  QUERY_INVARIANT- can be cached across the life of the query
	 *							as its value will never change
	 *		o  CONSTANT		  - can be cached across executions
     *  
     *  @see Qualifier#getUnknownRV
	 */
	public void clearOrderableCache()
	{
		// No Orderable caching here
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
