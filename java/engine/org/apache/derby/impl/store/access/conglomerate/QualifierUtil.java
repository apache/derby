/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.conglomerate
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.types.DataValueDescriptor;

public class QualifierUtil implements Qualifier 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
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
