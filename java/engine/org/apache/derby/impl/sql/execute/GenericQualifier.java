/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 *	This is the implementation for Qualifier.  It is used for generated scans.
 *
 *	@author Jeff Lichtman
 */

public class GenericQualifier implements Qualifier
{
	private int columnId;
	private int operator;
	private GeneratedMethod orderableGetter;
	private Activation	activation;
	private boolean orderedNulls;
	private boolean unknownRV;
	private boolean negateCompareResult;
	protected int variantType;

	private DataValueDescriptor orderableCache = null;

	public GenericQualifier(int columnId,
							int operator,
							GeneratedMethod orderableGetter,
							Activation activation,
							boolean orderedNulls,
							boolean unknownRV,
							boolean negateCompareResult,
							int variantType)
	{
		this.columnId = columnId;
		this.operator = operator;
		this.orderableGetter = orderableGetter;
		this.activation = activation;
		this.orderedNulls = orderedNulls;
		this.unknownRV = unknownRV;
		this.negateCompareResult = negateCompareResult;
		this.variantType = variantType;
	}

	/* 
	 * Qualifier interface
	 */

	/** 
	 * @see Qualifier#getColumnId
	 */
	public int getColumnId()
	{
		return columnId;
	}

	/** 
	 * @see Qualifier#getOrderable
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getOrderable() throws StandardException
	{
		if (variantType != VARIANT)
		{
			if (orderableCache == null)
			{
				orderableCache = (DataValueDescriptor) (orderableGetter.invoke(activation));
			}
			return orderableCache;
		}
		return (DataValueDescriptor) (orderableGetter.invoke(activation));
	}

	/** Get the operator to use in the comparison. 
     *
     *  @see Qualifier#getOperator
     **/
	public int getOperator()
	{
		return operator;
	}

	/** Should the result from the compare operation be negated?  If true
     *  then only rows which fail the compare operation will qualify.
     *
     *  @see Qualifier#negateCompareResult
     **/
	public boolean negateCompareResult()
	{
		return negateCompareResult;
	}

	/** Get the getOrderedNulls argument to use in the comparison. 
     *  
     *  @see Qualifier#getOrderedNulls
     **/
    public boolean getOrderedNulls()
	{
		return orderedNulls;
	}

	/** Get the getOrderedNulls argument to use in the comparison.
     *  
     *  @see Qualifier#getUnknownRV
     **/
    public boolean getUnknownRV()
	{
		return unknownRV;
	}

	/** Clear the DataValueDescriptor cache, if one exists.
	 *  (The DataValueDescriptor can be 1 of 3 types:
	 *		o  VARIANT		  - cannot be cached as its value can 
	 *							vary within a scan
	 *		o  SCAN_INVARIANT - can be cached within a scan as its
	 *							value will not change within a scan
	 *		o  QUERY_INVARIANT- can be cached across the life of the query
	 *							as its value will never change
	 *		o  CONSTANT		  - never changes
     *  
     *  @see Qualifier#getUnknownRV
	 */
	public void clearOrderableCache()
	{
		if ((variantType == SCAN_INVARIANT) || (variantType == VARIANT))
		{
			orderableCache = null;
		}
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
		if (variantType != CONSTANT)
		{
			orderableCache = null;
		}
	}

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "columnId: "+columnId+
				"\noperator: "+operator+
				"\norderedNulls: "+orderedNulls+
				"\nunknownRV: "+unknownRV+
				"\nnegateCompareResult: "+negateCompareResult;
		}
		else
		{
			return "";
		}
	}
}
