/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ScanQualifier;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.types.DataValueDescriptor;


/**
 *	This is the implementation for ScanQualifier.  It is used for system and user
 *  scans.
 *
 *	@version 0.1
 *	@author Jerry Brenner
 */

public class GenericScanQualifier implements ScanQualifier
{

	private int                 columnId        = -1;
	private DataValueDescriptor orderable       = null;
	private int                 operator        = -1;
	private boolean             negateCR        = false;
	private boolean             orderedNulls    = false;
	private boolean             unknownRV       = false;

	private boolean             properInit      = false;

	public GenericScanQualifier() 
	{
	}

	/* 
	 * Qualifier interface
	 */

	/** 
	 * @see Qualifier#getColumnId
	 */
	public int getColumnId()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return columnId;
	}

	/** 
	 * @see Qualifier#getOrderable
	 */
	public DataValueDescriptor getOrderable()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return orderable;
	}

	/** Get the operator to use in the comparison. 
     *
     *  @see Qualifier#getOperator
     **/
	public int getOperator()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return operator;
	}

	/** Should the result from the compare operation be negated?  If true
     *  then only rows which fail the compare operation will qualify.
     *
     *  @see Qualifier#negateCompareResult
     **/
	public boolean negateCompareResult()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return negateCR;
	}

	/** Get the getOrderedNulls argument to use in the comparison. 
     *  
     *  @see Qualifier#getOrderedNulls
     **/
    public boolean getOrderedNulls()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return orderedNulls;
	}

	/** Get the getOrderedNulls argument to use in the comparison.
     *  
     *  @see Qualifier#getUnknownRV
     **/
    public boolean getUnknownRV()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
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
	 *		o  CONSTANT		  - immutable
     *  
     *  @see Qualifier#getUnknownRV
	 */
	public void clearOrderableCache()
	{
		// No Orderable caching in ScanQualifiers
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

	/*
	 * ScanQualifier interface
	 */

	/**
	 * @see ScanQualifier#setQualifier
	 */
	public void setQualifier(
    int                 columnId, 
    DataValueDescriptor orderable, 
    int                 operator,
    boolean             negateCR, 
    boolean             orderedNulls, 
    boolean             unknownRV)
	{
		this.columnId = columnId;
		this.orderable = orderable;
		this.operator = operator;
		this.negateCR = negateCR;
		this.orderedNulls = orderedNulls;
		this.unknownRV = unknownRV;
		properInit = true;
	}
}




