/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.error.StandardException;

/**
 * A SortObserver is an object that is used as a callback by the
 * sorter.  It allows the sort client to do whatever they want
 * from the context of a sort.  It contains 2 callback methods:
 * <I>insertDuplicateKey()</I> and <I>insertNonDuplicateKey()</I>.  
 * On each <I>SortController.insert()</I>, one or the other of these 
 * methods will be called, depending on whether the given row has a
 * key that has been seen before or not.
 * <p>
 * Some sample uses include:
 * <UL><LI>
 *
 * <I>Sorts from Language</I>: Language typically recycles
 * data type wrappers.  So the language layer uses SortObservers
 * to clone rows that are kept by the sorter.  
 * </LI>
 *
 * <LI>
 * <I>Distinct sorts</I>: The sorter will call the sort observer
 * each time it identifies a duplicate row.  Based on what the
 * sort observer returns to the sorter, the sorter will either
 * retain (insert) the duplicate row, or discard the duplicate
 * row.  All you have to do to implement a distinct sort is to
 * tell the sorter to discard the row (return null from <I>
 * insertDuplicateKey()</I>).  Also, if you want to throw an 
 * exception on a duplicate (e.g. create a unique index), you 
 * can just throw an exception from your SortObserver.
 * </LI>
 *
 * <LI>
 * <I>Aggregates</I>: Vector (grouped) aggregates typically require
 * a sort.  Language can use a SortObserver to perform aggregations
 * as duplicate elements are encountered.  Scalar aggregates
 * can also be computed using a SortObserver.
 * </LI>
 * </UL>
 *
 * These are possible uses only.  You, kind reader, may do whatever 
 * you wish with this forgiving interface.
 *
 * @see SortController
 *
 **/
public interface SortObserver
{
	/**
	 * Called prior to inserting a distinct sort
	 * key; in other words, the first time that a
	 * key is inserted into the sorter, this method
	 * is called.  Subsequent inserts with the same
	 * key generate a call to insertDuplicateKey()
	 * instead.
	 * <p>
	 * This method will most commonly be used to clone
	 * the row that is retained by the sorter, or possibly
	 * to do some initialization of that row.
	 *
	 * @param insertRow the current row that the sorter
	 * 		is on the verge of retaining
	 *
	 * @return the row to be inserted by the sorter.  If null,
	 *		then nothing is inserted by the sorter.
	 *
	 * @exception StandardException either on unexpected exception,
	 * 		or on expected user error that is to percolate back
	 *		to the driver of the sort.
	 */
	DataValueDescriptor[] insertNonDuplicateKey(
    DataValueDescriptor[] insertRow) 
		throws StandardException;
	
	/**
	 * Called prior to inserting a duplicate sort
	 * key.   This method will typically be used
	 * to perform some aggregation on a row that is
	 * going to be discarded by the sorter.
	 *
	 * @param insertRow the current row that the sorter
	 * 		is on the verge of retaining.  It is a duplicate
	 * 		of existingRow.
	 *
	 * @param existingRow the row that is already in the
	 * 		the sorter which is a duplicate of insertRow
	 *
	 * @return the row to be inserted by the sorter.  If null,
	 *		then nothing is inserted by the sorter.  Distinct
	 *		sorts will want to return null.
	 *
	 * @exception StandardException either on unexpected exception,
	 * 		or on expected user error that is to percolate back
	 *		to the driver of the sort.
	 */
	DataValueDescriptor[] insertDuplicateKey(
    DataValueDescriptor[] insertRow, 
    DataValueDescriptor[] existingRow) 
			throws StandardException;

	public void addToFreeList(
    DataValueDescriptor[]   objectArray, 
    int                     maxFreeListSize);

	public DataValueDescriptor[] getArrayClone()
		throws StandardException;
}
