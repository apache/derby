/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access.conglomerate
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access.conglomerate;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.TransactionController;

/**

  The sort interface corresponds to an instance of an in-progress sort.
  Sorts are not persistent.

**/

public interface Sort
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	Open a sort controller.
	<p>
	The sort may have been dropped already, in which case
	this method should thrown an exception.

    @exception StandardException Standard exception policy.
	**/
	SortController open(TransactionManager tran)
		throws StandardException;

	/**
	Open a scan controller.
	<p>
	The sort may have been dropped already, in which case
	this method should thrown an exception.

    @exception StandardException Standard exception policy.
	**/

	ScanController openSortScan(
    TransactionManager  tran,
    boolean             hold)
			throws StandardException;

	/**
	Open a row Source to get rows out of the sorter.
	<p>
	The sort may have been dropped already, in which case
	this method should thrown an exception.

    @exception StandardException Standard exception policy.
	**/

	ScanControllerRowSource openSortRowSource(TransactionManager tran)
			throws StandardException;


	/**
	Drop the sort - this means release all its resources.
	<p>
	Note: drop is like close, it has to be tolerant of
	being called more than once, it must succeed or at
	least not throw any exceptions.
	**/
	void drop(TransactionController tran)
        throws StandardException;
}
