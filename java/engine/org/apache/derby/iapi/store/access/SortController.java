/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.types.CloneableObject;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.error.StandardException;


/**

  A sort controller is an interface for inserting rows
  into a sort.
  <p>
  A sort is created with the createSort method of
  TransactionController. The rows are read back with
  a scan controller returned from the openSortScan
  method of TranscationController.


  @see TransactionController#openSort
  @see ScanController

**/

public interface SortController
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	Close this sort controller.
	<p>
	Currently, since only one sort controller is allowed per sort,
	closing the sort controller means the last row has been
	inserted.
	**/
	void close();

	/**
    Insert a row into the sort.

    @param row The row to insert into the conglomerate.  The stored
	representations of the row's columns are copied into a new row
	somewhere in the conglomerate.

	@exception StandardException Standard exception policy.
	@see CloneableObject
    **/
    void insert(DataValueDescriptor[] row)
		throws StandardException;


    /**
     * Return SortInfo object which contains information about the current
     * state of the sort.
     * <p>
     *
     * @see SortInfo
     *
	 * @return The SortInfo object which contains info about current sort.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    SortInfo getSortInfo()
		throws StandardException;


}
