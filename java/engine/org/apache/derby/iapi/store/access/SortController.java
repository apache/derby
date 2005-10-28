/*

   Derby - Class org.apache.derby.iapi.store.access.SortController

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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
