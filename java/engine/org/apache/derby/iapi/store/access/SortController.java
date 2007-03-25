/*

   Derby - Class org.apache.derby.iapi.store.access.SortController

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;


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
	Inform SortController that all the rows have
    been inserted into it. 
	**/
	void completedInserts();

	/**
    Insert a row into the sort.

    @param row The row to insert into the SortController.  The stored
	representations of the row's columns are copied into a new row
	somewhere in the sort.

	@exception StandardException Standard exception policy.
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
