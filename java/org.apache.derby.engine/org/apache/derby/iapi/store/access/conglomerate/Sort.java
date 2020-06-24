/*

   Derby - Class org.apache.derby.iapi.store.access.conglomerate.Sort

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

package org.apache.derby.iapi.store.access.conglomerate;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;

/**

  The sort interface corresponds to an instance of an in-progress sort.
  Sorts are not persistent.

**/

public interface Sort
{
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

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
	ScanManager openSortScan(
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
