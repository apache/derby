/*

   Derby - Class org.apache.derby.impl.store.raw.data.AllocationActions

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.error.StandardException;


/**
	This interface describe the operations that has to do with page
	allocation/deallocation.  This interface is used for a special
	allocation page that records the allocation information and dispense the
	allocation policy.
*/

public interface AllocationActions {

	/**
		Set the allocation status of pageNumber to doStatus.  To undo this
		operation, set the allocation status of pageNumber to undoStatus
		
		@param t				The transaction
		@param allocpage		the allocation page
		@param pageNumber		the page to allocation or deallocation
		@param doStatus			set the allocation status of the page this value
		@param undoStatus		on undo, set the allocation status of the page
								this value 

		@exception StandardException	Standard Cloudscape error policy
	*/
	public void actionAllocatePage(RawTransaction t, BasePage allocPage, 
								   long pageNumber, int doStatus, int undoStatus)
		 throws StandardException;

	/**
		Chain one allocation page to the next.

		@param t				The transaction
		@param allocpage		the allocation page whose next page chain needs
								to be changed
		@param pageNumber		the next allocation page's number 
		@param pageOffset		the next allocation page's page offset

		@exception StandardException	Standard Cloudscape error policy
	*/
	public void actionChainAllocPage(RawTransaction t, BasePage allocPage, 
								long pageNumber, long pageOffset)
		 throws StandardException;


}
