/*

   Derby - Class org.apache.derby.impl.store.raw.data.LoggableAllocActions

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.impl.store.raw.data.AllocationActions;
import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.shared.common.error.StandardException;

public class LoggableAllocActions implements AllocationActions {

	/**
		Set the allocation status of pageNumber to doStatus.  To undo this
		operation, set the allocation status of pageNumber to undoStatus
		
		@param t				The transaction
		@param allocPage		the allocation page
		@param pageNumber		the page to allocation or deallocation
		@param doStatus			set the allocation status of the page this value
		@param undoStatus		on undo, set the allocation status of the page
								this value 

		@exception StandardException	Standard Derby error policy
	*/
	public void actionAllocatePage(RawTransaction t, BasePage allocPage, 
								   long pageNumber, int doStatus, int undoStatus)
		 throws StandardException
	{
		Loggable lop = new AllocPageOperation((AllocPage)allocPage, pageNumber, doStatus, undoStatus);

		// mark the page as pre-dirtied so that if a checkpoint happens after
		// the log record is sent to the log stream, the cache cleaning will
		// wait for this change.
		allocPage.preDirty();

		t.logAndDo(lop);
	}

	/**
		Chain one allocation page to the next.

		@param t				The transaction
		@param allocPage		the allocation page whose next page chain needs
								to be changed
		@param pageNumber		the next allocation page's number 
		@param pageOffset		the next allocation page's page offset

		@exception StandardException	Standard Derby error policy
	*/
	public void actionChainAllocPage(RawTransaction t, BasePage allocPage, 
								long pageNumber, long pageOffset)
		 throws StandardException
	{
		Loggable lop = new ChainAllocPageOperation((AllocPage)allocPage, pageNumber, pageOffset);

		// mark the page as pre-dirtied so that if a checkpoint happens after
		// the log record is sent to the log stream, the cache cleaning will
		// wait for this change.
		allocPage.preDirty();

		t.logAndDo(lop);
	}

    /**
     * Compress free pages.
     * <p>
     * Compress the free pages at the end of the range maintained by
     * this allocation page.  All pages being compressed should be FREE.
     * Only pages in the last allocation page can be compressed.
     * <p>
     *
     * @param t				        The transaction
     * @param allocPage		        the allocation page to do compress on.
     * @param new_highest_page      The new highest page on this allocation 
     *                              page.  The number is the offset of the page
     *                              in the array of pages maintained by this 
     *                              allocation page, for instance a value of 0 
     *                              indicates all page except the first one are
     *                              to be truncated.  If all pages are 
     *                              truncated then the offset is set to -1.
     * @param num_pages_truncated   The number of allocated pages in this 
     *                              allocation page prior to the truncate.  
     *                              Note that all pages from NewHighestPage+1 
     *                              through newHighestPage+num_pages_truncated 
     *                              should be FREE.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void actionCompressSpaceOperation(
	RawTransaction  t,
    BasePage        allocPage, 
    int             new_highest_page, 
    int             num_pages_truncated)
        throws StandardException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
        Loggable lop = null;
        
        // DERBY-606. As part of the fix for DERBY-606, negative values can be 
        // written to CompressSpace operation Log Records. In order for this 
        // fix to be backword compatible, we make sure that the implementation 
        // behaves the old way in soft upgrade mode, here. This is achieved by 
        // passing null to feature argument.
        if( t.getLogFactory().checkVersion(
                RawStoreFactory.DERBY_STORE_MAJOR_VERSION_10,
                RawStoreFactory.DERBY_STORE_MINOR_VERSION_3,
                null) )
        {
            lop = 
                new CompressSpacePageOperation(
                    (AllocPage)allocPage, 
                    new_highest_page, 
                    num_pages_truncated);
        } else {
            lop = new CompressSpacePageOperation10_2(
                    (AllocPage)allocPage, 
                    new_highest_page, 
                    num_pages_truncated);
        }
        allocPage.preDirty();

        t.logAndDo(lop);
    }
}
