/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.impl.store.raw.data.AllocationActions;
import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;

public class LoggableAllocActions implements AllocationActions {

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
		@param allocpage		the allocation page whose next page chain needs
								to be changed
		@param pageNumber		the next allocation page's number 
		@param pageOffset		the next allocation page's page offset

		@exception StandardException	Standard Cloudscape error policy
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

}
