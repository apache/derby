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

public class DirectAllocActions implements AllocationActions {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	public DirectAllocActions() {
	}

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
		((AllocPage)allocPage).setPageStatus((LogInstant)null, pageNumber, doStatus);
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
		((AllocPage)allocPage).chainNextAllocPage((LogInstant)null, pageNumber,
												  pageOffset);
	}

}
