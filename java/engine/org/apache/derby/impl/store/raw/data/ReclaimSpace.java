/*

   Derby - Class org.apache.derby.impl.store.raw.data.ReclaimSpace

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.PageTimeStamp;
import org.apache.derby.iapi.store.raw.PageKey;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.ContainerHandle;


/**
	Post commit work to reclaim some space from the raw store.  This is a
	wrapper class for the real serviceable class who wraps this on top of
	itself so different things can be identified.
*/
public final class ReclaimSpace implements Serviceable
{
	private  boolean serviceASAP;

	private  ContainerKey containerId;
	private  PageKey pageId;		// Not used for reclaiming container.
	private  RecordHandle headRowHandle; // Used for reclaiming overflow page
										 // and row reserved space.

	// The following is used for reclaiming column chain only.
	private int  columnId;		// Which column in the row to reclaim.
	private long columnPageId;	// Where the column chain pointer
	private int  columnRecordId; // is pointed at.
	private PageTimeStamp timeStamp; // Time stamp of columnPageId to make sure
									 // the post commit work doesn't get
									 // exercised more then once.

	private  int  attempts;

	private  DataFactory processor;	// processor knows how to reclaim file space

	private  int reclaim; // what is it we should be reclaiming 
	public static final int CONTAINER = 1;	// reclaim the entire container
	public static final int PAGE = 2; 		// reclaim an overflow page
	public static final int ROW_RESERVE = 3; // reclaim reserved space on a row
	public static final int COLUMN_CHAIN = 4; // reclaim a column chain


	private void initContainerInfo(ContainerKey containerId, int reclaim,
							  DataFactory processor, boolean serviceASAP)
	{
		this.containerId = containerId;
		this.reclaim = reclaim;
		this.attempts = 0;

		this.processor = processor;
		this.serviceASAP = serviceASAP;
	}

	// reclaim container
	public ReclaimSpace(int reclaim, ContainerKey containerId, 
						DataFactory processor, boolean serviceASAP)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(reclaim == CONTAINER);
		initContainerInfo(containerId, reclaim, processor, serviceASAP);
	}

	// reclaim page - undo of insert into overflow page
	public ReclaimSpace(int reclaim, PageKey pageId,
						DataFactory processor, boolean serviceASAP)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(reclaim == PAGE);
		initContainerInfo(pageId.getContainerId(), reclaim, processor, serviceASAP);

		this.pageId = pageId;
	}

	// reclaim row reserved space
	public ReclaimSpace(int reclaim, RecordHandle headRowHandle, 
						DataFactory processor, boolean serviceASAP) 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(reclaim == ROW_RESERVE);

		initContainerInfo(headRowHandle.getContainerId(), reclaim, processor, serviceASAP);

		this.headRowHandle = headRowHandle;
	}

	// reclaim column chain
	public ReclaimSpace(int reclaim, RecordHandle headRowHandle,
						int columnId, long ovPageId, int ovRecordId,
						PageTimeStamp timeStamp,
						DataFactory processor, boolean serviceASAP)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(reclaim == COLUMN_CHAIN);
		initContainerInfo(headRowHandle.getContainerId(), reclaim, processor, serviceASAP);

		this.headRowHandle = headRowHandle;
		this.columnId = columnId;
		this.columnPageId = ovPageId;
		this.columnRecordId = ovRecordId;
		this.timeStamp = timeStamp;
	}

	/*
	 * Serviceable methods
	 */

	public boolean serviceASAP()
	{
		return serviceASAP;
	}

	public int performWork(ContextManager context) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(context != null, "context is null");
			SanityManager.ASSERT(processor != null, "processor is null");
		}

		return processor.reclaimSpace(this, context);
	}


	// @return true, if this work needs to be done on a user thread immediately
	public boolean serviceImmediately()
	{

		//It's very important that we reclaim container space immediately
		//as part of post commit cleanup. Because typically could typically 
		//involve large amount of space freed and
		//we don't want conatiner reclaim requests lost if the server crashes
		//for some reasom before Container Reclaim requests could be
		//processed successfully by an asynchronous thread.
		//if(reclaim == CONTAINER)
		//	return true; else return false;
		return true;
	}	


	/*
	 * class specific methods
	 */

	public final ContainerKey getContainerId()
	{
		return containerId;
	}

	public final PageKey getPageId()
	{
		return pageId;
	}

	public final RecordHandle getHeadRowHandle()
	{
		return headRowHandle;
	}

	public final int getColumnId()
	{
		return columnId;
	}

	public final long getColumnPageId()
	{
		return columnPageId;
	}

	public final int getColumnRecordId()
	{
		return columnRecordId;
	}

	public final PageTimeStamp getPageTimeStamp()
	{
		return timeStamp;
	}

	public final int reclaimWhat()
	{
		return reclaim;
	}

	public final int incrAttempts()
	{
		return ++attempts;
	}

	// debug
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			if (reclaim == CONTAINER)
				return "Reclaim CONTAINER (" + containerId + ")";

			if (reclaim == PAGE)
				return "Reclaim PAGE (" + pageId + ") head row at " + headRowHandle;

			if (reclaim == ROW_RESERVE)
				return "Reclaim ROW_RESERVE (" + pageId + ")." + headRowHandle + ")";

			if (reclaim == COLUMN_CHAIN)
				return "Reclaim COLUMN_CHAIN ("+ pageId + ").(" + headRowHandle
								  + "," + columnId + ") at (" + columnPageId +
								  "," + columnRecordId + ")";
		}
		return null;

	}

}
