/*

   Derby - Class org.apache.derby.impl.store.raw.data.RecordId

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.services.locks.Latch;

import org.apache.derby.iapi.store.raw.RowLock;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.locks.VirtualLockTable;

import org.apache.derby.catalog.UUID;
import java.util.Hashtable;

/**
	Implementation of RecordHandle.

	<BR>
	MT - Mutable - Immutable identity : Thread Aware -
	<BR>The methods of RecordHandle only access the identity
	    of the object and so the object appears immutable to them, as required.
	<BR>The methods of Lockable  are single thread required.

*/
public final class RecordId implements RecordHandle {

	/**
		Page the record lives in.
		MT - Immutable
	*/
	private final PageKey pageId;

	/**
		The actual record id.
		MT - Immutable
	*/
	private final int recordId;

    /**
        Last slot number this record id was at.
    */
    transient private int slotNumberHint;

	public RecordId(ContainerKey container, long pageNumber, int recordId) {
		this.pageId = new PageKey(container, pageNumber);
		this.recordId = recordId;

		// FIRST_SLOT_NUMBER guaranteed to be zero
        // this.slotNumberHint = Page.FIRST_SLOT_NUMBER;
	}

	public RecordId(PageKey pageId, int recordId) {
		this.pageId = pageId;
		this.recordId = recordId;

		// FIRST_SLOT_NUMBER guaranteed to be zero
        // this.slotNumberHint = Page.FIRST_SLOT_NUMBER;
	}

	public RecordId(PageKey pageId, int recordId, int current_slot) {
		this.pageId = pageId;
		this.recordId = recordId;
        this.slotNumberHint = current_slot;
	}

	/*
	**	Methods of RecordHandle
	*/

	/**
		Get my record id.

		<BR>
		MT - thread safe

		@see RecordHandle#getId
	*/
	public int	getId() {
		return recordId;
	}

	/**
		Get my page number.

		<BR>
		MT - thread safe

		@see RecordHandle#getPageNumber
	*/

	public long getPageNumber() {
		return pageId.getPageNumber();
	}

	public Object getPageId() {
		return pageId;
	}

	public ContainerKey getContainerId() {
		return pageId.getContainerId();
	}


    /**
     * What slot number might the record be at?
     * <p>
     * The raw store guarantees that the record handle of a record will not
     * change, but its slot number may.  When a RecordId is constructed the
     * caller could have provided a slot number, if so return that slot number
     * hint here.  If a hint was not provided then the default 
     * Page.FIRST_SLOT_NUMBER will be returned.
     *
	 * @return The slot number the record handle may be at.
     **/
    public int getSlotNumberHint()
    {
        return(slotNumberHint);
    }

	/*
	** Methods of Lockable (from RecordHandle)
	*/

	/**
		Lock me.

		<BR>
		MT - Single thread required (methods of Lockable)

		@see org.apache.derby.iapi.services.locks.Lockable#lockEvent
	*/
	public void lockEvent(Latch lockInfo) {
	}


	/**
		Can this request be granted?

		<BR>
		MT - Single thread required (methods of Lockable)

		@see org.apache.derby.iapi.services.locks.Lockable#requestCompatible
	*/	 
	public boolean requestCompatible(
    Object requestedQualifier, 
    Object grantedQualifier)
    {

		if (SanityManager.DEBUG) {
            SanityManager.ASSERT((requestedQualifier == RowLock.RS2) ||
                                 (requestedQualifier == RowLock.RS3) ||
                                 (requestedQualifier == RowLock.RU2) ||
                                 (requestedQualifier == RowLock.RU3) ||
                                 (requestedQualifier == RowLock.RIP) ||
                                 (requestedQualifier == RowLock.RI)  ||
                                 (requestedQualifier == RowLock.RX2) ||
                                 (requestedQualifier == RowLock.RX3));
            SanityManager.ASSERT((grantedQualifier == RowLock.RS2) ||
                                 (grantedQualifier == RowLock.RS3) ||
                                 (grantedQualifier == RowLock.RU2) ||
                                 (grantedQualifier == RowLock.RU3) ||
                                 (grantedQualifier == RowLock.RIP) ||
                                 (grantedQualifier == RowLock.RI)  ||
                                 (grantedQualifier == RowLock.RX2) ||
                                 (grantedQualifier == RowLock.RX3));
		}

		RowLock rlRequested = (RowLock) requestedQualifier;
		RowLock rlGranted  = (RowLock) grantedQualifier;

		return(rlRequested.isCompatible(rlGranted));
	}

	/**
		Is a caller that holds a lock compatible with themselves?

		<BR>
		MT - Single thread required (methods of Lockable)

		@see org.apache.derby.iapi.services.locks.Lockable#lockerAlwaysCompatible
	*/	 
	public boolean lockerAlwaysCompatible() {
		return true;
	}

	/**
		Unlock me.

		<BR>
		MT - Single thread required (methods of Lockable)

		@see org.apache.derby.iapi.services.locks.Lockable#unlockEvent
	*/	 
	public void unlockEvent(Latch lockInfo) {
	}

	/*
	**		Methods of Object
	*/

	/**
		Implement value equality.
		<BR>
		MT - Thread safe
	*/
	public boolean equals(Object ref) {

		if (!(ref instanceof RecordId))
			return false;

		RecordId other = (RecordId) ref;

		return ((recordId == other.recordId)
			&& pageId.equals(other.pageId));
	}

	/**
		Return a hashcode based on value.
		<BR>
		MT - thread safe
	*/
	public int hashCode() {

		return (int) recordId ^ pageId.hashCode();
	}

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return "Record id=" + recordId + " " + pageId.toString(); 
        }
        else
        {
            return(null);
        }

    }

	/**
		This lockable wants to participate in the Virtual Lock table.
	 */
	public boolean lockAttributes(int flag, Hashtable attributes)
	{

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(attributes != null, 
				"cannot call lockProperties with null attribute list");
			SanityManager.ASSERT(pageId != null,
				"RecordId PageId is null");
		}

		if ((flag & VirtualLockTable.TABLE_AND_ROWLOCK) == 0)
			return false;

		attributes.put(VirtualLockTable.CONTAINERID, 
					   new Long(pageId.getContainerId().getContainerId()));

		attributes.put(VirtualLockTable.LOCKNAME,
					   "(" + pageId.getPageNumber() + "," + recordId + ")");

		attributes.put(VirtualLockTable.LOCKTYPE, "ROW");

		// don't new unnecessary things for now
		// attributes.put(VirtualLockTable.SEGMENTID, new Long(pageId.getContainerId().getSegmentId()));
		// attributes.put(VirtualLockTable.PAGENUM, new Long(pageId.getPageNumber()));
		// attributes.put(VirtualLockTable.PAGENUM, new Long(pageId.getPageNumber()));
		// attributes.put(VirtualLockTable.RECID, new Integer(getId()));

		return true;
	}

}
