/*

   Derby - Class org.apache.derby.iapi.store.raw.ContainerKey

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.util.Matchable;
import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.VirtualLockTable;

import java.util.Hashtable;

/**
	A key that identifies a Container within the RawStore.
	<BR> MT - Immutable
*/
public final class ContainerKey implements Matchable, Lockable
{
	private final long	segmentId;		// segment identifier
	private final long	containerId;	// container identifier

	/**
		Create a new ContainerKey
	*/
	public ContainerKey(long segmentId, long containerId) {
		this.segmentId = segmentId;
		this.containerId = containerId;
	}

	/**
		Return my identifier within the segment
	*/
	public long getContainerId() {
		return containerId;
	}

	/**
		Return my segment identifier
	*/
	public long getSegmentId() {
		return segmentId;
	}

	/*
	** Methods to read and write ContainerKeys.
	*/

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		CompressedNumber.writeLong(out, segmentId);
		CompressedNumber.writeLong(out, containerId);
	}

	public static ContainerKey read(ObjectInput in) throws IOException
	{
		long sid = CompressedNumber.readLong(in);
		long cid = CompressedNumber.readLong(in);

		return new ContainerKey(sid, cid);
	}

	/*
	** Methods of Object
	*/

	public boolean equals(Object other) {
		if (other == this)
			return true;

		if (other instanceof ContainerKey) {
			ContainerKey otherKey = (ContainerKey) other;

			return (containerId == otherKey.containerId) &&
					(segmentId == otherKey.segmentId);
		} else {
			return false;
		}
	}

	public int hashCode() {

		return (int) (segmentId ^ containerId);
	}

	public String toString() {

		return "Container(" + segmentId + ", " + containerId + ")";
	}

	/*
	** methods of Matchable
	*/

	public boolean match(Object key) {
		// instance of ContainerKey?
		if (equals(key))
			return true;

		if (key instanceof PageKey)
			return equals(((PageKey) key).getContainerId());

		if (key instanceof RecordHandle) {
			return equals(((RecordHandle) key).getContainerId());
		}
		return false;
	}
	/*
	** Methods of Lockable
	*/

	public void lockEvent(Latch lockInfo) {
	}
	 

	public boolean requestCompatible(Object requestedQualifier, Object grantedQualifier) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(requestedQualifier instanceof ContainerLock);
			SanityManager.ASSERT(grantedQualifier instanceof ContainerLock);
		}

		ContainerLock clRequested = (ContainerLock) requestedQualifier;
		ContainerLock clGranted  = (ContainerLock) grantedQualifier;

		return clRequested.isCompatible(clGranted);
	}

	/**
		This method will only be called if requestCompatible returned false.
		This results from two cases, some other compatabilty space has some
		lock that would conflict with the request, or this compatability space
		has a lock tha
	*/
	public boolean lockerAlwaysCompatible() {
		return true;
	}

	public void unlockEvent(Latch lockInfo) {
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
		}

		if ((flag & VirtualLockTable.TABLE_AND_ROWLOCK) == 0)
			return false;

		attributes.put(VirtualLockTable.CONTAINERID, 
					   new Long(getContainerId()));
		attributes.put(VirtualLockTable.LOCKNAME, "Tablelock");
		attributes.put(VirtualLockTable.LOCKTYPE, "TABLE");

		// attributes.put(VirtualLockTable.SEGMENTID, new Long(identity.getSegmentId()));

		return true;
	}
}
