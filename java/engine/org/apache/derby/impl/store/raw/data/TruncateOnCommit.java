/*

   Derby - Class org.apache.derby.impl.store.raw.data.TruncateOnCommit

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

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerLock;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.services.locks.Lockable;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Observable;

/**
	Truncate a temp table on a commit, abort or rollback to savepoint
*/

public class TruncateOnCommit extends ContainerHandleActionOnCommit {

	/**
		Truncate on a commit as well.
	*/
	private boolean commitAsWell;

	public TruncateOnCommit(ContainerKey identity, boolean commitAsWell) {

		super(identity);
		this.commitAsWell = commitAsWell;

		if (SanityManager.DEBUG) {
			if (identity.getSegmentId() != ContainerHandle.TEMPORARY_SEGMENT)
				SanityManager.THROWASSERT("segment id is not temp segment " + identity.getSegmentId());
		}
	}

	public void update(Observable obj, Object arg) {
		if (SanityManager.DEBUG) {
			if (arg == null)
				SanityManager.THROWASSERT("still on observer list " + this);
		}

		if (arg.equals(RawTransaction.ABORT) ||
			arg.equals(RawTransaction.SAVEPOINT_ROLLBACK) ||
			(commitAsWell && arg.equals(RawTransaction.COMMIT))) {
			openContainerAndDoIt((RawTransaction) obj);
		}

		// remove this object if we are commiting, aborting or the container is being dropped
		if (arg.equals(RawTransaction.COMMIT) || arg.equals(RawTransaction.ABORT)
			|| arg.equals(identity)) {
			obj.deleteObserver(this);
		}
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	protected void doIt(BaseContainerHandle handle)
		throws StandardException {

		handle.container.truncate(handle);
	}

	public boolean equals(Object other) {

		if (other instanceof TruncateOnCommit) {

			if (((TruncateOnCommit) other).commitAsWell
				!= commitAsWell)
				return false;

			return super.equals(other);
		}
		else
			return false;
	}
}
