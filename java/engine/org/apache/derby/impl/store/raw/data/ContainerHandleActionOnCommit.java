/*

   Derby - Class org.apache.derby.impl.store.raw.data.ContainerHandleActionOnCommit

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

/**
	An abstract class that opens the container at commit and delegates
	the actual work to a sub-class.
*/

public abstract class ContainerHandleActionOnCommit extends ContainerActionOnCommit {

	public ContainerHandleActionOnCommit(ContainerKey identity) {

		super(identity);
	}

	/*
	**	Methods of Observer
	*/

	/**
		Open the container and call the doIt method
	*/
	public void openContainerAndDoIt(RawTransaction xact) {

		BaseContainerHandle handle = null;
		try {
			handle = (BaseContainerHandle) xact.openContainer(identity, (LockingPolicy) null, 
				ContainerHandle.MODE_FORUPDATE | ContainerHandle.MODE_NO_ACTIONS_ON_COMMIT);

			// if the handle is null, the container may have been removed by a previous observer.
			if (handle != null) {
				try {
					doIt(handle);
				} catch (StandardException se) {
					xact.setObserverException(se);
				}
			}

		} catch (StandardException se) {

			// if we get this exception, then the container is readonly.
			// no problem if we can't open an closed temp container.
			if (identity.getSegmentId()  != ContainerHandle.TEMPORARY_SEGMENT)
				xact.setObserverException(se);
		} finally {
			if (handle != null)
				handle.close();
		}
	}

	protected abstract void doIt(BaseContainerHandle handle)
		throws StandardException;
}
