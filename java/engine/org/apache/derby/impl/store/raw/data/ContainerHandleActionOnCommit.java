/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
