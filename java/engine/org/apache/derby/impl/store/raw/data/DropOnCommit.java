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

import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.services.locks.Lockable;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.store.raw.ContainerKey;

import java.util.Observable;

/**
	Drop a table on a commit or abort
*/

public class DropOnCommit extends ContainerActionOnCommit {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	protected boolean isStreamContainer = false;

	/*
	**	Methods of Observer
	*/

	public DropOnCommit(ContainerKey identity) {

		super(identity);
	}

	public DropOnCommit(ContainerKey identity, boolean isStreamContainer) {

		super(identity);
		this.isStreamContainer = isStreamContainer;
	}

	/**
		Called when the transaction is about to complete.

		@see java.util.Observer#update
	*/
	public void update(Observable obj, Object arg) {
		if (SanityManager.DEBUG) {
			if (arg == null)
				SanityManager.THROWASSERT("still on observr list " + this);
		}

		if (arg.equals(RawTransaction.COMMIT) || arg.equals(RawTransaction.ABORT)) {

			RawTransaction xact = (RawTransaction) obj;

			try {
				if (this.isStreamContainer)
					xact.dropStreamContainer(identity.getSegmentId(), identity.getContainerId());
				else
					xact.dropContainer(identity);
			} catch (StandardException se) {
				xact.setObserverException(se);
			}

			obj.deleteObserver(this);
		}
	}
}
