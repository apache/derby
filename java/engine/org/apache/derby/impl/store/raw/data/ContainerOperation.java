/*

   Derby - Class org.apache.derby.impl.store.raw.data.ContainerOperation

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

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Undoable;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.util.ByteArray;

import java.io.ObjectOutput;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	Log operation to create, drop or remove a container.

	Both the doMe or the undoMe of a create actually caused the container
	header to be modified and flushed before the log record is flushed.  This
	is necessary for 2 reasons, one is that of ensuring enough disk space, and
	the other is because unlike any other operation, the log record create
	container is in the log stream before the container is in the container
	cache.  What this mean is that if a checkpoint started after the container
	operation but before the container is kept or is dirtied in the container
	cache, then checkpoint will not know to wait for the container to be kept
	or cleaned.  The checkpoint will erroneous assume that the operation does
	not need to be redone since its log instant is before the checkpoint but in
	fact the change has not been flushed to disk.

	A drop or remove container does not have this problem.  The container exist
	and is in kept state when the operation is logged so the checkpoint will
	not overlook it and it doesn't need to flush the container header.  In the
	case of remove, the stub is flushed for a different reason - that of
	ensuring disk space.

*/
public class ContainerOperation extends ContainerBasicOperation implements Undoable
{
	protected byte operation;		// create, drop, or remove

	// in previous version of contianerOperation, there may not
	// be a createByteArray
	transient protected boolean hasCreateByteArray = true;

	protected ByteArray createByteArray;	// information necessary to
															// recreate the container 

	protected static final byte CREATE = (byte)1;
	protected static final byte DROP = (byte)2;
	protected static final byte REMOVE = (byte)4;

	protected ContainerOperation(RawContainerHandle hdl, byte operation) 
		 throws StandardException
	{
		super(hdl);
		this.operation = operation;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public ContainerOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException  
	{
		super.writeExternal(out);
		out.writeByte(operation);

		if (operation == CREATE)
		{
			try
			{
				createByteArray = containerHdl.logCreateContainerInfo();
			}
			catch (StandardException se)
			{
				throw new IOException(se.toString());
			}
					
			createByteArray.writeExternal(out);
		}
	}

	/**
		@exception IOException cannot read log record from log stream
		@exception ClassNotFoundException cannot read ByteArray object
	 */
	public void readExternal(ObjectInput in)
		 throws IOException, ClassNotFoundException 
	{
		super.readExternal(in);
		operation = in.readByte();

		if (operation == CREATE && hasCreateByteArray)
		{
			createByteArray = new ByteArray();
			createByteArray.readExternal(in);
		}
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_CONTAINER;
	}


	/*
	 * override ContainerBasicOperation's findContainerForLoadTran
	 */
	/**
		If we are in load tran, and the operation is a create, the container
		may not (should not?) exist yet.  We need to recreate it.

		@exception StandardException Standard Cloudscape policy.
	 */
	protected RawContainerHandle findContainerForLoadTran(RawTransaction xact)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(createByteArray != null,
			"cannot reCreate container in load tran, createByteArray is null");

		long sid = containerId.getSegmentId();
		long cid = containerId.getContainerId();

		xact.reCreateContainerForLoadTran(sid, cid, createByteArray);

		// now we should be able to open this container
		return xact.openDroppedContainer(containerId, (LockingPolicy)null);
	}

	/** 
		@exception StandardException Standard Cloudscape error policy
	*/
	public final void doMe(Transaction tran, LogInstant instant, 
						   LimitObjectInput in)
		 throws StandardException
	{

		switch (operation)
		{
		case DROP:
			containerHdl.dropContainer(instant, true);
			//
			// RESOLVE: if it hasn't been stubbified, even at redo time, we will
			// want to earmark this as a post commit work because we know it will
			// not be wasted effort.
			//
			break;

		case REMOVE:
			containerHdl.removeContainer(instant);
			break;

		case CREATE:
			break;
			// nothing to do with create container, it has already been synced to
			// disk.  If the container is subsequently dropped or even removed,
			// that's fine too.  Don't bother to find it.
		}

		releaseResource(tran);
	}


	/**
		Undo of create, drop or remove

		@param tran the transaction that is undoing this operation
		@param hdl the container handle.  This is found here during runtime
		undo - in which case we made the CLR and passed in the containerHdl
		found in generateUndo and it is passed back to this; or it is found in
		the CLR's needsRedo and is passed in and this operation never found the
		container.  Either case, release resource at the end is safe
		@param CLRInstant the log instant of the CLR
		@param in optional data
		@param dataLength optional data length

		@exception StandardException Standard Cloudscape error policy
	*/
	public void undoMe(Transaction tran, RawContainerHandle hdl,
					   LogInstant CLRInstant, LimitObjectInput in)
		 throws StandardException
	{
		switch(operation)
		{
		case DROP:
			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(hdl != null, "container handle is null");
				SanityManager.ASSERT(hdl.getContainerStatus() != RawContainerHandle.COMMITTED_DROP,
									 "Undoing a drop but the container status is not dropped");
			}
			hdl.dropContainer(CLRInstant, false); // not dropped
			break;

		case CREATE: 
			// remove the container
			hdl.removeContainer(CLRInstant);
			break;

		case REMOVE:
			if (SanityManager.DEBUG) {
				SanityManager.THROWASSERT("cannot undo REMOVE, should not have generated a CLR in the first place");
			}
			break;
		}
		releaseResource(tran);

	}

	/**
		@see org.apache.derby.iapi.store.raw.Undoable
		@exception StandardException Standard Cloudscape error policy
	*/
	public Compensation generateUndo(Transaction tran, LimitObjectInput in)
		 throws StandardException
	{
		if (operation == REMOVE)
			return null;		// cannot undo REMOVE
		else
		{
			RawContainerHandle undoContainerHandle = findContainer(tran);
			
			// mark the container as pre-dirtied so that if a checkpoint
			// happens after the log record is sent to the log stream, the
			// cache cleaning will wait for this change.
			//
			// RESOLVE: don't do this now because if undo failed, this
			// container will be "stuck" in the preDirty state and checkpoint
			// will be stuck
			// undoContainerHandle.preDirty(true);
			//

			return new ContainerUndoOperation(undoContainerHandle, this);
		}
	}

	/** debug */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String str = super.toString();
			switch(operation)
			{
			case CREATE: str += " CREATE container " + containerId;
				break;
			case DROP: str += " DROP container " + containerId;
				break;
			case REMOVE: str += " REMOVE container " + containerId;
				break;
			}
			return str;
		}
		else
			return null;
	}


}
