/*

   Derby - Class org.apache.derby.impl.store.raw.data.ContainerUndoOperation

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Undoable;

import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;

import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/** A Container undo operation rolls back the change of a Container operation */
public class ContainerUndoOperation extends ContainerBasicOperation 
		implements Compensation 
{
	// the operation to rollback 
	transient private	ContainerOperation undoOp;

	/** During redo, the whole operation will be reconstituted from the log */

	/** 
		Set up a Container undo operation during run time rollback
		@exception StandardException container Handle is not active
	*/
	public ContainerUndoOperation(RawContainerHandle hdl, ContainerOperation op) 
		 throws StandardException
	{
		super(hdl);
		undoOp = op;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public ContainerUndoOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
	}

	/**
		@exception IOException cannot read log record from log stream
		@exception ClassNotFoundException cannot read ByteArray object
	 */
	public void readExternal(ObjectInput in) 
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_CONTAINER_UNDO;
	}

	/** 
		Compensation method
	*/

	/** Set up a Container undo operation during recovery redo. */
	public void setUndoOp(Undoable op)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(op instanceof ContainerOperation);
		}

		undoOp = (ContainerOperation)op;
	}

	/**
		Loggable methods
	*/

	/** Apply the undo operation, in this implementation of the
		RawStore, it can only call the undoMe method of undoOp

		@param xact			the Transaction that is doing the rollback
		@param instant		the log instant of this compenstaion operation
		@param in			optional data
		@param dataLengt	optional data length

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.

		@see ContainerOperation#generateUndo
	 */
	public final void doMe(Transaction xact, LogInstant instant, LimitObjectInput in) 
		 throws StandardException, IOException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(containerHdl != null, "clr has null containerHdl");
		}

		// if this is called during runtime rollback, generateUndo found
		// the container and have it opened there.
		// if this is called during recovery redo, this.needsRedo found 
		// the container and have it opened here.
		//
		// in either case, containerHdl is the opened container handle.

		undoOp.undoMe(xact, containerHdl, instant, in);
		releaseResource(xact);
	}

	/* make sure resource found in undoOp is released */
	public void releaseResource(Transaction xact)
	{
		if (undoOp != null)
			undoOp.releaseResource(xact);
		super.releaseResource(xact);
	}

	/* Undo operation is a COMPENSATION log operation */
	public int group()
	{
		return super.group() | Loggable.COMPENSATION | Loggable.RAWSTORE;
	}

}
