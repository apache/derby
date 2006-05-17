/*

   Derby - Class org.apache.derby.impl.store.raw.data.EncryptContainerUndoOperation

   Copyright 1998, 2006 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Undoable;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.error.StandardException;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/** A Encrypt Container undo operation rolls back the change of a 
 *  Encrypt Container operation 
 */
public class EncryptContainerUndoOperation implements Compensation 
{
	// the operation to rollback 
	transient private	EncryptContainerOperation undoOp;

	/** During redo, the whole operation will be reconstituted from the log */

	/** 
     *	Set up a Encrypt Container undo operation during run time rollback
     *  @param op Encrypt contaner operatation that is to be undone. 
     */
	public EncryptContainerUndoOperation(EncryptContainerOperation op) 
	{
		undoOp = op;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public EncryptContainerUndoOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
        // nothing to write.
	}

	/**
		@exception IOException cannot read log record from log stream
		@exception ClassNotFoundException cannot read ByteArray object
	 */
	public void readExternal(ObjectInput in) 
		 throws IOException, ClassNotFoundException
	{
        // nothing to read.
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_ENCRYPT_CONTAINER_UNDO;
	}

	/** 
		Compensation method
	*/

	/** Set up a Container undo operation during recovery redo. */
	public void setUndoOp(Undoable op)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(op instanceof EncryptContainerOperation);
		}

		undoOp = (EncryptContainerOperation)op;
	}

	/**
		Loggable methods
	*/

    
    /**
     * Check if this operation needs to be redone during recovery redo. 
     * Returns true if this op should be redone during recovery redo,
     * @param xact	the transaction that is doing the rollback
     * @return  true, if this operation needs to be redone during recovery.
     * @exception StandardException Standard Derby error policy
     */
    public boolean needsRedo(Transaction xact)
        throws StandardException
    {
        return true;
    }

    /**
       the default for prepared log is always null for all the operations
       that don't have optionalData.  If an operation has optional data,
       the operation need to prepare the optional data for this method.

       Encrypt Conatainer Undo Operation has no optional data to write out
	*/
    public ByteArray getPreparedLog()
    {
        return (ByteArray) null;
    }


    /** Apply the undo operation, in this implementation of the
        RawStore, it can only call the undoMe method of undoOp
        @param xact			the Transaction that is doing the rollback
        @param instant		the log instant of this compenstaion operation
        @param in			optional data
        @exception IOException Can be thrown by any of the methods of ObjectInput.
        @exception StandardException Standard Derby policy.

        @see EncryptContainerOperation#generateUndo
    */
    public final void doMe(Transaction xact, LogInstant instant, 
                           LimitObjectInput in) 
        throws StandardException, IOException
	{
        undoOp.undoMe(xact);
        releaseResource(xact);
    }

    /* make sure resource found in undoOp is released */
    public void releaseResource(Transaction xact)
	{
        if (undoOp != null)
            undoOp.releaseResource(xact);
    }

    /* Undo operation is a COMPENSATION log operation */
    public int group()
    {
        return Loggable.COMPENSATION | Loggable.RAWSTORE;
    }

    /**
	  DEBUG: Print self.
	*/
    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            String str = "CLR (Encrypt Container Undo): " ;
            if (undoOp != null)
                str += undoOp.toString();
            else
                str += "undo Operation not set";

            return str;
        }
        else
            return null;
    }
}
