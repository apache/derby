/*

   Derby - Class org.apache.derby.impl.store.raw.data.EncryptContainerOperation

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Undoable;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.util.ByteArray;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
 * Log operation to encrypt a container with a new encryption key or to encrypt
 * an unencrypted container while configuring the database for
 * encryption. Container is synced to the disk when encryption is 
 * is successful, there is nothing to do on a redo. If there is crash/error
 * while configuring a database for encryption; original version of the
 * container is put back during undo. 
 *
 * <PRE>
 *  @format_id	LOGOP_ENCRYPT_CONTAINER
 * 	the formatId is written by FormatIdOutputStream when this object is
 *	written out by writeObject
 * @purpose to record enctyption of container with a new encryption key.
 * @upgrade
 * @disk_layout
 *      containerId(ContainerKey)  the id of the container this operation applies to
 *	@end_format
 *  </PRE>
 *
 *  @see Undoable
 */
public class EncryptContainerOperation implements Undoable
{

	private ContainerKey containerId;

	protected EncryptContainerOperation(RawContainerHandle hdl) 
        throws StandardException
	{
		containerId = hdl.getId();
	}

    /*
     * Formatable methods
     */

    // no-arg constructor, required by Formatable
    public EncryptContainerOperation() { super(); }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        containerId.writeExternal(out);
    }

    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException 
    {
        containerId = ContainerKey.read(in);
    }


	/**
		Loggable methods
	*/

    /**
       the default for prepared log is always null for all the operations
       that don't have optionalData.  If an operation has optional data,
       the operation need to prepare the optional data for this method.
       
       Encrypt Operation has no optional data to write out
	*/
    public ByteArray getPreparedLog()
    {
        return (ByteArray) null;
    }

    public void releaseResource(Transaction tran)
    {
        // no resources held to release.
    }

    /**
       A space operation is a RAWSTORE log record
    */
    public int group()
    {
        return Loggable.RAWSTORE;
    }


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
        // this opeation should not be redone during recovery. Encrypted version
        // of the container are synced to the disk when it is complete. In case 
        // rollback containers are replaced with the origincal version. 
        return false;
	}


    /**
       Return my format identifier.
    */
    public int getTypeFormatId() {
        return StoredFormatIds.LOGOP_ENCRYPT_CONTAINER;
    }


    /**
     * Containers are not encryped on a redo. Nothing to do in this method.
     * @param tran      transaction doing the operation.
     * @param instant   log instant for this operation.
     * @param in        unused by this log operation.
     *
     * @exception StandardException Standard Cloudscape error policy
     */
    public final void doMe(Transaction tran, LogInstant instant, 
                           LimitObjectInput in)
		 throws StandardException
	{

        // nothing to do here, containers are not encrypted on redo, 
        // if confuring the database for encryption fails. it is  
        // undone during  recovery. Encryption of the container is done 
        // after the log record is flushed to the disk. 

        releaseResource(tran);
	}


    /**
       Undo of encrytpion of the container. Original version of the container
       that existed before the start of the database encryption is put back.
        
       @param tran the transaction that is undoing this operation
       @exception StandardException Standard Cloudscape error policy
    */
    public void undoMe(Transaction tran) throws StandardException
    {
        // restore the container to the state it was before the encrytpion.
        BaseDataFileFactory bdff = 
            (BaseDataFileFactory) ((RawTransaction) tran).getDataFactory();
        EncryptData ed = new EncryptData(bdff);
        ed.restoreContainer(containerId);
        releaseResource(tran);

	}

	/**
     * Generate a Compensation (EncryptContainerUndoOperation) that 
     * will rollback the changes made to the container during container 
     * encryption.
     * @param tran	the transaction doing the compensating
	 * @param in	optional input; not used by this operation.
     * @exception StandardException Standard Cloudscape error policy
     */
    public Compensation generateUndo(Transaction tran, LimitObjectInput in)
        throws StandardException
    {
        return new EncryptContainerUndoOperation(this);
    }

    /** debug */
    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return  "Encrypt container " + containerId;
        }
        
        return null;
    }
}
