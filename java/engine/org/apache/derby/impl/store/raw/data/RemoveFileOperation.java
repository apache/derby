/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Undoable;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.io.StorageFile;

import org.apache.derby.iapi.util.ByteArray;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.InputStream;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/** 
*/

public class RemoveFileOperation implements Undoable
{
	private String name;
	private long generationId;
	private boolean removeAtOnce;

	transient private StorageFile fileToGo;

	// no-arg constructor, required by Formatable
	public RemoveFileOperation()
	{
	}

	RemoveFileOperation(String name, long generationId, boolean removeAtOnce)
	{
		this.name = name;
		this.generationId = generationId;
		this.removeAtOnce = removeAtOnce;
	}

	/*
	 * Formatable methods
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeUTF(name);
		out.writeLong(generationId);
		out.writeBoolean(removeAtOnce);
	}

	public void readExternal(ObjectInput in) 
		 throws IOException, ClassNotFoundException 
	{
		name = in.readUTF();
		generationId = in.readLong();
		removeAtOnce = in.readBoolean();
	}
	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_REMOVE_FILE;
	}


	/**
		Loggable methods
	*/

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		Space Operation has no optional data to write out
	*/
	public ByteArray getPreparedLog()
	{
		return null;
	}

	public void releaseResource(Transaction tran)
	{
	}

	/**
		A space operation is a RAWSTORE log record
	*/
	public int group()
	{
		return Loggable.FILE_RESOURCE | Loggable.RAWSTORE ;
	}

	public void doMe(Transaction xact, LogInstant instant, 
						   LimitObjectInput in)
		 throws StandardException
	{
		if (fileToGo == null)
			return;

		BaseDataFileFactory bdff = 
			(BaseDataFileFactory) ((RawTransaction) xact).getDataFactory();
		
		bdff.fileToRemove(fileToGo, true);
	}


	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public boolean needsRedo(Transaction xact)
		 throws StandardException
	{
		if (!removeAtOnce)
			return false;

		FileResource fr = ((RawTransaction) xact).getDataFactory().getFileHandler();

		fileToGo = fr.getAsFile(name, generationId);

		if (fileToGo == null)
			return false;

        return fileToGo.exists();
	}


	public Compensation generateUndo(Transaction xact, LimitObjectInput in)
		throws StandardException, IOException {


		if (fileToGo != null) {
			BaseDataFileFactory bdff = 
				(BaseDataFileFactory) ((RawTransaction) xact).getDataFactory();
		
			bdff.fileToRemove(fileToGo, false);
		}

		return null;
	}
}


