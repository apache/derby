/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.log
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.util.ByteArray;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	A Log Operation that record the truncation low water marks
	@see Loggable
*/
public final class SaveLWMOperation implements Loggable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	// log trunaction point - when a SaveLWMOperation is read from the log
	// during recovery, the truncation LWM is added to the truncation lwm table
	// 
	// A checkpoint log record has the complete truncation lwm record.
	// Since removal of a truncation point is not logged, every time a
	// checkpoint log record is encouter, all truncation lwm will be reset
	// based on information in that operation.
	private UUID name;
	private LogInstant LWMinstant;
	private boolean set;		// to set or unset this LWM

	public SaveLWMOperation(UUID name, LogInstant instant, boolean set)
	{
		this.name = name;
		this.LWMinstant = instant;
		this.set = set;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public SaveLWMOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		out.writeBoolean(set);
		out.writeObject(name);

		long l = LogCounter.INVALID_LOG_INSTANT;

		if (LWMinstant != null)
			l = ((LogCounter)LWMinstant).getValueAsLong();

		CompressedNumber.writeLong(out, l);
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		set = in.readBoolean();
		name = (UUID)in.readObject();
		long l = CompressedNumber.readLong(in);

		if (l != LogCounter.INVALID_LOG_INSTANT)
			LWMinstant = new LogCounter(l);
		else
			LWMinstant = null;
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_SAVE_LWM;
	}

	/**
		Add truncation lwm
		@exception StandardException Standard Cloudscape error policy
	 */
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
		 throws StandardException
	{
		if (set)
			((RawTransaction)xact).addTruncationLWM(name, LWMinstant);
		else
			((RawTransaction)xact).removeTruncationLWM(name);
	}

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		This operation has no optional data to write out
	*/
	public ByteArray getPreparedLog()
	{
		return (ByteArray) null;
	}

	/**
	  Always redo
	*/
	public boolean needsRedo(Transaction xact)
	{
		return true;	
	}

	/**
		no resource to release
	*/
	public void releaseResource(Transaction xact)
	{}

	/**
		a raw store operation
	*/
	public int group()
	{
		return Loggable.RAWSTORE;
	}


	public TruncationPoint truncationLWM()
	{
		return new TruncationPoint(name, LWMinstant);
	}

	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			LogCounter logLWM = (LogCounter)LWMinstant;
			String str = set ? " SET " : " UNSET ";

			return str + "SaveLWMOperation : truncation point " + name + " " + logLWM;

		}
		else
			return null;
	}


}
