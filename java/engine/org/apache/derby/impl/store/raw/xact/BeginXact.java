/*

   Derby - Class org.apache.derby.impl.store.raw.xact.BeginXact

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

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.GlobalTransactionId;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.util.ByteArray;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	This operation indicates the beginning of a transaction.
	@see Loggable
*/

public class BeginXact implements Loggable {

	protected int transactionStatus;
	protected GlobalTransactionId xactId;


	public BeginXact(GlobalTransactionId xid, int s)
	{
		xactId = xid;
		transactionStatus = s;
	}

	/*
	 * Formatable methods
	 */
	public BeginXact()
	{  super() ; }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		out.writeInt(transactionStatus);
		out.writeObject(xactId);
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		transactionStatus = in.readInt();
		xactId = (GlobalTransactionId)in.readObject();
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_BEGIN_XACT;
	}

	/**
		Loggable methods
		@see Loggable
	*/

	/**
		Apply the change indicated by this operation and optional data.

		@param xact			the Transaction
		@param instant		the log instant of this operation
		@param in			optional data

	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
	{
		RawTransaction rt = (RawTransaction)xact;

		// If we are not doing fake logging for in memory database
		if (instant != null) 
		{
			rt.setFirstLogInstant(instant);

			// need to do this here rather than in the transaction object for
			// recovery.
			rt.addUpdateTransaction(transactionStatus);
		}
	}

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		BeginXact has no optional data to write out

		@param out Where and how to write to optional data.
		@see ObjectOutput
	*/
	public ByteArray getPreparedLog()
	{
		return (ByteArray) null;
	}

	/**
		Always redo a BeginXact.

		@param xact		The transaction trying to redo this operation
		@return true if operation needs redoing, false if not.
	*/
	public boolean needsRedo(Transaction xact)
	{
		return true;			// always redo this
	}


	/**
		BeginXact has no resource to release
	*/
	public void releaseResource(Transaction xact)
	{}


	/**
		BeginXact is both a FIRST and a RAWSTORE log record
	*/
	public int group()
	{
		int group = Loggable.FIRST | Loggable.RAWSTORE;
		return group;
	}

	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
		if (SanityManager.DEBUG)
			return "BeginXact " + xactId + " transactionStatus " + Integer.toHexString(transactionStatus);
		else
			return null;

	}

	/**
		BeginXact method
	*/
	public GlobalTransactionId getGlobalId()
	{
		return xactId;
	}


}

