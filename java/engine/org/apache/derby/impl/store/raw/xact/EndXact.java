/*

   Derby - Class org.apache.derby.impl.store.raw.xact.EndXact

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

import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.util.ByteArray;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	This operation indicates the End of a transaction.
	@see Loggable
*/

public class EndXact implements Loggable {

	private int transactionStatus;
	private GlobalTransactionId xactId;

	public EndXact(GlobalTransactionId xid, int s) {
		super();

		xactId = xid;
		transactionStatus = s;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public EndXact() 
	{ super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		out.writeObject(xactId);
		CompressedNumber.writeInt(out, transactionStatus);
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		xactId = (GlobalTransactionId)in.readObject();
		transactionStatus = CompressedNumber.readInt(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_END_XACT;
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
		@param dataLengt	optional data length

	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
	{

        if ((transactionStatus & Xact.END_PREPARED) == 0)
        {
            ((RawTransaction)xact).removeUpdateTransaction();
        }
        else
        {
            ((RawTransaction)xact).prepareTransaction();
        }
	}

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		EndXact has no optional data to write out

		@param out Where and how to write to optional data.
		@see ObjectOutput
	*/
	public ByteArray getPreparedLog()
	{
		return (ByteArray) null;
	}

	/**
		Always redo an EndXact.

		@param xact		The transaction trying to redo this operation
		@return true if operation needs redoing, false if not.
	*/
	public boolean needsRedo(Transaction xact)
	{
		return true;			// always redo this
	}


	/**
		EndXact has no resource to release
	*/
	public void releaseResource(Transaction xact)
	{}


	/**
		EndXact is a RAWSTORE log record.
	*/
	public int group()
	{
		int group = Loggable.RAWSTORE;

		if ((transactionStatus & Xact.END_COMMITTED) != 0)
			group |= (Loggable.COMMIT | Loggable.LAST);
		else if ((transactionStatus & Xact.END_ABORTED) != 0)
			group |= (Loggable.ABORT | Loggable.LAST);
        else if ((transactionStatus & Xact.END_PREPARED) != 0)
            group |= Loggable.PREPARE;

		return group;
	}
		  

	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String endStatus;
			switch(transactionStatus & 
                   (Xact.END_ABORTED | Xact.END_PREPARED | Xact.END_COMMITTED))
			{
                case Xact.END_ABORTED:	
                    endStatus = " Aborted"; 
                    break;
                case Xact.END_PREPARED:	
                    endStatus = " Prepared"; 
                    break;
                case Xact.END_COMMITTED:
                    endStatus = " Committed"; 
                    break;
                default:				
                    endStatus = "Unknown";
			}				
				
			return(
                "EndXact " + xactId + endStatus + 
                " : transactionStatus = " + endStatus); 
		}
		else
        {
			return null;
        }
	}
}
