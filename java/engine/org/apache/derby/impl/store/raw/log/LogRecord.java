/*

   Derby - Class org.apache.derby.impl.store.raw.log.LogRecord

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

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.RePreparable;
import org.apache.derby.iapi.store.raw.Undoable;

import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;


/**
	The log record written out to disk. This log record includes:
	<P>
    The is a holder object that may be setup using the setValue() and re-used
	rather than creating a new object for each actual log record.

	<P>	<PRE>
	The format of a log record is

	@format_id LOG_RECORD
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@purpose	The log record described every change to the persistent store
	@upgrade
	@disk_layout
		loggable group(CompressedInt)	the loggable's group value
		xactId(TransactionId)			The Transaction this log belongs to
		op(Loggable)					the log operation
	@end_format
	</PRE>

*/
public class LogRecord implements Formatable {

	private TransactionId	xactId;	// the transaction Id
	private Loggable		op;		// the loggable
	private int				group;	// the loggable's group value

	// the objectInput stream that contains the loggable object.  The
	// objectification of the transaction Id and the the loggable object is
	// delayed from readExternal time to getTransactionId and getLoggable time
	// to give the log scan an opportunity to discard the loggable based on
	// group value and xactId.
	transient ObjectInput input;   

	private static final int formatLength = FormatIdUtil.getFormatIdByteLength(StoredFormatIds.LOG_RECORD);

	public LogRecord() {
	}

	/*
	 * Formatable methods
	 */

	/**
		Write this out.
		@exception IOException error writing to log stream
	*/
	public void writeExternal(ObjectOutput out) throws IOException
	{
		CompressedNumber.writeInt(out, group);
		out.writeObject(xactId);
		out.writeObject(op);
	}

	/**
		Read this in
		@exception IOException error reading from log stream
		@exception ClassNotFoundException corrupted log stream
	*/
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		group = CompressedNumber.readInt(in);
		input = in;				// tie the input to this logRecord

		xactId = null;			// delay reading these until later
		op = null;
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOG_RECORD;
	}

	/*
	 * class specific methods
	 */
	public void setValue(TransactionId xactId, Loggable op)
	{
		this.xactId = xactId;
		this.op = op;

		this.group = op.group();
	}

	public static int formatOverhead()
	{
		return formatLength;
	}		

	public static int maxGroupStoredSize()
	{
		return CompressedNumber.MAX_INT_STORED_SIZE;
	}	

	public static int maxTransactionIdStoredSize(TransactionId tranId)
	{
		return tranId.getMaxStoredSize();
	}

	public TransactionId getTransactionId() 
		 throws IOException, ClassNotFoundException 
	{
		if (xactId != null)
			return xactId;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(input != null, 
					 "xactId not objectified but object input is not set"); 

		Object obj = input.readObject();
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(obj instanceof TransactionId,
   						 "log record not getting expected TransactionId");
	    }
		xactId = (TransactionId)obj;

		return xactId;
	}

    public Loggable getLoggable() throws IOException, ClassNotFoundException {

		if (op != null)			// If log operation is already objectified,
			return op;			// then just return it.

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(input != null, 
					 "logop not objectified but object input is not set");

		if (xactId == null)		// xactId is not read off yet
		{
			xactId = (TransactionId)input.readObject();
		}

		Object obj = input.readObject();

		if (SanityManager.DEBUG) {
			if ( ! (obj instanceof Loggable))
				SanityManager.THROWASSERT(
					"log record not getting expected Loggable: got : " +
					obj.getClass().getName());
		}
		op = (Loggable)obj;

		input = null;

		return op;
	}

    public RePreparable getRePreparable() 
        throws IOException, ClassNotFoundException 
    {
        return((RePreparable) getLoggable());
	}

	/**
		Skip over the loggable.  Set the input stream to point ot after the
		loggable as if the entire log record has been sucked in by the log
		record

		@exception StandardException if the loggable is not found, log is corrupt
	*/
	public void skipLoggable() throws StandardException
	{
		if (op != null)		// loggable already read off
			return;

		try
		{
			if (xactId == null)
				xactId = (TransactionId)input.readObject();	// get rid of the transactionId

			if (op == null)
				op = (Loggable)input.readObject();	// get rid of the loggable
		}
		catch(ClassNotFoundException cnfe)
		{
			throw StandardException.newException(SQLState.LOG_CORRUPTED, cnfe);
		}
		catch(IOException ioe)
		{
			throw StandardException.newException(SQLState.LOG_CORRUPTED, ioe);
		}
	}

	public Undoable getUndoable() throws IOException, ClassNotFoundException
	{
		if (op == null)
			getLoggable();		// objectify it

		if (op instanceof Undoable)
			return (Undoable) op;
		else
			return null;
	}

	public boolean isCLR()	{
		return ((group & Loggable.COMPENSATION) != 0);
	}

	public boolean isFirst()	{
		return ((group & Loggable.FIRST) != 0);
	}

	public boolean isComplete()	{
		return ((group & Loggable.LAST) != 0);
	}

	public boolean isPrepare()	{
		return ((group & Loggable.PREPARE) != 0);
	}

	public boolean requiresPrepareLocks()	{
		return ((group & Loggable.XA_NEEDLOCK) != 0);
	}

	public boolean isCommit()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((group & Loggable.LAST) == Loggable.LAST,
				 "calling isCommit on log record that is not last");
			SanityManager.ASSERT((group & (Loggable.COMMIT | Loggable.ABORT)) != 0,
				 "calling isCommit on log record before commit status is recorded");
		}
		return ((group & Loggable.COMMIT) != 0);
	}

	public boolean isAbort()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((group & Loggable.LAST) == Loggable.LAST,
				 "calling isAbort on log record that is not last");
			SanityManager.ASSERT((group & (Loggable.COMMIT | Loggable.ABORT)) != 0,
				 "calling isAbort on log record before abort status is recorded");
		}
		return ((group & Loggable.ABORT) != 0);
	}

	public int group()
	{
		return group;
	}


}
