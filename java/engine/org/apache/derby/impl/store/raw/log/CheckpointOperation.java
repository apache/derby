/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.log
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.util.ByteArray;

import java.io.Externalizable;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	A Log Operation that represents a checkpoint.
	@see Loggable
*/

public class CheckpointOperation implements Loggable 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	// redo LWM
	protected long	redoLWM;

	// undo LWM
	protected long	undoLWM;

	// other log trunaction points - after the checkpoint is read from the log
	// after a recovery, the array is then used to populate the in memory
	// truncation lwm table.
	protected TruncationPoint[]	truncationLWM;

	protected Formatable transactionTable;

	public CheckpointOperation(long redoLWM, long undoLWM, 
							   TruncationPoint[] tpoints, Formatable ttab)
	{
		this.redoLWM = redoLWM;
		this.undoLWM = undoLWM;
		this.truncationLWM = tpoints;
		this.transactionTable = ttab;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public CheckpointOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		CompressedNumber.writeLong(out, redoLWM);
		CompressedNumber.writeLong(out, undoLWM);
		if (truncationLWM == null)
			CompressedNumber.writeInt(out, 0);	// no other truncation LWM
		else
		{
			CompressedNumber.writeInt(out, truncationLWM.length);
			for (int i = 0; i < truncationLWM.length; i++)
			{
				out.writeObject(truncationLWM[i].getName());

				LogCounter l = (LogCounter)(truncationLWM[i].getLogInstant());
				CompressedNumber.writeLong(out,l.getValueAsLong());
			}
		}

		if (transactionTable == null)
			CompressedNumber.writeInt(out, 0);
		else
		{
			CompressedNumber.writeInt(out, 1);
			out.writeObject(transactionTable);
		}
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		redoLWM = CompressedNumber.readLong(in);
		undoLWM = CompressedNumber.readLong(in);

		int tsize = CompressedNumber.readInt(in); // is there any truncationLWM?
		if (tsize == 0)
			truncationLWM = null;
		else
		{
			truncationLWM = new TruncationPoint[tsize];
			UUID name;
			LogInstant instant;

			for (int i = 0; i < tsize; i++)
			{
				name = (UUID)in.readObject();

				long l = CompressedNumber.readLong(in);
				truncationLWM[i] = new TruncationPoint(name, new LogCounter(l));
			}
		}

		int haveTTab = CompressedNumber.readInt(in);
		if (haveTTab == 1)
			transactionTable = (Formatable)in.readObject();
		else
			transactionTable = (Formatable)null;
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_CHECKPOINT;
	}

	/**
		Loggable methods
	*/

	/**
	 *	Nothing to do unless we are rollforward recovery;
	 *  Redoing of checkpoints during rollforward recovery allows us to restart
	 *  the  roll-forward recovery from the last checkpoint redone during rollforward recovery, if
	 *  we happen to crash during the roll-forward recovery process.
	 *  Another reason why we need to do this is dropped table stub files
	 *  removed at checkpoint because the containerids may have been reused
	 *  after a checkpoint if the system was rebooted.
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in) throws StandardException
	{
		//redo the checkpoint if we are in roll-forward recovery only
		if(((RawTransaction)xact).inRollForwardRecovery())
		{
			((RawTransaction)xact).checkpointInRollForwardRecovery(instant, redoLWM);
		}
		return;
	}

	/**
		the default for prepared log is always null for all the operations
		that don't have optionalData.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		Checkpoint has no optional data to write out

		@param out Where and how to write to optional data.
		
	*/
	public ByteArray getPreparedLog()
	{
		return (ByteArray) null;
	}

	/**
		Checkpoint does not need to be redone unless
		we are doing rollforward recovery.
	*/
	public boolean needsRedo(Transaction xact)
	{
		
		if(((RawTransaction)xact).inRollForwardRecovery())
			return true;
		else
			return false;
	}


	/**
	  Checkpoint has not resource to release
	*/
	public void releaseResource(Transaction xact)
	{}

	/**
		Checkpoint is a raw store operation
	*/
	public int group()
	{
		return Loggable.RAWSTORE;
	}

	/**
		Access attributes of the checkpoint record
	*/
	public long redoLWM() 
	{
		return redoLWM;
	}

	public long undoLWM() 
	{
		return undoLWM;
	}

	public TruncationPoint[] truncationLWM()
	{
		return truncationLWM;
	}

	public Formatable getTransactionTable()
	{
		return transactionTable;
	}

	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			LogCounter undolwm = new LogCounter(undoLWM);
			LogCounter redolwm = new LogCounter(redoLWM);

			StringBuffer str = new StringBuffer(1000)
				.append("Checkpoint : \tredoLWM ")
				.append(redolwm.toString())
				.append("\n\t\tundoLWM ").append(undolwm.toString());

			if (truncationLWM != null)
			{
				LogCounter logLWM;
				for (int i = truncationLWM.length-1; i >= 0; i--)
				{
					logLWM = (LogCounter)(truncationLWM[i].getLogInstant());
					str.append(" truncation point ").append(i)
						.append(" ").append(logLWM.toString());
				}
			}

			if (transactionTable != null)
			{
				str.append(transactionTable.toString());
			}

			return str.toString();
		}
		else
			return null;
	}
}












