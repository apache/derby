/*

   Derby - Class org.apache.derby.impl.store.raw.xact.TransactionTableEntry

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.store.access.TransactionInfo;
import org.apache.derby.iapi.store.raw.GlobalTransactionId;
import org.apache.derby.iapi.store.raw.xact.TransactionId;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
	Transaction table entry is used to store all relavent information of a
	transaction into the transaction table for the use of checkpoint, recovery,
	Transaction management during Quiesce state, and for dumping transaction table.  Only works
	with the following classes: TransactionTable, XactFactory, Xact
	<BR>
	During run time, whenever any transaction is started, it is put into the
	transaction table.  Whenever any transaction is closed, it is removed from
	the transaction table.


*/

public class TransactionTableEntry implements Formatable, TransactionInfo, Cloneable
{
	// These fields are only populated if this TTE has been read in from the
	// log.  Otherwise, they are gotten from the transaction object myxact.
	private TransactionId           xid;
	private GlobalTransactionId     gid;
	private LogInstant              firstLog;
	private LogInstant              lastLog;

	// this field is always present - it is 0 for read only transaction, this
    // is a copy of the status from the Xact (the copy is necessary as during
    // recovery the Xact is shared by all transaction table entries during
    // redo and undo).
	private int                     transactionStatus;

	// fields useful for returning transaction information if read from 
    // transaction log during recovery
	private transient Xact    myxact; 
	private transient boolean update;
	private transient boolean recovery;         // is this a transaction read 
                                                // from the log during recovery?
	private transient boolean needExclusion;    // in a quiesce state , this 
                                                // transaction needs to be 
                                                // barred from activation 
                                                // during quiesce state

	private boolean isClone;		            // am I a clone made for the 
                                                // TransactionVTI?

	private transient LanguageConnectionContext lcc;


	/* package */
	// entry attribute
	static final int UPDATE		= 0x1;
	static final int RECOVERY	= 0x2;
	static final int EXCLUDE	= 0x4;

	TransactionTableEntry(
    Xact            xact, 
    TransactionId   tid, 
    int             status, 
    int             attribute)
	{
		myxact              = xact;
		xid                 = tid;
		transactionStatus   = status;

		update              = (attribute & UPDATE)   != 0;
		needExclusion       = (attribute & EXCLUDE)  != 0;
		recovery            = (attribute & RECOVERY) != 0;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(tid != null, "tid is null");
			if (update && xact.getFirstLogInstant() == null)
            {
				SanityManager.THROWASSERT(
                    "update transaction has firstLog = null");
            }


            /*
			if (!update && xact.getFirstLogInstant() != null)
            {
				SanityManager.THROWASSERT(
                    "read only transaction has firstLog = " + 
                    xact.getFirstLogInstant());
            }
            */
		}

		// Normally, we don't need to remember the gid, firstLog and lastLog
		// because myxact will have the same information.  However, in
		// recovery, there is only one transaction taking on different identity
		// as the log is replayed.  Then each transaction table entry has keep
		// its own identity and not rely on myxact.  These recovery
		// transactions are materialized in the transaction table via a
		// readObject in the checkpoint log record, or are added by
		// addUpdateTransaction when the log is scanned.
		if (recovery)
		{
			// make a copy of everything
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(update, "recovery but not update");

				if (tid != xact.getId())
                {
                    SanityManager.THROWASSERT(
                        "adding a update transaction during recovery " + 
                        " but the tids doesn't match" +
                        tid + " " + xact.getId());
                }
			}

			gid         = xact.getGlobalId();
			firstLog    = xact.getFirstLogInstant();
			lastLog     = xact.getLastLogInstant();
		}
	}

	/*
	 * Formatable methods
	 */
	public TransactionTableEntry()
	{ }

	public void writeExternal(ObjectOutput out) throws IOException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!recovery, "writing out a recovery transaction");
			SanityManager.ASSERT(update, "writing out read only transaction");
			SanityManager.ASSERT(myxact.getFirstLogInstant() != null, 
								 "myxact.getFirstLogInstant is null");
			SanityManager.ASSERT(!isClone, "cannot write out a clone");
		}

		// Why is is safe to access first and last log instant in myxact while
		// this is happening?  Because we only writes out update transaction
		// during run time.  When a read only transactions becomes an update
		// transaction , or when an update transaction commits, the beginXact
		// and endXact log record's doMe method will try to change the
		// transaction table entry's state to updat and non-update
		// respectively.  That change needs to go thru the transaction table
		// which is mutually exclusive to writing out the transaction table.
		// Since we are only looking at update transactions and it is "stuck"
		// in update state in the middle of a TransactionTable.writeExternal
		// call, all the fields we access in myxact is stable (actually the xid
		// is also stable but we already have it).
		//
		out.writeObject(xid);
		out.writeObject(myxact.getGlobalId());
		out.writeObject(myxact.getFirstLogInstant());
		out.writeObject(myxact.getLastLogInstant());
		out.writeInt(transactionStatus);
	}

	public void readExternal(ObjectInput in) 
		 throws ClassNotFoundException, IOException
	{
		// the only time a transaction table entry is written out is to the
		// log, so this must be read in during recovery.
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot write out a clone");

		xid = (TransactionId)in.readObject();
		gid = (GlobalTransactionId)in.readObject();
		firstLog = (LogInstant)in.readObject();
		lastLog = (LogInstant)in.readObject();
		transactionStatus = in.readInt();
		update = true;
		recovery = true;
		needExclusion = true;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(xid != null, "read in transaction table entry with null id");
			SanityManager.ASSERT(firstLog != null, "read in transaction table entry with firstLog");
		}

	}


	// set my transaction instance variable for a recovery transaction
	void setXact(Xact xact)
	{
        /*
        RESOLVE (mikem) - prepared transactions now call setXact() when they are
        not in recovery.
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(recovery, 
								 "setting non-recovery transaction table entry xact");
			SanityManager.ASSERT(!isClone, "cannot setXact with a clone");
		}
        */
		myxact = xact;

	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.RAW_STORE_TRANSACTION_TABLE_ENTRY;
	}

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer str = new StringBuffer(500).
				append("Xid=").append(getXid()).
				append(" gid=").append(getGid()).
				append(" firstLog=").append(getFirstLog()).
				append(" lastLog=").append(getLastLog()).
				append(" transactionStatus=").append(transactionStatus).
				append(" myxact=").append(myxact).
				append(" update=").append(update).
				append(" recovery=").append(recovery).
				append(" prepare=").append(isPrepared()).
				append(" needExclusion=").append(needExclusion).
				append("\n");
			return str.toString();
		}
		else
			return null;
	}

	void updateTransactionStatus(Xact xact, int status, int attribute)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(myxact == xact,
				"update transaction status for wrong xact");
			SanityManager.ASSERT(!isClone, "cannot change a clone");
		}

		this.update = (attribute & UPDATE) != 0;
	}

	void removeUpdateTransaction()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot change a clone");

		this.update = false;
		transactionStatus = 0;
		
	}

	void unsetRecoveryStatus()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot change a clone");

        // RESOLVE (mikem) - this is kind of ugly. move to a better place?

        firstLog = null;

		this.recovery = false;
	}

	void prepareTransaction()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot change a clone");

		transactionStatus |= Xact.END_PREPARED;
	}

    /**************************************************************************
     * get instance variables
     **************************************************************************
     */

	TransactionId getXid()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(xid != null, "TTE with null xid");
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");
		}

		return xid;
	}

	public final GlobalTransactionId getGid()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");

		if (gid != null)
			return gid;

		if (myxact != null)
			return myxact.getGlobalId();

		return null;
	}

	LogInstant getFirstLog()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");

			if (recovery)
            {
				SanityManager.ASSERT(
                    firstLog != null, 
                    "a recovery transaction with a null firstLog");
            }
			else
            {
				SanityManager.ASSERT(
                    firstLog == null, 
                    "a normal transaction with a non-null firstLog" +
                    "myxact.getFirstLogInstant() = " + myxact.getFirstLogInstant());
            }
		}

		if (firstLog != null)
			return firstLog;

		if (myxact != null)
			return myxact.getFirstLogInstant();

		return null;
	}

	LogInstant getLastLog()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");

		if (lastLog != null)
			return lastLog;

		if (myxact != null)
			return myxact.getLastLogInstant();

		return null;
	}

	public final Xact getXact()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");

		return myxact;
	}

	int getTransactionStatus()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");

		return transactionStatus;
	}

	boolean isUpdate()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");

		return update;
	}

	boolean isRecovery()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");

		return recovery;
	}

	boolean isPrepared()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");

		return((transactionStatus & Xact.END_PREPARED) != 0);
	}




	public boolean needExclusion()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClone, "cannot call method with a clone");

		return needExclusion;
	}

	/**
		Methods of TransactionInfo
	 */
	public String getTransactionIdString()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
                !recovery, "trying to display recovery transaction");
			SanityManager.ASSERT(myxact != null, "my xact is null");
			SanityManager.ASSERT(isClone, "Should only call method on a clone");
		}

		TransactionId t = myxact.getIdNoCheck();
		return (t == null) ? "CLOSED" : t.toString();
	}

	public String getGlobalTransactionIdString()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
                !recovery, "trying to display recovery transaction");
			SanityManager.ASSERT(myxact != null, "my xact is null");
			SanityManager.ASSERT(isClone, "Should only call method on a clone");
		}

		GlobalTransactionId gid = myxact.getGlobalId();
		return (gid == null) ? null : gid.toString();
	}

	public String getUsernameString()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isClone, "Should only call method on a clone");

		getlcc();
		return (lcc == null) ? null : lcc.getAuthorizationId();
	}

	public String getTransactionTypeString()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isClone, "Should only call method on a clone");

        if (myxact == null)
            return null;
        else if (myxact.getTransName() != null)
            return myxact.getTransName();
        else
            return myxact.getContextId();
	}

	public String getTransactionStatusString()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isClone, "Should only call method on a clone");

		return (myxact == null) ? null : myxact.getState();
	}

	public String getStatementTextString()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isClone, "Should only call method on a clone");

		getlcc();
		if (lcc != null)
		{
			StatementContext sc = lcc.getStatementContext();
			if (sc != null)
				return sc.getStatementText() ;
		}
		return null;

	}

	public String getFirstLogInstantString()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isClone, "Should only call method on a clone");

		LogInstant logInstant = 
            (myxact == null) ? null : myxact.getFirstLogInstant();

		return (logInstant == null) ? null : logInstant.toString();

	}		

	private void getlcc()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isClone, "Should only call method on a clone");

		if (lcc == null && myxact != null && myxact.xc != null)
		{
			XactContext xc = myxact.xc;

			lcc = (LanguageConnectionContext)
				xc.getContextManager().getContext(
                    LanguageConnectionContext.CONTEXT_ID);
		}
	}

	/**
		Cloneable
	 */
	protected Object clone()
	{
		try 
		{
			Object c = super.clone();
			((TransactionTableEntry)c).isClone = true;

			return c;
		}
		catch (CloneNotSupportedException e) 
		{
			// this should not happen, we are cloneable
			if (SanityManager.DEBUG) 
            {
				SanityManager.THROWASSERT(
                    "TransactionTableEntry cloneable but throws CloneNotSupportedException " + e);
			}
			return null;
		}				
	}
}
