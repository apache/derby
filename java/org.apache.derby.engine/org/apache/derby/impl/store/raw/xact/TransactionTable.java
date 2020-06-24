/*

   Derby - Class org.apache.derby.impl.store.raw.xact.TransactionTable

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

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.store.access.TransactionInfo;

import org.apache.derby.iapi.store.raw.GlobalTransactionId;

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.iapi.services.io.CompressedNumber;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
	The transaction table is used by the transaction factory to keep track of
	all transactions that are in the system.

	<BR> The transction table serves the following purposes: <OL> 

	<LI> checkpoint - when a checkpoint log record is written out, it writes
	out also all transactions that have updated the database.  RESOLVE: this is
	actually not used right now - rather, the transaction table is
	reconstructed during the redo phase by traversing from the undo LWM.  It is
	a goal to use this transaction table (and traversing from the redoLWM)
	instead of rebuilding it to speed up recovery.

	<LI> Quiesce State - when a  system enters the quiesce state, it needs to account
	for all transactions in the system, even those which are just started and
	are in their IDLE state.

	<LI> TransactionTable VTI - we need to get a snapshot of all transactions
	in the system for diagnostic purposes.
	</OL>

	In order to speed up the time it takes to look up a transaction from the
	transaction table, each transaction must have a unique transaction Id.
	This means newly coined transaction must also have a transaction Id.

	<P>During recovery, there is only one real xact object doing all the
	recovery work, but there could be many outstanding transactions that are
	gleamed from the log.  Each of these "recovery transactions" have its on
	entry into the transaction table but they all share the same Xact object.

	<P>Multithreading considerations:<BR>
	TransactionTable must be MT-safe it is called upon by many threads
	simultaneously (except during recovery)

//IC see: https://issues.apache.org/jira/browse/DERBY-6242
    <P>Methods that are only called during
    recovery don't need to take MT considerations, and can safely use iterators
    with no additional synchronization.

*/

public class TransactionTable implements Formatable
{
	/*
	 * Fields
	 */

    private final ConcurrentHashMap<TransactionId, TransactionTableEntry> trans;

	private TransactionId largestUpdateXactId;

	/**
		MT - not needed for constructor
	*/
	public TransactionTable()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
        trans = new ConcurrentHashMap<TransactionId, TransactionTableEntry>();
	}

	/*************************************************************
	 * generic methods called by all clients of transaction table
	 * Must be MT -safe
	 ************************************************************/
	private TransactionTableEntry findTransactionEntry(TransactionId id)
	{

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
                id != null, "findTransacionEntry with null id");

		// Hashtable is synchronized
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		return trans.get(id);
	}

    /**
     * Interface for visiting entries in the transaction table.
     * @see #visitEntries(EntryVisitor)
     */
    static interface EntryVisitor {
        /**
         * Visit an entry. {@link #visitEntries(EntryVisitor)} will call this
         * method once for each entry in the transaction table, or until
         * {@code false} is returned by this method.
         *
         * @param entry the {@code TransactionTableEntry} being visited
         * @return {@code true} if the scan of the transaction table should
         * continue, or {@code false} if the visitor has completed its work
         * and no more entries need to be visited
         */
        boolean visit(TransactionTableEntry entry);
    }

    /**
     * <p>
     * Visit all the entries in the transaction table.
     * </p>
     *
     * <p>
     * MT - MT safe
     * </p>
     *
     * <p>
     * Entries that are added to or removed from the transaction table while
     * it's being traversed, may or may not be visited. All the entries that
     * are present in the map when this method is called, and have not been
     * removed when the method returns, will have been visited exactly once
     * (except if the {@code visit()} method returns false before all entries
     * have been visited, in which case the traversal of the map will stop
     * earlier).
     * </p>
     *
     * <p>
     * Note however that this method does not guarantee that a single
     * {@code TransactionTableEntry} is not accessed concurrently by multiple
     * threads. If the visitor accesses some of the entry's mutable state, the
     * caller must ensure that appropriate synchronization protection is in
     * place. For example, if accessing the update state of the entry, the
     * caller must synchronize on "this" (the {@code TransactionTable}
     * instance).
     * </p>
     *
     * @param visitor the visitor to apply on each transaction table entry
     */
    void visitEntries(EntryVisitor visitor) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
        for (Object entry : trans.values()) {
            if (!visitor.visit((TransactionTableEntry) entry)) {
                // The visitor returned false, meaning that it's done with
                // all of its work and we can stop the scan.
//IC see: https://issues.apache.org/jira/browse/DERBY-3092
                break;
            }
        }
    }


	void add(Xact xact, boolean exclude)
	{
		TransactionId id = xact.getId();

//IC see: https://issues.apache.org/jira/browse/DERBY-4512
        TransactionTableEntry newEntry = new TransactionTableEntry(
                xact, id, 0, exclude ? TransactionTableEntry.EXCLUDE : 0);

		synchronized(this)
		{
            Object oldEntry = trans.put(id, newEntry);

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                        oldEntry == null,
                        "Trying to add a transaction that's already " +
                        "in the transaction table");

                if (SanityManager.DEBUG_ON("TranTrace"))
                {
                    SanityManager.DEBUG(
                        "TranTrace", "adding transaction " + id);
                    SanityManager.showTrace(new Throwable("TranTrace"));
                }
            }
		}

		if (SanityManager.DEBUG) {

			if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

				if (trans.size() > 50)
					System.out.println("memoryLeakTrace:TransactionTable " + trans.size());
			}
		}
	}

	/**
		remove the transaction Id an return false iff the transaction is found
		in the table and it doesn't need exclusion during quiesce state
	 */
	boolean remove(TransactionId id)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
                id != null, 
                "cannot remove transaction from table with null id");

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("TranTrace"))
            {
                SanityManager.DEBUG(
                    "TranTrace", "removing transaction " + id);
                SanityManager.showTrace(new Throwable("TranTrace"));
            }
        }

		// Hashtable is synchronized
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		 TransactionTableEntry ent = trans.remove(id);
		 return (ent == null || ent.needExclusion());
	}


	/**
		Change a transaction to update or add an update transaction to this table.

		@param tid the transaction id
		@param tran the transaction to be added
		@param transactionStatus the transaction status that is stored in the
				BeginXact log record
	 */
	public void addUpdateTransaction(TransactionId tid, RawTransaction tran,
									 int transactionStatus)
	{

		// we need to synchronize on the transaction table because we have to
		// prevent this state change from happening when the transaction table
		// itself is written out to the checkpoint.  This is the only
		// protection the TransactionTableEntry has to prevent fields in myxact
		// from changing underneath it while it is being written out.
		synchronized(this)
		{
			TransactionTableEntry ent = findTransactionEntry(tid);

			if (ent != null)
			{
				// this happens during run time, when a transaction that is
				// already started changed status to an update transaction

				ent.updateTransactionStatus((Xact)tran, transactionStatus,
											TransactionTableEntry.UPDATE) ;
			}
			else
			{
				// this happens during recovery, that's why we haven't seen
				// this transaction before - it is added in the doMe of the 
				// BeginXact log record.
				//
				// No matter what this transaction is, it won't need to be run
				// in quiesce state because we are in recovery.
				ent = new TransactionTableEntry((Xact)tran, tid, transactionStatus, 
												TransactionTableEntry.UPDATE | 
												TransactionTableEntry.EXCLUDE |
												TransactionTableEntry.RECOVERY);
				trans.put(tid, ent);

			}

			if (XactId.compare(ent.getXid(), largestUpdateXactId) > 0)
				largestUpdateXactId = ent.getXid();
		}
	}

	/**
	    Change update transaction to non-update

		<P>MT - MT safe, since vector is MT-safe.

		@param id the transaction Id
	  */
	void removeUpdateTransaction(TransactionId id)
	{
		// we need to synchronize on the transaction table because we have to
		// prevent this state change from happening when the transaction table
		// itself is written out to the checkpoint.  This is the only
		// protection the TransactionTableEntry has to prevent fields in myxact
		// from changing underneath it while it is being written out.

		synchronized (this)
		{
			TransactionTableEntry ent = findTransactionEntry(id);

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(ent != null, 
				 "removing update transaction that is not there");
			}

			ent.removeUpdateTransaction();

			// If we are committing a recovery transaction, remove it from the
			// transaction table.  The xact object which is doing the work is
			// not going to be closed even though the transaction is done.
			if (ent.isRecovery())
				remove(id);
		}

		return;
	}

    /**************************************************************************
     * Transaction table methods used by XA.
     **************************************************************************
     */

	/**
	    Change transaction to prepared.

		<P>MT - unsafe, caller is recovery, which is single threaded.

		@param id the transaction Id
	  */
	void prepareTransaction(TransactionId id)
	{
		// we need to synchronize on the transaction table because we have to
		// prevent this state change from happening when the transaction table
		// itself is written out to the checkpoint.  This is the only
		// protection the TransactionTableEntry has to prevent fields in myxact
		// from changing underneath it while it is being written out.

        TransactionTableEntry ent = findTransactionEntry(id);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                ent != null, "preparing transaction that is not there");
        }

        ent.prepareTransaction();

		return;
	}

    /**
     * Find a transaction in the table by Global transaction id.
     * <p>
     * This routine use to be only called during offline recovery so performance
     * was not critical.  Since that time more calls have been made, including
     * one in startGlobalTransaction() so a linear search may no longer
     * be appropriate.  See DERBY-828.
     *
	 * @return The ContextManager of the transaction being searched for.
     *
     * @param global_id The global transaction we are searching for.
     **/
	public ContextManager findTransactionContextByGlobalId(
    final GlobalXactId global_id)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
        for (TransactionTableEntry entry : trans.values()) {
            GlobalTransactionId entry_gid = entry.getGid();
            if (entry_gid != null && entry_gid.equals(global_id)) {
                return entry.getXact().getContextManager();
            }
        }

        return null;
	}


	/***********************************************************
	 * called when system is being quiesced, must be MT - safe
	 ***********************************************************/
	/**
		Return true if there is no transaction actively updating the database.
		New transaction may be started or old transaction committed
		right afterward, the caller of this routine must have other ways to
		stop transactions from starting or ending.

		<P>MT - safe
	*/
	boolean hasActiveUpdateTransaction()
	{
		synchronized (this)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
            for (TransactionTableEntry entry : trans.values()) {
                if (entry.isUpdate()) {
                    return true;
                }
            }
        }

        return false;
    }

	/************************************************************
	 * methods called only by checkpoint
	 ***********************************************************/
	/*
	 * Formatable methods
	 */

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.RAW_STORE_TRANSACTION_TABLE;
	}

	/**
	  @exception IOException problem reading the transaction table
	*/
	public void writeExternal(final ObjectOutput out) throws IOException
	{
		//don't let the transactions status change while writing out(beetle:5533)
        // We don't care if transactions are added or removed from the table
        // while we're writing it out, as long as the number of update
        // transactions is constant. Synchronizing on "this" prevents other
        // threads from adding or removing update transactions.

		synchronized(this)
        {
            int count = 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
            for (TransactionTableEntry entry : trans.values()) {
                if (entry.isUpdate()) {
                    count++;
                }
            }

            CompressedNumber.writeInt(out, count);

            // now write them out
            if (count > 0)
            {
                // Count the number of writes in debug builds.
                int writeCount = 0;

//IC see: https://issues.apache.org/jira/browse/DERBY-6242
                for (TransactionTableEntry entry : trans.values()) {
                    if (entry.isUpdate()) {
                        // only write out update transactions
                        out.writeObject(entry);
                        if (SanityManager.DEBUG) {
                            writeCount++;
                        }
                    }
                }

                // Verify that we wrote the expected number of transactions.
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(count == writeCount);
                }
			}
		}
	}

	/************************************************************
	 * methods called only by recovery
	 ************************************************************/


	/**
	  @exception IOException problem reading the transaction table
	  @exception ClassNotFoundException problem reading the transaction table
	 */
	public void readExternal(ObjectInput in) 
		 throws IOException, ClassNotFoundException
	{
		// RESOLVE: this is only read in checkpoint record, but we have not
		// finish the work on using this transaction table to cut down on redo
		// so this transaction table is effectively and futilely thrown away!

		int count = CompressedNumber.readInt(in);
		if (count == 0)
			return;

		for (int i = 0; i < count; i++)
		{
			TransactionTableEntry ent = 
				(TransactionTableEntry)in.readObject();

			if (SanityManager.DEBUG)
				SanityManager.ASSERT(
                    ent.getXid() != null,
                    "read in transaction table entry with null id");

			trans.put(ent.getXid(), ent);

			if (ent.isUpdate() && 
                XactId.compare(ent.getXid(), largestUpdateXactId) > 0)
            {
				largestUpdateXactId = ent.getXid();
            }
		}


	}

	/**
		Return the largest update transactionId I have seen so far.

		<P>MT - unsafe, caller is recovery, which is single threaded.
	*/
	public TransactionId largestUpdateXactId()
	{
		return largestUpdateXactId;
	}


	/**
		Is there an active internal transaction in the transaction table.

		<P>MT - unsafe, caller is recovery, which is single threaded.
	*/
	public boolean hasRollbackFirstTransaction()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
        for (TransactionTableEntry ent : trans.values())
		{
			if (ent != null && ent.isRecovery() && 
				(ent.getTransactionStatus() & 
                     Xact.RECOVERY_ROLLBACK_FIRST) != 0)
            {
				return true;
            }
		}
		return false;
	}

	/**
       Is there a prepared transaction that are recovered 
       durring the recovery in the transaction table.

       <P>MT - unsafe, caller is recovery, which is single threaded.
    */
    public boolean hasPreparedRecoveredXact()
    {
        return hasPreparedXact(true);
    }


    /**
       Is there a prepared transaction in the transaction table.
       <P>MT - unsafe, called during boot, which is single threaded.
    */
    public boolean hasPreparedXact()
    {
        return hasPreparedXact(false);
    }

    /**
     * Is there a prepared transaction in the transaction table.
     * 
     * <P>MT - unsafe, caller is recovery/at boot, which is single threaded.
     *
     * @param recovered  <code> true </code> to search  for transaction 
     *                  that are in prepared during recovery.  
     *                  recovered tranaction. 
     *                  <code> false &gt; to search for just prepared 
     *                  transactons. 
     * @return         <code> true if there is a prepared transaction and
     *                  recovered when <code> recovered </code> argument is 
     *                  <code> true </code>
     */

    private boolean hasPreparedXact(boolean recovered)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
        for (TransactionTableEntry ent : trans.values())
        {
            if (ent != null && 
                (ent.getTransactionStatus() & Xact.END_PREPARED) != 0)
            {
                if (recovered) {
                    if(ent.isRecovery())
                        return true;
                } else {
                    return true;
                }
            }
        }
        return false;
    }




	/**
		Get the most recently added transaction that says it needs to be
		rolled back first (an InternalXact) from the transaction table and make
		the passed in transaction assume its identity. 
		<B> Should only be used in recovery undo !! </B>
		RESOLVE: (sku)I don't think even these internal transactions need to be
		rolled back in the reverse order, because they are physical in nature.
		But it won't hurt.

		<P>MT - unsafe, caller is recovery, which is single threaded.
	*/
	public boolean getMostRecentRollbackFirstTransaction(RawTransaction tran)
	{

		if (trans.isEmpty())
		{
			// set tranaction to idle
			return findAndAssumeTransaction((TransactionId)null, tran);
		}

		TransactionId id = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
        for (TransactionTableEntry ent : trans.values())
		{
			if (ent != null && ent.isUpdate() && ent.isRecovery() &&
				(ent.getTransactionStatus() & Xact.RECOVERY_ROLLBACK_FIRST) != 0)
			{
				// try to locate the most recent one
				if (id == null || XactId.compare(id, ent.getXid()) < 0)
					id = ent.getXid();
			}
		}

		if (id == null)			// set transaction to idle
		{
			return findAndAssumeTransaction(id, tran);
		}
		else
		{
			// there is a rollback first transaction
			boolean found = 
                findAndAssumeTransaction(id, tran);

			if (SanityManager.DEBUG)
            {
                if (!found)
                {
                    SanityManager.THROWASSERT(
                        "cannot find transaction " + id + " in table");
                }
            }

			return true;
		}
	}

	/**
		Get the most recently non-prepared added transaction from the 
        transaction table and make the passed in transaction assume its 
        identity.  Prepared transactions will not be undone.

		RESOLVE: (sku) I don't think normal user transactions needs to be
		rolled back in order, but it won't hurt.

		<B> Should only be used in recovery undo !! </B>

		<P>MT - unsafe, caller is recovery, which is single threaded.
	*/
	public boolean getMostRecentTransactionForRollback(RawTransaction tran)
	{
        TransactionId id = null;

        if (!trans.isEmpty())
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
            for (TransactionTableEntry ent : trans.values())
			{
				if (ent != null         && 
                    ent.isUpdate()      && 
                    ent.isRecovery()    && 
                    !ent.isPrepared())
				{
					// try to locate the most recent one
					if (id == null || XactId.compare(id, ent.getXid()) < 0)
						id = ent.getXid();
				}

				if (SanityManager.DEBUG)
				{
					if (ent != null         && 
                        ent.isUpdate()      && 
                        ent.isRecovery()    &&
						(ent.getTransactionStatus() & 
                         Xact.RECOVERY_ROLLBACK_FIRST) != 0)
                    {
						SanityManager.THROWASSERT(
                            "still rollback first xacts in the tran table!");
                    }
				}
			}

			if (SanityManager.DEBUG)
            {
                // if all transactions are prepared then it is possible that
                // no transaction will be found, in that case id will be null.
                if (id != null)
                {
                    SanityManager.ASSERT(findTransactionEntry(id) != null);
                }
                else
                {
                    // all transactions in the table must be prepared.
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
                    for (TransactionTableEntry ent : trans.values())
                    {
                        SanityManager.ASSERT(ent.isPrepared());
                    }
                }
            }
		}

        return(findAndAssumeTransaction(id, tran));
	}	

	/**
		Get the most recently added transaction that says it is prepared during
        recovery the transaction table and make the passed in transaction 
        assume its identity. This routine turns off the isRecovery() state
		<B> Should only be used in recovery handle prepare after undo !! </B>

		<P>MT - unsafe, caller is recovery, which is single threaded.
	*/

    /**
     * Get the most recent recovered prepared transaction.
     * <p>
     * Get the most recently added transaction that says it is prepared during 
     * recovery the transaction table and make the passed in transaction 
     * assume its identity. 
     * <p>
     * This routine, unlike the redo and rollback getMostRecent*() routines
     * expects a brand new transaction to be passed in.  If a candidate 
     * transaction is found, then upon return the transaction table will 
     * be altered such that the old entry no longer exists, and a new entry
     * will exist pointing to the transaction passed in.  The new entry will
     * look the same as if the prepared transaction had been created during
     * runtime rather than recovery.
     *
     * <B> Should only be used in recovery handle prepare after undo !! </B>
     *
     * <P>MT - unsafe, caller is recovery, which is single threaded.
     *
	 * @return true if a candidate transaction has been found.  false if no
     *         prepared/recovery transactions found in the table.
     *
     * @param tran   Newly allocated transaction to add to link to a entry.
     *
     **/
	public boolean getMostRecentPreparedRecoveredXact(
    RawTransaction tran)
	{
        TransactionTableEntry   found_ent   = null;

        if (!trans.isEmpty())
		{
            TransactionId           id          = null;
            GlobalTransactionId     gid         = null;

//IC see: https://issues.apache.org/jira/browse/DERBY-6242
            for (TransactionTableEntry ent : trans.values())
			{
				if (ent != null         && 
                    ent.isRecovery()    && 
                    ent.isPrepared())
				{
					// try to locate the most recent one
					if (id == null || XactId.compare(id, ent.getXid()) < 0)
                    {
                        found_ent = ent;
						id        = ent.getXid();
						gid       = ent.getGid();
                    }
				}
			}

            if (SanityManager.DEBUG)
            {
                if (found_ent == null)
                {
                    // if no entry's were found then the transaction table
                    // should have the passed in idle tran, and the rest should
                    // be non-recover, prepared global transactions.
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
                    for (TransactionTableEntry ent : trans.values())
                    {
                        if (XactId.compare(ent.getXid(), tran.getId()) != 0)
                        {
                            SanityManager.ASSERT(
                                !ent.isRecovery() && ent.isPrepared());
                            SanityManager.ASSERT(ent.getGid() != null);
                        }
                    }
                }
            }

            if (found_ent != null)
            {
                // At this point there are 2 tt entries of interest:
                //     new_ent - the read only transaction entry that was 
                //               created when we allocated a new transaction.
                //               We will just throw this one away after 
                //               assuming the identity of the global xact.
                //     found_ent
                //             - the entry of the transaction that we are going
                //               to take over.
                TransactionTableEntry new_ent =
                    trans.remove(tran.getId());
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

                // At this point only the found_ent should be in the table.
                if (SanityManager.DEBUG)
                {
	                SanityManager.ASSERT(findTransactionEntry(id) == found_ent);
                }

                ((Xact) tran).assumeGlobalXactIdentity(found_ent);

                // transform this recovery entry, into a runtime entry.
                found_ent.unsetRecoveryStatus();
            }
		}

        return(found_ent != null);
	}

	/**
		Get the least recently added (oldest) transaction
		@return the RawTransaction's first log instant

		<P>MT - safe, caller can be recovery or checkpoint
	*/
	public LogInstant getFirstLogInstant()
	{
		// assume for now that it is acceptable to return null if a transaction
		// starts right in the middle of this call.

//IC see: https://issues.apache.org/jira/browse/DERBY-6242
        LogInstant logInstant = null;
        for (TransactionTableEntry entry : trans.values()) {
            if (entry.isUpdate()) {
                if (logInstant == null ||
                        entry.getFirstLog().lessThan(logInstant)) {
                    logInstant = entry.getFirstLog();
                }
            }
        }

        return logInstant;
	}

	/**
		Find a transaction using the transaction id, and make the passed in
		transaction assume the identity and properties of that transaction.

		<P>MT - unsafe, caller is recovery, which is single threaded.

		@param id transaction Id
		@param tran the transaction that was made to assume the transactionID
		and all other relevant information stored in the transaction table
		@return true if transaction can be found, false otherwise
	*/
	boolean findAndAssumeTransaction(
    TransactionId       id, 
    RawTransaction      tran)
	{
		// the only caller for this method right now is recovery.  
        // No need to put in any concurrency control
		TransactionTableEntry ent = null;

		if (id != null && !trans.isEmpty())
		{
			ent = findTransactionEntry(id);

			if (SanityManager.DEBUG)
			{
				if (ent != null)
					SanityManager.ASSERT(ent.isRecovery(),
					"assuming the id of a non-recovery transaction");
			}
		}

		// if no transaction entry found, set transaction to idle
        ((Xact)tran).assumeIdentity(ent);

		return(ent != null);

	}

	/**********************************************************
	 * Transaction table vti and diagnostics
	 * MT - unsafe, caller is getting a snap shot which may be inconsistent 
	 *********************************************************/

	/**
		Get a printable version of the transaction table
	 */
	public TransactionInfo[] getTransactionInfo()
	{
		if (trans.isEmpty())
			return null;

        if (SanityManager.DEBUG) {
            SanityManager.DEBUG("TranTrace", toString());
        }

        final ArrayList<TransactionTableEntry> tinfo = new ArrayList<TransactionTableEntry>();

        // Get clones of all the entries in the transaction table.
//IC see: https://issues.apache.org/jira/browse/DERBY-6242
        for (TransactionTableEntry entry : trans.values()) {
            tinfo.add((TransactionTableEntry) entry.clone());
        }

        return tinfo.toArray(new TransactionTableEntry[tinfo.size()]);
	}

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			final StringBuffer str = new StringBuffer(1000).
				append("\n**************************\n").
				append(super.toString()).
				append("\nTransaction Table: size = ").append(trans.size()).
				append(" largestUpdateXactId = ").append(largestUpdateXactId).
				append("\n");

            boolean hasReadOnlyTransaction = false;

//IC see: https://issues.apache.org/jira/browse/DERBY-6242
            for (TransactionTableEntry entry : trans.values()) {
                if (entry.isUpdate()) {
                    str.append(entry);
                } else {
                    hasReadOnlyTransaction = true;
                }
            }

            if (hasReadOnlyTransaction)
			{
				str.append("\n READ ONLY TRANSACTIONS \n");

                for (TransactionTableEntry entry : trans.values()) {
                    if (!entry.isUpdate()) {
                        str.append(entry);
                    }
                }
			}
			str.append("---------------------------");
			return str.toString();
		}
		else
			return null;
	}


}

