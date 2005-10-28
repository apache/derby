/*

   Derby - Class org.apache.derby.iapi.store.raw.xact.RawTransaction

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

package org.apache.derby.iapi.store.raw.xact;

import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.services.locks.LockFactory;

import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.GlobalTransactionId;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.catalog.UUID;


import java.util.Observable;

import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	RawTransaction is the form of Transaction used within the raw store. This
	allows the break down of RawStore functionality into (at least) three modules
	(Transactions, Data, Log) without exposing internal information on the
	external interface.

	<P>
	The transaction will notify any Observer's just before the transaction
	is committed, aborted or a rollback to savepoint occurs. The argument passed
	to the update() method of the Observer's will be one of
	<UL>
	<LI> RawTransaction.COMMIT - transaction is committing
	<LI> RawTransaction.ABORT - transaction is aborting
	<LI> RawTransaction.SAVEPOINTROLLBACK - transaction is being rolled back to a savepoint
	</UL>
	The observer's must perform a value equality check (equals()) on the 
    update arg to see why it is being notified.

	@see java.util.Observer
*/

public abstract class RawTransaction extends Observable implements Transaction {

	public static final Integer		COMMIT =             new Integer(0);
	public static final Integer		ABORT =              new Integer(1);
	public static final Integer     SAVEPOINT_ROLLBACK = new Integer(2);
	public static final Integer		LOCK_ESCALATE      = new Integer(3);

	protected StandardException		observerException;

	/**	
		Get the lock factory to be used during this transaction.
	*/
	public abstract LockFactory getLockFactory();

	/**	
		Get the data factory to be used during this transaction.
	*/
	public abstract DataFactory getDataFactory();

	/**
		Get cache statistics for the specified cache
	*/
	public abstract long[] getCacheStats(String cacheName);

	/**
		Reset the cache statistics for the specified cache
	*/
	public abstract void resetCacheStats(String cacheName);

	/**
		Get the log buffer to be used during this transaction.
	*/
	public abstract DynamicByteArrayOutputStream getLogBuffer();

	/**
		Log a compensation operation and then action it in the context of this 
        transaction.
		The CompensationOperation is logged in the transaction log file and 
        then its doMe method is called to perform the required change.  This 
        compensation operation will rollback the change that was done by the 
        Loggable Operation at undoInstant. 

		@param compensation	the Compensation Operation
		@param undoInstant	the LogInstant of the Loggable Operation this 
							compensation operation is going to roll back
		@param in			optional data for the rollback operation
		@param dataLengt	optional data length

		@see Compensation

		@exception StandardException  Standard cloudscape exception policy
	*/
	public abstract void logAndUndo(Compensation compensation, LogInstant undoInstant, 
									LimitObjectInput in) 
		throws StandardException;

	/** Methods to help logging and recovery */

	/** 
		Set the transaction Ids (Global and internal) of this transaction
	*/
	public abstract void setTransactionId(GlobalTransactionId id, TransactionId shortId);

	/**
		Set the transactionId (Global and internal) of this transaction using a
		log record that contains the Global id
	*/
	abstract public void setTransactionId(Loggable beginXact, TransactionId shortId);

		
	/**
		Get the shortId of this transaction.  May return null if transactio
		has no ID.
	*/
	abstract public TransactionId getId();

	/**
		Get the shortId of this transaction.  May return null if transactio
		has no ID.
	*/
	abstract public GlobalTransactionId getGlobalId();

	/**
		Add this raw transaction on to the list of update transaction
	*/
	public abstract void addUpdateTransaction(int transactionStatus);

	/**
		Remove this raw transaction from the list of update transaction
	*/
	public abstract void removeUpdateTransaction();

	/**
		Change the state of transaction in table to prepare.
	*/
	public abstract void prepareTransaction();

	/**
		Set the log instant for the first log record written by this 
        transaction.
	*/
	abstract public void setFirstLogInstant(LogInstant instant);

	/**
		Get the log instant for the first log record written by this 
        transaction.
	*/
	abstract public LogInstant getFirstLogInstant();

	/**
		Set the log instant for the last log record written by this transaction. 
	*/
	abstract public void setLastLogInstant(LogInstant instant);

	/**
		Get the log instant for the last log record written by this transaction. 
		If the transaction is unclear what its last log instant is, 
		than it may return null.
	*/
	abstract public LogInstant getLastLogInstant();


	/**
		Check to see if a logical operation is allowed by this transaction, 
        throws a TransactionExceotion if it isn't. This implementation allows
		logical operations. Transactions that need to disallow logical 
        operations should hide this method.

		@exception StandardException Standard Cloudscape error policy,
	*/
	public void checkLogicalOperationOk() throws StandardException {
	}

	/**
		Return true if this transaction should be rolled back first
		in recovery. This implementation returns false. Transactions that
		need to rollback first during recovery should hide this method.
	*/
	public boolean recoveryRollbackFirst() {
		return false;
	}

    /**
     * During recovery re-prepare a transaction.
     * <p>
     * After redo() and undo(), this routine is called on all outstanding 
     * in-doubt (prepared) transactions.  This routine re-acquires all 
     * logical write locks for operations in the xact, and then modifies
     * the transaction table entry to make the transaction look as if it
     * had just been prepared following startup after recovery.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    abstract public void reprepare()
		throws StandardException;

	/**
		Allow an Observer to indicate an exception to the transaction that
		is raised in its update() method.
	*/
	public void setObserverException(StandardException se) {
		if (observerException == null)
			observerException = se;
	}

	/**
		Start a nested top transaction. A nested top transaction behaves exactly
		like a user transaction. Nested top transaction allow system type work
		to proceed in a separate transaction to the current user transaction
		and be committed independently of the user transaction (usually before
		the user transaction).
		Only one nested top transaction can be active in a context at any one
        time.
		After a commit the transaction may be re-used.

		A nested top transaction conflicts on the logical locks of its "parent"
        transaction.

		@exception StandardException Standard Cloudscape error policy
	*/

	public abstract RawTransaction startNestedTopTransaction() throws StandardException;


	/**
		Open a container that may be dropped - use only by logging and recovery.
		During recovery redo, a log record may refer to a container that has
		long been dropped.  This interface is provided so a dropped container
		may be opened.

		If the container has been dropped and is known to be committed, then
		even if we open the dropped container with forUpdate true, the
		container will be silently opened as read only.  Logging and recovery
		code always check for committed drop status.  Anybody else wanting to
		use this interface must keep this in mind.

		@exception StandardException  Standard cloudscape exception policy
	*/
	public abstract RawContainerHandle openDroppedContainer
		(ContainerKey containerId, LockingPolicy locking)
		 throws StandardException;

	/**
		Recreate a container during load tran - use only by media recovery.

		@exception StandardException  Standard cloudscape exception policy
	 */
	public abstract void reCreateContainerForLoadTran
		(long segmentId, long containerId, ByteArray containerInfo)
		throws StandardException;


	/**
		Status that needs to go into the begin transaction log record, if there
		is one, to help with recovery
	*/
	protected abstract int statusForBeginXactLog();

	/**
		Status that needs to go into the end transaction log record, if there
		is one, to help with recovery
	*/
	protected abstract int statusForEndXactLog();

	/**	
		Is the transaction in the middle of an abort.
	*/
	public abstract boolean inAbort();

	/**
		Can this transaction handles post termination work
	*/
	public abstract boolean handlesPostTerminationWork();

	/**
		Make this transaction aware that it is being used by recovery
	 */
	public abstract void recoveryTransaction();

	/**
		Allow my users to notigy my observers.
	*/
	public void notifyObservers(Object arg) {
		if (countObservers() != 0) {
			setChanged();
			super.notifyObservers(arg);
		}
	}

	
	/**	
	 *Retunrs true if the transaction is part of rollforward recovery
	 */
	public abstract boolean inRollForwardRecovery();


	/**	
	 * redo a checkpoint during rollforward recovery
	 */
	public abstract void checkpointInRollForwardRecovery(LogInstant cinstant,
														 long redoLWM) 
		throws StandardException;

}


